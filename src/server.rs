//! [`Server`]: a schema's resources mounted on an axum [`Router`].

use std::collections::BTreeMap;
use std::collections::btree_map::Entry as Slot;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, MethodRouter, get};
use fold::{Db, Reader, Writer};
use schemars::SchemaGenerator;
use schemars::generate::SchemaSettings;
use serde_json::{Value, json};

use crate::openapi::{self, Info};
use crate::resource::{Call, Describe, Entry, Method};
use crate::wire::{require_json, respond};
use crate::{IntoProblem, Problem, Serve};

/// Serves one database: every resource its schema exposes, plus the
/// `/_`-prefixed meta routes and `/openapi.json`.
///
/// The library never owns a runtime or a listener: [`into_router`]
/// returns a plain [`Router`] to nest, layer, and serve however the
/// application likes.
///
/// [`into_router`]: Server::into_router
pub struct Server<S: Serve> {
    reader: Reader<S>,
    writer: Writer<S>,
    title: String,
    version: String,
    description: Option<String>,
}

struct Shared<S: Serve> {
    reader: Reader<S>,
    writer: Writer<S>,
    entries: Vec<Entry<S>>,
    described: Vec<Describe>,
    openapi: Value,
    schema: Value,
}

type St<S> = State<Arc<Shared<S>>>;

impl<S: Serve> Server<S> {
    /// Serve `db`, moving it onto its own writer thread (see
    /// [`Db::serve`]). The database closes once the router, and every
    /// clone of it, is dropped.
    pub fn new(db: Db<S>) -> Self
    where
        Db<S>: Send,
    {
        let (reader, writer) = db.serve();
        Server::from_parts(reader, writer)
    }

    /// Serve an already-served database, sharing its writer with the
    /// rest of the application.
    pub fn from_parts(reader: Reader<S>, writer: Writer<S>) -> Self {
        let name = std::any::type_name::<S>();
        Server {
            reader,
            writer,
            title: name.rsplit("::").next().unwrap_or(name).to_owned(),
            version: "0".to_owned(),
            description: None,
        }
    }

    /// The OpenAPI `info.title`; the schema's type name by default.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    /// The OpenAPI `info.version` (of your API, not of fold-http).
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// The OpenAPI document this server will serve, without serving it.
    pub fn openapi(&self) -> Value {
        let entries = S::entries();
        let (described, schemas) = describe(&entries);
        openapi::document(&self.info(), &described, schemas)
    }

    fn info(&self) -> Info<'_> {
        Info {
            title: &self.title,
            version: &self.version,
            description: self.description.as_deref(),
        }
    }

    /// Mount every route.
    ///
    /// # Panics
    /// If a resource name is reserved (`_`-prefixed) or not a path
    /// segment, or two routes claim the same method and path: mistakes
    /// in the schema, caught at startup rather than per request.
    pub fn into_router(self) -> Router {
        let entries = S::entries();
        let (described, schemas) = describe(&entries);
        let openapi = openapi::document(&self.info(), &described, schemas.clone());
        let schema = json!({ "resources": &described, "components": { "schemas": schemas } });
        let shared = Arc::new(Shared {
            reader: self.reader,
            writer: self.writer,
            entries,
            described,
            openapi,
            schema,
        });

        let mut mounted: BTreeMap<String, (MethodRouter<Arc<Shared<S>>>, Vec<Method>)> =
            BTreeMap::new();
        for (i, d) in shared.described.iter().enumerate() {
            let name = &d.name;
            assert!(
                !name.is_empty() && !name.starts_with('_') && !name.contains('/'),
                "resource name {name:?} is reserved or not a path segment",
            );
            for (j, route) in d.routes.iter().enumerate() {
                let path = format!("/{name}{}", route.path);
                let (router, methods) = match mounted.entry(path.clone()) {
                    Slot::Vacant(v) => v.insert((MethodRouter::new(), Vec::new())),
                    Slot::Occupied(o) => o.into_mut(),
                };
                assert!(
                    !methods.contains(&route.method),
                    "two routes for {:?} {path}",
                    route.method,
                );
                methods.push(route.method);
                let filter = match route.method {
                    Method::Get => MethodFilter::GET,
                    Method::Post => MethodFilter::POST,
                    Method::Put => MethodFilter::PUT,
                    Method::Delete => MethodFilter::DELETE,
                };
                let taken = std::mem::take(router);
                *router = if route.path.contains("{key}") {
                    taken.on(
                        filter,
                        move |State(st): St<S>,
                              key: Result<Path<String>, PathRejection>,
                              RawQuery(query): RawQuery,
                              headers: HeaderMap,
                              body: Bytes| async move {
                            match key {
                                Ok(Path(key)) => {
                                    dispatch(st, i, j, Some(key), query, headers, body).await
                                }
                                Err(e) => Problem::bad_request(e.body_text()).into_response(),
                            }
                        },
                    )
                } else {
                    taken.on(
                        filter,
                        move |State(st): St<S>,
                              RawQuery(query): RawQuery,
                              headers: HeaderMap,
                              body: Bytes| async move {
                            dispatch(st, i, j, None, query, headers, body).await
                        },
                    )
                };
            }
        }

        let mut router: Router<Arc<Shared<S>>> = Router::new()
            .route(
                "/_schema",
                get(|State(st): St<S>| async move { axum::Json(st.schema.clone()) }),
            )
            .route(
                "/_health",
                get(|| async { axum::Json(json!({ "status": "ok" })) }),
            )
            .route(
                "/openapi.json",
                get(|State(st): St<S>| async move { axum::Json(st.openapi.clone()) }),
            );
        for (path, (method_router, _)) in mounted {
            router = router.route(&path, method_router);
        }
        router
            .fallback(|uri: Uri| async move {
                Problem::not_found(format!("no route for {}", uri.path()))
            })
            .layer(axum::middleware::map_response(problem_405))
            .with_state(shared)
    }
}

/// Every entry's description, sharing one set of schema components.
fn describe<S: Serve>(entries: &[Entry<S>]) -> (Vec<Describe>, serde_json::Map<String, Value>) {
    let mut settings = SchemaSettings::draft2020_12();
    settings.definitions_path = "/components/schemas".into();
    settings.meta_schema = None;
    let mut g: SchemaGenerator = settings.into_generator();
    let described = entries
        .iter()
        .map(|e| {
            let mut d = (e.describe)(&e.name, &mut g);
            d.description = e.description.clone();
            d
        })
        .collect();
    g.subschema_for::<Problem>();
    (described, g.take_definitions(true))
}

async fn dispatch<S: Serve>(
    st: Arc<Shared<S>>,
    i: usize,
    j: usize,
    key: Option<String>,
    query: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let route = &st.described[i].routes[j];
    let call = Call {
        op: route.op,
        key,
        query,
        body,
    };
    if route.method.is_read() {
        return read(st, i, call).await;
    }
    if route.body.is_some()
        && let Err(p) = require_json(&headers)
    {
        return p.into_response();
    }
    write(st, i, j, call).await
}

/// Answer a read from one fresh snapshot, off the async workers.
async fn read<S: Serve>(st: Arc<Shared<S>>, i: usize, call: Call) -> Response {
    let res = tokio::task::spawn_blocking(move || {
        let snap = st.reader.snapshot();
        let facades = st.reader.at(&snap);
        (st.entries[i].read)(&facades, &call).map(|data| (snap.frontier().outer, data))
    })
    .await;
    match res {
        Ok(Ok((epoch, data))) => respond(StatusCode::OK, epoch, data),
        Ok(Err(problem)) => problem.into_response(),
        Err(e) => Problem::internal(format!("read failed: {e}")).into_response(),
    }
}

/// Decode a write here, then apply it on the writer thread as one
/// transaction.
async fn write<S: Serve>(st: Arc<Shared<S>>, i: usize, j: usize, call: Call) -> Response {
    let Some(stage) = &st.entries[i].stage else {
        return Problem::method_not_allowed().into_response();
    };
    let apply = match stage(call) {
        Ok(apply) => apply,
        Err(problem) => return problem.into_response(),
    };
    let writer = st.writer.clone();
    let res = tokio::task::spawn_blocking(move || {
        writer.write(move |tx| {
            // This transaction commits as the snapshot's outer epoch;
            // the first frontier that includes it is the next one.
            let epoch = tx.snap().frontier().outer + 1;
            apply(&mut **tx).map(|written| (epoch, written))
        })
    })
    .await;
    match res {
        Ok(Ok((epoch, written))) => {
            let route = &st.described[i].routes[j];
            let status = if written.created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            let location = (written.created && route.method == Method::Post)
                .then(|| location(&st.described[i].name, &written.data))
                .flatten();
            let mut res = respond(status, epoch, written.data);
            if let Some(location) = location {
                res.headers_mut().insert(header::LOCATION, location);
            }
            res
        }
        Ok(Err(e)) => e.into_problem().into_response(),
        // `Writer` panics only when the database thread is gone.
        Err(e) if e.is_panic() => {
            Problem::unavailable("the database writer has stopped").into_response()
        }
        Err(e) => Problem::internal(format!("write failed: {e}")).into_response(),
    }
}

/// `Location` of a minted row, from the key its write reported. Only
/// keys that are a path segment as written (numbers, plain strings) get
/// one.
fn location(name: &str, data: &Value) -> Option<HeaderValue> {
    let segment = match data.get("key")? {
        Value::String(s) if !s.contains(['/', '?', '#', '%']) => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    HeaderValue::try_from(format!("/{name}/{segment}")).ok()
}

/// axum answers a wrong method with an empty 405; give it the problem
/// body every other error has, keeping its `Allow` header.
async fn problem_405(res: Response) -> Response {
    if res.status() != StatusCode::METHOD_NOT_ALLOWED
        || res.headers().contains_key(header::CONTENT_TYPE)
    {
        return res;
    }
    let allow = res.headers().get(header::ALLOW).cloned();
    let mut out = Problem::method_not_allowed().into_response();
    if let Some(allow) = allow {
        out.headers_mut().insert(header::ALLOW, allow);
    }
    out
}
