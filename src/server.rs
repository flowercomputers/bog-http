//! [`Server`]: a schema's resources mounted on an axum [`Router`].

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get};
use fold::{Db, Reader, Writer};
use serde_json::json;

use crate::resource::{Entry, ReadOp, WriteOp};
use crate::wire::{ListParams, require_json, respond};
use crate::{IntoProblem, Problem, Serve};

/// Serves one database: every resource its schema exposes, plus the
/// `/_`-prefixed meta routes.
///
/// The library never owns a runtime or a listener: [`into_router`]
/// returns a plain [`Router`] to nest, layer, and serve however the
/// application likes.
///
/// [`into_router`]: Server::into_router
pub struct Server<S: Serve> {
    reader: Reader<S>,
    writer: Writer<S>,
}

struct Shared<S: Serve> {
    reader: Reader<S>,
    writer: Writer<S>,
    entries: Vec<Entry<S>>,
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
        Server { reader, writer }
    }

    pub fn into_router(self) -> Router {
        let shared = Arc::new(Shared {
            reader: self.reader,
            writer: self.writer,
            entries: S::entries(),
        });
        let mut router: Router<Arc<Shared<S>>> = Router::new()
            .route("/_schema", get(schema::<S>))
            .route("/_health", get(health));
        for (i, entry) in shared.entries.iter().enumerate() {
            let name = &entry.describe.name;
            assert!(
                !name.is_empty() && !name.starts_with('_') && !name.contains('/'),
                "resource name {name:?} is reserved or not a path segment",
            );
            let ops = entry.describe.ops;

            let mut collection = MethodRouter::new();
            if ops.list {
                collection = collection.get(move |State(st): St<S>, uri: Uri| async move {
                    match Query::<ListParams>::try_from_uri(&uri) {
                        Ok(Query(params)) => read(st, i, ReadOp::List(params)).await,
                        Err(e) => Problem::bad_request(e.body_text()).into_response(),
                    }
                });
            }
            if ops.insert {
                collection = collection.post(
                    move |State(st): St<S>, headers: HeaderMap, body: Bytes| async move {
                        if let Err(p) = require_json(&headers) {
                            return p.into_response();
                        }
                        write(st, i, WriteOp::Insert(body)).await
                    },
                );
            }

            let mut item = MethodRouter::new();
            if ops.get {
                item = item.get(
                    move |State(st): St<S>, key: Result<Path<String>, PathRejection>| async move {
                        match key {
                            Ok(Path(key)) => read(st, i, ReadOp::Get(key)).await,
                            Err(e) => Problem::bad_request(e.body_text()).into_response(),
                        }
                    },
                );
            }
            if ops.upsert {
                item = item.put(
                    move |State(st): St<S>,
                          key: Result<Path<String>, PathRejection>,
                          headers: HeaderMap,
                          body: Bytes| async move {
                        let key = match key {
                            Ok(Path(key)) => key,
                            Err(e) => return Problem::bad_request(e.body_text()).into_response(),
                        };
                        if let Err(p) = require_json(&headers) {
                            return p.into_response();
                        }
                        write(st, i, WriteOp::Upsert(key, body)).await
                    },
                );
            }
            if ops.remove {
                item = item.delete(
                    move |State(st): St<S>, key: Result<Path<String>, PathRejection>| async move {
                        match key {
                            Ok(Path(key)) => write(st, i, WriteOp::Remove(key)).await,
                            Err(e) => Problem::bad_request(e.body_text()).into_response(),
                        }
                    },
                );
            }

            if ops.list || ops.insert {
                router = router.route(&format!("/{name}"), collection);
            }
            if ops.get || ops.upsert || ops.remove {
                router = router.route(&format!("/{name}/{{key}}"), item);
            }
        }
        router
            .fallback(|uri: Uri| async move {
                Problem::not_found(format!("no route for {}", uri.path()))
            })
            .layer(axum::middleware::map_response(problem_405))
            .with_state(shared)
    }
}

/// Answer a read from one fresh snapshot, off the async workers.
async fn read<S: Serve>(st: Arc<Shared<S>>, i: usize, op: ReadOp) -> Response {
    let res = tokio::task::spawn_blocking(move || {
        let snap = st.reader.snapshot();
        let facades = st.reader.at(&snap);
        (st.entries[i].read)(&facades, &op).map(|data| (snap.frontier().outer, data))
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
async fn write<S: Serve>(st: Arc<Shared<S>>, i: usize, op: WriteOp) -> Response {
    let Some(stage) = &st.entries[i].stage else {
        return Problem::method_not_allowed().into_response();
    };
    let apply = match stage(op) {
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
            let status = if written.created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            let location = written
                .created
                .then(|| location(&st.entries[i].describe.name, &written.data))
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

/// `Location` of a created row, from the key its write reported. Only
/// keys that are a path segment as written (numbers, plain strings) get
/// one.
fn location(name: &str, data: &serde_json::Value) -> Option<HeaderValue> {
    let segment = match data.get("key")? {
        serde_json::Value::String(s) if !s.contains(['/', '?', '#', '%']) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => return None,
    };
    HeaderValue::try_from(format!("/{name}/{segment}")).ok()
}

async fn schema<S: Serve>(State(st): St<S>) -> Response {
    let resources: Vec<_> = st.entries.iter().map(|e| &e.describe).collect();
    axum::Json(json!({ "resources": resources })).into_response()
}

async fn health() -> Response {
    axum::Json(json!({ "status": "ok" })).into_response()
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
