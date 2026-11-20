//! [`Server`]: a schema's resources mounted on an axum [`Router`].

use std::collections::btree_map::Entry as Slot;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::PathRejection;
use axum::extract::{DefaultBodyLimit, Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, MethodRouter, get, post};
use bog::schema::Db;
use futures_util::StreamExt;
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, SchemaGenerator};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::{Semaphore, broadcast, watch};

use crate::custom::{CUSTOM_OP, Custom, Handler, check_path};
use crate::guard::Guard;
use crate::live::{Change, Hubs};
use crate::openapi::{self, Info};
use crate::resource::{Call, Describe, Entry, Method, Route, TxFn, Written};
use crate::tx::{self, TxOp};
use crate::wire::{parse_body, parse_query, require_json, respond, to_json};
use crate::{Problem, Serve};

/// The request header naming the least epoch a read must reflect.
pub const MIN_EPOCH_HEADER: &str = "bog-min-epoch";

/// Serves one database: every resource its schema exposes, custom
/// routes, `POST /_tx`, the `/_`-prefixed meta routes, and
/// `/openapi.json`.
///
/// The library never owns a runtime or a listener: [`into_router`]
/// returns a plain [`Router`] to nest, layer, and serve however the
/// application likes.
///
/// [`into_router`]: Server::into_router
pub struct Server<S: Serve> {
    pub(crate) db: Arc<Db<S>>,
    pub(crate) title: String,
    pub(crate) version: String,
    pub(crate) description: Option<String>,
    pub(crate) custom: Vec<Custom<S>>,
    pub(crate) min_epoch_wait: Duration,
    pub(crate) max_pending_writes: usize,
    pub(crate) body_limit: usize,
    pub(crate) closing: watch::Sender<bool>,
    pub(crate) guard: Guard,
    pub(crate) activity: Arc<Activity>,
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    pub(crate) idle_exit: Option<Duration>,
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    pub(crate) discovery: Option<std::path::PathBuf>,
}

/// When the server last heard a request, and how many streams are open:
/// what [`Server::idle_exit`] watches.
pub(crate) struct Activity {
    start: std::time::Instant,
    /// Milliseconds after `start`.
    last: std::sync::atomic::AtomicU64,
    pub(crate) streams: std::sync::atomic::AtomicUsize,
}

impl Activity {
    fn new() -> Self {
        Activity {
            start: std::time::Instant::now(),
            last: 0.into(),
            streams: 0.into(),
        }
    }

    fn stamp(&self) {
        let now = self.start.elapsed().as_millis() as u64;
        self.last
            .fetch_max(now, std::sync::atomic::Ordering::Relaxed);
    }

    /// How long since the last request began, if no stream is open.
    #[cfg_attr(not(feature = "serve"), allow(dead_code))]
    pub(crate) fn idle(&self) -> Option<Duration> {
        if self.streams.load(std::sync::atomic::Ordering::Relaxed) > 0 {
            return None;
        }
        let last = Duration::from_millis(self.last.load(std::sync::atomic::Ordering::Relaxed));
        Some(self.start.elapsed().saturating_sub(last))
    }
}

/// Counts an open stream for as long as it lives.
struct Open(Arc<Activity>);

impl Open {
    fn new(activity: &Arc<Activity>) -> Self {
        activity
            .streams
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Open(activity.clone())
    }
}

impl Drop for Open {
    fn drop(&mut self) {
        self.0
            .streams
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Ends a server's live streams, so a graceful shutdown can finish:
/// open event streams otherwise never complete. [`Server::serve`] uses
/// it on ctrl-c / SIGTERM; an application serving the router itself
/// triggers it when it starts shutting down.
#[derive(Clone)]
pub struct Closer(watch::Sender<bool>);

impl Closer {
    pub fn close(&self) {
        self.0.send_replace(true);
    }
}

struct Shared<S: Serve> {
    db: Arc<Db<S>>,
    entries: Vec<Entry<S>>,
    described: Vec<Describe>,
    custom: Vec<Custom<S>>,
    custom_routes: Vec<Route>,
    openapi: Value,
    schema: Value,
    /// Admission control: one permit per write in flight.
    permits: Arc<Semaphore>,
    max_pending_writes: usize,
    /// The newest epoch a write through this server committed.
    committed: watch::Sender<u64>,
    min_epoch_wait: Duration,
    hubs: Arc<Hubs>,
    closing: watch::Sender<bool>,
    activity: Arc<Activity>,
}

type St<S> = State<Arc<Shared<S>>>;

impl<S: Serve> Server<S> {
    /// Serve `db`. Pass a `Db` to hand it over, or an `Arc<Db>` to keep
    /// reading and writing it from the rest of the application.
    pub fn new(db: impl Into<Arc<Db<S>>>) -> Self {
        let name = std::any::type_name::<S>();
        Server {
            db: db.into(),
            title: name.rsplit("::").next().unwrap_or(name).to_owned(),
            version: "0".to_owned(),
            description: None,
            custom: Vec::new(),
            min_epoch_wait: Duration::from_secs(5),
            max_pending_writes: 1024,
            body_limit: 2 << 20,
            closing: watch::Sender::new(false),
            guard: Guard::default(),
            activity: Arc::new(Activity::new()),
            idle_exit: None,
            discovery: None,
        }
    }

    /// Require `Authorization: Bearer <token>` on every request but
    /// `/_health`. Clients that can't set headers (`EventSource`) may
    /// pass `?access_token=<token>` instead; it's removed before routing.
    /// Documented in OpenAPI as a bearer security scheme.
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        let token: String = token.into();
        assert!(
            !token.is_empty(),
            "an empty bearer token admits no one meaningfully"
        );
        self.guard.token = Some(token.into());
        self
    }

    /// Answer only requests whose `Host` names one of `hosts` (any port):
    /// the defense against DNS rebinding, where a web page reaches a
    /// local server under its own domain. [`serve`](Server::serve) sets
    /// `localhost`, `127.0.0.1`, and `[::1]` itself when it binds a
    /// loopback address and this wasn't called.
    pub fn allowed_hosts<'a>(mut self, hosts: impl IntoIterator<Item = &'a str>) -> Self {
        let hosts: Vec<String> = hosts.into_iter().map(|h| h.to_ascii_lowercase()).collect();
        self.guard.hosts = Some(hosts.into());
        self
    }

    /// A handle that ends this server's live streams; see [`Closer`].
    pub fn closer(&self) -> Closer {
        Closer(self.closing.clone())
    }

    /// The OpenAPI `info.title`; the schema's type name by default.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    /// The OpenAPI `info.version` (of your API, not of bog-http).
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// How long a read with `bog-min-epoch` waits for that epoch before
    /// answering `503 epoch-not-reached`. 5 s by default.
    pub fn min_epoch_wait(mut self, wait: Duration) -> Self {
        self.min_epoch_wait = wait;
        self
    }

    /// How many writes may be queued or running at once; beyond that a
    /// write is refused with `503 overloaded` and `Retry-After`, rather
    /// than queueing without bound. 1024 by default; 0 makes the server
    /// read-only.
    pub fn max_pending_writes(mut self, n: usize) -> Self {
        self.max_pending_writes = n;
        self
    }

    /// The largest request body accepted, in bytes. 2 MiB by default.
    pub fn body_limit(mut self, bytes: usize) -> Self {
        self.body_limit = bytes;
        self
    }

    /// A custom read: `GET path`, answered by `f` from one snapshot with
    /// the decoded query `Q` (use [`NoParams`](crate::NoParams) for
    /// none). Documented in OpenAPI from `Q` and `R`.
    ///
    /// # Panics
    /// If `path` is `/_`-prefixed, has parameters, or doesn't start
    /// with `/`.
    pub fn get<Q, R, F>(mut self, path: &str, summary: &str, f: F) -> Self
    where
        Q: DeserializeOwned + JsonSchema + 'static,
        R: Serialize + JsonSchema + 'static,
        F: Fn(&S::Read<'_>, Q) -> Result<R, Problem> + Send + Sync + 'static,
    {
        check_path(path);
        let (p, s) = (path.to_owned(), summary.to_owned());
        self.custom.push(Custom {
            path: path.to_owned(),
            describe: Box::new(move |g| {
                Route::get(p.clone(), CUSTOM_OP, s.clone())
                    .query::<Q>()
                    .response::<R>(g)
            }),
            handler: Handler::Read(Arc::new(move |r: &S::Read<'_>, call: &Call| {
                to_json(&f(r, call.query()?)?)
            })),
        });
        self
    }

    /// A custom write: `POST path` with a JSON body `B`, run by `f` as
    /// one transaction. `f`'s `Err` rolls everything back and answers as
    /// a [`Problem`] (see its `From<bog::Error>`); so do the schema's
    /// constraints. To refuse with a status of your own, return
    /// `Err(bog::Error::reject(Problem::…))`.
    ///
    /// # Panics
    /// As [`get`](Server::get).
    pub fn post<B, R, F>(mut self, path: &str, summary: &str, f: F) -> Self
    where
        B: DeserializeOwned + JsonSchema + Send + 'static,
        R: Serialize + JsonSchema + 'static,
        F: Fn(&mut S::Write<'_>, B) -> bog::Result<R> + Send + Sync + 'static,
    {
        check_path(path);
        let (p, s) = (path.to_owned(), summary.to_owned());
        let f = Arc::new(f);
        self.custom.push(Custom {
            path: path.to_owned(),
            describe: Box::new(move |g| {
                Route::post(p.clone(), CUSTOM_OP, s.clone())
                    .body::<B>(g)
                    .response::<R>(g)
            }),
            handler: Handler::Write(Arc::new(move |call| {
                let body: B = call.body()?;
                let f = f.clone();
                let apply: TxFn<S> =
                    Box::new(move |tx: &mut S::Write<'_>| Ok(Written::ok(&f(tx, body)?)));
                Ok(apply)
            })),
        });
        self
    }

    /// The OpenAPI document this server will serve, without serving it.
    pub fn openapi(&self) -> Value {
        let entries = S::entries();
        let (described, custom, schemas) = describe(&entries, &self.custom);
        openapi::document(&self.info(), &described, &custom, schemas)
    }

    fn info(&self) -> Info<'_> {
        Info {
            title: &self.title,
            version: &self.version,
            description: self.description.as_deref(),
            bearer: self.guard.token.is_some(),
        }
    }

    /// Mount every route.
    ///
    /// # Panics
    /// If a resource name is reserved (`_`-prefixed) or not a path
    /// segment, or two routes claim the same method and path: mistakes
    /// in the schema or setup, caught at startup rather than per
    /// request.
    pub fn into_router(self) -> Router {
        let entries = S::entries();
        let (described, custom_routes, schemas) = describe(&entries, &self.custom);
        let openapi = openapi::document(&self.info(), &described, &custom_routes, schemas.clone());
        let schema = json!({ "resources": &described, "components": { "schemas": schemas } });
        let committed = watch::Sender::new(self.db.snapshot().epoch());
        let body_limit = self.body_limit;
        let guard = self.guard.clone();
        let activity = self.activity.clone();
        let shared = Arc::new(Shared {
            db: self.db,
            entries,
            described,
            custom: self.custom,
            custom_routes,
            openapi,
            schema,
            permits: Arc::new(Semaphore::new(self.max_pending_writes)),
            max_pending_writes: self.max_pending_writes,
            committed,
            min_epoch_wait: self.min_epoch_wait,
            hubs: Arc::default(),
            closing: self.closing,
            activity: self.activity.clone(),
        });

        let mut mounted = Mounted::default();
        for (i, d) in shared.described.iter().enumerate() {
            let name = &d.name;
            assert!(
                !name.is_empty() && !name.starts_with('_') && !name.contains('/'),
                "resource name {name:?} is reserved or not a path segment",
            );
            for (j, route) in d.routes.iter().enumerate() {
                let path = format!("/{name}{}", route.path);
                let keyed = route.path.contains("{key}");
                mounted.add(&path, route.method, |r, filter| {
                    if keyed {
                        r.on(
                            filter,
                            move |State(st): St<S>,
                                  key: Result<Path<String>, PathRejection>,
                                  RawQuery(query): RawQuery,
                                  headers: HeaderMap,
                                  body: Bytes| async move {
                                match key {
                                    Ok(Path(key)) => {
                                        resource(st, i, j, Some(key), query, headers, body).await
                                    }
                                    Err(e) => Problem::bad_request(e.body_text()).into_response(),
                                }
                            },
                        )
                    } else {
                        r.on(
                            filter,
                            move |State(st): St<S>,
                                  RawQuery(query): RawQuery,
                                  headers: HeaderMap,
                                  body: Bytes| async move {
                                resource(st, i, j, None, query, headers, body).await
                            },
                        )
                    }
                });
            }
        }
        for (c, route) in shared.custom_routes.iter().enumerate() {
            mounted.add(&shared.custom[c].path, route.method, |r, filter| {
                r.on(
                    filter,
                    move |State(st): St<S>,
                          RawQuery(query): RawQuery,
                          headers: HeaderMap,
                          body: Bytes| async move {
                        custom(st, c, query, headers, body).await
                    },
                )
            });
        }

        let mut router: Router<Arc<Shared<S>>> = Router::new()
            .route(
                "/_schema",
                get(|State(st): St<S>| async move { axum::Json(st.schema.clone()) }),
            )
            .route(
                "/_health",
                get(|State(st): St<S>| async move { health(&st) }),
            )
            .route(
                "/openapi.json",
                get(|State(st): St<S>| async move { axum::Json(st.openapi.clone()) }),
            )
            .route(
                "/_live",
                get(
                    |State(st): St<S>, RawQuery(query): RawQuery, headers: HeaderMap| async move {
                        multiplex(st, query, headers).await
                    },
                ),
            )
            .route(
                "/_tx",
                post(
                    |State(st): St<S>, headers: HeaderMap, body: Bytes| async move {
                        transaction(st, headers, body).await
                    },
                ),
            );
        for (path, (method_router, _)) in mounted.0 {
            router = router.route(&path, method_router);
        }
        let router: Router = router
            .fallback(|uri: Uri| async move {
                Problem::not_found(format!("no route for {}", uri.path()))
            })
            .layer(DefaultBodyLimit::max(body_limit))
            .layer(axum::middleware::map_response(problem_status))
            .layer(axum::middleware::from_fn(
                move |req: axum::extract::Request, next: axum::middleware::Next| {
                    activity.stamp();
                    next.run(req)
                },
            ))
            .with_state(shared);
        if guard.is_open() {
            router
        } else {
            router.layer(axum::middleware::from_fn_with_state(
                guard,
                crate::guard::guard,
            ))
        }
    }
}

/// Method routers by path, refusing a second handler for a method.
struct Mounted<S: Serve>(BTreeMap<String, (MethodRouter<Arc<Shared<S>>>, Vec<Method>)>);

impl<S: Serve> Default for Mounted<S> {
    fn default() -> Self {
        Mounted(BTreeMap::new())
    }
}

impl<S: Serve> Mounted<S> {
    fn add(
        &mut self,
        path: &str,
        method: Method,
        on: impl FnOnce(MethodRouter<Arc<Shared<S>>>, MethodFilter) -> MethodRouter<Arc<Shared<S>>>,
    ) {
        let (router, methods) = match self.0.entry(path.to_owned()) {
            Slot::Vacant(v) => v.insert((MethodRouter::new(), Vec::new())),
            Slot::Occupied(o) => o.into_mut(),
        };
        assert!(
            !methods.contains(&method),
            "two routes for {method:?} {path}"
        );
        methods.push(method);
        let filter = match method {
            Method::Get => MethodFilter::GET,
            Method::Post => MethodFilter::POST,
            Method::Put => MethodFilter::PUT,
            Method::Delete => MethodFilter::DELETE,
        };
        *router = on(std::mem::take(router), filter);
    }
}

/// Liveness, and what an operator wants at a glance: the store's
/// epoch, open streams, writes in flight.
fn health<S: Serve>(st: &Shared<S>) -> axum::Json<Value> {
    axum::Json(json!({
        "status": "ok",
        "epoch": st.db.snapshot().epoch(),
        "streams": st.activity.streams.load(std::sync::atomic::Ordering::Relaxed),
        "pending_writes": st.max_pending_writes - st.permits.available_permits(),
    }))
}

/// Every entry's and custom route's description, sharing one set of
/// schema components.
fn describe<S: Serve>(
    entries: &[Entry<S>],
    custom: &[Custom<S>],
) -> (Vec<Describe>, Vec<Route>, serde_json::Map<String, Value>) {
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
    let custom = custom.iter().map(|c| (c.describe)(&mut g)).collect();
    g.subschema_for::<Problem>();
    (described, custom, g.take_definitions(true))
}

/// A generated resource route.
async fn resource<S: Serve>(
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
        if wants_events(&headers) {
            if !route.live {
                return Problem::not_acceptable("this route doesn't stream").into_response();
            }
            return stream(st, i, call, &headers).await;
        }
        return read(st, &headers, move |st, r| (st.entries[i].read)(r, &call)).await;
    }
    if route.body.is_some()
        && let Err(p) = require_json(&headers)
    {
        return p.into_response();
    }
    let Some(stage) = &st.entries[i].stage else {
        return Problem::method_not_allowed().into_response();
    };
    let apply = match stage(call) {
        Ok(apply) => apply,
        Err(problem) => return problem.into_response(),
    };
    match commit(&st, apply).await {
        Ok((epoch, written)) => {
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
        Err(problem) => problem.into_response(),
    }
}

/// A custom route.
async fn custom<S: Serve>(
    st: Arc<Shared<S>>,
    c: usize,
    query: Option<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let call = Call {
        op: CUSTOM_OP,
        key: None,
        query,
        body,
    };
    match &st.custom[c].handler {
        Handler::Read(_) => {
            read(st, &headers, move |st, r| match &st.custom[c].handler {
                Handler::Read(f) => f(r, &call),
                Handler::Write(_) => unreachable!("handler kinds never change"),
            })
            .await
        }
        Handler::Write(stage) => {
            if let Err(p) = require_json(&headers) {
                return p.into_response();
            }
            let apply = match stage(&call) {
                Ok(apply) => apply,
                Err(problem) => return problem.into_response(),
            };
            match commit(&st, apply).await {
                Ok((epoch, written)) => respond(StatusCode::OK, epoch, written.data),
                Err(problem) => problem.into_response(),
            }
        }
    }
}

/// `POST /_tx`.
async fn transaction<S: Serve>(st: Arc<Shared<S>>, headers: HeaderMap, body: Bytes) -> Response {
    if let Err(p) = require_json(&headers) {
        return p.into_response();
    }
    let ops: Vec<TxOp> = match parse_body(&body) {
        Ok(ops) => ops,
        Err(p) => return p.into_response(),
    };
    let staged = tx::stage::<S>(
        &st.described,
        |i, call| match &st.entries[i].stage {
            Some(stage) => stage(call),
            None => Err(Problem::method_not_allowed()),
        },
        ops,
    );
    let apply = match staged {
        Ok(apply) => apply,
        Err(problem) => return problem.into_response(),
    };
    match commit(&st, apply).await {
        Ok((epoch, written)) => respond(StatusCode::OK, epoch, written.data),
        Err(problem) => problem.into_response(),
    }
}

/// Answer a read from one fresh snapshot (at or after the request's
/// `bog-min-epoch`), off the async workers.
async fn read<S: Serve>(
    st: Arc<Shared<S>>,
    headers: &HeaderMap,
    answer: impl FnOnce(&Shared<S>, &S::Read<'_>) -> Result<Value, Problem> + Send + 'static,
) -> Response {
    if let Err(problem) = reach_min_epoch(&st, headers).await {
        return problem.into_response();
    }
    match snapshot(st, answer).await {
        Ok((epoch, data)) => respond(StatusCode::OK, epoch, data),
        Err(problem) => problem.into_response(),
    }
}

/// `answer` over one fresh snapshot, on the blocking pool: the data and
/// the snapshot's epoch.
async fn snapshot<S: Serve>(
    st: Arc<Shared<S>>,
    answer: impl FnOnce(&Shared<S>, &S::Read<'_>) -> Result<Value, Problem> + Send + 'static,
) -> Result<(u64, Value), Problem> {
    tokio::task::spawn_blocking(move || {
        let snap = st.db.snapshot();
        let facades = st.db.at(&snap);
        answer(&st, &facades).map(|data| (snap.epoch(), data))
    })
    .await
    .map_err(|e| Problem::internal(format!("read failed: {e}")))?
}

/// Whether the client asked for Server-Sent Events.
fn wants_events(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .any(|m| {
            m.split(';')
                .next()
                .is_some_and(|m| m.trim().eq_ignore_ascii_case("text/event-stream"))
        })
}

/// A live `GET`: Server-Sent Events. A `snapshot` event with exactly what
/// the plain `GET` answers, then a `change` event per commit that touches
/// the resource; each event's `id` is its epoch. A reconnect carrying
/// `Last-Event-ID` resumes from the hub's ring when it can, and gets a
/// fresh snapshot when it can't; so does a client that falls behind.
///
/// With `snapshot=all` in the query, a listing's snapshot is the whole
/// listing: the first page as `snapshot`, then each further page as
/// `snapshot.more`, all read from one snapshot (see [`snapshot_items`]).
async fn stream<S: Serve>(
    st: Arc<Shared<S>>,
    i: usize,
    call: Call,
    headers: &HeaderMap,
) -> Response {
    if let Err(problem) = reach_min_epoch(&st, headers).await {
        return problem.into_response();
    }
    let resume = last_event_id(headers).and_then(|id| id.parse::<u64>().ok());
    let items = match feed(st, i, call, resume).await {
        Ok(items) => items,
        Err(problem) => return problem.into_response(),
    };
    let events = items.map(|item| {
        // Mid-snapshot, there's nothing to resume from: an id that
        // isn't an epoch makes a reconnect start over.
        let id = if item.partial {
            PARTIAL_ID.to_owned()
        } else {
            item.epoch.to_string()
        };
        Ok::<_, std::convert::Infallible>(Event::default().event(item.kind).id(id).data(item.data))
    });
    Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response()
}

/// The most resources one `/_live` stream may carry.
pub const MAX_LIVE: usize = 32;

/// `GET /_live?a=<a's query>&b=<b's query>`: several resources' streams on
/// one connection. Browsers allow six HTTP/1.1 connections per host, and
/// each stream holds one for good: a page following three resources in
/// two tabs would leave nothing for its other requests.
///
/// Each query parameter names a resource and carries, URL-encoded, the
/// query its own stream would take (`messages=order%3Ddesc%26limit%3D50`).
/// Events are named `<resource>.snapshot` and `<resource>.change`, with
/// the same data as on the resource's own stream. The `id` is every
/// resource's epoch, dot-separated in request order, so `Last-Event-ID`
/// resumes each one exactly.
async fn multiplex<S: Serve>(
    st: Arc<Shared<S>>,
    query: Option<String>,
    headers: HeaderMap,
) -> Response {
    let wanted: Vec<(String, String)> = match parse_query(query.as_deref()) {
        Ok(w) => w,
        Err(problem) => return problem.into_response(),
    };
    if wanted.is_empty() || wanted.len() > MAX_LIVE {
        return Problem::bad_request(format!(
            "name 1 to {MAX_LIVE} resources to follow, as ?<resource>=<its query>"
        ))
        .into_response();
    }
    let mut picked = Vec::with_capacity(wanted.len());
    for (name, query) in &wanted {
        let Some((i, d)) = st
            .described
            .iter()
            .enumerate()
            .find(|(_, d)| &d.name == name)
        else {
            return Problem::bad_request(format!("no resource {name:?}")).into_response();
        };
        let Some(route) = d.routes.iter().find(|r| r.live && r.path.is_empty()) else {
            return Problem::not_acceptable(format!("{name} doesn't stream")).into_response();
        };
        if picked.iter().any(|(j, _, _): &(usize, _, _)| *j == i) {
            return Problem::bad_request(format!("{name} named twice")).into_response();
        }
        let call = Call {
            op: route.op,
            key: None,
            query: (!query.is_empty()).then(|| query.clone()),
            body: Bytes::new(),
        };
        picked.push((i, name.clone(), call));
    }
    if let Err(problem) = reach_min_epoch(&st, &headers).await {
        return problem.into_response();
    }
    // `e1.e2.…`, one per resource; anything else resumes nothing.
    let resume: Vec<Option<u64>> = last_event_id(&headers)
        .map(|id| id.split('.').map(|e| e.parse().ok()).collect())
        .filter(|ids: &Vec<Option<u64>>| ids.len() == picked.len())
        .unwrap_or_else(|| vec![None; picked.len()]);

    // Until a resource sends something, its slot is what the client
    // already holds: the epoch it resumed from, or nothing.
    let mut epochs: Vec<String> = resume
        .iter()
        .map(|r| r.map_or_else(|| PARTIAL_ID.to_owned(), |e| e.to_string()))
        .collect();
    let mut feeds = Vec::with_capacity(picked.len());
    for (k, ((i, name, call), resume)) in picked.into_iter().zip(resume).enumerate() {
        match feed(st.clone(), i, call, resume).await {
            Ok(items) => feeds.push(Box::pin(items.map(move |item| (k, name.clone(), item)))),
            Err(problem) => return problem.into_response(),
        }
    }
    let events = futures_util::stream::select_all(feeds).map(move |(k, name, item)| {
        epochs[k] = if item.partial {
            PARTIAL_ID.to_owned()
        } else {
            item.epoch.to_string()
        };
        let id = epochs.join(".");
        Ok::<_, std::convert::Infallible>(
            Event::default()
                .event(format!("{name}.{}", item.kind))
                .id(id)
                .data(item.data),
        )
    });
    Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response()
}

fn last_event_id(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
}

/// One event of a resource's feed, before it's framed.
struct Item {
    kind: &'static str,
    epoch: u64,
    data: String,
    /// A snapshot page with more to follow: not a point to resume from.
    partial: bool,
}

/// The `id` (or `/_live` id component) of a snapshot page with more to
/// follow: not an epoch, so a reconnect gets a fresh snapshot.
const PARTIAL_ID: &str = "-";

/// The most rows a `snapshot=all` snapshot sends. Past it, the last page
/// keeps its `next` cursor: the client can tell it was cut short.
pub const MAX_SNAPSHOT_ROWS: usize = 100_000;

/// Resource `i`'s feed for one client: a snapshot (or a replay from
/// `resume`), then every later change, resyncing with a snapshot if the
/// client falls behind; ends when the server closes or the database does.
async fn feed<S: Serve>(
    st: Arc<Shared<S>>,
    i: usize,
    mut call: Call,
    resume: Option<u64>,
) -> Result<impl futures_util::Stream<Item = Item> + Send + 'static, Problem> {
    let all = take_snapshot_all(&mut call)?;
    // An id past the store's epoch came from another store (or a restored
    // one): there's nothing to resume, so start over with a snapshot.
    let resume = resume.filter(|&e| e <= st.db.snapshot().epoch());
    let joined = st
        .hubs
        .join(i, resume, || {
            let snap = st.db.snapshot();
            let live = (st.entries[i].live)(&st.db.at(&snap))?;
            // bog subscribes from the snapshot's epoch, but a commit
            // published between the two is not delivered: only changes
            // after a snapshot taken now are certain to arrive.
            Some((st.db.snapshot().epoch(), live))
        })
        .ok_or_else(|| Problem::not_acceptable("this resource doesn't stream"))?;

    let mut queue = VecDeque::new();
    let floor = match (joined.replay, resume) {
        (Some(replay), Some(from)) => {
            let floor = replay.last().map_or(from, |c| c.epoch);
            queue.extend(replay.iter().map(|c| change_item(c)));
            floor
        }
        _ => {
            // Subscribed already: take the snapshot now, and the tail
            // above its epoch is exactly what it lacks. A malformed query
            // fails here, as a plain problem rather than a stream.
            let items = snapshot_items(&st, i, &call, all).await?;
            let floor = items[0].epoch;
            queue.extend(items);
            floor
        }
    };

    let closing = st.closing.subscribe();
    let open = Open::new(&st.activity);
    let feed = Feed {
        st,
        i,
        call,
        all,
        rx: joined.rx,
        floor,
        queue,
        closing,
        _open: open,
    };
    Ok(futures_util::stream::unfold(feed, |mut f| async move {
        loop {
            if let Some(item) = f.queue.pop_front() {
                return Some((item, f));
            }
            let next = tokio::select! {
                next = f.rx.recv() => next,
                _ = f.closing.wait_for(|closing| *closing) => return None,
            };
            match next {
                Ok(change) if change.epoch > f.floor => {
                    f.floor = change.epoch;
                    f.queue.push_back(change_item(&change));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::debug!(target: "bog_http", resource = %f.st.described[f.i].name, missed, "stream fell behind: resyncing");
                    match snapshot_items(&f.st, f.i, &f.call, f.all).await {
                        Ok(items) => {
                            f.floor = items[0].epoch;
                            f.queue.extend(items);
                        }
                        Err(_) => return None,
                    }
                }
                // The database closed: end the stream.
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }))
}

struct Feed<S: Serve> {
    st: Arc<Shared<S>>,
    i: usize,
    call: Call,
    /// Snapshots carry the whole listing (`snapshot=all`).
    all: bool,
    rx: broadcast::Receiver<Arc<Change>>,
    /// The newest epoch this client has: changes at or below it are
    /// already in what it was sent.
    floor: u64,
    queue: VecDeque<Item>,
    closing: watch::Receiver<bool>,
    _open: Open,
}

/// Resource `i`'s snapshot events: what its `GET` answers, or with
/// `all`, every page of the listing, each read from the same snapshot so
/// together they are one consistent state at one epoch.
async fn snapshot_items<S: Serve>(
    st: &Arc<Shared<S>>,
    i: usize,
    call: &Call,
    all: bool,
) -> Result<Vec<Item>, Problem> {
    let mut call = call.clone();
    let st = st.clone();
    tokio::task::spawn_blocking(move || {
        let snap = st.db.snapshot();
        let facades = st.db.at(&snap);
        let epoch = snap.epoch();
        let mut items = Vec::new();
        let mut rows = 0;
        loop {
            let data = (st.entries[i].read)(&facades, &call)?;
            rows += data["items"].as_array().map_or(0, Vec::len);
            let next = data["next"].as_str().map(str::to_owned);
            items.push(Item {
                kind: if items.is_empty() {
                    "snapshot"
                } else {
                    "snapshot.more"
                },
                epoch,
                data: json!({ "epoch": epoch, "data": data }).to_string(),
                partial: true,
            });
            match next {
                Some(after) if all && rows < MAX_SNAPSHOT_ROWS => {
                    call.query = Some(with_after(call.query.as_deref(), &after)?);
                }
                _ => break,
            }
        }
        if let Some(last) = items.last_mut() {
            last.partial = false;
        }
        Ok(items)
    })
    .await
    .map_err(|e| Problem::internal(format!("read failed: {e}")))?
}

/// Take a stream's own `snapshot=all` out of the query its resource
/// reads.
fn take_snapshot_all(call: &mut Call) -> Result<bool, Problem> {
    let mut pairs: Vec<(String, String)> = parse_query(call.query.as_deref())?;
    let mut all = false;
    let mut err = None;
    pairs.retain(|(k, v)| {
        if k != "snapshot" {
            return true;
        }
        match v.as_str() {
            "all" => all = true,
            _ => {
                err = Some(Problem::bad_request(format!(
                    "snapshot={v}: the only option is `all`"
                )))
            }
        }
        false
    });
    if let Some(problem) = err {
        return Err(problem);
    }
    call.query = encode_query(&pairs)?;
    Ok(all)
}

/// `query` with its `after` cursor replaced by `after`.
fn with_after(query: Option<&str>, after: &str) -> Result<String, Problem> {
    let mut pairs: Vec<(String, String)> = parse_query(query)?;
    pairs.retain(|(k, _)| k != "after");
    pairs.push(("after".to_owned(), after.to_owned()));
    Ok(encode_query(&pairs)?.unwrap_or_default())
}

fn encode_query(pairs: &[(String, String)]) -> Result<Option<String>, Problem> {
    if pairs.is_empty() {
        return Ok(None);
    }
    serde_urlencoded::to_string(pairs)
        .map(Some)
        .map_err(|e| Problem::internal(format!("query: {e}")))
}

fn change_item(change: &Change) -> Item {
    Item {
        kind: "change",
        epoch: change.epoch,
        data: change.data.clone(),
        partial: false,
    }
}

/// Wait, bounded, until the store's frontier reaches the request's
/// `bog-min-epoch`.
///
/// Wakes on every commit through this server, and polls besides, so
/// writes made to a shared [`Db`] directly are seen too.
async fn reach_min_epoch<S: Serve>(st: &Shared<S>, headers: &HeaderMap) -> Result<(), Problem> {
    let Some(raw) = headers.get(MIN_EPOCH_HEADER) else {
        return Ok(());
    };
    let wanted: u64 = raw
        .to_str()
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| Problem::bad_request(format!("{MIN_EPOCH_HEADER} must be an integer")))?;
    let deadline = tokio::time::Instant::now() + st.min_epoch_wait;
    let mut commits = st.committed.subscribe();
    loop {
        let current = st.db.snapshot().epoch();
        if current >= wanted {
            return Ok(());
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(Problem::epoch_not_reached(wanted, current));
        }
        let nap = (deadline - now).min(Duration::from_millis(10));
        tokio::select! {
            _ = commits.changed() => {}
            _ = tokio::time::sleep(nap) => {}
        }
    }
}

/// Run one transaction off the async workers, under admission control:
/// bog serializes writers, so the permits bound how many wait.
async fn commit<S: Serve>(st: &Arc<Shared<S>>, apply: TxFn<S>) -> Result<(u64, Written), Problem> {
    let Ok(permit) = st.permits.clone().try_acquire_owned() else {
        tracing::warn!(target: "bog_http", "write refused: too many pending");
        return Err(Problem::overloaded());
    };
    let db = st.db.clone();
    let res = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        let (written, epoch) = db.write_epoch(apply)?;
        // A write that changed nothing committed nothing: every
        // snapshot already reflects it.
        Ok::<_, bog::Error>((epoch.unwrap_or_else(|| db.snapshot().epoch()), written))
    })
    .await;
    match res {
        Ok(Ok((epoch, written))) => {
            tracing::debug!(target: "bog_http", epoch, "committed");
            st.committed.send_if_modified(|newest| {
                let advanced = epoch > *newest;
                if advanced {
                    *newest = epoch;
                }
                advanced
            });
            Ok((epoch, written))
        }
        Ok(Err(e)) => {
            let problem = Problem::from(e);
            tracing::debug!(target: "bog_http", status = problem.status, at = problem.at.as_deref(), "write refused");
            Err(problem)
        }
        Err(e) => Err(Problem::internal(format!("write failed: {e}"))),
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

/// axum answers a wrong method (405) or an oversized body (413) with a
/// plain-text body; give them the problem body every other error has,
/// keeping their headers (`Allow`).
async fn problem_status(res: Response) -> Response {
    let problem = match res.status() {
        StatusCode::METHOD_NOT_ALLOWED => Problem::method_not_allowed(),
        StatusCode::PAYLOAD_TOO_LARGE => Problem::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload-too-large",
            "Payload too large",
        ),
        _ => return res,
    };
    let is_problem = res
        .headers()
        .get(header::CONTENT_TYPE)
        .is_some_and(|v| v.as_bytes() == crate::PROBLEM_JSON.as_bytes());
    if is_problem {
        return res;
    }
    let allow = res.headers().get(header::ALLOW).cloned();
    let mut out = problem.into_response();
    if let Some(allow) = allow {
        out.headers_mut().insert(header::ALLOW, allow);
    }
    out
}
