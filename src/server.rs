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
use fold::{Db, Reader, Tx, Writer};
use schemars::generate::SchemaSettings;
use schemars::{JsonSchema, SchemaGenerator};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tokio::sync::{Semaphore, broadcast, watch};

use crate::custom::{CUSTOM_OP, Custom, Handler, TxFn, check_path};
use crate::live::{Change, Hubs};
use crate::openapi::{self, Info};
use crate::resource::{Call, Describe, Entry, Method, Route, Written};
use crate::tx::{self, TxOp};
use crate::wire::{parse_body, require_json, respond, to_json};
use crate::{IntoProblem, Problem, Serve};

/// The request header naming the least epoch a read must reflect.
pub const MIN_EPOCH_HEADER: &str = "fold-min-epoch";

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
    reader: Reader<S>,
    writer: Writer<S>,
    title: String,
    version: String,
    description: Option<String>,
    custom: Vec<Custom<S>>,
    min_epoch_wait: Duration,
    max_pending_writes: usize,
    body_limit: usize,
    closing: watch::Sender<bool>,
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
    reader: Reader<S>,
    writer: Writer<S>,
    entries: Vec<Entry<S>>,
    described: Vec<Describe>,
    custom: Vec<Custom<S>>,
    custom_routes: Vec<Route>,
    openapi: Value,
    schema: Value,
    /// Admission control: one permit per write in flight.
    permits: Arc<Semaphore>,
    /// The newest epoch a write through this server committed.
    committed: watch::Sender<u64>,
    min_epoch_wait: Duration,
    hubs: Arc<Hubs>,
    closing: watch::Sender<bool>,
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
            custom: Vec::new(),
            min_epoch_wait: Duration::from_secs(5),
            max_pending_writes: 1024,
            body_limit: 2 << 20,
            closing: watch::Sender::new(false),
        }
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

    /// The OpenAPI `info.version` (of your API, not of fold-http).
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// How long a read with `fold-min-epoch` waits for that epoch before
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
        F: Fn(&S::Read, Q) -> Result<R, Problem> + Send + Sync + 'static,
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
            handler: Handler::Read(Arc::new(move |r, call| to_json(&f(r, call.query()?)?))),
        });
        self
    }

    /// A custom write: `POST path` with a JSON body `B`, run by `f` as
    /// one transaction. `f`'s `Err` rolls everything back and answers
    /// through [`IntoProblem`]; so do the schema's constraints.
    ///
    /// # Panics
    /// As [`get`](Server::get).
    pub fn post<B, R, F>(mut self, path: &str, summary: &str, f: F) -> Self
    where
        B: DeserializeOwned + JsonSchema + Send + 'static,
        R: Serialize + JsonSchema + 'static,
        F: Fn(&mut Tx<'_, S>, B) -> Result<R, S::Error> + Send + Sync + 'static,
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
                let apply: TxFn<S> = Box::new(move |tx| Ok(Written::ok(&f(tx, body)?)));
                Ok(apply)
            })),
        });
        self
    }

    /// Serve on `addr` until ctrl-c or SIGTERM, then shut down
    /// gracefully: end live streams, finish in-flight requests, and
    /// checkpoint, so the next open replays no WAL.
    ///
    /// `layer` wraps the router first: CORS, auth, tracing, extra
    /// routes. Bind `127.0.0.1` unless the API is meant for the network;
    /// fold-http has no authentication of its own.
    #[cfg(feature = "serve")]
    pub async fn serve(
        self,
        addr: impl tokio::net::ToSocketAddrs,
        layer: impl FnOnce(Router) -> Router,
    ) -> std::io::Result<()> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let writer = self.writer.clone();
        let closer = self.closer();
        let app = layer(self.into_router());
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                stop_signal().await;
                closer.close();
            })
            .await?;
        tokio::task::spawn_blocking(move || writer.checkpoint())
            .await
            .map_err(std::io::Error::other)?
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
        let committed = watch::Sender::new(self.reader.snapshot().frontier().outer);
        let body_limit = self.body_limit;
        let shared = Arc::new(Shared {
            reader: self.reader,
            writer: self.writer,
            entries,
            described,
            custom: self.custom,
            custom_routes,
            openapi,
            schema,
            permits: Arc::new(Semaphore::new(self.max_pending_writes)),
            committed,
            min_epoch_wait: self.min_epoch_wait,
            hubs: Arc::default(),
            closing: self.closing,
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
                get(|| async { axum::Json(json!({ "status": "ok" })) }),
            )
            .route(
                "/openapi.json",
                get(|State(st): St<S>| async move { axum::Json(st.openapi.clone()) }),
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
        router
            .fallback(|uri: Uri| async move {
                Problem::not_found(format!("no route for {}", uri.path()))
            })
            .layer(DefaultBodyLimit::max(body_limit))
            .layer(axum::middleware::map_response(problem_status))
            .with_state(shared)
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
    match commit(&st, Box::new(move |tx| apply(&mut **tx))).await {
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
/// `fold-min-epoch`), off the async workers.
async fn read<S: Serve>(
    st: Arc<Shared<S>>,
    headers: &HeaderMap,
    answer: impl FnOnce(&Shared<S>, &S::Read) -> Result<Value, Problem> + Send + 'static,
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
    answer: impl FnOnce(&Shared<S>, &S::Read) -> Result<Value, Problem> + Send + 'static,
) -> Result<(u64, Value), Problem> {
    tokio::task::spawn_blocking(move || {
        let snap = st.reader.snapshot();
        let facades = st.reader.at(&snap);
        answer(&st, &facades).map(|data| (snap.frontier().outer, data))
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
async fn stream<S: Serve>(
    st: Arc<Shared<S>>,
    i: usize,
    call: Call,
    headers: &HeaderMap,
) -> Response {
    if let Err(problem) = reach_min_epoch(&st, headers).await {
        return problem.into_response();
    }
    let resume = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok());
    let joined = st.hubs.join(i, resume, || {
        let snap = st.reader.snapshot();
        let facades = st.reader.at(&snap);
        (st.entries[i].live)(&facades).map(|live| (snap.frontier().outer, live))
    });
    let Some(joined) = joined else {
        return Problem::not_acceptable("this resource doesn't stream").into_response();
    };

    let mut queue = VecDeque::new();
    let floor = match (joined.replay, resume) {
        (Some(replay), Some(from)) => {
            let floor = replay.last().map_or(from, |c| c.epoch);
            queue.extend(replay.iter().map(|c| change_event(c)));
            floor
        }
        _ => {
            // Subscribed already: take the snapshot now, and the tail
            // above its epoch is exactly what it lacks. A malformed query
            // fails here, as a plain problem rather than a stream.
            match snapshot_event(&st, i, &call).await {
                Ok((epoch, event)) => {
                    queue.push_back(event);
                    epoch
                }
                Err(problem) => return problem.into_response(),
            }
        }
    };

    let closing = st.closing.subscribe();
    let feed = Feed {
        st,
        i,
        call,
        rx: joined.rx,
        floor,
        queue,
        closing,
    };
    let events = futures_util::stream::unfold(feed, |mut f| async move {
        loop {
            if let Some(event) = f.queue.pop_front() {
                return Some((Ok::<_, std::convert::Infallible>(event), f));
            }
            let next = tokio::select! {
                next = f.rx.recv() => next,
                _ = f.closing.wait_for(|closing| *closing) => return None,
            };
            match next {
                Ok(change) if change.epoch > f.floor => {
                    f.floor = change.epoch;
                    f.queue.push_back(change_event(&change));
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    match snapshot_event(&f.st, f.i, &f.call).await {
                        Ok((epoch, event)) => {
                            f.floor = epoch;
                            f.queue.push_back(event);
                        }
                        Err(_) => return None,
                    }
                }
                // The database closed: end the stream.
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });
    Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response()
}

struct Feed<S: Serve> {
    st: Arc<Shared<S>>,
    i: usize,
    call: Call,
    rx: broadcast::Receiver<Arc<Change>>,
    /// The newest epoch this client has: changes at or below it are
    /// already in what it was sent.
    floor: u64,
    queue: VecDeque<Event>,
    closing: watch::Receiver<bool>,
}

async fn snapshot_event<S: Serve>(
    st: &Arc<Shared<S>>,
    i: usize,
    call: &Call,
) -> Result<(u64, Event), Problem> {
    let call = call.clone();
    let (epoch, data) = snapshot(st.clone(), move |st, r| (st.entries[i].read)(r, &call)).await?;
    let body = json!({ "epoch": epoch, "data": data }).to_string();
    Ok((
        epoch,
        Event::default()
            .event("snapshot")
            .id(epoch.to_string())
            .data(body),
    ))
}

fn change_event(change: &Change) -> Event {
    Event::default()
        .event("change")
        .id(change.epoch.to_string())
        .data(&change.data)
}

/// Wait, bounded, until the store's frontier reaches the request's
/// `fold-min-epoch`.
///
/// Wakes on every commit through this server, and polls besides, so
/// writes through a shared [`Writer`] are seen too.
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
        let current = st.reader.snapshot().frontier().outer;
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

/// Run one transaction on the writer thread, under admission control.
async fn commit<S: Serve>(st: &Arc<Shared<S>>, apply: TxFn<S>) -> Result<(u64, Written), Problem> {
    let Ok(permit) = st.permits.clone().try_acquire_owned() else {
        return Err(Problem::overloaded());
    };
    let writer = st.writer.clone();
    let res = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        writer.write(move |tx| {
            // This transaction commits as the snapshot's outer epoch;
            // the first frontier that includes it is the next one.
            let epoch = tx.snap().frontier().outer + 1;
            apply(tx).map(|written| (epoch, written))
        })
    })
    .await;
    match res {
        Ok(Ok((epoch, written))) => {
            st.committed.send_if_modified(|newest| {
                let advanced = epoch > *newest;
                if advanced {
                    *newest = epoch;
                }
                advanced
            });
            Ok((epoch, written))
        }
        Ok(Err(e)) => Err(e.into_problem()),
        // `Writer` panics only when the database thread is gone.
        Err(e) if e.is_panic() => Err(Problem::unavailable("the database writer has stopped")),
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

/// Ctrl-c, or SIGTERM where there is one.
#[cfg(feature = "serve")]
async fn stop_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
}
