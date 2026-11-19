#![forbid(unsafe_code)]
// Erased write closures are spelled out once, in `resource`.
#![allow(clippy::type_complexity)]

//! bog-http: serve any [bog] schema over HTTP.
//!
//! Derive [`Serve`] next to `Schema`, and every source and view becomes a
//! typed resource: tables and events are writable, views are read-only,
//! every read answers from one consistent snapshot, and every response
//! carries the epoch it reflects.
//!
//! ```ignore
//! #[derive(Schema, Serve)]
//! struct Shop {
//!     customers: Table<Id<Customer>, Customer>,
//!     orders: Events<Order>,
//!     #[view] revenue: MapView<Id<Customer>, i64>,
//! }
//!
//! let db = Shop::open("./shop").build()?;
//! let app: axum::Router = bog_http::Server::new(db).into_router();
//! ```
//!
//! # Routes
//! Each kind mounts the routes its operations allow, under `/{field}`:
//!
//! | kind | reads | writes |
//! |---|---|---|
//! | `Table<K, V>` | `GET /n` page of `{key, value}`, `GET /n/{key}` | `PUT /n/{key}`, `DELETE /n/{key}`; `POST /n` mints when `K = Id<V>` |
//! | `Events<V>` | as a table | `POST /n` appends, minting the id |
//! | `Set<T>` | `GET /n` page of members, `GET /n/{member}` | `PUT`, `DELETE /n/{member}` |
//! | `Cell<V>` | `GET /n` (`null` while unset) | `PUT /n`, `DELETE /n` |
//! | `Multiset<T>` | `GET /n` page of `{value, count}`, `GET /n/{value}` count | `POST /n {value, times}`, `DELETE /n/{value}?times=` |
//! | `MapView<K, V>` | as a table | |
//! | `OrderedView<K, V>` | as a table, plus `?by=value&min=&max=` | |
//! | `GroupView<K, V>` | `GET /n` page of `{key, value, count}`, `GET /n/{key}` page of a key's values | |
//! | `ValueView<V>` | `GET /n` | |
//! | `SearchView<D>` | `GET /n?q=&k=` hits, `GET /n/complete?prefix=&k=` | |
//!
//! Listings take `?limit=` (≤ 1000), `?order=asc|desc`, and `?after=`,
//! the opaque `next` cursor of the previous page (keyset pagination).
//! Meta routes: `GET /openapi.json` (OpenAPI 3.1), `GET /_schema` (the
//! same descriptions, compact), `GET /_health`.
//!
//! Successes are `{"epoch", "data"}` with a `bog-epoch` header; errors
//! are RFC 9457 `application/problem+json` (see [`Problem`]). Keys in
//! paths parse as JSON, falling back to a bare string (see
//! [`wire::parse_key`]).
//!
//! # Writes
//! Every write is one bog transaction, decoded on the request's thread
//! and applied on the blocking pool; a malformed request never opens a
//! transaction, and a refusal rolls everything back (`422`, naming the
//! refusing source or view in `at`).
//!
//! - `POST /_tx` applies many writes, across any sources, atomically:
//!   `[{"resource": "prefs", "op": "upsert", "key": "theme", "body": 1}, …]`.
//!   Its OpenAPI body is a `oneOf` of every write route.
//! - [`Server::get`] and [`Server::post`] add typed application routes:
//!   a read over one snapshot, or a transaction. Both are documented in
//!   OpenAPI from their types.
//! - Writes return the epoch they committed as. Send it back as
//!   `bog-min-epoch` on a read to read your own writes; the read waits
//!   (bounded by [`Server::min_epoch_wait`]) until that epoch is visible.
//! - [`Server::max_pending_writes`] bounds the writes waiting: past it,
//!   writes get `503 overloaded` with `Retry-After` instead of piling up.
//!
//! # Live
//! Every listing and singleton `GET` also streams: ask with
//! `Accept: text/event-stream` (as `EventSource` does) and the first event,
//! `snapshot`, is exactly what the plain `GET` answers; then one `change`
//! event per commit that touches the resource, `{epoch, changes: […]}`.
//! A change is `{key?, op, value?, old?}`: `insert` carries the new
//! `value`, `delete` the `old` one, `replace` both. Counted kinds
//! (multisets, grouped views) send `{key?, value, diff}`, the change in
//! count. Each event's `id` is its epoch, so a reconnect resumes
//! from `Last-Event-ID` (replayed, or a fresh snapshot when too old). One
//! hub per resource drains bog's subscription; a client that falls
//! behind gets a fresh snapshot instead of an unbounded backlog.
//!
//! A page following several resources should use `GET /_live`, which
//! carries any of them on one connection
//! (`?messages=order%3Ddesc&total=`, events named `messages.change`):
//! browsers allow six HTTP/1.1 connections per host, and each open
//! stream holds one for good.
//!
//! # Serving
//! [`Server::serve`] (TCP) and [`Server::serve_unix`] bind, serve until
//! ctrl-c, SIGTERM, or [`Server::idle_exit`], end live streams, drain
//! requests, and checkpoint. For a local app on the same machine:
//!
//! - [`Server::discovery`] keeps a JSON file saying where the server is
//!   (`url` or `socket`, `pid`), so an app can connect to a running one
//!   or start its own; a second server on the same store fails cleanly
//!   (bog locks the store's directory).
//! - [`Server::idle_exit`] lets an on-demand server leave once unused.
//! - A loopback bind answers only to `localhost` names (DNS rebinding);
//!   [`Server::bearer_token`] keeps out other local processes and pages,
//!   and Unix sockets are created owner-only.
//! - [`cors`] admits a browser app on other origins, listed explicitly.
//!
//! `GET /_health` reports the epoch, open streams, and pending writes.
//! Events go to [`tracing`](https://docs.rs/tracing) under the
//! `bog_http` target. For anything else, [`Server::into_router`] is a
//! plain axum [`Router`](axum::Router).
//!
//! # Serving your own kinds
//! A source or view type from outside bog serves by implementing
//! [`Resource`] (and [`SourceResource`] if writable): describe its
//! [`Route`]s, answer reads from its facade, stage writes for the
//! transaction. The derive needs nothing more.

mod custom;
mod guard;
mod live;
mod openapi;
mod problem;
mod resource;
mod resources;
#[cfg(feature = "serve")]
mod serve;
mod server;
mod tx;
pub mod wire;

pub use custom::NoParams;
pub use guard::ACCESS_TOKEN_PARAM;

pub use problem::{PROBLEM_JSON, Problem};
pub use resource::{
    Apply, Call, Describe, Entry, Live, MINT_OP, Method, Mint, Resource, Route, SourceResource,
    Stage, Tie, TxFn, Wire, Written, inline_schema, schema_in,
};
pub use resources::{
    By, CompleteParams, Completion, Hit, MultisetAdd, MultisetRemove, OrderedParams, SearchParams,
};
pub use server::{Closer, MAX_LIVE, MIN_EPOCH_HEADER, Server};

/// A CORS layer for a browser app on another origin (a dev server, a
/// desktop shell): these origins only, with the methods and headers the
/// API uses, exposing `bog-epoch`, `Location`, and `Retry-After`.
///
/// There is deliberately no "any origin": a server on `localhost` that
/// any web page may call is one any site you visit can read and write.
///
/// # Panics
/// If an origin isn't a valid header value.
#[cfg(feature = "cors")]
pub fn cors<'a>(origins: impl IntoIterator<Item = &'a str>) -> tower_http::cors::CorsLayer {
    use axum::http::{HeaderName, HeaderValue, Method, header};
    let origins: Vec<HeaderValue> = origins
        .into_iter()
        .map(|o| o.parse().expect("an origin is a header value"))
        .collect();
    tower_http::cors::CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE])
        .allow_headers([
            header::CONTENT_TYPE,
            header::ACCEPT,
            HeaderName::from_static(MIN_EPOCH_HEADER),
            HeaderName::from_static("last-event-id"),
        ])
        .expose_headers([
            HeaderName::from_static(wire::EPOCH_HEADER),
            header::LOCATION,
            header::RETRY_AFTER,
        ])
}
pub use tx::MAX_TX_OPS;

#[cfg(feature = "derive")]
pub use bog_http_derive::Serve;

/// A schema servable over HTTP: the list of its served fields.
///
/// Derive it with `#[derive(Serve)]` beside `#[derive(Schema)]`; skip a
/// field with `#[serve(skip)]`. A hand-written impl builds [`Entry`]s
/// directly.
pub trait Serve: bog::schema::Declare {
    fn entries() -> Vec<Entry<Self>>;
}

/// Support for the derive. Not public API.
#[doc(hidden)]
pub mod __private {
    use std::marker::PhantomData;

    use bog::schema::{Data, Id, SourceDecl, Table};

    use crate::{Mint, Wire};

    pub use bog;

    /// Autoref specialization: `(&&Probe::<D>::NEW).mint()` finds
    /// [`MintById`] when `D` is `Table<Id<V>, V>` and [`MintNone`]
    /// otherwise, which the derive can't tell apart from type tokens.
    pub struct Probe<D>(PhantomData<fn() -> D>);

    impl<D> Probe<D> {
        pub const NEW: Self = Probe(PhantomData);
    }

    pub trait MintById<D: SourceDecl> {
        fn mint(&self) -> Option<Mint<D>>;
    }

    impl<V: Data + Wire> MintById<Table<Id<V>, V>> for &Probe<Table<Id<V>, V>> {
        fn mint(&self) -> Option<Mint<Table<Id<V>, V>>> {
            Some(Mint {
                stage: crate::resources::mint_table::<V>,
                route: crate::resources::mint_table_route::<V>,
            })
        }
    }

    pub trait MintNone<D: SourceDecl> {
        fn mint(&self) -> Option<Mint<D>>;
    }

    impl<D: SourceDecl> MintNone<D> for Probe<D> {
        fn mint(&self) -> Option<Mint<D>> {
            None
        }
    }
}
