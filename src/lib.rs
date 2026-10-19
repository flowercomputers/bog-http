#![forbid(unsafe_code)]
// Erased write closures are spelled out once, in `resource`.
#![allow(clippy::type_complexity)]

//! fold-http: serve any [fold] schema over HTTP.
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
//!     #[view] revenue: MapView<Id<Customer>, isize>,
//! }
//!
//! let db = Shop::open("./shop").build()?;
//! let app: axum::Router = fold_http::Server::new(db).into_router();
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
//! | `Bag<T>` | `GET /n` page of `{value, count}`, `GET /n/{value}` count | `POST /n {value, times}`, `DELETE /n/{value}?times=` |
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
//! Successes are `{"epoch", "data"}` with a `fold-epoch` header; errors
//! are RFC 9457 `application/problem+json` (see [`Problem`]). Keys in
//! paths parse as JSON, falling back to a bare string (see
//! [`wire::parse_key`]).
//!
//! # Writes
//! Every write is one fold transaction, decoded on the request's thread
//! and applied on fold's writer thread; a malformed request never reaches
//! the writer, and a refusal rolls everything back (`422`, naming the
//! refusing source or view in `at`).
//!
//! - `POST /_tx` applies many writes, across any sources, atomically:
//!   `[{"resource": "prefs", "op": "upsert", "key": "theme", "body": 1}, …]`.
//!   Its OpenAPI body is a `oneOf` of every write route.
//! - [`Server::get`] and [`Server::post`] add typed application routes:
//!   a read over one snapshot, or a transaction. Both are documented in
//!   OpenAPI from their types.
//! - Writes return the epoch they committed into. Send it back as
//!   `fold-min-epoch` on a read to read your own writes; the read waits
//!   (bounded by [`Server::min_epoch_wait`]) until that epoch is visible.
//! - [`Server::max_pending_writes`] bounds the writer's queue: past it,
//!   writes get `503 overloaded` with `Retry-After` instead of piling up.
//!
//! # Serving your own kinds
//! A source or view type from outside fold serves by implementing
//! [`Resource`] (and [`SourceResource`] if writable): describe its
//! [`Route`]s, answer reads from its facade, stage writes for the writer
//! thread. The derive needs nothing more.

mod custom;
mod openapi;
mod problem;
mod resource;
mod resources;
mod server;
mod tx;
pub mod wire;

pub use custom::NoParams;

pub use problem::{IntoProblem, PROBLEM_JSON, Problem};
pub use resource::{
    Apply, Call, Describe, Entry, MINT_OP, Method, Mint, Resource, Route, SourceResource, Stage,
    Wire, Written, inline_schema, schema_in,
};
pub use resources::{
    BagAdd, BagRemove, By, CompleteParams, Completion, Hit, OrderedParams, SearchParams,
};
pub use server::{MIN_EPOCH_HEADER, Server};
pub use tx::MAX_TX_OPS;

#[cfg(feature = "derive")]
pub use fold_http_derive::Serve;

/// A schema servable over HTTP: the list of its served fields.
///
/// Derive it with `#[derive(Serve)]` beside `#[derive(Schema)]`; skip a
/// field with `#[serve(skip)]`. A hand-written impl builds [`Entry`]s
/// directly.
pub trait Serve: fold::Declare<Error: IntoProblem> {
    fn entries() -> Vec<Entry<Self>>;
}

/// Support for the derive. Not public API.
#[doc(hidden)]
pub mod __private {
    use std::marker::PhantomData;

    use fold::Id;
    use fold::schema::{Data, Error, SourceDecl, Table};

    use crate::{Mint, Wire};

    pub use fold;

    /// Autoref specialization: `(&&Probe::<D>::NEW).mint::<E>()` finds
    /// [`MintById`] when `D` is `Table<Id<V>, V>` and [`MintNone`]
    /// otherwise, which the derive can't tell apart from type tokens.
    pub struct Probe<D>(PhantomData<fn() -> D>);

    impl<D> Probe<D> {
        pub const NEW: Self = Probe(PhantomData);
    }

    pub trait MintById<D: SourceDecl> {
        fn mint<E: Error>(&self) -> Option<Mint<D::Write<E>, E>>;
    }

    impl<V: Data + Wire> MintById<Table<Id<V>, V>> for &Probe<Table<Id<V>, V>> {
        fn mint<E: Error>(&self) -> Option<Mint<<Table<Id<V>, V> as SourceDecl>::Write<E>, E>> {
            Some(Mint {
                stage: crate::resources::mint_table::<V, E>,
                route: crate::resources::mint_table_route::<V>,
            })
        }
    }

    pub trait MintNone<D: SourceDecl> {
        fn mint<E: Error>(&self) -> Option<Mint<D::Write<E>, E>>;
    }

    impl<D: SourceDecl> MintNone<D> for Probe<D> {
        fn mint<E: Error>(&self) -> Option<Mint<D::Write<E>, E>> {
            None
        }
    }
}
