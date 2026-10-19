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
//! For a resource `n`, as its kind allows:
//!
//! | route | |
//! |---|---|
//! | `GET /n?limit=&after=&order=` | a page of `{key, value}` rows, keyset-paginated |
//! | `GET /n/{key}` | one row's value, or 404 |
//! | `POST /n` | append (events) or insert minting an id (`Table<Id<V>, V>`): `201` |
//! | `PUT /n/{key}` | upsert: `201` if created, else `200` |
//! | `DELETE /n/{key}` | remove: `{removed}` |
//! | `GET /_schema` | every resource's kind, key and value JSON Schemas, ops |
//! | `GET /_health` | liveness |
//!
//! Successes are `{"epoch", "data"}` with a `fold-epoch` header; errors
//! are RFC 9457 `application/problem+json` (see [`Problem`]). Keys in
//! paths parse as JSON, falling back to a bare string (see
//! [`wire::parse_key`]).

mod problem;
mod resource;
mod resources;
mod server;
pub mod wire;

pub use problem::{IntoProblem, PROBLEM_JSON, Problem};
pub use resource::{
    Apply, Describe, Entry, Ops, ReadOp, Resource, SourceResource, Stage, Wire, WriteOp, Written,
    schema_of,
};
pub use server::Server;

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

    use crate::{Stage, Wire};

    pub use fold;

    /// Autoref specialization: `(&&Probe::<D>::NEW).mint::<E>()` finds
    /// [`MintById`] when `D` is `Table<Id<V>, V>` and [`MintNone`]
    /// otherwise, which the derive can't tell apart from type tokens.
    pub struct Probe<D>(PhantomData<fn() -> D>);

    impl<D> Probe<D> {
        pub const NEW: Self = Probe(PhantomData);
    }

    pub trait MintById<D: SourceDecl> {
        fn mint<E: Error>(&self) -> Option<Stage<D::Write<E>, E>>;
    }

    impl<V: Data + Wire> MintById<Table<Id<V>, V>> for &Probe<Table<Id<V>, V>> {
        fn mint<E: Error>(&self) -> Option<Stage<<Table<Id<V>, V> as SourceDecl>::Write<E>, E>> {
            Some(crate::resources::mint_table::<V, E>)
        }
    }

    pub trait MintNone<D: SourceDecl> {
        fn mint<E: Error>(&self) -> Option<Stage<D::Write<E>, E>>;
    }

    impl<D: SourceDecl> MintNone<D> for Probe<D> {
        fn mint<E: Error>(&self) -> Option<Stage<D::Write<E>, E>> {
            None
        }
    }
}
