//! [`Resource`](crate::Resource) impls for fold3's declaration types.
//!
//! Sources (`Table`, `Events`, `Set`, `Cell`, `Multiset`) are in
//! [`sources`]; views (`MapView`, `OrderedView`, `GroupView`,
//! `ValueView`, `SearchView`) in [`views`]. Every impl is over `(MK, MV)`,
//! the [`Via`] encoders of the kind's key and value (its [`Shape`]), and
//! puts keys and values on the wire as [`W`]s.
//!
//! [`Shape`]: crate::Shape

mod sources;
mod views;

pub use sources::{MultisetAdd, MultisetRemove};
pub(crate) use sources::{mint_table, mint_table_route};
pub use views::{By, CompleteParams, Completion, Hit, OrderedParams, SearchParams};

use std::ops::Bound;

use fold3::schema::{Change, Data, Read};
use schemars::SchemaGenerator;
use serde_json::{Value, json};

use crate::resource::{Call, Route};
use crate::via::{Via, W};
use crate::wire::{
    DEFAULT_LIMIT, ListParams, Order, Page, Row, decode_cursor, limit, paginate_results, to_json,
};
use crate::{IntoProblem, Problem};

/// A fold3 read result as a problem.
pub(crate) fn read<T>(r: fold3::Result<T>) -> Result<T, Problem> {
    r.map_err(IntoProblem::into_problem)
}

/// `GET /n`: a page of `{key, value}` rows in key order.
fn list_route<K, V, MK: Via<K>, MV: Via<V>>(g: &mut SchemaGenerator) -> Route {
    Route::get("", "list", "List rows in key order")
        .query::<ListParams>()
        .response::<Page<Row<W<K, MK>, W<V, MV>>>>(g)
        .live()
}

/// `GET /n/{key}`: one row's value.
fn get_route<K, V, MK: Via<K>, MV: Via<V>>(g: &mut SchemaGenerator) -> Route {
    Route::get("/{key}", "get", "Get the value at a key")
        .key::<W<K, MK>>(g)
        .response::<W<V, MV>>(g)
        .may_404()
}

/// A `Read<K, V>`'s rows strictly after `after`, in `order`.
fn keyed_rows<'a, K: Data, V: Data>(
    read: &Read<'a, K, V>,
    order: Order,
    after: Option<K>,
) -> Box<dyn Iterator<Item = fold3::Result<(K, V)>> + 'a> {
    match (order, after) {
        (Order::Asc, None) => Box::new(read.iter()),
        (Order::Asc, Some(k)) => Box::new(read.range((Bound::Excluded(k), Bound::Unbounded))),
        (Order::Desc, None) => Box::new(read.iter_rev()),
        (Order::Desc, Some(k)) => Box::new(read.range_rev(..k)),
    }
}

/// A page of a `Read<K, V>` in key order.
fn list_keyed<K: Data, V: Data, MK: Via<K>, MV: Via<V>>(
    read: &Read<'_, K, V>,
    order: Order,
    requested: Option<usize>,
    after: Option<&str>,
) -> Result<Value, Problem> {
    let limit = limit(requested, DEFAULT_LIMIT)?;
    let after = after
        .map(decode_cursor::<W<K, MK>>)
        .transpose()?
        .map(W::into_inner);
    let rows = keyed_rows(read, order, after).map(|r| {
        r.map(|(key, value)| Row {
            key: W::<K, MK>::new(key),
            value: W::<V, MV>::new(value),
        })
    });
    to_json(&paginate_results(rows, limit, |r| r.key.clone())?)
}

/// Answer `list` and `get` for any `Read<K, V>`-backed resource.
fn answer_keyed<K: Data, V: Data, MK: Via<K>, MV: Via<V>>(
    facade: &Read<'_, K, V>,
    call: &Call,
) -> Result<Value, Problem> {
    match call.op {
        "list" => {
            let p: ListParams = call.query()?;
            list_keyed::<K, V, MK, MV>(facade, p.order, p.limit, p.after.as_deref())
        }
        "get" => {
            let key = call.key::<W<K, MK>>()?.into_inner();
            match read(facade.get(&key))? {
                Some(value) => to_json(&W::<V, MV>::new(value)),
                None => Err(no_row(call)),
            }
        }
        _ => Err(call.unrouted()),
    }
}

fn no_row(call: &Call) -> Problem {
    Problem::not_found(format!(
        "no row at key {}",
        call.key.as_deref().unwrap_or_default()
    ))
}

/// A change's rows as `(value, diff)`: a replaced value is retracted,
/// then the new one inserted.
fn diffs<V>(change: Change<V>) -> impl Iterator<Item = (V, i64)> {
    let (old, new) = match change {
        Change::Insert(v) => (None, Some(v)),
        Change::Delete(v) => (Some(v), None),
        Change::Replace(o, n) => (Some(o), Some(n)),
    };
    old.map(|v| (v, -1)).into_iter().chain(new.map(|v| (v, 1)))
}

/// A change to a keyed row, as `{key, value, diff}` rows.
fn keyed_change<K: Clone, V, MK: Via<K>, MV: Via<V>>((key, change): (K, Change<V>)) -> Vec<Value> {
    diffs(change)
        .map(|(value, diff)| {
            json!({
                "key": W::<K, MK>::new(key.clone()),
                "value": W::<V, MV>::new(value),
                "diff": diff,
            })
        })
        .collect()
}

/// An unkeyed row (a member, an element, a singleton): `{value, diff}`.
fn value_row<T, M: Via<T>>(value: T, diff: i64) -> Value {
    json!({ "value": W::<T, M>::new(value), "diff": diff })
}

/// A change to a singleton, as `{value, diff}` rows.
fn value_change<V, MV: Via<V>>(change: Change<V>) -> Vec<Value> {
    diffs(change)
        .map(|(v, d)| value_row::<V, MV>(v, d))
        .collect()
}
