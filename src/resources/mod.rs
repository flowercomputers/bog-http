//! [`Resource`](crate::Resource) impls for fold's declaration types.
//!
//! Sources (`Table`, `Events`, `Set`, `Cell`, `Bag`) are in [`sources`];
//! views (`MapView`, `OrderedView`, `GroupView`, `ValueView`,
//! `SearchView`) in [`views`].

mod sources;
mod views;

pub use sources::{BagAdd, BagRemove};
pub(crate) use sources::{mint_table, mint_table_route};
pub use views::{By, CompleteParams, Completion, Hit, OrderedParams, SearchParams};

use std::ops::Bound;

use fold::Read;
use fold::schema::Data;
use schemars::SchemaGenerator;
use serde_json::Value;

use crate::resource::{Call, Route};
use crate::wire::{
    DEFAULT_LIMIT, ListParams, Order, Page, Row, decode_cursor, limit, paginate, to_json,
};
use crate::{Problem, Wire};

/// `GET /n`: a page of `{key, value}` rows in key order.
fn list_route<K: Wire, V: Wire>(g: &mut SchemaGenerator) -> Route {
    Route::get("", "list", "List rows in key order")
        .query::<ListParams>()
        .response::<Page<Row<K, V>>>(g)
        .live()
}

/// `GET /n/{key}`: one row's value.
fn get_route<K: Wire, V: Wire>(g: &mut SchemaGenerator) -> Route {
    Route::get("/{key}", "get", "Get the value at a key")
        .key::<K>(g)
        .response::<V>(g)
        .may_404()
}

/// A `Read<K, V>`'s rows strictly after `after`, in `order`.
fn keyed_rows<'a, K: Data, V: Data>(
    read: &'a Read<K, V>,
    order: Order,
    after: Option<K>,
) -> Box<dyn Iterator<Item = (K, V)> + 'a> {
    match (order, after) {
        (Order::Asc, None) => Box::new(read.iter()),
        (Order::Asc, Some(k)) => Box::new(read.range((Bound::Excluded(k), Bound::Unbounded))),
        (Order::Desc, None) => Box::new(read.iter_rev()),
        (Order::Desc, Some(k)) => Box::new(read.range_rev(..k)),
    }
}

/// A page of a `Read<K, V>` in key order.
fn list_keyed<K: Data + Wire, V: Data + Wire>(
    read: &Read<K, V>,
    order: Order,
    requested: Option<usize>,
    after: Option<&str>,
) -> Result<Value, Problem> {
    let limit = limit(requested, DEFAULT_LIMIT)?;
    let after = after.map(decode_cursor::<K>).transpose()?;
    let rows = keyed_rows(read, order, after).map(|(key, value)| Row { key, value });
    to_json(&paginate(rows, limit, |r| r.key.clone())?)
}

/// Answer `list` and `get` for any `Read<K, V>`-backed resource.
fn answer_keyed<K: Data + Wire, V: Data + Wire>(
    read: &Read<K, V>,
    call: &Call,
) -> Result<Value, Problem> {
    match call.op {
        "list" => {
            let p: ListParams = call.query()?;
            list_keyed(read, p.order, p.limit, p.after.as_deref())
        }
        "get" => {
            let key: K = call.key()?;
            match read.get(&key) {
                Some(value) => to_json(&value),
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

/// A change to a keyed row: `{key, value, diff}`.
fn keyed_change<K: serde::Serialize, V: serde::Serialize>(
    (key, value): (K, V),
    diff: fold::Diff,
) -> Value {
    serde_json::json!({ "key": key, "value": value, "diff": diff })
}

/// A change to an unkeyed value (a member, an element, a singleton):
/// `{value, diff}`.
fn value_change<T: serde::Serialize>(value: T, diff: fold::Diff) -> Value {
    serde_json::json!({ "value": value, "diff": diff })
}
