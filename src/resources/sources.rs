//! Sources: `Table`, `Events`, `Set`, `Cell`, `Multiset`.

use std::ops::Bound;

use bog::schema::Cell;
use bog::schema::{
    CellRead, CellWrite, Change, Data, Delta, Events, EventsWrite, Id, Multiset, MultisetRead,
    MultisetWrite, Read, Set, SetRead, SetWrite, Table, TableWrite,
};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::Value;

use super::{answer_keyed, counted_change, get_route, keyed_change, list_route, value_change};
use crate::resource::{
    Apply, Call, Describe, Live, MINT_OP, Resource, Route, SourceResource, Written, schema_in,
};
use crate::wire::{
    Counted, DEFAULT_LIMIT, KeyOf, ListParams, Order, Removed, decode_cursor, limit, paginate,
    to_json,
};
use crate::{Problem, Wire};

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

impl<K: Data + Wire, V: Data + Wire> Resource for Table<K, V> {
    type Facade<'a> = Read<'a, K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "table", schema_in::<V>(g))
            .keyed(schema_in::<K>(g))
            .route(list_route::<K, V>(g))
            .route(get_route::<K, V>(g))
            .route(
                Route::put("/{key}", "upsert", "Insert or replace the value at a key")
                    .key::<K>(g)
                    .body::<V>(g)
                    .response::<KeyOf<K>>(g)
                    .creates(),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove the row at a key")
                    .key::<K>(g)
                    .response::<Removed>(g),
            )
    }

    fn answer(facade: &Read<'_, K, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed(facade, call)
    }

    fn live(facade: &Read<'_, K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
    }
}

impl<K: Data + Wire, V: Data + Wire> SourceResource for Table<K, V> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        match call.op {
            "upsert" => {
                let key: K = call.key()?;
                let value: V = call.body()?;
                Ok(Box::new(move |w: &TableWrite<'_, K, V>| {
                    let created = !w.contains(&key)?;
                    let out = Written::ok(&KeyOf { key: key.clone() }).created_if(created);
                    w.upsert(key, value)?;
                    Ok(out)
                }))
            }
            "remove" => {
                let key: K = call.key()?;
                Ok(Box::new(move |w: &TableWrite<'_, K, V>| {
                    let removed = w.remove(&key)?;
                    Ok(Written::ok(&Removed { removed }))
                }))
            }
            _ => Err(call.unrouted()),
        }
    }
}

/// `POST` for a `Table<Id<V>, V>`: insert, minting the id.
pub(crate) fn mint_table<V: Data + Wire>(call: Call) -> Result<Apply<Table<Id<V>, V>>, Problem> {
    let value: V = call.body()?;
    Ok(Box::new(move |w: &TableWrite<'_, Id<V>, V>| {
        let key = w.insert(value)?;
        Ok(Written::ok(&KeyOf { key }).created_if(true))
    }))
}

pub(crate) fn mint_table_route<V: Data + Wire>(g: &mut SchemaGenerator) -> Route {
    Route::post("", MINT_OP, "Insert a row, minting its id")
        .body::<V>(g)
        .response::<KeyOf<Id<V>>>(g)
        .creates()
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

impl<V: Data + Wire> Resource for Events<V> {
    type Facade<'a> = Read<'a, Id<V>, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "events", schema_in::<V>(g))
            .keyed(schema_in::<Id<V>>(g))
            .route(list_route::<Id<V>, V>(g))
            .route(get_route::<Id<V>, V>(g))
            .route(
                Route::post("", "append", "Append an event, minting its id")
                    .body::<V>(g)
                    .response::<KeyOf<Id<V>>>(g)
                    .creates(),
            )
    }

    fn answer(facade: &Read<'_, Id<V>, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed(facade, call)
    }

    fn live(facade: &Read<'_, Id<V>, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
    }
}

impl<V: Data + Wire> SourceResource for Events<V> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        if call.op != "append" {
            return Err(call.unrouted());
        }
        let value: V = call.body()?;
        Ok(Box::new(move |w: &EventsWrite<'_, V>| {
            let key = w.push(value)?;
            Ok(Written::ok(&KeyOf { key }).created_if(true))
        }))
    }
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

impl<T: Data + Wire> Resource for Set<T> {
    type Facade<'a> = SetRead<'a, T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "set", schema_in::<T>(g))
            .route(
                Route::get("", "list", "List members in order")
                    .query::<ListParams>()
                    .response::<crate::wire::Page<T>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "contains", "Test membership: `true`, or 404")
                    .key::<T>(g)
                    .response::<bool>(g)
                    .may_404(),
            )
            .route(
                Route::put("/{key}", "insert", "Add a member")
                    .key::<T>(g)
                    .response::<KeyOf<T>>(g)
                    .creates(),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove a member")
                    .key::<T>(g)
                    .response::<Removed>(g),
            )
    }

    fn answer(facade: &SetRead<'_, T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "list" => {
                let p: ListParams = call.query()?;
                let limit = limit(p.limit, DEFAULT_LIMIT)?;
                let after = p.after.as_deref().map(decode_cursor::<T>).transpose()?;
                let rows: Box<dyn Iterator<Item = bog::Result<T>> + '_> = match (p.order, after) {
                    (Order::Asc, None) => Box::new(facade.iter()),
                    (Order::Asc, Some(t)) => {
                        Box::new(facade.range((Bound::Excluded(t), Bound::Unbounded)))
                    }
                    (Order::Desc, None) => Box::new(facade.iter_rev()),
                    (Order::Desc, Some(t)) => Box::new(facade.range_rev(..t)),
                };
                to_json(&paginate(rows, limit, T::clone)?)
            }
            "contains" => {
                let member: T = call.key()?;
                if facade.contains(&member)? {
                    Ok(Value::Bool(true))
                } else {
                    Err(Problem::not_found(format!(
                        "{} is not a member",
                        call.key.as_deref().unwrap_or_default()
                    )))
                }
            }
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &SetRead<'_, T>) -> Option<Live> {
        Some(Live::new(
            facade.subscribe(),
            |(member, change): (T, _)| match change {
                Change::Delete(()) => serde_json::json!({ "op": "delete", "value": member }),
                _ => serde_json::json!({ "op": "insert", "value": member }),
            },
        ))
    }
}

impl<T: Data + Wire> SourceResource for Set<T> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        let member: T = call.key()?;
        match call.op {
            "insert" => Ok(Box::new(move |w: &SetWrite<'_, T>| {
                let created = w.insert(member.clone())?;
                Ok(Written::ok(&KeyOf { key: member }).created_if(created))
            })),
            "remove" => Ok(Box::new(move |w: &SetWrite<'_, T>| {
                let removed = w.remove(&member)?;
                Ok(Written::ok(&Removed { removed }))
            })),
            _ => Err(call.unrouted()),
        }
    }
}

// ---------------------------------------------------------------------------
// Cell
// ---------------------------------------------------------------------------

impl<V: Data + Wire> Resource for Cell<V> {
    type Facade<'a> = CellRead<'a, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "cell", schema_in::<V>(g))
            .route(
                Route::get("", "get", "Get the value: `null` while unset")
                    .response::<Option<V>>(g)
                    .live(),
            )
            .route(
                Route::put("", "set", "Set the value")
                    .body::<V>(g)
                    .response::<V>(g)
                    .creates(),
            )
            .route(Route::delete("", "clear", "Unset the value").response::<Removed>(g))
    }

    fn answer(facade: &CellRead<'_, V>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "get" => to_json(&facade.get()?),
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &CellRead<'_, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), value_change))
    }
}

impl<V: Data + Wire> SourceResource for Cell<V> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        match call.op {
            "set" => {
                let value: V = call.body()?;
                Ok(Box::new(move |w: &CellWrite<'_, V>| {
                    let created = w.get()?.is_none();
                    let out = Written::ok(&value).created_if(created);
                    w.set(value)?;
                    Ok(out)
                }))
            }
            "clear" => Ok(Box::new(move |w: &CellWrite<'_, V>| {
                let removed = w.get()?.is_some();
                w.clear()?;
                Ok(Written::ok(&Removed { removed }))
            })),
            _ => Err(call.unrouted()),
        }
    }
}

// ---------------------------------------------------------------------------
// Multiset
// ---------------------------------------------------------------------------

/// `POST /n`: add copies of a value.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "MultisetAdd_{T}")]
pub struct MultisetAdd<T> {
    pub value: T,
    /// How many copies to add; 1 by default.
    pub times: Option<Delta>,
}

/// `DELETE /n/{key}`: remove copies of a value.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MultisetRemove {
    /// How many copies to remove, at most as many as there are; 1 by
    /// default.
    pub times: Option<Delta>,
}

fn times(requested: Option<Delta>) -> Result<Delta, Problem> {
    match requested.unwrap_or(1) {
        n if n > 0 => Ok(n),
        n => Err(Problem::bad_request(format!(
            "times must be positive, got {n}"
        ))),
    }
}

impl<T: Data + Wire> Resource for Multiset<T> {
    type Facade<'a> = MultisetRead<'a, T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "multiset", schema_in::<T>(g))
            .route(
                Route::get("", "list", "List values in order, with their counts")
                    .query::<ListParams>()
                    .response::<crate::wire::Page<Counted<T>>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "count", "Count copies of a value (0 if none)")
                    .key::<T>(g)
                    .response::<Delta>(g),
            )
            .route(
                Route::post("", "add", "Add copies of a value")
                    .body::<MultisetAdd<T>>(g)
                    .response::<Counted<T>>(g),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove copies of a value")
                    .key::<T>(g)
                    .query::<MultisetRemove>()
                    .response::<Counted<T>>(g),
            )
    }

    fn answer(facade: &MultisetRead<'_, T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "list" => {
                let p: ListParams = call.query()?;
                let limit = limit(p.limit, DEFAULT_LIMIT)?;
                let after = p.after.as_deref().map(decode_cursor::<T>).transpose()?;
                let rows: Box<dyn Iterator<Item = bog::Result<(T, Delta)>> + '_> =
                    match (p.order, after) {
                        (Order::Asc, None) => Box::new(facade.iter()),
                        (Order::Asc, Some(t)) => {
                            Box::new(facade.range((Bound::Excluded(t), Bound::Unbounded)))
                        }
                        (Order::Desc, None) => Box::new(facade.range_rev(..)),
                        (Order::Desc, Some(t)) => Box::new(facade.range_rev(..t)),
                    };
                let rows = rows.map(|r| r.map(|(value, count)| Counted { value, count }));
                to_json(&paginate(rows, limit, |c| c.value.clone())?)
            }
            "count" => to_json(&facade.get(&call.key::<T>()?)?),
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &MultisetRead<'_, T>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), counted_change))
    }
}

impl<T: Data + Wire> SourceResource for Multiset<T> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        match call.op {
            "add" => {
                let MultisetAdd { value, times: n }: MultisetAdd<T> = call.body()?;
                let n = times(n)?;
                Ok(Box::new(move |w: &MultisetWrite<'_, T>| {
                    w.add(value.clone(), n)?;
                    let count = w.get(&value)?;
                    Ok(Written::ok(&Counted { value, count }))
                }))
            }
            "remove" => {
                let value: T = call.key()?;
                let n = times(call.query::<MultisetRemove>()?.times)?;
                Ok(Box::new(move |w: &MultisetWrite<'_, T>| {
                    let n = n.min(w.get(&value)?.max(0));
                    if n > 0 {
                        w.remove(value.clone(), n)?;
                    }
                    let count = w.get(&value)?;
                    Ok(Written::ok(&Counted { value, count }))
                }))
            }
            _ => Err(call.unrouted()),
        }
    }
}
