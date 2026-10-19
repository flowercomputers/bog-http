//! Sources: `Table`, `Events`, `Set`, `Cell`, `Bag`.

use fold::schema::{
    Bag, BagWrite, Cell, CellWrite, Data, Error, Events, EventsWrite, Set, SetWrite, Table,
    TableWrite,
};
use fold::{BagRead, CellRead, Diff, Id, Read, SetRead};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::Value;

use super::{answer_keyed, get_route, keyed_change, list_route, value_change};
use crate::resource::{
    Apply, Call, Describe, Live, MINT_OP, Resource, Route, SourceResource, Written, schema_in,
};
use crate::wire::{
    Counted, DEFAULT_LIMIT, KeyOf, ListParams, Removed, arranged, decode_cursor, limit, paginate,
    to_json,
};
use crate::{Problem, Wire};

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

impl<K: Data + Wire, V: Data + Wire> Resource for Table<K, V> {
    type Facade = Read<K, V>;

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

    fn answer(facade: &Read<K, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed(facade, call)
    }

    fn live(facade: &Read<K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
    }
}

impl<K: Data + Wire, V: Data + Wire> SourceResource for Table<K, V> {
    fn stage<E: Error>(call: Call) -> Result<Apply<TableWrite<K, V, E>, E>, Problem> {
        match call.op {
            "upsert" => {
                let key: K = call.key()?;
                let value: V = call.body()?;
                Ok(Box::new(move |w: &mut TableWrite<K, V, E>| {
                    let created = w.get(&key).is_none();
                    let out = Written::ok(&KeyOf { key: key.clone() }).created_if(created);
                    w.upsert(key, value)?;
                    Ok(out)
                }))
            }
            "remove" => {
                let key: K = call.key()?;
                Ok(Box::new(move |w: &mut TableWrite<K, V, E>| {
                    let removed = w.get(&key).is_some();
                    w.remove(&key);
                    Ok(Written::ok(&Removed { removed }))
                }))
            }
            _ => Err(call.unrouted()),
        }
    }
}

/// `POST` for a `Table<Id<V>, V>`: insert, minting the id.
pub(crate) fn mint_table<V: Data + Wire, E: Error>(
    call: Call,
) -> Result<Apply<TableWrite<Id<V>, V, E>, E>, Problem> {
    let value: V = call.body()?;
    Ok(Box::new(move |w: &mut TableWrite<Id<V>, V, E>| {
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
    type Facade = Read<Id<V>, V>;

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

    fn answer(facade: &Read<Id<V>, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed(facade, call)
    }

    fn live(facade: &Read<Id<V>, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
    }
}

impl<V: Data + Wire> SourceResource for Events<V> {
    fn stage<E: Error>(call: Call) -> Result<Apply<EventsWrite<V, E>, E>, Problem> {
        if call.op != "append" {
            return Err(call.unrouted());
        }
        let value: V = call.body()?;
        Ok(Box::new(move |w: &mut EventsWrite<V, E>| {
            let key = w.push(value)?;
            Ok(Written::ok(&KeyOf { key }).created_if(true))
        }))
    }
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

impl<T: Data + Wire> Resource for Set<T> {
    type Facade = SetRead<T>;

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

    fn answer(facade: &SetRead<T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "list" => {
                let p: ListParams = call.query()?;
                let limit = limit(p.limit, DEFAULT_LIMIT)?;
                let after = p.after.as_deref().map(decode_cursor::<T>).transpose()?;
                let rows = arranged(facade.arrangement(), p.order, after.map(|t| (t, ())))
                    .map(|((t, ()), _)| t);
                to_json(&paginate(rows, limit, T::clone)?)
            }
            "contains" => {
                let member: T = call.key()?;
                if facade.contains(&member) {
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

    fn live(facade: &SetRead<T>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), |(t, ()), d| {
            value_change(t, d)
        }))
    }
}

impl<T: Data + Wire> SourceResource for Set<T> {
    fn stage<E: Error>(call: Call) -> Result<Apply<SetWrite<T, E>, E>, Problem> {
        let member: T = call.key()?;
        match call.op {
            "insert" => Ok(Box::new(move |w: &mut SetWrite<T, E>| {
                let created = !w.contains(&member);
                let out = Written::ok(&KeyOf {
                    key: member.clone(),
                })
                .created_if(created);
                w.insert(member)?;
                Ok(out)
            })),
            "remove" => Ok(Box::new(move |w: &mut SetWrite<T, E>| {
                let removed = w.contains(&member);
                w.remove(&member);
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
    type Facade = CellRead<V>;

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

    fn answer(facade: &CellRead<V>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "get" => to_json(&facade.get()),
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &CellRead<V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), |((), v), d| {
            value_change(v, d)
        }))
    }
}

impl<V: Data + Wire> SourceResource for Cell<V> {
    fn stage<E: Error>(call: Call) -> Result<Apply<CellWrite<V, E>, E>, Problem> {
        match call.op {
            "set" => {
                let value: V = call.body()?;
                Ok(Box::new(move |w: &mut CellWrite<V, E>| {
                    let created = w.get().is_none();
                    let out = Written::ok(&value).created_if(created);
                    w.set(value)?;
                    Ok(out)
                }))
            }
            "clear" => Ok(Box::new(move |w: &mut CellWrite<V, E>| {
                let removed = w.get().is_some();
                w.clear();
                Ok(Written::ok(&Removed { removed }))
            })),
            _ => Err(call.unrouted()),
        }
    }
}

// ---------------------------------------------------------------------------
// Bag
// ---------------------------------------------------------------------------

/// `POST /n`: add copies of a value.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "BagAdd_{T}")]
pub struct BagAdd<T> {
    pub value: T,
    /// How many copies to add; 1 by default.
    pub times: Option<Diff>,
}

/// `DELETE /n/{key}`: remove copies of a value.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct BagRemove {
    /// How many copies to remove, at most as many as there are; 1 by
    /// default.
    pub times: Option<Diff>,
}

fn times(requested: Option<Diff>) -> Result<Diff, Problem> {
    match requested.unwrap_or(1) {
        n if n > 0 => Ok(n),
        n => Err(Problem::bad_request(format!(
            "times must be positive, got {n}"
        ))),
    }
}

impl<T: Data + Wire> Resource for Bag<T> {
    type Facade = BagRead<T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "bag", schema_in::<T>(g))
            .route(
                Route::get("", "list", "List values in order, with their counts")
                    .query::<ListParams>()
                    .response::<crate::wire::Page<Counted<T>>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "count", "Count copies of a value (0 if none)")
                    .key::<T>(g)
                    .response::<Diff>(g),
            )
            .route(
                Route::post("", "add", "Add copies of a value")
                    .body::<BagAdd<T>>(g)
                    .response::<Counted<T>>(g),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove copies of a value")
                    .key::<T>(g)
                    .query::<BagRemove>()
                    .response::<Counted<T>>(g),
            )
    }

    fn answer(facade: &BagRead<T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "list" => {
                let p: ListParams = call.query()?;
                let limit = limit(p.limit, DEFAULT_LIMIT)?;
                let after = p.after.as_deref().map(decode_cursor::<T>).transpose()?;
                let rows = arranged(facade.arrangement(), p.order, after)
                    .map(|(value, count)| Counted { value, count });
                to_json(&paginate(rows, limit, |c| c.value.clone())?)
            }
            "count" => to_json(&facade.get(&call.key::<T>()?)),
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &BagRead<T>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), value_change))
    }
}

impl<T: Data + Wire> SourceResource for Bag<T> {
    fn stage<E: Error>(call: Call) -> Result<Apply<BagWrite<T, E>, E>, Problem> {
        match call.op {
            "add" => {
                let BagAdd { value, times: n }: BagAdd<T> = call.body()?;
                let n = times(n)?;
                Ok(Box::new(move |w: &mut BagWrite<T, E>| {
                    w.add(value.clone(), n)?;
                    let count = w.get(&value);
                    Ok(Written::ok(&Counted { value, count }))
                }))
            }
            "remove" => {
                let value: T = call.key()?;
                let n = times(call.query::<BagRemove>()?.times)?;
                Ok(Box::new(move |w: &mut BagWrite<T, E>| {
                    let n = n.min(w.get(&value).max(0));
                    if n > 0 {
                        w.remove(value.clone(), n);
                    }
                    let count = w.get(&value);
                    Ok(Written::ok(&Counted { value, count }))
                }))
            }
            _ => Err(call.unrouted()),
        }
    }
}
