//! Sources: `Table`, `Events`, `Set`, `Cell`, `Multiset`.

use std::ops::Bound;

use fold3::schema::{
    Cell, CellRead, CellWrite, Change, Data, Delta, Events, EventsWrite, Id, Multiset,
    MultisetRead, MultisetWrite, Read, Set, SetRead, SetWrite, Table, TableWrite,
};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::Value;

use super::{answer_keyed, get_route, keyed_change, list_route, read, value_change, value_row};
use crate::Problem;
use crate::resource::{
    Apply, Call, Describe, Live, MINT_OP, Resource, Route, SourceResource, Written, schema_in,
};
use crate::via::{Shape, Via, ViaId, W};
use crate::wire::{
    Counted, DEFAULT_LIMIT, KeyOf, ListParams, Order, Page, Removed, decode_cursor, limit,
    paginate_results, to_json,
};

// ---------------------------------------------------------------------------
// Table
// ---------------------------------------------------------------------------

impl<K: 'static, V: 'static> Shape for Table<K, V> {
    type Key = K;
    type Value = V;
}

impl<K: Data, V: Data, MK: Via<K>, MV: Via<V>> Resource<(MK, MV)> for Table<K, V> {
    type Facade<'a> = Read<'a, K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "table", schema_in::<W<V, MV>>(g))
            .keyed(schema_in::<W<K, MK>>(g))
            .route(list_route::<K, V, MK, MV>(g))
            .route(get_route::<K, V, MK, MV>(g))
            .route(
                Route::put("/{key}", "upsert", "Insert or replace the value at a key")
                    .key::<W<K, MK>>(g)
                    .body::<W<V, MV>>(g)
                    .response::<KeyOf<W<K, MK>>>(g)
                    .creates(),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove the row at a key")
                    .key::<W<K, MK>>(g)
                    .response::<Removed>(g),
            )
    }

    fn answer(facade: &Read<'_, K, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed::<K, V, MK, MV>(facade, call)
    }

    fn live(facade: &Read<'_, K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change::<K, V, MK, MV>))
    }
}

impl<K: Data, V: Data, MK: Via<K>, MV: Via<V>> SourceResource<(MK, MV)> for Table<K, V> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        match call.op {
            "upsert" => {
                let key = call.key::<W<K, MK>>()?.into_inner();
                let value = call.body::<W<V, MV>>()?.into_inner();
                Ok(Box::new(move |w: &TableWrite<'_, K, V>| {
                    let created = !w.contains(&key)?;
                    let out = Written::ok(&KeyOf {
                        key: W::<K, MK>::new(key.clone()),
                    })
                    .created_if(created);
                    w.upsert(key, value)?;
                    Ok(out)
                }))
            }
            "remove" => {
                let key = call.key::<W<K, MK>>()?.into_inner();
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
pub(crate) fn mint_table<V: Data, MV: Via<V>>(
    call: Call,
) -> Result<Apply<Table<Id<V>, V>>, Problem> {
    let value = call.body::<W<V, MV>>()?.into_inner();
    Ok(Box::new(move |w: &TableWrite<'_, Id<V>, V>| {
        let key = w.insert(value)?;
        Ok(Written::ok(&KeyOf {
            key: W::<Id<V>, ViaId>::new(key),
        })
        .created_if(true))
    }))
}

pub(crate) fn mint_table_route<V: Data, MV: Via<V>>(g: &mut SchemaGenerator) -> Route {
    Route::post("", MINT_OP, "Insert a row, minting its id")
        .body::<W<V, MV>>(g)
        .response::<KeyOf<W<Id<V>, ViaId>>>(g)
        .creates()
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

impl<V: 'static> Shape for Events<V> {
    type Key = Id<V>;
    type Value = V;
}

impl<V: Data, MK: Via<Id<V>>, MV: Via<V>> Resource<(MK, MV)> for Events<V> {
    type Facade<'a> = Read<'a, Id<V>, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "events", schema_in::<W<V, MV>>(g))
            .keyed(schema_in::<W<Id<V>, MK>>(g))
            .route(list_route::<Id<V>, V, MK, MV>(g))
            .route(get_route::<Id<V>, V, MK, MV>(g))
            .route(
                Route::post("", "append", "Append an event, minting its id")
                    .body::<W<V, MV>>(g)
                    .response::<KeyOf<W<Id<V>, MK>>>(g)
                    .creates(),
            )
    }

    fn answer(facade: &Read<'_, Id<V>, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed::<Id<V>, V, MK, MV>(facade, call)
    }

    fn live(facade: &Read<'_, Id<V>, V>) -> Option<Live> {
        Some(Live::new(
            facade.subscribe(),
            keyed_change::<Id<V>, V, MK, MV>,
        ))
    }
}

impl<V: Data, MK: Via<Id<V>>, MV: Via<V>> SourceResource<(MK, MV)> for Events<V> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        if call.op != "append" {
            return Err(call.unrouted());
        }
        let value = call.body::<W<V, MV>>()?.into_inner();
        Ok(Box::new(move |w: &EventsWrite<'_, V>| {
            let key = w.push(value)?;
            Ok(Written::ok(&KeyOf {
                key: W::<Id<V>, MK>::new(key),
            })
            .created_if(true))
        }))
    }
}

// ---------------------------------------------------------------------------
// Set
// ---------------------------------------------------------------------------

impl<T: 'static> Shape for Set<T> {
    type Key = T;
    type Value = ();
}

impl<T: Data, MK: Via<T>, MV: 'static> Resource<(MK, MV)> for Set<T> {
    type Facade<'a> = SetRead<'a, T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "set", schema_in::<W<T, MK>>(g))
            .route(
                Route::get("", "list", "List members in order")
                    .query::<ListParams>()
                    .response::<Page<W<T, MK>>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "contains", "Test membership: `true`, or 404")
                    .key::<W<T, MK>>(g)
                    .response::<bool>(g)
                    .may_404(),
            )
            .route(
                Route::put("/{key}", "insert", "Add a member")
                    .key::<W<T, MK>>(g)
                    .response::<KeyOf<W<T, MK>>>(g)
                    .creates(),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove a member")
                    .key::<W<T, MK>>(g)
                    .response::<Removed>(g),
            )
    }

    fn answer(facade: &SetRead<'_, T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "list" => {
                let p: ListParams = call.query()?;
                let limit = limit(p.limit, DEFAULT_LIMIT)?;
                let after = p
                    .after
                    .as_deref()
                    .map(decode_cursor::<W<T, MK>>)
                    .transpose()?
                    .map(W::into_inner);
                let rows: Box<dyn Iterator<Item = fold3::Result<T>>> = match (p.order, after) {
                    (Order::Asc, None) => Box::new(facade.iter()),
                    (Order::Asc, Some(t)) => {
                        Box::new(facade.range((Bound::Excluded(t), Bound::Unbounded)))
                    }
                    (Order::Desc, None) => Box::new(facade.iter_rev()),
                    // fold3's sets have no reverse range: skip down to the
                    // cursor from the greatest member.
                    (Order::Desc, Some(t)) => Box::new(
                        facade
                            .iter_rev()
                            .skip_while(move |r| r.as_ref().is_ok_and(|m| *m >= t)),
                    ),
                };
                let rows = rows.map(|r| r.map(W::<T, MK>::new));
                to_json(&paginate_results(rows, limit, W::clone)?)
            }
            "contains" => {
                let member = call.key::<W<T, MK>>()?.into_inner();
                if read(facade.contains(&member))? {
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
            |(t, change): (T, Change<()>)| {
                let diff = match change {
                    Change::Insert(()) => 1,
                    Change::Delete(()) => -1,
                    Change::Replace((), ()) => 0,
                };
                (diff != 0).then(|| value_row::<T, MK>(t, diff))
            },
        ))
    }
}

impl<T: Data, MK: Via<T>, MV: 'static> SourceResource<(MK, MV)> for Set<T> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        let member = call.key::<W<T, MK>>()?.into_inner();
        match call.op {
            "insert" => Ok(Box::new(move |w: &SetWrite<'_, T>| {
                let key = W::<T, MK>::new(member.clone());
                let created = w.insert(member)?;
                Ok(Written::ok(&KeyOf { key }).created_if(created))
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

impl<V: 'static> Shape for Cell<V> {
    type Key = ();
    type Value = V;
}

impl<V: Data, MK: 'static, MV: Via<V>> Resource<(MK, MV)> for Cell<V> {
    type Facade<'a> = CellRead<'a, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "cell", schema_in::<W<V, MV>>(g))
            .route(
                Route::get("", "get", "Get the value: `null` while unset")
                    .response::<Option<W<V, MV>>>(g)
                    .live(),
            )
            .route(
                Route::put("", "set", "Set the value")
                    .body::<W<V, MV>>(g)
                    .response::<W<V, MV>>(g)
                    .creates(),
            )
            .route(Route::delete("", "clear", "Unset the value").response::<Removed>(g))
    }

    fn answer(facade: &CellRead<'_, V>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "get" => to_json(&read(facade.get())?.map(W::<V, MV>::new)),
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &CellRead<'_, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), value_change::<V, MV>))
    }
}

impl<V: Data, MK: 'static, MV: Via<V>> SourceResource<(MK, MV)> for Cell<V> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        match call.op {
            "set" => {
                let value = call.body::<W<V, MV>>()?;
                Ok(Box::new(move |w: &CellWrite<'_, V>| {
                    let created = w.get()?.is_none();
                    let out = Written::ok(&value).created_if(created);
                    w.set(value.into_inner())?;
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

impl<T: 'static> Shape for Multiset<T> {
    type Key = T;
    type Value = ();
}

impl<T: Data, MK: Via<T>, MV: 'static> Resource<(MK, MV)> for Multiset<T> {
    type Facade<'a> = MultisetRead<'a, T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "multiset", schema_in::<W<T, MK>>(g))
            .route(
                Route::get("", "list", "List values in order, with their counts")
                    .query::<ListParams>()
                    .response::<Page<Counted<W<T, MK>>>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "count", "Count copies of a value (0 if none)")
                    .key::<W<T, MK>>(g)
                    .response::<Delta>(g),
            )
            .route(
                Route::post("", "add", "Add copies of a value")
                    .body::<MultisetAdd<W<T, MK>>>(g)
                    .response::<Counted<W<T, MK>>>(g),
            )
            .route(
                Route::delete("/{key}", "remove", "Remove copies of a value")
                    .key::<W<T, MK>>(g)
                    .query::<MultisetRemove>()
                    .response::<Counted<W<T, MK>>>(g),
            )
    }

    fn answer(facade: &MultisetRead<'_, T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "list" => {
                let p: ListParams = call.query()?;
                let limit = limit(p.limit, DEFAULT_LIMIT)?;
                let after = p
                    .after
                    .as_deref()
                    .map(decode_cursor::<W<T, MK>>)
                    .transpose()?
                    .map(W::into_inner);
                // fold3's multisets read only forwards, from the start:
                // ascending pages skip to the cursor, descending ones
                // reverse the whole multiset.
                let rows: Box<dyn Iterator<Item = fold3::Result<(T, Delta)>>> =
                    match (p.order, after) {
                        (Order::Asc, None) => Box::new(facade.iter()),
                        (Order::Asc, Some(t)) => Box::new(
                            facade
                                .iter()
                                .skip_while(move |r| r.as_ref().is_ok_and(|(v, _)| *v <= t)),
                        ),
                        (Order::Desc, after) => {
                            let mut all = facade.iter().collect::<fold3::Result<Vec<_>>>();
                            if let Ok(all) = &mut all {
                                if let Some(t) = &after {
                                    all.retain(|(v, _)| v < t);
                                }
                                all.reverse();
                            }
                            match all {
                                Ok(all) => Box::new(all.into_iter().map(Ok)),
                                Err(e) => Box::new(std::iter::once(Err(e))),
                            }
                        }
                    };
                let rows = rows.map(|r| {
                    r.map(|(value, count)| Counted {
                        value: W::<T, MK>::new(value),
                        count,
                    })
                });
                to_json(&paginate_results(rows, limit, |c| c.value.clone())?)
            }
            "count" => {
                let value = call.key::<W<T, MK>>()?.into_inner();
                to_json(&read(facade.get(&value))?)
            }
            _ => Err(call.unrouted()),
        }
    }

    fn live(facade: &MultisetRead<'_, T>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), |(t, d)| {
            Some(value_row::<T, MK>(t, d))
        }))
    }
}

impl<T: Data, MK: Via<T>, MV: 'static> SourceResource<(MK, MV)> for Multiset<T> {
    fn stage(call: Call) -> Result<Apply<Self>, Problem> {
        match call.op {
            "add" => {
                let MultisetAdd { value, times: n }: MultisetAdd<W<T, MK>> = call.body()?;
                let (value, n) = (value.into_inner(), times(n)?);
                Ok(Box::new(move |w: &MultisetWrite<'_, T>| {
                    w.add(value.clone(), n)?;
                    let count = w.get(&value)?;
                    Ok(Written::ok(&Counted {
                        value: W::<T, MK>::new(value),
                        count,
                    }))
                }))
            }
            "remove" => {
                let value = call.key::<W<T, MK>>()?.into_inner();
                let n = times(call.query::<MultisetRemove>()?.times)?;
                Ok(Box::new(move |w: &MultisetWrite<'_, T>| {
                    let n = n.min(w.get(&value)?.max(0));
                    if n > 0 {
                        w.remove(value.clone(), n)?;
                    }
                    let count = w.get(&value)?;
                    Ok(Written::ok(&Counted {
                        value: W::<T, MK>::new(value),
                        count,
                    }))
                }))
            }
            _ => Err(call.unrouted()),
        }
    }
}
