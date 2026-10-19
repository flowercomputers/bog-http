//! [`Resource`] impls for fold's declaration types.

use fold::schema::{Data, Error, Events, EventsWrite, MapView, Table, TableWrite};
use fold::{Id, MapRead, Read};
use serde_json::{Value, json};

use crate::resource::{
    Apply, Describe, Ops, ReadOp, Resource, SourceResource, WriteOp, Written, schema_of,
};
use crate::wire::{page, parse_body, parse_key, to_json};
use crate::{Problem, Wire};

/// Listing and point reads, shared by every `Read<K, V>`-backed kind.
fn read_keyed<K: Data + Wire, V: Data + Wire>(
    read: &Read<K, V>,
    op: &ReadOp,
) -> Result<Value, Problem> {
    match op {
        ReadOp::List(params) => page(read, params),
        ReadOp::Get(segment) => {
            let key: K = parse_key(segment)?;
            match read.get(&key) {
                Some(value) => to_json(&value),
                None => Err(Problem::not_found(format!("no row at key {segment}"))),
            }
        }
    }
}

fn keyed<K: Wire, V: Wire>(name: &str, kind: &'static str, ops: Ops) -> Describe {
    Describe {
        name: name.to_owned(),
        kind,
        key: Some(schema_of::<K>()),
        value: schema_of::<V>(),
        ops,
    }
}

const READ_ONLY: Ops = Ops {
    list: true,
    get: true,
    insert: false,
    upsert: false,
    remove: false,
};

impl<K: Data + Wire, V: Data + Wire> Resource for Table<K, V> {
    type Facade = Read<K, V>;

    fn describe(name: &str) -> Describe {
        keyed::<K, V>(
            name,
            "table",
            Ops {
                upsert: true,
                remove: true,
                ..READ_ONLY
            },
        )
    }

    fn answer(facade: &Read<K, V>, op: &ReadOp) -> Result<Value, Problem> {
        read_keyed(facade, op)
    }
}

impl<K: Data + Wire, V: Data + Wire> SourceResource for Table<K, V> {
    fn stage<E: Error>(op: WriteOp) -> Result<Apply<TableWrite<K, V, E>, E>, Problem> {
        match op {
            WriteOp::Upsert(segment, body) => {
                let key: K = parse_key(&segment)?;
                let value: V = parse_body(&body)?;
                let key_json = to_json(&key)?;
                Ok(Box::new(move |w: &mut TableWrite<K, V, E>| {
                    let created = w.get(&key).is_none();
                    w.upsert(key, value)?;
                    Ok(Written {
                        data: json!({ "key": key_json }),
                        created,
                    })
                }))
            }
            WriteOp::Remove(segment) => {
                let key: K = parse_key(&segment)?;
                Ok(Box::new(move |w: &mut TableWrite<K, V, E>| {
                    let removed = w.get(&key).is_some();
                    w.remove(&key);
                    Ok(Written {
                        data: json!({ "removed": removed }),
                        created: false,
                    })
                }))
            }
            WriteOp::Insert(_) => Err(Problem::method_not_allowed()),
        }
    }
}

/// `POST` for a `Table<Id<V>, V>`: insert, minting the id.
pub(crate) fn mint_table<V: Data + Wire, E: Error>(
    op: WriteOp,
) -> Result<Apply<TableWrite<Id<V>, V, E>, E>, Problem> {
    let WriteOp::Insert(body) = op else {
        return Err(Problem::method_not_allowed());
    };
    let value: V = parse_body(&body)?;
    Ok(Box::new(move |w: &mut TableWrite<Id<V>, V, E>| {
        let id = w.insert(value)?;
        Ok(Written {
            data: json!({ "key": id.raw() }),
            created: true,
        })
    }))
}

impl<V: Data + Wire> Resource for Events<V> {
    type Facade = Read<Id<V>, V>;

    fn describe(name: &str) -> Describe {
        keyed::<Id<V>, V>(
            name,
            "events",
            Ops {
                insert: true,
                ..READ_ONLY
            },
        )
    }

    fn answer(facade: &Read<Id<V>, V>, op: &ReadOp) -> Result<Value, Problem> {
        read_keyed(facade, op)
    }
}

impl<V: Data + Wire> SourceResource for Events<V> {
    fn stage<E: Error>(op: WriteOp) -> Result<Apply<EventsWrite<V, E>, E>, Problem> {
        let WriteOp::Insert(body) = op else {
            return Err(Problem::method_not_allowed());
        };
        let value: V = parse_body(&body)?;
        Ok(Box::new(move |w: &mut EventsWrite<V, E>| {
            let id = w.push(value)?;
            Ok(Written {
                data: json!({ "key": id.raw() }),
                created: true,
            })
        }))
    }
}

impl<K: Data + Wire, V: Data + Wire> Resource for MapView<K, V> {
    type Facade = MapRead<K, V>;

    fn describe(name: &str) -> Describe {
        keyed::<K, V>(name, "map_view", READ_ONLY)
    }

    fn answer(facade: &MapRead<K, V>, op: &ReadOp) -> Result<Value, Problem> {
        read_keyed(facade, op)
    }
}
