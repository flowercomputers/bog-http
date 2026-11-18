//! Views: `MapView`, `OrderedView`, `GroupView`, `ValueView`,
//! `SearchView`. All read-only.

use std::ops::Bound;

use fold3::schema::{
    Data, Delta, GroupRead, GroupView, MapRead, MapView, OrderedRead, OrderedView, SearchRead,
    SearchView, Tokenizer, ValueRead, ValueView,
};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::Value;

use super::{answer_keyed, get_route, keyed_change, list_keyed, list_route, read, value_change};
use crate::Problem;
use crate::resource::{Call, Describe, Live, Resource, Route, schema_in};
use crate::via::{Shape, Via, W};
use crate::wire::{
    Counted, DEFAULT_LIMIT, GroupRow, ListParams, Order, Page, Row, decode_cursor, limit,
    paginate_results, parse_key, to_json,
};

// ---------------------------------------------------------------------------
// MapView
// ---------------------------------------------------------------------------

impl<K: 'static, V: 'static> Shape for MapView<K, V> {
    type Key = K;
    type Value = V;
}

impl<K: Data, V: Data, MK: Via<K>, MV: Via<V>> Resource<(MK, MV)> for MapView<K, V> {
    type Facade<'a> = MapRead<'a, K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "map_view", schema_in::<W<V, MV>>(g))
            .keyed(schema_in::<W<K, MK>>(g))
            .route(list_route::<K, V, MK, MV>(g))
            .route(get_route::<K, V, MK, MV>(g))
    }

    fn answer(facade: &MapRead<'_, K, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed::<K, V, MK, MV>(facade, call)
    }

    fn live(facade: &MapRead<'_, K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change::<K, V, MK, MV>))
    }
}

// ---------------------------------------------------------------------------
// OrderedView
// ---------------------------------------------------------------------------

/// What a listing of an ordered view is sorted by.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum By {
    #[default]
    Key,
    Value,
}

/// Query parameters of an ordered view's listing.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct OrderedParams {
    /// Page size, 1 to 1000; 100 by default.
    pub limit: Option<usize>,
    /// The `next` cursor of the previous page.
    pub after: Option<String>,
    /// `asc` (default) or `desc`.
    #[serde(default)]
    pub order: Order,
    /// Sort by `key` (default) or by `value` (ties by key).
    #[serde(default)]
    pub by: By,
    /// With `by=value`: the least value to include, as JSON.
    pub min: Option<String>,
    /// With `by=value`: the greatest value to include, as JSON.
    pub max: Option<String>,
}

impl<K: 'static, V: 'static> Shape for OrderedView<K, V> {
    type Key = K;
    type Value = V;
}

impl<K: Data, V: Data, MK: Via<K>, MV: Via<V>> Resource<(MK, MV)> for OrderedView<K, V> {
    type Facade<'a> = OrderedRead<'a, K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "ordered_view", schema_in::<W<V, MV>>(g))
            .keyed(schema_in::<W<K, MK>>(g))
            .route(
                Route::get("", "list", "List rows by key, or by value with `by=value`")
                    .query::<OrderedParams>()
                    .response::<Page<Row<W<K, MK>, W<V, MV>>>>(g)
                    .live(),
            )
            .route(get_route::<K, V, MK, MV>(g))
    }

    fn live(facade: &OrderedRead<'_, K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change::<K, V, MK, MV>))
    }

    fn answer(facade: &OrderedRead<'_, K, V>, call: &Call) -> Result<Value, Problem> {
        if call.op != "list" {
            return answer_keyed::<K, V, MK, MV>(facade, call);
        }
        let p: OrderedParams = call.query()?;
        if p.by == By::Key {
            if p.min.is_some() || p.max.is_some() {
                return Err(Problem::bad_request("min and max need by=value"));
            }
            return list_keyed::<K, V, MK, MV>(facade, p.order, p.limit, p.after.as_deref());
        }
        let limit = limit(p.limit, DEFAULT_LIMIT)?;
        let bound = |v: Option<&String>| -> Result<Bound<V>, Problem> {
            Ok(match v {
                Some(json) => Bound::Included(parse_key::<W<V, MV>>(json)?.into_inner()),
                None => Bound::Unbounded,
            })
        };
        let (min, max) = (bound(p.min.as_ref())?, bound(p.max.as_ref())?);
        let after = p
            .after
            .as_deref()
            .map(decode_cursor::<(W<V, MV>, W<K, MK>)>)
            .transpose()?
            .map(|(v, k)| (v.into_inner(), k.into_inner()));
        // Resume at the cursor's value, then skip its ties up to and
        // including the cursor's key: a seek, not a scan from the start.
        let rows: Box<dyn Iterator<Item = fold3::Result<(K, V)>>> = match (p.order, after) {
            (Order::Asc, None) => Box::new(facade.by_value((min, max))),
            (Order::Asc, Some((cv, ck))) => Box::new(
                facade
                    .by_value((Bound::Included(cv.clone()), max))
                    .skip_while(move |r| r.as_ref().is_ok_and(|(k, v)| *v == cv && *k <= ck)),
            ),
            (Order::Desc, None) => Box::new(facade.by_value_rev((min, max))),
            (Order::Desc, Some((cv, ck))) => Box::new(
                facade
                    .by_value_rev((min, Bound::Included(cv.clone())))
                    .skip_while(move |r| r.as_ref().is_ok_and(|(k, v)| *v == cv && *k >= ck)),
            ),
        };
        let rows = rows.map(|r| {
            r.map(|(key, value)| Row {
                key: W::<K, MK>::new(key),
                value: W::<V, MV>::new(value),
            })
        });
        to_json(&paginate_results(rows, limit, |r| {
            (r.value.clone(), r.key.clone())
        })?)
    }
}

// ---------------------------------------------------------------------------
// GroupView
// ---------------------------------------------------------------------------

impl<K: 'static, V: 'static> Shape for GroupView<K, V> {
    type Key = K;
    type Value = V;
}

impl<K: Data, V: Data, MK: Via<K>, MV: Via<V>> Resource<(MK, MV)> for GroupView<K, V> {
    type Facade<'a> = GroupRead<'a, K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "group_view", schema_in::<W<V, MV>>(g))
            .keyed(schema_in::<W<K, MK>>(g))
            .route(
                Route::get("", "list", "List every (key, value) pair, with counts")
                    .query::<ListParams>()
                    .response::<Page<GroupRow<W<K, MK>, W<V, MV>>>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "values", "List a key's values, with counts")
                    .key::<W<K, MK>>(g)
                    .query::<ListParams>()
                    .response::<Page<Counted<W<V, MV>>>>(g),
            )
    }

    fn live(facade: &GroupRead<'_, K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), |(key, value, diff)| {
            Some(serde_json::json!({
                "key": W::<K, MK>::new(key),
                "value": W::<V, MV>::new(value),
                "diff": diff,
            }))
        }))
    }

    fn answer(facade: &GroupRead<'_, K, V>, call: &Call) -> Result<Value, Problem> {
        let p: ListParams = call.query()?;
        let limit = limit(p.limit, DEFAULT_LIMIT)?;
        match call.op {
            "list" => {
                let after = p
                    .after
                    .as_deref()
                    .map(decode_cursor::<(W<K, MK>, W<V, MV>)>)
                    .transpose()?
                    .map(|(k, v)| (k.into_inner(), v.into_inner()));
                // Resume at the cursor's key, then skip its values up to
                // and including the cursor's: a seek to the key, then a
                // scan of that one group.
                let rows: Box<dyn Iterator<Item = fold3::Result<(K, V, Delta)>>> =
                    match (p.order, after) {
                        (Order::Asc, None) => Box::new(facade.iter()),
                        (Order::Asc, Some((ck, cv))) => Box::new(
                            facade
                                .range((Bound::Included(ck.clone()), Bound::Unbounded))
                                .skip_while(move |r| {
                                    r.as_ref().is_ok_and(|(k, v, _)| *k == ck && *v <= cv)
                                }),
                        ),
                        (Order::Desc, None) => Box::new(facade.range_rev(..)),
                        (Order::Desc, Some((ck, cv))) => Box::new(
                            facade
                                .range_rev((Bound::Unbounded, Bound::Included(ck.clone())))
                                .skip_while(move |r| {
                                    r.as_ref().is_ok_and(|(k, v, _)| *k == ck && *v >= cv)
                                }),
                        ),
                    };
                let rows = rows.map(|r| {
                    r.map(|(key, value, count)| GroupRow {
                        key: W::<K, MK>::new(key),
                        value: W::<V, MV>::new(value),
                        count,
                    })
                });
                to_json(&paginate_results(rows, limit, |r| {
                    (r.key.clone(), r.value.clone())
                })?)
            }
            "values" => {
                let key = call.key::<W<K, MK>>()?.into_inner();
                let after = p
                    .after
                    .as_deref()
                    .map(decode_cursor::<W<V, MV>>)
                    .transpose()?
                    .map(W::into_inner);
                let rows: Box<dyn Iterator<Item = fold3::Result<(V, Delta)>>> =
                    match (p.order, after) {
                        (Order::Asc, None) => Box::new(facade.values(&key)),
                        (Order::Asc, Some(cv)) => Box::new(
                            facade
                                .values(&key)
                                .skip_while(move |r| r.as_ref().is_ok_and(|(v, _)| *v <= cv)),
                        ),
                        (Order::Desc, None) => Box::new(facade.values_rev(&key)),
                        (Order::Desc, Some(cv)) => Box::new(
                            facade
                                .values_rev(&key)
                                .skip_while(move |r| r.as_ref().is_ok_and(|(v, _)| *v >= cv)),
                        ),
                    };
                let rows = rows.map(|r| {
                    r.map(|(value, count)| Counted {
                        value: W::<V, MV>::new(value),
                        count,
                    })
                });
                to_json(&paginate_results(rows, limit, |c| c.value.clone())?)
            }
            _ => Err(call.unrouted()),
        }
    }
}

// ---------------------------------------------------------------------------
// ValueView
// ---------------------------------------------------------------------------

impl<V: 'static> Shape for ValueView<V> {
    type Key = ();
    type Value = V;
}

impl<V: Data, MK: 'static, MV: Via<V>> Resource<(MK, MV)> for ValueView<V> {
    type Facade<'a> = ValueRead<'a, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "value_view", schema_in::<W<V, MV>>(g)).route(
            Route::get("", "get", "Get the value: `null` until the view has one")
                .response::<Option<W<V, MV>>>(g)
                .live(),
        )
    }

    fn live(facade: &ValueRead<'_, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), value_change::<V, MV>))
    }

    fn answer(facade: &ValueRead<'_, V>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "get" => to_json(&read(facade.get())?.map(W::<V, MV>::new)),
            _ => Err(call.unrouted()),
        }
    }
}

// ---------------------------------------------------------------------------
// SearchView
// ---------------------------------------------------------------------------

/// Query parameters of a search.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SearchParams {
    /// The query text; any term may match (BM25).
    pub q: String,
    /// How many hits, 1 to 1000; 10 by default.
    pub k: Option<usize>,
}

/// Query parameters of a completion.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CompleteParams {
    /// The prefix to complete.
    pub prefix: String,
    /// How many terms, 1 to 1000; 10 by default.
    pub k: Option<usize>,
}

/// One search result.
#[derive(Debug, Clone, serde::Serialize, JsonSchema)]
#[schemars(rename = "Hit_{D}")]
pub struct Hit<D> {
    /// The matching document.
    pub key: D,
    pub score: f64,
}

/// One completion: a term and how many documents contain it.
#[derive(Debug, Clone, serde::Serialize, JsonSchema)]
pub struct Completion {
    pub term: String,
    pub count: usize,
}

const DEFAULT_K: usize = 10;

impl<D: 'static, T> Shape for SearchView<D, T> {
    type Key = D;
    type Value = ();
}

impl<D: Data, T: Tokenizer, MK: Via<D>, MV: 'static> Resource<(MK, MV)> for SearchView<D, T> {
    type Facade<'a> = SearchRead<'a, D, T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "search_view", schema_in::<W<D, MK>>(g))
            .route(
                Route::get("", "search", "Full-text search, best first")
                    .query::<SearchParams>()
                    .response::<Vec<Hit<W<D, MK>>>>(g),
            )
            .route(
                Route::get(
                    "/complete",
                    "complete",
                    "Complete a term, most common first",
                )
                .query::<CompleteParams>()
                .response::<Vec<Completion>>(g),
            )
    }

    fn answer(facade: &SearchRead<'_, D, T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "search" => {
                let p: SearchParams = call.query()?;
                let k = limit(p.k, DEFAULT_K)?;
                let hits: Vec<_> = read(facade.search(&p.q, k))?
                    .into_iter()
                    .map(|(key, score)| Hit {
                        key: W::<D, MK>::new(key),
                        score,
                    })
                    .collect();
                to_json(&hits)
            }
            "complete" => {
                let p: CompleteParams = call.query()?;
                let k = limit(p.k, DEFAULT_K)?;
                let terms: Vec<_> = read(facade.complete(&p.prefix, k))?
                    .into_iter()
                    .map(|(term, count)| Completion { term, count })
                    .collect();
                to_json(&terms)
            }
            _ => Err(call.unrouted()),
        }
    }
}
