//! Views: `MapView`, `OrderedView`, `GroupView`, `ValueView`,
//! `SearchView`. All read-only.

use std::ops::Bound;

use fold::schema::{Data, GroupView, MapView, OrderedView, SearchView, Tokenizer, ValueView};
use fold::{GroupRead, MapRead, OrderedRead, SearchRead, ValueRead};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::Value;

use super::{answer_keyed, get_route, keyed_change, list_keyed, list_route, value_change};
use crate::resource::{Call, Describe, Live, Resource, Route, schema_in};
use crate::wire::{
    Counted, DEFAULT_LIMIT, GroupRow, ListParams, Order, Page, Row, arranged, decode_cursor, limit,
    paginate, parse_key, to_json,
};
use crate::{Problem, Wire};

// ---------------------------------------------------------------------------
// MapView
// ---------------------------------------------------------------------------

impl<K: Data + Wire, V: Data + Wire> Resource for MapView<K, V> {
    type Facade = MapRead<K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "map_view", schema_in::<V>(g))
            .keyed(schema_in::<K>(g))
            .route(list_route::<K, V>(g))
            .route(get_route::<K, V>(g))
    }

    fn answer(facade: &MapRead<K, V>, call: &Call) -> Result<Value, Problem> {
        answer_keyed(facade, call)
    }

    fn live(facade: &MapRead<K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
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

impl<K: Data + Wire, V: Data + Wire> Resource for OrderedView<K, V> {
    type Facade = OrderedRead<K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "ordered_view", schema_in::<V>(g))
            .keyed(schema_in::<K>(g))
            .route(
                Route::get("", "list", "List rows by key, or by value with `by=value`")
                    .query::<OrderedParams>()
                    .response::<Page<Row<K, V>>>(g)
                    .live(),
            )
            .route(get_route::<K, V>(g))
    }

    fn live(facade: &OrderedRead<K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
    }

    fn answer(facade: &OrderedRead<K, V>, call: &Call) -> Result<Value, Problem> {
        if call.op != "list" {
            return answer_keyed(facade, call);
        }
        let p: OrderedParams = call.query()?;
        if p.by == By::Key {
            if p.min.is_some() || p.max.is_some() {
                return Err(Problem::bad_request("min and max need by=value"));
            }
            return list_keyed(facade, p.order, p.limit, p.after.as_deref());
        }
        let limit = limit(p.limit, DEFAULT_LIMIT)?;
        let bound = |v: Option<&String>| -> Result<Bound<V>, Problem> {
            Ok(match v {
                Some(json) => Bound::Included(parse_key(json)?),
                None => Bound::Unbounded,
            })
        };
        let (min, max) = (bound(p.min.as_ref())?, bound(p.max.as_ref())?);
        let after: Option<(V, K)> = p.after.as_deref().map(decode_cursor).transpose()?;
        // Resume at the cursor's value, then skip its ties up to and
        // including the cursor's key: a seek, not a scan from the start.
        let rows: Box<dyn Iterator<Item = (K, V)> + '_> = match (p.order, after) {
            (Order::Asc, None) => Box::new(facade.by_value((min, max))),
            (Order::Asc, Some((cv, ck))) => Box::new(
                facade
                    .by_value((Bound::Included(cv.clone()), max))
                    .skip_while(move |(k, v)| *v == cv && *k <= ck),
            ),
            (Order::Desc, None) => Box::new(facade.by_value_rev((min, max))),
            (Order::Desc, Some((cv, ck))) => Box::new(
                facade
                    .by_value_rev((min, Bound::Included(cv.clone())))
                    .skip_while(move |(k, v)| *v == cv && *k >= ck),
            ),
        };
        let rows = rows.map(|(key, value)| Row { key, value });
        to_json(&paginate(rows, limit, |r| {
            (r.value.clone(), r.key.clone())
        })?)
    }
}

// ---------------------------------------------------------------------------
// GroupView
// ---------------------------------------------------------------------------

impl<K: Data + Wire, V: Data + Wire> Resource for GroupView<K, V> {
    type Facade = GroupRead<K, V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "group_view", schema_in::<V>(g))
            .keyed(schema_in::<K>(g))
            .route(
                Route::get("", "list", "List every (key, value) pair, with counts")
                    .query::<ListParams>()
                    .response::<Page<GroupRow<K, V>>>(g)
                    .live(),
            )
            .route(
                Route::get("/{key}", "values", "List a key's values, with counts")
                    .key::<K>(g)
                    .query::<ListParams>()
                    .response::<Page<Counted<V>>>(g),
            )
    }

    fn live(facade: &GroupRead<K, V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), keyed_change))
    }

    fn answer(facade: &GroupRead<K, V>, call: &Call) -> Result<Value, Problem> {
        let p: ListParams = call.query()?;
        let limit = limit(p.limit, DEFAULT_LIMIT)?;
        let arrangement = facade.arrangement();
        match call.op {
            "list" => {
                let after: Option<(K, V)> = p.after.as_deref().map(decode_cursor).transpose()?;
                let rows = arranged(arrangement, p.order, after)
                    .map(|((key, value), count)| GroupRow { key, value, count });
                to_json(&paginate(rows, limit, |r| {
                    (r.key.clone(), r.value.clone())
                })?)
            }
            "values" => {
                let key: K = call.key()?;
                let after: Option<V> = p.after.as_deref().map(decode_cursor).transpose()?;
                let rows: Box<dyn Iterator<Item = ((K, V), fold::Diff)> + '_> =
                    match (p.order, after) {
                        (Order::Asc, None) => Box::new(arrangement.scan_from(|(k, _)| *k < key)),
                        (Order::Asc, Some(cv)) => {
                            Box::new(arrangement.scan_from(|(k, v)| (k, v) <= (&key, &cv)))
                        }
                        (Order::Desc, None) => {
                            Box::new(arrangement.iter_rev_from(|(k, _)| *k <= key))
                        }
                        (Order::Desc, Some(cv)) => {
                            Box::new(arrangement.iter_rev_from(|(k, v)| (k, v) < (&key, &cv)))
                        }
                    };
                let rows = rows
                    .take_while(|((k, _), _)| *k == key)
                    .map(|((_, value), count)| Counted { value, count });
                to_json(&paginate(rows, limit, |c| c.value.clone())?)
            }
            _ => Err(call.unrouted()),
        }
    }
}

// ---------------------------------------------------------------------------
// ValueView
// ---------------------------------------------------------------------------

impl<V: Data + Wire> Resource for ValueView<V> {
    type Facade = ValueRead<V>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "value_view", schema_in::<V>(g)).route(
            Route::get("", "get", "Get the value: `null` until the view has one")
                .response::<Option<V>>(g)
                .live(),
        )
    }

    fn live(facade: &ValueRead<V>) -> Option<Live> {
        Some(Live::new(facade.subscribe(), |((), v), d| {
            value_change(v, d)
        }))
    }

    fn answer(facade: &ValueRead<V>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "get" => to_json(&facade.get()),
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

impl<D: Data + Wire, T: Tokenizer> Resource for SearchView<D, T> {
    type Facade = SearchRead<D, T>;

    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe {
        Describe::new(name, "search_view", schema_in::<D>(g))
            .route(
                Route::get("", "search", "Full-text search, best first")
                    .query::<SearchParams>()
                    .response::<Vec<Hit<D>>>(g),
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

    fn answer(facade: &SearchRead<D, T>, call: &Call) -> Result<Value, Problem> {
        match call.op {
            "search" => {
                let p: SearchParams = call.query()?;
                let k = limit(p.k, DEFAULT_K)?;
                let hits: Vec<_> = facade
                    .search(&p.q, k)
                    .into_iter()
                    .map(|(key, score)| Hit { key, score })
                    .collect();
                to_json(&hits)
            }
            "complete" => {
                let p: CompleteParams = call.query()?;
                let k = limit(p.k, DEFAULT_K)?;
                let terms: Vec<_> = facade
                    .complete(&p.prefix, k)
                    .into_iter()
                    .map(|(term, count)| Completion { term, count })
                    .collect();
                to_json(&terms)
            }
            _ => Err(call.unrouted()),
        }
    }
}
