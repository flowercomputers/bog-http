//! The wire contract shared by every resource: the response envelope,
//! key, query, and body decoding, keyset pagination, and the response
//! shapes resources share.

use std::ops::Bound;

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fold::Diff;
use fold::schema::Data;
use fold::storage::Arrangement;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Problem;

/// The response header carrying the epoch of the snapshot a response was
/// read at, or the epoch a write committed into.
pub const EPOCH_HEADER: &str = "fold-epoch";

/// The default page size of a listing.
pub const DEFAULT_LIMIT: usize = 100;
/// The largest page a listing, or a search's `k`, may ask for.
pub const MAX_LIMIT: usize = 1000;

/// Every successful response body.
///
/// `epoch` is the snapshot's frontier: the number of transactions it
/// reflects. A write's epoch is the first snapshot that includes it, so a
/// read reporting `epoch >= n` has seen every write that reported `n`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub epoch: u64,
    pub data: T,
}

pub(crate) fn respond(status: StatusCode, epoch: u64, data: Value) -> Response {
    let mut res = (status, axum::Json(Envelope { epoch, data })).into_response();
    res.headers_mut()
        .insert(EPOCH_HEADER, HeaderValue::from(epoch));
    res
}

/// Decode a key from one path segment (already percent-decoded).
///
/// The segment is read as JSON first, so `42`, `true`, and `[1,"a"]`
/// address numeric, boolean, and tuple keys; if that fails it is read
/// as a bare JSON string, so `/users/alice` addresses the key `"alice"`
/// and `/users/42` still addresses `"42"` in a string-keyed table.
pub fn parse_key<K: DeserializeOwned>(segment: &str) -> Result<K, Problem> {
    if let Ok(key) = serde_json::from_str(segment) {
        return Ok(key);
    }
    serde_json::from_value(Value::String(segment.to_owned()))
        .map_err(|e| Problem::bad_request(format!("invalid key {segment:?}: {e}")))
}

/// Decode a JSON request body.
pub fn parse_body<T: DeserializeOwned>(body: &[u8]) -> Result<T, Problem> {
    serde_json::from_slice(body).map_err(|e| Problem::bad_request(format!("invalid body: {e}")))
}

/// Decode a query string (absent means empty).
pub fn parse_query<Q: DeserializeOwned>(query: Option<&str>) -> Result<Q, Problem> {
    serde_urlencoded::from_str(query.unwrap_or(""))
        .map_err(|e| Problem::bad_request(format!("invalid query: {e}")))
}

/// Refuse a body that doesn't declare itself JSON (`application/json`
/// or any `+json` type).
pub(crate) fn require_json(headers: &HeaderMap) -> Result<(), Problem> {
    let json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .map(|m| {
            let m = m.trim();
            m.eq_ignore_ascii_case("application/json")
                || (m.len() > 5 && m[m.len() - 5..].eq_ignore_ascii_case("+json"))
        })
        .unwrap_or(false);
    if json {
        Ok(())
    } else {
        Err(Problem::unsupported_media_type())
    }
}

pub fn to_json<T: Serialize>(value: &T) -> Result<Value, Problem> {
    serde_json::to_value(value).map_err(|e| Problem::internal(format!("serialize: {e}")))
}

/// Iteration order of a listing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

/// Query parameters of a listing.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Page size, 1 to 1000; 100 by default.
    pub limit: Option<usize>,
    /// The `next` cursor of the previous page.
    pub after: Option<String>,
    /// `asc` (default) or `desc`.
    #[serde(default)]
    pub order: Order,
}

/// A page size from a request: `None` is the default, out of range is
/// refused.
pub fn limit(requested: Option<usize>, default: usize) -> Result<usize, Problem> {
    match requested {
        None => Ok(default),
        Some(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
        Some(n) => Err(Problem::bad_request(format!(
            "limit must be between 1 and {MAX_LIMIT}, got {n}"
        ))),
    }
}

/// An opaque position in a listing: the last row served, as base64url
/// JSON. Opaque to clients so the encoding can change.
pub fn encode_cursor<C: Serialize>(position: &C) -> Result<String, Problem> {
    let json =
        serde_json::to_vec(position).map_err(|e| Problem::internal(format!("cursor: {e}")))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

pub fn decode_cursor<C: DeserializeOwned>(cursor: &str) -> Result<C, Problem> {
    URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|json| serde_json::from_slice(&json).ok())
        .ok_or_else(|| Problem::bad_request("invalid cursor"))
}

/// A page of a listing. `next` is the cursor for the following page,
/// absent on the last.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "Page_{T}")]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<String>,
}

/// Take up to `limit` rows, and a cursor (from `position` of the last
/// one) if more remain.
///
/// Keyset pagination: the next page resumes strictly after the cursor's
/// position, so pages stay stable under concurrent writes and skipping
/// to a page costs nothing.
pub fn paginate<T, C: Serialize>(
    mut rows: impl Iterator<Item = T>,
    limit: usize,
    position: impl Fn(&T) -> C,
) -> Result<Page<T>, Problem> {
    let mut items = Vec::with_capacity(limit.min(64));
    for row in rows.by_ref() {
        items.push(row);
        if items.len() == limit {
            break;
        }
    }
    let next = match items.last() {
        Some(last) if items.len() == limit && rows.next().is_some() => {
            Some(encode_cursor(&position(last))?)
        }
        _ => None,
    };
    Ok(Page { items, next })
}

/// An arrangement's rows strictly after `after` in `order`.
pub fn arranged<'a, T: Data>(
    arrangement: &'a Arrangement<T>,
    order: Order,
    after: Option<T>,
) -> Box<dyn Iterator<Item = (T, Diff)> + 'a> {
    match (order, after) {
        (Order::Asc, None) => Box::new(arrangement.iter()),
        (Order::Asc, Some(t)) => {
            Box::new(arrangement.range((Bound::Excluded(t), Bound::Unbounded)))
        }
        (Order::Desc, None) => Box::new(arrangement.iter_rev()),
        (Order::Desc, Some(t)) => Box::new(arrangement.range_rev(..t)),
    }
}

/// A keyed row.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "Row_{K}_{V}")]
pub struct Row<K, V> {
    pub key: K,
    pub value: V,
}

/// A value with its multiplicity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "Counted_{T}")]
pub struct Counted<T> {
    pub value: T,
    pub count: Diff,
}

/// One value of a key in a grouped view, with its multiplicity.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "GroupRow_{K}_{V}")]
pub struct GroupRow<K, V> {
    pub key: K,
    pub value: V,
    pub count: Diff,
}

/// The key a write addressed or minted.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "KeyOf_{K}")]
pub struct KeyOf<K> {
    pub key: K,
}

/// Whether a delete found something to remove.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct Removed {
    pub removed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_parse_as_json_then_as_strings() {
        assert_eq!(parse_key::<u64>("42").unwrap(), 42);
        assert_eq!(parse_key::<String>("alice").unwrap(), "alice");
        assert_eq!(parse_key::<String>("42").unwrap(), "42");
        assert_eq!(parse_key::<String>("\"quoted\"").unwrap(), "quoted");
        assert_eq!(
            parse_key::<(u8, String)>("[1,\"a\"]").unwrap(),
            (1, "a".into())
        );
        assert_eq!(parse_key::<u64>("nope").unwrap_err().status, 400);
    }

    #[test]
    fn cursors_round_trip_and_reject_garbage() {
        let c = encode_cursor(&("k", 7u32)).unwrap();
        assert_eq!(decode_cursor::<(String, u32)>(&c).unwrap(), ("k".into(), 7));
        assert_eq!(decode_cursor::<u64>("!!").unwrap_err().status, 400);
    }

    #[test]
    fn pages_end_exactly_at_the_last_row() {
        let page = paginate(1..=4, 2, |n| *n).unwrap();
        assert_eq!(page.items, [1, 2]);
        assert_eq!(decode_cursor::<i32>(&page.next.unwrap()).unwrap(), 2);
        let page = paginate(1..=4, 4, |n| *n).unwrap();
        assert_eq!((page.items.len(), page.next), (4, None));
        let page = paginate(0..0, 4, |n| *n).unwrap();
        assert!(page.items.is_empty() && page.next.is_none());
    }

    #[test]
    fn queries_decode_with_defaults() {
        let p: ListParams = parse_query(None).unwrap();
        assert_eq!((p.limit, p.order), (None, Order::Asc));
        let p: ListParams = parse_query(Some("limit=5&order=desc")).unwrap();
        assert_eq!((p.limit, p.order), (Some(5), Order::Desc));
        assert!(parse_query::<ListParams>(Some("bogus=1")).is_err());
        assert!(parse_query::<ListParams>(Some("limit=x")).is_err());
    }

    #[test]
    fn json_content_types() {
        let mut h = HeaderMap::new();
        assert!(require_json(&h).is_err());
        for ok in [
            "application/json",
            "Application/JSON; charset=utf-8",
            "application/merge-patch+json",
        ] {
            h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ok));
            assert!(require_json(&h).is_ok(), "{ok}");
        }
        h.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/plain"));
        assert!(require_json(&h).is_err());
    }
}
