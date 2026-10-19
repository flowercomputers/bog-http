//! The wire contract shared by every resource: the response envelope,
//! key and body decoding, and keyset pagination.

use std::ops::Bound;

use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use fold::Read;
use fold::schema::Data;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Problem, Wire};

/// The response header carrying the epoch of the snapshot a response was
/// read at, or the epoch a write committed into.
pub const EPOCH_HEADER: &str = "fold-epoch";

/// The default page size of a listing.
pub const DEFAULT_LIMIT: usize = 100;
/// The largest page a listing will return.
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

pub(crate) fn to_json<T: Serialize>(value: &T) -> Result<Value, Problem> {
    serde_json::to_value(value).map_err(|e| Problem::internal(format!("serialize: {e}")))
}

/// Iteration order of a listing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    #[default]
    Asc,
    Desc,
}

/// Query parameters of a listing: `?limit=&after=&order=`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListParams {
    /// Page size, `1..=MAX_LIMIT`; defaults to [`DEFAULT_LIMIT`].
    pub limit: Option<usize>,
    /// The `next` cursor of the previous page.
    pub after: Option<String>,
    #[serde(default)]
    pub order: Order,
}

impl ListParams {
    pub fn limit(&self) -> Result<usize, Problem> {
        match self.limit {
            None => Ok(DEFAULT_LIMIT),
            Some(n) if (1..=MAX_LIMIT).contains(&n) => Ok(n),
            Some(n) => Err(Problem::bad_request(format!(
                "limit must be between 1 and {MAX_LIMIT}, got {n}"
            ))),
        }
    }
}

/// An opaque position in a listing: the last key served, as base64url
/// JSON. Opaque to clients so the encoding can change.
pub fn encode_cursor<K: Serialize>(key: &K) -> Result<String, Problem> {
    let json = serde_json::to_vec(key).map_err(|e| Problem::internal(format!("cursor: {e}")))?;
    Ok(URL_SAFE_NO_PAD.encode(json))
}

pub fn decode_cursor<K: DeserializeOwned>(cursor: &str) -> Result<K, Problem> {
    URL_SAFE_NO_PAD
        .decode(cursor)
        .ok()
        .and_then(|json| serde_json::from_slice(&json).ok())
        .ok_or_else(|| Problem::bad_request("invalid cursor"))
}

/// One page of a key-ordered relation: `{items: [{key, value}], next}`.
///
/// Keyset pagination: a page starts strictly after the cursor's key, so
/// pages stay stable under concurrent writes and cost nothing to skip.
pub fn page<K: Data + Wire, V: Data + Wire>(
    read: &Read<K, V>,
    params: &ListParams,
) -> Result<Value, Problem> {
    let limit = params.limit()?;
    let after = params
        .after
        .as_deref()
        .map(decode_cursor::<K>)
        .transpose()?;
    let rows: Box<dyn Iterator<Item = (K, V)> + '_> = match (params.order, after) {
        (Order::Asc, None) => Box::new(read.iter()),
        (Order::Asc, Some(k)) => Box::new(read.range((Bound::Excluded(k), Bound::Unbounded))),
        (Order::Desc, None) => Box::new(read.iter_rev()),
        (Order::Desc, Some(k)) => Box::new(read.range_rev(..k)),
    };
    let mut items = Vec::with_capacity(limit.min(64));
    let mut last = None;
    let mut more = false;
    for (k, v) in rows {
        if items.len() == limit {
            more = true;
            break;
        }
        items.push(serde_json::json!({ "key": to_json(&k)?, "value": to_json(&v)? }));
        last = Some(k);
    }
    let next = match (more, last) {
        (true, Some(k)) => Some(encode_cursor(&k)?),
        _ => None,
    };
    Ok(serde_json::json!({ "items": items, "next": next }))
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
