//! `POST /_tx`: many writes, across any sources, as one transaction.

use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::resource::{Call, Describe, TxFn, Written};
use crate::{Problem, Serve};

/// The most operations one transaction may carry.
pub const MAX_TX_OPS: usize = 1000;

/// One operation of a transaction: a write route, addressed by resource
/// and op instead of method and path.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TxOp {
    resource: String,
    op: String,
    #[serde(default)]
    key: Option<Value>,
    #[serde(default)]
    query: Option<Map<String, Value>>,
    #[serde(default)]
    body: Option<Value>,
}

/// Find each op's route, decode it exactly as its own route would, and
/// stage them all into one transaction. Any malformed op refuses the
/// whole transaction before the writer sees it.
pub(crate) fn stage<S: Serve>(
    described: &[Describe],
    stage_of: impl Fn(usize, Call) -> Result<TxFn<S>, Problem>,
    ops: Vec<TxOp>,
) -> Result<TxFn<S>, Problem> {
    if ops.is_empty() || ops.len() > MAX_TX_OPS {
        return Err(Problem::bad_request(format!(
            "a transaction carries 1 to {MAX_TX_OPS} operations, got {}",
            ops.len()
        )));
    }
    let mut applies = Vec::with_capacity(ops.len());
    for (index, op) in ops.into_iter().enumerate() {
        let apply = stage_one::<S>(described, &stage_of, op).map_err(|p| p.with_index(index))?;
        applies.push(apply);
    }
    Ok(Box::new(move |tx: &mut S::Write<'_>| {
        let mut results = Vec::with_capacity(applies.len());
        for apply in applies {
            results.push(apply(tx)?.data);
        }
        Ok(Written {
            data: json!({ "results": results }),
            created: false,
        })
    }))
}

fn stage_one<S: Serve>(
    described: &[Describe],
    stage_of: &impl Fn(usize, Call) -> Result<TxFn<S>, Problem>,
    op: TxOp,
) -> Result<TxFn<S>, Problem> {
    let Some((i, d)) = described
        .iter()
        .enumerate()
        .find(|(_, d)| d.name == op.resource)
    else {
        return Err(Problem::bad_request(format!(
            "no resource {:?}",
            op.resource
        )));
    };
    let Some(route) = d
        .routes
        .iter()
        .find(|r| r.op == op.op && !r.method.is_read())
    else {
        return Err(Problem::bad_request(format!(
            "{} has no write op {:?}",
            d.name, op.op
        )));
    };
    let keyed = route.path.contains("{key}");
    if keyed != op.key.is_some() {
        return Err(Problem::bad_request(format!(
            "{}.{} {} a key",
            d.name,
            route.op,
            if keyed { "needs" } else { "takes no" }
        )));
    }
    if route.body.is_some() != op.body.is_some() {
        return Err(Problem::bad_request(format!(
            "{}.{} {} a body",
            d.name,
            route.op,
            if route.body.is_some() {
                "needs"
            } else {
                "takes no"
            }
        )));
    }
    let query = match op.query {
        Some(q) => Some(
            serde_urlencoded::to_string(&q)
                .map_err(|e| Problem::bad_request(format!("invalid query: {e}")))?,
        ),
        None => None,
    };
    let call = Call {
        op: route.op,
        // As JSON text, which `parse_key` reads first: unambiguous for
        // every key type, strings included.
        key: op.key.map(|k| k.to_string()),
        query,
        body: op
            .body
            .map(|b| serde_json::to_vec(&b).unwrap_or_default().into())
            .unwrap_or_default(),
    };
    stage_of(i, call)
}
