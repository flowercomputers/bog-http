//! Typed custom routes: application endpoints that read a snapshot or run
//! a transaction, documented in OpenAPI beside the generated ones.

use std::sync::Arc;

use fold::{Declare, Schema, Tx};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Deserialize;
use serde_json::Value;

use crate::Problem;
use crate::resource::{Call, Route, Written};

/// The query (or body) of a custom route that takes none: `{}`, and
/// nothing else.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NoParams {}

/// A whole transaction, staged: runs once on the writer thread.
pub(crate) type TxFn<S> =
    Box<dyn for<'a> FnOnce(&mut Tx<'a, S>) -> Result<Written, <S as Declare>::Error> + Send>;

pub(crate) type ReadHandler<S> =
    Arc<dyn Fn(&<S as Schema>::Read, &Call) -> Result<Value, Problem> + Send + Sync>;

/// Decodes a request into a [`TxFn`] on the request's thread.
pub(crate) type WriteHandler<S> = Arc<dyn Fn(&Call) -> Result<TxFn<S>, Problem> + Send + Sync>;

pub(crate) enum Handler<S: Declare> {
    Read(ReadHandler<S>),
    Write(WriteHandler<S>),
}

pub(crate) struct Custom<S: Declare> {
    pub(crate) path: String,
    pub(crate) describe: Box<dyn Fn(&mut SchemaGenerator) -> Route + Send + Sync>,
    pub(crate) handler: Handler<S>,
}

/// The op every custom route carries; its OpenAPI `operationId` comes
/// from its method and path instead.
pub(crate) const CUSTOM_OP: &str = "custom";

pub(crate) fn check_path(path: &str) {
    assert!(
        path.starts_with('/') && path.len() > 1,
        "custom route {path:?} must start with '/'"
    );
    assert!(
        !path.starts_with("/_") && path != "/openapi.json",
        "custom route {path:?} is in fold-http's reserved namespace"
    );
    assert!(
        !path.contains(['{', '}', '*', '?', '#']),
        "custom route {path:?}: path parameters aren't supported; use the query or body"
    );
}
