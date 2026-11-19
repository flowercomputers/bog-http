//! [`Problem`]: every error response, as RFC 9457 `application/problem+json`.

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bog::Rejected;
use schemars::JsonSchema;
use serde::Serialize;

/// The media type of every error body.
pub const PROBLEM_JSON: &str = "application/problem+json";

/// An error response: an HTTP status plus a machine-readable `type`.
///
/// `type` is a stable identifier clients can branch on; `title` is a
/// short human summary of that type; `detail` explains this occurrence.
/// Refusals carry `at`, the source or view that refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
pub struct Problem {
    /// Stable, machine-readable: `bad-request`, `not-found`,
    /// `method-not-allowed`, `unsupported-media-type`, `rejected`,
    /// `overloaded`, `epoch-not-reached`, `unavailable`, `internal`.
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub title: &'static str,
    pub status: u16,
    /// What went wrong this time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    /// For `rejected`: the source or view that refused.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// For `/_tx`: the index of the operation at fault.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<usize>,
    /// Seconds a client should wait before retrying: the `Retry-After`
    /// header, not part of the body.
    #[serde(skip)]
    #[schemars(skip)]
    pub retry_after: Option<u32>,
}

impl Problem {
    pub fn new(status: StatusCode, kind: &'static str, title: &'static str) -> Self {
        Problem {
            kind,
            title,
            status: status.as_u16(),
            detail: None,
            at: None,
            index: None,
            retry_after: None,
        }
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_at(mut self, at: impl Into<String>) -> Self {
        self.at = Some(at.into());
        self
    }

    pub fn with_index(mut self, index: usize) -> Self {
        self.index = Some(index);
        self
    }

    pub fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after = Some(seconds);
        self
    }

    pub fn status(&self) -> StatusCode {
        StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// A malformed key, query, or body.
    pub fn bad_request(detail: impl Into<String>) -> Self {
        Problem::new(StatusCode::BAD_REQUEST, "bad-request", "Bad request").with_detail(detail)
    }

    pub fn not_found(detail: impl Into<String>) -> Self {
        Problem::new(StatusCode::NOT_FOUND, "not-found", "Not found").with_detail(detail)
    }

    pub fn method_not_allowed() -> Self {
        Problem::new(
            StatusCode::METHOD_NOT_ALLOWED,
            "method-not-allowed",
            "Method not allowed",
        )
    }

    /// The client accepts only what this route can't produce (a stream
    /// from a route that doesn't stream).
    pub fn not_acceptable(detail: impl Into<String>) -> Self {
        Problem::new(
            StatusCode::NOT_ACCEPTABLE,
            "not-acceptable",
            "Not acceptable",
        )
        .with_detail(detail)
    }

    pub fn unsupported_media_type() -> Self {
        Problem::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported-media-type",
            "Unsupported media type",
        )
        .with_detail("request bodies must be application/json")
    }

    /// A schema constraint refused the write.
    pub fn rejected(at: impl Into<String>, reason: impl Into<String>) -> Self {
        Problem::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "rejected",
            "Write rejected",
        )
        .with_at(at)
        .with_detail(reason)
    }

    /// The store could not complete the operation (I/O, a poisoned store).
    pub fn unavailable(detail: impl Into<String>) -> Self {
        Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Service unavailable",
        )
        .with_detail(detail)
    }

    /// Too many writes already queued: back off and retry.
    pub fn overloaded() -> Self {
        Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "overloaded",
            "Too many pending writes",
        )
        .with_retry_after(1)
    }

    /// A `bog-min-epoch` read whose epoch didn't arrive in time.
    pub fn epoch_not_reached(wanted: u64, current: u64) -> Self {
        Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "epoch-not-reached",
            "Epoch not reached",
        )
        .with_detail(format!("asked for epoch {wanted}, at {current}"))
        .with_retry_after(1)
    }

    pub fn internal(detail: impl Into<String>) -> Self {
        Problem::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "Internal server error",
        )
        .with_detail(detail)
    }
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.status, self.title)?;
        if let Some(detail) = &self.detail {
            write!(f, ": {detail}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Problem {}

impl IntoResponse for Problem {
    fn into_response(self) -> Response {
        let body = serde_json::to_vec(&self).expect("a Problem always serializes");
        let mut res = (self.status(), body).into_response();
        res.headers_mut()
            .insert(header::CONTENT_TYPE, HeaderValue::from_static(PROBLEM_JSON));
        if let Some(seconds) = self.retry_after {
            res.headers_mut()
                .insert(header::RETRY_AFTER, HeaderValue::from(seconds));
        }
        res
    }
}

/// How a bog error becomes a response.
///
/// A refusal ([`Error::Rejected`](bog::Error::Rejected)) answers by
/// what it carries: a [`Rejected`] is a `422` naming the source or view
/// in `at`; a [`Problem`] is itself, so a write closure chooses its own
/// status with `Error::reject(Problem::…)`; any other error is a `422`
/// with its message. Everything else is the store's fault, not the
/// request's: `503`.
impl From<bog::Error> for Problem {
    fn from(e: bog::Error) -> Self {
        use bog::Error;
        if let Some(rejected) = e.rejected::<Rejected>() {
            return Problem::rejected(&rejected.at, &rejected.reason);
        }
        if let Some(problem) = e.rejected::<Problem>() {
            return problem.clone();
        }
        match e {
            Error::Rejected(why) => Problem::new(
                StatusCode::UNPROCESSABLE_ENTITY,
                "rejected",
                "Write rejected",
            )
            .with_detail(why.to_string()),
            Error::OutOfMemory(_) => Problem::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload-too-large",
                "Payload too large",
            )
            .with_detail(e.to_string()),
            // The commit stands; say so rather than invite a retry.
            Error::AfterCommit { .. } => Problem::internal(e.to_string()),
            _ => Problem::unavailable(e.to_string()),
        }
    }
}
