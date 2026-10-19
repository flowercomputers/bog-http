//! [`Problem`]: every error response, as RFC 9457 `application/problem+json`.

use std::borrow::Cow;

use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use fold::Rejected;
use serde::Serialize;

/// The media type of every error body.
pub const PROBLEM_JSON: &str = "application/problem+json";

/// An error response: an HTTP status plus a machine-readable `type`.
///
/// `type` is a stable identifier clients can branch on; `title` is a
/// short human summary of that type; `detail` explains this occurrence.
/// Refusals carry `at`, the source or view that refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Problem {
    #[serde(rename = "type")]
    pub kind: Cow<'static, str>,
    pub title: Cow<'static, str>,
    pub status: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
}

impl Problem {
    pub fn new(status: StatusCode, kind: &'static str, title: &'static str) -> Self {
        Problem {
            kind: Cow::Borrowed(kind),
            title: Cow::Borrowed(title),
            status: status.as_u16(),
            detail: None,
            at: None,
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

    /// The store could not complete the operation (I/O, a dead writer).
    pub fn unavailable(detail: impl Into<String>) -> Self {
        Problem::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Service unavailable",
        )
        .with_detail(detail)
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
        res
    }
}

/// How a schema's transaction error becomes a response.
///
/// Implemented for [`Rejected`]; a schema with its own
/// [`Declare::Error`](fold::Declare::Error) implements it to choose its
/// statuses.
pub trait IntoProblem {
    fn into_problem(self) -> Problem;
}

impl IntoProblem for Rejected {
    fn into_problem(self) -> Problem {
        // `From<io::Error>` files WAL failures under `at = "io"`: the
        // server's fault, not the request's.
        if self.at == "io" {
            Problem::unavailable(self.reason)
        } else {
            Problem::rejected(self.at, self.reason)
        }
    }
}

impl IntoProblem for Problem {
    fn into_problem(self) -> Problem {
        self
    }
}
