//! Who may call: a bearer token, and which `Host` names are accepted.
//!
//! Both matter even on `localhost`. Any process on the machine can
//! connect to a loopback port, and any web page can make the browser
//! send requests to one: same-origin rules stop it reading the
//! answers, unless DNS rebinding points the page's own domain at
//! `127.0.0.1`. Then it's same-origin, but its `Host` header still names
//! the attacker's domain, which the host check refuses.

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::Problem;

/// The query parameter a token may travel in, for clients that can't set
/// headers (`EventSource`). Removed before routing.
pub const ACCESS_TOKEN_PARAM: &str = "access_token";

/// Requests to these answer without a token: liveness probes.
const OPEN_PATHS: &[&str] = &["/_health"];

#[derive(Clone, Default)]
pub(crate) struct Guard {
    pub(crate) token: Option<Arc<str>>,
    /// Lowercase host names (no port); `None` accepts any.
    pub(crate) hosts: Option<Arc<[String]>>,
}

impl Guard {
    pub(crate) fn is_open(&self) -> bool {
        self.token.is_none() && self.hosts.is_none()
    }
}

/// The host names a loopback-bound server answers to by default.
#[cfg(any(feature = "serve", test))]
pub(crate) fn loopback_hosts() -> Vec<String> {
    ["localhost", "127.0.0.1", "[::1]", "::1"]
        .iter()
        .map(|h| h.to_string())
        .collect()
}

pub(crate) async fn guard(State(g): State<Guard>, mut req: Request, next: Next) -> Response {
    if let Some(hosts) = &g.hosts
        && !req
            .headers()
            .get(header::HOST)
            .is_some_and(|host| host_allowed(host, hosts))
    {
        return Problem::new(
            StatusCode::FORBIDDEN,
            "host-not-allowed",
            "Host not allowed",
        )
        .with_detail("this server doesn't answer to that host name")
        .into_response();
    }
    let Some(token) = &g.token else {
        return next.run(req).await;
    };
    let (presented, uri) = take_token(req.uri());
    let presented = presented.or_else(|| {
        req.headers()
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split_once(' '))
            .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
            .map(|(_, t)| t.trim().to_owned())
    });
    if let Some(uri) = uri {
        *req.uri_mut() = uri;
    }
    let open = OPEN_PATHS.contains(&req.uri().path());
    match presented {
        Some(p) if same(p.as_bytes(), token.as_bytes()) => next.run(req).await,
        _ if open => next.run(req).await,
        presented => {
            let mut res = Problem::new(StatusCode::UNAUTHORIZED, "unauthorized", "Unauthorized")
                .with_detail(if presented.is_some() {
                    "that token isn't valid here"
                } else {
                    "send `Authorization: Bearer <token>`, or `?access_token=` where headers can't be set"
                })
                .into_response();
            res.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            res
        }
    }
}

fn host_allowed(host: &HeaderValue, hosts: &[String]) -> bool {
    let Ok(host) = host.to_str() else {
        return false;
    };
    let host = host.trim().to_ascii_lowercase();
    // Strip a port: `[::1]:80` keeps its brackets, `a:80` loses `:80`.
    let name = match host.rfind(':') {
        Some(i) if !host[i..].contains(']') && host[..i].matches(':').count() == 0 => &host[..i],
        Some(i) if host.starts_with('[') && host[..i].ends_with(']') => &host[..i],
        _ => host.as_str(),
    };
    hosts.iter().any(|h| h == name)
}

/// The `access_token` query parameter, and the URI without it.
fn take_token(uri: &Uri) -> (Option<String>, Option<Uri>) {
    let Some(query) = uri.query() else {
        return (None, None);
    };
    let mut token = None;
    let rest: Vec<(String, String)> = form_pairs(query)
        .into_iter()
        .filter(|(k, v)| {
            if k == ACCESS_TOKEN_PARAM {
                token = Some(v.clone());
                false
            } else {
                true
            }
        })
        .collect();
    if token.is_none() {
        return (None, None);
    }
    let mut path_and_query = uri.path().to_owned();
    if !rest.is_empty() {
        path_and_query.push('?');
        path_and_query
            .push_str(&serde_urlencoded::to_string(&rest).expect("pairs of strings encode"));
    }
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = path_and_query.parse().ok();
    (token, Uri::from_parts(parts).ok())
}

fn form_pairs(query: &str) -> Vec<(String, String)> {
    serde_urlencoded::from_str(query).unwrap_or_default()
}

/// Compare without leaking, through timing, how much of a guess was
/// right.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosts_match_without_their_ports() {
        let hosts = loopback_hosts();
        for ok in [
            "localhost",
            "LOCALHOST:10777",
            "127.0.0.1:80",
            "[::1]:8080",
            "[::1]",
        ] {
            assert!(host_allowed(&HeaderValue::from_static(ok), &hosts), "{ok}");
        }
        for bad in [
            "evil.example",
            "evil.example:10777",
            "localhost.evil.example",
            "127.0.0.2",
        ] {
            assert!(
                !host_allowed(&HeaderValue::from_static(bad), &hosts),
                "{bad}"
            );
        }
    }

    #[test]
    fn the_token_parameter_is_taken_out_of_the_query() {
        let uri: Uri = "/messages?order=desc&access_token=s3cr%2Bt&limit=5"
            .parse()
            .unwrap();
        let (token, rest) = take_token(&uri);
        assert_eq!(token.as_deref(), Some("s3cr+t"));
        assert_eq!(rest.unwrap().to_string(), "/messages?order=desc&limit=5");
        let (token, rest) = take_token(&"/x?access_token=t".parse().unwrap());
        assert_eq!(
            (token.as_deref(), rest.unwrap().to_string().as_str()),
            (Some("t"), "/x")
        );
        assert_eq!(take_token(&"/x?a=1".parse().unwrap()), (None, None));
    }

    #[test]
    fn tokens_compare_in_full() {
        assert!(same(b"abc", b"abc"));
        assert!(!same(b"abc", b"abd") && !same(b"abc", b"ab") && !same(b"", b"a"));
    }
}
