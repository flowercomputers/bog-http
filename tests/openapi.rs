//! The generated OpenAPI document, snapshotted. Re-record with
//! `UPDATE_GOLDEN=1 cargo test --test openapi` and review the diff.

mod common;

use axum::http::{Method, StatusCode};
use common::{Shop, send};
use bog_http::Server;
use serde_json::Value;

const GOLDEN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/golden/openapi.json");

#[tokio::test]
async fn openapi_matches_golden() {
    let app = Server::new(Shop::memory().build().unwrap())
        .title("Shop")
        .version("1.0.0")
        .into_router();
    let res = send(&app, Method::GET, "/openapi.json", None).await;
    assert_eq!(res.status, StatusCode::OK);
    let got = serde_json::to_string_pretty(&res.body).unwrap() + "\n";
    if std::env::var_os("UPDATE_GOLDEN").is_some() {
        std::fs::write(GOLDEN, &got).unwrap();
        return;
    }
    let want = std::fs::read_to_string(GOLDEN).expect("golden missing: run with UPDATE_GOLDEN=1");
    assert!(
        got == want,
        "openapi.json drifted from {GOLDEN}; re-record with UPDATE_GOLDEN=1 and review the diff"
    );
}

/// Every `$ref` resolves, and every mounted route is documented.
#[tokio::test]
async fn openapi_is_closed_and_complete() {
    let app = common::app();
    let doc = send(&app, Method::GET, "/openapi.json", None).await.body;

    fn refs<'a>(v: &'a Value, out: &mut Vec<&'a str>) {
        match v {
            Value::Object(o) => {
                if let Some(Value::String(r)) = o.get("$ref") {
                    out.push(r);
                }
                o.values().for_each(|v| refs(v, out));
            }
            Value::Array(a) => a.iter().for_each(|v| refs(v, out)),
            _ => {}
        }
    }
    let mut all = Vec::new();
    refs(&doc, &mut all);
    assert!(!all.is_empty());
    for r in all {
        let pointer = r.strip_prefix('#').expect("local ref");
        assert!(doc.pointer(pointer).is_some(), "dangling $ref {r}");
    }

    let schema = send(&app, Method::GET, "/_schema", None).await.body;
    for resource in schema["resources"].as_array().unwrap() {
        for route in resource["routes"].as_array().unwrap() {
            let path = format!(
                "/{}{}",
                resource["name"].as_str().unwrap(),
                route["path"].as_str().unwrap()
            );
            let method = route["method"].as_str().unwrap().to_lowercase();
            assert!(
                doc["paths"][&path][&method].is_object(),
                "{method} {path} undocumented"
            );
        }
    }
}
