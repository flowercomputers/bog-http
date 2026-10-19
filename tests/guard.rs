//! Access control: bearer tokens and the Host check.

mod common;

use axum::http::{Method, StatusCode, header};
use common::{send, send_with, server};
use serde_json::json;

#[tokio::test]
async fn a_token_guards_everything_but_health() {
    let app = server().bearer_token("s3cret").into_router();

    let res = send(&app, Method::GET, "/prefs", None).await;
    assert_eq!(res.status, StatusCode::UNAUTHORIZED);
    assert_eq!(res.body["type"], "unauthorized");
    assert_eq!(res.header(header::WWW_AUTHENTICATE), "Bearer");

    let bad = send_with(
        &app,
        Method::GET,
        "/prefs",
        None,
        &[("authorization", "Bearer nope")],
    )
    .await;
    assert_eq!(bad.status, StatusCode::UNAUTHORIZED);

    let ok = send_with(
        &app,
        Method::PUT,
        "/prefs/a",
        Some(json!(1)),
        &[("authorization", "Bearer s3cret")],
    )
    .await;
    assert_eq!(ok.status, StatusCode::CREATED);

    // Where headers can't be set: the query, which is then removed, so
    // a listing's strict query parsing never sees it.
    let ok = send(
        &app,
        Method::GET,
        "/prefs?limit=5&access_token=s3cret",
        None,
    )
    .await;
    assert_eq!(ok.status, StatusCode::OK, "{:?}", ok.body);
    assert_eq!(ok.data()["items"][0]["key"], "a");

    assert_eq!(
        send(&app, Method::GET, "/_health", None).await.status,
        StatusCode::OK
    );
    assert_eq!(
        send(&app, Method::GET, "/openapi.json", None).await.status,
        StatusCode::UNAUTHORIZED
    );

    let doc = send(&app, Method::GET, "/openapi.json?access_token=s3cret", None)
        .await
        .body;
    assert_eq!(doc["security"], json!([{"bearer": []}]));
    assert_eq!(
        doc["components"]["securitySchemes"]["bearer"]["scheme"],
        "bearer"
    );
    assert_eq!(doc["paths"]["/_health"]["get"]["security"], json!([]));
    assert!(doc["paths"]["/prefs"]["get"]["responses"]["401"].is_object());
}

#[tokio::test]
async fn only_listed_hosts_are_answered() {
    let app = server()
        .allowed_hosts(["localhost", "127.0.0.1"])
        .into_router();
    for (host, want) in [
        ("localhost:10777", StatusCode::OK),
        ("127.0.0.1", StatusCode::OK),
        ("evil.example", StatusCode::FORBIDDEN),
        ("localhost.evil.example:10777", StatusCode::FORBIDDEN),
    ] {
        let res = send_with(&app, Method::GET, "/prefs", None, &[("host", host)]).await;
        assert_eq!(res.status, want, "{host}");
    }
    let res = send_with(
        &app,
        Method::GET,
        "/prefs",
        None,
        &[("host", "evil.example")],
    )
    .await;
    assert_eq!(res.body["type"], "host-not-allowed");
}

#[tokio::test]
async fn an_open_server_documents_no_security() {
    let app = server().into_router();
    let doc = send(&app, Method::GET, "/openapi.json", None).await.body;
    assert!(doc.get("security").is_none());
}
