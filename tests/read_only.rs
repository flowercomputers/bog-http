//! `#[serve(read_only)]`: a source served for reading, written only
//! through the application's own routes.

use axum::http::{Method, StatusCode};
use fold_http::{Serve, Server};
use fold3::schema::*;
use serde::Deserialize;
use serde_json::json;

mod common;
use common::send;

#[derive(Schema, Serve)]
struct Bank {
    /// Written only by `/deposit`.
    #[serve(read_only)]
    balances: Table<String, i64>,
    notes: Set<String>,
}

impl Declare for Bank {
    fn declare(_: &mut BankBuilder) {}
}

#[derive(Deserialize, schemars::JsonSchema)]
struct Deposit {
    account: String,
    amount: i64,
}

fn app() -> axum::Router {
    Server::new(Bank::memory().build().unwrap())
        .post(
            "/deposit",
            "Deposit",
            |tx: &mut BankWrite<'_>, d: Deposit| {
                let now = tx.balances.get(&d.account)?.unwrap_or(0) + d.amount;
                tx.balances.upsert(d.account, now)?;
                Ok(now)
            },
        )
        .into_router()
}

#[tokio::test]
async fn read_only_sources_serve_reads_and_refuse_writes() {
    let app = app();
    let res = send(
        &app,
        Method::POST,
        "/deposit",
        Some(json!({"account": "ada", "amount": 5})),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);

    assert_eq!(
        send(&app, Method::GET, "/balances/ada", None).await.data(),
        &json!(5)
    );
    let list = send(&app, Method::GET, "/balances", None).await;
    assert_eq!(list.data()["items"], json!([{"key": "ada", "value": 5}]));

    let put = send(&app, Method::PUT, "/balances/ada", Some(json!(1_000_000))).await;
    assert_eq!(put.status, StatusCode::METHOD_NOT_ALLOWED);
    let del = send(&app, Method::DELETE, "/balances/ada", None).await;
    assert_eq!(del.status, StatusCode::METHOD_NOT_ALLOWED);
    let tx = send(
        &app,
        Method::POST,
        "/_tx",
        Some(json!([{"resource": "balances", "op": "upsert", "key": "ada", "body": 9}])),
    )
    .await;
    assert_eq!(tx.status, StatusCode::BAD_REQUEST, "{:?}", tx.body);
    assert_eq!(
        send(&app, Method::GET, "/balances/ada", None).await.data(),
        &json!(5)
    );

    // Other sources keep their writes.
    assert_eq!(
        send(&app, Method::PUT, "/notes/hi", None).await.status,
        StatusCode::CREATED
    );

    // Documented as read-only, too.
    let doc = send(&app, Method::GET, "/openapi.json", None).await.body;
    let item = &doc["paths"]["/balances/{key}"];
    assert!(item["get"].is_object());
    assert!(item.get("put").is_none() && item.get("delete").is_none());
    let schema = send(&app, Method::GET, "/_schema", None).await.body;
    let balances = schema["resources"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == "balances")
        .unwrap();
    assert!(
        balances["routes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r["method"] == "GET")
    );
    assert_eq!(balances["description"], "Written only by `/deposit`.");
}
