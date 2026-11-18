//! The write side: transactions, custom routes, read-your-writes, and
//! admission control.

mod common;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use common::{Order, ShopWrite, app, raw, send, send_with, server};
use fold_http::{NoParams, Problem};
use fold3::Id;
use fold3::schema::Rejected;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

async fn tx(app: &axum::Router, ops: Value) -> common::Res {
    send(app, Method::POST, "/_tx", Some(ops)).await
}

#[tokio::test]
async fn transactions_apply_across_sources_in_one_epoch() {
    let app = app();
    let before = send(&app, Method::GET, "/prefs", None).await.epoch();
    let res = tx(
        &app,
        json!([
            {"resource": "customers", "op": "insert", "body": {"name": "ada", "region": "eu"}},
            {"resource": "prefs", "op": "upsert", "key": "theme", "body": 1},
            {"resource": "prefs", "op": "upsert", "key": "42", "body": 2},
            {"resource": "tags", "op": "insert", "key": "new"},
            {"resource": "stock", "op": "add", "body": {"value": "lp", "times": 2}},
            {"resource": "stock", "op": "remove", "key": "lp", "query": {"times": 1}},
            {"resource": "motd", "op": "set", "body": "hi"},
        ]),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "{:?}", res.body);
    assert_eq!(res.epoch(), before + 1, "one transaction, one epoch");
    let results = res.data()["results"].as_array().unwrap();
    assert_eq!(results.len(), 7);
    assert_eq!(
        results[2],
        json!({"key": "42"}),
        "a string key stays a string"
    );
    assert_eq!(
        results[5],
        json!({"value": "lp", "count": 1}),
        "later ops see earlier ones"
    );

    let id = results[0]["key"].as_u64().unwrap();
    assert_eq!(
        send(&app, Method::GET, &format!("/customers/{id}"), None)
            .await
            .data()["name"],
        "ada"
    );
    assert_eq!(
        send(&app, Method::GET, "/prefs/42", None).await.data(),
        &json!(2)
    );
    assert_eq!(
        send(&app, Method::GET, "/motd", None).await.data(),
        &json!("hi")
    );

    // Within one transaction, a remove sees the insert before it.
    let res = tx(
        &app,
        json!([
            {"resource": "tags", "op": "insert", "key": "tmp"},
            {"resource": "tags", "op": "remove", "key": "tmp"},
        ]),
    )
    .await;
    assert_eq!(res.data()["results"][1], json!({"removed": true}));
    assert_eq!(
        send(&app, Method::GET, "/tags/tmp", None).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_refused_transaction_applies_nothing() {
    let app = app();
    let before = send(&app, Method::GET, "/prefs", None).await.epoch();
    let res = tx(
        &app,
        json!([
            {"resource": "prefs", "op": "upsert", "key": "a", "body": 1},
            {"resource": "orders", "op": "append", "body": {"customer": 999, "cents": 5}},
        ]),
    )
    .await;
    assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(res.body["at"], "orders");
    let res = send(&app, Method::GET, "/prefs/a", None).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
    let after = send(&app, Method::GET, "/prefs", None).await.epoch();
    assert_eq!(after, before, "no epoch burned");
}

#[tokio::test]
async fn malformed_transactions_name_the_bad_operation() {
    let app = app();
    let good = json!({"resource": "prefs", "op": "upsert", "key": "a", "body": 1});
    let cases = [
        (json!([]), None),
        (json!([{"resource": "nope", "op": "insert"}]), Some(0)),
        (
            json!([good, {"resource": "prefs", "op": "upsert", "key": "b", "body": "x"}]),
            Some(1),
        ),
        (json!([good, {"resource": "prefs", "op": "list"}]), Some(1)),
        (
            json!([{"resource": "spend", "op": "upsert", "key": 1, "body": 1}]),
            Some(0),
        ),
        (
            json!([{"resource": "prefs", "op": "upsert", "body": 1}]),
            Some(0),
        ),
        (
            json!([{"resource": "tags", "op": "insert", "key": "x", "body": 1}]),
            Some(0),
        ),
        (
            json!([{"resource": "prefs", "op": "upsert", "key": "a", "body": 1, "extra": 0}]),
            None,
        ),
        (
            Value::Array(vec![good.clone(); fold_http::MAX_TX_OPS + 1]),
            None,
        ),
    ];
    for (ops, index) in cases {
        let res = tx(&app, ops.clone()).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{ops}: {:?}", res.body);
        assert_eq!(
            res.body.get("index").and_then(Value::as_u64),
            index.map(|i| i as u64),
            "{ops}"
        );
    }
    assert_eq!(
        send(&app, Method::GET, "/prefs/a", None).await.status,
        StatusCode::NOT_FOUND
    );
}

#[derive(Deserialize, JsonSchema)]
struct Top {
    n: usize,
}

#[derive(Deserialize, JsonSchema)]
struct Checkout {
    customer: u64,
    cents: i64,
}

#[derive(Serialize, JsonSchema)]
struct Receipt {
    order: u64,
    total: i64,
}

const LIMIT: i64 = 1000;

fn shop_with_routes() -> axum::Router {
    server()
        .get(
            "/top",
            "Best customers by spend",
            |r: &common::ShopRead<'_>, q: Top| {
                Ok(r.ranked
                    .top(q.n)
                    .map_err(|e| Problem::internal(e.to_string()))?
                    .into_iter()
                    .map(|(id, cents)| (id.raw(), cents))
                    .collect::<Vec<_>>())
            },
        )
        .get(
            "/totals",
            "Spend and order count, from one snapshot",
            |r: &common::ShopRead<'_>, _: NoParams| {
                let rows = |e: fold3::Error| Problem::internal(e.to_string());
                let spend: i64 = r
                    .spend
                    .iter()
                    .map(|r| r.map(|(_, c)| c))
                    .sum::<Result<_, _>>()
                    .map_err(rows)?;
                let paid: i64 = r
                    .orders
                    .iter()
                    .map(|r| r.map(|(_, o)| o.cents))
                    .sum::<Result<_, _>>()
                    .map_err(rows)?;
                let count = r.orders.len().map_err(rows)? as i64;
                if spend != paid || r.order_count.get().map_err(rows)?.unwrap_or(0) != count {
                    return Err(Problem::internal(format!(
                        "torn: spend {spend}, paid {paid}, count {count}"
                    )));
                }
                Ok(json!({"spend": spend, "orders": count}))
            },
        )
        .post(
            "/checkout",
            "Order, refusing past the spend limit",
            |tx: &mut ShopWrite<'_>, c: Checkout| {
                let customer = Id::new(c.customer);
                let spent = tx.spend.get(&customer)?.unwrap_or(0);
                if spent + c.cents > LIMIT {
                    return Err(fold3::Error::reject(Rejected::new(
                        "checkout",
                        format!("over the {LIMIT} limit"),
                    )));
                }
                let order = tx.orders.push(Order {
                    customer,
                    cents: c.cents,
                })?;
                Ok(Receipt {
                    order: order.raw(),
                    total: spent + c.cents,
                })
            },
        )
        .into_router()
}

#[tokio::test]
async fn custom_routes_read_snapshots_and_run_transactions() {
    let app = shop_with_routes();
    let ada = send(
        &app,
        Method::POST,
        "/customers",
        Some(json!({"name": "ada", "region": "eu"})),
    )
    .await;
    let ada = ada.data()["key"].as_u64().unwrap();

    let res = send(
        &app,
        Method::POST,
        "/checkout",
        Some(json!({"customer": ada, "cents": 600})),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "{:?}", res.body);
    assert_eq!(res.data()["total"], 600);
    let epoch = res.epoch();

    // Over the limit: refused, rolled back, no epoch burned.
    let res = send(
        &app,
        Method::POST,
        "/checkout",
        Some(json!({"customer": ada, "cents": 500})),
    )
    .await;
    assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(res.body["at"], "checkout");
    let res = send(&app, Method::GET, "/top?n=5", None).await;
    assert_eq!(res.data(), &json!([[ada, 600]]));
    assert_eq!(res.epoch(), epoch);

    assert_eq!(
        send(&app, Method::GET, "/totals", None).await.data(),
        &json!({"spend": 600, "orders": 1})
    );
    assert_eq!(
        send(&app, Method::GET, "/totals?x=1", None).await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        send(&app, Method::GET, "/top", None).await.status,
        StatusCode::BAD_REQUEST,
        "n is required"
    );
    let res = send(
        &app,
        Method::POST,
        "/checkout",
        Some(json!({"customer": ada})),
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);

    let doc = send(&app, Method::GET, "/openapi.json", None).await.body;
    let top = &doc["paths"]["/top"]["get"];
    assert_eq!(top["summary"], "Best customers by spend");
    assert_eq!(top["parameters"][0]["name"], "n");
    let checkout = &doc["paths"]["/checkout"]["post"];
    assert_eq!(
        checkout["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/Checkout"
    );
    assert!(checkout["responses"]["422"].is_object());
}

#[test]
#[should_panic(expected = "two routes")]
fn custom_routes_cannot_shadow_generated_ones() {
    let _ = server()
        .get("/customers", "x", |_: &common::ShopRead, _: NoParams| Ok(0))
        .into_router();
}

#[test]
#[should_panic(expected = "reserved")]
fn custom_routes_stay_out_of_the_reserved_namespace() {
    let _ = server().get("/_mine", "x", |_: &common::ShopRead, _: NoParams| Ok(0));
}

#[tokio::test]
async fn min_epoch_reads_wait_for_their_writes() {
    let app = server()
        .min_epoch_wait(Duration::from_millis(100))
        .into_router();
    let res = send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    let written = res.epoch().to_string();

    let res = send_with(
        &app,
        Method::GET,
        "/prefs/a",
        None,
        &[("fold-min-epoch", &written)],
    )
    .await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.epoch() >= written.parse().unwrap());

    // An epoch that never comes: refused once the wait is up.
    let future = (res.epoch() + 5).to_string();
    let res = send_with(
        &app,
        Method::GET,
        "/prefs/a",
        None,
        &[("fold-min-epoch", &future)],
    )
    .await;
    assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(res.body["type"], "epoch-not-reached");
    assert_eq!(res.header(header::RETRY_AFTER), "1");

    let res = send_with(
        &app,
        Method::GET,
        "/prefs/a",
        None,
        &[("fold-min-epoch", "soon")],
    )
    .await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_read_wakes_on_the_commit() {
    let app = server()
        .min_epoch_wait(Duration::from_secs(5))
        .into_router();
    let next = (send(&app, Method::GET, "/prefs", None).await.epoch() + 1).to_string();
    let reader = {
        let app = app.clone();
        tokio::spawn(async move {
            send_with(
                &app,
                Method::GET,
                "/prefs/late",
                None,
                &[("fold-min-epoch", &next)],
            )
            .await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!reader.is_finished(), "the read waits for its epoch");
    send(&app, Method::PUT, "/prefs/late", Some(json!(7))).await;
    let res = tokio::time::timeout(Duration::from_secs(1), reader)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((res.status, res.data()), (StatusCode::OK, &json!(7)));
}

#[tokio::test]
async fn admission_control_refuses_writes_past_the_limit() {
    let app = server().max_pending_writes(0).into_router();
    for (method, path, body) in [
        (Method::PUT, "/prefs/a", Some(json!(1))),
        (
            Method::POST,
            "/_tx",
            Some(json!([{"resource": "tags", "op": "insert", "key": "x"}])),
        ),
    ] {
        let res = send(&app, method, path, body).await;
        assert_eq!(res.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(res.body["type"], "overloaded");
        assert_eq!(res.header(header::RETRY_AFTER), "1");
    }
    assert_eq!(
        send(&app, Method::GET, "/prefs", None).await.status,
        StatusCode::OK,
        "reads unaffected"
    );
}

#[tokio::test]
async fn oversized_bodies_get_a_problem() {
    let app = server().body_limit(16).into_router();
    let req = Request::put("/prefs/a")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(format!("\"{}\"", "x".repeat(64))))
        .unwrap();
    let res = raw(&app, req).await;
    assert_eq!(res.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(res.header(header::CONTENT_TYPE), "application/problem+json");
}
