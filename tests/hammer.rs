//! Concurrent writers and readers: every read sees one consistent
//! snapshot, and every write is exactly one epoch.

mod common;

use axum::http::{Method, StatusCode};
use bog_http::{NoParams, Problem};
use common::{send, server};
use serde_json::json;

const WRITERS: u64 = 8;
const ORDERS: u64 = 25;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn snapshots_never_tear_under_concurrent_writes() {
    let app = server()
        .get(
            "/invariant",
            "Views agree with their sources",
            |r: &common::ShopRead<'_>, _: NoParams| {
                let mut spend = 0;
                for row in r.spend.iter() {
                    spend += row?.1;
                }
                let (mut paid, mut count) = (0, 0);
                for row in r.orders.iter() {
                    paid += row?.1.cents;
                    count += 1;
                }
                let mut ranked = 0;
                for row in r.ranked.iter() {
                    ranked += row?.1;
                }
                if spend != paid || ranked != paid || r.order_count.get()?.unwrap_or(0) != count {
                    return Err(Problem::internal(format!(
                        "torn: spend {spend} ranked {ranked} paid {paid} count {count}"
                    )));
                }
                Ok(count)
            },
        )
        .into_router();
    let ada = send(
        &app,
        Method::POST,
        "/customers",
        Some(json!({"name": "ada", "region": "eu"})),
    )
    .await;
    let ada = ada.data()["key"].as_u64().unwrap();

    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let app = app.clone();
            tokio::spawn(async move {
                let mut epochs = Vec::new();
                for i in 0..ORDERS {
                    let res = send(
                        &app,
                        Method::POST,
                        "/orders",
                        Some(json!({"customer": ada, "cents": 1 + w * 100 + i})),
                    )
                    .await;
                    assert_eq!(res.status, StatusCode::CREATED, "{:?}", res.body);
                    epochs.push(res.epoch());
                }
                epochs
            })
        })
        .collect();
    let readers: Vec<_> = (0..WRITERS)
        .map(|_| {
            let app = app.clone();
            tokio::spawn(async move {
                let mut last = (0, 0);
                for _ in 0..ORDERS * 2 {
                    let res = send(&app, Method::GET, "/invariant", None).await;
                    assert_eq!(res.status, StatusCode::OK, "{:?}", res.body);
                    let seen = (res.epoch(), res.data().as_u64().unwrap());
                    assert!(
                        seen >= last,
                        "reads never go back in time: {seen:?} after {last:?}"
                    );
                    last = seen;
                }
            })
        })
        .collect();

    let mut epochs = Vec::new();
    for w in writers {
        epochs.extend(w.await.unwrap());
    }
    for r in readers {
        r.await.unwrap();
    }
    epochs.sort();
    let n = epochs.len();
    epochs.dedup();
    assert_eq!(epochs.len(), n, "no two writes share an epoch");
    assert_eq!(n as u64, WRITERS * ORDERS);
    let first = epochs[0];
    assert_eq!(
        epochs,
        (first..first + n as u64).collect::<Vec<_>>(),
        "and none is skipped"
    );

    let res = send(&app, Method::GET, "/invariant", None).await;
    assert_eq!(res.data(), &json!(WRITERS * ORDERS));
}
