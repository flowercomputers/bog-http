//! Every fold kind, served: sets, cells, multisets, ordered, grouped, value,
//! and search views.

mod common;

use axum::http::{Method, StatusCode};
use common::{Res, app, send};
use serde_json::{Value, json};

fn items(res: &Res) -> &Vec<Value> {
    res.data()["items"].as_array().expect("a page")
}

/// Every page of a listing, following `next`.
async fn walk(app: &axum::Router, base: &str) -> Vec<Value> {
    let sep = if base.contains('?') { '&' } else { '?' };
    let mut out = Vec::new();
    let mut url = base.to_owned();
    loop {
        let res = send(app, Method::GET, &url, None).await;
        assert_eq!(res.status, StatusCode::OK, "{url}: {:?}", res.body);
        out.extend(items(&res).iter().cloned());
        match res.data()["next"].as_str() {
            Some(next) => url = format!("{base}{sep}after={next}"),
            None => return out,
        }
    }
}

async fn customer(app: &axum::Router, name: &str) -> u64 {
    let res = send(
        app,
        Method::POST,
        "/customers",
        Some(json!({"name": name, "region": "eu"})),
    )
    .await;
    res.data()["key"].as_u64().unwrap()
}

async fn order(app: &axum::Router, customer: u64, cents: i64) {
    let res = send(
        app,
        Method::POST,
        "/orders",
        Some(json!({"customer": customer, "cents": cents})),
    )
    .await;
    assert_eq!(res.status, StatusCode::CREATED, "{:?}", res.body);
}

#[tokio::test]
async fn sets_add_test_and_remove_members() {
    let app = app();
    let res = send(&app, Method::PUT, "/tags/new", None).await;
    assert_eq!(
        (res.status, res.data()),
        (StatusCode::CREATED, &json!({"key": "new"}))
    );
    let res = send(&app, Method::PUT, "/tags/new", None).await;
    assert_eq!(res.status, StatusCode::OK, "already a member");
    for t in ["sale", "local", "vinyl"] {
        send(&app, Method::PUT, &format!("/tags/{t}"), None).await;
    }

    assert_eq!(
        send(&app, Method::GET, "/tags/sale", None).await.data(),
        &json!(true)
    );
    assert_eq!(
        send(&app, Method::GET, "/tags/gone", None).await.status,
        StatusCode::NOT_FOUND
    );

    assert_eq!(
        walk(&app, "/tags?limit=3").await,
        json!(["local", "new", "sale", "vinyl"])
            .as_array()
            .unwrap()
            .clone()
    );
    assert_eq!(
        walk(&app, "/tags?limit=1&order=desc").await,
        json!(["vinyl", "sale", "new", "local"])
            .as_array()
            .unwrap()
            .clone()
    );

    let res = send(&app, Method::DELETE, "/tags/sale", None).await;
    assert_eq!(res.data(), &json!({"removed": true}));
    let res = send(&app, Method::DELETE, "/tags/sale", None).await;
    assert_eq!(res.data(), &json!({"removed": false}));
}

#[tokio::test]
async fn cells_set_and_clear() {
    let app = app();
    assert_eq!(
        send(&app, Method::GET, "/motd", None).await.data(),
        &json!(null)
    );
    let res = send(&app, Method::PUT, "/motd", Some(json!("hello"))).await;
    assert_eq!(
        (res.status, res.data()),
        (StatusCode::CREATED, &json!("hello"))
    );
    let res = send(&app, Method::PUT, "/motd", Some(json!("bye"))).await;
    assert_eq!(res.status, StatusCode::OK);
    assert_eq!(
        send(&app, Method::GET, "/motd", None).await.data(),
        &json!("bye")
    );
    let res = send(&app, Method::DELETE, "/motd", None).await;
    assert_eq!(res.data(), &json!({"removed": true}));
    assert_eq!(
        send(&app, Method::GET, "/motd", None).await.data(),
        &json!(null)
    );
}

#[tokio::test]
async fn multisets_count_copies() {
    let app = app();
    let res = send(
        &app,
        Method::POST,
        "/stock",
        Some(json!({"value": "lp", "times": 3})),
    )
    .await;
    assert_eq!(
        (res.status, res.data()),
        (StatusCode::OK, &json!({"value": "lp", "count": 3}))
    );
    send(&app, Method::POST, "/stock", Some(json!({"value": "cd"}))).await;
    assert_eq!(
        send(&app, Method::GET, "/stock/lp", None).await.data(),
        &json!(3)
    );
    assert_eq!(
        send(&app, Method::GET, "/stock/tape", None).await.data(),
        &json!(0)
    );

    let res = send(&app, Method::DELETE, "/stock/lp?times=2", None).await;
    assert_eq!(res.data(), &json!({"value": "lp", "count": 1}));
    // Removing more than there are stops at zero.
    let res = send(&app, Method::DELETE, "/stock/lp?times=9", None).await;
    assert_eq!(res.data(), &json!({"value": "lp", "count": 0}));

    send(
        &app,
        Method::POST,
        "/stock",
        Some(json!({"value": "lp", "times": 2})),
    )
    .await;
    assert_eq!(
        walk(&app, "/stock?limit=1").await,
        vec![
            json!({"value": "cd", "count": 1}),
            json!({"value": "lp", "count": 2})
        ]
    );
    send(&app, Method::POST, "/stock", Some(json!({"value": "ep"}))).await;
    assert_eq!(
        walk(&app, "/stock?limit=1&order=desc").await,
        vec![
            json!({"value": "lp", "count": 2}),
            json!({"value": "ep", "count": 1}),
            json!({"value": "cd", "count": 1})
        ]
    );
    for bad in [
        json!({"value": "lp", "times": 0}),
        json!({"value": "lp", "extra": 1}),
    ] {
        let res = send(&app, Method::POST, "/stock", Some(bad)).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST);
    }
}

#[tokio::test]
async fn ordered_views_list_by_key_or_by_value() {
    let app = app();
    let mut ids = Vec::new();
    for (name, cents) in [("a", 300), ("b", 100), ("c", 300), ("d", 200), ("e", 500)] {
        let id = customer(&app, name).await;
        order(&app, id, cents).await;
        ids.push((id, cents));
    }
    let keys = |rows: Vec<Value>| -> Vec<u64> {
        rows.iter().map(|r| r["key"].as_u64().unwrap()).collect()
    };

    // by key: ids are minted in order
    let by_key = keys(walk(&app, "/ranked?limit=2").await);
    assert_eq!(by_key, ids.iter().map(|(id, _)| *id).collect::<Vec<_>>());

    // by value, ascending, ties by key: b(100) d(200) a(300) c(300) e(500)
    let want: Vec<u64> = [1, 3, 0, 2, 4].iter().map(|&i| ids[i].0).collect();
    for limit in [1, 2, 3, 10] {
        assert_eq!(
            keys(walk(&app, &format!("/ranked?by=value&limit={limit}")).await),
            want,
            "limit {limit}"
        );
        let mut rev = want.clone();
        rev.reverse();
        assert_eq!(
            keys(walk(&app, &format!("/ranked?by=value&order=desc&limit={limit}")).await),
            rev,
            "desc limit {limit}"
        );
    }

    // value bounds, inclusive
    let mid = walk(&app, "/ranked?by=value&min=200&max=300&limit=1").await;
    assert_eq!(keys(mid), [ids[3].0, ids[0].0, ids[2].0]);

    let res = send(&app, Method::GET, "/ranked?min=1", None).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST, "bounds need by=value");
    let res = send(&app, Method::GET, &format!("/ranked/{}", ids[4].0), None).await;
    assert_eq!(res.data(), &json!(500));
}

#[tokio::test]
async fn group_views_list_pairs_and_per_key_values() {
    let app = app();
    for t in ["ab", "cd", "xyz", "q", "ef"] {
        send(&app, Method::PUT, &format!("/tags/{t}"), None).await;
    }
    let all = walk(&app, "/tags_by_len?limit=2").await;
    assert_eq!(all.len(), 5);
    assert_eq!(all[0], json!({"key": 1, "value": "q", "count": 1}));
    assert_eq!(all[4], json!({"key": 3, "value": "xyz", "count": 1}));

    let values = |rows: Vec<Value>| -> Vec<String> {
        rows.iter()
            .map(|r| r["value"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(
        values(walk(&app, "/tags_by_len/2?limit=1").await),
        ["ab", "cd", "ef"]
    );
    assert_eq!(
        values(walk(&app, "/tags_by_len/2?limit=2&order=desc").await),
        ["ef", "cd", "ab"]
    );
    assert!(
        walk(&app, "/tags_by_len/7").await.is_empty(),
        "an empty group is an empty page"
    );
}

#[tokio::test]
async fn value_views_and_search() {
    let app = app();
    assert_eq!(
        send(&app, Method::GET, "/order_count", None).await.data(),
        &json!(null)
    );
    let ada = customer(&app, "ada lovelace").await;
    let alan = customer(&app, "alan turing").await;
    customer(&app, "grace hopper").await;
    order(&app, ada, 1).await;
    order(&app, alan, 1).await;
    assert_eq!(
        send(&app, Method::GET, "/order_count", None).await.data(),
        &json!(2)
    );

    let res = send(&app, Method::GET, "/names?q=turing", None).await;
    let hits = res.data().as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["key"], json!(alan));
    assert!(hits[0]["score"].as_f64().unwrap() > 0.0);

    let res = send(&app, Method::GET, "/names?q=ada+grace&k=1", None).await;
    assert_eq!(res.data().as_array().unwrap().len(), 1, "k caps the hits");

    let res = send(&app, Method::GET, "/names/complete?prefix=a", None).await;
    let terms: Vec<_> = res
        .data()
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["term"].as_str().unwrap())
        .collect();
    assert_eq!(terms, ["ada", "alan"]);

    assert_eq!(
        send(&app, Method::GET, "/names", None).await.status,
        StatusCode::BAD_REQUEST,
        "q is required"
    );
    assert_eq!(
        send(&app, Method::GET, "/names?q=x&k=0", None).await.status,
        StatusCode::BAD_REQUEST
    );
}
