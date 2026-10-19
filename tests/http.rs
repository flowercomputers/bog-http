mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use common::{app, raw, send};
use serde_json::json;

#[tokio::test]
async fn insert_mints_an_id_and_reads_back() {
    let app = app();
    let res = send(
        &app,
        Method::POST,
        "/customers",
        Some(json!({"name": "ada", "region": "eu"})),
    )
    .await;
    assert_eq!(res.status, StatusCode::CREATED);
    let id = res.data()["key"].as_u64().unwrap();
    assert_eq!(res.header(header::LOCATION), format!("/customers/{id}"));
    assert_eq!(res.header("fold-epoch"), res.epoch().to_string());
    let written_at = res.epoch();

    let res = send(&app, Method::GET, &format!("/customers/{id}"), None).await;
    assert_eq!(res.status, StatusCode::OK);
    assert!(res.epoch() >= written_at, "a later read sees the write");
    assert_eq!(res.data(), &json!({"name": "ada", "region": "eu"}));
}

#[tokio::test]
async fn events_flow_into_views_and_constraints_refuse() {
    let app = app();
    let ada = send(
        &app,
        Method::POST,
        "/customers",
        Some(json!({"name": "ada", "region": "eu"})),
    )
    .await;
    let ada = ada.data()["key"].as_u64().unwrap();

    for cents in [250, 750] {
        let res = send(
            &app,
            Method::POST,
            "/orders",
            Some(json!({"customer": ada, "cents": cents})),
        )
        .await;
        assert_eq!(res.status, StatusCode::CREATED, "{:?}", res.body);
    }
    let res = send(&app, Method::GET, &format!("/spend/{ada}"), None).await;
    assert_eq!(res.data(), &json!(1000));

    // A source check refuses at the write call.
    let res = send(
        &app,
        Method::POST,
        "/orders",
        Some(json!({"customer": ada, "cents": 0})),
    )
    .await;
    assert_eq!(res.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(res.header(header::CONTENT_TYPE), "application/problem+json");
    assert_eq!(res.body["type"], "rejected");
    assert_eq!(res.body["at"], "orders");

    // A foreign key refuses after the graph runs, rolling it all back.
    let before = send(&app, Method::GET, "/orders", None).await;
    let res = send(
        &app,
        Method::POST,
        "/orders",
        Some(json!({"customer": 999_999, "cents": 5})),
    )
    .await;
    assert_eq!(
        res.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "{:?}",
        res.body
    );
    let after = send(&app, Method::GET, "/orders", None).await;
    assert_eq!(after.data(), before.data(), "nothing was half-applied");
    assert_eq!(after.epoch(), before.epoch(), "a refusal burns no epoch");
    let res = send(&app, Method::GET, &format!("/spend/{ada}"), None).await;
    assert_eq!(res.data(), &json!(1000));
}

#[tokio::test]
async fn upsert_and_remove_by_string_key() {
    let app = app();
    let res = send(&app, Method::PUT, "/prefs/theme", Some(json!(1))).await;
    assert_eq!(res.status, StatusCode::CREATED);
    assert_eq!(res.data(), &json!({"key": "theme"}));
    let res = send(&app, Method::PUT, "/prefs/theme", Some(json!(2))).await;
    assert_eq!(res.status, StatusCode::OK, "replacing is not creating");

    // A numeric-looking segment still addresses a string key.
    send(&app, Method::PUT, "/prefs/42", Some(json!(7))).await;
    assert_eq!(
        send(&app, Method::GET, "/prefs/42", None).await.data(),
        &json!(7)
    );
    // Percent-encoded segments decode.
    send(&app, Method::PUT, "/prefs/a%20b", Some(json!(3))).await;
    assert_eq!(
        send(&app, Method::GET, "/prefs/a%20b", None).await.data(),
        &json!(3)
    );

    assert_eq!(
        send(&app, Method::GET, "/prefs/theme", None).await.data(),
        &json!(2)
    );
    let res = send(&app, Method::DELETE, "/prefs/theme", None).await;
    assert_eq!(res.data(), &json!({"removed": true}));
    let res = send(&app, Method::DELETE, "/prefs/theme", None).await;
    assert_eq!(res.data(), &json!({"removed": false}));
    let res = send(&app, Method::GET, "/prefs/theme", None).await;
    assert_eq!(res.status, StatusCode::NOT_FOUND);
    assert_eq!(res.body["type"], "not-found");
}

#[tokio::test]
async fn listings_paginate_by_keyset_both_ways() {
    let app = app();
    for (i, k) in ["e", "a", "d", "b", "c"].iter().enumerate() {
        send(&app, Method::PUT, &format!("/prefs/{k}"), Some(json!(i))).await;
    }
    let walk = |order: &'static str| {
        let app = app.clone();
        async move {
            let mut keys = Vec::new();
            let mut url = format!("/prefs?limit=2&order={order}");
            loop {
                let res = send(&app, Method::GET, &url, None).await;
                assert_eq!(res.status, StatusCode::OK, "{:?}", res.body);
                let items = res.data()["items"].as_array().unwrap();
                assert!(items.len() <= 2);
                keys.extend(
                    items
                        .iter()
                        .map(|it| it["key"].as_str().unwrap().to_owned()),
                );
                match res.data()["next"].as_str() {
                    Some(next) => url = format!("/prefs?limit=2&order={order}&after={next}"),
                    None => break,
                }
            }
            keys
        }
    };
    assert_eq!(walk("asc").await, ["a", "b", "c", "d", "e"]);
    assert_eq!(walk("desc").await, ["e", "d", "c", "b", "a"]);

    let res = send(&app, Method::GET, "/prefs", None).await;
    assert_eq!(res.data()["items"][0], json!({"key": "a", "value": 1}));
    assert_eq!(res.data()["next"], json!(null));
}

#[tokio::test]
async fn malformed_requests_get_problems() {
    let app = app();
    let cases = [
        (Method::GET, "/nope", None, StatusCode::NOT_FOUND),
        (Method::GET, "/customers/abc", None, StatusCode::BAD_REQUEST),
        (Method::GET, "/prefs?limit=0", None, StatusCode::BAD_REQUEST),
        (
            Method::GET,
            "/prefs?limit=5000",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (Method::GET, "/prefs?bogus=1", None, StatusCode::BAD_REQUEST),
        (
            Method::GET,
            "/prefs?after=%21%21",
            None,
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::PUT,
            "/prefs/x",
            Some(json!("not a number")),
            StatusCode::BAD_REQUEST,
        ),
        (
            Method::POST,
            "/customers",
            Some(json!({"name": "no region"})),
            StatusCode::BAD_REQUEST,
        ),
        // Views and the skipped field have no write routes, or none at all.
        (
            Method::PUT,
            "/spend/1",
            Some(json!(1)),
            StatusCode::METHOD_NOT_ALLOWED,
        ),
        (Method::GET, "/by_region", None, StatusCode::NOT_FOUND),
        (
            Method::DELETE,
            "/orders/1",
            None,
            StatusCode::METHOD_NOT_ALLOWED,
        ),
    ];
    for (method, path, body, want) in cases {
        let res = send(&app, method.clone(), path, body).await;
        assert_eq!(res.status, want, "{method} {path}: {:?}", res.body);
        assert_eq!(
            res.header(header::CONTENT_TYPE),
            "application/problem+json",
            "{method} {path}"
        );
        assert_eq!(res.body["status"], want.as_u16(), "{method} {path}");
    }

    let res = send(&app, Method::DELETE, "/orders/1", None).await;
    assert_eq!(res.header(header::ALLOW), "GET,HEAD");

    let req = Request::put("/prefs/x").body(Body::from("1")).unwrap();
    let res = raw(&app, req).await;
    assert_eq!(res.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let req = Request::put("/prefs/x")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{"))
        .unwrap();
    assert_eq!(raw(&app, req).await.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn schema_describes_every_served_field() {
    let app = app();
    let res = send(&app, Method::GET, "/_schema", None).await;
    let resources = res.body["resources"].as_array().unwrap();
    let kinds: Vec<_> = resources
        .iter()
        .map(|r| (r["name"].as_str().unwrap(), r["kind"].as_str().unwrap()))
        .collect();
    assert_eq!(
        kinds,
        [
            ("customers", "table"),
            ("prefs", "table"),
            ("orders", "events"),
            ("tags", "set"),
            ("motd", "cell"),
            ("stock", "bag"),
            ("spend", "map_view"),
            ("ranked", "ordered_view"),
            ("tags_by_len", "group_view"),
            ("order_count", "value_view"),
            ("names", "search_view"),
        ]
    );

    let ops = |i: usize| -> Vec<String> {
        resources[i]["routes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| {
                format!(
                    "{} {}",
                    r["method"].as_str().unwrap(),
                    r["op"].as_str().unwrap()
                )
            })
            .collect()
    };
    assert_eq!(
        ops(0),
        [
            "GET list",
            "GET get",
            "PUT upsert",
            "DELETE remove",
            "POST insert"
        ],
        "Table<Id<V>, V> mints"
    );
    assert_eq!(
        ops(1),
        ["GET list", "GET get", "PUT upsert", "DELETE remove"],
        "Table<String, _> doesn't"
    );
    assert_eq!(ops(6), ["GET list", "GET get"], "views are read-only");

    assert_eq!(resources[1]["key"]["type"], "string");
    assert_eq!(
        resources[0]["value"]["$ref"],
        "#/components/schemas/Customer"
    );
    assert_eq!(resources[3]["description"], "Labels on the shop.");
    let order = &res.body["components"]["schemas"]["Order"];
    assert_eq!(order["properties"]["customer"]["type"], "integer");
    assert_eq!(
        send(&app, Method::GET, "/_health", None).await.status,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_commit_once_each() {
    let app = app();
    let tasks: Vec<_> = (0..64)
        .map(|i| {
            let app = app.clone();
            tokio::spawn(async move {
                let res = send(
                    &app,
                    Method::PUT,
                    &format!("/prefs/k{i:02}"),
                    Some(json!(i)),
                )
                .await;
                assert_eq!(res.status, StatusCode::CREATED);
                res.epoch()
            })
        })
        .collect();
    let mut epochs = Vec::new();
    for t in tasks {
        epochs.push(t.await.unwrap());
    }
    epochs.sort();
    epochs.dedup();
    assert_eq!(epochs.len(), 64, "every write is its own transaction");

    let res = send(&app, Method::GET, "/prefs?limit=1000", None).await;
    assert_eq!(res.data()["items"].as_array().unwrap().len(), 64);
    assert_eq!(res.epoch(), *epochs.last().unwrap());
}
