//! Live feeds over Server-Sent Events.

mod common;

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use common::{app, send};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

#[derive(Debug)]
struct Sse {
    event: String,
    id: Option<u64>,
    id_text: Option<String>,
    data: Value,
}

/// An open event stream, read incrementally.
struct Stream {
    body: Body,
    buf: String,
}

impl Stream {
    async fn open(
        app: &Router,
        path: &str,
        last_event_id: Option<u64>,
    ) -> Result<Stream, (StatusCode, Value)> {
        Stream::open_raw(app, path, last_event_id.map(|id| id.to_string())).await
    }

    async fn open_with_id(
        app: &Router,
        path: &str,
        id: &str,
    ) -> Result<Stream, (StatusCode, Value)> {
        Stream::open_raw(app, path, Some(id.to_owned())).await
    }

    async fn open_raw(
        app: &Router,
        path: &str,
        last_event_id: Option<String>,
    ) -> Result<Stream, (StatusCode, Value)> {
        let mut req = Request::get(path).header(header::ACCEPT, "text/event-stream");
        if let Some(id) = last_event_id {
            req = req.header("last-event-id", id);
        }
        let res = app
            .clone()
            .oneshot(req.body(Body::empty()).unwrap())
            .await
            .unwrap();
        if res.status() != StatusCode::OK {
            let status = res.status();
            let bytes = res.into_body().collect().await.unwrap().to_bytes();
            return Err((
                status,
                serde_json::from_slice(&bytes).unwrap_or(Value::Null),
            ));
        }
        assert_eq!(res.headers()[header::CONTENT_TYPE], "text/event-stream");
        Ok(Stream {
            body: res.into_body(),
            buf: String::new(),
        })
    }

    /// Whether the server ends the stream within a second, after any
    /// events still buffered.
    async fn ends(mut self) -> bool {
        tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(frame) = self.body.frame().await {
                frame.unwrap();
            }
        })
        .await
        .is_ok()
    }

    /// The next event, skipping keep-alive comments.
    async fn next(&mut self) -> Sse {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(end) = self.buf.find("\n\n") {
                    let raw: String = self.buf.drain(..end + 2).collect();
                    let (mut event, mut id, mut data) =
                        (String::from("message"), None, String::new());
                    let mut id_text = None;
                    for line in raw.lines() {
                        if let Some(v) = line.strip_prefix("event:") {
                            event = v.trim().into();
                        } else if let Some(v) = line.strip_prefix("id:") {
                            id = v.trim().parse().ok();
                            id_text = Some(v.trim().to_owned());
                        } else if let Some(v) = line.strip_prefix("data:") {
                            data += v.trim_start();
                        }
                    }
                    if data.is_empty() {
                        continue; // a comment
                    }
                    return Sse {
                        event,
                        id,
                        id_text,
                        data: serde_json::from_str(&data).unwrap(),
                    };
                }
                let frame = self.body.frame().await.expect("stream ended").unwrap();
                if let Ok(bytes) = frame.into_data() {
                    self.buf += std::str::from_utf8(&bytes).unwrap();
                }
            }
        })
        .await
        .expect("an event within 5s")
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stream_is_a_snapshot_then_each_commits_changes() {
    let app = app();
    send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    let mut s = Stream::open(&app, "/prefs?limit=10", None).await.unwrap();

    let first = s.next().await;
    assert_eq!(first.event, "snapshot");
    assert_eq!(
        first.data["data"]["items"],
        json!([{"key": "a", "value": 1}]),
        "what the plain GET answers"
    );
    assert_eq!(first.id, Some(first.data["epoch"].as_u64().unwrap()));

    let put = send(&app, Method::PUT, "/prefs/b", Some(json!(2))).await;
    let change = s.next().await;
    assert_eq!(change.event, "change");
    assert_eq!(
        change.id,
        Some(put.epoch()),
        "a change carries its write's epoch"
    );
    assert_eq!(
        change.data,
        json!({"epoch": put.epoch(), "changes": [{"key": "b", "op": "insert", "value": 2}]})
    );

    // An upsert of a present key is one replace, with the value before.
    send(&app, Method::PUT, "/prefs/a", Some(json!(5))).await;
    assert_eq!(
        s.next().await.data["changes"],
        json!([{"key": "a", "op": "replace", "old": 1, "value": 5}])
    );

    // Writes elsewhere don't reach this stream; one transaction is one
    // event however many rows it touches.
    send(&app, Method::PUT, "/tags/x", None).await;
    let tx = send(
        &app,
        Method::POST,
        "/_tx",
        Some(json!([
            {"resource": "prefs", "op": "upsert", "key": "c", "body": 3},
            {"resource": "prefs", "op": "remove", "key": "b"},
        ])),
    )
    .await;
    let next = s.next().await;
    assert_eq!(next.id, Some(tx.epoch()));
    assert_eq!(
        next.data["changes"],
        json!([
            {"key": "b", "op": "delete", "old": 2},
            {"key": "c", "op": "insert", "value": 3},
        ])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn singletons_and_views_stream_too() {
    let app = app();
    let mut count = Stream::open(&app, "/order_count", None).await.unwrap();
    let mut motd = Stream::open(&app, "/motd", None).await.unwrap();
    assert_eq!(count.next().await.data["data"], json!(null));
    assert_eq!(motd.next().await.data["data"], json!(null));

    let ada = send(
        &app,
        Method::POST,
        "/customers",
        Some(json!({"name": "ada", "region": "eu"})),
    )
    .await;
    let ada = ada.data()["key"].as_u64().unwrap();
    send(
        &app,
        Method::POST,
        "/orders",
        Some(json!({"customer": ada, "cents": 5})),
    )
    .await;
    assert_eq!(
        count.next().await.data["changes"],
        json!([{"op": "insert", "value": 1}])
    );

    send(&app, Method::PUT, "/motd", Some(json!("hi"))).await;
    assert_eq!(
        motd.next().await.data["changes"],
        json!([{"op": "insert", "value": "hi"}])
    );
    send(&app, Method::DELETE, "/motd", None).await;
    assert_eq!(
        motd.next().await.data["changes"],
        json!([{"op": "delete", "old": "hi"}])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sets_and_counted_kinds_stream_their_own_shapes() {
    let app = app();
    let mut tags = Stream::open(&app, "/tags", None).await.unwrap();
    let mut by_len = Stream::open(&app, "/tags_by_len", None).await.unwrap();
    let mut stock = Stream::open(&app, "/stock", None).await.unwrap();
    for s in [&mut tags, &mut by_len, &mut stock] {
        s.next().await;
    }

    // A set's member comes or goes; a grouped view counts it.
    send(&app, Method::PUT, "/tags/xy", None).await;
    assert_eq!(
        tags.next().await.data["changes"],
        json!([{"op": "insert", "value": "xy"}])
    );
    assert_eq!(
        by_len.next().await.data["changes"],
        json!([{"key": 2, "value": "xy", "diff": 1}])
    );
    send(&app, Method::DELETE, "/tags/xy", None).await;
    assert_eq!(
        tags.next().await.data["changes"],
        json!([{"op": "delete", "value": "xy"}])
    );
    assert_eq!(
        by_len.next().await.data["changes"],
        json!([{"key": 2, "value": "xy", "diff": -1}])
    );

    // A multiset's change is a change in count.
    send(
        &app,
        Method::POST,
        "/stock",
        Some(json!({"value": "lp", "times": 3})),
    )
    .await;
    assert_eq!(
        stock.next().await.data["changes"],
        json!([{"value": "lp", "diff": 3}])
    );
    send(&app, Method::DELETE, "/stock/lp?times=2", None).await;
    assert_eq!(
        stock.next().await.data["changes"],
        json!([{"value": "lp", "diff": -2}])
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reconnect_resumes_from_its_last_event() {
    let app = app();
    // A second client keeps the hub (and its ring) alive in between.
    let mut keeper = Stream::open(&app, "/prefs", None).await.unwrap();
    keeper.next().await;
    let mut s = Stream::open(&app, "/prefs", None).await.unwrap();
    s.next().await;
    send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    let seen = s.next().await.id.unwrap();
    drop(s);

    let b = send(&app, Method::PUT, "/prefs/b", Some(json!(2))).await;
    let c = send(&app, Method::PUT, "/prefs/c", Some(json!(3))).await;
    let mut s = Stream::open(&app, "/prefs", Some(seen)).await.unwrap();
    let first = s.next().await;
    assert_eq!(
        (first.event.as_str(), first.id),
        ("change", Some(b.epoch())),
        "replayed, no snapshot"
    );
    assert_eq!(s.next().await.id, Some(c.epoch()));
    let d = send(&app, Method::PUT, "/prefs/d", Some(json!(4))).await;
    assert_eq!(
        s.next().await.id,
        Some(d.epoch()),
        "then live, without a gap or a repeat"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resume_the_hub_cant_serve_starts_from_a_snapshot() {
    let app = app();
    send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    send(&app, Method::PUT, "/prefs/b", Some(json!(1))).await;
    // Epoch 1 predates this hub: it can't replay what came after.
    let mut s = Stream::open(&app, "/prefs", Some(1)).await.unwrap();
    let first = s.next().await;
    assert_eq!(first.event, "snapshot");
    assert_eq!(first.data["data"]["items"].as_array().unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_client_that_falls_behind_gets_a_fresh_snapshot() {
    let app = app();
    let mut s = Stream::open(&app, "/prefs?limit=1", None).await.unwrap();
    assert_eq!(s.next().await.event, "snapshot");
    // Far more commits than the hub buffers, while this client reads none.
    let n = fold_http_ring() + 100;
    for i in 0..n {
        send(
            &app,
            Method::PUT,
            &format!("/prefs/k{i:05}"),
            Some(json!(i)),
        )
        .await;
    }
    let mut last = 0;
    let mut resynced = false;
    while last < n as u64 {
        let e = s.next().await;
        if e.event == "snapshot" {
            resynced = true;
        }
        let epoch = e.id.unwrap();
        assert!(
            epoch > last,
            "epochs only move forward: {epoch} after {last}"
        );
        last = epoch;
    }
    assert!(resynced, "lagging past the buffer resyncs with a snapshot");
}

fn fold_http_ring() -> usize {
    1024
}

#[tokio::test]
async fn routes_that_dont_stream_say_so() {
    let app = app();
    let err = Stream::open(&app, "/names?q=x", None).await.err().unwrap();
    assert_eq!(err.0, StatusCode::NOT_ACCEPTABLE);
    assert_eq!(err.1["type"], "not-acceptable");
    let err = Stream::open(&app, "/prefs?limit=0", None)
        .await
        .err()
        .unwrap();
    assert_eq!(
        err.0,
        StatusCode::BAD_REQUEST,
        "a bad query fails before streaming"
    );

    let doc = send(&app, Method::GET, "/openapi.json", None).await.body;
    let list = &doc["paths"]["/prefs"]["get"];
    assert!(list["responses"]["200"]["content"]["text/event-stream"].is_object());
    assert!(
        list["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["name"] == "Last-Event-ID")
    );
    assert!(
        doc["paths"]["/names"]["get"]["responses"]["200"]["content"]["text/event-stream"].is_null()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closing_the_server_ends_its_streams() {
    let server = common::server();
    let closer = server.closer();
    let app = server.into_router();
    let mut s = Stream::open(&app, "/prefs", None).await.unwrap();
    s.next().await;
    closer.close();
    assert!(
        s.ends().await,
        "an open stream would hold a graceful shutdown forever"
    );
    let mut late = Stream::open(&app, "/prefs", None).await.unwrap();
    assert_eq!(
        late.next().await.event,
        "snapshot",
        "a stream opened while closing still gets its snapshot"
    );
    assert!(late.ends().await);
}

#[cfg(feature = "cors")]
#[tokio::test]
async fn cors_admits_only_the_listed_origins() {
    let app = app().layer(fold_http::cors(["http://localhost:5173"]));
    let preflight = |origin: &'static str| {
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/prefs/a")
            .header(header::ORIGIN, origin)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, "PUT")
            .header(
                header::ACCESS_CONTROL_REQUEST_HEADERS,
                "content-type,fold-min-epoch",
            )
            .body(Body::empty())
            .unwrap()
    };
    let ok = app
        .clone()
        .oneshot(preflight("http://localhost:5173"))
        .await
        .unwrap();
    assert_eq!(
        ok.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
        "http://localhost:5173"
    );
    let evil = app
        .clone()
        .oneshot(preflight("https://evil.example"))
        .await
        .unwrap();
    assert!(
        evil.headers()
            .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .is_none()
    );

    let get = Request::get("/prefs")
        .header(header::ORIGIN, "http://localhost:5173")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(get).await.unwrap();
    let exposed = res.headers()[header::ACCESS_CONTROL_EXPOSE_HEADERS]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(exposed.contains("fold-epoch"), "{exposed}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_connection_carries_many_resources() {
    let app = app();
    send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    // Each resource takes its own query, URL-encoded.
    let mut s = Stream::open(
        &app,
        "/_live?prefs=limit%3D10%26order%3Ddesc&motd=&tags=",
        None,
    )
    .await
    .unwrap();
    let mut firsts = Vec::new();
    for _ in 0..3 {
        let e = s.next().await;
        assert!(e.event.ends_with(".snapshot"), "{e:?}");
        firsts.push(e.event);
    }
    firsts.sort();
    assert_eq!(firsts, ["motd.snapshot", "prefs.snapshot", "tags.snapshot"]);

    let put = send(&app, Method::PUT, "/motd", Some(json!("hi"))).await;
    let e = s.next().await;
    assert_eq!(e.event, "motd.change");
    assert_eq!(
        e.data,
        json!({"epoch": put.epoch(), "changes": [{"op": "insert", "value": "hi"}]})
    );
    let id = e.id_text.clone().unwrap();
    let parts: Vec<u64> = id.split('.').map(|p| p.parse().unwrap()).collect();
    assert_eq!(
        parts.len(),
        3,
        "one epoch per resource, in request order: {id}"
    );
    assert_eq!(parts[1], put.epoch(), "motd's position carries its epoch");

    // Resume all three exactly from that id: only what came after.
    drop(s);
    let keeper = Stream::open(
        &app,
        "/_live?prefs=limit%3D10%26order%3Ddesc&motd=&tags=",
        None,
    )
    .await
    .unwrap();
    let tag = send(&app, Method::PUT, "/tags/x", None).await;
    let mut s = Stream::open_with_id(
        &app,
        "/_live?prefs=limit%3D10%26order%3Ddesc&motd=&tags=",
        &id,
    )
    .await
    .unwrap();
    let e = s.next().await;
    assert_eq!(
        (e.event.as_str(), e.data["epoch"].as_u64()),
        ("tags.change", Some(tag.epoch()))
    );
    drop(keeper);
}

#[tokio::test]
async fn a_multiplexed_stream_refuses_what_it_cant_carry() {
    let app = app();
    for (query, status) in [
        ("", StatusCode::BAD_REQUEST),
        ("nope=", StatusCode::BAD_REQUEST),
        ("prefs=&prefs=", StatusCode::BAD_REQUEST),
        ("names=", StatusCode::NOT_ACCEPTABLE),
        ("prefs=limit%3D0", StatusCode::BAD_REQUEST),
    ] {
        let err = Stream::open(&app, &format!("/_live?{query}"), None)
            .await
            .err()
            .unwrap();
        assert_eq!(err.0, status, "{query}: {:?}", err.1);
    }
}
