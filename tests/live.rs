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
        let mut req = Request::get(path).header(header::ACCEPT, "text/event-stream");
        if let Some(id) = last_event_id {
            req = req.header("last-event-id", id.to_string());
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
                    for line in raw.lines() {
                        if let Some(v) = line.strip_prefix("event:") {
                            event = v.trim().into();
                        } else if let Some(v) = line.strip_prefix("id:") {
                            id = v.trim().parse().ok();
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
        json!({"epoch": put.epoch(), "changes": [{"key": "b", "value": 2, "diff": 1}]})
    );

    // An upsert retracts the old row and adds the new one.
    send(&app, Method::PUT, "/prefs/a", Some(json!(5))).await;
    let mut changes = s.next().await.data["changes"].as_array().unwrap().clone();
    changes.sort_by_key(|c| c["diff"].as_i64());
    assert_eq!(
        changes,
        [
            json!({"key": "a", "value": 1, "diff": -1}),
            json!({"key": "a", "value": 5, "diff": 1})
        ]
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
    assert_eq!(next.data["changes"].as_array().unwrap().len(), 2);
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
        json!([{"value": 1, "diff": 1}])
    );

    send(&app, Method::PUT, "/motd", Some(json!("hi"))).await;
    assert_eq!(
        motd.next().await.data["changes"],
        json!([{"value": "hi", "diff": 1}])
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
