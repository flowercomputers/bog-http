//! Hardening probes: each test names one way bog-http itself could be
//! pushed into misbehaving (not bog underneath it), and asserts what it
//! should do instead. Fast and deterministic; the load-shaped ones live
//! in `tests/load.rs`.

mod common;
#[path = "support/sse.rs"]
mod sse;

use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use bog::schema::*;
use bog_http::{NoParams, Problem, Serve, Server};
use common::{app, raw, send, send_with, server};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sse::{Next, Stream};

// ---------------------------------------------------------------------
// Live feeds

/// An `EventSource` resends its last id after any reconnect, including
/// one to a server restarted on a different (or restored) store whose
/// epochs are lower. A resume point the server has never reached can't
/// be honoured: it should start the client over with a snapshot, not
/// treat it as caught up and drop every change up to that epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_last_event_id_from_the_future_gets_a_snapshot() {
    let app = app();
    send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;

    let mut s = Stream::open(&app, "/prefs", Some("1000000")).await.unwrap();
    let first = s.next_within(Duration::from_secs(1)).await;
    let first = match first {
        Next::Event(e) => e,
        _ => panic!("no event: a client resuming past the store's epoch is left with nothing"),
    };
    assert_eq!(first.event, "snapshot");

    let mut s = Stream::open(&app, "/_live?prefs=", Some("1000000"))
        .await
        .unwrap();
    match s.next_within(Duration::from_secs(1)).await {
        Next::Event(e) => assert_eq!(e.event, "prefs.snapshot"),
        _ => panic!("no event on /_live either"),
    }
}

/// On `/_live`, the event id holds one epoch per resource. Before a
/// resource has sent anything on this connection its slot reads `0`,
/// which a reconnect takes as "this client holds that resource as of
/// epoch 0", and while the hub still has every change since 0 it
/// replays them without a snapshot, on top of whatever the client
/// actually holds. A resource the connection never delivered must
/// start from a snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_multiplexed_resume_never_claims_a_resource_it_had_not_sent() {
    let app = app();
    let path = "/_live?prefs=&tags=";
    // Keeps both hubs running from epoch 0, so their rings go back to it.
    let mut keeper = Stream::open(&app, path, None).await.unwrap();
    keeper.next().await;
    keeper.next().await;
    send(&app, Method::PUT, "/tags/a", None).await;
    send(&app, Method::PUT, "/prefs/x", Some(json!(1))).await;

    // A client that drops after its first event: one resource sent, the
    // other not.
    let mut s = Stream::open(&app, path, None).await.unwrap();
    let first = s.next().await;
    drop(s);
    let other = if first.event.starts_with("prefs.") {
        "tags"
    } else {
        "prefs"
    };
    let id = first.id.unwrap();

    let mut s = Stream::open(&app, path, Some(&id)).await.unwrap();
    loop {
        let e = s.next().await;
        if let Some(kind) = e.event.strip_prefix(&format!("{other}.")) {
            assert_eq!(
                kind, "snapshot",
                "resumed with {id:?}: {other} was never sent on that connection, \
                 yet it was replayed from epoch 0 without a snapshot"
            );
            break;
        }
    }
}

/// A value whose `Serialize` fails for one input (a map with non-string
/// keys does this with serde_json; here, 13). Reading it is a 500 for
/// that request. Streaming it must not take the resource's live feed
/// down for good: later commits still reach clients, or their streams
/// end so they reconnect.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec, Deserialize, JsonSchema)]
struct Picky {
    n: u32,
}

impl Serialize for Picky {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.n == 13 {
            return Err(serde::ser::Error::custom("13 doesn't serialize"));
        }
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Picky", 1)?;
        st.serialize_field("n", &self.n)?;
        st.end()
    }
}

#[derive(Schema, Serve)]
struct Fussy {
    rows: Table<u32, Picky>,
}

impl Declare for Fussy {
    fn declare(_: &mut FussyBuilder) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_that_fails_to_serialize_does_not_wedge_its_feed() {
    let app = Server::new(Fussy::memory().build().unwrap()).into_router();
    let mut before = Stream::open(&app, "/rows", None).await.unwrap();
    assert_eq!(before.next().await.event, "snapshot");

    let poison = send(&app, Method::PUT, "/rows/1", Some(json!({"n": 13}))).await;
    assert!(poison.status.is_success(), "{:?}", poison.body);
    send(&app, Method::PUT, "/rows/2", Some(json!({"n": 14}))).await;

    // The client that was listening: the later change, or an end.
    let mut saw_later = false;
    loop {
        match before.next_within(Duration::from_secs(2)).await {
            Next::Event(e) if e.data.to_string().contains("\"n\":14") => {
                saw_later = true;
                break;
            }
            Next::Event(_) => {}
            Next::Ended => break,
            Next::Quiet => panic!(
                "the feed went silent after an unserializable change: \
                 later commits never arrive and the stream never ends"
            ),
        }
    }

    // A client joining afterwards (reading around the poisoned row).
    let mut after = Stream::open(&app, "/rows?order=desc&limit=1", None)
        .await
        .unwrap();
    assert_eq!(after.next().await.event, "snapshot");
    let put = send(&app, Method::PUT, "/rows/3", Some(json!({"n": 15}))).await;
    match after.next_within(Duration::from_secs(2)).await {
        Next::Event(e) => assert_eq!(e.epoch(), put.epoch()),
        Next::Ended => {}
        Next::Quiet => panic!(
            "a client joining after the bad change never hears of later ones \
             (saw the later change before: {saw_later})"
        ),
    }
}

// ---------------------------------------------------------------------
// Handlers that panic

/// A custom write that panics mid-transaction answers 500, and the next
/// write goes through: one bad handler mustn't make the server
/// read-only.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_custom_write_leaves_the_server_writable() {
    let app = server()
        .post(
            "/boom",
            "Panics mid-transaction",
            |tx: &mut common::ShopWrite<'_>, _: Value| -> bog::Result<()> {
                tx.prefs.upsert("half".into(), 1)?;
                panic!("handler bug");
            },
        )
        .into_router();
    let boom = send(&app, Method::POST, "/boom", Some(json!(null))).await;
    assert_eq!(
        boom.status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "{:?}",
        boom.body
    );
    assert_eq!(
        send(&app, Method::GET, "/prefs/half", None).await.status,
        StatusCode::NOT_FOUND,
        "the panicked transaction applied nothing"
    );
    let after = send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    assert!(after.status.is_success(), "{:?}", after.body);
}

/// A custom read that panics answers 500 as a problem; other reads,
/// and streams, carry on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_panicking_custom_read_is_a_problem() {
    let app = server()
        .get(
            "/boom",
            "Panics",
            |_: &common::ShopRead<'_>, _: NoParams| -> Result<(), Problem> {
                panic!("handler bug")
            },
        )
        .into_router();
    let boom = send(&app, Method::GET, "/boom", None).await;
    assert_eq!(boom.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(boom.headers[header::CONTENT_TYPE], bog_http::PROBLEM_JSON);
    assert_eq!(
        send(&app, Method::GET, "/prefs", None).await.status,
        StatusCode::OK
    );
}

// ---------------------------------------------------------------------
// Hostile input

/// JSON nested past serde_json's recursion limit, in a body, a `/_tx`
/// op, and a path key: a 400, not a stack overflow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deeply_nested_json_is_refused() {
    let app = app();
    let deep = "[".repeat(500_000);
    for path in ["/prefs/a", "/_tx", "/customers"] {
        let method = if path == "/prefs/a" {
            Method::PUT
        } else {
            Method::POST
        };
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(deep.clone()))
            .unwrap();
        let res = raw(&app, req).await;
        assert_eq!(res.status, StatusCode::BAD_REQUEST, "{path}");
    }
    let key = "%5B".repeat(10_000);
    let res = send(&app, Method::GET, &format!("/customers/{key}"), None).await;
    assert_eq!(res.status, StatusCode::BAD_REQUEST);
}

/// The body limit holds for a body sent without `content-length`
/// (chunked), not only for one that declares its size.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_body_limit_holds_for_streamed_bodies() {
    let app = server().body_limit(64 * 1024).into_router();
    let chunk = bytes_of(16 * 1024);
    let chunks = (0..16).map(move |_| Ok::<_, std::io::Error>(chunk.clone()));
    let req = Request::builder()
        .method(Method::POST)
        .uri("/_tx")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from_stream(futures_util::stream::iter(chunks)))
        .unwrap();
    let res = raw(&app, req).await;
    assert_eq!(res.status, StatusCode::PAYLOAD_TOO_LARGE);
}

fn bytes_of(n: usize) -> axum::body::Bytes {
    axum::body::Bytes::from(vec![b' '; n])
}

// ---------------------------------------------------------------------
// Access control

/// With hosts restricted, a request naming no host at all (HTTP/1.0, or
/// a hand-written client) is refused like one naming the wrong host.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_hosts_refuse_requests_naming_no_host() {
    let app = server().allowed_hosts(["localhost"]).into_router();
    let req = Request::get("/prefs").body(Body::empty()).unwrap();
    assert!(req.headers().get(header::HOST).is_none());
    let res = raw(&app, req).await;
    assert_eq!(res.status, StatusCode::FORBIDDEN, "{:?}", res.body);
}

/// The auth scheme is case-insensitive (RFC 9110 §11.1): `bearer x` is
/// the same credential as `Bearer x`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_bearer_scheme_is_case_insensitive() {
    let app = server().bearer_token("s3cret").into_router();
    for scheme in ["Bearer", "bearer", "BEARER"] {
        let auth = format!("{scheme} s3cret");
        let res = send_with(
            &app,
            Method::GET,
            "/prefs",
            None,
            &[("authorization", &auth)],
        )
        .await;
        assert_eq!(res.status, StatusCode::OK, "{scheme}");
    }
}

// ---------------------------------------------------------------------
// Admission control

/// Clients that give up on their writes (a timeout, a closed tab) don't
/// leak write permits: once the abandoned writes finish, the server is
/// back to full capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_writes_give_back_their_permits() {
    let app = server()
        .max_pending_writes(4)
        .post(
            "/slow",
            "A slow transaction",
            |tx: &mut common::ShopWrite<'_>, n: u32| {
                std::thread::sleep(Duration::from_millis(50));
                tx.prefs.upsert(format!("slow{n}"), n)
            },
        )
        .into_router();
    for n in 0..40u32 {
        let app = app.clone();
        // Give up almost at once; the write itself keeps running.
        let _ = tokio::time::timeout(
            Duration::from_millis(5),
            send(&app, Method::POST, "/slow", Some(json!(n))),
        )
        .await;
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let health = send(&app, Method::GET, "/_health", None).await;
        if health.body["pending_writes"] == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "permits never came back: {:?}",
            health.body
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let after = send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    assert!(after.status.is_success());
}
