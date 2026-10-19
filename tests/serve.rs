#![cfg(feature = "serve")]
//! Serving for real: sockets, the discovery file, idle exit.

mod common;

use std::io::{Read, Write};
use std::time::Duration;

use common::server;
use serde_json::Value;

/// One HTTP/1.1 request over a fresh connection, closed after.
fn http(
    stream: &mut impl ReadWrite,
    method: &str,
    path: &str,
    host: &str,
    body: Option<&str>,
) -> (u16, String) {
    let body = body.unwrap_or("");
    let ct = if body.is_empty() {
        ""
    } else {
        "content-type: application/json\r\n"
    };
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nhost: {host}\r\nconnection: close\r\n{ct}content-length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut out = String::new();
    stream.read_to_string(&mut out).unwrap();
    let status = out[9..12].parse().unwrap();
    let body = out
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

trait ReadWrite: Read + Write {}
impl<T: Read + Write> ReadWrite for T {}

fn read_json(path: &std::path::Path) -> Value {
    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

/// Wait for a file to appear (the server is listening).
async fn appears(path: &std::path::Path) {
    for _ in 0..200 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{} never appeared", path.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tcp_discovery_host_check_and_idle_exit() {
    let dir = tempfile::tempdir().unwrap();
    let found = dir.path().join("shop.serve.json");
    let serving = tokio::spawn(
        server()
            .discovery(&found)
            .idle_exit(Duration::from_millis(400))
            .serve("127.0.0.1:0", |r| r),
    );
    appears(&found).await;
    let advert = read_json(&found);
    assert_eq!(advert["pid"], std::process::id());
    assert_eq!(advert["auth"], false);
    let url = advert["url"].as_str().unwrap().to_owned();
    let addr = url.strip_prefix("http://").unwrap().to_owned();

    let (status, body) = tokio::task::spawn_blocking({
        let addr = addr.clone();
        move || {
            http(
                &mut std::net::TcpStream::connect(&addr).unwrap(),
                "PUT",
                "/prefs/a",
                "localhost",
                Some("1"),
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(status, 201, "{body}");

    // A loopback bind answers only to loopback names: no DNS rebinding.
    let (status, _) = tokio::task::spawn_blocking({
        let addr = addr.clone();
        move || {
            http(
                &mut std::net::TcpStream::connect(&addr).unwrap(),
                "GET",
                "/prefs",
                "evil.example",
                None,
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(status, 403);

    // Then nothing for a while: it exits on its own, and tidies up.
    let done = tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("idle exit");
    done.unwrap().unwrap();
    assert!(!found.exists(), "the discovery file goes with the server");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unix_sockets_are_private_and_not_stolen() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::UnixStream;

    let dir = tempfile::tempdir().unwrap();
    let sock = dir.path().join("shop.sock");
    let found = dir.path().join("shop.serve.json");
    let serving = tokio::spawn(
        server()
            .discovery(&found)
            .idle_exit(Duration::from_millis(400))
            .serve_unix(sock.clone(), |r| r),
    );
    appears(&found).await;
    assert_eq!(read_json(&found)["socket"], sock.display().to_string());
    let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o600, "owner-only");

    let (status, body) = tokio::task::spawn_blocking({
        let sock = sock.clone();
        move || {
            http(
                &mut UnixStream::connect(&sock).unwrap(),
                "PUT",
                "/tags/x",
                "localhost",
                None,
            )
        }
    })
    .await
    .unwrap();
    assert_eq!(status, 201, "{body}");

    // A second server can't take a socket someone is listening on.
    let err = server().serve_unix(sock.clone(), |r| r).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);

    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("idle exit")
        .unwrap()
        .unwrap();
    assert!(!sock.exists() && !found.exists());

    // A stale socket file (its server gone) is replaced, not refused.
    drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
    assert!(sock.exists());
    let again = tokio::spawn(
        server()
            .idle_exit(Duration::from_millis(100))
            .serve_unix(sock.clone(), |r| r),
    );
    tokio::time::timeout(Duration::from_secs(5), again)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_stream_keeps_an_idle_server_up() {
    let dir = tempfile::tempdir().unwrap();
    let found = dir.path().join("s.json");
    let serving = tokio::spawn(
        server()
            .discovery(&found)
            .idle_exit(Duration::from_millis(200))
            .serve("127.0.0.1:0", |r| r),
    );
    appears(&found).await;
    let addr = read_json(&found)["url"]
        .as_str()
        .unwrap()
        .strip_prefix("http://")
        .unwrap()
        .to_owned();
    let mut stream = std::net::TcpStream::connect(&addr).unwrap();
    write!(
        stream,
        "GET /prefs HTTP/1.1\r\nhost: localhost\r\naccept: text/event-stream\r\n\r\n"
    )
    .unwrap();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert!(!serving.is_finished(), "a live stream counts as use");
    drop(stream);
    tokio::time::timeout(Duration::from_secs(5), serving)
        .await
        .expect("exits once the stream closes")
        .unwrap()
        .unwrap();
}
