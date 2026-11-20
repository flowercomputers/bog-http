//! A Server-Sent Events client over the router, in process, for tests
//! that hold many streams and need to tell "quiet" from "ended".

#![allow(dead_code)]

use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

#[derive(Debug, Clone)]
pub struct Event {
    pub event: String,
    /// As sent: an epoch, `e1.e2…` on `/_live`, or `-` mid-snapshot.
    pub id: Option<String>,
    pub data: Value,
}

impl Event {
    pub fn epoch(&self) -> u64 {
        self.data["epoch"]
            .as_u64()
            .expect("every event carries an epoch")
    }
}

pub enum Next {
    Event(Event),
    /// Nothing arrived in time; the stream is still open.
    Quiet,
    /// The server ended the stream.
    Ended,
}

pub struct Stream {
    body: Body,
    buf: Vec<u8>,
}

impl Stream {
    pub async fn open(
        app: &Router,
        path: &str,
        last_event_id: Option<&str>,
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
        Ok(Stream {
            body: res.into_body(),
            buf: Vec::new(),
        })
    }

    /// The next event within `wait`, skipping keep-alive comments.
    pub async fn next_within(&mut self, wait: Duration) -> Next {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            if let Some(end) = self.buf.windows(2).position(|w| w == b"\n\n") {
                let raw: Vec<u8> = self.buf.drain(..end + 2).collect();
                if let Some(event) = parse(&raw) {
                    return Next::Event(event);
                }
                continue;
            }
            match tokio::time::timeout_at(deadline, self.body.frame()).await {
                Err(_) => return Next::Quiet,
                Ok(None) | Ok(Some(Err(_))) => return Next::Ended,
                Ok(Some(Ok(frame))) => {
                    if let Ok(bytes) = frame.into_data() {
                        self.buf.extend_from_slice(&bytes);
                    }
                }
            }
        }
    }

    /// The next event, failing the test if none comes within 5 s.
    pub async fn next(&mut self) -> Event {
        match self.next_within(Duration::from_secs(5)).await {
            Next::Event(e) => e,
            Next::Quiet => panic!("no event within 5s"),
            Next::Ended => panic!("the stream ended"),
        }
    }
}

fn parse(raw: &[u8]) -> Option<Event> {
    let raw = std::str::from_utf8(raw).expect("utf-8 events");
    let (mut event, mut id, mut data) = (String::from("message"), None, String::new());
    for line in raw.lines() {
        if let Some(v) = line.strip_prefix("event:") {
            event = v.trim().into();
        } else if let Some(v) = line.strip_prefix("id:") {
            id = Some(v.trim().to_owned());
        } else if let Some(v) = line.strip_prefix("data:") {
            data += v.trim_start();
        }
    }
    if data.is_empty() {
        return None; // a comment
    }
    Some(Event {
        event,
        id,
        data: serde_json::from_str(&data).expect("event data is JSON"),
    })
}
