//! A tiny shop schema and an in-process request helper.

#![allow(dead_code)]

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use fold::Id;
use fold::schema::*;
use fold::storage::Codec;
use fold_http::Serve;
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tower::ServiceExt;

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec, Serialize, Deserialize, JsonSchema,
)]
pub struct Customer {
    pub name: String,
    pub region: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec, Serialize, Deserialize, JsonSchema,
)]
pub struct Order {
    pub customer: Id<Customer>,
    pub cents: isize,
}

#[derive(Schema, Serve)]
pub struct Shop {
    customers: Table<Id<Customer>, Customer>,
    prefs: Table<String, u32>,
    orders: Events<Order>,
    /// Customer → total spend.
    #[view]
    spend: MapView<Id<Customer>, isize>,
    #[view]
    #[serve(skip)]
    by_region: MapView<String, isize>,
}

impl Declare for Shop {
    type Error = Rejected;

    fn declare(b: &mut ShopBuilder<Rejected>) {
        b.orders.reject(|o| o.cents <= 0);
        b.orders.foreign_key(&b.customers, |o| o.customer);
        b.spend
            .define(&b.orders.key_by(|o| o.customer).map(|o| o.cents).sum());
        b.by_region
            .define(&b.customers.key_by(|c| c.region.clone()).map(|_| 1).sum());
    }
}

pub fn app() -> Router {
    fold_http::Server::new(Shop::memory().build().unwrap()).into_router()
}

pub struct Res {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Value,
}

impl Res {
    pub fn epoch(&self) -> u64 {
        self.body["epoch"].as_u64().expect("an epoch")
    }
    pub fn data(&self) -> &Value {
        &self.body["data"]
    }
    pub fn header(&self, name: impl header::AsHeaderName) -> &str {
        self.headers
            .get(name)
            .expect("header present")
            .to_str()
            .unwrap()
    }
}

/// One request against the router, in process.
pub async fn send(app: &Router, method: Method, path: &str, body: Option<Value>) -> Res {
    let mut req = Request::builder().method(method).uri(path);
    let body = match body {
        Some(json) => {
            req = req.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(&json).unwrap())
        }
        None => Body::empty(),
    };
    raw(app, req.body(body).unwrap()).await
}

pub async fn raw(app: &Router, req: Request<Body>) -> Res {
    let res = app.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()))
    };
    Res {
        status,
        headers,
        body,
    }
}
