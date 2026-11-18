//! A tiny shop schema and an in-process request helper.

#![allow(dead_code)]

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode, header};
use fold_http::Serve;
use fold3::schema::*;
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
    #[serde(with = "fold_http::id")]
    #[schemars(with = "u64")]
    pub customer: Id<Customer>,
    pub cents: i64,
}

#[derive(Schema, Serve)]
pub struct Shop {
    pub customers: Table<Id<Customer>, Customer>,
    pub prefs: Table<String, u32>,
    pub orders: Events<Order>,
    /// Labels on the shop.
    pub tags: Set<String>,
    pub motd: Cell<String>,
    pub stock: Multiset<String>,
    /// Customer → total spend.
    #[view]
    pub spend: MapView<Id<Customer>, i64>,
    /// Customers by spend.
    #[view]
    pub ranked: OrderedView<Id<Customer>, i64>,
    #[view]
    pub tags_by_len: GroupView<u64, String>,
    #[view]
    pub order_count: ValueView<i64>,
    /// Customers by name.
    #[view]
    pub names: SearchView<Id<Customer>>,
    #[view]
    #[serve(skip)]
    pub by_region: MapView<String, i64>,
}

impl Declare for Shop {
    fn declare(b: &mut ShopBuilder) {
        b.orders.reject(|o| o.cents <= 0);
        b.orders.foreign_key(&b.customers, |o| o.customer);
        let spend = b.orders.key_by(|o| o.customer).map(|o| o.cents).sum();
        b.spend.define(&spend);
        b.ranked.define(&spend);
        b.tags_by_len
            .define(&b.tags.map_with(|t, _| t.clone()).key_by(|t| t.len() as u64));
        b.order_count.define(&b.orders.count());
        b.names.define(&b.customers.map(|c| c.name.clone()));
        b.by_region
            .define(&b.customers.key_by(|c| c.region.clone()).map(|_| 1).sum());
    }
}

pub fn server() -> fold_http::Server<Shop> {
    fold_http::Server::new(Shop::memory().build().unwrap())
}

pub fn app() -> Router {
    server().into_router()
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
    send_with(app, method, path, body, &[]).await
}

/// [`send`] with extra request headers.
pub async fn send_with(
    app: &Router,
    method: Method,
    path: &str,
    body: Option<Value>,
    headers: &[(&str, &str)],
) -> Res {
    let mut req = Request::builder().method(method).uri(path);
    for (name, value) in headers {
        req = req.header(*name, *value);
    }
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
