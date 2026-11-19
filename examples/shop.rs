//! A served shop: `cargo run --example shop`, then
//!
//! ```sh
//! curl -s localhost:10777/customers -H 'content-type: application/json' \
//!      -d '{"name":"ada","region":"eu"}'
//! curl -s localhost:10777/orders -H 'content-type: application/json' \
//!      -d '{"customer":<id>,"cents":1250}'
//! curl -s localhost:10777/spend
//! curl -s localhost:10777/_schema
//! ```
//!
//! Data lives in `./shop.bog` (override with `BOG_DIR`).

use bog_http::{Serve, Server};
use bog::schema::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec, Serialize, Deserialize, JsonSchema,
)]
struct Customer {
    name: String,
    region: String,
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec, Serialize, Deserialize, JsonSchema,
)]
struct Order {
    customer: Id<Customer>,
    cents: i64,
}

#[derive(Schema, Serve)]
struct Shop {
    customers: Table<Id<Customer>, Customer>,
    orders: Events<Order>,
    /// Customer → total spend, maintained on every write.
    #[view]
    spend: MapView<Id<Customer>, i64>,
    /// Region → revenue.
    #[view]
    revenue: MapView<String, i64>,
}

impl Declare for Shop {
    fn declare(b: &mut ShopBuilder) {
        b.orders.reject(|o| o.cents <= 0);
        b.orders.foreign_key(&b.customers, |o| o.customer);
        b.spend
            .define(&b.orders.key_by(|o| o.customer).map(|o| o.cents).sum());
        b.revenue.define(
            &b.orders
                .key_by(|o| o.customer)
                .with(&b.customers, |_, o, c| (c.region.clone(), o.cents))
                .key_by(|(region, _)| region.clone())
                .map(|(_, cents)| *cents)
                .sum(),
        );
    }
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let dir = std::env::var("BOG_DIR").unwrap_or_else(|_| "./shop.bog".into());
    let db = Shop::open(&dir).build().map_err(std::io::Error::other)?;
    let app = Server::new(db).into_router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:10777").await?;
    eprintln!("serving {dir} on http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
}
