//! The gallery's chat (fold2-demos `src/chat.rs`), as a live web API.
//!
//! The schema is the gallery's, unchanged, so this serves the gallery's
//! own data: point `FOLD_DIR` at a copy of `fold2-demos/data/chat` (never
//! the original while the gallery runs; a store has one writer). Then
//! open <http://127.0.0.1:10777>, in two windows to watch it push.
//!
//! ```sh
//! cp -R ../fold2-demos/data/chat /tmp/chat.fold
//! FOLD_DIR=/tmp/chat.fold cargo run --example chat
//! ```
//!
//! Everything the page does is plain fold-http:
//! - `GET /_live?messages=…&total=&by_author=`: one event stream carrying
//!   three resources: the newest messages and then each new one as it
//!   commits, and the two count views likewise. One connection, because
//!   browsers allow six per host and each open stream keeps one;
//! - `POST /send`: a custom route stamping the server's clock;
//! - `GET /find?q=`: a custom route over the search view, completing an
//!   unfinished last word from the index's own terms, as the gallery does.
//!
//! `CHAT_ORIGIN` admits one more browser origin through CORS (a dev
//! server, a desktop shell) besides the page served here.

use std::time::{SystemTime, UNIX_EPOCH};

use axum::response::Html;
use axum::routing::get;
use fold_http::{Problem, Serve, Server};
use fold3::schema::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A browser's id: 128 random bits, on the wire as 32 hex digits (a
/// JSON number can't hold it).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec)]
pub struct Author(pub u128);

impl Serialize for Author {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{:032x}", self.0))
    }
}

impl<'de> Deserialize<'de> for Author {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let hex = String::deserialize(d)?;
        if hex.len() != 32 {
            return Err(serde::de::Error::custom("an author is 32 hex digits"));
        }
        u128::from_str_radix(&hex, 16)
            .map(Author)
            .map_err(serde::de::Error::custom)
    }
}

impl JsonSchema for Author {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Author".into()
    }
    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^[0-9a-f]{32}$",
            "description": "A browser's id: 128 bits as 32 hex digits.",
        })
    }
}

#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Codec, Serialize, Deserialize, JsonSchema,
)]
pub struct Message {
    pub author: Author,
    pub text: String,
    /// Seconds since the Unix epoch.
    pub sent: u64,
}

#[derive(Schema, Serve)]
pub struct Chat {
    /// The conversation: append-only, ids minted in send order.
    pub messages: Events<Message>,
    /// Author → messages sent.
    #[view]
    pub by_author: MapView<Author, i64>,
    /// How many messages there are.
    #[view]
    pub total: ValueView<i64>,
    /// BM25 over every message's text.
    #[view]
    pub search: SearchView<Id<Message>>,
}

impl Declare for Chat {
    fn declare(b: &mut ChatBuilder) {
        b.messages.reject(|m| m.text.trim().is_empty());
        b.by_author.define(&b.messages.key_by(|m| m.author).count());
        b.total.define(&b.messages.count());
        b.search.define(&b.messages.map(|m| m.text.clone()));
    }
}

#[derive(Deserialize, JsonSchema)]
struct Send {
    author: Author,
    /// 1 to 1000 characters, not all whitespace.
    text: String,
}

#[derive(Serialize, JsonSchema)]
struct Sent {
    #[serde(with = "fold_http::id")]
    #[schemars(with = "u64")]
    id: Id<Message>,
}

#[derive(Deserialize, JsonSchema)]
struct Find {
    /// The search box: whole words, and a last word still being typed
    /// (no space after it yet) completed from the index's terms.
    q: String,
}

#[derive(Serialize, JsonSchema)]
struct Found {
    #[serde(with = "fold_http::id")]
    #[schemars(with = "u64")]
    id: Id<Message>,
    message: Message,
    score: f64,
}

const SHOWN: usize = 100;
const COMPLETIONS: usize = 16;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let dir = std::env::var("FOLD_DIR").unwrap_or_else(|_| "./chat.fold".into());
    let addr = std::env::var("CHAT_ADDR").unwrap_or_else(|_| "127.0.0.1:10777".into());
    let db = Chat::open(&dir).build().map_err(std::io::Error::other)?;
    // As the gallery does on open: settle any view rebuild before serving.
    db.checkpoint().map_err(std::io::Error::other)?;

    let server = Server::new(db)
        .title("Chat")
        .version("1")
        .description("The gallery's chat: an append-only log, live counts, and search.")
        .post("/send", "Send a message as an author", |tx, s: Send| {
            let text = s.text.trim();
            if text.chars().count() > 1000 {
                return Err(fold3::Error::reject(Rejected::new(
                    "send",
                    "at most 1000 characters",
                )));
            }
            let id = tx.messages.push(Message {
                author: s.author,
                text: text.into(),
                sent: now(),
            })?;
            Ok(Sent { id })
        })
        .get(
            "/find",
            "Search messages, best first",
            |r: &ChatRead<'_>, f: Find| {
                let failed = |e: fold3::Error| Problem::internal(e.to_string());
                let mut terms = Simple::tokenize(&f.q);
                if f.q.ends_with(char::is_alphanumeric)
                    && let Some(prefix) = terms.pop()
                {
                    terms.extend(
                        r.search
                            .complete(&prefix, COMPLETIONS)
                            .map_err(failed)?
                            .into_iter()
                            .map(|(t, _)| t),
                    );
                }
                let mut ranked = r.search.search(&terms.join(" "), SHOWN).map_err(failed)?;
                // Equal scores: the newer message first.
                ranked.sort_by(|(a, sa), (b, sb)| sb.total_cmp(sa).then(b.cmp(a)));
                let mut found = Vec::with_capacity(ranked.len());
                for (id, score) in ranked {
                    if let Some(message) = r.messages.get(&id).map_err(failed)? {
                        found.push(Found { id, message, score });
                    }
                }
                Ok(found)
            },
        );

    let page = || async { Html(include_str!("index.html")) };
    let mut origins = vec![format!("http://{addr}")];
    origins.extend(std::env::var("CHAT_ORIGIN").ok());
    eprintln!("chat: serving {dir} on http://{addr} (API: /openapi.json)");
    server
        .serve(&addr, |router| {
            router
                .route("/", get(page))
                .layer(fold_http::cors(origins.iter().map(String::as_str)))
        })
        .await
}
