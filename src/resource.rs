//! [`Resource`]: how one schema field is served. Implemented on bog's
//! declaration types (`Table<K, V>`, `MapView<K, V>`, …), so the derive
//! never inspects a field's type — it asks the type.
//!
//! A resource describes itself as a list of [`Route`]s. The same list
//! mounts the router, fills `/_schema`, and generates the OpenAPI
//! document, so the three can't drift apart.

use std::borrow::Cow;
use std::marker::PhantomData;
use std::time::Duration;

use axum::body::Bytes;
use bog::schema::{Closed, Schema, SourceDecl, Subscription};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::wire::{parse_body, parse_key, parse_query};
use crate::{Problem, Serve};

/// A type that crosses the wire: JSON both ways, with a JSON Schema.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be served: it needs serde and a JSON Schema",
    label = "served here",
    note = "derive `serde::Serialize`, `serde::Deserialize`, and `schemars::JsonSchema` for `{Self}`, or #[serve(skip)] the field"
)]
pub trait Wire: Serialize + DeserializeOwned + JsonSchema + Send + Sync + 'static {}
impl<T: Serialize + DeserializeOwned + JsonSchema + Send + Sync + 'static> Wire for T {}

/// An HTTP method a route answers. `GET` routes read from a snapshot;
/// every other method is a write transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Method {
    pub fn is_read(self) -> bool {
        self == Method::Get
    }

    /// Lowercase, as OpenAPI keys operations.
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "get",
            Method::Post => "post",
            Method::Put => "put",
            Method::Delete => "delete",
        }
    }
}

/// One route of a resource, and everything needed to document it.
#[derive(Debug, Clone, Serialize)]
pub struct Route {
    pub method: Method,
    /// Relative to the resource: `""`, `"/{key}"`, or a static suffix
    /// such as `"/complete"`. Only `{key}` may be a parameter.
    pub path: Cow<'static, str>,
    /// Identifies the operation to [`Resource::answer`] or
    /// [`SourceResource::stage`].
    pub op: &'static str,
    pub summary: Cow<'static, str>,
    /// JSON Schema of the `{key}` path parameter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    /// JSON Schema (an inline object) of the query parameters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<Value>,
    /// JSON Schema of the request body.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Value>,
    /// JSON Schema of the response's `data`.
    pub response: Value,
    /// Succeeds with `201 Created` when it creates (else `200`).
    pub creates: bool,
    /// May answer `404`.
    pub may_404: bool,
    /// Also streams: with `Accept: text/event-stream` this `GET` answers
    /// with its usual data as a `snapshot` event, then a `change` event
    /// per commit (see [`Resource::live`]).
    pub live: bool,
}

impl Route {
    pub fn new(
        method: Method,
        path: impl Into<Cow<'static, str>>,
        op: &'static str,
        summary: impl Into<Cow<'static, str>>,
    ) -> Self {
        Route {
            method,
            path: path.into(),
            op,
            summary: summary.into(),
            key: None,
            query: None,
            body: None,
            response: Value::Bool(true),
            creates: false,
            may_404: false,
            live: false,
        }
    }

    pub fn get(
        path: impl Into<Cow<'static, str>>,
        op: &'static str,
        summary: impl Into<Cow<'static, str>>,
    ) -> Self {
        Route::new(Method::Get, path, op, summary)
    }

    pub fn post(
        path: impl Into<Cow<'static, str>>,
        op: &'static str,
        summary: impl Into<Cow<'static, str>>,
    ) -> Self {
        Route::new(Method::Post, path, op, summary)
    }

    pub fn put(
        path: impl Into<Cow<'static, str>>,
        op: &'static str,
        summary: impl Into<Cow<'static, str>>,
    ) -> Self {
        Route::new(Method::Put, path, op, summary)
    }

    pub fn delete(
        path: impl Into<Cow<'static, str>>,
        op: &'static str,
        summary: impl Into<Cow<'static, str>>,
    ) -> Self {
        Route::new(Method::Delete, path, op, summary)
    }

    /// The `{key}` path parameter is a `K`.
    pub fn key<K: JsonSchema>(mut self, g: &mut SchemaGenerator) -> Self {
        self.key = Some(schema_in::<K>(g));
        self
    }

    /// Query parameters, described by `Q`'s (inlined) schema.
    pub fn query<Q: JsonSchema>(mut self) -> Self {
        self.query = Some(inline_schema::<Q>());
        self
    }

    pub fn body<B: JsonSchema>(mut self, g: &mut SchemaGenerator) -> Self {
        self.body = Some(schema_in::<B>(g));
        self
    }

    pub fn response<R: JsonSchema>(mut self, g: &mut SchemaGenerator) -> Self {
        self.response = schema_in::<R>(g);
        self
    }

    pub fn creates(mut self) -> Self {
        self.creates = true;
        self
    }

    pub fn may_404(mut self) -> Self {
        self.may_404 = true;
        self
    }

    /// Mark a `GET` as streamable; see [`Route::live`](Route#structfield.live).
    pub fn live(mut self) -> Self {
        self.live = true;
        self
    }
}

/// A resource's self-description.
#[derive(Debug, Clone, Serialize)]
pub struct Describe {
    pub name: String,
    /// The bog kind, `snake_case` (`table`, `events`, `map_view`, …).
    /// An open set: third-party kinds name themselves.
    pub kind: &'static str,
    /// The field's doc comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema of the key, for keyed resources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    /// JSON Schema of the value (a set's member, a multiset's element).
    pub value: Value,
    pub routes: Vec<Route>,
}

impl Describe {
    pub fn new(name: &str, kind: &'static str, value: Value) -> Self {
        Describe {
            name: name.to_owned(),
            kind,
            description: None,
            key: None,
            value,
            routes: Vec::new(),
        }
    }

    pub fn keyed(mut self, key: Value) -> Self {
        self.key = Some(key);
        self
    }

    pub fn route(mut self, route: Route) -> Self {
        self.routes.push(route);
        self
    }
}

/// `T`'s schema within the document `g` is building: a `$ref` into
/// `components/schemas` for named types, inline for primitives.
pub fn schema_in<T: JsonSchema>(g: &mut SchemaGenerator) -> Value {
    serde_json::to_value(g.subschema_for::<T>()).expect("a JSON Schema always serializes")
}

/// `T`'s schema with every subschema inlined: for query parameters,
/// which OpenAPI lists one by one.
pub fn inline_schema<T: JsonSchema>() -> Value {
    let mut settings = schemars::generate::SchemaSettings::draft2020_12();
    settings.inline_subschemas = true;
    settings.meta_schema = None;
    let schema = settings.into_generator().into_root_schema_for::<T>();
    serde_json::to_value(schema).expect("a JSON Schema always serializes")
}

/// One request to a resource, routed but not yet typed.
#[derive(Debug, Clone)]
pub struct Call {
    /// The [`Route::op`] that matched.
    pub op: &'static str,
    /// The raw (percent-decoded) `{key}` segment.
    pub key: Option<String>,
    /// The raw query string.
    pub query: Option<String>,
    pub body: Bytes,
}

impl Call {
    /// The `{key}` segment as a `K`; see [`parse_key`].
    pub fn key<K: DeserializeOwned>(&self) -> Result<K, Problem> {
        let segment = self
            .key
            .as_deref()
            .ok_or_else(|| Problem::internal(format!("route {} has no key", self.op)))?;
        parse_key(segment)
    }

    pub fn query<Q: DeserializeOwned>(&self) -> Result<Q, Problem> {
        parse_query(self.query.as_deref())
    }

    pub fn body<B: DeserializeOwned>(&self) -> Result<B, Problem> {
        parse_body(&self.body)
    }

    /// The answer for an op this resource never declared.
    pub fn unrouted(&self) -> Problem {
        Problem::internal(format!("no handler for op {:?}", self.op))
    }
}

/// The result of an applied write.
#[derive(Debug, Clone)]
pub struct Written {
    pub data: Value,
    /// The write created what it addressed: `201 Created`.
    pub created: bool,
}

impl Written {
    pub fn ok<T: Serialize>(data: &T) -> Self {
        Written {
            data: serde_json::to_value(data).unwrap_or(Value::Null),
            created: false,
        }
    }

    pub fn created_if(mut self, created: bool) -> Self {
        self.created = created;
        self
    }
}

/// A typed write on source `D`'s write facade, decoded on the request's
/// thread and applied inside the transaction.
pub type Apply<D> =
    Box<dyn for<'a> FnOnce(&<D as SourceDecl>::Write<'a>) -> bog::Result<Written> + Send>;

/// Decode a write call into an [`Apply`].
pub type Stage<D> = fn(Call) -> Result<Apply<D>, Problem>;

/// A whole transaction on schema `S`, staged: runs once, inside
/// [`Db::write`](bog::schema::Db::write).
pub type TxFn<S> =
    Box<dyn for<'a> FnOnce(&mut <S as Schema>::Write<'a>) -> bog::Result<Written> + Send>;

/// An extra `POST` route a source gains from its key type (a
/// `Table<Id<V>, V>` minting ids), which its own impl can't express.
pub struct Mint<D: SourceDecl> {
    pub stage: Stage<D>,
    pub route: fn(&mut SchemaGenerator) -> Route,
}

/// The op of a [`Mint`] route.
pub const MINT_OP: &str = "insert";

/// A resource's change feed: a bog [`Subscription`] whose changes
/// render as JSON. Each poll yields one commit's changes with the epoch
/// it committed as.
///
/// [`Subscription`]: bog::schema::Subscription
pub struct Live {
    poll: Box<dyn FnMut(Duration) -> Result<Option<(u64, Vec<Value>)>, Closed> + Send>,
}

impl Live {
    /// Render each change of `sub` with `change`.
    pub fn new<T: 'static>(
        sub: Subscription<T>,
        change: impl Fn(T) -> Value + Send + 'static,
    ) -> Self {
        Live {
            poll: Box::new(move |wait| {
                Ok(sub
                    .recv_timeout(wait)?
                    // a commit whose changes all cancelled out says nothing
                    .filter(|commit| !commit.changes.is_empty())
                    .map(|commit| {
                        let changes = commit.changes.into_iter().map(&change).collect();
                        (commit.epoch, changes)
                    }))
            }),
        }
    }

    /// The next commit's `(epoch, changes)`, waiting up to `wait`; `Err`
    /// once the database has closed.
    pub fn poll(&mut self, wait: Duration) -> Result<Option<(u64, Vec<Value>)>, Closed> {
        (self.poll)(wait)
    }
}

/// A schema field servable over HTTP.
pub trait Resource: 'static {
    /// The read facade this resource is served from: the field's type in
    /// the schema's derived read struct.
    type Facade<'a>;

    /// Describe the resource; schemas go through `g` so the whole
    /// document shares one set of components.
    fn describe(name: &str, g: &mut SchemaGenerator) -> Describe;

    /// Answer a `GET` route from one snapshot's facade.
    fn answer(facade: &Self::Facade<'_>, call: &Call) -> Result<Value, Problem>;

    /// The resource's change feed, subscribed at the facade's snapshot,
    /// for its [`live`](Route::live) routes. `None`: not streamable.
    fn live(facade: &Self::Facade<'_>) -> Option<Live> {
        let _ = facade;
        None
    }
}

/// A source field: also writable. Its [`Facade`](Resource::Facade) is
/// its [`SourceDecl::Read`].
pub trait SourceResource: SourceDecl + Resource {
    /// Decode a write route's call into a write on this source's write
    /// facade. Runs on the request's thread, so malformed requests never
    /// open a transaction.
    fn stage(call: Call) -> Result<Apply<Self>, Problem>;
}

/// The second argument of a field accessor: it names the facades'
/// lifetime `'a`, which a `fn` pointer can't bind from `S::Read<'a>`
/// alone, a projection. Pass `PhantomData`.
pub type Tie<'a> = PhantomData<&'a ()>;

type ReadFn<S> =
    Box<dyn for<'a> Fn(&<S as Schema>::Read<'a>, &Call) -> Result<Value, Problem> + Send + Sync>;
type StageFn<S> = Box<dyn Fn(Call) -> Result<TxFn<S>, Problem> + Send + Sync>;
type DescribeFn = Box<dyn Fn(&str, &mut SchemaGenerator) -> Describe + Send + Sync>;
type LiveFn<S> = Box<dyn for<'a> Fn(&<S as Schema>::Read<'a>) -> Option<Live> + Send + Sync>;

/// One served field of schema `S`, type-erased. Built by
/// `#[derive(Serve)]`; also the escape hatch for hand-written impls.
pub struct Entry<S: Schema> {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) describe: DescribeFn,
    pub(crate) read: ReadFn<S>,
    pub(crate) live: LiveFn<S>,
    pub(crate) stage: Option<StageFn<S>>,
}

impl<S: Serve> Entry<S> {
    /// A read-only field: a view, or a source served
    /// `#[serve(read_only)]` (its write routes are dropped).
    pub fn view<D: Resource>(
        name: &str,
        facade: for<'r, 'a> fn(&'r S::Read<'a>, Tie<'a>) -> &'r D::Facade<'a>,
    ) -> Self {
        Entry {
            name: name.to_owned(),
            description: None,
            // A source served read-only describes its writes too: leave
            // them out of the router, `/_schema`, OpenAPI, and `/_tx`.
            describe: Box::new(|name, g| {
                let mut d = D::describe(name, g);
                d.routes.retain(|r| r.method.is_read());
                d
            }),
            read: Box::new(move |r: &S::Read<'_>, call: &Call| {
                D::answer(facade(r, PhantomData), call)
            }),
            live: Box::new(move |r: &S::Read<'_>| D::live(facade(r, PhantomData))),
            stage: None,
        }
    }

    /// A source field, with its [`Mint`] route if it has one.
    pub fn source<D: SourceResource>(
        name: &str,
        facade: for<'r, 'a> fn(&'r S::Read<'a>, Tie<'a>) -> &'r D::Facade<'a>,
        write: for<'r, 'a> fn(&'r S::Write<'a>, Tie<'a>) -> &'r <D as SourceDecl>::Write<'a>,
        mint: Option<Mint<D>>,
    ) -> Self {
        let (mint_stage, mint_route) = match mint {
            Some(m) => (Some(m.stage), Some(m.route)),
            None => (None, None),
        };
        Entry {
            name: name.to_owned(),
            description: None,
            describe: Box::new(move |name, g| {
                let describe = D::describe(name, g);
                match mint_route {
                    Some(route) => describe.route(route(g)),
                    None => describe,
                }
            }),
            read: Box::new(move |r: &S::Read<'_>, call: &Call| {
                D::answer(facade(r, PhantomData), call)
            }),
            live: Box::new(move |r: &S::Read<'_>| D::live(facade(r, PhantomData))),
            stage: Some(Box::new(move |call| {
                let apply = match mint_stage {
                    Some(mint) if call.op == MINT_OP => mint(call)?,
                    _ => D::stage(call)?,
                };
                let tx: TxFn<S> =
                    Box::new(move |w: &mut S::Write<'_>| apply(write(w, PhantomData)));
                Ok(tx)
            })),
        }
    }

    /// Document the field (the derive passes its doc comment).
    pub fn description(mut self, text: &str) -> Self {
        let text = text.trim();
        if !text.is_empty() {
            self.description = Some(text.to_owned());
        }
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
