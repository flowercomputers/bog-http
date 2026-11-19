//! [`Resource`]: how one schema field is served. Implemented on fold's
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
use fold3::schema::{Closed, Declare, Schema, SourceDecl, Subscription};
use schemars::{JsonSchema, SchemaGenerator};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::wire::{parse_body, parse_key, parse_query};
use crate::{Problem, Serve};

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
    /// The fold kind, `snake_case` (`table`, `events`, `map_view`, …).
    /// An open set: third-party kinds name themselves.
    pub kind: &'static str,
    /// The field's doc comment.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema of the key, for keyed resources.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<Value>,
    /// JSON Schema of the value (a set's member, a bag's element).
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

/// A typed write to source `D`, decoded on the request's thread and
/// applied inside the transaction.
pub type Apply<D> =
    Box<dyn for<'a> FnOnce(&<D as SourceDecl>::Write<'a>) -> fold3::Result<Written> + Send>;

/// Decode a write call into an [`Apply`].
pub type Stage<D> = fn(Call) -> Result<Apply<D>, Problem>;

/// An extra `POST` route a source gains from its key type (a
/// `Table<Id<V>, V>` minting ids), which its own impl can't express.
/// `M` is the field's encoders, as for [`Resource`].
pub struct Mint<D: SourceDecl, M> {
    pub stage: Stage<D>,
    pub route: fn(&mut SchemaGenerator) -> Route,
    pub via: PhantomData<fn() -> M>,
}

/// The op of a [`Mint`] route.
pub const MINT_OP: &str = "insert";

/// A resource's change feed: a fold3 [`Subscription`] whose changes
/// render as JSON. Each poll yields one commit's changes, with the epoch
/// it committed as.
pub struct Live {
    poll: Box<dyn FnMut(Duration) -> Result<Option<(u64, Vec<Value>)>, Closed> + Send>,
}

impl Live {
    /// Render `sub`'s changes with `rows`: each change becomes any number
    /// of rows (a replaced value is a retraction and an insertion).
    pub fn new<T: Send + 'static, I: IntoIterator<Item = Value>>(
        sub: Subscription<T>,
        rows: impl Fn(T) -> I + Send + 'static,
    ) -> Self {
        Live {
            poll: Box::new(move |wait| {
                Ok(sub.recv_timeout(wait)?.map(|commit| {
                    let out = commit.changes.into_iter().flat_map(&rows).collect();
                    (commit.epoch, out)
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

/// A schema field servable over HTTP, its keys and values encoded
/// through `M`: a pair of [`Via`](crate::Via) encoders, for its
/// [`Shape`](crate::Shape)'s key and value types.
pub trait Resource<M>: 'static {
    /// The read facade this resource is served from: the field's type in
    /// the schema's derived read struct, bound to one snapshot.
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

/// A source field: also writable.
pub trait SourceResource<M>: SourceDecl + Resource<M> {
    /// Decode a write route's call into a write on this source's write
    /// facade. Runs on the request's thread, so malformed requests never
    /// reach the transaction.
    fn stage(call: Call) -> Result<Apply<Self>, Problem>;
}

/// A whole transaction over schema `S`, staged: runs once, inside
/// [`Db::write`](fold3::schema::Db::write).
pub type TxFn<S> =
    Box<dyn for<'a> FnOnce(&mut <S as Schema>::Write<'a>) -> fold3::Result<Written> + Send>;

type ReadFn<S> =
    Box<dyn for<'a> Fn(&<S as Schema>::Read<'a>, &Call) -> Result<Value, Problem> + Send + Sync>;
type StageFn<S> = Box<dyn Fn(Call) -> Result<TxFn<S>, Problem> + Send + Sync>;
type DescribeFn = Box<dyn Fn(&str, &mut SchemaGenerator) -> Describe + Send + Sync>;
type LiveFn<S> = Box<dyn for<'a> Fn(&<S as Schema>::Read<'a>) -> Option<Live> + Send + Sync>;

/// Picks a field's read facade out of the schema's read struct and
/// hands it to `k`: `|r, k| k(&r.field)`. (A callback rather than a
/// returned reference, since the facade's lifetime appears only through
/// the schema's associated types.)
pub type ReadLens<S, D, M> =
    for<'r, 'a> fn(&'r <S as Schema>::Read<'a>, &mut dyn FnMut(&<D as Resource<M>>::Facade<'a>));

/// Picks a source's write facade out of the schema's write struct and
/// hands it to `k`: `|w, k| k(&w.field)`.
pub type WriteLens<S, D> =
    for<'r, 'a> fn(&'r <S as Schema>::Write<'a>, &mut dyn FnMut(&<D as SourceDecl>::Write<'a>));

/// One served field of schema `S`, type-erased. Built by
/// `#[derive(Serve)]`; also the escape hatch for hand-written impls.
pub struct Entry<S: Declare> {
    pub(crate) name: String,
    pub(crate) description: Option<String>,
    pub(crate) describe: DescribeFn,
    pub(crate) read: ReadFn<S>,
    pub(crate) live: LiveFn<S>,
    pub(crate) stage: Option<StageFn<S>>,
}

impl<S: Serve> Entry<S> {
    /// A read-only field: a view, or a source served
    /// `#[serve(read_only)]` (its write routes are dropped). Its key and
    /// value are encoded through `via` (see [`Resource`]).
    pub fn view<D: Resource<M>, M: 'static>(name: &str, via: M, facade: ReadLens<S, D, M>) -> Self {
        let _ = via;
        Entry {
            name: name.to_owned(),
            description: None,
            // A source served read-only describes its writes too: leave
            // them out of the router, `/_schema`, OpenAPI, and `/_tx`.
            describe: Box::new(|name, g| {
                let mut d = <D as Resource<M>>::describe(name, g);
                d.routes.retain(|r| r.method.is_read());
                d
            }),
            read: Box::new(move |r, call| {
                let mut out = None;
                facade(r, &mut |f| out = Some(<D as Resource<M>>::answer(f, call)));
                out.unwrap_or_else(|| Err(Problem::internal("read lens never called back")))
            }),
            live: Box::new(move |r| {
                let mut out = None;
                facade(r, &mut |f| out = <D as Resource<M>>::live(f));
                out
            }),
            stage: None,
        }
    }

    /// A source field, its key and value encoded through `via`, with
    /// its [`Mint`] route if it has one.
    pub fn source<D: SourceResource<M>, M: 'static>(
        name: &str,
        via: M,
        facade: ReadLens<S, D, M>,
        write: WriteLens<S, D>,
        mint: Option<Mint<D, M>>,
    ) -> Self {
        let _ = via;
        let (mint_stage, mint_route) = match mint {
            Some(m) => (Some(m.stage), Some(m.route)),
            None => (None, None),
        };
        Entry {
            name: name.to_owned(),
            description: None,
            describe: Box::new(move |name, g| {
                let describe = <D as Resource<M>>::describe(name, g);
                match mint_route {
                    Some(route) => describe.route(route(g)),
                    None => describe,
                }
            }),
            read: Box::new(move |r, call| {
                let mut out = None;
                facade(r, &mut |f| out = Some(<D as Resource<M>>::answer(f, call)));
                out.unwrap_or_else(|| Err(Problem::internal("read lens never called back")))
            }),
            live: Box::new(move |r| {
                let mut out = None;
                facade(r, &mut |f| out = <D as Resource<M>>::live(f));
                out
            }),
            stage: Some(Box::new(move |call| {
                let apply = match mint_stage {
                    Some(mint) if call.op == MINT_OP => mint(call)?,
                    _ => <D as SourceResource<M>>::stage(call)?,
                };
                Ok(Box::new(move |w: &mut S::Write<'_>| {
                    let mut apply = Some(apply);
                    let mut out = None;
                    write(w, &mut |dw| {
                        if let Some(apply) = apply.take() {
                            out = Some(apply(dw));
                        }
                    });
                    out.unwrap_or_else(|| {
                        Err(fold3::Error::reject(Problem::internal(
                            "write lens never called back",
                        )))
                    })
                }) as TxFn<S>)
            })),
        }
    }

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
