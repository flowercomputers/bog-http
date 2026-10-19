//! [`Resource`]: how one schema field is served. Implemented on fold's
//! declaration types (`Table<K, V>`, `MapView<K, V>`, …), so the derive
//! never inspects a field's type — it asks the type.

use axum::body::Bytes;
use fold::schema::Error;
use fold::{Declare, SourceDecl, WriteOf};
use schemars::JsonSchema;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::wire::ListParams;
use crate::{Problem, Serve};

/// A type that crosses the wire: JSON both ways, with a JSON Schema.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be served: it needs serde and a JSON Schema",
    label = "served here",
    note = "derive `serde::Serialize`, `serde::Deserialize`, and `schemars::JsonSchema` for `{Self}`, or #[serve(skip)] the field"
)]
pub trait Wire: Serialize + DeserializeOwned + JsonSchema + Send + Sync + 'static {}
impl<T: Serialize + DeserializeOwned + JsonSchema + Send + Sync + 'static> Wire for T {}

/// What a resource supports, by route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Ops {
    /// `GET /{name}`
    pub list: bool,
    /// `GET /{name}/{key}`
    pub get: bool,
    /// `POST /{name}`: append, minting the key.
    pub insert: bool,
    /// `PUT /{name}/{key}`
    pub upsert: bool,
    /// `DELETE /{name}/{key}`
    pub remove: bool,
}

/// A resource's self-description: what `/_schema` reports and what the
/// router mounts.
#[derive(Debug, Clone, Serialize)]
pub struct Describe {
    pub name: String,
    /// The fold kind, `snake_case` (`table`, `events`, `map_view`, …).
    /// An open set: third-party kinds name themselves.
    pub kind: &'static str,
    /// JSON Schema of the key, for keyed resources.
    pub key: Option<Value>,
    /// JSON Schema of the value.
    pub value: Value,
    pub ops: Ops,
}

/// The JSON Schema of `T`, as a value.
pub fn schema_of<T: JsonSchema>() -> Value {
    serde_json::to_value(schemars::schema_for!(T)).expect("a JSON Schema always serializes")
}

/// A read request, decoded from route and query but not yet typed.
#[derive(Debug, Clone)]
pub enum ReadOp {
    List(ListParams),
    /// The raw (percent-decoded) key path segment.
    Get(String),
}

/// A write request, decoded from route but not yet typed.
#[derive(Debug, Clone)]
pub enum WriteOp {
    Insert(Bytes),
    Upsert(String, Bytes),
    Remove(String),
}

/// The result of an applied write.
#[derive(Debug, Clone)]
pub struct Written {
    pub data: Value,
    /// The write created what it addressed: `201 Created`.
    pub created: bool,
}

/// A typed write, decoded off the writer thread and applied on it.
pub type Apply<W, E> = Box<dyn FnOnce(&mut W) -> Result<Written, E> + Send>;

/// Decode a write request into an [`Apply`].
pub type Stage<W, E> = fn(WriteOp) -> Result<Apply<W, E>, Problem>;

/// A schema field servable over HTTP.
pub trait Resource: 'static {
    /// The read facade this resource is served from: the field's type in
    /// the schema's derived read struct.
    type Facade;

    fn describe(name: &str) -> Describe;

    /// Answer a read from one snapshot's facade.
    fn answer(facade: &Self::Facade, op: &ReadOp) -> Result<Value, Problem>;
}

/// A source field: also writable.
pub trait SourceResource: SourceDecl + Resource<Facade = <Self as SourceDecl>::Read> {
    /// Decode `op` into a write on this source's write facade. Runs on
    /// the request's thread, so malformed requests never reach the
    /// writer.
    fn stage<E: Error>(op: WriteOp) -> Result<Apply<Self::Write<E>, E>, Problem>;
}

type ReadFn<S> =
    Box<dyn Fn(&<S as fold::Schema>::Read, &ReadOp) -> Result<Value, Problem> + Send + Sync>;
type StageFn<S> =
    Box<dyn Fn(WriteOp) -> Result<Apply<WriteOf<S>, <S as Declare>::Error>, Problem> + Send + Sync>;

/// One served field of schema `S`, type-erased. Built by
/// `#[derive(Serve)]`; also the escape hatch for hand-written impls.
pub struct Entry<S: Declare> {
    pub(crate) describe: Describe,
    pub(crate) read: ReadFn<S>,
    pub(crate) stage: Option<StageFn<S>>,
}

impl<S: Serve> Entry<S> {
    /// A read-only field: a view.
    pub fn view<D: Resource>(name: &str, facade: fn(&S::Read) -> &D::Facade) -> Self {
        Entry {
            describe: D::describe(name),
            read: Box::new(move |r, op| D::answer(facade(r), op)),
            stage: None,
        }
    }

    /// A source field. `mint`, when present, serves `POST /{name}` for a
    /// source whose own [`stage`](SourceResource::stage) doesn't
    /// (a `Table<Id<V>, V>` minting ids).
    pub fn source<D: SourceResource>(
        name: &str,
        facade: fn(&S::Read) -> &D::Facade,
        write: fn(&mut WriteOf<S>) -> &mut D::Write<S::Error>,
        mint: Option<Stage<D::Write<S::Error>, S::Error>>,
    ) -> Self {
        let mut describe = D::describe(name);
        describe.ops.insert |= mint.is_some();
        Entry {
            describe,
            read: Box::new(move |r, op| D::answer(facade(r), op)),
            stage: Some(Box::new(move |op| {
                let apply = match (op, mint) {
                    (op @ WriteOp::Insert(_), Some(mint)) => mint(op)?,
                    (op, _) => D::stage::<S::Error>(op)?,
                };
                Ok(Box::new(move |w: &mut WriteOf<S>| apply(write(w))))
            })),
        }
    }

    pub fn describe(&self) -> &Describe {
        &self.describe
    }
}
