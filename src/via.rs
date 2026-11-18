//! How fold3 types cross the wire, without fold3 knowing about serde.
//!
//! A served key or value type `T` goes through an encoder `M: Via<T>`,
//! picked for it by `#[derive(Serve)]`: [`ViaId`] for fold3's [`Id`]
//! (its raw `u64`), [`ViaSerde`] for everything else (serde and
//! schemars). Resources wrap their keys and values in [`W<T, M>`], which
//! implements serde and schemars through `M`, so every response shape
//! (`Row`, `Page`, cursors) is still plain serde.
//!
//! The encoder is a type parameter rather than a blanket impl because
//! Rust's coherence rules forbid both "every serde type" and "`Id`" as
//! impls of one trait while `Id` is fold3's and serde's traits are
//! serde's.
//!
//! An `Id` inside one of your own types needs [`id`]:
//!
//! ```ignore
//! #[derive(Serialize, Deserialize, JsonSchema, Codec, ..)]
//! struct Order {
//!     #[serde(with = "fold_http::id")]
//!     #[schemars(with = "u64")]
//!     customer: Id<Customer>,
//! }
//! ```

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use fold3::schema::Id;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Encodes `T` as JSON, and describes it as a JSON Schema.
pub trait Via<T>: Send + Sync + 'static {
    fn serialize<S: Serializer>(value: &T, s: S) -> Result<S::Ok, S::Error>;
    fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<T, D::Error>;
    fn schema_name() -> Cow<'static, str>;
    fn schema_id() -> Cow<'static, str>;
    fn inline_schema() -> bool;
    fn json_schema(g: &mut SchemaGenerator) -> Schema;
}

/// A type with its own serde and schemars impls.
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be served: it needs serde and a JSON Schema",
    label = "served here",
    note = "derive `serde::Serialize`, `serde::Deserialize`, and `schemars::JsonSchema` for `{Self}`, or #[serve(skip)] the field",
    note = "an `Id` inside `{Self}` needs `#[serde(with = \"fold_http::id\")]` and `#[schemars(with = \"u64\")]`"
)]
pub trait Wire: Serialize + DeserializeOwned + JsonSchema {}
impl<T: Serialize + DeserializeOwned + JsonSchema> Wire for T {}

/// Through the type's own serde and schemars impls.
pub struct ViaSerde;

impl<T: Wire> Via<T> for ViaSerde {
    fn serialize<S: Serializer>(value: &T, s: S) -> Result<S::Ok, S::Error> {
        value.serialize(s)
    }
    fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<T, D::Error> {
        T::deserialize(d)
    }
    fn schema_name() -> Cow<'static, str> {
        T::schema_name()
    }
    fn schema_id() -> Cow<'static, str> {
        T::schema_id()
    }
    fn inline_schema() -> bool {
        T::inline_schema()
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        T::json_schema(g)
    }
}

/// A fold3 [`Id`], as its raw `u64`: the type parameter is a
/// compile-time tag only, so `Id<User>` and `Id<Order>` encode alike.
pub struct ViaId;

impl<T: 'static> Via<Id<T>> for ViaId {
    fn serialize<S: Serializer>(value: &Id<T>, s: S) -> Result<S::Ok, S::Error> {
        id::serialize(value, s)
    }
    fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Id<T>, D::Error> {
        id::deserialize(d)
    }
    fn schema_name() -> Cow<'static, str> {
        "Id".into()
    }
    fn schema_id() -> Cow<'static, str> {
        "fold_http::Id".into()
    }
    fn inline_schema() -> bool {
        true
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        <u64 as JsonSchema>::json_schema(g)
    }
}

/// A `T` on the wire, encoded through `M`.
pub struct W<T, M>(pub T, PhantomData<fn() -> M>);

impl<T, M> W<T, M> {
    pub fn new(value: T) -> Self {
        W(value, PhantomData)
    }

    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Clone, M> Clone for W<T, M> {
    fn clone(&self) -> Self {
        W::new(self.0.clone())
    }
}

impl<T: fmt::Debug, M> fmt::Debug for W<T, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<T, M: Via<T>> Serialize for W<T, M> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        M::serialize(&self.0, s)
    }
}

impl<'de, T, M: Via<T>> Deserialize<'de> for W<T, M> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        M::deserialize(d).map(W::new)
    }
}

impl<T, M: Via<T>> JsonSchema for W<T, M> {
    fn inline_schema() -> bool {
        M::inline_schema()
    }
    fn schema_name() -> Cow<'static, str> {
        M::schema_name()
    }
    fn schema_id() -> Cow<'static, str> {
        M::schema_id()
    }
    fn json_schema(g: &mut SchemaGenerator) -> Schema {
        M::json_schema(g)
    }
}

/// The key and value types a served field puts on the wire, for
/// `#[derive(Serve)]` to pick their encoders: `()` where a kind has none.
pub trait Shape {
    type Key: 'static;
    type Value: 'static;
}

/// `#[serde(with = "fold_http::id")]` for an [`Id`] field of your own
/// types: its raw `u64`. Pair it with `#[schemars(with = "u64")]`.
pub mod id {
    use fold3::schema::Id;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S: Serializer>(id: &Id<T>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u64(id.raw())
    }

    pub fn deserialize<'de, T, D: Deserializer<'de>>(d: D) -> Result<Id<T>, D::Error> {
        u64::deserialize(d).map(Id::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_raw_numbers_and_serde_types_are_themselves() {
        let id: W<Id<String>, ViaId> = W::new(Id::new(42));
        assert_eq!(serde_json::to_string(&id).unwrap(), "42");
        let back: W<Id<String>, ViaId> = serde_json::from_str("7").unwrap();
        assert_eq!(back.0, Id::new(7));
        let s: W<String, ViaSerde> = W::new("a".into());
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"a\"");
        assert_eq!(<W<String, ViaSerde>>::schema_name(), "string");
        assert_eq!(<W<Id<()>, ViaId>>::schema_name(), "Id");
    }
}
