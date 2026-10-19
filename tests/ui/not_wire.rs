use fold::schema::*;
use fold::storage::Codec;
use fold_http::Serve;

/// Storable, but has no serde or JSON Schema: not servable.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Codec)]
struct Opaque(u32);

#[derive(Schema, Serve)]
struct Db {
    rows: Table<String, Opaque>,
}

impl Declare for Db {
    type Error = Rejected;
    fn declare(_: &mut DbBuilder<Rejected>) {}
}

fn main() {}
