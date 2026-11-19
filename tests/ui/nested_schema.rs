use bog::schema::*;
use bog_http::Serve;

#[derive(Schema)]
struct Inner {
    rows: Table<String, u32>,
}

impl Declare for Inner {
    fn declare(_: &mut InnerBuilder) {}
}

#[derive(Schema, Serve)]
struct Outer {
    #[schema]
    inner: Inner,
}

impl Declare for Outer {
    fn declare(_: &mut OuterBuilder) {}
}

fn main() {}
