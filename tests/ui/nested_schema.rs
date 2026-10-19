use fold::schema::*;
use fold_http::Serve;

#[derive(Schema)]
struct Inner {
    rows: Table<String, u32>,
}

impl Declare for Inner {
    type Error = Rejected;
    fn declare(_: &mut InnerBuilder<Rejected>) {}
}

#[derive(Schema, Serve)]
struct Outer {
    #[schema]
    inner: Inner,
}

impl Declare for Outer {
    type Error = Rejected;
    fn declare(_: &mut OuterBuilder<Rejected>) {}
}

fn main() {}
