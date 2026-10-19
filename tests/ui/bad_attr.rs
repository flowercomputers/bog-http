use fold::schema::*;
use fold_http::Serve;

#[derive(Schema, Serve)]
struct Db {
    #[serve(hide)]
    rows: Table<String, u32>,
}

impl Declare for Db {
    type Error = Rejected;
    fn declare(_: &mut DbBuilder<Rejected>) {}
}

fn main() {}
