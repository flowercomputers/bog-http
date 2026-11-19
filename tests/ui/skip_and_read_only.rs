use bog::schema::*;
use bog_http::Serve;

#[derive(Schema, Serve)]
struct Db {
    #[serve(skip, read_only)]
    rows: Table<String, i64>,
}

impl Declare for Db {
    fn declare(_: &mut DbBuilder) {}
}

fn main() {}
