use bog::schema::*;
use bog_http::Serve;

#[derive(Schema, Serve)]
struct Db {
    #[serve(hide)]
    rows: Table<String, u32>,
}

impl Declare for Db {
    fn declare(_: &mut DbBuilder) {}
}

fn main() {}
