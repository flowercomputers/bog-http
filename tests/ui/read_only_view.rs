use fold3::schema::*;
use fold_http::Serve;

#[derive(Schema, Serve)]
struct Db {
    rows: Table<String, i64>,
    #[view]
    #[serve(read_only)]
    total: MapView<String, i64>,
}

impl Declare for Db {
    fn declare(b: &mut DbBuilder) {
        b.total.define(&*b.rows);
    }
}

fn main() {}
