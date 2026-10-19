# fold-http

Serve any [fold2](https://github.com/flowercomputers/fold2) schema as a
live, typed HTTP API.

```rust
#[derive(Schema, Serve)]
struct Chat {
    messages: Events<Message>,
    #[view] total: ValueView<Diff>,
    #[view] search: SearchView<Id<Message>>,
}

Server::new(Chat::open("./chat.fold").build()?)
    .serve("127.0.0.1:10777", |router| router)
    .await
```

That's `GET /messages` (keyset-paginated), `GET /messages/{id}`,
`POST /messages`, `GET /total`, `GET /search?q=`, `POST /_tx` for atomic
multi-source writes, and `GET /openapi.json`. Every listing also streams
over Server-Sent Events: a snapshot, then each commit's changes.

Try it with the gallery's chat:

```sh
cargo run --example chat   # then open http://127.0.0.1:10777
```

Builds against `../fold2` on its `fold-http` branch. See the crate docs
(`cargo doc --open`) for the wire contract, and `DESIGN.md` for the why.
