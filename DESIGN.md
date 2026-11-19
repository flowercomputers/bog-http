# bog-http — design draft

Renamed from fold-http on 2026-11-19 (see the last "Progress" entry). Everything before that
entry keeps the names of its time: fold-http, `fold-epoch`, fold2, fold3.

Status: milestones 1–5 landed (see "Progress" at the end). Written 2026-10-19 after surveying fold2
(`master`, ab26fde) and Sam's `bog-serve` (bog-kit `sam/vibe-bog-serve`, 5a0fb83).

## The goal behind the goal

The literal ask is "a crate that serves a fold over HTTP". The outcome we
actually want: **any fold2 schema becomes a typed, live, networked backend
with no hand-written glue** — the same struct you `#[derive(Schema)]` on is
the API, its OpenAPI doc, its live feed, and (via codegen) its client.

Acceptance test that isn't us grading ourselves: port the hand-rolled HTTP in
`fold2-demos` (17 apps, ~1k lines of bespoke http/ws/sse) and the
`searchfight` harness onto fold-http. If they shrink to schema + a few custom
routes, and searchfight's numbers hold or improve, the crate is general.

## What we're building on

**fold2 gives us the right primitives, but no reflection.**

| fold2 has | what HTTP gets from it |
|---|---|
| `Db::serve() -> (Reader<S>, Writer<S>)` | `Reader` is `Clone + Send + Sync`; `Writer` is a cloneable handle to a single writer thread. Maps straight onto a server. |
| `Snapshot` + `frontier() -> Epoch` | Every response can carry a durable, monotonic epoch — better than bog-serve's in-memory `seq` that resets per process. |
| `facade.subscribe()` = snapshot + gapless deltas | A real live feed: initial state, then `(row, ±diff)` deltas. bog-serve could only say "something changed, re-read". |
| `Rejected { at, reason }` | Structured 422s that name the refusing source/view. |
| `range(after..)`, `iter_rev`, `top(n)` | Keyset (cursor) pagination for free; no O(offset) skips. |
| Public `SourceDecl` / `ViewDecl` traits | We can attach HTTP behaviour per source/view kind from outside fold2. |

Gaps we must design around:

- **No runtime schema description.** Field names exist only inside the derive;
  types only as Rust types. → fold-http needs its own derive (below).
- **No serde.** Codec is positional binary. → fold-http adds
  `Serialize + DeserializeOwned + JsonSchema` bounds at the HTTP edge only.
- **Subscriptions are unbounded std mpsc.** → fold-http must own fan-out and
  backpressure; one fold subscription per view, never one per client.
- **Schema evolution bricks durable keyspaces (panic on open).** → the server
  must fingerprint and refuse cleanly, not panic mid-request.
- **Everything is blocking.** → reads/writes go through `spawn_blocking` or a
  dedicated pool; nothing blocking on tokio workers (bog-serve does this).

## Shape of the crate

```
fold-http/             workspace
  fold-http/           library: Router builder, resources, wire types, OpenAPI
  fold-http-derive/    #[derive(Serve)] — sits next to #[derive(Schema)]
  fold-http-cli?/      later: `fold serve`, `fold api`, client codegen
```

User code:

```rust
#[derive(Schema, Serve)]
struct Shop {
    customers: Table<Id<Customer>, Customer>,
    orders:    Events<Order>,
    #[view] revenue: MapView<String, isize>,
    #[view] top_sellers: OrderedView<Id<Album>, isize>,
    #[view] search: SearchView<Id<Album>>,
    #[serve(skip)] #[view] internal: MapView<u64, u64>,
}

let db = Shop::open(dir).build()?;
let app: axum::Router = fold_http::Server::new(db)
    .route_get("/albums/{id}/related", related)   // typed custom routes
    .route_tx("/checkout", checkout)              // custom atomic write
    .into_router();
// or: fold_http::serve(app, Bind::tcp("127.0.0.1:10777")).with_graceful_shutdown(..)
```

### Reflection without touching fold2

`#[derive(Serve)]` sees the same struct tokens as `#[derive(Schema)]`. It
doesn't parse types; it emits, per field,
`<FieldTy as fold_http::Resource>::describe(name)` and a dispatch arm calling
`<FieldTy as Resource>::handle(&read.field, req)`. `Resource` is implemented
in fold-http for each fold2 decl type (`Table`, `Events`, `Set`, `Cell`, `Bag`,
`MapView`, `OrderedView`, `GroupView`, `ValueView`, `SearchView`, …) under
serde/schemars bounds. Third-party sources/views opt in by implementing
`Resource` — an open set, unlike bog-serve's closed `ViewKind` enum.

Nested `#[schema]` fields become path prefixes (`/ledger/accounts/...`).

Longer term, a serde-free `describe()` belongs in fold2 itself (next.md already
lists "migrations & fingerprinting / describe"); fold-http would then consume
it. We shouldn't block on it.

### Route surface (generated)

Every resource lives under `/{name}`; capabilities follow from its kind.

| kind | read | write |
|---|---|---|
| Table / MapView / OrderedView | `GET /{n}?after=&before=&limit=&order=`, `GET /{n}/{key}` | Table: `PUT /{n}/{key}`, `DELETE /{n}/{key}`, `PATCH` later |
| Table<Id<V>,V>, Events | as above | `POST /{n}` → minted id |
| OrderedView | `?by=value&min=&max=`, `/top?n=`, `/bottom?n=` | — |
| GroupView | `GET /{n}/{key}` (values+diffs), `/top?n=` | — |
| Set / Bag | list, `GET /{n}/{member}` | `PUT`/`DELETE` (Bag: `?times=`) |
| Cell / ValueView | `GET /{n}` | Cell: `PUT /{n}`, `DELETE /{n}` |
| SearchView | `GET /{n}/search?q=&k=`, `/complete?prefix=` | — |
| every resource | `GET /{n}/live` (SSE: snapshot, then deltas) | |

Cross-cutting:

- `POST /_tx` — one atomic transaction over many sources:
  `[{"op":"upsert","source":"customers","key":..,"value":..}, ...]`. One fold
  epoch, all-or-nothing. (bog-serve's `/batch`, but schema-wide.)
- `GET /_schema` (fingerprint + description), `GET /openapi.json`,
  `GET /_health/live`, `GET /_health/ready` (ready = db open, writer alive,
  no `checkpoint_error`).
- `_`-prefixed reserved namespace, so schema field names can't collide;
  custom-route collisions are a build-time error, not an axum panic.

### Wire contract

- **Envelope:** `{ "epoch": {outer,inner}, "data": ... }`, plus
  `Fold-Epoch` response header. Reads and the epoch come from one snapshot.
- **Read-your-writes across requests:** `Fold-Min-Epoch: <epoch>` request
  header (or `?min_epoch=`) waits (bounded) until the reader's frontier
  reaches it. Writes return the epoch they committed at. Cheap, and it's what
  makes a load-balanced read replica story possible later.
- **Pagination:** opaque keyset cursors (`next` in the response), never offset.
- **Errors:** RFC 9457 `application/problem+json`; `Rejected` → 422 with
  `at`/`reason`; unknown resource 404; bad key/body 400; method not allowed
  405 with `Allow`; overloaded 503 with `Retry-After`. All documented in
  OpenAPI.
- **Keys in paths:** typed via the key's JSON schema; `Id<T>` as its raw u64;
  composite keys as JSON. Precise rules written down and fuzzed.
- **Streaming bodies:** large scans stream as NDJSON when
  `Accept: application/x-ndjson`; JSON arrays otherwise (bounded by limit).

### Live feeds

One "hub" per subscribed resource: a blocking task drains the fold
`Subscription`, stamps each delta with its epoch, and fans out over a bounded
`tokio::sync::broadcast`. A lagging client gets a `resync` event (fresh
snapshot) instead of unbounded memory growth. Hubs start lazily on the first
subscriber and stop when the last one leaves. SSE first (`id:` = epoch;
`Last-Event-ID` resumes if still in the hub's ring buffer, else resync);
WebSocket later, sharing the hub.

### Concurrency & lifecycle

- Reads: `Reader` on a blocking pool sized for the store; no locks of ours.
- Writes: fold2's `Writer` already serializes on its thread; we add a bounded
  queue + `503` admission control so a write flood can't grow memory.
- Panics in user closures: caught at the boundary → 500, db poisoning
  surfaced via readiness, never a poisoned `RwLock` that kills every route.
- Library never owns the runtime; returns `axum::Router` / tower `Service`.
  An optional `serve()` helper adds TCP or UDS bind, **127.0.0.1 by default**,
  graceful shutdown on SIGTERM/ctrl-c that drains in-flight requests and then
  checkpoints *with the writer stopped* (closes bog-serve's ack-without-
  checkpoint race), idle-exit for daemon mode, and the discovery sidecar.
- Open errors (`Locked`, fingerprint drift) are `Result`s, never panics.
- Auth, CORS, compression, timeouts, rate limits: tower layers the user adds;
  we ship a small bearer-token layer and sane body-size/page-size limits.
- `tracing` spans per request with resource/op/epoch; optional metrics.

### OpenAPI

Generated from the same `describe()` that builds the router (single source of
truth, as bog-serve does), but done properly: schemars `$defs` →
`components/schemas` with `$ref` (recursive types work), documented error
responses, typed path params, custom routes with real summaries and
descriptions. Golden-snapshotted and validated in CI with an external
validator; a generated TypeScript client must type-check against a sample app.

## How we'll know it's right (outside judges)

- **Model-based property tests:** random op sequences against the HTTP API and
  against an in-memory `BTreeMap` model; every read must agree, every epoch
  monotone, every SSE stream replayed onto its snapshot equal to a fresh read.
- **Hammer test** (kept from bog-serve): concurrent writers + readers checking
  a cross-view invariant inside one snapshot; no torn reads.
- **Crash test:** kill -9 the server mid-write-load; on restart every
  acknowledged write is present (Strict durability), none half-applied.
- **Fuzzing:** path/key/query/body parsing (`cargo fuzz`).
- **Load targets** (deliberately aggressive, measured with `oha`, on a laptop):
  point read p99 < 500µs at 50k rps; commit-to-SSE-visible p99 < 5ms;
  1k concurrent SSE clients with flat memory. Benchmarked against hand-rolled
  `fold2-demos` HTTP and an equivalent axum+SQLite baseline.
- **API hygiene:** `cargo-semver-checks`, `cargo-deny`, MSRV pinned, docs with
  runnable examples, `#![deny(missing_docs)]` on the public surface.

## Proposed milestones

1. **Skeleton:** workspace, `Resource` trait, derive, Table/Events/MapView,
   envelope/errors, in-process tests via `tower::oneshot`.
2. **Full read surface:** every fold2 decl type, cursors, OpenAPI + golden.
3. **Writes:** per-source routes, `/_tx`, custom typed routes, admission
   control, min-epoch reads.
4. **Live:** hubs, SSE, resync, Last-Event-ID.
5. **Ops:** `serve()` helper, UDS, graceful shutdown, readiness, fingerprint,
   auth layer, tracing.
6. **Proof:** property/crash/fuzz/load suites; port 2–3 fold2-demos apps and
   searchfight.

## Open questions

1. Derive in fold-http (no fold2 change) vs. adding `describe()` to fold2 now.
2. axum/tower (as bog-serve) — any reason not to?
3. Which fold2 version do we pin: path dep on `../fold2` while both move?
4. Is the fold2-demos/searchfight port the acceptance bar you want?
5. Should fold-http also become bog-kit's server, i.e. replace bog-serve once
   bog-kit moves to fold2?

## Progress

### Milestone 1 — done (2026-10-19)

- Workspace: `fold-http` + `fold-http-derive`, path dep on `../fold2`.
- fold2 gained opt-in `serde` / `schemars` features: `Id<T>` is its raw
  `u64` on the wire. (Orphan rule: fold-http can't add these itself, and
  any row type holding an `Id` needs them.)
- `Resource` / `SourceResource` traits on fold's decl types; impls for
  `Table`, `Events`, `MapView`. `#[derive(Serve)]` with `#[serve(skip)]`;
  `Table<Id<V>, V>` gets `POST` (minting) via autoref specialization, since
  the derive can't see through type tokens.
- Writes are decoded on the request thread and applied on fold's writer
  thread, so malformed requests never reach the writer.
- Wire: `{epoch, data}` + `fold-epoch` header; RFC 9457 problems
  (`rejected` 422 with `at`, 400/404/405 with `Allow`/415/503); keyset
  cursors; `201` + `Location` on create.
- Tests: 7 in-process HTTP tests (constraints and rollback, pagination
  both ways, error matrix, schema, 64 concurrent writers), 4 derive
  compile-fail tests, unit tests for keys, cursors, content types.
  Smoke-tested over TCP with `examples/shop.rs`, including restart.

Decisions taken while building, worth revisiting:

- `epoch` is one `u64` (`frontier.outer`), not `{outer, inner}`:
  committed snapshots always have `inner == 0`.
- Meta routes (`/_schema`, `/_health`) are not enveloped; they aren't
  snapshot reads.
- `Resource::answer`, not `read`, to avoid clashing with
  `SourceDecl::read` on the same types.
- Nested `#[schema]` fields are a compile error asking for
  `#[serve(skip)]`, until path prefixes land.

### Milestone 2 — done (2026-10-19)

- Every fold2 kind serves: `Set`, `Cell`, `Bag`, `OrderedView` (by key,
  or by value with bounds and `(value, key)` cursors), `GroupView` (all
  pairs, or one key's values), `ValueView`, `SearchView` (BM25 search,
  completion).
- Resources describe themselves as `Route`s (method, path, op, schemas
  for key/query/body/response, 201/404 flags). One list mounts the
  router, fills `/_schema`, and generates OpenAPI; `Ops` flags are gone.
- `GET /openapi.json`: OpenAPI 3.1 with shared `components/schemas`
  (`$ref`s, so recursive types work), stable names for generic shapes
  (`Page_Row_Id_Customer`), documented problems, the `fold-epoch`
  header, field doc comments as descriptions. `Server::title/version/
  description` fill `info`.
- fold2 (`fold-http` branch): `SetRead::arrangement()` and
  `BagRead::arrangement()`, like the other read facades, so sets and
  bags paginate by cursor.
- Outside judges: golden snapshot + `$ref` closure + every-route-
  documented tests; `scripts/check-openapi.sh` runs
  `openapi-spec-validator`, Redocly lint, and type-checks a client
  against `openapi-typescript` output. All pass.

Decisions worth revisiting:

- Static subpaths share the key namespace (`/names/complete`); kinds
  that have both `/{key}` and static routes would shadow those keys.
  None do today; the router asserts no method/path collides.
- A bag `DELETE` removes at most the copies present; counts never go
  negative over HTTP.
- `Cell` and `ValueView` answer `null` when empty rather than 404.
- `security` is undocumented until auth lands (milestone 5).

### Milestone 3 — done (2026-10-19)

- `POST /_tx`: up to 1000 writes across any sources as one transaction.
  Each op is addressed by `{resource, op, key?, query?, body?}` and
  decoded exactly as its own route would be; a malformed op refuses the
  whole transaction before the writer sees it, naming it by `index`.
  Later ops see earlier ones. OpenAPI documents the body as a `oneOf`
  of every write route, so generated clients type-check transactions.
- Typed custom routes: `Server::get(path, summary, |read, Q| …)` reads
  one snapshot; `Server::post(path, summary, |tx, B| …)` runs a
  transaction whose `Err` rolls back and answers via `IntoProblem`.
  Paths are checked against the reserved namespace and against every
  generated route at startup. `NoParams` for routes without input.
- Read-your-writes: `fold-min-epoch` on any read waits, bounded by
  `min_epoch_wait` (5 s), for that epoch, waking on each commit and
  polling for writes made through a shared `Writer`; otherwise
  `503 epoch-not-reached` with `Retry-After`.
- Admission control: `max_pending_writes` (1024) permits; past it,
  `503 overloaded` with `Retry-After`. `0` serves read-only.
- `body_limit` (2 MiB) with a `413` problem; axum's plain-text 405/413
  bodies become problems.
- One commit path for resource writes, `/_tx`, and custom writes.
- Tests: transactions (atomicity, ordering, refusal, every malformed
  shape), custom routes (snapshot reads, check-and-set rollback,
  OpenAPI), min-epoch (immediate, timeout, wake-on-commit), admission,
  body limit, and a hammer: 8 writers and 8 readers checking that views
  agree with their sources inside every snapshot, epochs gap-free and
  unique, reads never going back in time.

Decisions worth revisiting:

- A transaction can't refer to an id minted by an earlier op in the
  same transaction; a custom `post` route can. Result references
  (`"$0.key"`) would close the gap.
- Custom routes have no path parameters yet.
- Admission control counts writes, not their size.

### Milestone 4 — done (2026-10-19), with the local half of 5

Goal driving it: a live web API for a demo app (the gallery's chat) that
a local app can read and write on-device.

- Live feeds. `Accept: text/event-stream` on any listing or singleton
  `GET`: a `snapshot` event (the plain `GET`'s answer), then a `change`
  per commit, `id` = epoch. One hub per resource, started with its
  first client and stopped (≤ 250 ms) after its last; it drains fold's
  unbounded subscription on its own thread, serializes each commit once,
  and broadcasts over a bounded channel (1024). Gapless by construction:
  subscribe, then snapshot, then drop changes at or below the snapshot's
  epoch. A lagging client gets a fresh snapshot. `Last-Event-ID`
  replays from the hub's ring (1024) when nothing it missed was evicted,
  else snapshots. Search views don't stream (`406`).
- `Server::serve(addr, layer)`: bind, serve until ctrl-c / SIGTERM, end
  live streams (`Closer`, so graceful shutdown can't hang on them),
  drain, checkpoint. `fold_http::cors([...])`: explicit origins only.
- `examples/chat`: the gallery's Chat schema, unchanged, served whole —
  generated routes, a `/send` custom write stamping the clock, a `/find`
  custom read with prefix completion — and a page that renders entirely
  from `EventSource` streams. Verified in a browser: live push from
  another client, sends from the page, search, graceful stop with open
  streams (exits in ~0.5 s, WAL empty), restart, and the page
  reconnecting on its own and receiving the next message live.
- Tests: SSE ordering and epochs, per-commit grouping, singletons,
  resume, resume-too-old, lag → resync (1124 commits unread), 406,
  closing ends streams, CORS allowlist; hub lifecycle unit test.

Found by the acceptance test, **a fold2 bug, not fold-http's**: opening
the gallery's `data/chat` (written by the gallery's build) and writing
once takes `total` from 7 to 15 while `messages` goes 7 → 8. Reproduced
with fold alone and with the gallery binary itself on current fold2; a
store reopened by the binary that wrote it is fine. It looks like the
replay-on-fingerprint-change path in `Database::boot` restoring a count
operator's state and also replaying into it.

Still open from 5: auth, Unix sockets, idle-exit daemon mode, fingerprint
drift as a clean error, readiness beyond liveness, tracing/metrics.

### Milestone 5 — done (2026-10-19)

- Access control: `bearer_token` on every route but `/_health`, with
  `?access_token=` for `EventSource` (stripped before routing, compared
  in constant time); `allowed_hosts`, applied automatically to loopback
  binds by `serve`, refuses foreign `Host` headers (DNS rebinding).
  OpenAPI documents the scheme and the 401s.
- `serve_unix`: owner-only (`0600`) sockets, a stale socket replaced, a
  live one never taken over (`AddrInUse`), removed on exit.
- Spawn-or-connect for local apps: `discovery(path)` writes
  `{pid, url | socket, auth, title}` atomically once listening and
  removes it on exit (only if still ours); `idle_exit(d)` shuts down once
  no request has arrived for `d` and no stream is open.
- `/_health`: `{status, epoch, streams, pending_writes}`. `tracing`
  events (target `fold_http`) for serving, commits, refusals, overload,
  hubs starting and stopping, and stream resyncs.
- Tests over real sockets: discovery + Host check + idle exit on TCP;
  Unix socket mode, takeover refusal, cleanup, stale-socket replacement;
  an open stream holding off idle exit.

Not done, and why:

- Schema-drift detection beyond fold2's own: fold2 rebuilds derived
  state when the graph changes, and a changed *row type* panics in its
  codec at open. A clean error for the latter belongs in fold2 (a type
  fingerprint per keyspace).
- Metrics: `tracing` events are the hook; a Prometheus endpoint is an
  application's choice of exporter.
- HTTP/2: browsers only use it with TLS. Until then a page should follow
  everything through one `/_live` stream (six connections per host).

### fold3 port (2026-11-12, branch `fold3`)

Builds against `../fold3` on its `fold-http` branch instead of fold2.
The route surface is unchanged; what moved:

- No writer thread: the server holds an `Arc<Db<S>>` (`Server::new`
  takes a `Db` or an `Arc<Db>`; `from_parts` is gone). Reads and writes
  run on the blocking pool, and fold3 serializes writers.
- One error type. `Declare::Error` and `IntoProblem` are gone;
  `From<fold3::Error> for Problem` maps a `Rejected` to `422` with `at`,
  a `Problem` carried by `Error::reject` to itself (how a custom route
  picks a status), any other refusal to `422`, and store failures to
  `503`.
- Facades borrow their snapshot: `Resource::Facade<'a>` is a GAT, and
  the derive emits accessor `fn` items taking a `Tie<'a>` marker, since
  a `fn` pointer can't bind `'a` from `S::Read<'a>` alone.
- Live changes are fold3's, natively: `{key?, op, value?, old?}` with
  `op` one of `insert`, `delete`, `replace`; counted kinds (multisets,
  grouped views) send `{key?, value, diff}`.
- Epochs are fold3's `u64` as it is: a write reports the epoch it
  committed as, and a write that changed nothing reports the epoch
  already there (it commits nothing, so "its" epoch would never come).
- `Bag` is `Multiset` (kind `multiset`, `MultisetAdd`/`MultisetRemove`);
  multiplicities and sums are `i64`.

fold3 (`fold-http` branch) gained: serde/schemars for `Id` behind
features; `Db::write_epoch` (and `commit` returning the epoch);
`SetRead::range_rev`, `MultisetRead::range`/`range_rev`, and
`GroupRead::rows`/`rows_rev` for cursors that resume mid-group.

Worth revisiting:

- A hub's replay floor is a snapshot taken after it subscribes: fold3's
  facade `subscribe()` may miss a commit published between its snapshot
  and the subscription, so nothing at or below that floor is replayed.
- `fold3::Error::AfterCommit` (committed, maintenance failed) answers
  `500` though the write stands.
- The chat example no longer opens the gallery's fold2 data.

### bog-http (2026-11-19)

fold3 was renamed bog and made public; this crate follows. Only names
moved:

- Crates: `bog-http` and `bog-http-derive` (`bog_http::…`), building on
  `bog` (`bog::schema::…`) in place of `fold3`.
- Wire: the epoch headers are `bog-epoch` and `bog-min-epoch`, and the
  OpenAPI header component is `BogEpoch`. Routes, bodies and events are
  as they were.
- Around it: the `tracing` target and the discovery file's version key
  are `bog_http`, hub threads are named `bog-http hub N`, and the
  examples read `BOG_DIR` and default to `./chat.bog` / `./shop.bog`.
- The dependency is bog's public repo, on its `bog-http` branch: bog
  with the additions listed under the fold3 port, which `master` does
  not carry yet.

### Read-only sources, whole-listing snapshots (2026-11-19)

Ported from the other fold3 port (PR #1, "Port to fold3"), which found
both gaps building a first app, turf:

- `#[serve(read_only)]` on a source serves its reads and drops its write
  routes from the router, `/_schema`, OpenAPI and `/_tx`, for sources an
  application writes only through its own transactions. The derive
  rejects it on views and alongside `skip`.
- `snapshot=all` in a stream's query (on `/_live`, inside a resource's
  own query) makes its snapshot the whole listing: every page read from
  one snapshot, the first as `snapshot` and the rest as `snapshot.more`,
  up to `MAX_SNAPSHOT_ROWS`. Pages before the last carry the id `-`, not
  an epoch, so a reconnect mid-snapshot starts over rather than resuming
  from a state the client never finished receiving. It's opt-in: a
  stream asking for `limit=15` wants fifteen rows.

That port's third addition, an `Option<Id<_>>` helper, isn't needed
here: bog's `Id` has serde and schemars impls, so `Option<Id<_>>` is
covered by theirs.
