//! Load-shaped hardening: many writers and many live clients at once,
//! checking the one property live feeds promise (every client's view,
//! built from its snapshot plus the changes after it, ends equal to the
//! store) while clients disconnect, resume, lag, and restart hubs.
//!
//! The convergence tests run by default at a small scale; raise it with
//! `BOG_HTTP_LOAD=<n>` (a multiplier). The `#[ignore]`d ones measure
//! latency or wait on real sockets; run them with
//! `cargo test --release --test load -- --ignored --nocapture`.

mod common;
#[path = "support/sse.rs"]
mod sse;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::http::{Method, StatusCode};
use bog_http::NoParams;
use common::{app, send, send_with, server};
use serde_json::{Value, json};
use sse::{Event, Next, Stream};

fn scale() -> usize {
    std::env::var("BOG_HTTP_LOAD")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
}

/// A small deterministic PRNG (xorshift64*), one per task.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

type Errors = Arc<Mutex<Vec<String>>>;

fn fail(errors: &Errors, msg: String) {
    let mut e = errors.lock().unwrap();
    if e.len() < 50 {
        e.push(msg);
    }
}

// ---------------------------------------------------------------------
// A client-side model of one followed resource

/// What a client believes one resource holds, rebuilt from snapshots and
/// changes, checking every change against it: an insert of a present
/// key, a delete of a missing one, or an `old` that isn't what the
/// client has, means it missed or repeated something.
#[derive(Default, Clone)]
struct Model {
    rows: BTreeMap<String, Value>,
    epoch: u64,
    /// Connected without a usable resume point: the next event for this
    /// resource must be a snapshot.
    needs_snapshot: bool,
    /// Mid-snapshot: more pages to come (the event id said `-`).
    partial: bool,
}

impl Model {
    /// Holds a whole state at `epoch` or later.
    fn at(&self, epoch: u64) -> bool {
        self.epoch >= epoch && !self.partial
    }
}

/// `prefs` (a table: `{key, value}` rows) or `tags` (a set of strings).
#[derive(Clone, Copy, PartialEq, Debug)]
enum Kind {
    Table,
    Set,
}

impl Model {
    fn row(kind: Kind, item: &Value) -> (String, Value) {
        match kind {
            Kind::Table => (
                item["key"].as_str().unwrap().to_owned(),
                item["value"].clone(),
            ),
            Kind::Set => (item.as_str().unwrap().to_owned(), Value::Bool(true)),
        }
    }

    /// Apply one event (`snapshot`, `snapshot.more` or `change`); `Err`
    /// says what didn't add up.
    fn apply(&mut self, kind: Kind, what: &str, e: &Event) -> Result<(), String> {
        let epoch = e.epoch();
        match what {
            "snapshot" => {
                if epoch < self.epoch {
                    return Err(format!("snapshot at {epoch} after {}", self.epoch));
                }
                self.rows.clear();
                self.epoch = epoch;
                self.needs_snapshot = false;
                self.add_page(kind, e);
            }
            "snapshot.more" => {
                if epoch != self.epoch {
                    return Err(format!(
                        "snapshot.more at {epoch}, snapshot at {}",
                        self.epoch
                    ));
                }
                self.add_page(kind, e);
            }
            "change" => {
                if self.needs_snapshot {
                    return Err(format!("change at {epoch} before any snapshot"));
                }
                if epoch <= self.epoch {
                    return Err(format!(
                        "change at {epoch} after {}: repeated or reordered",
                        self.epoch
                    ));
                }
                self.epoch = epoch;
                for c in e.data["changes"].as_array().unwrap() {
                    self.change(kind, c)
                        .map_err(|why| format!("epoch {epoch}: {why}: {c}"))?;
                }
            }
            other => return Err(format!("unexpected event {other}")),
        }
        Ok(())
    }

    fn add_page(&mut self, kind: Kind, e: &Event) {
        for item in e.data["data"]["items"].as_array().unwrap() {
            let (k, v) = Model::row(kind, item);
            self.rows.insert(k, v);
        }
    }

    fn change(&mut self, kind: Kind, c: &Value) -> Result<(), &'static str> {
        let key = match kind {
            Kind::Table => c["key"].as_str().unwrap().to_owned(),
            Kind::Set => c["value"].as_str().unwrap().to_owned(),
        };
        let value = match kind {
            Kind::Table => c["value"].clone(),
            Kind::Set => Value::Bool(true),
        };
        let have = self.rows.get(&key);
        match c["op"].as_str().unwrap() {
            "insert" if have.is_some() => Err("insert of a key already present"),
            "insert" => {
                self.rows.insert(key, value);
                Ok(())
            }
            "delete" if have.is_none() => Err("delete of a missing key"),
            "delete" if kind == Kind::Table && have != Some(&c["old"]) => {
                Err("delete whose old value isn't the one held")
            }
            "delete" => {
                self.rows.remove(&key);
                Ok(())
            }
            "replace" if have != Some(&c["old"]) => {
                Err("replace whose old value isn't the one held")
            }
            "replace" => {
                self.rows.insert(key, value);
                Ok(())
            }
            _ => Err("unknown op"),
        }
    }
}

/// The keys on which `have` and `want` disagree, for a failure message.
fn diff(have: &BTreeMap<String, Value>, want: &BTreeMap<String, Value>) -> String {
    let mut out = Vec::new();
    for k in have
        .keys()
        .chain(want.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        if have.get(k) != want.get(k) {
            out.push(format!(
                "{k}: have {:?}, store {:?}",
                have.get(k),
                want.get(k)
            ));
        }
    }
    out.truncate(5);
    out.join("; ")
}

/// The store's current rows of `/prefs` or `/tags`, every page.
async fn listing(app: &Router, path: &str, kind: Kind) -> BTreeMap<String, Value> {
    let mut rows = BTreeMap::new();
    let mut after: Option<String> = None;
    loop {
        let url = match &after {
            Some(a) => format!("{path}?limit=1000&after={a}"),
            None => format!("{path}?limit=1000"),
        };
        let page = send(app, Method::GET, &url, None).await;
        for item in page.data()["items"].as_array().unwrap() {
            let (k, v) = Model::row(kind, item);
            rows.insert(k, v);
        }
        match page.data()["next"].as_str() {
            Some(n) => after = Some(n.to_owned()),
            None => return rows,
        }
    }
}

// ---------------------------------------------------------------------
// Writers

/// One random write: an upsert or delete on `prefs`, an insert or remove
/// on `tags`, or a `/_tx` of several.
async fn write_once(app: &Router, rng: &mut Rng) {
    fn op(rng: &mut Rng) -> Value {
        match rng.below(4) {
            0 | 1 => json!({"resource": "prefs", "op": "upsert",
                            "key": format!("k{}", rng.below(60)), "body": rng.below(1000)}),
            2 => json!({"resource": "prefs", "op": "remove", "key": format!("k{}", rng.below(60))}),
            _ => json!({"resource": "tags", "op": if rng.chance(60) { "insert" } else { "remove" },
                        "key": format!("t{}", rng.below(30))}),
        }
    }
    let res = match rng.below(100) {
        0..40 => {
            let path = format!("/prefs/k{}", rng.below(60));
            send(app, Method::PUT, &path, Some(json!(rng.below(1000)))).await
        }
        40..55 => {
            send(
                app,
                Method::DELETE,
                &format!("/prefs/k{}", rng.below(60)),
                None,
            )
            .await
        }
        55..70 => send(app, Method::PUT, &format!("/tags/t{}", rng.below(30)), None).await,
        70..80 => {
            send(
                app,
                Method::DELETE,
                &format!("/tags/t{}", rng.below(30)),
                None,
            )
            .await
        }
        _ => {
            let n = 2 + rng.below(4);
            let ops: Vec<Value> = (0..n).map(|_| op(rng)).collect();
            send(app, Method::POST, "/_tx", Some(Value::Array(ops))).await
        }
    };
    assert!(res.status.is_success(), "a write failed: {:?}", res.body);
}

/// A last write touching both resources; its epoch is where every client
/// must end up.
async fn marker(app: &Router) -> u64 {
    let res = send(
        app,
        Method::POST,
        "/_tx",
        Some(json!([
            {"resource": "prefs", "op": "upsert", "key": "zz-marker", "body": 1},
            {"resource": "tags", "op": "insert", "key": "zz-marker"},
        ])),
    )
    .await;
    assert_eq!(res.status, StatusCode::OK, "{:?}", res.body);
    res.epoch()
}

// ---------------------------------------------------------------------
// Clients

/// How a client follows: one resource on its own stream, or both on
/// `/_live`.
#[derive(Clone, Copy, Debug)]
enum Follow {
    Prefs,
    Both,
}

impl Follow {
    fn path(self, limit: u64) -> String {
        match self {
            Follow::Prefs => format!("/prefs?snapshot=all&limit={limit}"),
            Follow::Both => format!(
                "/_live?prefs=snapshot%3Dall%26limit%3D{limit}&tags=snapshot%3Dall%26limit%3D{limit}"
            ),
        }
    }
    fn kinds(self) -> &'static [(&'static str, Kind)] {
        match self {
            Follow::Prefs => &[("prefs", Kind::Table)],
            Follow::Both => &[("prefs", Kind::Table), ("tags", Kind::Set)],
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct Stats {
    connects: usize,
    resumed_without_snapshot: usize,
    snapshots: usize,
    changes: usize,
}

/// How a client behaves between connections.
#[derive(Clone, Copy)]
struct Habits {
    /// Events read per connection, at most.
    events: u64,
    /// Pause between connections, in ms: past the hub's idle check
    /// (250 ms), a lone client's hub stops and restarts.
    pause_ms: (u64, u64),
    /// Pause between each writer's writes, in ms, so writing outlasts
    /// the clients' pauses.
    pace_ms: u64,
}

struct Client {
    follow: Follow,
    models: Vec<Model>,
    last_id: Option<String>,
    stats: Stats,
    rng: Rng,
    limit: u64,
}

impl Client {
    fn new(follow: Follow, seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let limit = 1 + rng.below(40);
        Client {
            follow,
            models: vec![Model::default(); follow.kinds().len()],
            last_id: None,
            stats: Stats::default(),
            rng,
            limit,
        }
    }

    /// Connect, resuming from the last id; unless `resume_only`, now and
    /// then with none, or a garbled one, which must both mean a snapshot.
    async fn connect(
        &mut self,
        app: &Router,
        errors: &Errors,
        resume_only: bool,
    ) -> Option<Stream> {
        let id = match self.rng.below(10) {
            0 if !resume_only => None,
            1 if !resume_only => Some("garbage".to_owned()),
            _ => self.last_id.clone(),
        };
        let parts: Vec<Option<u64>> = match &id {
            Some(id) => id.split('.').map(|p| p.parse().ok()).collect(),
            None => vec![],
        };
        for (k, m) in self.models.iter_mut().enumerate() {
            let resumes =
                parts.len() == self.follow.kinds().len() && parts[k].is_some_and(|e| e == m.epoch);
            // Resuming at the epoch the client holds needs no snapshot;
            // anything else (no id, a garbled one, a component that isn't
            // where this client is) does.
            m.needs_snapshot = !resumes;
        }
        self.stats.connects += 1;
        match Stream::open(app, &self.follow.path(self.limit), id.as_deref()).await {
            Ok(s) => Some(s),
            Err(e) => {
                fail(errors, format!("connect refused: {e:?}"));
                None
            }
        }
    }

    /// Apply one event; `false` if it didn't add up (already recorded).
    fn take(&mut self, e: &Event, errors: &Errors) -> bool {
        let (resource, what) = match self.follow {
            Follow::Prefs => ("prefs", e.event.as_str()),
            Follow::Both => e.event.split_once('.').unwrap(),
        };
        let k = self
            .follow
            .kinds()
            .iter()
            .position(|(n, _)| *n == resource)
            .unwrap();
        let kind = self.follow.kinds()[k].1;
        match what {
            "snapshot" => self.stats.snapshots += 1,
            "change" => self.stats.changes += 1,
            _ => {}
        }
        if let Err(why) = self.models[k].apply(kind, what, e) {
            fail(
                errors,
                format!("{:?} client, {resource}: {why}", self.follow),
            );
            return false;
        }
        let id = e.id.as_deref().unwrap_or_default();
        self.models[k].partial = id.split('.').nth(k) == Some("-");
        self.last_id = e.id.clone();
        true
    }

    /// Follow until `done`, dropping and resuming the connection often.
    async fn churn(
        &mut self,
        app: &Router,
        habits: Habits,
        done: &tokio::sync::watch::Receiver<bool>,
        errors: &Errors,
    ) {
        while !*done.borrow() {
            let Some(mut s) = self.connect(app, errors, false).await else {
                return;
            };
            let mut first = true;
            for _ in 0..1 + self.rng.below(habits.events) {
                match s.next_within(Duration::from_millis(200)).await {
                    Next::Event(e) => {
                        if first && !e.event.ends_with("snapshot") {
                            self.stats.resumed_without_snapshot += 1;
                        }
                        first = false;
                        if !self.take(&e, errors) {
                            return;
                        }
                    }
                    Next::Quiet => break,
                    Next::Ended => {
                        fail(errors, "a stream ended while the server was up".into());
                        return;
                    }
                }
                if self.rng.chance(10) {
                    tokio::time::sleep(Duration::from_millis(self.rng.below(5))).await;
                }
            }
            drop(s);
            let (lo, hi) = habits.pause_ms;
            tokio::time::sleep(Duration::from_millis(lo + self.rng.below(hi - lo + 1))).await;
        }
    }

    /// Reconnect (resuming) and read until every followed resource has
    /// reached `epoch`.
    async fn catch_up(&mut self, app: &Router, epoch: u64, errors: &Errors) {
        // A resume, never a fresh start: the replay path is what's
        // being checked here.
        let Some(mut s) = self.connect(app, errors, true).await else {
            return;
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !self.models.iter().all(|m| m.at(epoch)) {
            if Instant::now() > deadline {
                let at: Vec<u64> = self.models.iter().map(|m| m.epoch).collect();
                fail(
                    errors,
                    format!("{:?} client stuck at {at:?}, wanted {epoch}", self.follow),
                );
                return;
            }
            match s.next_within(Duration::from_millis(500)).await {
                Next::Event(e) => {
                    if !self.take(&e, errors) {
                        return;
                    }
                }
                Next::Quiet => {}
                Next::Ended => {
                    fail(errors, "stream ended during catch-up".into());
                    return;
                }
            }
        }
    }
}

/// Run `writers` random writers and `clients` churning clients, then a
/// marker write; every client must end holding exactly what the store
/// holds.
async fn converge(writers: usize, writes: usize, clients: usize, habits: Habits) -> Stats {
    let app = app();
    let errors: Errors = Arc::default();
    let (done_tx, done_rx) = tokio::sync::watch::channel(false);

    let mut followers = Vec::new();
    for c in 0..clients {
        let (app, errors, done) = (app.clone(), errors.clone(), done_rx.clone());
        let follow = if c % 2 == 0 {
            Follow::Both
        } else {
            Follow::Prefs
        };
        followers.push(tokio::spawn(async move {
            let mut client = Client::new(follow, 1000 + c as u64);
            client.churn(&app, habits, &done, &errors).await;
            client
        }));
    }
    let mut writing = Vec::new();
    for w in 0..writers {
        let app = app.clone();
        writing.push(tokio::spawn(async move {
            let mut rng = Rng::new(w as u64 + 1);
            for _ in 0..writes {
                write_once(&app, &mut rng).await;
                if habits.pace_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(habits.pace_ms)).await;
                }
            }
        }));
    }
    for w in writing {
        w.await.unwrap();
    }
    let epoch = marker(&app).await;
    done_tx.send_replace(true);

    let prefs = listing(&app, "/prefs", Kind::Table).await;
    let tags = listing(&app, "/tags", Kind::Set).await;
    let mut total = Stats::default();
    for f in followers {
        let mut client = f.await.unwrap();
        client.catch_up(&app, epoch, &errors).await;
        for (k, (name, _)) in client.follow.kinds().iter().enumerate() {
            let want = if *name == "prefs" { &prefs } else { &tags };
            if client.models[k].at(epoch) && &client.models[k].rows != want {
                fail(
                    &errors,
                    format!(
                        "{:?} client's {name} differs from the store at epoch {epoch}",
                        client.follow
                    ),
                );
            }
        }
        total.connects += client.stats.connects;
        total.resumed_without_snapshot += client.stats.resumed_without_snapshot;
        total.snapshots += client.stats.snapshots;
        total.changes += client.stats.changes;
    }
    let errors = errors.lock().unwrap();
    assert!(
        errors.is_empty(),
        "{} problems, first: {:#?}",
        errors.len(),
        &errors[..errors.len().min(10)]
    );
    total
}

/// Many clients dropping and resuming while writers commit: each must
/// end equal to the store, never seeing a change twice, out of order,
/// or against state it doesn't hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_clients_converge_under_churn() {
    let n = scale();
    let stats = converge(
        4,
        150 * n,
        12 * n,
        Habits {
            events: 40,
            pause_ms: (0, 30),
            pace_ms: 0,
        },
    )
    .await;
    eprintln!("churn: {stats:?}");
    assert!(
        stats.resumed_without_snapshot > 0,
        "some reconnects should resume from the ring: {stats:?}"
    );
}

/// Clients so few and so idle between connections that their hubs stop
/// and restart over and over: nothing committed while no hub ran may go
/// missing from a resume.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hub_restarts_lose_nothing() {
    let n = scale();
    let stats = converge(
        1,
        120 * n,
        2,
        Habits {
            events: 5,
            pause_ms: (260, 450),
            pace_ms: 20,
        },
    )
    .await;
    eprintln!("hub restarts: {stats:?}");
    assert!(
        stats.connects >= 8,
        "too few reconnects to have restarted hubs: {stats:?}"
    );
}

/// Every client stops reading while far more commits land than a hub
/// buffers; all of them fall behind at once and must resync (with the
/// whole listing, `snapshot=all`) and converge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lag_storm_resyncs_every_client() {
    let clients = 40 * scale();
    let app = app();
    let errors: Errors = Arc::default();
    let mut followers = Vec::new();
    for c in 0..clients {
        let mut client = Client::new(
            if c % 2 == 0 {
                Follow::Both
            } else {
                Follow::Prefs
            },
            c as u64,
        );
        let mut s = client.connect(&app, &errors, true).await.unwrap();
        for _ in 0..client.follow.kinds().len() {
            let e = s.next().await;
            client.take(&e, &errors);
        }
        followers.push((client, s));
    }
    let ring = 1024;
    let start = Instant::now();
    let mut rng = Rng::new(99);
    for _ in 0..2 * ring + 100 {
        write_once(&app, &mut rng).await;
    }
    let wrote = start.elapsed();
    let epoch = marker(&app).await;

    let start = Instant::now();
    let mut tasks = Vec::new();
    for (mut client, mut s) in followers {
        let errors = errors.clone();
        tasks.push(tokio::spawn(async move {
            let before = client.stats.snapshots;
            let deadline = Instant::now() + Duration::from_secs(30);
            while !client.models.iter().all(|m| m.at(epoch)) && Instant::now() < deadline {
                match s.next_within(Duration::from_secs(1)).await {
                    Next::Event(e) => {
                        if !client.take(&e, &errors) {
                            break;
                        }
                    }
                    Next::Quiet => {}
                    Next::Ended => break,
                }
            }
            (client.stats.snapshots > before, client)
        }));
    }
    let prefs = listing(&app, "/prefs", Kind::Table).await;
    let tags = listing(&app, "/tags", Kind::Set).await;
    let mut resynced = 0;
    for t in tasks {
        let (did, client) = t.await.unwrap();
        resynced += did as usize;
        for (k, (name, _)) in client.follow.kinds().iter().enumerate() {
            let want = if *name == "prefs" { &prefs } else { &tags };
            if !client.models[k].at(epoch) {
                fail(
                    &errors,
                    format!("{name}: stuck at {}", client.models[k].epoch),
                );
            } else if &client.models[k].rows != want {
                fail(
                    &errors,
                    format!(
                        "{name}: differs from the store: {}",
                        diff(&client.models[k].rows, want)
                    ),
                );
            }
        }
    }
    eprintln!(
        "lag storm: {clients} clients, {} commits in {wrote:?}, all caught up in {:?}, {resynced} resynced",
        2 * ring + 101,
        start.elapsed()
    );
    let errors = errors.lock().unwrap();
    assert!(errors.is_empty(), "{:#?}", &errors[..errors.len().min(10)]);
    assert_eq!(resynced, clients, "every client fell behind and resynced");
}

// ---------------------------------------------------------------------
// Latency under pressure (measured; run with --ignored --release)

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

async fn timed_writes(app: &Router, n: usize) -> Vec<Duration> {
    let mut lat = Vec::with_capacity(n);
    for i in 0..n {
        let t = Instant::now();
        let res = send(app, Method::PUT, &format!("/prefs/w{i}"), Some(json!(i))).await;
        assert!(res.status.is_success(), "{:?}", res.body);
        lat.push(t.elapsed());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    lat.sort();
    lat
}

/// Reads and writes share tokio's blocking pool, and only writes are
/// admission-controlled. Thousands of slow reads (a custom route that
/// takes 20 ms, standing in for a big listing or search) shouldn't push
/// a write's latency into seconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measures latency; run with --release --ignored --nocapture"]
async fn reads_cannot_starve_writes() {
    let app = server()
        .get(
            "/slow",
            "A slow read",
            |_: &common::ShopRead<'_>, _: NoParams| {
                std::thread::sleep(Duration::from_millis(20));
                Ok(())
            },
        )
        .into_router();
    let quiet = timed_writes(&app, 50).await;

    let readers = 4000 * scale();
    let mut flood = Vec::new();
    for _ in 0..readers {
        let app = app.clone();
        flood.push(tokio::spawn(async move {
            send(&app, Method::GET, "/slow", None).await.status
        }));
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let loaded = timed_writes(&app, 50).await;
    for f in flood {
        assert_eq!(f.await.unwrap(), StatusCode::OK);
    }
    eprintln!(
        "write latency p50/p99: quiet {:?}/{:?}, under {readers} slow reads {:?}/{:?}",
        percentile(&quiet, 0.5),
        percentile(&quiet, 0.99),
        percentile(&loaded, 0.5),
        percentile(&loaded, 0.99),
    );
    assert!(
        percentile(&loaded, 0.99) < Duration::from_millis(500),
        "writes queued behind reads"
    );
}

/// A read with `bog-min-epoch` ahead of the store waits (5 s by
/// default), polling every 10 ms. Thousands of them, from clients that
/// sent a bad epoch, shouldn't slow everyone else down.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measures latency; run with --release --ignored --nocapture"]
async fn min_epoch_waiters_stay_cheap() {
    let app = server()
        .min_epoch_wait(Duration::from_secs(3))
        .into_router();
    send(&app, Method::PUT, "/prefs/a", Some(json!(1))).await;
    let quiet = timed_reads(&app, 50).await;

    let waiters = 5000 * scale();
    let mut held = Vec::new();
    for _ in 0..waiters {
        let app = app.clone();
        held.push(tokio::spawn(async move {
            send_with(
                &app,
                Method::GET,
                "/prefs",
                None,
                &[("bog-min-epoch", "1000000")],
            )
            .await
            .status
        }));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let loaded = timed_reads(&app, 50).await;
    for h in held {
        assert_eq!(h.await.unwrap(), StatusCode::SERVICE_UNAVAILABLE);
    }
    eprintln!(
        "read latency p50/p99: quiet {:?}/{:?}, beside {waiters} min-epoch waiters {:?}/{:?}",
        percentile(&quiet, 0.5),
        percentile(&quiet, 0.99),
        percentile(&loaded, 0.5),
        percentile(&loaded, 0.99),
    );
    assert!(percentile(&loaded, 0.99) < Duration::from_millis(200));
}

async fn timed_reads(app: &Router, n: usize) -> Vec<Duration> {
    let mut lat = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        assert_eq!(
            send(app, Method::GET, "/prefs", None).await.status,
            StatusCode::OK
        );
        lat.push(t.elapsed());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    lat.sort();
    lat
}

// ---------------------------------------------------------------------
// Real sockets: `Server::serve`

#[cfg(feature = "serve")]
mod sockets {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    use super::common::server;

    /// Serve on an ephemeral loopback port; its address once listening.
    async fn serving(
        configure: impl FnOnce(
            bog_http::Server<super::common::Shop>,
        ) -> bog_http::Server<super::common::Shop>,
    ) -> (
        String,
        tokio::task::JoinHandle<std::io::Result<()>>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let disc = dir.path().join("server.json");
        let task = tokio::spawn(
            configure(server())
                .discovery(&disc)
                .serve("127.0.0.1:0", |r| r),
        );
        for _ in 0..200 {
            if let Ok(b) = std::fs::read(&disc) {
                let v: serde_json::Value = serde_json::from_slice(&b).unwrap();
                let url = v["url"].as_str().unwrap();
                return (url.trim_start_matches("http://").to_owned(), task, dir);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("never listened");
    }

    /// A client that starts an upload and goes silent holds its request
    /// open. Shutdown (here, idle exit) waits for in-flight requests, so
    /// without a read timeout it waits forever, and the store is never
    /// checkpointed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_upload_does_not_hold_shutdown_forever() {
        let (addr, task, _dir) = serving(|s| s.idle_exit(Duration::from_millis(300))).await;
        let mut c = TcpStream::connect(&addr).unwrap();
        write!(
            c,
            "PUT /prefs/a HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n1"
        )
        .unwrap();
        let ended = tokio::time::timeout(Duration::from_secs(10), task).await;
        drop(c);
        assert!(
            ended.is_ok(),
            "idle exit fired, but serve() never returned: it waits on the stalled upload"
        );
    }

    /// A connection that sends half a request line and stops: the
    /// cheapest way to hold a server's socket. hyper closes those after
    /// 30 s, but only when given a timer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "waits 35 s; run with --ignored"]
    async fn half_sent_requests_are_dropped() {
        let (addr, task, _dir) = serving(|s| s).await;
        let mut c = TcpStream::connect(&addr).unwrap();
        c.write_all(b"GET /_health HTTP/1.1\r\n").unwrap();
        tokio::time::sleep(Duration::from_secs(35)).await;
        c.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
        let mut buf = [0u8; 256];
        // A server that gave up has answered 408 or closed: the read
        // returns. One still waiting leaves it to time out.
        let closed = c.read(&mut buf).is_ok();
        task.abort();
        assert!(
            closed,
            "the half-sent request still holds its connection after 35 s"
        );
    }
}
