//! Live feeds: one hub per streamed resource, fanning its fold
//! subscription out to every client.
//!
//! A hub starts with its first client and stops with its last. Its
//! thread drains the subscription (fold's channel is unbounded, so
//! something must) and broadcasts each commit's changes once, already
//! serialized, over a bounded channel. A client that falls behind that
//! channel isn't buffered without bound: it gets a fresh snapshot.
//!
//! Each hub also keeps its last [`RING`] changes, so a client that
//! reconnects with `Last-Event-ID` resumes where it left off, without a
//! snapshot, if nothing it missed has been evicted.
//!
//! Gaplessness: a client subscribes to the hub *before* taking its
//! snapshot, then drops changes at or below the snapshot's epoch. A
//! change above it committed after the snapshot, so after the
//! subscription, so it arrives.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use tokio::sync::broadcast;

use crate::resource::Live;

/// Changes each hub keeps for resuming clients, and the depth of its
/// broadcast channel.
pub const RING: usize = 1024;

/// How long a hub's thread waits on its subscription between checks for
/// whether anyone is still listening.
const IDLE_CHECK: Duration = Duration::from_millis(250);

/// One commit's changes to one resource, serialized once for every
/// client: `{"epoch", "changes": [...]}`.
pub(crate) struct Change {
    pub(crate) epoch: u64,
    pub(crate) data: String,
}

struct Hub {
    tx: broadcast::Sender<Arc<Change>>,
    ring: Mutex<Ring>,
}

struct Ring {
    changes: VecDeque<Arc<Change>>,
    /// Changes at or below this epoch can't be replayed: they predate
    /// the hub, or were evicted.
    gone_through: u64,
}

/// What joining a hub hands a client.
pub(crate) struct Joined {
    pub(crate) rx: broadcast::Receiver<Arc<Change>>,
    /// Changes after the client's `Last-Event-ID`, if all of them are
    /// still in the ring.
    pub(crate) replay: Option<Vec<Arc<Change>>>,
}

/// Every running hub, by resource index.
#[derive(Default)]
pub(crate) struct Hubs {
    hubs: Mutex<HashMap<usize, Arc<Hub>>>,
}

impl Hubs {
    /// Join resource `i`'s hub, starting it with `start` (the snapshot
    /// epoch its subscription begins after, and the feed) if it isn't
    /// running. `None` if the resource can't stream.
    pub(crate) fn join(
        self: &Arc<Self>,
        i: usize,
        resume: Option<u64>,
        start: impl FnOnce() -> Option<(u64, Live)>,
    ) -> Option<Joined> {
        let mut hubs = self.hubs.lock().unwrap();
        let hub = match hubs.get(&i) {
            Some(hub) => hub.clone(),
            None => {
                let (since, live) = start()?;
                let hub = Arc::new(Hub {
                    tx: broadcast::Sender::new(RING),
                    ring: Mutex::new(Ring {
                        changes: VecDeque::new(),
                        gone_through: since,
                    }),
                });
                let (hubs_, hub_) = (self.clone(), hub.clone());
                std::thread::Builder::new()
                    .name(format!("fold-http hub {i}"))
                    .spawn(move || run(hubs_, i, hub_, live))
                    .expect("spawn a hub thread");
                hubs.insert(i, hub.clone());
                tracing::debug!(target: "fold_http", resource = i, since, "hub started");
                hub
            }
        };
        // Subscribe and read the ring under the ring's lock, which the
        // hub's thread holds to append and broadcast: nothing slips
        // between the replay and the live tail.
        let ring = hub.ring.lock().unwrap();
        let rx = hub.tx.subscribe();
        let replay = resume.filter(|&e| e >= ring.gone_through).map(|e| {
            ring.changes
                .iter()
                .filter(|c| c.epoch > e)
                .cloned()
                .collect()
        });
        Some(Joined { rx, replay })
    }

    /// How many hubs are running: one per streamed resource.
    #[cfg(test)]
    pub(crate) fn running(&self) -> usize {
        self.hubs.lock().unwrap().len()
    }
}

/// A hub's thread: drain, serialize, keep, broadcast; stop when the last
/// client has gone or the database has closed.
fn run(hubs: Arc<Hubs>, i: usize, hub: Arc<Hub>, mut live: Live) {
    loop {
        match live.poll(IDLE_CHECK) {
            Ok(Some((epoch, changes))) => {
                let data = json!({ "epoch": epoch, "changes": changes }).to_string();
                let change = Arc::new(Change { epoch, data });
                let mut ring = hub.ring.lock().unwrap();
                if ring.changes.len() == RING
                    && let Some(old) = ring.changes.pop_front()
                {
                    ring.gone_through = old.epoch;
                }
                ring.changes.push_back(change.clone());
                let _ = hub.tx.send(change);
            }
            Ok(None) => {}
            Err(_closed) => {
                stop(&hubs, i, &hub, true);
                return;
            }
        }
        if hub.tx.receiver_count() == 0 && stop(&hubs, i, &hub, false) {
            return;
        }
    }
}

/// Unregister `hub` if no client holds it (or unconditionally). Under
/// the registry's lock, so a client can't join a hub that's leaving.
fn stop(hubs: &Hubs, i: usize, hub: &Arc<Hub>, always: bool) -> bool {
    let mut map = hubs.hubs.lock().unwrap();
    if !always && hub.tx.receiver_count() > 0 {
        return false;
    }
    if map.get(&i).is_some_and(|h| Arc::ptr_eq(h, hub)) {
        map.remove(&i);
        tracing::debug!(target: "fold_http", resource = i, "hub stopped");
    }
    true
}

#[cfg(test)]
mod tests {
    use fold3::schema::*;

    use super::*;

    #[derive(Schema)]
    struct Kv {
        rows: Table<u32, u32>,
    }

    impl Declare for Kv {
        fn declare(_: &mut KvBuilder) {}
    }

    fn feed(db: &Db<Kv>) -> impl FnOnce() -> Option<(u64, Live)> {
        let snap = db.snapshot();
        let read = db.at(&snap);
        let live = Live::new(read.rows.subscribe(), |(k, c)| json!([k, c.after()]));
        let since = snap.epoch();
        move || Some((since, live))
    }

    #[test]
    fn one_hub_per_resource_until_its_last_client_leaves() {
        let db = Kv::memory().build().unwrap();
        let hubs = Arc::new(Hubs::default());
        let a = hubs.join(0, None, feed(&db)).unwrap();
        let b = hubs
            .join(0, None, || unreachable!("the running hub is shared"))
            .unwrap();
        assert_eq!(hubs.running(), 1);

        db.write(|tx| tx.rows.upsert(1, 10)).unwrap();
        let mut a = a.rx;
        let got = a.blocking_recv().unwrap();
        assert_eq!(got.data, r#"{"changes":[[1,10]],"epoch":1}"#);
        assert_eq!(
            b.rx.len(),
            1,
            "every client gets the same serialized change"
        );

        drop((a, b));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while hubs.running() > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the hub stops once nobody listens"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        // And starts afresh for the next client.
        assert!(hubs.join(0, None, feed(&db)).is_some());
        assert_eq!(hubs.running(), 1);
    }
}
