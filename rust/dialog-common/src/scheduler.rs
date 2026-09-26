//! A priority window over the env's remote block reads: the "who goes
//! first" half of bead dialog-db-88.
//!
//! Every reader that learns it needs a block submits it the moment it
//! knows (a traversal when a node lands, integrate's opening pass when a
//! path stops, a scan when it sees the next sibling, a preload when its
//! range is hinted). Nothing in those readers meters the reads out any
//! more; a per-reader cap could not express what actually matters,
//! which is that a demand read is never queued behind a speculative
//! walk and that a request still waiting here can be reordered or
//! dropped for free, while one already handed to the transport cannot.
//!
//! [`Scheduler`] holds one lane per remote site. A lane admits at most
//! its window of reads at once and picks the next one by
//! [`Priority`], then by arrival. It is also the digest-keyed
//! single-flight ([`WeakFlight`](crate::WeakFlight) folded in): joiners
//! of one key share one computation, and a joiner arriving at a higher
//! priority promotes a waiting key.
//!
//! The liveness rule from [`Flight`](crate::Flight) is kept: work makes
//! progress exactly while some caller is awaiting it, and nothing here
//! waits on progress it cannot drive. A caller waiting for a slot
//! co-drives the admitted work of its lane (it polls those shared
//! futures itself), so a holder whose own joiner has parked, say inside
//! an unpolled read-ahead set, is still driven to completion by whoever
//! is waiting behind it. A slot is held by a guard inside the shared
//! future, so abandoning the work (every joiner dropped) frees the slot
//! with it, and a waiter that is dropped before admission simply leaves
//! the queue.
//!
//! The window is a per-site setting. What the transport can carry is
//! the transport's fact (the browser caps connections per host on
//! HTTP/1.1, an h2 origin multiplexes), and a page cannot read it for a
//! cross-origin response without `Timing-Allow-Origin`, so the window
//! defaults to [`DEFAULT_WINDOW`] and the application sets it per site
//! from what it knows about the route.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use futures_util::FutureExt;
use futures_util::future::Shared;

use crate::{ConditionalSend, ConditionalSync};

#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;

#[cfg(not(target_arch = "wasm32"))]
type Stored<V> = Shared<BoxFuture<'static, V>>;
#[cfg(target_arch = "wasm32")]
type Stored<V> = Shared<LocalBoxFuture<'static, V>>;

#[cfg(not(target_arch = "wasm32"))]
type WeakStored<V> = futures_util::future::WeakShared<BoxFuture<'static, V>>;
#[cfg(target_arch = "wasm32")]
type WeakStored<V> = futures_util::future::WeakShared<LocalBoxFuture<'static, V>>;

#[cfg(not(target_arch = "wasm32"))]
type Lock<T> = parking_lot::Mutex<T>;
// A worker context is single-threaded; RefCell is enough, and no borrow
// is ever held across an await or a poll of another future.
#[cfg(target_arch = "wasm32")]
type Lock<T> = std::cell::RefCell<T>;

#[cfg(not(target_arch = "wasm32"))]
type Guard<'a, T> = parking_lot::MutexGuard<'a, T>;
#[cfg(target_arch = "wasm32")]
type Guard<'a, T> = std::cell::RefMut<'a, T>;

#[cfg(not(target_arch = "wasm32"))]
fn lock<T>(lock: &Lock<T>) -> Guard<'_, T> {
    lock.lock()
}

#[cfg(target_arch = "wasm32")]
fn lock<T>(lock: &Lock<T>) -> Guard<'_, T> {
    lock.borrow_mut()
}

/// How many reads a lane admits at once until the application sets its
/// window.
///
/// Sized for an origin that multiplexes (h2 and h3 carry dozens of
/// streams on one connection). Over HTTP/1.1 a browser holds six
/// connections per host and queues the rest itself, which loses nothing
/// but the ordering of those six; a site known to be on such a route
/// can be set narrower.
pub const DEFAULT_WINDOW: usize = 64;

/// How urgently a read is wanted, highest first.
///
/// Demand is a read some evaluation is blocked on. The two speculative
/// ranks are the preload queue's: a range the evaluation has committed
/// to, and one a decision point ahead may abandon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Priority {
    /// Speculative: the evaluation may read this; a decision ahead may
    /// abandon it.
    Maybe,
    /// Speculative: the evaluation will read this unless it fails first.
    Likely,
    /// Someone is waiting on this read right now.
    Demand,
}

/// How many joins a lane has taken at each priority since it was
/// created: which readers a site's traffic came from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Tally {
    /// Joins at [`Priority::Demand`].
    pub demand: u64,
    /// Joins at [`Priority::Likely`].
    pub likely: u64,
    /// Joins at [`Priority::Maybe`].
    pub maybe: u64,
}

/// A per-site priority window over keyed, shared computations. See the
/// module docs.
pub struct Scheduler<Site, K, V> {
    state: Arc<Lock<State<Site, K, V>>>,
}

struct State<Site, K, V> {
    /// In-flight work by key, held weakly (see `WeakFlight`).
    inflight: HashMap<K, WeakStored<V>>,
    lanes: HashMap<Site, Lane<K>>,
    default_window: usize,
    /// Arrival order across every lane.
    sequence: u64,
}

struct Lane<K> {
    window: usize,
    /// Keys holding a slot: their shared future has been let through and
    /// has not completed or been abandoned.
    admitted: HashSet<K>,
    /// Keys waiting for a slot, with what the heap entry must match to
    /// still be current.
    waiting: HashMap<K, Waiting>,
    /// Admission order. Entries go stale when a key is promoted, admitted
    /// or dropped; they are skipped on pop.
    order: BinaryHeap<Entry<K>>,
    tally: Tally,
}

struct Waiting {
    priority: Priority,
    sequence: u64,
    waker: Option<Waker>,
}

struct Entry<K> {
    priority: Priority,
    sequence: u64,
    key: K,
}

impl<K> PartialEq for Entry<K> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl<K> Eq for Entry<K> {}

impl<K> PartialOrd for Entry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for Entry<K> {
    /// Higher priority first; among equals, the earlier arrival.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

impl<K> Lane<K>
where
    K: Eq + Hash,
{
    fn new(window: usize) -> Self {
        Self {
            window,
            admitted: HashSet::new(),
            waiting: HashMap::new(),
            order: BinaryHeap::new(),
            tally: Tally::default(),
        }
    }

    /// Let waiting keys through while the window has room, highest
    /// priority first, waking each one admitted.
    fn admit_ready(&mut self) {
        while self.admitted.len() < self.window {
            let Some(entry) = self.order.pop() else {
                return;
            };
            let current = self.waiting.get(&entry.key).is_some_and(|waiting| {
                waiting.priority == entry.priority && waiting.sequence == entry.sequence
            });
            if !current {
                continue;
            }
            let waiting = self
                .waiting
                .remove(&entry.key)
                .expect("checked to be waiting above");
            self.admitted.insert(entry.key);
            if let Some(waker) = waiting.waker {
                waker.wake();
            }
        }
    }

    fn count(&mut self, priority: Priority) {
        match priority {
            Priority::Demand => self.tally.demand += 1,
            Priority::Likely => self.tally.likely += 1,
            Priority::Maybe => self.tally.maybe += 1,
        }
    }
}

impl<K> Lane<K>
where
    K: Eq + Hash + Clone,
{
    /// Raise a waiting key's priority; a key already admitted or not
    /// waiting here is left alone.
    fn promote(&mut self, key: &K, priority: Priority) {
        let Some(waiting) = self.waiting.get_mut(key) else {
            return;
        };
        if waiting.priority >= priority {
            return;
        }
        waiting.priority = priority;
        self.order.push(Entry {
            priority,
            sequence: waiting.sequence,
            key: key.clone(),
        });
    }
}

impl<Site, K, V> State<Site, K, V>
where
    Site: Eq + Hash + Clone,
    K: Eq + Hash + Clone,
{
    fn lane(&mut self, site: &Site) -> &mut Lane<K> {
        let window = self.default_window;
        self.lanes
            .entry(site.clone())
            .or_insert_with(|| Lane::new(window))
    }
}

impl<Site, K, V> Default for Scheduler<Site, K, V> {
    fn default() -> Self {
        Self::new(DEFAULT_WINDOW)
    }
}

impl<Site, K, V> std::fmt::Debug for Scheduler<Site, K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("Scheduler")
            .field("inflight", &state.inflight.len())
            .field("lanes", &state.lanes.len())
            .finish()
    }
}

impl<Site, K, V> Scheduler<Site, K, V> {
    /// A scheduler whose lanes open with `window` slots each.
    pub fn new(window: usize) -> Self {
        Self {
            state: Arc::new(Lock::new(State {
                inflight: HashMap::new(),
                lanes: HashMap::new(),
                default_window: window,
                sequence: 0,
            })),
        }
    }

    fn lock(&self) -> Guard<'_, State<Site, K, V>> {
        lock(&self.state)
    }
}

impl<Site, K, V> Scheduler<Site, K, V>
where
    Site: Eq + Hash + Clone + Unpin + ConditionalSend + 'static,
    K: Eq + Hash + Clone + Unpin + ConditionalSend + 'static,
    V: Clone + ConditionalSend + ConditionalSync + 'static,
{
    /// Set how many reads `site`'s lane admits at once. Widening lets
    /// waiting reads through immediately; narrowing takes effect as
    /// admitted reads complete.
    pub fn set_window(&self, site: &Site, window: usize) {
        let mut state = self.lock();
        let lane = state.lane(site);
        lane.window = window;
        lane.admit_ready();
    }

    /// The window `site`'s lane admits at once.
    pub fn window(&self, site: &Site) -> usize {
        self.lock().lane(site).window
    }

    /// Reads of `site` holding a slot right now.
    pub fn admitted(&self, site: &Site) -> usize {
        self.lock().lane(site).admitted.len()
    }

    /// Reads of `site` waiting for a slot right now.
    pub fn waiting(&self, site: &Site) -> usize {
        self.lock().lane(site).waiting.len()
    }

    /// How many joins `site`'s lane has taken at each priority.
    pub fn tally(&self, site: &Site) -> Tally {
        self.lock().lane(site).tally
    }

    /// Join the in-flight computation for `key`, or queue `make()` as the
    /// shared one on `site`'s lane at `priority`.
    ///
    /// Every joiner polls the shared future itself, so completion never
    /// depends on the caller that started it, and a joiner waiting for a
    /// slot drives the lane's admitted work meanwhile. The work is held
    /// weakly: if every joiner drops before completion it drops with
    /// them, freeing its slot or its place in the queue, and the next
    /// caller under the key starts clean. Joining a waiting key at a
    /// higher priority promotes it. Outcomes, failures included, are
    /// shared only with callers in flight, never cached.
    pub async fn join<F, Make>(&self, site: Site, key: K, priority: Priority, make: Make) -> V
    where
        Make: FnOnce() -> F,
        F: Future<Output = V> + ConditionalSend + 'static,
    {
        let shared = {
            let mut state = self.lock();
            state.lane(&site).count(priority);
            match state.inflight.get(&key).and_then(|weak| weak.upgrade()) {
                Some(shared) => {
                    state.lane(&site).promote(&key, priority);
                    shared
                }
                None => {
                    let ticket = Ticket {
                        state: self.state.clone(),
                        site,
                        key: key.clone(),
                        priority,
                        phase: Phase::Queued,
                    };
                    let work = make();
                    let future = async move {
                        let slot = ticket.await;
                        let value = work.await;
                        drop(slot);
                        value
                    };
                    #[cfg(not(target_arch = "wasm32"))]
                    let shared = future.boxed().shared();
                    #[cfg(target_arch = "wasm32")]
                    let shared = future.boxed_local().shared();
                    if let Some(weak) = shared.downgrade() {
                        state.inflight.insert(key.clone(), weak);
                    }
                    shared
                }
            }
            // The guard drops here: the lock is never held across an await.
        };

        let value = shared.clone().await;

        // Remove the entry we completed (or one already dead); a fresh
        // flight started under the same key is left alone.
        let mut state = self.lock();
        if state.inflight.get(&key).is_some_and(|current| {
            current
                .upgrade()
                .is_none_or(|current| current.ptr_eq(&shared))
        }) {
            state.inflight.remove(&key);
        }
        drop(state);

        value
    }
}

#[derive(PartialEq, Eq)]
enum Phase {
    /// Waiting for, or granted but not yet holding, a slot.
    Queued,
    /// The slot has been handed to its guard.
    Admitted,
}

/// The wait for a slot, at the head of every shared future.
struct Ticket<Site, K, V>
where
    Site: Eq + Hash,
    K: Eq + Hash,
{
    state: Arc<Lock<State<Site, K, V>>>,
    site: Site,
    key: K,
    priority: Priority,
    phase: Phase,
}

/// Holds a slot on a lane for as long as the work runs. Dropping it,
/// on completion or on abandonment, frees the slot and admits whoever
/// is next.
struct Slot<Site, K, V>
where
    Site: Eq + Hash,
    K: Eq + Hash,
{
    state: Arc<Lock<State<Site, K, V>>>,
    site: Site,
    key: K,
}

impl<Site, K, V> Future for Ticket<Site, K, V>
where
    Site: Eq + Hash + Clone + Unpin,
    K: Eq + Hash + Clone + Unpin,
    V: Clone,
{
    type Output = Slot<Site, K, V>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            // Queue, or refresh the waker, and take a slot if one is
            // free for us; otherwise collect the lane's admitted work.
            let admitted: Vec<Stored<V>> = {
                let mut state = lock(&this.state);
                let State {
                    inflight,
                    lanes,
                    default_window,
                    sequence,
                } = &mut *state;
                let lane = lanes
                    .entry(this.site.clone())
                    .or_insert_with(|| Lane::new(*default_window));
                if !lane.admitted.contains(&this.key) {
                    match lane.waiting.get_mut(&this.key) {
                        Some(waiting) => waiting.waker = Some(context.waker().clone()),
                        None => {
                            let arrived = *sequence;
                            *sequence += 1;
                            lane.waiting.insert(
                                this.key.clone(),
                                Waiting {
                                    priority: this.priority,
                                    sequence: arrived,
                                    waker: Some(context.waker().clone()),
                                },
                            );
                            lane.order.push(Entry {
                                priority: this.priority,
                                sequence: arrived,
                                key: this.key.clone(),
                            });
                        }
                    }
                    lane.admit_ready();
                }
                if lane.admitted.contains(&this.key) {
                    this.phase = Phase::Admitted;
                    return Poll::Ready(this.slot());
                }
                lane.admitted
                    .iter()
                    .filter_map(|key| inflight.get(key).and_then(|weak| weak.upgrade()))
                    .collect()
                // The lock drops here, before any other future is polled
                // and before the collected handles can drop.
            };

            // Co-drive the admitted work: whoever is waiting on this lane
            // makes progress for whatever holds its slots, so a holder
            // whose own joiner parked cannot hold the lane hostage.
            let mut landed = false;
            for mut shared in admitted {
                if shared.poll_unpin(context).is_ready() {
                    landed = true;
                }
            }
            if !landed {
                return Poll::Pending;
            }
            // A completion freed a slot and ran admission; look again.
        }
    }
}

impl<Site, K, V> Ticket<Site, K, V>
where
    Site: Eq + Hash + Clone,
    K: Eq + Hash + Clone,
{
    fn slot(&self) -> Slot<Site, K, V> {
        Slot {
            state: self.state.clone(),
            site: self.site.clone(),
            key: self.key.clone(),
        }
    }
}

impl<Site, K, V> Drop for Ticket<Site, K, V>
where
    Site: Eq + Hash,
    K: Eq + Hash,
{
    fn drop(&mut self) {
        if self.phase == Phase::Admitted {
            return;
        }
        // Abandoned before the slot was taken: leave the queue, and give
        // back a slot granted while nobody was polling.
        let mut state = lock(&self.state);
        let Some(lane) = state.lanes.get_mut(&self.site) else {
            return;
        };
        lane.waiting.remove(&self.key);
        if lane.admitted.remove(&self.key) {
            lane.admit_ready();
        }
    }
}

impl<Site, K, V> Drop for Slot<Site, K, V>
where
    Site: Eq + Hash,
    K: Eq + Hash,
{
    fn drop(&mut self) {
        let mut state = lock(&self.state);
        if let Some(lane) = state.lanes.get_mut(&self.site) {
            lane.admitted.remove(&self.key);
            lane.admit_ready();
        }
    }
}

// Gated on `helpers` because `dialog_common::test` (the cross-target
// async test macro) is only exported there; CI's test archives build the
// workspace with `--features integration-tests`, which implies it.
#[cfg(all(test, feature = "helpers"))]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    use futures_util::future::{join_all, join3, join4};

    use super::{Priority, Scheduler, Tally};
    use crate::ConditionalSend;

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A piece of work that records its first poll and completes once
    /// opened. Every test hand-polls with a noop waker, so a gate needs
    /// no waker of its own: the next poll re-checks it.
    #[derive(Clone, Default)]
    struct Gate {
        started: Arc<AtomicBool>,
        open: Arc<AtomicBool>,
    }

    impl Gate {
        fn started(&self) -> bool {
            self.started.load(Ordering::SeqCst)
        }

        fn open(&self) {
            self.open.store(true, Ordering::SeqCst);
        }

        fn work(&self, value: u8) -> impl Future<Output = u8> + ConditionalSend + 'static {
            let started = self.started.clone();
            let open = self.open.clone();
            std::future::poll_fn(move |_| {
                started.store(true, Ordering::SeqCst);
                if open.load(Ordering::SeqCst) {
                    Poll::Ready(value)
                } else {
                    Poll::Pending
                }
            })
        }
    }

    type Scheduled = Scheduler<&'static str, u8, u8>;

    fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        Pin::new(future).poll(&mut context)
    }

    /// A lane admits at most its window, first come first served among
    /// equals, and the next in line goes out the moment a slot frees.
    #[dialog_common::test]
    async fn it_admits_at_most_the_window_per_site() {
        let scheduler = Scheduled::new(2);
        let gates: Vec<Gate> = (0..5).map(|_| Gate::default()).collect();
        let mut all = Box::pin(join_all(gates.iter().enumerate().map(|(at, gate)| {
            scheduler.join("site", at as u8, Priority::Demand, move || {
                gate.work(at as u8)
            })
        })));

        assert!(poll_once(&mut all).is_pending());
        let started: Vec<bool> = gates.iter().map(Gate::started).collect();
        assert_eq!(started, [true, true, false, false, false]);
        assert_eq!(scheduler.admitted(&"site"), 2);
        assert_eq!(scheduler.waiting(&"site"), 3);

        gates[0].open();
        assert!(poll_once(&mut all).is_pending());
        let started: Vec<bool> = gates.iter().map(Gate::started).collect();
        assert_eq!(started, [true, true, true, false, false]);

        for gate in &gates {
            gate.open();
        }
        // Each poll lands the admitted work and admits the next.
        let mut polls = 0;
        let values = loop {
            if let Poll::Ready(values) = poll_once(&mut all) {
                break values;
            }
            polls += 1;
            assert!(polls < 10, "the lane drains in a few polls");
        };
        assert_eq!(values, [0, 1, 2, 3, 4]);
        assert_eq!(scheduler.admitted(&"site"), 0);
    }

    /// Priority decides who takes a freed slot, and arrival breaks ties:
    /// a Likely read queued after two Maybe reads goes before both.
    #[dialog_common::test]
    async fn it_admits_by_priority_then_arrival() {
        let scheduler = Scheduled::new(1);
        let (a, b, c, d) = (
            Gate::default(),
            Gate::default(),
            Gate::default(),
            Gate::default(),
        );
        let mut all = Box::pin(join4(
            scheduler.join("site", 1, Priority::Demand, || a.work(1)),
            scheduler.join("site", 2, Priority::Maybe, || b.work(2)),
            scheduler.join("site", 3, Priority::Likely, || c.work(3)),
            scheduler.join("site", 4, Priority::Maybe, || d.work(4)),
        ));

        assert!(poll_once(&mut all).is_pending());
        assert!(a.started() && !b.started() && !c.started() && !d.started());

        a.open();
        assert!(poll_once(&mut all).is_pending());
        assert!(
            c.started(),
            "the Likely read goes before the earlier Maybe reads"
        );
        assert!(!b.started() && !d.started());

        c.open();
        assert!(poll_once(&mut all).is_pending());
        assert!(
            b.started(),
            "among Maybe reads the earlier arrival goes first"
        );
        assert!(!d.started());

        b.open();
        assert!(poll_once(&mut all).is_pending());
        assert!(d.started());
    }

    /// Joining a waiting key at a higher priority moves it up: a demand
    /// read for a block a speculative walk already queued is not stuck
    /// behind the walk.
    #[dialog_common::test]
    async fn it_promotes_a_waiting_key_joined_at_a_higher_priority() {
        let scheduler = Scheduled::new(1);
        let (a, b, c) = (Gate::default(), Gate::default(), Gate::default());
        let mut all = Box::pin(join3(
            scheduler.join("site", 1, Priority::Demand, || a.work(1)),
            scheduler.join("site", 2, Priority::Maybe, || b.work(2)),
            scheduler.join("site", 3, Priority::Likely, || c.work(3)),
        ));
        assert!(poll_once(&mut all).is_pending());
        assert!(a.started() && !b.started() && !c.started());

        let mut demand = Box::pin(scheduler.join("site", 2, Priority::Demand, || b.work(2)));
        assert!(poll_once(&mut demand).is_pending());
        assert_eq!(
            scheduler.waiting(&"site"),
            2,
            "the demand joiner shares the queued read"
        );

        a.open();
        assert!(poll_once(&mut all).is_pending());
        assert!(b.started(), "the promoted read takes the slot");
        assert!(!c.started());

        b.open();
        assert_eq!(poll_once(&mut demand), Poll::Ready(2));
    }

    /// A waiter drives the lane's admitted work: a holder whose own
    /// joiner was polled once and then parked is completed by the caller
    /// waiting behind it, which then takes the slot. This is the liveness
    /// rule that a notification-based design broke.
    #[dialog_common::test]
    async fn it_lets_a_waiter_drive_a_parked_holder() {
        let scheduler = Scheduled::new(1);
        let a = Gate::default();
        let mut parked = Box::pin(scheduler.join("site", 1, Priority::Demand, || a.work(1)));
        assert!(poll_once(&mut parked).is_pending());
        assert!(a.started());

        // The holder's own joiner is never polled again; its work opens
        // meanwhile, but only a poll can land it.
        a.open();
        let b = Gate::default();
        b.open();
        let mut waiter = Box::pin(scheduler.join("site", 2, Priority::Maybe, || b.work(2)));
        let mut polls = 0;
        let value = loop {
            if let Poll::Ready(value) = poll_once(&mut waiter) {
                break value;
            }
            polls += 1;
            assert!(polls < 4, "the waiter drives the parked holder through");
        };
        assert_eq!(value, 2);
        assert_eq!(scheduler.admitted(&"site"), 0);

        drop(parked);
    }

    /// A waiter dropped before admission leaves the queue: the slot its
    /// turn would have taken goes to the next in line.
    #[dialog_common::test]
    async fn it_frees_a_dropped_waiters_place() {
        let scheduler = Scheduled::new(1);
        let (a, b, c) = (Gate::default(), Gate::default(), Gate::default());
        let mut holder = Box::pin(scheduler.join("site", 1, Priority::Demand, || a.work(1)));
        let mut dropped = Box::pin(scheduler.join("site", 2, Priority::Likely, || b.work(2)));
        let mut next = Box::pin(scheduler.join("site", 3, Priority::Maybe, || c.work(3)));
        assert!(poll_once(&mut holder).is_pending());
        assert!(poll_once(&mut dropped).is_pending());
        assert!(poll_once(&mut next).is_pending());
        assert_eq!(scheduler.waiting(&"site"), 2);

        drop(dropped);
        assert_eq!(scheduler.waiting(&"site"), 1);

        a.open();
        assert_eq!(poll_once(&mut holder), Poll::Ready(1));
        assert!(poll_once(&mut next).is_pending());
        assert!(
            c.started(),
            "the Maybe read is admitted, not the dropped Likely one"
        );
        assert!(!b.started());
    }

    /// Abandoning admitted work (every joiner dropped) frees its slot: a
    /// holder that nobody awaits any more cannot keep the lane full.
    #[dialog_common::test]
    async fn it_frees_the_slot_of_abandoned_work() {
        let scheduler = Scheduled::new(1);
        let (a, b) = (Gate::default(), Gate::default());
        let mut abandoned = Box::pin(scheduler.join("site", 1, Priority::Demand, || a.work(1)));
        assert!(poll_once(&mut abandoned).is_pending());
        assert_eq!(scheduler.admitted(&"site"), 1);

        drop(abandoned);
        assert_eq!(scheduler.admitted(&"site"), 0);

        b.open();
        let mut next = Box::pin(scheduler.join("site", 2, Priority::Maybe, || b.work(2)));
        assert_eq!(poll_once(&mut next), Poll::Ready(2));
    }

    /// A slot granted to a key while nothing polled it is given back if
    /// that key's work is dropped before it is polled again.
    #[dialog_common::test]
    async fn it_gives_back_a_slot_granted_to_dropped_work() {
        let scheduler = Scheduled::new(1);
        let (a, b, c) = (Gate::default(), Gate::default(), Gate::default());
        let mut holder = Box::pin(scheduler.join("site", 1, Priority::Demand, || a.work(1)));
        let mut granted = Box::pin(scheduler.join("site", 2, Priority::Likely, || b.work(2)));
        let mut next = Box::pin(scheduler.join("site", 3, Priority::Maybe, || c.work(3)));
        assert!(poll_once(&mut holder).is_pending());
        assert!(poll_once(&mut granted).is_pending());
        assert!(poll_once(&mut next).is_pending());

        // The holder lands; the slot passes to key 2, which nobody polls.
        a.open();
        assert_eq!(poll_once(&mut holder), Poll::Ready(1));
        assert_eq!(scheduler.admitted(&"site"), 1);
        assert!(!b.started());

        drop(granted);
        assert_eq!(
            scheduler.admitted(&"site"),
            1,
            "the slot passed on to key 3"
        );
        assert!(poll_once(&mut next).is_pending());
        assert!(c.started());
    }

    /// Lanes are per site: a full lane on one site does not hold a read
    /// on another.
    #[dialog_common::test]
    async fn it_keeps_a_window_per_site() {
        let scheduler = Scheduled::new(1);
        let (a, b, c) = (Gate::default(), Gate::default(), Gate::default());
        let mut all = Box::pin(join3(
            scheduler.join("one", 1, Priority::Demand, || a.work(1)),
            scheduler.join("one", 2, Priority::Demand, || b.work(2)),
            scheduler.join("two", 3, Priority::Demand, || c.work(3)),
        ));
        assert!(poll_once(&mut all).is_pending());
        assert!(a.started() && !b.started() && c.started());
    }

    /// Widening a lane lets waiting reads through at once.
    #[dialog_common::test]
    async fn it_admits_waiting_reads_when_the_window_widens() {
        let scheduler = Scheduled::new(1);
        let gates: Vec<Gate> = (0..3).map(|_| Gate::default()).collect();
        let mut all = Box::pin(join_all(gates.iter().enumerate().map(|(at, gate)| {
            scheduler.join("site", at as u8, Priority::Demand, move || {
                gate.work(at as u8)
            })
        })));
        assert!(poll_once(&mut all).is_pending());
        assert_eq!(gates.iter().filter(|gate| gate.started()).count(), 1);

        scheduler.set_window(&"site", 3);
        assert_eq!(scheduler.window(&"site"), 3);
        assert!(poll_once(&mut all).is_pending());
        assert_eq!(gates.iter().filter(|gate| gate.started()).count(), 3);
    }

    /// Concurrent joins of one key run the work once and share its value,
    /// and the lane tallies every join by priority.
    #[dialog_common::test]
    async fn it_shares_one_computation_per_key_and_tallies_joins() {
        let scheduler = Scheduled::new(4);
        let runs = Arc::new(AtomicUsize::new(0));
        let gate = Gate::default();
        let make = || {
            let runs = runs.clone();
            let work = gate.work(7);
            move || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                work.await
            }
        };
        let mut all = Box::pin(join3(
            scheduler.join("site", 1, Priority::Demand, make()),
            scheduler.join("site", 1, Priority::Likely, make()),
            scheduler.join("site", 1, Priority::Maybe, make()),
        ));
        // Every joiner is in before the work can land.
        assert!(poll_once(&mut all).is_pending());
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "one computation for three joiners"
        );

        gate.open();
        let mut polls = 0;
        let values = loop {
            if let Poll::Ready(values) = poll_once(&mut all) {
                break values;
            }
            polls += 1;
            assert!(polls < 4, "the shared work lands for every joiner");
        };
        assert_eq!(values, (7, 7, 7));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(
            scheduler.tally(&"site"),
            Tally {
                demand: 1,
                likely: 1,
                maybe: 1
            }
        );

        let again = scheduler.join("site", 1, Priority::Demand, make()).await;
        assert_eq!(again, 7);
        assert_eq!(runs.load(Ordering::SeqCst), 2, "nothing was cached");
        assert_eq!(scheduler.admitted(&"site"), 0);
    }
}
