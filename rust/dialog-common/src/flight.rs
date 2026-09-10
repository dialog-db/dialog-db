//! Single-flight join for identical concurrent requests.
//!
//! Two callers asking the remote for the same immutable thing at the
//! same moment each paid a full round trip: nothing here waited on a
//! fetch another caller had in flight, because nothing could DRIVE that
//! fetch — a future advances only while its owner polls it, and an
//! owner parked between two yields left every waiter parked with it.
//! That rule was load-bearing (an earlier claim-based dedup deadlocked
//! a proof walk against its own in-flight fetch) but its price was real:
//! a cold boot's concurrent tree walks fetched the same root block up
//! to sixteen times.
//!
//! [`Flight`] removes the price without touching the rule, by removing
//! the OWNER. The in-flight work is a [`Shared`] future: every caller
//! that joins it polls the same underlying future, so whoever is
//! actually awaiting makes progress for everyone, and a joiner never
//! depends on the liveness of whoever arrived first. The re-entrant
//! shape that sank the claim design — a walk needing a block whose
//! fetch it is itself waiting on — now drives that fetch forward
//! instead of parking behind it.
//!
//! The map holds only in-flight work. Whoever observes completion
//! removes the entry (scoped by pointer identity, so a fresh flight
//! started under the same key is left alone), which means outcomes are
//! never cached here — a failure is shared with everyone already
//! joined, and the next caller starts clean. Callers therefore keep
//! their existing retry semantics.
//!
//! Only requests whose answer is identical for every caller belong in a
//! flight: content-addressed block reads and permit redeems for one
//! object. Mutable reads (a branch head cell) must not join one.

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;

use crate::ConditionalSend;
use futures_util::FutureExt;
use futures_util::future::Shared;

#[cfg(not(target_arch = "wasm32"))]
use futures_util::future::BoxFuture;
#[cfg(target_arch = "wasm32")]
use futures_util::future::LocalBoxFuture;

#[cfg(not(target_arch = "wasm32"))]
type Stored<V> = Shared<BoxFuture<'static, V>>;
#[cfg(target_arch = "wasm32")]
type Stored<V> = Shared<LocalBoxFuture<'static, V>>;

#[cfg(not(target_arch = "wasm32"))]
type Map<K, V> = parking_lot::Mutex<HashMap<K, Stored<V>>>;
// A worker context is single-threaded; RefCell is enough, and the
// borrow never crosses an await (see `join`).
#[cfg(target_arch = "wasm32")]
type Map<K, V> = std::cell::RefCell<HashMap<K, Stored<V>>>;

/// A map of in-flight computations, joined by key: the process-wide
/// form the transport registries and the operator's hydration use.
///
/// See the module docs for the semantics; [`join`](Flight::join) is
/// the whole API.
pub struct Flight<K, V> {
    inflight: Map<K, V>,
}

impl<K, V> Default for Flight<K, V> {
    fn default() -> Self {
        Self {
            inflight: Map::default(),
        }
    }
}

impl<K, V> std::fmt::Debug for Flight<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Flight")
            .field("inflight", &self.lock().len())
            .finish()
    }
}

impl<K, V> Flight<K, V> {
    #[cfg(not(target_arch = "wasm32"))]
    fn lock(&self) -> parking_lot::MutexGuard<'_, HashMap<K, Stored<V>>> {
        self.inflight.lock()
    }

    #[cfg(target_arch = "wasm32")]
    fn lock(&self) -> std::cell::RefMut<'_, HashMap<K, Stored<V>>> {
        self.inflight.borrow_mut()
    }
}

impl<K, V> Flight<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Join the in-flight computation for `key`, or start `make()` as
    /// the shared one.
    ///
    /// Every joiner polls the shared future itself, so completion never
    /// depends on the caller that started it. The entry is removed once
    /// any joiner observes completion; outcomes — failures included —
    /// are shared only with callers already in flight, never cached.
    pub async fn join<F, Make>(&self, key: K, make: Make) -> V
    where
        Make: FnOnce() -> F,
        F: Future<Output = V> + ConditionalSend + 'static,
    {
        let shared = {
            let mut inflight = self.lock();
            match inflight.get(&key) {
                Some(shared) => shared.clone(),
                None => {
                    #[cfg(not(target_arch = "wasm32"))]
                    let shared = make().boxed().shared();
                    #[cfg(target_arch = "wasm32")]
                    let shared = make().boxed_local().shared();
                    inflight.insert(key.clone(), shared.clone());
                    shared
                }
            }
            // The guard drops here: the map is never held across an await.
        };

        let value = shared.clone().await;

        let mut inflight = self.lock();
        if inflight
            .get(&key)
            .is_some_and(|current| current.ptr_eq(&shared))
        {
            inflight.remove(&key);
        }
        drop(inflight);

        value
    }
}

#[cfg(not(target_arch = "wasm32"))]
type WeakStored<V> = futures_util::future::WeakShared<BoxFuture<'static, V>>;
#[cfg(target_arch = "wasm32")]
type WeakStored<V> = futures_util::future::WeakShared<LocalBoxFuture<'static, V>>;

#[cfg(not(target_arch = "wasm32"))]
type WeakMap<K, V> = parking_lot::Mutex<HashMap<K, WeakStored<V>>>;
#[cfg(target_arch = "wasm32")]
type WeakMap<K, V> = std::cell::RefCell<HashMap<K, WeakStored<V>>>;

/// A [`Flight`] whose map holds its in-flight work weakly.
///
/// The strong [`Shared`] handles live only in the joiners, so the work
/// makes progress exactly while some caller is awaiting it and drops
/// with its last joiner — nothing long-lived owns an in-flight future
/// or anything that future captured. That is what lets a process-wide
/// holder (an operator field) share work whose futures capture the
/// holder's own internals: abandoning every joiner breaks any would-be
/// reference cycle by dropping the future itself.
///
/// Everything else matches [`Flight`]: joiners co-drive one shared
/// future per key, outcomes are shared only with callers in flight and
/// never cached, and only requests whose answer is identical for every
/// caller belong here.
pub struct WeakFlight<K, V> {
    inflight: WeakMap<K, V>,
}

impl<K, V> Default for WeakFlight<K, V> {
    fn default() -> Self {
        Self {
            inflight: WeakMap::default(),
        }
    }
}

impl<K, V> std::fmt::Debug for WeakFlight<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WeakFlight")
            .field("inflight", &self.lock().len())
            .finish()
    }
}

impl<K, V> WeakFlight<K, V> {
    #[cfg(not(target_arch = "wasm32"))]
    fn lock(&self) -> parking_lot::MutexGuard<'_, HashMap<K, WeakStored<V>>> {
        self.inflight.lock()
    }

    #[cfg(target_arch = "wasm32")]
    fn lock(&self) -> std::cell::RefMut<'_, HashMap<K, WeakStored<V>>> {
        self.inflight.borrow_mut()
    }
}

impl<K, V> WeakFlight<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Join the in-flight computation for `key`, or start `make()` as
    /// the shared one.
    ///
    /// Every joiner polls the shared future itself, so completion never
    /// depends on the caller that started it. The map holds the work
    /// weakly: if every joiner drops before completion the work drops
    /// with them, and the next caller under the key starts clean. The
    /// entry is removed once any joiner observes completion; outcomes —
    /// failures included — are shared only with callers already in
    /// flight, never cached.
    pub async fn join<F, Make>(&self, key: K, make: Make) -> V
    where
        Make: FnOnce() -> F,
        F: Future<Output = V> + ConditionalSend + 'static,
    {
        let shared = {
            let mut inflight = self.lock();
            match inflight.get(&key).and_then(|weak| weak.upgrade()) {
                Some(shared) => shared,
                None => {
                    #[cfg(not(target_arch = "wasm32"))]
                    let shared = make().boxed().shared();
                    #[cfg(target_arch = "wasm32")]
                    let shared = make().boxed_local().shared();
                    if let Some(weak) = shared.downgrade() {
                        inflight.insert(key.clone(), weak);
                    }
                    shared
                }
            }
            // The guard drops here: the map is never held across an await.
        };

        let value = shared.clone().await;

        // Remove the entry we completed (or one already dead); a fresh
        // flight started under the same key is left alone.
        let mut inflight = self.lock();
        if inflight.get(&key).is_some_and(|current| {
            current
                .upgrade()
                .is_none_or(|current| current.ptr_eq(&shared))
        }) {
            inflight.remove(&key);
        }
        drop(inflight);

        value
    }
}

// Gated on `helpers` because `dialog_common::test` (the cross-target
// async test macro) is only exported there; CI's test archives build the
// workspace with `--features integration-tests`, which implies it.
#[cfg(all(test, feature = "helpers"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{Flight, WeakFlight};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test_configure;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    async fn yield_once() {
        let mut yielded = false;
        std::future::poll_fn(move |context| {
            if yielded {
                std::task::Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        })
        .await
    }

    /// Concurrent joins of one key run the computation once and all see
    /// its value.
    #[dialog_common::test]
    async fn it_runs_one_computation_for_concurrent_joins() {
        let flight = Flight::<u8, u8>::default();
        let runs = Arc::new(AtomicUsize::new(0));

        let make = || {
            let runs = runs.clone();
            || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                7u8
            }
        };

        let (first, second) =
            futures_util::future::join(flight.join(1, make()), flight.join(1, make())).await;

        assert_eq!((first, second), (7, 7));
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    /// The joiner drives the shared computation itself: an initiator
    /// polled once and then parked forever must not park the joiner
    /// with it. This is the invariant whose violation sank the old
    /// claim-based dedup.
    // Native only: the bound is tokio's timer, which has no wasm runtime.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_lets_a_joiner_drive_a_flight_the_initiator_parked() {
        use std::task::{Context, Poll};

        let flight = Arc::new(Flight::<u8, u8>::default());
        let runs = Arc::new(AtomicUsize::new(0));

        let make = || {
            let runs = runs.clone();
            || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                9u8
            }
        };

        // The initiator starts the flight and is never polled again.
        let mut parked = Box::pin(flight.join(1, make()));
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(parked.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(runs.load(Ordering::SeqCst), 1, "the flight is in flight");

        let joined = flight.join(1, make());
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), joined)
            .await
            .expect("a joiner must be able to drive a parked flight");
        assert_eq!(value, 9);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "the joiner joined, not re-ran"
        );

        drop(parked);
    }

    /// A failure is shared with the callers already joined and cached
    /// for nobody: the next join starts a fresh computation.
    #[dialog_common::test]
    async fn it_shares_a_failure_without_caching_it() {
        let flight = Flight::<u8, Result<u8, &'static str>>::default();
        let runs = Arc::new(AtomicUsize::new(0));

        let failing = {
            let runs = runs.clone();
            || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                Err("no")
            }
        };
        let (first, second) = futures_util::future::join(
            flight.join(1, failing),
            flight.join(1, {
                let runs = runs.clone();
                || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    yield_once().await;
                    Err("no")
                }
            }),
        )
        .await;
        assert_eq!(first, Err("no"));
        assert_eq!(second, Err("no"));
        assert_eq!(runs.load(Ordering::SeqCst), 1, "the failure was shared");

        let retried = flight
            .join(1, {
                let runs = runs.clone();
                || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    Ok(3)
                }
            })
            .await;
        assert_eq!(retried, Ok(3));
        assert_eq!(runs.load(Ordering::SeqCst), 2, "nothing was cached");
    }

    /// Sequential joins each run their own computation: the map holds
    /// in-flight work only.
    #[dialog_common::test]
    async fn it_removes_the_entry_once_the_flight_lands() {
        let flight = Flight::<u8, u8>::default();
        let runs = Arc::new(AtomicUsize::new(0));

        for expected in 1..=2 {
            let value = flight
                .join(1, {
                    let runs = runs.clone();
                    || async move {
                        runs.fetch_add(1, Ordering::SeqCst);
                        5u8
                    }
                })
                .await;
            assert_eq!(value, 5);
            assert_eq!(runs.load(Ordering::SeqCst), expected);
        }
    }

    /// Concurrent joins of one key on a weak flight run the computation
    /// once, and the entry is gone once it lands.
    #[dialog_common::test]
    async fn it_shares_a_weak_flight_between_concurrent_joiners() {
        let flight = WeakFlight::<u8, u8>::default();
        let runs = Arc::new(AtomicUsize::new(0));

        let make = || {
            let runs = runs.clone();
            || async move {
                runs.fetch_add(1, Ordering::SeqCst);
                yield_once().await;
                7u8
            }
        };

        let (first, second) =
            futures_util::future::join(flight.join(1, make()), flight.join(1, make())).await;
        assert_eq!((first, second), (7, 7));
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        let again = flight.join(1, make()).await;
        assert_eq!(again, 7);
        assert_eq!(runs.load(Ordering::SeqCst), 2, "nothing was cached");
    }

    /// Abandoning every joiner drops the in-flight work and everything
    /// it captured: the map's hold is weak, so a long-lived holder never
    /// keeps a future (or a captured handle) alive on its own. The next
    /// caller under the key starts clean.
    // Native only: the drop probe hand-polls with a noop waker.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_drops_abandoned_work_with_its_last_joiner() {
        use std::task::{Context, Poll};

        let flight = WeakFlight::<u8, u8>::default();
        let runs = Arc::new(AtomicUsize::new(0));
        let captured = Arc::new(());

        let make = {
            let runs = runs.clone();
            let captured = captured.clone();
            || async move {
                let _held = captured;
                runs.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<()>().await;
                unreachable!("the flight is abandoned before completion")
            }
        };

        let mut parked = Box::pin(flight.join(1, make));
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(parked.as_mut().poll(&mut context), Poll::Pending));
        assert_eq!(runs.load(Ordering::SeqCst), 1, "the flight is in flight");
        assert_eq!(
            Arc::strong_count(&captured),
            2,
            "the work holds its capture"
        );

        drop(parked);
        assert_eq!(
            Arc::strong_count(&captured),
            1,
            "the last joiner takes the work and its captures down with it"
        );

        let fresh = flight
            .join(1, {
                let runs = runs.clone();
                || async move {
                    runs.fetch_add(1, Ordering::SeqCst);
                    3u8
                }
            })
            .await;
        assert_eq!(fresh, 3);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "the next caller starts clean"
        );
    }
}
