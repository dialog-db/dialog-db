use std::any::{Any, type_name};
use std::collections::BTreeMap;
use std::sync::Arc;

use dialog_capability::{Command, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_network::HydrationRequest;
use parking_lot::Mutex;

// Operator-dependent helpers (test_operator, unique_name, ...) live in
// `dialog_operator::helpers`: the operator sits above this crate, so tests
// import them from there via the dev-dependency. `test_repo` is the one
// exception: it returns THIS crate's types, and through the dev-dependency
// cycle the operator's copy of this crate is a distinct compilation — its
// `Repository` is not `crate::Repository` — so this crate's tests need a
// local one built from `crate::` paths.

/// Create a test repository (this crate's types) using the given operator
/// as the effect environment.
#[cfg(test)]
pub async fn test_repo(
    operator: &dialog_operator::Operator<VolatileSpaceForTests>,
    profile: &dialog_identity::Profile,
) -> crate::Repository<dialog_credentials::Credential> {
    use crate::RepositoryExt as _;
    use dialog_identity::SpaceHandle;
    use dialog_operator::helpers::unique_name;
    let handle = SpaceHandle {
        profile_did: dialog_varsig::Principal::did(profile),
        name: unique_name("repo"),
    };
    handle
        .open()
        .perform(operator)
        .await
        .expect("test_repo: failed to open repository")
}

/// The volatile space type test operators run over.
#[cfg(test)]
use dialog_storage::provider::storage::VolatileSpace as VolatileSpaceForTests;

/// A [`Provider`] wrapper that tallies every effect execution by its
/// type name, so a test can measure an operation's cost in effect
/// dispatches rather than wall time. Archive `Get` carries one digest
/// per call, so its tally is exactly the number of block reads.
///
/// It also measures how many block reads are in flight AT ONCE, which is
/// what decides whether a phase costs one round trip per block or one for
/// the whole batch. Every read yields to the executor before it is
/// answered, so reads issued by concurrently polled work genuinely overlap
/// and show up in [`peak_block_reads_in_flight`](Self::peak_block_reads_in_flight).
/// A phase that awaits each read before issuing the next pins that peak at
/// 1 no matter how many reads it does.
///
/// The same overlap is tracked PER PHASE for hydrations, keyed by the
/// label the reading store was tagged with
/// ([`peak_hydrations_in_flight`](Self::peak_hydrations_in_flight)). A
/// pull reads through several phases at once, so one global peak can be
/// high while an individual phase is strictly serial — which is exactly
/// the failure being chased. Hydration is the effect that costs a network
/// round trip, so its per-label peak is the number that matters.
///
/// Clones share the tally.
#[derive(Debug, Clone)]
pub struct Counting<P> {
    inner: P,
    counts: Arc<Mutex<BTreeMap<&'static str, u64>>>,
    reads: Arc<Mutex<InFlight>>,
    hydrations: Arc<Mutex<BTreeMap<&'static str, InFlight>>>,
}

/// Concurrency of a set of reads: how many are open now, and the most
/// that were ever open at once.
#[derive(Debug, Default, Clone)]
struct InFlight {
    current: usize,
    peak: usize,
}

impl InFlight {
    fn enter(&mut self) {
        self.current += 1;
        self.peak = self.peak.max(self.current);
    }

    fn leave(&mut self) {
        self.current -= 1;
    }
}

/// Yields to the executor exactly once, giving work polled alongside the
/// caller a chance to run before the caller resumes.
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

impl<P> Counting<P> {
    /// Wrap `inner`, starting with an empty tally.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            counts: Arc::new(Mutex::new(BTreeMap::new())),
            reads: Arc::new(Mutex::new(InFlight::default())),
            hydrations: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// The greatest number of block reads ever in flight at once since the
    /// last [`reset`](Self::reset).
    ///
    /// 1 means the reads were strictly serial: each was awaited before the
    /// next was issued, so over a remote archive each costs its own round
    /// trip. Greater than 1 means that many round trips overlapped.
    pub fn peak_block_reads_in_flight(&self) -> usize {
        self.reads.lock().peak
    }

    /// The greatest number of hydrations ever in flight at once for the
    /// phase tagged `label`, since the last [`reset`](Self::reset).
    ///
    /// `None` when that phase hydrated nothing — which is itself worth
    /// asserting on, since a phase that never reaches the remote cannot be
    /// the one costing round trips.
    pub fn peak_hydrations_in_flight(&self, label: &str) -> Option<usize> {
        self.hydrations
            .lock()
            .get(label)
            .map(|in_flight| in_flight.peak)
    }

    /// Every phase that hydrated, with its peak overlap, keyed by label.
    pub fn hydration_peaks(&self) -> BTreeMap<&'static str, usize> {
        self.hydrations
            .lock()
            .iter()
            .map(|(label, in_flight)| (*label, in_flight.peak))
            .collect()
    }

    /// Total executions of effects whose type name contains `needle`
    /// (e.g. `"archive::Get"`).
    pub fn count(&self, needle: &str) -> u64 {
        self.counts
            .lock()
            .iter()
            .filter(|(name, _)| name.contains(needle))
            .map(|(_, tally)| *tally)
            .sum()
    }

    /// Block reads performed so far: archive `Get` executions.
    pub fn block_reads(&self) -> u64 {
        self.count("archive::Get")
    }

    /// Clear the tally and the observed concurrency.
    pub fn reset(&self) {
        self.counts.lock().clear();
        *self.reads.lock() = InFlight::default();
        self.hydrations.lock().clear();
    }

    /// The full tally, keyed by effect type name.
    pub fn snapshot(&self) -> BTreeMap<&'static str, u64> {
        self.counts.lock().clone()
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<C, P> Provider<C> for Counting<P>
where
    C: Command + 'static,
    C::Input: ConditionalSend,
    P: Provider<C> + ConditionalSync,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        let name = type_name::<C>();
        *self.counts.lock().entry(name).or_insert(0) += 1;

        // Only the reads that cost a network round trip are timed for
        // overlap: archive `Get` (one digest per call) and `Hydrate` (one
        // block fetched from the remote). A hydration also carries the
        // label of the phase that asked for it, so its overlap is tracked
        // per phase as well as globally.
        let hydration = (&input as &dyn Any)
            .downcast_ref::<HydrationRequest>()
            .and_then(|request| request.label);
        let timed = hydration.is_some() || name.contains("archive::Get");

        if !timed {
            return self.inner.execute(input).await;
        }

        self.reads.lock().enter();
        if let Some(label) = hydration {
            self.hydrations.lock().entry(label).or_default().enter();
        }

        // Yield before answering, so reads issued by work polled alongside
        // this one are in flight together and their overlap is observable.
        // Without this a same-tick read could complete before its sibling
        // is ever polled, and a concurrent phase would read as serial.
        yield_once().await;
        let output = self.inner.execute(input).await;

        self.reads.lock().leave();
        if let Some(label) = hydration {
            if let Some(in_flight) = self.hydrations.lock().get_mut(label) {
                in_flight.leave();
            }
        }

        output
    }
}
