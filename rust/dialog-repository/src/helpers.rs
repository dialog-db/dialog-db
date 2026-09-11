use std::any::type_name;
use std::collections::BTreeMap;
use std::sync::Arc;

use dialog_capability::{Command, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
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
/// It also measures how many block reads are in flight AT ONCE. That is
/// the quantity that decides wall time over a remote archive: n reads
/// awaited one after another cost n round trips, while n overlapped reads
/// cost one. A tally cannot tell those apart — both count n — so
/// [`peak_block_reads_in_flight`](Self::peak_block_reads_in_flight)
/// reports the overlap directly. Every read yields once before it is
/// answered, so reads issued by concurrently polled work are genuinely in
/// flight together.
///
/// Clones share the tally.
#[derive(Debug, Clone)]
pub struct Counting<P> {
    inner: P,
    counts: Arc<Mutex<BTreeMap<&'static str, u64>>>,
    reads: Arc<Mutex<InFlight>>,
}

/// How many reads are open now, and the most that were ever open at once.
#[derive(Debug, Default)]
struct InFlight {
    current: usize,
    peak: usize,
}

/// Yields to the executor exactly once, so work polled alongside the
/// caller gets a chance to run before the caller resumes.
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
        }
    }

    /// The greatest number of block reads ever in flight at once since
    /// the last [`reset`](Self::reset).
    ///
    /// 1 means strictly serial: each read was awaited before the next was
    /// issued, so over a remote archive each cost its own round trip.
    pub fn peak_block_reads_in_flight(&self) -> usize {
        self.reads.lock().peak
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

        // Only block reads are timed: they are the effects that cost a
        // network round trip apiece over a remote archive.
        if !name.contains("archive::Get") {
            return self.inner.execute(input).await;
        }

        {
            let mut reads = self.reads.lock();
            reads.current += 1;
            reads.peak = reads.peak.max(reads.current);
        }
        // Yield before answering, so a read issued by work polled
        // alongside this one is in flight together with it. Without this
        // a same-tick read could complete before its sibling is polled,
        // and genuinely concurrent work would measure as serial.
        yield_once().await;
        let output = self.inner.execute(input).await;
        self.reads.lock().current -= 1;

        output
    }
}
