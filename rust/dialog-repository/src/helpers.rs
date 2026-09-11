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

/// Fill `branch` with what a tonk profile's account branch carries, at a
/// scale that makes a cold clone do real work.
///
/// Modelled on what a real profile carries (see `tonk-account` and
/// `onboarding.rs`), which is SMALL: a few delegations -- passkey
/// recovery, account recovery, device grants -- each decomposing into
/// facts plus a signed envelope blob, alongside a handful of device-link
/// rows. A profile is not a data store; its branch holds definitions and
/// authority, not bulk.
///
/// `scale` of 1 targets the shape the app's own `/diagnose` view reports
/// for a real profile: ONE index node with ~68 children (fanout 256,
/// max-segment 65536), i.e. a root plus a wide child level. That shape is
/// the whole point -- the download walks the root, then a level of ~68
/// siblings whose reads are independent. Fetched together they cost one
/// round trip; fetched one at a time they cost 68, which over a 2s link
/// is the difference between a second and a minute.
///
/// Returns the number of delegations retained, so a caller can assert
/// the envelope blobs actually shipped.
#[cfg(test)]
pub async fn fill_account_branch<Env>(
    branch: &crate::Branch,
    scale: usize,
    env: &Env,
) -> anyhow::Result<usize>
where
    Env: dialog_capability::Provider<dialog_effects::archive::Get>
        + dialog_capability::Provider<dialog_effects::archive::Put>
        + dialog_capability::Provider<dialog_effects::memory::Resolve>
        + dialog_capability::Provider<dialog_effects::memory::Publish>
        + dialog_capability::Provider<dialog_effects::authority::Identify>
        + dialog_capability::Provider<dialog_effects::authority::Attest>
        + dialog_capability::Provider<dialog_effects::archive::Import>
        + dialog_capability::Provider<dialog_effects::blob::Write>
        + dialog_capability::Provider<crate::Hydrate>
        + dialog_capability::Provider<
            dialog_capability::Fork<dialog_network::Network, dialog_effects::memory::Resolve>,
        >
        + ConditionalSync
        + 'static,
{
    use dialog_artifacts::{Artifact, Instruction, Value};
    use dialog_credentials::Ed25519Signer;
    use dialog_varsig::Principal as _;
    use futures_util::stream;

    let space = Ed25519Signer::generate().await?;
    let delegations = 3 * scale;
    // Sized so the tree reaches a wide child level rather than a single
    // leaf: the siblings are what must overlap.
    for _ in 0..delegations {
        let holder = Ed25519Signer::generate().await?;
        let delegation = dialog_ucan_core::DelegationBuilder::new()
            .issuer(dialog_credentials::Signer::from(space.clone()))
            .audience(&holder.did())
            .subject(dialog_ucan_core::subject::Subject::Specific(space.did()))
            .command(vec!["storage".to_string()])
            .try_build()
            .await?;
        branch
            .delegations()
            .retain(dialog_ucan::UcanDelegation::new(
                dialog_ucan_core::DelegationChain::new(delegation),
            ))
            .perform(env)
            .await?;
    }

    for round in 0..(6 * scale) {
        let rows: Vec<_> = (0..150)
            .map(|i| {
                Instruction::Assert(Artifact {
                    the: "device/link".parse().expect("valid attribute"),
                    of: format!("device:{round}-{i}").parse().expect("valid entity"),
                    is: Value::String(format!("device-{round}-{i}").repeat(24)),
                    cause: None,
                })
            })
            .collect();
        branch.commit(stream::iter(rows)).perform(env).await?;
    }

    Ok(delegations)
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
    writes: Arc<Mutex<InFlight>>,
    forks: Arc<Mutex<InFlight>>,
}

/// How many reads are open now, and the most that were ever open at once.
#[derive(Debug, Default)]
struct InFlight {
    current: usize,
    peak: usize,
    /// Consecutive opens that were alone in flight, and the longest such
    /// run seen. See [`Counting::longest_serial_fetch_run`].
    alone: usize,
    longest_alone: usize,
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
            writes: Arc::new(Mutex::new(InFlight::default())),
            forks: Arc::new(Mutex::new(InFlight::default())),
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

    /// The most block WRITES ever in flight at once: the same measure as
    /// [`peak_block_reads_in_flight`](Self::peak_block_reads_in_flight),
    /// for the upload direction. The push is the control the download is
    /// compared against, so its overlap has to be observable too.
    pub fn peak_block_writes_in_flight(&self) -> usize {
        self.writes.lock().peak
    }

    /// The most REMOTE fetches ever in flight at once: `Hydrate` (a block
    /// read that missed locally and went to the remote) and forked
    /// effects (a push's uploads).
    ///
    /// A local read is cheap and its overlap does not matter; what decides
    /// wall time is how many round trips are open together, so this is the
    /// quantity a HAR reports and the one an assertion should use.
    pub fn peak_forks_in_flight(&self) -> usize {
        self.forks.lock().peak
    }

    /// The longest run of remote fetches that were each ALONE in flight.
    ///
    /// A peak hides this. One phase that fans out lifts the peak above 1
    /// while another is still strictly serial, so a run whose download
    /// makes thirty one-at-a-time round trips and whose upload then
    /// overlaps eleven reports a healthy peak and a serial download at the
    /// same time -- which is exactly the shape a HAR of the app shows.
    ///
    /// This counts the serialization directly: how many fetches in a row
    /// had no company.
    pub fn longest_serial_fetch_run(&self) -> usize {
        self.forks.lock().longest_alone
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
        *self.writes.lock() = InFlight::default();
        *self.forks.lock() = InFlight::default();
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

        // What costs a round trip, and what does not.
        //
        // A block read that misses locally becomes a `Hydrate` -- that
        // effect IS the remote fetch (it resolves the route, fetches, and
        // writes back), so it is the download's round trip. A push's
        // uploads cross as `Fork<RemoteSite, Put>`. A bare `archive::Get`
        // is the local store: those overlap for free and say nothing
        // about wall time, which is why an earlier peak taken over all
        // block reads read healthy while every fetch was serial.
        //
        // Both remote forms feed one gauge, so the two directions report
        // the same quantity the HAR does: fetches open at once.
        let gauge = if name.contains("hydrate::Hydrate") || name.contains("fork::Fork") {
            &self.forks
        } else if name.contains("archive::Get") {
            &self.reads
        } else if name.contains("archive::Put") || name.contains("archive::Import") {
            &self.writes
        } else {
            return self.inner.execute(input).await;
        };

        {
            let mut open = gauge.lock();
            open.current += 1;
            open.peak = open.peak.max(open.current);
            if open.current == 1 {
                open.alone += 1;
                open.longest_alone = open.longest_alone.max(open.alone);
            } else {
                open.alone = 0;
            }
        }
        // Yield before answering, so a read issued by work polled
        // alongside this one is in flight together with it. Without this
        // a same-tick read could complete before its sibling is polled,
        // and genuinely concurrent work would measure as serial.
        yield_once().await;
        let output = self.inner.execute(input).await;
        gauge.lock().current -= 1;

        output
    }
}
