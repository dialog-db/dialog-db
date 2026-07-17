use std::any::type_name;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::{Repository, RepositoryExt as _};
use dialog_capability::{Command, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_operator::{Operator, Profile};
use dialog_storage::provider::storage::VolatileSpace;
use parking_lot::Mutex;

// Re-export operator-level helpers.
pub use dialog_operator::helpers::{
    generate_data, test_operator, test_operator_with_profile, unique_name,
};

/// A [`Provider`] wrapper that tallies every effect execution by its
/// type name, so a test can measure an operation's cost in effect
/// dispatches rather than wall time. Archive `Get` carries one digest
/// per call, so its tally is exactly the number of block reads.
///
/// Clones share the tally.
#[derive(Debug, Clone)]
pub struct Counting<P> {
    inner: P,
    counts: Arc<Mutex<BTreeMap<&'static str, u64>>>,
}

impl<P> Counting<P> {
    /// Wrap `inner`, starting with an empty tally.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            counts: Arc::new(Mutex::new(BTreeMap::new())),
        }
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

    /// Clear the tally.
    pub fn reset(&self) {
        self.counts.lock().clear();
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
        *self.counts.lock().entry(type_name::<C>()).or_insert(0) += 1;
        self.inner.execute(input).await
    }
}

/// Create a test repository using the given operator and profile.
pub async fn test_repo(
    operator: &Operator<VolatileSpace>,
    profile: &Profile,
) -> Repository<Credential> {
    profile
        .repository(unique_name("repo"))
        .open()
        .perform(operator)
        .await
        .expect("test_repo: failed to open repository")
}
