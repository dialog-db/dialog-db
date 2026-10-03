//! A memory provider that loses chosen publishes, for tests of how
//! a read-modify-write recovers from a cell moving under it.

use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory::prelude::PublishExt as _;
use dialog_effects::memory::{Edition, List, MemoryError, Publish, Resolve, Retract, Version};
use dialog_effects::storage::Location;
use parking_lot::Mutex;

use crate::provider::Volatile;
use crate::resource::Resource;

/// Which publishes of one cell are lost, counted from when the plan was
/// set.
#[derive(Debug)]
struct Plan {
    seen: usize,
    lost: (Bound<usize>, Bound<usize>),
}

/// A memory provider that answers chosen publishes of chosen cells with
/// [`MemoryError::VersionMismatch`] without writing, as a cell another
/// writer moved would, and delegates everything else to `M`.
///
/// The cell is left as it was, so a caller that re-reads it after the
/// mismatch sees what it saw before: exactly what a writer that lost a
/// race to one that wrote the same bytes would see. Plans are keyed by
/// `(space, cell)` within this provider, which a
/// [`Storage`](crate::provider::storage::Storage) mounts one of per
/// subject, so a plan touches one subject's cell.
#[derive(Debug, Clone)]
pub struct Flaky<M = Volatile> {
    inner: M,
    plans: Arc<Mutex<HashMap<(String, String), Plan>>>,
}

impl<M> Flaky<M> {
    /// Wrap `inner`, losing nothing until a plan is set.
    pub fn new(inner: M) -> Self {
        Self {
            inner,
            plans: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Lose the publishes of `cell` in `space` whose ordinals, counted
    /// from now, fall in `attempts` -- `2..3` for the third one only,
    /// `0..` for every one from here on; every other publish goes
    /// through. Replaces any earlier plan for the cell.
    pub fn lose_publishes(&self, space: &str, cell: &str, attempts: impl RangeBounds<usize>) {
        self.plans.lock().insert(
            (space.to_string(), cell.to_string()),
            Plan {
                seen: 0,
                lost: (
                    attempts.start_bound().cloned(),
                    attempts.end_bound().cloned(),
                ),
            },
        );
    }

    /// Lose the next `count` publishes of `cell` in `space`.
    pub fn lose_next_publishes(&self, space: &str, cell: &str, count: usize) {
        self.lose_publishes(space, cell, 0..count);
    }

    /// How many publishes of `cell` in `space` were attempted since its
    /// plan was set, lost ones included.
    pub fn publishes(&self, space: &str, cell: &str) -> usize {
        self.plans
            .lock()
            .get(&(space.to_string(), cell.to_string()))
            .map_or(0, |plan| plan.seen)
    }

    /// Whether this publish is one the plan loses, counting it either way.
    fn loses(&self, space: &str, cell: &str) -> bool {
        let mut plans = self.plans.lock();
        let Some(plan) = plans.get_mut(&(space.to_string(), cell.to_string())) else {
            return false;
        };
        let ordinal = plan.seen;
        plan.seen += 1;
        plan.lost.contains(&ordinal)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<M> Provider<Publish> for Flaky<M>
where
    M: Provider<Publish> + ConditionalSync,
{
    async fn execute(&self, effect: Capability<Publish>) -> Result<Version, MemoryError> {
        if self.loses(effect.space(), effect.cell()) {
            return Err(MemoryError::VersionMismatch {
                expected: effect.when().cloned(),
                actual: None,
            });
        }
        effect.perform(&self.inner).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<M> Provider<Resolve> for Flaky<M>
where
    M: Provider<Resolve> + ConditionalSync,
{
    async fn execute(
        &self,
        effect: Capability<Resolve>,
    ) -> Result<Option<Edition<Vec<u8>>>, MemoryError> {
        effect.perform(&self.inner).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<M> Provider<Retract> for Flaky<M>
where
    M: Provider<Retract> + ConditionalSync,
{
    async fn execute(&self, effect: Capability<Retract>) -> Result<(), MemoryError> {
        effect.perform(&self.inner).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<M> Provider<List> for Flaky<M>
where
    M: Provider<List> + ConditionalSync,
{
    async fn execute(&self, effect: Capability<List>) -> Result<Vec<String>, MemoryError> {
        effect.perform(&self.inner).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<M> Resource<Location> for Flaky<M>
where
    M: Resource<Location> + ConditionalSync,
{
    type Error = M::Error;

    fn is_not_found(error: &Self::Error) -> bool {
        M::is_not_found(error)
    }

    async fn open(location: &Location) -> Result<Self, Self::Error> {
        Ok(Self::new(M::open(location).await?))
    }

    async fn load(location: &Location) -> Result<Self, Self::Error> {
        Ok(Self::new(M::load(location).await?))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::Flaky;
    use crate::helpers::unique_subject;
    use crate::provider::Volatile;
    use dialog_effects::memory::MemoryError;
    use dialog_effects::prelude::*;

    /// The planned publishes are lost without touching the cell, and
    /// the ones after them go through.
    #[dialog_common::test]
    async fn it_loses_the_planned_publishes_and_no_others() -> anyhow::Result<()> {
        let provider = Flaky::new(Volatile::new());
        let subject = unique_subject("flaky");
        let publish = |content: &'static [u8]| {
            subject
                .clone()
                .writer()
                .memory()
                .space("local")
                .cell("test")
                .publish(content.to_vec(), None)
        };

        provider.lose_next_publishes("local", "test", 2);
        for _ in 0..2 {
            let lost = publish(b"first").perform(&provider).await;
            assert!(matches!(lost, Err(MemoryError::VersionMismatch { .. })));
        }
        let resolved = subject
            .clone()
            .reader()
            .memory()
            .space("local")
            .cell("test")
            .resolve()
            .perform(&provider)
            .await?;
        assert!(resolved.is_none(), "a lost publish wrote nothing");

        publish(b"first").perform(&provider).await?;
        assert_eq!(provider.publishes("local", "test"), 3);
        Ok(())
    }
}
