//! Retract command for emptying a cell.

use super::cell::Cache;
use crate::RetractError;
use dialog_capability::Provider;
use dialog_effects::memory::prelude::CellScope;
use dialog_effects::memory::{self, Version, prelude::CellExt};

/// Command to empty a cell.
///
/// Created by [`Cell::retract`](super::Cell::retract). Invoke
/// `.perform(&env)` to run against the local environment.
///
/// Retraction is compare-and-swap like a publish: it names the version
/// it expects to remove, so a concurrent write is refused rather than
/// silently discarded. The expected version comes from the cache, which
/// means a caller that has not resolved the cell has nothing to retract
/// and is told so rather than deleting whatever happens to be there.
pub struct Retract<T, Codec: Clone> {
    /// Capability chain targeting the cell to empty.
    pub capability: CellScope,
    /// Cached edition, supplying the version to CAS against.
    pub cache: Cache<T, Codec>,
}

impl<T, Codec> Retract<T, Codec>
where
    T: Clone,
    Codec: Clone,
{
    /// Perform the retraction against the local environment.
    ///
    /// Succeeds when the cell is already empty: the caller asked for it
    /// to hold nothing, and it holds nothing.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RetractError>
    where
        Env: Provider<memory::Retract>,
    {
        let Some(when) = self.cache.version() else {
            return Err(RetractError::Unobserved);
        };
        self.capability.retract(when).perform(env).await?;
        self.cache.clear();
        Ok(())
    }

    /// Perform the retraction against a version the caller already
    /// holds, rather than the cache's.
    ///
    /// For a caller that read the version elsewhere -- from a listing,
    /// or from a sibling handle -- and has no resolved cache of its own.
    pub async fn expecting<Env>(self, when: Version, env: &Env) -> Result<(), RetractError>
    where
        Env: Provider<memory::Retract>,
    {
        self.capability.retract(when).perform(env).await?;
        self.cache.clear();
        Ok(())
    }
}
