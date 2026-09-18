//! Retract command for removing a cell's contents.

use super::cell::Cache;
use crate::RetractError;
use dialog_capability::{Capability, Policy as _, Provider};
use dialog_effects::memory::{self, prelude::CellExt};

/// Command to remove a cell's contents.
///
/// Created by [`Cell::retract`](super::Cell::retract). Like
/// [`Publish`](super::Publish) it is a compare-and-set against the
/// version the cache last saw, so a retract racing a write fails loudly
/// rather than discarding it. A cell the cache has never seen has no
/// version to set against, which is [`RetractError::Unobserved`]:
/// resolve it first.
///
/// There is no remote counterpart. A cell lives under the subject that
/// owns it, and removing it is that owner's business; nothing in the
/// protocol asks one replica to delete another's.
pub struct Retract<T, Codec: Clone> {
    /// Capability chain targeting the cell to retract.
    pub capability: Capability<memory::Cell>,
    /// Cached edition supplying the compare-and-set version.
    pub cache: Cache<T, Codec>,
}

impl<T, Codec> Retract<T, Codec>
where
    T: Clone,
    Codec: Clone,
{
    /// Perform the retract against the local environment.
    ///
    /// Clears the cache on success, so the handle reads as an empty cell
    /// rather than serving the contents it just removed.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RetractError>
    where
        Env: Provider<memory::Retract>,
    {
        let Some(when) = self.cache.version() else {
            return Err(RetractError::Unobserved {
                cell: memory::Cell::of(&self.capability).cell.clone(),
            });
        };
        self.capability.retract(when).perform(env).await?;
        self.cache.clear();
        Ok(())
    }
}
