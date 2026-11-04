//! The [`Load`] command: read a spilled value wherever the environment can
//! reach it.

use dialog_capability::{Capability, Command, Provider};
use dialog_common::{Buffer, ConditionalSync, Priority};

use super::Read;
use crate::archive::ArchiveError;

/// Command: the whole blob a [`Read`] capability names, from the local
/// store, or else from wherever the environment knows the subject's blobs
/// live, written back into the local blob store before it is returned.
///
/// The blob counterpart of [`archive::Load`](crate::archive::Load), and a
/// command for the same reason: [`Read`] stays the one attenuated,
/// forkable read, which reads only the store it is performed on.
///
/// A value spilled before values moved to the blob store is a block in the
/// subject's [default catalog](crate::archive::prelude::DEFAULT_CATALOG),
/// so a provider that finds no blob looks there before it looks anywhere
/// else, and fails as the archive fails. The capability's range is not
/// read: a load is of the whole value.
#[derive(Debug, Clone)]
pub struct Load {
    /// The blob to read.
    pub capability: Capability<Read>,
    /// How a hydration this read causes ranks against the site's other
    /// reads. Never reaches the wire.
    pub priority: Priority,
}

impl Load {
    /// Load the blob `capability` names, as a read someone is waiting on.
    pub fn new(capability: Capability<Read>) -> Self {
        Self {
            capability,
            priority: Priority::Demand,
        }
    }

    /// Rank a hydration this read causes as `priority`.
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Perform this load against `env`.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Buffer>, ArchiveError>
    where
        Env: Provider<Load> + ConditionalSync + ?Sized,
    {
        env.execute(self).await
    }
}

impl Command for Load {
    type Input = Self;
    type Output = Result<Option<Buffer>, ArchiveError>;
}

impl From<Capability<Read>> for Load {
    fn from(capability: Capability<Read>) -> Self {
        Self::new(capability)
    }
}
