//! The [`Load`] command: read a spilled value wherever the environment can
//! reach it.

use dialog_capability::{Capability, Command, Provider};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync, Priority};

use super::Read;
use crate::archive::ArchiveError;
use crate::archive::prelude::ArchiveScope;

/// Command: the whole blob a [`Read`] of `digest` in `archive` names (see
/// [`capability`](Self::capability)), from the local store, or else from wherever the environment knows the subject's blobs
/// live, written back into the local blob store before it is returned.
///
/// The blob counterpart of [`archive::Load`](crate::archive::Load), and a
/// command for the same reason: [`Read`] stays the one attenuated,
/// forkable read, which reads only the store it is performed on.
///
/// A value spilled before values moved to the blob store is a block in the
/// subject's [default catalog](crate::archive::prelude::DEFAULT_CATALOG),
/// so a provider that finds no blob looks there before it looks anywhere
/// else, and fails as the archive fails. A load is of the whole value.
#[derive(Debug, Clone)]
pub struct Load {
    /// The archive whose blob store holds the value.
    pub archive: ArchiveScope,
    /// The value's digest.
    pub digest: Blake3Hash,
    /// How a hydration this read causes ranks against the site's other
    /// reads. Never reaches the wire.
    pub priority: Priority,
}

impl Load {
    /// Load the blob `digest` names in `archive`, as a read someone is
    /// waiting on.
    pub fn new(archive: ArchiveScope, digest: impl Into<Blake3Hash>) -> Self {
        Self {
            archive,
            digest: digest.into(),
            priority: Priority::Demand,
        }
    }

    /// The local read of this blob: what a store performs to answer.
    pub fn capability(&self) -> Capability<Read> {
        self.archive.blob().read(self.digest.clone())
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
