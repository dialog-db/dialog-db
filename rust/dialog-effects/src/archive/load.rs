//! The [`Load`] command: read a block wherever the environment can reach it.

use dialog_capability::{Capability, Command, Provider};
use dialog_common::{Blake3Hash, Buffer, ConditionalSync, Priority};

use super::prelude::CatalogScope;
use super::{ArchiveError, Get};

/// Command: the block a [`Get`] of `digest` in `catalog` names (see
/// [`capability`](Self::capability)), from the local archive,
/// or else from wherever the environment knows the subject's blocks live,
/// written back into the local archive before it is returned.
///
/// [`Get`] is the one attenuated, signable, forkable read of a block, and
/// it reads only the store it is performed on. `Load` is what a reader
/// that wants the block itself performs: an environment that knows no
/// other place to look answers it exactly as `Get`, and one that does (a
/// peer whose open branches track a remote) hydrates a local miss from
/// there. It is a command and not an effect because nothing can answer it
/// on another's behalf: it is never forked to a remote, and a remote's own
/// hydration is no answer to it.
///
/// A read that must see only what the local archive holds performs `Get`.
///
/// The bytes are returned as the store holds them: a reader that needs
/// them to be the block asked for checks them against the digest.
///
/// It names the catalog and the digest rather than carrying the `Get`
/// built from them: a store answering it builds that capability once to
/// read, and a provider that hydrates a miss reads where to from the
/// catalog without unpicking the capability.
#[derive(Debug, Clone)]
pub struct Load {
    /// The catalog the block lives in.
    pub catalog: CatalogScope,
    /// The block's digest.
    pub digest: Blake3Hash,
    /// How a hydration this read causes ranks against the site's other
    /// reads. Never reaches the wire.
    pub priority: Priority,
}

impl Load {
    /// Load the block `digest` names in `catalog`, as a read someone is
    /// waiting on.
    pub fn new(catalog: CatalogScope, digest: impl Into<Blake3Hash>) -> Self {
        Self {
            catalog,
            digest: digest.into(),
            priority: Priority::Demand,
        }
    }

    /// The local read of this block: what a store performs to answer.
    pub fn capability(&self) -> Capability<Get> {
        self.catalog.get(self.digest.clone())
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
