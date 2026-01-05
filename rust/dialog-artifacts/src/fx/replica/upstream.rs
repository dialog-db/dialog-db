//! Upstream configuration for effectful branches.
//!
//! An upstream represents another branch (local or remote) that a branch
//! tracks for synchronization purposes.

pub use crate::replica::UpstreamState;

use super::branch::Branch;
use super::error::ReplicaError;
use super::remote::RemoteBranch;
use super::types::{BranchId, Revision, Site};
use super::Replica;
use crate::fx::effects::{effectful, Memory, RemoteBackend};
use crate::fx::local::Address as LocalAddress;
use crate::fx::remote::Address as RemoteAddress;
use dialog_common::fx::Effect;

/// Upstream represents a branch being tracked (local or remote).
///
/// This is the effectful version that works with the algebraic effects system.
#[derive(Debug, Clone)]
pub enum Upstream {
    /// A local branch upstream
    Local(Branch),
    /// A remote branch upstream
    Remote(RemoteBranch),
}

impl Upstream {
    /// Opens an upstream from its state descriptor.
    ///
    /// Returns an effect that, when performed, will load the upstream branch.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress> + RemoteBackend)]
    pub fn open(state: UpstreamState, replica: Replica) -> Result<Self, ReplicaError> {
        match state {
            UpstreamState::Local { branch } => {
                let branch = perform!(Branch::load(branch, replica))?;
                Ok(Upstream::Local(branch))
            }
            UpstreamState::Remote { site, branch } => {
                let remote_branch =
                    perform!(RemoteBranch::open(replica.address().clone(), site, branch))?;
                Ok(Upstream::Remote(remote_branch))
            }
        }
    }

    /// Returns the branch id of this upstream.
    pub fn id(&self) -> &BranchId {
        match self {
            Upstream::Local(branch) => branch.id(),
            Upstream::Remote(branch) => branch.id(),
        }
    }

    /// Returns revision this branch is at.
    pub fn revision(&self) -> Option<Revision> {
        match self {
            Upstream::Local(branch) => Some(branch.revision()),
            Upstream::Remote(branch) => branch.revision().cloned(),
        }
    }

    /// Returns site of the branch. If local returns None otherwise
    /// returns site identifier.
    pub fn site(&self) -> Option<&Site> {
        match self {
            Upstream::Local(_) => None,
            Upstream::Remote(branch) => Some(branch.site()),
        }
    }

    /// Returns true if this upstream is a local branch.
    pub fn is_local(&self) -> bool {
        matches!(self, Upstream::Local(_))
    }

    /// Converts this upstream to its state descriptor.
    pub fn to_state(&self) -> UpstreamState {
        match self {
            Upstream::Local(branch) => UpstreamState::Local {
                branch: branch.id().clone(),
            },
            Upstream::Remote(remote) => UpstreamState::Remote {
                site: remote.site().clone(),
                branch: remote.id().clone(),
            },
        }
    }

    /// Fetches the current revision from the upstream.
    ///
    /// Returns the updated Upstream along with the fetched revision.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn fetch(self) -> Result<(Self, Option<Revision>), ReplicaError> {
        match self {
            Upstream::Local(branch) => {
                let revision = branch.revision().clone();
                Ok((Upstream::Local(branch), Some(revision)))
            }
            Upstream::Remote(branch) => {
                let (branch, revision) = perform!(branch.fetch())?;
                Ok((Upstream::Remote(branch), revision))
            }
        }
    }

    /// Publishes a revision to the upstream.
    ///
    /// Returns the updated Upstream.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn publish(self, revision: Revision) -> Result<Self, ReplicaError> {
        match self {
            Upstream::Local(branch) => {
                let branch = perform!(branch.reset(revision))?;
                Ok(Upstream::Local(branch))
            }
            Upstream::Remote(branch) => {
                let branch = perform!(branch.publish(revision))?;
                Ok(Upstream::Remote(branch))
            }
        }
    }
}

impl From<Branch> for Upstream {
    fn from(branch: Branch) -> Self {
        Self::Local(branch)
    }
}

impl From<RemoteBranch> for Upstream {
    fn from(branch: RemoteBranch) -> Self {
        Self::Remote(branch)
    }
}

impl From<Upstream> for UpstreamState {
    fn from(upstream: Upstream) -> Self {
        upstream.to_state()
    }
}
