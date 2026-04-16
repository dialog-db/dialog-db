mod fetch;
mod load;
mod open;
mod publish;
mod reference;

use std::ops::Deref;

use crate::repository::branch::upstream::UpstreamState;
use crate::repository::node_reference::NodeReference;
use crate::repository::revision::Revision;
use fetch::Fetch;
use publish::Publish;

pub use reference::RemoteBranchReference;

/// A loaded remote branch.
///
/// Wraps a [`RemoteBranchReference`] that has been resolved (the revision
/// cell has been fetched from storage).
#[derive(Debug, Clone)]
pub struct RemoteBranch(RemoteBranchReference);

impl RemoteBranch {
    /// Create from a resolved reference.
    pub fn new(reference: RemoteBranchReference) -> Self {
        Self(reference)
    }

    /// Fetch the latest revision from the remote.
    pub fn fetch(&self) -> Fetch<'_> {
        Fetch::new(self)
    }

    /// Publish a revision to the remote.
    pub fn publish(&self, revision: Revision) -> Publish<'_> {
        Publish::new(self, revision)
    }
}

impl Deref for RemoteBranch {
    type Target = RemoteBranchReference;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&RemoteBranch> for UpstreamState {
    fn from(rb: &RemoteBranch) -> Self {
        UpstreamState::Remote {
            name: rb.repository.site().name(),
            branch: rb.name(),
            tree: NodeReference::default(),
        }
    }
}

impl From<RemoteBranch> for UpstreamState {
    fn from(rb: RemoteBranch) -> Self {
        UpstreamState::from(&rb)
    }
}
