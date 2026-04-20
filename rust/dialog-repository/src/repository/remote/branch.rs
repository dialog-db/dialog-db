mod fetch;
mod load;
mod open;
mod publish;
mod reference;

pub use fetch::*;
pub use load::*;
pub use open::*;
pub use publish::*;
pub use reference::*;

use std::ops::Deref;

use crate::repository::branch::UpstreamState;
use crate::repository::revision::Revision;
use crate::repository::tree::TreeReference;

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
    pub fn fetch(&self) -> FetchRemoteBranch<'_> {
        FetchRemoteBranch::new(self)
    }

    /// Publish a revision to the remote.
    pub fn publish(&self, revision: Revision) -> PublishRemoteBranch<'_> {
        PublishRemoteBranch::new(self, revision)
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
            name: rb.repository.site().name().to_string(),
            branch: rb.name().to_string(),
            tree: TreeReference::default(),
        }
    }
}

impl From<RemoteBranch> for UpstreamState {
    fn from(rb: RemoteBranch) -> Self {
        UpstreamState::from(&rb)
    }
}
