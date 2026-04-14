mod fetch;
mod publish;
mod selector;

use crate::RemoteAddress;
use crate::repository::branch::state::{BranchName, UpstreamState};
use crate::repository::cell::{Cell, Retain};
use crate::repository::node_reference::NodeReference;
use crate::repository::remote::RemoteName;
use crate::repository::revision::Revision;

/// A loaded remote branch.
///
/// Holds a local cache of the remote revision and the connection info
/// needed to fetch from / publish to the remote.
#[derive(Debug, Clone)]
pub struct RemoteBranch {
    /// Local cache of the remote branch revision.
    /// Path: `remote/{name}/branch/{branch}/revision`
    pub(crate) revision: Cell<Revision>,

    /// Remote address (site + subject).
    pub(crate) address: Retain<RemoteAddress>,

    /// The name of the remote this branch belongs to.
    pub(crate) remote_name: RemoteName,
}

impl RemoteBranch {
    /// The branch name, derived from the revision cell path.
    pub fn name(&self) -> BranchName {
        let cell_name = self.revision.name();
        cell_name
            .strip_prefix("branch/")
            .and_then(|s| s.strip_suffix("/revision"))
            .unwrap_or(cell_name)
            .into()
    }

    /// The name of the remote this branch belongs to.
    pub fn remote_name(&self) -> RemoteName {
        self.remote_name.clone()
    }

    /// The cached remote revision, if fetched.
    pub fn revision(&self) -> Option<Revision> {
        self.revision.get()
    }

    /// The remote address.
    pub fn address(&self) -> RemoteAddress {
        self.address.get().clone()
    }

    /// Fetch the latest revision from the remote.
    pub fn fetch(&self) -> fetch::Fetch<'_> {
        fetch::Fetch::new(self)
    }

    /// Publish a revision to the remote.
    pub fn publish(&self, revision: Revision) -> publish::Publish<'_> {
        publish::Publish::new(self, revision)
    }
}

impl From<&RemoteBranch> for UpstreamState {
    fn from(rb: &RemoteBranch) -> Self {
        UpstreamState::Remote {
            name: rb.remote_name(),
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
