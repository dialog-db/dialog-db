mod fetch;
mod publish;
mod selector;

#[allow(unused_imports)]
pub use selector::RemoteBranchSelector;

use crate::RemoteAddress;
use crate::repository::cell::{Cell, Retain};
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
}

impl RemoteBranch {
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
