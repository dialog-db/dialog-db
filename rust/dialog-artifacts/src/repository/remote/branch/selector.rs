use dialog_capability::Did;

use crate::repository::branch::BranchName;
use crate::repository::remote::address::RemoteAddress;
use crate::repository::remote::repository::RemoteRepository;

/// A reference to a named branch at a remote repository.
///
/// Holds the branch name, address, and subject — enough to construct
/// a typed `RemoteBranch<A>` when an operation is needed.
pub struct RemoteBranchSelector {
    name: BranchName,
    address: RemoteAddress,
}

impl RemoteBranchSelector {
    /// The branch name.
    pub fn name(&self) -> &BranchName {
        &self.name
    }

    /// The full remote address (site + subject).
    pub fn address(&self) -> &RemoteAddress {
        &self.address
    }

    /// The subject DID.
    pub fn subject(&self) -> &Did {
        &self.address.subject
    }
}

impl RemoteRepository {
    /// Get a branch selector at this remote repository.
    pub fn branch(&self, name: impl Into<BranchName>) -> RemoteBranchSelector {
        RemoteBranchSelector {
            name: name.into(),
            address: self.address(),
        }
    }
}
