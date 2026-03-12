use dialog_capability::Did;

use super::SiteName;
use super::branch::RemoteBranch;
use crate::RemoteAddress;
use crate::repository::branch::BranchName;

/// A cursor pointing to a specific repository at a remote site.
///
/// Created by [`RemoteSite::repository`](super::site::RemoteSite::repository).
/// Holds the remote name, credentials, and subject DID identifying the repository.
///
/// Call [`.branch(name)`](RemoteRepository::branch) to get a cursor into a
/// specific branch within this repository.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    remote: SiteName,
    address: RemoteAddress,
    subject: Did,
}

impl RemoteRepository {
    pub(super) fn new(remote: SiteName, address: RemoteAddress, subject: Did) -> Self {
        Self {
            remote,
            address,
            subject,
        }
    }

    /// The address for authenticating remote operations.
    pub fn address(&self) -> &RemoteAddress {
        &self.address
    }

    /// The subject DID of the repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// The remote name.
    pub fn remote(&self) -> &SiteName {
        &self.remote
    }

    /// Get a cursor into a specific branch at this remote repository.
    pub fn branch(&self, name: impl Into<BranchName>) -> RemoteBranch {
        RemoteBranch::new(
            self.remote.clone(),
            self.address.clone(),
            self.subject.clone(),
            name.into(),
        )
    }
}
