use dialog_capability::Did;

use super::SiteName;
use crate::RemoteAddress;

/// A cursor pointing to a specific repository at a remote site.
///
/// Created by [`RemoteSite::repository`](super::site::RemoteSite::repository).
/// Holds the remote name, address, and subject DID identifying the repository.
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
    /// Create a new remote repository cursor.
    pub fn new(remote: SiteName, address: RemoteAddress, subject: Did) -> Self {
        Self {
            remote,
            address,
            subject,
        }
    }

    /// The address for remote operations.
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
}
