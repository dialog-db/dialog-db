use dialog_capability::Did;

use super::branch::RemoteBranch;
use crate::repository::Site;
use crate::repository::branch::BranchId;

/// A cursor pointing to a specific repository at a remote site.
///
/// Created by [`RemoteSite::repository`](super::site::RemoteSite::repository).
/// Holds the site address and subject DID identifying the repository.
///
/// Call [`.branch(name)`](RemoteRepository::branch) to get a cursor into a
/// specific branch within this repository.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    /// The remote site address.
    pub(super) site: Site,
    /// The subject DID of the repository.
    pub(super) subject: Did,
}

impl RemoteRepository {
    /// The remote site address.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// The subject DID of the repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Get a cursor into a specific branch at this remote repository.
    pub fn branch(&self, name: impl Into<BranchId>) -> RemoteBranch {
        RemoteBranch {
            site: self.site.clone(),
            subject: self.subject.clone(),
            branch: name.into(),
        }
    }
}
