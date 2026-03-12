use dialog_capability::Did;
use dialog_s3_credentials::Credentials;

use super::branch::RemoteBranch;
use crate::repository::Site;
use crate::repository::branch::BranchId;

/// A cursor pointing to a specific repository at a remote site.
///
/// Created by [`RemoteSite::repository`](super::site::RemoteSite::repository).
/// Holds the remote name, credentials, and subject DID identifying the repository.
///
/// Call [`.branch(name)`](RemoteRepository::branch) to get a cursor into a
/// specific branch within this repository.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    /// The remote name (e.g., "origin") used to look up configuration.
    pub(super) remote: String,
    /// The remote site address.
    pub(super) site: Site,
    /// The credentials for authenticating remote operations.
    pub(super) credentials: Credentials,
    /// The subject DID of the repository.
    pub(super) subject: Did,
}

impl RemoteRepository {
    /// The remote site address.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// The credentials for authenticating remote operations.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// The subject DID of the repository.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// The remote name.
    pub fn remote(&self) -> &str {
        &self.remote
    }

    /// Get a cursor into a specific branch at this remote repository.
    pub fn branch(&self, name: impl Into<BranchId>) -> RemoteBranch {
        RemoteBranch {
            remote: self.remote.clone(),
            site: self.site.clone(),
            credentials: self.credentials.clone(),
            subject: self.subject.clone(),
            branch: name.into(),
        }
    }
}
