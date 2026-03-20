use dialog_capability::Did;
use dialog_s3_credentials::s3::S3Site;

use super::SiteName;
use super::branch::RemoteBranch;
use crate::repository::branch::BranchName;

/// A cursor pointing to a specific repository at a remote site.
///
/// Created by [`RemoteSite::repository`](super::site::RemoteSite::repository).
/// Holds the remote name, site configuration, and subject DID identifying the repository.
///
/// Call [`.branch(name)`](RemoteRepository::branch) to get a cursor into a
/// specific branch within this repository.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    remote: SiteName,
    site: S3Site,
    subject: Did,
}

impl RemoteRepository {
    pub(super) fn new(remote: SiteName, site: S3Site, subject: Did) -> Self {
        Self {
            remote,
            site,
            subject,
        }
    }

    /// The site configuration for remote operations.
    pub fn site(&self) -> &S3Site {
        &self.site
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
            self.site.clone(),
            self.subject.clone(),
            name.into(),
        )
    }
}
