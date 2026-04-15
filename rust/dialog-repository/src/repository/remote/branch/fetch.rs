//! Fetch command for remote branches.

use dialog_capability::Provider;
use dialog_capability::fork::Fork;
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::S3;
use dialog_remote_ucan_s3::UcanSite;

use super::RemoteBranch;
use crate::SiteAddress;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command to fetch the latest revision from the remote.
///
/// Resolves the remote branch revision via Fork and updates
/// the local cache cell.
pub struct Fetch<'a> {
    branch: &'a RemoteBranch,
}

impl<'a> Fetch<'a> {
    /// Create a new fetch command.
    pub fn new(branch: &'a RemoteBranch) -> Self {
        Self { branch }
    }

    /// Execute the fetch.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<UcanSite, memory_fx::Resolve>>
            + Provider<memory_fx::Publish>
            + ConditionalSync,
    {
        let address = self.branch.address();

        // Resolve from remote via fork
        match address.address {
            SiteAddress::S3(ref addr) => {
                self.branch.remote.resolve().fork(addr).perform(env).await?;
            }
            SiteAddress::Ucan(ref addr) => {
                self.branch.remote.resolve().fork(addr).perform(env).await?;
            }
        }

        let revision = self.branch.remote.get();

        // Update local cache
        if let Some(ref rev) = revision {
            self.branch.local.publish(rev.clone()).perform(env).await?;
        }

        Ok(revision)
    }
}
