//! Publish command for remote branches.

use dialog_capability::fork::Fork;
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::S3;

use super::RemoteBranch;
use crate::SiteAddress as SiteAddressEnum;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Memory;
use crate::repository::revision::Revision;

/// Command to publish a revision to the remote.
///
/// Publishes the revision to the remote memory via Fork and updates
/// the local cache cell.
pub struct Publish<'a> {
    branch: &'a RemoteBranch,
    revision: Revision,
}

impl<'a> Publish<'a> {
    pub(super) fn new(branch: &'a RemoteBranch, revision: Revision) -> Self {
        Self { branch, revision }
    }

    /// Execute the publish.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<S3, memory_fx::Publish>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Publish>>
            + Provider<memory_fx::Publish>
            + ConditionalSync,
    {
        let address = self.branch.address();
        let subject = Subject::from(address.subject.clone());
        let branch_name = self
            .branch
            .revision
            .name()
            .strip_prefix("branch/")
            .and_then(|s| s.strip_suffix("/revision"))
            .unwrap_or("");

        let cell_cap = Memory::new(subject)
            .branch(branch_name)
            .cell_capability("revision");

        match address.address {
            SiteAddressEnum::S3(ref addr) => {
                publish_remote(&cell_cap, addr, &self.revision, env).await?;
            }
            #[cfg(feature = "ucan")]
            SiteAddressEnum::Ucan(ref addr) => {
                publish_remote(&cell_cap, addr, &self.revision, env).await?;
            }
        }

        // Update local cache
        self.branch.revision.publish(self.revision, env).await?;

        Ok(())
    }
}

async fn publish_remote<A, Env>(
    cell_cap: &Capability<memory_fx::Cell>,
    address: &A,
    revision: &Revision,
    env: &Env,
) -> Result<(), RepositoryError>
where
    A: SiteAddress,
    A::Site: Site,
    Env: Provider<Fork<A::Site, memory_fx::Resolve>>
        + Provider<Fork<A::Site, memory_fx::Publish>>
        + ConditionalSync,
{
    // Resolve to get current edition
    let resolve_result: Option<memory_fx::Publication> = cell_cap
        .clone()
        .invoke(memory_fx::Resolve)
        .fork(address)
        .perform(env)
        .await?;

    let edition = resolve_result.map(|pub_data| pub_data.edition);

    let content = serde_ipld_dagcbor::to_vec(revision)
        .map_err(|e| RepositoryError::StorageError(format!("Failed to encode revision: {}", e)))?;

    cell_cap
        .clone()
        .invoke(memory_fx::Publish::new(content, edition))
        .fork(address)
        .perform(env)
        .await?;

    Ok(())
}
