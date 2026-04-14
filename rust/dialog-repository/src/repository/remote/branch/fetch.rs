//! Fetch command for remote branches.

use dialog_capability::fork::Fork;
use dialog_capability::site::{Site, SiteAddress};
use dialog_capability::{Capability, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::memory as memory_fx;
use dialog_remote_s3::S3;
use dialog_storage::{CborEncoder, Encoder};

use super::RemoteBranch;
use crate::SiteAddress as SiteAddressEnum;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Memory;
use crate::repository::revision::Revision;

/// Command to fetch the latest revision from the remote.
///
/// Resolves the remote branch revision via Fork and updates
/// the local cache cell.
pub struct Fetch<'a> {
    branch: &'a RemoteBranch,
}

impl<'a> Fetch<'a> {
    pub(super) fn new(branch: &'a RemoteBranch) -> Self {
        Self { branch }
    }

    /// Execute the fetch.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<Fork<S3, memory_fx::Resolve>>
            + Provider<Fork<dialog_remote_ucan_s3::UcanSite, memory_fx::Resolve>>
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

        let revision = match address.address {
            SiteAddressEnum::S3(ref addr) => resolve_remote(&cell_cap, addr, env).await?,
            #[cfg(feature = "ucan")]
            SiteAddressEnum::Ucan(ref addr) => resolve_remote(&cell_cap, addr, env).await?,
        };

        // Update local cache
        if let Some(ref rev) = revision {
            self.branch.revision.publish(rev.clone(), env).await?;
        }

        Ok(revision)
    }
}

async fn resolve_remote<A, Env>(
    cell_cap: &Capability<memory_fx::Cell>,
    address: &A,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    A: SiteAddress,
    A::Site: Site,
    Env: Provider<Fork<A::Site, memory_fx::Resolve>> + ConditionalSync,
{
    let result: Option<memory_fx::Publication> = cell_cap
        .clone()
        .invoke(memory_fx::Resolve)
        .fork(address)
        .perform(env)
        .await?;

    match result {
        None => Ok(None),
        Some(publication) => {
            let revision: Revision = CborEncoder.decode(&publication.content).await?;
            Ok(Some(revision))
        }
    }
}
