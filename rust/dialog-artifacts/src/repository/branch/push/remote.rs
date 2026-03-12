use dialog_capability::{Did, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use futures_util::StreamExt;

use crate::repository::Site;
use crate::repository::branch::Branch;
use crate::repository::branch::novelty::novelty;
use crate::repository::branch::state::BranchId;
use crate::repository::error::RepositoryError;
use crate::repository::remote::RemoteBranch;
use crate::repository::revision::Revision;

/// Push local changes to a remote upstream branch.
///
/// 1. Build `RemoteBranch` from upstream state
/// 2. Compute novel nodes via `novelty()`
/// 3. Read each node's raw bytes from local archive, upload to remote
/// 4. Publish revision to remote
pub(super) async fn push<Env>(
    branch: &Branch,
    site: &Site,
    upstream_branch_id: &BranchId,
    upstream_subject: &Did,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + Provider<RemoteInvocation<archive_fx::Put, Site>>
        + Provider<RemoteInvocation<memory_fx::Resolve, Site>>
        + Provider<RemoteInvocation<memory_fx::Publish, Site>>
        + ConditionalSync
        + 'static,
{
    let remote_branch = RemoteBranch {
        site: site.clone(),
        subject: upstream_subject.clone(),
        branch: upstream_branch_id.clone(),
    };

    let branch_revision = branch.revision();
    let branch_base = branch.base();
    let catalog = branch.archive().index();

    // Compute novel nodes and upload each one's raw bytes
    let nodes = novelty(
        *branch_base.hash(),
        *branch_revision.tree().hash(),
        env,
        catalog.clone(),
    );
    tokio::pin!(nodes);

    while let Some(node_result) = nodes.next().await {
        let node = node_result.map_err(|e| RepositoryError::PushFailed {
            cause: format!("Failed to compute novelty: {}", e),
        })?;

        let hash = *node.hash();

        // Read raw bytes from local archive
        let get_cap = catalog.clone().invoke(archive_fx::Get::new(hash));
        let bytes = get_cap.perform(env).await.map_err(|e| {
            RepositoryError::PushFailed {
                cause: format!("Failed to read local block: {}", e),
            }
        })?;

        if let Some(bytes) = bytes {
            // Upload to remote
            remote_branch
                .upload_block(hash, bytes, env)
                .await
                .map_err(|e| RepositoryError::PushFailed {
                    cause: format!("Failed to upload block: {}", e),
                })?;
        }
    }

    // Publish revision to remote
    remote_branch
        .publish(branch_revision.clone(), env)
        .await
        .map_err(|e| RepositoryError::PushFailed {
            cause: format!("Failed to publish revision: {}", e),
        })?;

    Ok(Some(branch_revision))
}
