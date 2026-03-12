use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;

use crate::repository::branch::state::BranchId;
use crate::repository::branch::Branch;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Push local changes to a local upstream branch.
///
/// Fast-forward: if the upstream's tree matches our base (it hasn't diverged),
/// reset upstream to our revision and return success.
/// Diverged: return `Ok(None)`.
pub(super) async fn push<Env>(
    branch: &Branch,
    upstream_id: &BranchId,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<archive_fx::Get>
        + Provider<archive_fx::Put>
        + Provider<memory_fx::Resolve>
        + Provider<memory_fx::Publish>
        + ConditionalSync
        + 'static,
{
    let issuer = branch.issuer().clone();
    let subject = branch.subject().clone();

    // Load the upstream branch
    let upstream = Branch::load(upstream_id.clone(), issuer, subject)
        .perform(env)
        .await?;

    let branch_revision = branch.revision();
    let branch_base = branch.base();

    // Fast-forward check: upstream's tree must match our base
    if upstream.revision().tree() != &branch_base {
        // Upstream has diverged — can't fast-forward
        return Ok(None);
    }

    // Reset upstream to our revision
    let _upstream = upstream
        .reset(branch_revision.clone())
        .perform(env)
        .await?;

    Ok(Some(branch_revision))
}
