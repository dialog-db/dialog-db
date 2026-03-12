use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use crate::repository::branch::Branch;
use crate::repository::branch::state::BranchId;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Fetch the current revision from a local upstream branch.
///
/// Does NOT modify local state.
pub(crate) async fn fetch<Env>(
    branch: &Branch,
    upstream_id: &BranchId,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<memory_fx::Resolve>,
{
    let issuer = branch.issuer().clone();
    let subject = branch.subject().clone();

    let upstream = Branch::load(upstream_id.clone(), issuer, subject)
        .perform(env)
        .await?;

    Ok(Some(upstream.revision()))
}
