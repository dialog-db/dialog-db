use dialog_capability::{Did, Provider};
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;

use crate::repository::Site;
use crate::repository::branch::state::BranchId;
use crate::repository::error::RepositoryError;
use crate::repository::remote::RemoteBranch;
use crate::repository::revision::Revision;

/// Fetch the current revision from a remote upstream branch.
///
/// Does NOT modify local state.
pub(super) async fn fetch<Env>(
    site: &Site,
    upstream_branch_id: &BranchId,
    upstream_subject: &Did,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<RemoteInvocation<memory_fx::Resolve, Site>>,
{
    let remote_branch = RemoteBranch {
        site: site.clone(),
        subject: upstream_subject.clone(),
        branch: upstream_branch_id.clone(),
    };

    remote_branch.resolve(env).await
}
