use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;
use dialog_s3_credentials::Credentials;

use super::Branch;
use super::state::BranchId;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

/// Command struct for fetching the upstream branch's current revision.
///
/// Borrows `&Branch` (non-consuming). Reads `branch.state().upstream` to
/// dispatch to local or remote fetch logic.
///
/// Does NOT modify local state — only reads from upstream.
pub struct Fetch<'a> {
    pub(super) branch: &'a Branch,
}

impl Fetch<'_> {
    /// Execute the fetch operation, returning the upstream revision.
    ///
    /// Returns `None` if the upstream has no revision yet.
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve>
            + Provider<RemoteInvocation<memory_fx::Resolve, Credentials>>,
    {
        let state = self.branch.state();
        let upstream = state.upstream.as_ref().ok_or_else(|| {
            RepositoryError::BranchHasNoUpstream {
                id: self.branch.id(),
            }
        })?;

        match upstream {
            crate::repository::branch::state::UpstreamState::Local { branch: id } => {
                fetch_local(self.branch, id, env).await
            }
            crate::repository::branch::state::UpstreamState::Remote {
                site,
                branch: id,
                subject,
            } => fetch_remote(site, id, subject, env).await,
        }
    }
}

/// Fetch the current revision from a local upstream branch.
///
/// Does NOT modify local state.
pub(crate) async fn fetch_local<Env>(
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

/// Fetch the current revision from a remote upstream branch.
///
/// Does NOT modify local state. Looks up credentials from the persisted
/// `RemoteSite` configuration.
async fn fetch_remote<Env>(
    site: &str,
    upstream_branch_id: &BranchId,
    upstream_subject: &dialog_capability::Did,
    env: &Env,
) -> Result<Option<Revision>, RepositoryError>
where
    Env: Provider<memory_fx::Resolve>
        + Provider<RemoteInvocation<memory_fx::Resolve, Credentials>>,
{
    let remote_site =
        crate::repository::remote::RemoteSite::load(site, upstream_subject, env).await?;

    let remote_branch = crate::repository::remote::RemoteBranch {
        remote: remote_site.name().to_string(),
        site: remote_site.site().clone(),
        credentials: remote_site.credentials().clone(),
        subject: upstream_subject.clone(),
        branch: upstream_branch_id.clone(),
    };

    remote_branch.resolve(env).await
}

#[cfg(test)]
mod tests {
    use super::super::Branch;
    use super::super::tests::{test_issuer, test_subject};
    use crate::artifacts::{Artifact, Instruction};
    use crate::repository::branch::state::UpstreamState;
    use dialog_storage::provider::Volatile;
    use futures_util::stream;

    #[dialog_common::test]
    async fn it_fetches_local_upstream_revision() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;
        let (main, _) = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;
        let main_revision = main.revision();

        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&env)
            .await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
            })
            .perform(&env)
            .await?;

        let fetched = super::fetch_local(&feature, &"main".into(), &env).await?;

        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().tree(), main_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_does_not_modify_local_state_on_fetch() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        let main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;
        let (_main, _) = main
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:main".parse()?,
                is: crate::Value::String("Main data".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&env)
            .await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
            })
            .perform(&env)
            .await?;

        let feature_revision_before = feature.revision();

        let _fetched = super::fetch_local(&feature, &"main".into(), &env).await?;

        assert_eq!(feature.revision(), feature_revision_before);

        Ok(())
    }
}
