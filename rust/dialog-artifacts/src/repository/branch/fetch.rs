use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;

use super::Branch;
use crate::repository::Site;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

mod local;
mod remote;

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
            + Provider<RemoteInvocation<memory_fx::Resolve, Site>>,
    {
        let state = self.branch.state();
        let upstream = state.upstream.as_ref().ok_or_else(|| {
            RepositoryError::BranchHasNoUpstream {
                id: self.branch.id(),
            }
        })?;

        match upstream {
            crate::repository::branch::state::UpstreamState::Local { branch: id } => {
                local::fetch(self.branch, id, env).await
            }
            crate::repository::branch::state::UpstreamState::Remote {
                site,
                branch: id,
                subject,
            } => remote::fetch(site, id, subject, env).await,
        }
    }
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

        // Create main and commit
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

        // Create feature tracking main
        let feature = Branch::open("feature", issuer, test_subject())
            .perform(&env)
            .await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
            })
            .perform(&env)
            .await?;

        // Fetch should return main's revision
        let fetched = super::local::fetch(
            &feature,
            &"main".into(),
            &env,
        )
        .await?;

        assert!(fetched.is_some());
        assert_eq!(fetched.unwrap().tree(), main_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_does_not_modify_local_state_on_fetch() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        // Create main and commit
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

        // Create feature tracking main
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

        // Fetch
        let _fetched = super::local::fetch(
            &feature,
            &"main".into(),
            &env,
        )
        .await?;

        // Feature revision should NOT have changed
        assert_eq!(feature.revision(), feature_revision_before);

        Ok(())
    }
}
