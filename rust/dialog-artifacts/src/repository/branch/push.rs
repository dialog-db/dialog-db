use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use dialog_effects::archive as archive_fx;
use dialog_effects::memory as memory_fx;
use dialog_effects::remote::RemoteInvocation;

use super::Branch;
use crate::repository::Site;
use crate::repository::error::RepositoryError;
use crate::repository::revision::Revision;

pub(crate) mod local;
mod remote;

/// Command struct for pushing local changes to an upstream branch.
///
/// Borrows `&Branch` (non-consuming). Reads `branch.state().upstream` to
/// dispatch to local or remote push logic.
pub struct Push<'a> {
    pub(super) branch: &'a Branch,
}

impl Push<'_> {
    /// Execute the push operation.
    ///
    /// Returns `Some(revision)` on success, `None` if the push could not
    /// fast-forward (diverged).
    pub async fn perform<Env>(self, env: &Env) -> Result<Option<Revision>, RepositoryError>
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
        let state = self.branch.state();
        let upstream = state.upstream.as_ref().ok_or_else(|| {
            RepositoryError::BranchHasNoUpstream {
                id: self.branch.id(),
            }
        })?;

        match upstream {
            crate::repository::branch::state::UpstreamState::Local { branch: id } => {
                local::push(self.branch, id, env).await
            }
            crate::repository::branch::state::UpstreamState::Remote {
                site,
                branch: id,
                subject,
            } => remote::push(self.branch, site, id, subject, env).await,
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
    async fn it_pushes_to_local_upstream() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        // Create main branch
        let _main = Branch::open("main", issuer.clone(), test_subject())
            .perform(&env)
            .await?;

        // Create feature branch tracking main
        let feature = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&env)
            .await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
            })
            .perform(&env)
            .await?;

        // Commit to feature
        let artifact = Artifact {
            the: "user/name".parse()?,
            of: "user:123".parse()?,
            is: crate::Value::String("Alice".to_string()),
            cause: None,
        };
        let (feature, _) = feature
            .commit(stream::iter(vec![Instruction::Assert(artifact)]))
            .perform(&env)
            .await?;

        let feature_revision = feature.revision();

        // Push feature -> main (using local push directly)
        let result = super::local::push(&feature, &"main".into(), &env).await?;
        assert!(result.is_some());

        // Verify main got updated
        let main_reloaded = Branch::load("main", issuer, test_subject())
            .perform(&env)
            .await?;
        assert_eq!(main_reloaded.revision().tree(), feature_revision.tree());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_none_when_local_upstream_diverged() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        // Create main branch and commit
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

        // Create feature from empty, track main
        let feature = Branch::open("feature", issuer.clone(), test_subject())
            .perform(&env)
            .await?;
        feature
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
            })
            .perform(&env)
            .await?;

        // Commit to feature
        let (feature, _) = feature
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/email".parse()?,
                of: "user:feature".parse()?,
                is: crate::Value::String("feature@test.com".to_string()),
                cause: None,
            })]))
            .perform(&env)
            .await?;

        // Push should fail (diverged — main has changes feature doesn't know about)
        let result = super::local::push(&feature, &"main".into(), &env).await?;
        assert!(result.is_none(), "Push should return None when diverged");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_no_upstream_by_default() -> anyhow::Result<()> {
        let env = Volatile::new();
        let issuer = test_issuer().await;

        let branch = Branch::open("feature", issuer, test_subject())
            .perform(&env)
            .await?;

        // No upstream set
        assert!(branch.state().upstream.is_none());

        Ok(())
    }
}
