use dialog_capability::Provider;
use dialog_effects::memory::Publish;

use super::{Branch, UpstreamState};
use crate::repository::error::RepositoryError;

/// Command struct for setting a branch's upstream.
pub struct SetUpstream<'a> {
    branch: &'a Branch,
    upstream: UpstreamState,
}

impl<'a> SetUpstream<'a> {
    fn new(branch: &'a Branch, upstream: UpstreamState) -> Self {
        Self { branch, upstream }
    }
}

impl Branch {
    /// Create a command to set the upstream for this branch.
    ///
    /// Accepts both `UpstreamState` and `RemoteBranch` directly via
    /// `impl Into<UpstreamState>`.
    pub fn set_upstream(&self, upstream: impl Into<UpstreamState>) -> SetUpstream<'_> {
        SetUpstream::new(self, upstream.into())
    }
}

impl SetUpstream<'_> {
    /// Execute the set_upstream operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Publish>,
    {
        // Validate: upstream must not be this branch itself
        if let UpstreamState::Local { ref branch, .. } = self.upstream
            && *branch == self.branch.name()
        {
            return Err(RepositoryError::BranchUpstreamIsItself {
                name: self.branch.name().to_string(),
            });
        }

        self.branch
            .upstream
            .publish(self.upstream)
            .perform(env)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::repository::branch::UpstreamState;
    use crate::repository::error::RepositoryError;
    use crate::repository::tree::TreeReference;
    use anyhow::Result;

    use crate::helpers::{test_operator_with_profile, test_repo};

    #[dialog_common::test]
    async fn it_sets_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("feature").open().perform(&operator).await?;

        // Create upstream branch
        let _main = repo.branch("main").open().perform(&operator).await?;

        branch
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: TreeReference::default(),
            })
            .perform(&operator)
            .await?;

        let upstream = branch.upstream();
        assert_eq!(
            upstream,
            Some(UpstreamState::Local {
                branch: "main".into(),
                tree: TreeReference::default(),
            })
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_sets_remote_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .set_upstream(UpstreamState::Remote {
                name: "origin".into(),
                branch: "main".into(),
                tree: TreeReference::default(),
            })
            .perform(&operator)
            .await?;

        let upstream = branch.upstream();
        match upstream {
            Some(UpstreamState::Remote { name, branch, .. }) => {
                assert_eq!(name, "origin");
                assert_eq!(branch.as_str(), "main");
            }
            _ => panic!("Expected Remote upstream"),
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_setting_upstream_to_self() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let result = branch
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: TreeReference::default(),
            })
            .perform(&operator)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchUpstreamIsItself { .. })
        ));

        Ok(())
    }
}
