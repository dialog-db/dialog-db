use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::state::UpstreamState;
use crate::repository::error::RepositoryError;

/// Command struct for setting a branch's upstream.
pub struct SetUpstream<'a> {
    branch: &'a Branch,
    upstream: UpstreamState,
}

impl<'a> SetUpstream<'a> {
    pub(super) fn new(branch: &'a Branch, upstream: UpstreamState) -> Self {
        Self { branch, upstream }
    }
}

impl SetUpstream<'_> {
    /// Execute the set_upstream operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory_fx::Publish>,
    {
        // Validate: upstream must not be this branch itself
        if let UpstreamState::Local { ref branch, .. } = self.upstream
            && branch == self.branch.name()
        {
            return Err(RepositoryError::BranchUpstreamIsItself {
                name: self.branch.name().clone(),
            });
        }

        self.branch
            .upstream
            .publish(Some(self.upstream), env)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::repository::branch::state::UpstreamState;
    use crate::repository::error::RepositoryError;
    use crate::repository::node_reference::NodeReference;

    use crate::helpers::{test_operator, test_repo};

    #[dialog_common::test]
    async fn it_sets_local_upstream() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;

        let branch = repo.branch("feature").open().perform(&operator).await?;

        // Create upstream branch
        let _main = repo.branch("main").open().perform(&operator).await?;

        branch
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&operator)
            .await?;

        let upstream = branch.upstream();
        assert_eq!(
            upstream,
            Some(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_sets_remote_upstream() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .set_upstream(UpstreamState::Remote {
                name: "origin".into(),
                branch: "main".into(),
                tree: NodeReference::default(),
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
    async fn it_errors_setting_upstream_to_self() -> anyhow::Result<()> {
        let operator = test_operator().await;
        let repo = test_repo(&operator).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let result = branch
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
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
