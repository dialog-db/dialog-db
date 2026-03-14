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
    use dialog_s3_credentials::Address as S3Address;
    use dialog_s3_credentials::s3::Credentials as S3Credentials;
    use dialog_storage::provider::Volatile;

    use crate::RemoteAddress;
    use crate::repository::branch::state::UpstreamState;
    use crate::repository::error::RepositoryError;
    use crate::repository::node_reference::NodeReference;
    use crate::repository::remote::RemoteBranch;

    use super::super::tests::{test_issuer, test_subject};
    use crate::repository::Repository;

    #[dialog_common::test]
    async fn it_sets_local_upstream() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());

        let branch = repo.open_branch("feature").perform(&env).await?;

        // Create upstream branch
        let _main = repo.open_branch("main").perform(&env).await?;

        branch
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&env)
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
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());
        let branch = repo.open_branch("main").perform(&env).await?;

        let s3_addr = S3Address::new("https://s3.us-east-1.amazonaws.com", "us-east-1", "bucket");
        let remote_branch = RemoteBranch::new(
            "origin".into(),
            RemoteAddress::S3(S3Credentials::public(s3_addr).unwrap()),
            "did:test:remote-repo".parse()?,
            "main".into(),
        );

        branch.set_upstream(remote_branch).perform(&env).await?;

        let upstream = branch.upstream();
        match upstream {
            Some(UpstreamState::Remote {
                name,
                branch,
                subject,
                ..
            }) => {
                assert_eq!(name, "origin");
                assert_eq!(branch.as_str(), "main");
                assert_eq!(subject, "did:test:remote-repo".parse()?);
            }
            _ => panic!("Expected Remote upstream"),
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_setting_upstream_to_self() -> anyhow::Result<()> {
        let env = Volatile::new();

        let repo = Repository::new(test_issuer().await, test_subject());
        let branch = repo.open_branch("main").perform(&env).await?;

        let result = branch
            .set_upstream(UpstreamState::Local {
                branch: "main".into(),
                tree: NodeReference::default(),
            })
            .perform(&env)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchUpstreamIsItself { .. })
        ));

        Ok(())
    }
}
