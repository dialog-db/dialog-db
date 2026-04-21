use dialog_capability::Provider;
use dialog_effects::memory::Publish;

use crate::{Branch, SetUpstreamError, Upstream, UpstreamBranch};

/// Command struct for setting a branch's upstream.
pub struct SetUpstream<'a> {
    branch: &'a Branch,
    upstream: Upstream,
}

impl Branch {
    /// Create a command to set the upstream for this branch.
    ///
    /// Accepts either a `&Branch` or a `&RemoteBranch` (converted via
    /// `From` impls on [`UpstreamBranch`]).
    pub fn set_upstream(&self, source: impl Into<UpstreamBranch>) -> SetUpstream<'_> {
        SetUpstream {
            branch: self,
            upstream: Upstream::from(source.into()),
        }
    }
}

impl SetUpstream<'_> {
    /// Execute the set_upstream operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), SetUpstreamError>
    where
        Env: Provider<Publish>,
    {
        // Validate: upstream must not be this branch itself
        if let Upstream::Local { ref branch, .. } = self.upstream
            && *branch == self.branch.name()
        {
            return Err(SetUpstreamError::UpstreamIsItself {
                branch: self.branch.name().to_string(),
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

    use crate::{SetUpstreamError, Upstream};
    use anyhow::Result;

    use crate::helpers::{test_operator_with_profile, test_repo};

    #[dialog_common::test]
    async fn it_sets_local_upstream() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        let main = repo.branch("main").open().perform(&operator).await?;

        feature.set_upstream(&main).perform(&operator).await?;

        let upstream = feature.upstream();
        assert!(matches!(
            upstream,
            Some(Upstream::Local { ref branch, .. }) if branch == "main"
        ));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_sets_remote_upstream() -> Result<()> {
        use crate::SiteAddress;
        use dialog_remote_s3::Address;

        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let site = SiteAddress::S3(
            Address::builder("https://s3.us-east-1.amazonaws.com")
                .region("us-east-1")
                .bucket("bucket")
                .build()
                .unwrap(),
        );
        let origin = repo
            .remote("origin")
            .create(site)
            .perform(&operator)
            .await?;
        let remote_main = origin.branch("main").open().perform(&operator).await?;

        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.set_upstream(&remote_main).perform(&operator).await?;

        let upstream = branch.upstream();
        assert!(matches!(
            upstream,
            Some(Upstream::Remote { ref remote, ref branch, .. })
                if remote == "origin" && branch == "main"
        ));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_errors_setting_upstream_to_self() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let result = branch.set_upstream(&branch).perform(&operator).await;

        assert!(matches!(
            result,
            Err(SetUpstreamError::UpstreamIsItself { .. })
        ));

        Ok(())
    }
}
