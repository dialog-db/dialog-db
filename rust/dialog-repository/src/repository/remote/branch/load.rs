//! Command to load an existing remote branch.

use crate::{
    BranchReference, LoadRemoteBranchError, OpenRemoteBranch, RemoteBranch, RemoteRepository,
};
use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

/// Command to load an existing remote branch.
///
/// Behaves like [`OpenRemoteBranch`] but errors if the persisted
/// snapshot cache has no revision (the branch has never been fetched
/// locally).
pub struct LoadRemoteBranch {
    open: OpenRemoteBranch,
}

impl LoadRemoteBranch {
    /// Construct from an owned remote repository and a branch reference.
    pub(super) fn new(repository: RemoteRepository, branch: BranchReference) -> Self {
        Self {
            open: OpenRemoteBranch::new(repository, branch),
        }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<RemoteBranch, LoadRemoteBranchError>
    where
        Env: Provider<Resolve>,
    {
        let branch = self.open.perform(env).await?;
        if branch.revision().is_none() {
            return Err(LoadRemoteBranchError::NotFound {
                name: branch.name().to_string(),
            });
        }
        Ok(branch)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::LoadRemoteBranchError;
    use crate::helpers::{test_operator_with_profile, test_repo};
    use crate::repository::remote::SiteAddress;
    use anyhow::Result;
    use dialog_remote_s3::Address;

    fn test_site() -> SiteAddress {
        SiteAddress::S3(
            Address::builder("https://s3.us-east-1.amazonaws.com")
                .region("us-east-1")
                .bucket("bucket")
                .build()
                .unwrap(),
        )
    }

    #[dialog_common::test]
    async fn it_errors_loading_remote_branch_never_fetched() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let origin = repo
            .remote("origin")
            .create(test_site())
            .perform(&operator)
            .await?;

        let result = origin.branch("main").load().perform(&operator).await;
        assert!(
            matches!(result, Err(LoadRemoteBranchError::NotFound { .. })),
            "Load should fail with NotFound for never-fetched branch, got: {result:?}"
        );

        Ok(())
    }
}
