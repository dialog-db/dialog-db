use dialog_capability::Provider;
use dialog_effects::memory as memory_fx;

use super::Branch;
use super::reference::BranchReference;
use super::upstream::UpstreamState;
use crate::repository::error::RepositoryError;
use crate::repository::memory::Cell;
use crate::repository::revision::Revision;

/// Command to open a branch, creating it with defaults if it doesn't exist.
pub struct OpenBranch {
    branch: BranchReference,
}

impl OpenBranch {
    /// Create from a branch reference.
    pub fn new(branch: BranchReference) -> Self {
        Self { branch }
    }

    /// Execute the open operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<memory_fx::Resolve> + Provider<memory_fx::Publish>,
    {
        let revision: Cell<Option<Revision>> = self.branch.cell("revision");
        revision.resolve().perform(env).await?;
        if revision.content().is_none() {
            revision.publish(None).perform(env).await?;
        }

        let upstream: Cell<Option<UpstreamState>> = self.branch.cell("upstream");
        upstream.resolve().perform(env).await?;

        Ok(Branch {
            reference: self.branch,
            revision,
            upstream,
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};

    #[dialog_common::test]
    async fn it_opens_new_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let branch = repo.branch("main").open().perform(&operator).await?;

        assert_eq!(branch.name().as_str(), "main");
        assert!(
            branch.revision().is_none(),
            "New branch should have no revision"
        );
        Ok(())
    }
}
