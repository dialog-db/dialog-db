use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

use super::{Branch, BranchReference};
use crate::repository::error::RepositoryError;

/// Command to load an existing branch, erroring if it has no revision yet.
pub struct LoadBranch {
    branch: BranchReference,
}

impl LoadBranch {
    /// Create from a branch reference.
    pub fn new(branch: BranchReference) -> Self {
        Self { branch }
    }

    /// Execute the load operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, RepositoryError>
    where
        Env: Provider<Resolve>,
    {
        let revision = self.branch.revision();
        revision.resolve().perform(env).await?;
        if revision.content().is_none() {
            return Err(RepositoryError::BranchNotFound {
                name: self.branch.name().to_string(),
            });
        }

        let upstream = self.branch.upstream();
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

    use anyhow::Result;
    use dialog_capability::Subject;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    use crate::repository::error::RepositoryError;
    use crate::repository::memory::RepositoryMemoryExt;

    #[dialog_common::test]
    async fn it_fails_loading_missing_branch() -> Result<()> {
        let provider = Volatile::new();

        let result = Subject::from(did!("key:zBranchLoadTest"))
            .branch("nonexistent")
            .load()
            .perform(&provider)
            .await;

        assert!(matches!(
            result,
            Err(RepositoryError::BranchNotFound { .. })
        ));
        Ok(())
    }
}
