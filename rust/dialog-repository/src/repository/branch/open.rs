use dialog_capability::Provider;
use dialog_effects::memory::Resolve;

use super::{Branch, BranchReference};
use crate::repository::error::RepositoryError;

/// Command to open a branch. Resolves the branch's revision and upstream
/// cells without ever erroring on a missing revision — a freshly-opened
/// branch that has never been committed to simply has `None` revision.
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
        Env: Provider<Resolve>,
    {
        let revision = self.branch.revision();
        revision.resolve().perform(env).await?;

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

    use crate::repository::memory::RepositoryMemoryExt;

    #[dialog_common::test]
    async fn it_opens_branch_with_no_revision() -> Result<()> {
        let provider = Volatile::new();
        let branch = Subject::from(did!("key:zBranchOpenTest"))
            .branch("main")
            .open()
            .perform(&provider)
            .await?;

        assert_eq!(branch.name(), "main");
        assert!(branch.revision().is_none());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_reopens_same_branch() -> Result<()> {
        let provider = Volatile::new();
        let subject = Subject::from(did!("key:zBranchReopenTest"));

        subject.branch("main").open().perform(&provider).await?;
        let branch = subject.branch("main").open().perform(&provider).await?;

        assert_eq!(branch.name(), "main");
        Ok(())
    }
}
