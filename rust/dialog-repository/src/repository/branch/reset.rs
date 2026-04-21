use dialog_capability::Provider;
use dialog_effects::memory::Publish;

use super::Branch;
use crate::PublishError;
use crate::repository::revision::Revision;

/// Command that resets a branch to a given revision.
pub struct Reset<'a> {
    branch: &'a Branch,
    revision: Revision,
}

impl<'a> Reset<'a> {
    fn new(branch: &'a Branch, revision: Revision) -> Self {
        Self { branch, revision }
    }
}

impl Branch {
    /// Create a command to reset the branch to a given revision.
    pub fn reset(&self, revision: Revision) -> Reset<'_> {
        Reset::new(self, revision)
    }
}

impl Reset<'_> {
    /// Execute the reset operation.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), PublishError>
    where
        Env: Provider<Publish>,
    {
        self.branch
            .revision
            .publish(self.revision)
            .perform(env)
            .await
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use std::collections::HashSet;

    use dialog_capability::Subject;
    use dialog_prolly_tree::EMPT_TREE_HASH;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    use crate::repository::memory::RepositoryMemoryExt;
    use crate::repository::revision::Revision;
    use crate::repository::tree::TreeReference;

    #[dialog_common::test]
    async fn it_sets_revision() -> Result<()> {
        let provider = Volatile::new();
        let subject = Subject::from(did!("key:zBranchResetTest"));

        let branch = subject.branch("main").open().perform(&provider).await?;
        assert!(branch.revision().is_none());

        let revision = Revision {
            subject: subject.did().clone(),
            issuer: subject.did().clone(),
            authority: subject.did().clone(),
            tree: TreeReference::from(EMPT_TREE_HASH),
            cause: HashSet::new(),
            period: 0,
            moment: 0,
        };
        branch.reset(revision.clone()).perform(&provider).await?;

        assert_eq!(branch.revision(), Some(revision));
        Ok(())
    }
}
