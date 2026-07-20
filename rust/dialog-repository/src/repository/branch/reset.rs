use dialog_capability::Provider;
use dialog_effects::memory::Publish;

use crate::{Branch, PublishError, Revision};

/// Command that resets a branch to a given revision.
///
/// Reset exists for fast-forward bookkeeping (advancing a cell to a head
/// established elsewhere, e.g. by a push), **not for rewind**. It is an
/// unconditional cell publish with no ancestry check: resetting a branch
/// BACKWARDS and then committing re-mints an already-used `(origin,
/// edition)` version — the protocol corruption rule 1 of
/// `notes/version-control.md` forbids. Peers whose watermark already
/// observes that version will silently drop the re-minted revision's
/// writes, and replicas holding the two same-version revisions diverge to
/// a content-hash tie-break. If a branch must move backwards, mint new
/// history (retract/replace forward) instead of rewinding the head.
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
    ///
    /// Fast-forward bookkeeping only — see [`Reset`] for why rewinding a
    /// branch backwards and committing corrupts the version protocol.
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

    use dialog_artifacts::history::Edition;
    use dialog_capability::Subject;
    use dialog_storage::provider::Volatile;
    use dialog_varsig::did;

    use crate::{EMPTY_TREE_HASH, RepositoryMemoryExt, Revision, TreeReference};

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
            branch: "main".into(),
            tree: TreeReference::from(EMPTY_TREE_HASH),
            cause: HashSet::new(),
            edition: Edition::GENESIS,
            context: None,
            signature: Vec::new(),
        };
        branch.reset(revision.clone()).perform(&provider).await?;

        assert_eq!(branch.revision(), Some(revision));
        Ok(())
    }
}
