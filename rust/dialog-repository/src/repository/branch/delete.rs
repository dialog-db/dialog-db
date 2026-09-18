use dialog_capability::Provider;
use dialog_effects::memory::{Publish, Resolve, Retract as RetractCell};

use crate::{BranchReference, DeleteBranchError, RetractError};

/// Command to delete a branch: remove every cell that makes it exist.
///
/// A branch is nothing but its cells — the head revision, the upstream
/// tracking state, and this replica's induction watermark — so removing
/// them removes the branch. Its revisions are unaffected: they are
/// content-addressed blocks in the archive, reachable from any other head
/// that descends from them, and a branch that was merged elsewhere before
/// being deleted loses nothing. A branch that was *not* merged loses the
/// only reference to its work, which is exactly what deleting a branch
/// means.
///
/// Created by [`BranchReference::delete`].
///
/// # Not atomic, but idempotent
///
/// Each cell is removed in its own compare-and-set, so a failure partway
/// leaves the branch partly gone. Retrying is safe: a cell that is
/// already absent is skipped, so the retry finishes the job rather than
/// failing on what the first attempt achieved.
///
/// # What else may be pointing at it
///
/// Dialog cannot enumerate branches, so it cannot tell whether another
/// branch tracks this one as a local upstream. One that does keeps a
/// tracking entry naming a branch that is gone, and push attribution
/// reads that as *unknown provenance* — the sole-remote fast path is
/// skipped and existence probes decide instead (see
/// `notes/version-control.md`, invariant 3). That is the safe outcome and
/// costs one probe per by-reference frontier root; a caller that can
/// enumerate its branches, and would rather not pay it, should clear the
/// tracking entries first.
pub struct DeleteBranch {
    branch: BranchReference,
}

impl From<BranchReference> for DeleteBranch {
    fn from(branch: BranchReference) -> Self {
        Self { branch }
    }
}

impl DeleteBranch {
    /// Execute the delete.
    ///
    /// `Ok(())` means the branch holds no cells, whether this call
    /// removed them or found them already gone.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), DeleteBranchError>
    where
        Env: Provider<Resolve> + Provider<RetractCell> + Provider<Publish>,
    {
        // Head first. It is what every reader resolves, so a delete
        // interrupted after this point leaves a branch that answers as
        // empty rather than one that still serves data while its
        // tracking state is gone.
        let head = self.branch.revision();
        head.resolve().perform(env).await?;
        self.retract("revision", head.retract().perform(env).await)?;

        let upstream = self.branch.upstream();
        upstream.resolve().perform(env).await?;
        self.retract("upstream", upstream.retract().perform(env).await)?;

        let induction = self.branch.induction();
        induction.resolve().perform(env).await?;
        self.retract("induction", induction.retract().perform(env).await)?;

        Ok(())
    }

    /// Fold one cell's removal into the delete's verdict.
    ///
    /// A cell that was never written has no version to set against, which
    /// [`Cell::retract`](crate::Cell::retract) reports as
    /// [`RetractError::Unobserved`]. Here that is success, not failure: a
    /// branch with no upstream, or one that never induced, is a branch
    /// missing a cell we were about to remove anyway.
    fn retract(
        &self,
        cell: &'static str,
        result: Result<(), RetractError>,
    ) -> Result<(), DeleteBranchError> {
        match result {
            Ok(()) | Err(RetractError::Unobserved { .. }) => Ok(()),
            Err(source) => Err(DeleteBranchError::Retract {
                branch: self.branch.name().to_string(),
                cell,
                source,
            }),
        }
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use anyhow::Result;
    use dialog_artifacts::Entity;
    use dialog_operator::Operator;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::the;
    use dialog_storage::provider::storage::VolatileSpace;

    use crate::helpers::test_repo;
    use crate::{Branch, LoadBranchError};

    /// Commit one fact, so the branch has a head to remove.
    async fn write(branch: &Branch, operator: &Operator<VolatileSpace>, note: &str) -> Result<()> {
        branch
            .transaction()
            .assert(the!("test/note").of(Entity::new()?).is(note.to_string()))
            .commit()
            .publish()
            .perform(operator)
            .await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn it_removes_a_branch_that_has_a_head() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        write(&feature, &operator, "kept").await?;
        assert!(feature.revision().is_some());

        repo.branch("feature").delete().perform(&operator).await?;

        // A fresh handle sees an empty branch, and the load that
        // distinguishes "exists" from "never written to" says it is gone.
        let reopened = repo.branch("feature").open().perform(&operator).await?;
        assert!(reopened.revision().is_none());
        assert!(matches!(
            repo.branch("feature").load().perform(&operator).await,
            Err(LoadBranchError::NotFound { .. })
        ));
        Ok(())
    }

    #[dialog_common::test]
    async fn it_leaves_the_branches_it_was_not_asked_about() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        for name in ["main", "feature"] {
            let branch = repo.branch(name).open().perform(&operator).await?;
            write(&branch, &operator, name).await?;
        }
        let main_head = repo
            .branch("main")
            .open()
            .perform(&operator)
            .await?
            .revision();

        repo.branch("feature").delete().perform(&operator).await?;

        let main = repo.branch("main").open().perform(&operator).await?;
        assert_eq!(main.revision(), main_head);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_drops_the_upstream_along_with_the_branch() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let main = repo.branch("main").open().perform(&operator).await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        write(&feature, &operator, "tracked").await?;
        feature.set_upstream(&main).perform(&operator).await?;
        assert!(feature.upstream().is_some());

        repo.branch("feature").delete().perform(&operator).await?;

        // Not just the head: a branch whose tracking state outlived it
        // would come back pointing somewhere on its first commit.
        let reopened = repo.branch("feature").open().perform(&operator).await?;
        assert!(reopened.revision().is_none());
        assert!(reopened.upstream().is_none());
        Ok(())
    }

    /// A branch nothing was ever written to has no cells to remove, and
    /// deleting it is success rather than a not-found error — the same
    /// reading `open` gives it. This is also what makes a retry of a
    /// half-finished delete finish the job.
    #[dialog_common::test]
    async fn it_deletes_a_branch_that_was_never_written_to() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        repo.branch("untouched").delete().perform(&operator).await?;
        repo.branch("untouched").delete().perform(&operator).await?;
        Ok(())
    }

    #[dialog_common::test]
    async fn a_deleted_name_is_reusable_from_scratch() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;

        let feature = repo.branch("feature").open().perform(&operator).await?;
        write(&feature, &operator, "first life").await?;
        let before = feature.revision().expect("committed");

        repo.branch("feature").delete().perform(&operator).await?;

        // Re-created, the name commits from genesis rather than
        // resurrecting the head it had — which is what makes delete a
        // removal and not a rewind.
        let reborn = repo.branch("feature").open().perform(&operator).await?;
        write(&reborn, &operator, "second life").await?;
        let after = reborn.revision().expect("committed");
        assert_ne!(after.tree, before.tree);
        assert_eq!(after.edition, before.edition);
        Ok(())
    }
}
