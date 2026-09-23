//! Branch capability providers for Operator.
//!
//! A branch exists as a set of memory cells; the fact that it exists is
//! what makes it findable, and that fact lives in the repository's
//! registry branch (see [`REGISTRY`]).
//!
//! # Which half is authoritative
//!
//! Creating and deleting touch both halves, and the two cannot be one
//! compare-and-swap. The cell is the truth and the fact describes it,
//! so the cell is written first on create and removed first on delete,
//! with the fact following. A create that stops part-way leaves a
//! branch that exists but does not list; a delete that stops part-way
//! leaves one that lists but points at nothing. Repeating the same
//! operation finishes either.

use super::Operator;
use core::fmt::Display;
use dialog_capability::{Capability, Fork, Policy, Provider, Subject};
use dialog_common::Blake3Hash;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::Void;
use dialog_effects::archive::prelude::{ArchiveScope, GetBlockExt as _};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt as _};
use dialog_effects::branch::{self as branch_fx, BranchError, BranchRecord};
use dialog_effects::memory::{MemoryError, Publish, Resolve, Retract};
use dialog_effects::method;
use dialog_repository::registry::{active, forget, list, record, switch};
use dialog_repository::schema::{Branch as BranchConcept, Replica};
use dialog_repository::{
    Branch, EMPTY_TREE_HASH, PublishError, REGISTRY, RemoteSite, RepositoryMemoryExt, RetractError,
};

/// The environment a branch operation runs against.
///
/// Branch effects read and write a branch's cells and commit to the
/// registry, so they need the whole local write path rather than a
/// single provider.
pub trait BranchEnv:
    Provider<Get>
    + Provider<Put>
    + Provider<Import>
    + Provider<Resolve>
    + Provider<Publish>
    + Provider<Retract>
    + Provider<Identify>
    + Provider<Attest>
    + Provider<dialog_repository::Hydrate>
    + Provider<dialog_artifacts::Preload>
    + Provider<dialog_artifacts::Speculation>
    + Provider<Fork<RemoteSite, Resolve>>
    + ConditionalSync
    + 'static
{
}

impl<T> BranchEnv for T where
    T: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Retract>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<dialog_repository::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static
{
}

/// Turn any error into the branch domain's own, preserving its text.
fn failed(error: impl Display) -> BranchError {
    BranchError::Memory(MemoryError::Storage(error.to_string()))
}

impl<S> Operator<S>
where
    S: Clone,
    Self: BranchEnv,
{
    /// The registry branch for `subject`, held open by this operator.
    async fn registry(&self, subject: &dialog_varsig::Did) -> Result<Branch, BranchError> {
        Subject::from(subject.clone())
            .registry()
            .open()
            .perform(self)
            .await
            .map_err(failed)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<branch_fx::Create> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: BranchEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<branch_fx::Create>) -> Result<(), BranchError> {
        let subject = input.subject().clone();
        let name = branch_fx::Branch::<method::Put>::of(&input).name.clone();

        if let Some(reason) = branch_fx::invalid_name(&name) {
            return Err(BranchError::Refused {
                name,
                operation: "created",
                reason,
            });
        }
        if name == REGISTRY {
            return Err(BranchError::Refused {
                name,
                operation: "created",
                reason: "the registry describes itself and is never recorded",
            });
        }

        // The branch itself goes first: its cell is the truth, and the
        // registry fact only describes it. A crash after the publish
        // leaves a branch that exists but does not list, which a repeated
        // create converges with (the same revision publishes as a no-op
        // and the fact is recorded). The reverse order would leave a
        // branch that lists but points at nothing.
        if let Some(revision) = branch_fx::Create::of(&input).revision.clone() {
            // A branch points only at a revision whose signature holds and
            // whose tree this repository has: anything else points at
            // content nobody vouches for, or at nothing.
            if revision.verify().is_err() {
                return Err(BranchError::Refused {
                    name,
                    operation: "created",
                    reason: "the revision's signature does not hold",
                });
            }
            if *revision.tree.hash() != EMPTY_TREE_HASH {
                let tree = ArchiveScope::new(Subject::from(subject.clone()))
                    .index()
                    .read()
                    .get(Blake3Hash::from(*revision.tree.hash()))
                    .perform(self)
                    .await
                    .map_err(failed)?;
                if tree.is_none() {
                    return Err(BranchError::Refused {
                        name,
                        operation: "created",
                        reason: "the repository does not hold the revision's tree",
                    });
                }
            }
            // A fresh cell, never resolved: publishing with no expected
            // version is what refuses to move a branch that already
            // points at a different revision, while one that already
            // points at this revision converges.
            let cell = Subject::from(subject.clone())
                .branch(name.as_str())
                .revision();
            cell.publish(revision)
                .perform(self)
                .await
                .map_err(|error| match error {
                    PublishError::VersionMismatch { .. } => BranchError::Refused {
                        name: name.clone(),
                        operation: "created",
                        reason: "it already points at a different revision",
                    },
                    error => failed(error),
                })?;
        }

        // Only once the branch exists is it recorded. Through the
        // registry, which writes under the machinery scope:
        // `dialog.branch/*` is reserved, and an application write of it
        // is refused.
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject);
        record(&registry, &operator, name.as_str(), self)
            .await
            .map_err(failed)?;

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<branch_fx::List> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: BranchEnv + ConditionalSend,
{
    async fn execute(
        &self,
        input: Capability<branch_fx::List>,
    ) -> Result<Vec<BranchRecord>, BranchError> {
        let subject = input.subject().clone();
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject);

        let branches = list(&registry, &operator, self).await.map_err(failed)?;
        Ok(branches.into_iter().map(BranchRecord::from).collect())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<branch_fx::Delete> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: BranchEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<branch_fx::Delete>) -> Result<(), BranchError> {
        let subject = input.subject().clone();
        let name = branch_fx::Branch::<Void>::of(&input).name.clone();

        if let Some(reason) = branch_fx::invalid_name(&name) {
            return Err(BranchError::Refused {
                name,
                operation: "deleted",
                reason,
            });
        }
        if name == REGISTRY {
            return Err(BranchError::Refused {
                name,
                operation: "deleted",
                reason: "the registry holds every other branch's record",
            });
        }

        let expected = &branch_fx::Delete::of(&input).revision;
        let moved = || BranchError::Refused {
            name: name.clone(),
            operation: "deleted",
            reason: "it no longer points at the revision the delete expects",
        };

        // The branch the replica works on is not deleted out from under
        // it: switching away comes first.
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject.clone());
        let replica = Replica::new(operator.profile().clone(), subject.clone());
        let this = BranchConcept::new(&replica, name.as_str()).this;
        if active(&registry, &operator, self).await.map_err(failed)? == Some(this) {
            return Err(BranchError::Refused {
                name,
                operation: "deleted",
                reason: "it is the branch the replica has switched to",
            });
        }

        // The cells go first, the head before the rest. It is checked
        // against the revision the caller named: a branch that moved
        // since they last looked is not the branch they decided to
        // delete, and is left alone. A head that is already gone passes:
        // either the branch was empty, or an earlier delete got this far,
        // and in both there is no work left to lose.
        let reference = Subject::from(subject.clone()).branch(name.as_str());

        let revision = reference.revision();
        revision.resolve().perform(self).await.map_err(failed)?;
        match revision.content() {
            None => {}
            Some(head) if Some(&head) == expected.as_ref() => {
                // The retraction names the version just read, so a commit
                // that lands between the check above and this point is
                // refused by the store rather than destroyed.
                revision
                    .retract()
                    .perform(self)
                    .await
                    .map_err(|error| match error {
                        RetractError::VersionMismatch { .. } => moved(),
                        error => failed(error),
                    })?;
            }
            Some(_) => return Err(moved()),
        }

        let tracking = reference.tracking();
        tracking.resolve().perform(self).await.map_err(failed)?;
        if tracking.content().is_some() {
            tracking.retract().perform(self).await.map_err(failed)?;
        }

        let induction = reference.induction();
        induction.resolve().perform(self).await.map_err(failed)?;
        if induction.content().is_some() {
            induction.retract().perform(self).await.map_err(failed)?;
        }

        // And the fact goes last: the branch is gone before it stops
        // being listed, never the other way around.

        forget(&registry, &operator, name.as_str(), self)
            .await
            .map_err(failed)?;

        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<branch_fx::Switch> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: BranchEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<branch_fx::Switch>) -> Result<(), BranchError> {
        let subject = input.subject().clone();
        let branch = &branch_fx::Switch::of(&input).branch;

        // Only the record changes. The branch is pointed at, never
        // looked up: it may live on another replica, where there is
        // nothing here to find.
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject);
        switch(&registry, &operator, branch, self)
            .await
            .map_err(failed)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::Operator;
    use crate::helpers::{test_operator_with_profile, test_repo};
    use dialog_artifacts::{Artifact, Entity, Instruction, Value};
    use dialog_capability::identity::TreeReference;
    use dialog_capability::{Did, Subject};
    use dialog_effects::MethodExt as _;
    use dialog_effects::authority::{Identify, OperatorExt as _};
    use dialog_effects::branch::BranchRecord;
    use dialog_effects::branch::prelude::*;
    use dialog_query::{Output as _, Query, Term};
    use dialog_repository::schema::{ActiveBranch, Branch as BranchConcept, Replica};
    use dialog_repository::{REGISTRY, RepositoryMemoryExt as _, Revision};
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::stream;

    /// Mint a real head on `name` by committing nothing to it.
    async fn commit(
        operator: &Operator<VolatileSpace>,
        subject: &Did,
        name: &str,
    ) -> anyhow::Result<Revision> {
        let branch = Subject::from(subject.clone())
            .branch(name)
            .open()
            .perform(operator)
            .await?;
        Ok(branch
            .commit(stream::iter(Vec::<Instruction>::new()))
            .allow_empty()
            .perform(operator)
            .await?)
    }

    /// Where `name` points, as seen by opening it.
    async fn head(
        operator: &Operator<VolatileSpace>,
        subject: &Did,
        name: &str,
    ) -> anyhow::Result<Option<Revision>> {
        let branch = Subject::from(subject.clone())
            .branch(name)
            .open()
            .perform(operator)
            .await?;
        Ok(branch.revision())
    }

    /// The branch the replica has switched to, if any.
    async fn active(
        operator: &Operator<VolatileSpace>,
        subject: &Did,
    ) -> anyhow::Result<Vec<Entity>> {
        let identity = Identify.perform(operator).await?;
        let replica = Replica::new(identity.profile().clone(), subject.clone());
        let registry = Subject::from(subject.clone())
            .branch(REGISTRY)
            .open()
            .perform(operator)
            .await?;
        let rows: Vec<ActiveBranch> = registry
            .query()
            .select(Query::<ActiveBranch> {
                this: replica.this.into(),
                branch: Term::var("branch"),
            })
            .perform(operator)
            .try_vec()
            .await?;
        Ok(rows.into_iter().map(|row| row.branch.0).collect())
    }

    /// The entity of the branch `name` on the replica `operator` views.
    async fn entity(
        operator: &Operator<VolatileSpace>,
        subject: &Did,
        name: &str,
    ) -> anyhow::Result<Entity> {
        let identity = Identify.perform(operator).await?;
        let replica = Replica::new(identity.profile().clone(), subject.clone());
        Ok(BranchConcept::new(&replica, name).this)
    }

    async fn listed(
        operator: &Operator<VolatileSpace>,
        subject: &Did,
    ) -> anyhow::Result<Vec<String>> {
        Ok(Subject::from(subject.clone())
            .reader()
            .branches()
            .list()
            .perform(operator)
            .await?
            .into_iter()
            .map(|branch| branch.name)
            .collect())
    }

    /// Creating at a revision makes a branch that points at it -- one
    /// minted on another branch, the way a git branch points at any
    /// commit.
    #[dialog_common::test]
    async fn it_creates_a_branch_at_a_revision() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("feature")
            .create()
            .revision(source.clone())
            .perform(&operator)
            .await?;

        assert_eq!(head(&operator, &did, "feature").await?, Some(source));
        assert!(listed(&operator, &did).await?.contains(&"feature".into()));
        Ok(())
    }

    /// The first commit on a branch created from another mints onto the
    /// pointed-at revision: a head of its own, one edition on, so the
    /// fork stays connected to where it came from.
    #[dialog_common::test]
    async fn it_commits_onto_the_revision_a_branch_was_created_at() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("feature")
            .create()
            .revision(source.clone())
            .perform(&operator)
            .await?;
        let forked = commit(&operator, &did, "feature").await?;

        assert_ne!(forked, source, "the fork mints a head of its own");
        assert_eq!(forked.edition, source.edition.successor());
        assert_eq!(head(&operator, &did, "main").await?, Some(source));
        Ok(())
    }

    /// Without a revision the branch is created empty: recorded, with
    /// nothing to point at yet.
    #[dialog_common::test]
    async fn it_creates_an_empty_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("feature")
            .create()
            .perform(&operator)
            .await?;

        assert_eq!(head(&operator, &did, "feature").await?, None);
        let names = listed(&operator, &did).await?;
        assert!(names.contains(&"feature".into()), "{names:?}");
        assert!(
            names.contains(&REGISTRY.into()),
            "the registry lists itself: {names:?}"
        );
        Ok(())
    }

    /// Creating the same branch at the same revision twice converges.
    #[dialog_common::test]
    async fn it_creates_idempotently() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        for _ in 0..2 {
            Subject::from(did.clone())
                .writer()
                .branches()
                .branch("twice")
                .create()
                .revision(source.clone())
                .perform(&operator)
                .await?;
        }

        assert_eq!(head(&operator, &did, "twice").await?, Some(source));
        let names = listed(&operator, &did).await?;
        assert_eq!(names.iter().filter(|name| *name == "twice").count(), 1);
        Ok(())
    }

    /// A create never moves an existing branch: pointing it somewhere
    /// else is refused, and it stays where it was.
    #[dialog_common::test]
    async fn it_refuses_to_move_an_existing_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let first = commit(&operator, &did, "main").await?;
        let second = commit(&operator, &did, "main").await?;

        let create = |revision: Revision| {
            Subject::from(did.clone())
                .writer()
                .branches()
                .branch("feature")
                .create()
                .revision(revision)
        };
        create(first.clone()).perform(&operator).await?;
        let moved = create(second).perform(&operator).await;

        assert!(moved.is_err(), "a create over another revision is refused");
        assert_eq!(head(&operator, &did, "feature").await?, Some(first));
        Ok(())
    }

    /// The branch is published before it is recorded, so a create that
    /// fails to publish records nothing: it never lists a branch it did
    /// not make.
    #[dialog_common::test]
    async fn it_records_only_a_branch_it_created() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        // `main` exists as cells but was never created through the
        // capability, so it has no registry fact.
        let existing = commit(&operator, &did, "main").await?;
        let other = commit(&operator, &did, "scratch").await?;
        assert_ne!(existing, other);

        let refused = Subject::from(did.clone())
            .writer()
            .branches()
            .branch("main")
            .create()
            .revision(other)
            .perform(&operator)
            .await;

        assert!(refused.is_err());
        let names = listed(&operator, &did).await?;
        assert!(
            !names.contains(&"main".into()),
            "nothing recorded: {names:?}"
        );
        Ok(())
    }

    /// Deleting at the revision the branch points at removes it: it no
    /// longer opens to a head, and it is no longer listed.
    #[dialog_common::test]
    async fn it_deletes_a_branch_at_its_revision() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("doomed")
            .create()
            .revision(source.clone())
            .perform(&operator)
            .await?;
        Subject::from(did.clone())
            .voider()
            .branches()
            .branch("doomed")
            .delete(source)
            .perform(&operator)
            .await?;

        assert_eq!(head(&operator, &did, "doomed").await?, None);
        let names = listed(&operator, &did).await?;
        assert!(!names.contains(&"doomed".into()), "{names:?}");
        Ok(())
    }

    /// A branch that moved since the caller looked is not the branch
    /// they decided to delete: the delete is refused and the branch is
    /// left exactly as it was.
    #[dialog_common::test]
    async fn it_refuses_to_delete_a_branch_that_moved() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("feature")
            .create()
            .revision(source.clone())
            .perform(&operator)
            .await?;
        // Someone commits to it after the caller last looked.
        let advanced = commit(&operator, &did, "feature").await?;

        let refused = Subject::from(did.clone())
            .voider()
            .branches()
            .branch("feature")
            .delete(source)
            .perform(&operator)
            .await;

        assert!(
            refused.is_err(),
            "the delete names a revision it no longer holds"
        );
        assert_eq!(head(&operator, &did, "feature").await?, Some(advanced));
        assert!(listed(&operator, &did).await?.contains(&"feature".into()));
        Ok(())
    }

    /// A branch name is one path segment. The filesystem store resolves
    /// `./meta`, `meta/` and `x/../meta` to the registry's own cells,
    /// and `scratch/../main` reaches past a `scratch*` delegation, so a
    /// name that is not a single plain segment is refused before
    /// anything is written.
    #[dialog_common::test]
    async fn it_refuses_a_name_that_is_not_one_segment() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();

        for name in [
            "./meta",
            "meta/",
            "x/../meta",
            "scratch/../main",
            "a/b",
            "",
            ".",
            "..",
        ] {
            let created = Subject::from(did.clone())
                .writer()
                .branches()
                .branch(name)
                .create()
                .perform(&operator)
                .await;
            assert!(created.is_err(), "{name:?} is not a branch name");
        }
        let names = listed(&operator, &did).await?;
        assert_eq!(names, vec![REGISTRY.to_string()], "nothing was recorded");
        Ok(())
    }

    /// A delete that retracted the head and then stopped, before the
    /// branch was forgotten, is finished by deleting again: the branch
    /// no longer points at the revision the caller named, but only
    /// because this delete already moved it.
    #[dialog_common::test]
    async fn it_finishes_a_delete_that_stopped_part_way() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("doomed")
            .create()
            .revision(source.clone())
            .perform(&operator)
            .await?;

        // The first attempt got as far as retracting the head.
        let head_cell = Subject::from(did.clone()).branch("doomed").revision();
        head_cell.resolve().perform(&operator).await?;
        head_cell.retract().perform(&operator).await?;
        assert!(listed(&operator, &did).await?.contains(&"doomed".into()));

        Subject::from(did.clone())
            .voider()
            .branches()
            .branch("doomed")
            .delete(source)
            .perform(&operator)
            .await?;

        let names = listed(&operator, &did).await?;
        assert!(!names.contains(&"doomed".into()), "{names:?}");
        Ok(())
    }

    /// A branch created empty is deleted by expecting it to point at
    /// nothing.
    #[dialog_common::test]
    async fn it_deletes_an_empty_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("empty")
            .create()
            .perform(&operator)
            .await?;
        Subject::from(did.clone())
            .voider()
            .branches()
            .branch("empty")
            .delete(None)
            .perform(&operator)
            .await?;

        let names = listed(&operator, &did).await?;
        assert!(!names.contains(&"empty".into()), "{names:?}");
        Ok(())
    }

    /// The branch the replica has switched to is the one it works on, so
    /// it is not deleted out from under it: switch away first.
    #[dialog_common::test]
    async fn it_refuses_to_delete_the_active_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("feature")
            .create()
            .revision(source.clone())
            .perform(&operator)
            .await?;
        Subject::from(did.clone())
            .writer()
            .branches()
            .switch(entity(&operator, &did, "feature").await?)
            .perform(&operator)
            .await?;

        let deleted = Subject::from(did.clone())
            .voider()
            .branches()
            .branch("feature")
            .delete(source.clone())
            .perform(&operator)
            .await;
        assert!(deleted.is_err(), "the active branch was deleted");
        assert_eq!(head(&operator, &did, "feature").await?, Some(source));
        Ok(())
    }

    /// A branch is created at a revision only if the revision is signed
    /// by its issuer as it stands: a tampered one is refused.
    #[dialog_common::test]
    async fn it_refuses_to_create_at_a_tampered_revision() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let mut forged = commit(&operator, &did, "main").await?;
        forged.tree = TreeReference::default();

        let created = Subject::from(did.clone())
            .writer()
            .branches()
            .branch("forged")
            .create()
            .revision(forged)
            .perform(&operator)
            .await;
        assert!(created.is_err(), "a tampered revision was pointed at");
        assert_eq!(head(&operator, &did, "forged").await?, None);
        Ok(())
    }

    /// A branch is created at a revision only if the repository holds its
    /// tree: one minted in another repository points at nothing here.
    #[dialog_common::test]
    async fn it_refuses_to_create_at_a_revision_whose_tree_it_lacks() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let elsewhere = test_repo(&operator, &profile).await;
        let foreign = elsewhere
            .branch("main")
            .open()
            .perform(&operator)
            .await?
            .commit(stream::iter(vec![Instruction::Assert(Artifact {
                the: "user/name".parse()?,
                of: "user:elsewhere".parse()?,
                is: Value::String("Elsewhere".into()),
                cause: None,
            })]))
            .perform(&operator)
            .await?;

        let created = Subject::from(repo.did())
            .writer()
            .branches()
            .branch("borrowed")
            .create()
            .revision(foreign)
            .perform(&operator)
            .await;
        assert!(
            created.is_err(),
            "a revision whose tree is elsewhere was pointed at"
        );
        Ok(())
    }

    /// The registry holds every other branch's record, so it refuses to
    /// be created or deleted through the same capability.
    #[dialog_common::test]
    async fn it_refuses_to_touch_the_registry() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let source = commit(&operator, &did, "main").await?;

        let created = Subject::from(did.clone())
            .writer()
            .branches()
            .branch(REGISTRY)
            .create()
            .perform(&operator)
            .await;
        assert!(created.is_err(), "the registry is never created");

        let deleted = Subject::from(did.clone())
            .voider()
            .branches()
            .branch(REGISTRY)
            .delete(source)
            .perform(&operator)
            .await;
        assert!(deleted.is_err(), "the registry is never deleted");
        Ok(())
    }

    /// Switching records the branch as the replica's active one, and
    /// switching again replaces it rather than adding a second.
    #[dialog_common::test]
    async fn it_switches_the_active_branch() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        assert_eq!(active(&operator, &did).await?, Vec::<Entity>::new());

        let main = entity(&operator, &did, "main").await?;
        let feature = entity(&operator, &did, "feature").await?;
        for branch in [&main, &feature] {
            Subject::from(did.clone())
                .writer()
                .branches()
                .switch(branch.clone())
                .perform(&operator)
                .await?;
        }

        assert_eq!(active(&operator, &did).await?, vec![feature]);
        Ok(())
    }

    /// The branch is pointed at, not looked up: one that is not on this
    /// replica can be switched to.
    #[dialog_common::test]
    async fn it_switches_to_a_branch_it_does_not_hold() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();
        let elsewhere: Entity = "did:key:zElsewhere".parse()?;

        Subject::from(did.clone())
            .writer()
            .branches()
            .switch(elsewhere.clone())
            .perform(&operator)
            .await?;

        assert_eq!(active(&operator, &did).await?, vec![elsewhere]);
        Ok(())
    }

    /// Listing returns each branch as the registry records it: the
    /// entity derived from `(replica, name)`, its name, and its replica.
    #[dialog_common::test]
    async fn it_lists_branch_records() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let did = repo.did();

        Subject::from(did.clone())
            .writer()
            .branches()
            .branch("feature")
            .create()
            .perform(&operator)
            .await?;

        let identity = Identify.perform(&operator).await?;
        let replica = Replica::new(identity.profile().clone(), did.clone());
        let expected = BranchRecord::from(BranchConcept::new(&replica, "feature"));

        let records = Subject::from(did.clone())
            .reader()
            .branches()
            .list()
            .perform(&operator)
            .await?;
        assert!(records.contains(&expected), "{records:?}");
        assert!(
            records.iter().all(|record| record.replica == replica.this),
            "every branch is on this replica: {records:?}"
        );
        Ok(())
    }
}
