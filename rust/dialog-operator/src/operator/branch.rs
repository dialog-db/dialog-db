//! Branch capability providers for Operator.
//!
//! A branch exists as a set of memory cells; the fact that it exists is
//! what makes it findable, and that fact lives in the repository's
//! registry branch (see [`REGISTRY`]).
//!
//! # Which half is authoritative
//!
//! Creating and deleting touch both halves, and the two cannot be one
//! compare-and-swap. The fact is therefore written first on create and
//! retracted last on delete, which fixes the direction a partial
//! failure falls in: a crash leaves cells that a later create
//! overwrites harmlessly, never a branch that lists but cannot be
//! opened.

use super::Operator;
use core::fmt::Display;
use dialog_capability::{Capability, Fork, Policy, Provider, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::Void;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::OperatorExt as _;
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::branch::{self as branch_fx, BranchError};
use dialog_effects::memory::{MemoryError, Publish, Resolve, Retract};
use dialog_effects::method;
use dialog_query::{Output as _, Query, Term};
use dialog_repository::registry::{forget, record};
use dialog_repository::schema::{Branch as BranchConcept, Replica};
use dialog_repository::{Branch, REGISTRY, RemoteSite, RepositoryMemoryExt};

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
    /// Open the registry branch for `subject`.
    async fn registry(&self, subject: &dialog_varsig::Did) -> Result<Branch, BranchError> {
        Subject::from(subject.clone())
            .branch(REGISTRY)
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

        if name == REGISTRY {
            return Err(BranchError::Refused {
                name,
                operation: "created",
                reason: "the registry describes itself and is never recorded",
            });
        }

        // The fact goes in first: a crash after this leaves a branch
        // that lists and opens empty, which a later create converges
        // with. The reverse order would leave cells nothing points at.
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject);

        // Through the registry, which writes under the machinery scope:
        // `dialog.branch/*` is reserved, and an application write of it
        // is refused.
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
    ) -> Result<Vec<String>, BranchError> {
        let subject = input.subject().clone();
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject.clone());
        let replica = Replica::new(operator.profile().clone(), subject);

        // Every branch recorded on this replica. The registry itself
        // comes back among them without ever having been recorded --
        // its fact is synthesized into the query's overlay.
        let rows: Vec<BranchConcept> = Box::pin(
            registry
                .query()
                .select(Query::<BranchConcept> {
                    this: Term::var("this"),
                    name: Term::var("name"),
                    replica: replica.this.clone().into(),
                })
                .perform(self)
                .try_vec(),
        )
        .await
        .map_err(failed)?;

        let mut names: Vec<String> = rows.into_iter().map(|row| row.name.0).collect();
        names.sort();
        names.dedup();
        Ok(names)
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

        if name == REGISTRY {
            return Err(BranchError::Refused {
                name,
                operation: "deleted",
                reason: "the registry holds every other branch's record",
            });
        }

        // The cells go first. Each is resolved before it is retracted,
        // because a retraction names the version it removes -- a cell
        // this replica never read is one it must not destroy.
        let reference = Subject::from(subject.clone()).branch(name.as_str());

        let revision = reference.revision();
        revision.resolve().perform(self).await.map_err(failed)?;
        if revision.content().is_some() {
            revision.retract().perform(self).await.map_err(failed)?;
        }

        let upstream = reference.upstream();
        upstream.resolve().perform(self).await.map_err(failed)?;
        if upstream.content().is_some() {
            upstream.retract().perform(self).await.map_err(failed)?;
        }

        let induction = reference.induction();
        induction.resolve().perform(self).await.map_err(failed)?;
        if induction.content().is_some() {
            induction.retract().perform(self).await.map_err(failed)?;
        }

        // And the fact goes last, so a crash leaves cells nothing
        // points at rather than a branch that lists but cannot open.
        let registry = self.registry(&subject).await?;
        let operator = self.build_authority(subject);

        forget(&registry, &operator, name.as_str(), self)
            .await
            .map_err(failed)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_operator_with_profile, test_repo};
    use dialog_capability::Subject;
    use dialog_effects::MethodExt as _;
    use dialog_effects::branch::prelude::*;
    use dialog_repository::REGISTRY;

    /// A created branch is listed. The registry lists itself without
    /// ever having been recorded, because its fact is synthesized into
    /// the query's overlay rather than written.
    #[dialog_common::test]
    async fn it_creates_and_lists() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let subject = Subject::from(repo.did());

        subject
            .clone()
            .writer()
            .branches()
            .branch("feature")
            .create()
            .perform(&operator)
            .await?;

        let names = subject
            .clone()
            .reader()
            .branches()
            .list()
            .perform(&operator)
            .await?;

        assert!(
            names.contains(&"feature".to_string()),
            "a created branch is listed: {names:?}"
        );
        assert!(
            names.contains(&REGISTRY.to_string()),
            "the registry lists itself: {names:?}"
        );

        Ok(())
    }

    /// Creating is idempotent: the fact is derived from (replica, name),
    /// so recording the same branch twice converges on one record
    /// rather than listing it twice.
    #[dialog_common::test]
    async fn it_creates_idempotently() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let subject = Subject::from(repo.did());

        for _ in 0..2 {
            subject
                .clone()
                .writer()
                .branches()
                .branch("twice")
                .create()
                .perform(&operator)
                .await?;
        }

        let names = subject
            .clone()
            .reader()
            .branches()
            .list()
            .perform(&operator)
            .await?;

        assert_eq!(
            names.iter().filter(|name| *name == "twice").count(),
            1,
            "creating twice converges on one record: {names:?}"
        );

        Ok(())
    }

    /// Deleting retracts the fact, so the branch stops being listed.
    #[dialog_common::test]
    async fn it_deletes_and_forgets() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let subject = Subject::from(repo.did());

        subject
            .clone()
            .writer()
            .branches()
            .branch("doomed")
            .create()
            .perform(&operator)
            .await?;

        subject
            .clone()
            .voider()
            .branches()
            .branch("doomed")
            .delete()
            .perform(&operator)
            .await?;

        let names = subject
            .clone()
            .reader()
            .branches()
            .list()
            .perform(&operator)
            .await?;

        assert!(
            !names.contains(&"doomed".to_string()),
            "a deleted branch stops being listed: {names:?}"
        );

        Ok(())
    }

    /// The registry holds every other branch's record, so it refuses to
    /// be created or deleted through the same capability.
    #[dialog_common::test]
    async fn it_refuses_to_touch_the_registry() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let subject = Subject::from(repo.did());

        let created = subject
            .clone()
            .writer()
            .branches()
            .branch(REGISTRY)
            .create()
            .perform(&operator)
            .await;
        assert!(created.is_err(), "the registry is never created");

        let deleted = subject
            .clone()
            .voider()
            .branches()
            .branch(REGISTRY)
            .delete()
            .perform(&operator)
            .await;
        assert!(deleted.is_err(), "the registry is never deleted");

        Ok(())
    }
}
