//! The branch registry: where a branch's existence is recorded.
//!
//! A branch exists as a set of memory cells. The fact that it exists is
//! what makes it findable, and that fact lives in a branch of its own,
//! named [`REGISTRY`](crate::REGISTRY).
//!
//! # Why this lives here
//!
//! A branch records itself with `dialog.branch/*` facts, and the
//! `dialog.` namespace is reserved for machinery-written facts --
//! revision records, delegation records -- so that application writes
//! cannot forge them. Recording a branch is the same kind of write, so
//! it goes through the same [`machinery`](super::Commit::machinery)
//! scope, beside the delegation records that already use it.

use dialog_artifacts::{Changes, Entity, Statement};
use dialog_capability::{Capability, Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, Operator, OperatorExt as _};
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::{Output as _, Query, Term};
use futures_util::stream;

use crate::schema::{ActiveBranch, Branch as BranchConcept, BranchPull, BranchPush, Replica};
use crate::{Branch, CommitError, RemoteSite};

/// The environment a registry write runs against.
pub trait RegistryEnv:
    Provider<Get>
    + Provider<Put>
    + Provider<Import>
    + Provider<Resolve>
    + Provider<Publish>
    + Provider<Identify>
    + Provider<Attest>
    + Provider<crate::Hydrate>
    + Provider<dialog_artifacts::Preload>
    + Provider<dialog_artifacts::Speculation>
    + Provider<Fork<RemoteSite, Resolve>>
    + ConditionalSync
    + 'static
{
}

impl<T> RegistryEnv for T where
    T: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<crate::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static
{
}

/// Record `name` as a branch of the replica `operator` views.
///
/// Idempotent: the fact is derived from `(replica, name)`, so recording
/// the same branch twice converges on one record rather than
/// conflicting.
pub async fn record<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    name: &str,
    env: &Env,
) -> Result<(), CommitError> {
    write(registry, operator, name, env, Written::Asserted).await
}

/// Forget `name`, so it stops being listed.
///
/// The caller retracts the branch's cells first: this half is what a
/// listing reads, so retracting it last means a failure part-way leaves
/// cells nothing points at rather than a branch that lists but cannot
/// be opened.
pub async fn forget<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    name: &str,
    env: &Env,
) -> Result<(), CommitError> {
    write(registry, operator, name, env, Written::Retracted).await
}

/// Which way the record goes.
#[derive(Clone, Copy)]
enum Written {
    Asserted,
    Retracted,
}

async fn write<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    name: &str,
    env: &Env,
    written: Written,
) -> Result<(), CommitError> {
    let replica = Replica::new(operator.profile().clone(), registry.of().clone());
    let record = BranchConcept::new(&replica, name);

    let mut changes = Changes::new();
    match written {
        Written::Asserted => record.assert(&mut changes),
        Written::Retracted => record.retract(&mut changes),
    }

    apply(registry, changes, env).await
}

/// Switch the replica `operator` views to the branch `branch`.
///
/// Records which branch is active as a cardinality-one fact on the
/// replica, so switching again supersedes the previous one. The branch
/// is named by its entity rather than a name because it need not be on
/// this replica: it is not looked up, only pointed at.
pub async fn switch<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    branch: &Entity,
    env: &Env,
) -> Result<(), CommitError> {
    let replica = Replica::new(operator.profile().clone(), registry.of().clone());
    let active = ActiveBranch {
        this: replica.this,
        branch: branch.clone().into(),
    };

    let mut changes = Changes::new();
    active.assert(&mut changes);

    apply(registry, changes, env).await
}

pub(crate) fn pull(branch: &BranchConcept, upstream: &BranchConcept) -> BranchPull {
    BranchPull {
        this: branch.this.clone(),
        pull: upstream.this.clone().into(),
    }
}

pub(crate) fn push(branch: &BranchConcept, upstream: &BranchConcept) -> BranchPush {
    BranchPush {
        this: branch.this.clone(),
        push: upstream.this.clone().into(),
    }
}

/// Commit `changes` to the registry under the machinery scope, which
/// is what lets them write the reserved `dialog.` namespace.
pub(crate) async fn apply<Env: RegistryEnv>(
    registry: &Branch,
    changes: Changes,
    env: &Env,
) -> Result<(), CommitError> {
    let instructions = changes.into_instructions();
    if instructions.is_empty() {
        return Ok(());
    }

    Box::pin(
        registry
            .commit(stream::iter(instructions))
            .machinery()
            .allow_empty()
            .perform(env),
    )
    .await?;

    Ok(())
}

/// Every branch recorded on the replica `operator` views.
///
/// The registry itself comes back among them without ever having been
/// recorded: its fact is synthesized into the query's overlay, which is
/// what keeps a registry from having to exist before it can be created.
pub async fn list<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    env: &Env,
) -> Result<Vec<BranchConcept>, dialog_query::EvaluationError> {
    let replica = Replica::new(operator.profile().clone(), registry.of().clone());

    Box::pin(
        registry
            .query()
            .select(Query::<BranchConcept> {
                this: Term::var("this"),
                name: Term::var("name"),
                replica: replica.this.clone().into(),
            })
            .perform(env)
            .try_vec(),
    )
    .await
}

#[cfg(test)]
mod tests {

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_repo;
    use crate::schema::{Branch as BranchConcept, Replica};
    use crate::{REGISTRY, RepositoryMemoryExt};
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_capability::Subject;
    use dialog_effects::authority::Identify;
    use dialog_operator::helpers::test_operator_with_profile;
    use futures_util::StreamExt as _;

    /// A recorded branch is listed; the registry lists itself without
    /// ever having been recorded.
    #[dialog_common::test]
    async fn it_records_and_lists() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let identity = Identify.perform(&operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;

        super::record(&registry, &identity, "feature", &operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        let names: Vec<String> = super::list(&registry, &identity, &operator)
            .await?
            .into_iter()
            .map(|branch| branch.name.0)
            .collect();

        assert!(
            names.contains(&"feature".to_string()),
            "a recorded branch is listed: {names:?}"
        );
        assert!(
            names.contains(&REGISTRY.to_string()),
            "the registry lists itself: {names:?}"
        );

        Ok(())
    }

    /// The active branch is written under the name tonk reads it by,
    /// `dialog.replica/active-branch`, on the replica's entity. The
    /// name is the contract, so it is checked against the stored claim
    /// rather than through the concept that also defines it.
    #[dialog_common::test]
    async fn it_records_the_active_branch_by_name() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let identity = Identify.perform(&operator).await?;
        let replica = Replica::new(profile.did(), repo.did());
        let feature = BranchConcept::new(&replica, "feature").this;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .open()
            .perform(&operator)
            .await?;
        super::switch(&registry, &identity, &feature, &operator).await?;

        let registry = Subject::from(repo.did())
            .branch(REGISTRY)
            .load()
            .perform(&operator)
            .await?;
        let select = registry.claims().select(
            ArtifactSelector::new()
                .the("dialog.replica/active-branch".parse()?)
                .of(replica.this.clone()),
        );
        let store = crate::NetworkedIndex::new(&operator, select.catalog(), None);
        let rows: Vec<_> = select.execute(store).await?.collect::<Vec<_>>().await;

        assert_eq!(rows.len(), 1, "one active branch recorded: {}", rows.len());
        let artifact = rows.into_iter().next().expect("one row")?;
        assert_eq!(artifact.to_owned()?.is, Value::Entity(feature));
        Ok(())
    }
}
