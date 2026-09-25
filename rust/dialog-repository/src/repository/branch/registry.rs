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
use dialog_capability::{Capability, Fork, Provider, Subject};
use dialog_common::{ConditionalSync, Holds};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, Operator, OperatorExt as _};
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::{Output as _, Query, Term};
use futures_util::stream;
use std::sync::Arc;

use crate::schema::{ActiveBranch, Branch as BranchConcept, BranchPull, BranchPush, Replica};
use crate::{Branch, CommitError, REGISTRY, RemoteSite, RepositoryMemoryExt as _, ResolveError};

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
    + Holds
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
        + Holds
        + ConditionalSync
        + 'static
{
}

/// The registry branch of a repository, addressed before it is opened.
///
/// Built by [`RepositoryMemoryExt::registry`](crate::RepositoryMemoryExt::registry).
#[derive(Debug, Clone)]
pub struct RegistryReference {
    subject: Subject,
}

impl RegistryReference {
    /// Address the registry branch of `subject`.
    pub fn new(subject: Subject) -> Self {
        Self { subject }
    }

    /// Open the registry branch, reusing the one the environment holds.
    pub fn open(self) -> OpenRegistry {
        OpenRegistry {
            subject: self.subject,
        }
    }
}

/// Command to open a repository's registry branch.
///
/// The environment holds the opened branch, so every command that reads
/// or writes the registry through the same environment shares one warm
/// handle and its caches. A held handle has its head re-read before it
/// is returned, so writes through other handles are seen.
#[derive(Debug, Clone)]
pub struct OpenRegistry {
    subject: Subject,
}

impl OpenRegistry {
    /// Execute against `env`.
    pub async fn perform<Env: RegistryEnv>(self, env: &Env) -> Result<Branch, ResolveError> {
        let key = held(&self.subject);
        let held = env
            .held(&key)
            .and_then(|held| held.downcast_ref::<Branch>().cloned());
        if let Some(registry) = held {
            registry.refresh(env).await?;
            return Ok(registry);
        }
        let registry = self.subject.branch(REGISTRY).open().perform(env).await?;
        env.hold(key, Arc::new(registry.clone()));
        Ok(registry)
    }
}

/// The key the registry branch of `subject` is held under.
fn held(subject: &Subject) -> String {
    format!("dialog.registry:{}", subject.did())
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

/// Forget `name`, so it stops being listed, along with every branch it
/// pulls from and pushes to: a branch created again under the name is a
/// new branch, and starts with none.
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
        Written::Retracted => {
            let (pulls, pushes) = relations(registry, &record, env).await?;
            for pull in pulls {
                pull.retract(&mut changes);
            }
            for push in pushes {
                push.retract(&mut changes);
            }
            record.retract(&mut changes);
        }
    }

    apply(registry, changes, env).await
}

/// Every pull and push relation recorded from `branch`.
async fn relations<Env: RegistryEnv>(
    registry: &Branch,
    branch: &BranchConcept,
    env: &Env,
) -> Result<(Vec<BranchPull>, Vec<BranchPush>), CommitError> {
    let pulls = Box::pin(
        registry
            .query()
            .select(Query::<BranchPull> {
                this: branch.this.clone().into(),
                pull: Term::var("pull"),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(|error| CommitError::Registry(error.to_string()))?;
    let pushes = Box::pin(
        registry
            .query()
            .select(Query::<BranchPush> {
                this: branch.this.clone().into(),
                push: Term::var("push"),
            })
            .perform(env)
            .try_vec(),
    )
    .await
    .map_err(|error| CommitError::Registry(error.to_string()))?;
    Ok((pulls, pushes))
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

/// The branch the replica `operator` views has switched to, if any.
pub async fn active<Env: RegistryEnv>(
    registry: &Branch,
    operator: &Capability<Operator>,
    env: &Env,
) -> Result<Option<Entity>, dialog_query::EvaluationError> {
    let replica = Replica::new(operator.profile().clone(), registry.of().clone());
    let rows: Vec<ActiveBranch> = Box::pin(
        registry
            .query()
            .select(Query::<ActiveBranch> {
                this: replica.this.into(),
                branch: Term::var("branch"),
            })
            .perform(env)
            .try_vec(),
    )
    .await?;
    Ok(rows.into_iter().next().map(|row| row.branch.0))
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
    use crate::{REGISTRY, RepositoryExt as _, RepositoryMemoryExt};
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_capability::Subject;
    use dialog_common::Holds as _;
    use dialog_effects::authority::Identify;
    use dialog_operator::helpers::{test_operator_with_profile, unique_name};
    use futures_util::StreamExt as _;

    /// Opening or loading a repository leaves its registry held open, so
    /// listing branches and resolving upstreams start from a warm branch.
    /// Creating one does not: a new repository's registry is empty.
    #[dialog_common::test]
    async fn it_is_held_once_its_repository_is_opened() -> anyhow::Result<()> {
        let (operator, profile) = test_operator_with_profile().await;

        let name = unique_name("opened");
        let created = profile
            .repository(name.clone())
            .create()
            .perform(&operator)
            .await?;
        assert!(operator.held(&super::held(&created.subject())).is_none());
        let opened = profile.repository(name).open().perform(&operator).await?;
        assert!(operator.held(&super::held(&opened.subject())).is_some());

        let name = unique_name("loaded");
        profile
            .repository(name.clone())
            .create()
            .perform(&operator)
            .await?;
        let loaded = profile.repository(name).load().perform(&operator).await?;
        assert!(operator.held(&super::held(&loaded.subject())).is_some());
        Ok(())
    }

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
