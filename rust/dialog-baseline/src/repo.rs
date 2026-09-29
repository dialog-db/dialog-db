//! Repository-layer harness: the workloads driven through a branch of a
//! repository, the way an application stores facts.
//!
//! A branch commit adds version tagging, history claims, a signed revision
//! record, and head publication on top of the index writes, so these
//! numbers are what an application actually pays.
//!
//! Construction mirrors `dialog-query`'s `BenchEnv` (operator + repository +
//! branch over volatile or platform-temp storage), pared down to just the
//! commit path. The branch handle is opened once and held across commits —
//! the realistic application shape, which also keeps the branch-owned record
//! and node caches warm the way a running app would.
use std::str::FromStr;

use anyhow::Result;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::ArtifactTree;
use dialog_artifacts::{Artifact, ArtifactSelector, Attribute, Changes, Entity, Value};
use dialog_capability::{Fork, Provider, Subject};
use dialog_common::Blake3Hash as NodeHash;
use dialog_common::{ConditionalSync, Holds};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::memory::{List, Publish, Resolve};
use dialog_effects::space::{Create as SpaceCreate, Load as SpaceLoad};
use dialog_effects::storage::Location;
use dialog_peer::helpers::unique_name;
use dialog_peer::{Peer, Session};
use dialog_repository::LocalIndex;
use dialog_repository::{
    Branch, BranchReference, PeersEnv, RemoteSite, RepositoryExt as _, TransactionBatch,
    TransactionCommit,
};
use dialog_storage::NativeTempSpace;
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use futures_util::stream;
use futures_util::{StreamExt as _, TryStreamExt as _};

use crate::metered::{Meter, Metered, Tally};
use crate::se::{SeLog, se_instructions};
use crate::{
    FactRow, Instruction, NAME_ATTRIBUTE, ROLE_ATTRIBUTE, artifacts_for, instructions_for,
};

/// Remove every store under the temp storage base. Benchmark plumbing:
/// the `repo_*` disk rows create a fresh uniquely-named store per
/// iteration and nothing deletes them afterwards, so a long criterion run
/// otherwise accumulates gigabytes of dead stores. Benches call this in
/// their setup closures, keeping at most one live store on disk.
pub fn clean_temp_storage() {
    std::fs::remove_dir_all(dialog_storage::temp_storage_base()).ok();
}

/// A repository with one open branch, generic over the operator's space so
/// the volatile (in-memory) and temp (on-disk) variants share the workload
/// methods. Construct via [`DialogRepo::volatile`] or [`DialogRepo::temp`].
pub struct DialogRepo<Env> {
    operator: Env,
    branch: Branch,
}

/// A repository in memory, as [`DialogRepo::volatile`] opens it.
pub type VolatileRepo = DialogRepo<Peer<VolatileSpace, Session>>;

/// An in-memory repository whose archive block traffic a `M` observes, as
/// [`DialogRepo::metered`] opens it.
pub type MeteredRepo<M = Tally> = DialogRepo<Metered<Peer<VolatileSpace, Session>, M>>;

impl DialogRepo<Peer<VolatileSpace, Session>> {
    /// Open a fresh volatile (in-memory) repository — the CPU-isolation
    /// signal, like `dialog_mem`.
    pub async fn volatile() -> Result<Self> {
        Self::volatile_through(|session| session).await
    }
}

impl<M: Meter + Clone + 'static> DialogRepo<Metered<Peer<VolatileSpace, Session>, M>> {
    /// Open a fresh volatile repository whose archive block traffic `meter`
    /// observes.
    pub async fn metered(meter: M) -> Result<Self> {
        Self::volatile_through(|session| Metered::new(session, meter)).await
    }
}

impl DialogRepo<Peer<NativeTempSpace, Session>> {
    /// Open a fresh repository rooted in the platform temp directory — the
    /// real-latency signal, like `dialog_disk`.
    pub async fn temp() -> Result<Self> {
        let storage = dialog_peer::helpers::test_owned(Storage::temp()).await;
        let profile = dialog_peer::helpers::open_peer(
            storage.clone(),
            Location::profile(unique_name("baseline")),
        )
        .await?;
        let operator = profile
            .session(b"baseline")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        Self::assemble(operator, &profile).await
    }
}

impl<Env> DialogRepo<Env>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Import>
        + Provider<Resolve>
        + Provider<Publish>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<SpaceLoad>
        + Provider<SpaceCreate>
        + Provider<List>
        + PeersEnv
        + Provider<dialog_repository::Hydrate>
        + Provider<dialog_artifacts::Preload>
        + Provider<dialog_artifacts::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static,
{
    /// Open a fresh volatile (in-memory) repository, operating through
    /// what `wrap` makes of its session.
    pub async fn volatile_through(
        wrap: impl FnOnce(Peer<VolatileSpace, Session>) -> Env,
    ) -> Result<Self> {
        let storage = dialog_peer::helpers::test_storage().await;
        let profile = dialog_peer::helpers::open_peer(
            storage.clone(),
            Location::profile(unique_name("baseline")),
        )
        .await?;
        let session = profile
            .session(b"baseline")
            .space(profile.state())
            .allow(Subject::any())
            .await?;
        Self::assemble(wrap(session), &profile).await
    }

    /// Open the repository under `peer` and its `main` branch.
    async fn assemble<S: Clone>(operator: Env, profile: &Peer<S>) -> Result<Self> {
        let repo = profile
            .space(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok(Self { operator, branch })
    }

    /// Commit each row as its own branch commit (the small-commit shape).
    pub async fn insert_per_row_transactions(&self, rows: &[FactRow]) -> Result<()> {
        for row in rows {
            let instructions = stream::iter(artifacts_for(row)?.map(Instruction::Assert));
            self.branch
                .commit(instructions)
                .perform(&self.operator)
                .await?;
        }
        Ok(())
    }

    /// Commit every row in one branch commit (the bulk-load shape).
    pub async fn insert_one_transaction(&self, rows: &[FactRow]) -> Result<()> {
        let instructions = instructions_for(rows)?;
        self.branch
            .commit(stream::iter(instructions))
            .perform(&self.operator)
            .await?;
        Ok(())
    }

    /// The operator commits and reads run as.
    pub fn operator(&self) -> &Env {
        &self.operator
    }

    /// The branch every workload commits to.
    pub fn branch(&self) -> &Branch {
        &self.branch
    }

    /// The tree the branch head names.
    pub fn tree(&self) -> ArtifactTree {
        match self.branch.revision() {
            Some(revision) => ArtifactTree::from_hash(NodeHash::from(*revision.tree.hash())),
            None => ArtifactTree::empty(),
        }
    }

    /// Loads the branch's tree nodes and spilled values from its archive.
    pub fn index(&self) -> LocalIndex<'_, Env> {
        LocalIndex::new(&self.operator, self.branch.archive().index())
    }

    /// Every fact matching `selector`, materialized.
    pub async fn collect(&self, selector: ArtifactSelector<Constrained>) -> Result<Vec<Artifact>> {
        let rows = self
            .branch
            .claims()
            .select(selector)
            .to_owned()
            .perform(&self.operator)
            .await?;
        Ok(rows.try_collect().await?)
    }

    /// The `(entity, value)` pairs `selector` matches, materialized per row
    /// to exactly the degree the SQLite arms materialize theirs (a
    /// `String` per column read off the statement row).
    async fn scan_pairs(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<(String, Value)>> {
        let rows = self
            .branch
            .claims()
            .select(selector)
            .perform(&self.operator)
            .await?;
        futures_util::pin_mut!(rows);
        let mut pairs = Vec::new();
        while let Some(row) = rows.next().await {
            let row = row?;
            let parts = row.parts()?;
            let entity = String::from_utf8(parts.entity.to_vec())?;
            pairs.push((entity, row.value()?));
        }
        Ok(pairs)
    }

    /// Point lookup: the value of `(entity, stuff/name)`.
    pub async fn point_get(&self, entity: &str) -> Result<Option<Value>> {
        let selector = ArtifactSelector::new()
            .the(Attribute::from_str(NAME_ATTRIBUTE)?)
            .of(Entity::from_str(entity)?);
        Ok(self
            .scan_pairs(selector)
            .await?
            .pop()
            .map(|(_, value)| value))
    }

    /// Attribute scan: every `stuff/name` fact.
    pub async fn attribute_scan(&self) -> Result<usize> {
        let selector = ArtifactSelector::new().the(Attribute::from_str(NAME_ATTRIBUTE)?);
        Ok(self.scan_pairs(selector).await?.len())
    }

    /// Two-attribute hash join on the shared entity: one AEV scan per
    /// attribute, joined in memory. The storage-layer ceiling for the
    /// `query_join` engine benchmark.
    pub async fn join(&self) -> Result<usize> {
        let names = self
            .scan_pairs(ArtifactSelector::new().the(Attribute::from_str(NAME_ATTRIBUTE)?))
            .await?;
        let roles = self
            .scan_pairs(ArtifactSelector::new().the(Attribute::from_str(ROLE_ATTRIBUTE)?))
            .await?;
        let names_by_entity: std::collections::HashMap<String, Value> = names.into_iter().collect();
        Ok(roles
            .into_iter()
            .filter(|(entity, _)| names_by_entity.contains_key(entity))
            .count())
    }

    /// The current title of a post (point read of a superseded pair).
    pub async fn se_title(&self, post: &str) -> Result<Option<Value>> {
        let selector = ArtifactSelector::new()
            .the(Attribute::from_str("se.post/title")?)
            .of(Entity::from_str(post)?);
        Ok(self.collect(selector).await?.pop().map(|found| found.is))
    }

    /// All entities whose `se.post/kind` is `kind` (a VAE-indexed lookup).
    pub async fn se_by_kind(&self, kind: &str) -> Result<usize> {
        let selector = ArtifactSelector::new()
            .the(Attribute::from_str("se.post/kind")?)
            .is(Value::String(kind.to_owned()));
        Ok(self.collect(selector).await?.len())
    }

    /// The root of the tree the branch head names, if it has one.
    pub fn root(&self) -> Option<NodeHash> {
        self.branch
            .revision()
            .map(|revision| NodeHash::from(*revision.tree.hash()))
    }

    /// Publish a commit that changes nothing but flushes every write
    /// buffer, leaving the head on the canonical tree its facts determine.
    pub async fn canonicalize(&self) -> Result<()> {
        self.branch
            .commit(stream::iter(Vec::new()))
            .allow_empty()
            .canonicalize()
            .perform(&self.operator)
            .await?;
        Ok(())
    }

    /// A fresh handle on the branch: its caches start empty, the way a
    /// replica that has not read the tree yet starts.
    pub async fn reopen(&self) -> Result<Branch> {
        Ok(BranchReference::from(&self.branch)
            .load()
            .perform(&self.operator)
            .await?)
    }

    /// Every fact `branch` holds that matches `selector`, materialized.
    pub async fn collect_from(
        &self,
        branch: &Branch,
        selector: ArtifactSelector<Constrained>,
    ) -> Result<Vec<Artifact>> {
        let rows = branch
            .claims()
            .select(selector)
            .to_owned()
            .perform(&self.operator)
            .await?;
        Ok(rows.try_collect().await?)
    }

    /// Stages one link of a chain: with no `tip` it commits `changes`
    /// onto the branch head, otherwise it amends the tip with them, so a
    /// chain mints one version however many links it has. Nothing is
    /// published.
    pub async fn stage_link(
        &self,
        tip: Option<TransactionBatch>,
        changes: Changes,
        canonicalize: bool,
    ) -> Result<TransactionBatch> {
        fn canonical<Line>(commit: TransactionCommit<Line>, yes: bool) -> TransactionCommit<Line> {
            if yes { commit.canonicalize() } else { commit }
        }
        Ok(match tip {
            None => {
                let commit = self.branch.transaction().integrate(changes).commit();
                canonical(commit, canonicalize)
                    .perform(&self.operator)
                    .await?
            }
            Some(tip) => {
                let commit = tip.transaction().integrate(changes).commit().amend();
                canonical(commit, canonicalize)
                    .perform(&self.operator)
                    .await?
            }
        })
    }

    /// Stages `links` as one chain on the branch (see
    /// [`stage_link`](Self::stage_link)), canonicalizing every
    /// `canonicalize_every` links and after the last.
    pub async fn stage(
        &self,
        links: impl IntoIterator<Item = Changes>,
        canonicalize_every: usize,
    ) -> Result<TransactionBatch> {
        let mut links = links.into_iter().peekable();
        let mut tip: Option<TransactionBatch> = None;
        let mut staged = 0usize;
        while let Some(changes) = links.next() {
            staged += 1;
            let canonicalize = staged.is_multiple_of(canonicalize_every) || links.peek().is_none();
            tip = Some(self.stage_link(tip, changes, canonicalize).await?);
        }
        tip.ok_or_else(|| anyhow::anyhow!("there is nothing to stage"))
    }

    /// Stages the Stack Exchange log as one chain, one transaction per
    /// link. See [`DialogRepo::stage`].
    pub async fn stage_se(
        &self,
        log: &SeLog,
        canonicalize_every: usize,
    ) -> Result<TransactionBatch> {
        let links = log
            .transactions
            .iter()
            .map(|commit| se_instructions(commit).map(crate::changes_of))
            .collect::<Result<Vec<_>>>()?;
        self.stage(links, canonicalize_every).await
    }

    /// Publish `transactions` as branch commits, `group` transactions to
    /// a commit.
    pub async fn publish_grouped(
        &self,
        transactions: impl IntoIterator<Item = Vec<Instruction>>,
        group: usize,
    ) -> Result<()> {
        let mut pending = Vec::new();
        let mut gathered = 0usize;
        for transaction in transactions {
            pending.extend(transaction);
            gathered += 1;
            if gathered.is_multiple_of(group) {
                self.branch
                    .commit(stream::iter(std::mem::take(&mut pending)))
                    .perform(&self.operator)
                    .await?;
            }
        }
        if !pending.is_empty() {
            self.branch
                .commit(stream::iter(pending))
                .perform(&self.operator)
                .await?;
        }
        Ok(())
    }

    /// Publish the Stack Exchange log, `group` transactions to a commit.
    pub async fn publish_se_grouped(&self, log: &SeLog, group: usize) -> Result<()> {
        let transactions = log
            .transactions
            .iter()
            .map(|commit| se_instructions(commit))
            .collect::<Result<Vec<_>>>()?;
        self.publish_grouped(transactions, group).await
    }

    /// Replay the Stack Exchange log, one branch commit per transaction,
    /// with the exact instruction mapping [`DialogFacts::replay_se`] uses.
    pub async fn replay_se(&self, log: &SeLog) -> Result<()> {
        for commit in &log.transactions {
            let instructions = se_instructions(commit)?;
            self.branch
                .commit(stream::iter(instructions))
                .perform(&self.operator)
                .await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::DialogRepo;
    use crate::generate_rows;
    use crate::se::SeLog;
    use anyhow::Result;

    /// The harness drives every workload shape through `Branch::commit`
    /// without erroring — pins the operator/repository/branch assembly the
    /// `repo_*` bench configurations depend on.
    #[tokio::test]
    async fn it_commits_every_workload_shape_through_the_branch() -> Result<()> {
        let repo = DialogRepo::volatile().await?;
        let rows = generate_rows(3);
        repo.insert_per_row_transactions(&rows).await?;
        repo.insert_one_transaction(&generate_rows(5)[3..]).await?;
        repo.replay_se(&SeLog::synthetic(4)).await?;
        Ok(())
    }
}
