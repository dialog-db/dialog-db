//! Repository-layer write harness: the same workloads driven through
//! dialog-repository's `Branch::commit`.
//!
//! Real applications do not write through the raw [`Artifacts`] fact store;
//! they commit through a branch, which adds version tagging, history claims,
//! a signed revision record, and head publication on top of the same index
//! writes. The `repo_*` benchmark configurations measure that surface, so
//! the scoreboard shows both the storage layer and what an application
//! actually pays.
//!
//! Construction mirrors `dialog-query`'s `BenchEnv` (operator + repository +
//! branch over volatile or platform-temp storage), pared down to just the
//! commit path. The branch handle is opened once and held across commits —
//! the realistic application shape, which also keeps the branch-owned record
//! and node caches warm the way a running app would.
//!
//! [`Artifacts`]: dialog_artifacts::Artifacts

use anyhow::Result;
use dialog_capability::{Fork, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::memory::{Publish, Resolve};
use dialog_effects::space::{Create as SpaceCreate, Load as SpaceLoad};
use dialog_network::Network;
use dialog_operator::helpers::unique_name;
use dialog_operator::{DeriveOperator as _, Operator, Profile};
use dialog_repository::{Branch, RemoteSite, RepositoryExt as _};
use dialog_storage::NativeTempSpace;
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use futures_util::stream;

use crate::se::{SeLog, se_instructions};
use crate::{DialogFacts, FactRow, Instruction};

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

impl DialogRepo<Operator<VolatileSpace>> {
    /// Open a fresh volatile (in-memory) repository — the CPU-isolation
    /// signal, like `dialog_mem`.
    pub async fn volatile() -> Result<Self> {
        let storage = Storage::volatile();
        let profile = Profile::open(unique_name("baseline"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"baseline")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
            .await?;
        Self::assemble(operator, &profile).await
    }
}

impl DialogRepo<Operator<NativeTempSpace>> {
    /// Open a fresh repository rooted in the platform temp directory — the
    /// real-latency signal, like `dialog_disk`.
    pub async fn temp() -> Result<Self> {
        let storage = Storage::temp();
        let profile = Profile::open(unique_name("baseline"))
            .perform(&storage)
            .await?;
        let operator = profile
            .derive(b"baseline")
            .allow(Subject::any())
            .network(Network::default())
            .build(storage)
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
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    /// Open the repository under `profile` and its `main` branch.
    async fn assemble(operator: Env, profile: &Profile) -> Result<Self> {
        let repo = profile
            .repository(unique_name("repo"))
            .open()
            .perform(&operator)
            .await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        Ok(Self { operator, branch })
    }

    /// Commit each row as its own branch commit (the small-commit shape).
    pub async fn insert_per_row_transactions(&self, rows: &[FactRow]) -> Result<()> {
        for row in rows {
            let instructions =
                stream::iter(DialogFacts::artifacts_for(row)?.map(Instruction::Assert));
            self.branch
                .commit(instructions)
                .perform(&self.operator)
                .await?;
        }
        Ok(())
    }

    /// Commit every row in one branch commit (the bulk-load shape).
    pub async fn insert_one_transaction(&self, rows: &[FactRow]) -> Result<()> {
        let mut instructions = Vec::with_capacity(rows.len() * 2);
        for row in rows {
            instructions.extend(DialogFacts::artifacts_for(row)?.map(Instruction::Assert));
        }
        self.branch
            .commit(stream::iter(instructions))
            .perform(&self.operator)
            .await?;
        Ok(())
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
