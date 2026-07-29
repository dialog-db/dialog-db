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

use std::path::PathBuf;

use anyhow::Result;
use dialog_capability::{Command, Fork, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::memory::{Publish, Resolve};
use dialog_effects::space::{Create as SpaceCreate, Load as SpaceLoad};
use dialog_effects::storage::Location;
use dialog_network::Network;
use dialog_operator::helpers::unique_name;
use dialog_operator::{Operator, Profile};
use dialog_repository::{Branch, RemoteSite, RepositoryExt as _};
use dialog_storage::provider::dcaa::fold_threshold_from_env;
use dialog_storage::provider::storage::{Storage, VolatileSpace};
use dialog_storage::provider::{Dcaa, FileSystemError, Space};
use dialog_storage::resource::Resource;
use dialog_storage::{NativeTempSpace, TempFileSystem};
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

/// A [`Dcaa`] provider whose [`Location`] is redirected into the platform
/// temp directory the same way [`TempFileSystem`] redirects, so the DCAA
/// benchmark rows write next to (and never over) the file-per-block rows.
/// The fold threshold is read from `DIALOG_DCAA_FOLD` at open (default 32;
/// 0 folds the delta chain on every commit). `DURABLE` selects the fsync
/// policy at the type level so the durable and relaxed rows can coexist
/// in one bench process without racing on env vars: `true` fsyncs every
/// commit, `false` skips it — the file-per-block archive's durability
/// level, isolating DCAA's non-fsync overhead.
#[derive(Clone, Debug)]
pub struct TempDcaa<const DURABLE: bool = true> {
    inner: Dcaa,
}

impl<const DURABLE: bool> TempDcaa<DURABLE> {
    /// The space root directory the archive files live under.
    pub fn root(&self) -> &PathBuf {
        self.inner.root()
    }
}

/// Forward every command the wrapped [`Dcaa`] can handle, mirroring
/// [`TempFileSystem`]'s forwarding impl.
#[async_trait::async_trait]
impl<const DURABLE: bool, C> Provider<C> for TempDcaa<DURABLE>
where
    C: Command,
    C::Input: ConditionalSync + 'static,
    Dcaa: Provider<C> + ConditionalSync,
{
    async fn execute(&self, input: C::Input) -> C::Output {
        self.inner.execute(input).await
    }
}

#[async_trait::async_trait]
impl<const DURABLE: bool> Resource<Location> for TempDcaa<DURABLE> {
    type Error = FileSystemError;

    async fn open(location: &Location) -> Result<Self, FileSystemError> {
        // Reuse TempFileSystem's redirection rule to compute the root,
        // then hand the path to the DCAA provider.
        let fs = TempFileSystem::open(location).await?;
        let root: PathBuf = fs.handle().try_into()?;
        Ok(Self {
            inner: Dcaa::configured(root, fold_threshold_from_env(), DURABLE),
        })
    }
}

/// A [`Space`] whose archive is a DCAA single-file store and whose other
/// fields (memory, credential, certificate, blob) stay on
/// [`TempFileSystem`] — the archive is the only concern DCAA covers.
pub type DcaaTempSpace =
    Space<TempDcaa, TempFileSystem, TempFileSystem, TempFileSystem, TempFileSystem>;

/// [`DcaaTempSpace`] with the per-commit fdatasync skipped: the durability
/// control that makes the DCAA vs file-per-block comparison
/// apples-to-apples.
pub type DcaaNosyncTempSpace =
    Space<TempDcaa<false>, TempFileSystem, TempFileSystem, TempFileSystem, TempFileSystem>;

/// A repository with one open branch, generic over the operator's space so
/// the volatile (in-memory) and temp (on-disk) variants share the workload
/// methods. Construct via [`DialogRepo::volatile`], [`DialogRepo::temp`],
/// or [`DialogRepo::dcaa`].
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

impl DialogRepo<Operator<DcaaTempSpace>> {
    /// Open a fresh repository whose block archive is a DCAA single-file
    /// store under the platform temp directory. Unlike `temp()`'s
    /// file-per-block archive, every commit here is DURABLE: the archive
    /// fsyncs once per `Import`/`Put`.
    pub async fn dcaa() -> Result<Self> {
        let storage: Storage<DcaaTempSpace> = Storage::new();
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

impl DialogRepo<Operator<DcaaNosyncTempSpace>> {
    /// Open a fresh DCAA-archived repository with the per-commit
    /// fdatasync SKIPPED — the durability control: same store, same
    /// bytes, same recovery, but only the file-per-block archive's
    /// crash guarantees. The delta against [`DialogRepo::dcaa`] is the
    /// price of durability; the delta against [`DialogRepo::temp`] is
    /// DCAA's non-fsync overhead.
    pub async fn dcaa_nosync() -> Result<Self> {
        let storage: Storage<DcaaNosyncTempSpace> = Storage::new();
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

    /// The DCAA-archived harness drives the same workload shapes through
    /// `Branch::commit` — pins the Space wiring the `repo_dcaa` bench
    /// configuration depends on.
    #[tokio::test]
    async fn it_commits_every_workload_shape_over_the_dcaa_archive() -> Result<()> {
        let repo = DialogRepo::dcaa().await?;
        let rows = generate_rows(3);
        repo.insert_per_row_transactions(&rows).await?;
        repo.insert_one_transaction(&generate_rows(5)[3..]).await?;
        repo.replay_se(&SeLog::synthetic(4)).await?;
        Ok(())
    }
}
