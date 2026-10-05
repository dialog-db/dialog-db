#![warn(missing_docs)]
// Native-only: SQLite links a bundled C library and the harness drives
// filesystem stores and a multi-threaded runtime, none of which exist on
// wasm32. The crate compiles to nothing there so the workspace's wasm
// builds are unaffected.
#![cfg(not(target_arch = "wasm32"))]

//! Reference benchmarks: dialog-db vs SQLite on identical workloads.
//!
//! SQLite is the bar dialog-db aims to clear for local performance while
//! keeping content-addressed storage for partial on-demand replication.
//! This crate pins that bar down with numbers instead of assumptions: the
//! same fact-shaped workload is run against
//!
//! - **SQLite** modeling the dialog information model faithfully: one
//!   `facts` table whose primary key is the EAV ordering, plus secondary
//!   AEV and VAE indexes — the exact three orderings `dialog-artifacts`
//!   maintains in its prolly tree.
//! - **dialog-db** through a repository branch ([`repo::DialogRepo`]):
//!   `commit` and `select`, over both volatile (in-memory) and native
//!   on-disk storage.
//!
//! The workload shape mirrors the existing `dialog-query` benches
//! (`seed_stuff`: each entity carries a `stuff/name` and a `stuff/role`)
//! so numbers line up across crates.
//!
//! Durability caveat, so comparisons stay honest: dialog's filesystem
//! backend does not fsync block writes today, so the closest SQLite
//! configuration is `synchronous=OFF` (the `sqlite_disk_nosync` variant).
//! The `sqlite_disk` variant uses WAL + `synchronous=NORMAL`, i.e. what a
//! production SQLite deployment would actually run, and is the number to
//! beat once dialog has an explicit durability story.

pub mod metered;
pub mod nodes;
pub mod repo;
pub mod se;

use std::str::FromStr;

use anyhow::Result;
use base58::ToBase58;
use dialog_artifacts::{Artifact, Attribute, Changes, Entity, Instruction, Update as _, Value};
use dialog_peer::{Peer, Session};
use dialog_storage::NativeTempSpace;
use dialog_storage::provider::storage::VolatileSpace;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rusqlite::Connection;
use tempfile::TempDir;

/// The attribute carrying an entity's name, mirroring
/// `dialog-query`'s `stuff/name`.
pub const NAME_ATTRIBUTE: &str = "stuff/name";

/// The attribute carrying an entity's role, mirroring
/// `dialog-query`'s `stuff/role`.
pub const ROLE_ATTRIBUTE: &str = "stuff/role";

/// Number of distinct role values; keeps the role attribute low-cardinality
/// the way real enum-ish attributes are.
pub const ROLE_COUNT: usize = 8;

/// One entity's worth of seeded facts, in both representations.
#[derive(Clone, Debug)]
pub struct FactRow {
    /// The entity identifier (`entity:<base58>`), identical across both
    /// stores.
    pub entity: String,
    /// The `stuff/name` value for this entity.
    pub name: String,
    /// The `stuff/role` value for this entity.
    pub role: String,
}

/// Deterministically generate `count` entities with a name and a role each,
/// seeded the same way `dialog-peer`'s `generate_data` seeds its
/// entities so runs are reproducible.
pub fn generate_rows(count: usize) -> Vec<FactRow> {
    let mut rng = ChaCha8Rng::from_seed([7u8; 32]);
    (0..count)
        .map(|i| FactRow {
            entity: format!("entity:{}", rng.r#gen::<[u8; 32]>().to_base58()),
            name: format!("name{i}"),
            role: format!("role{}", i % ROLE_COUNT),
        })
        .collect()
}

/// How the SQLite connection is persisted and synced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqliteMode {
    /// `:memory:` database — CPU-isolation signal.
    Memory,
    /// On-disk database, `journal_mode=WAL`, `synchronous=NORMAL`: the
    /// production-realistic configuration.
    Disk,
    /// On-disk database, `journal_mode=WAL`, `synchronous=OFF`: durability
    /// semantics equivalent to dialog's current fsync-free filesystem
    /// backend.
    DiskNoSync,
}

/// A SQLite fact store modeling dialog's information model: one row per
/// fact, EAV primary key, AEV + VAE secondary indexes.
pub struct SqliteFacts {
    connection: Connection,
    // Held so the on-disk database lives as long as the store.
    _dir: Option<TempDir>,
}

impl SqliteFacts {
    /// The underlying connection, for sibling workload modules.
    pub(crate) fn connection(&self) -> &Connection {
        &self.connection
    }

    /// The underlying connection, mutably, for sibling workload modules.
    pub(crate) fn connection_mut(&mut self) -> &mut Connection {
        &mut self.connection
    }

    /// Open a fresh store in the given mode with the schema applied.
    pub fn open(mode: SqliteMode) -> Result<Self> {
        let (connection, dir) = match mode {
            SqliteMode::Memory => (Connection::open_in_memory()?, None),
            SqliteMode::Disk | SqliteMode::DiskNoSync => {
                let dir = TempDir::new()?;
                let connection = Connection::open(dir.path().join("facts.sqlite"))?;
                connection.pragma_update(None, "journal_mode", "WAL")?;
                let synchronous = if mode == SqliteMode::DiskNoSync {
                    "OFF"
                } else {
                    "NORMAL"
                };
                connection.pragma_update(None, "synchronous", synchronous)?;
                (connection, Some(dir))
            }
        };
        connection.execute_batch(
            "CREATE TABLE facts (
                 the TEXT NOT NULL,
                 of  TEXT NOT NULL,
                 val TEXT NOT NULL,
                 PRIMARY KEY (of, the, val)
             ) WITHOUT ROWID;
             CREATE INDEX facts_aev ON facts (the, of, val);
             CREATE INDEX facts_vae ON facts (val, the, of);",
        )?;
        Ok(Self {
            connection,
            _dir: dir,
        })
    }

    /// Insert each row in its own transaction (the small-commit shape:
    /// real edits are 1-5 facts per transaction).
    pub fn insert_per_row_transactions(&mut self, rows: &[FactRow]) -> Result<()> {
        for row in rows {
            let tx = self.connection.transaction()?;
            {
                let mut statement =
                    tx.prepare_cached("INSERT INTO facts (the, of, val) VALUES (?1, ?2, ?3)")?;
                statement.execute((NAME_ATTRIBUTE, &row.entity, &row.name))?;
                statement.execute((ROLE_ATTRIBUTE, &row.entity, &row.role))?;
            }
            tx.commit()?;
        }
        Ok(())
    }

    /// Insert every row in one transaction (the bulk-load shape).
    pub fn insert_one_transaction(&mut self, rows: &[FactRow]) -> Result<()> {
        let tx = self.connection.transaction()?;
        {
            let mut statement =
                tx.prepare_cached("INSERT INTO facts (the, of, val) VALUES (?1, ?2, ?3)")?;
            for row in rows {
                statement.execute((NAME_ATTRIBUTE, &row.entity, &row.name))?;
                statement.execute((ROLE_ATTRIBUTE, &row.entity, &row.role))?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Point lookup: the value of `(entity, stuff/name)`.
    pub fn point_get(&self, entity: &str) -> Result<Option<String>> {
        let mut statement = self
            .connection
            .prepare_cached("SELECT val FROM facts WHERE of = ?1 AND the = ?2")?;
        let mut result = statement.query((entity, NAME_ATTRIBUTE))?;
        Ok(match result.next()? {
            Some(found) => Some(found.get(0)?),
            None => None,
        })
    }

    /// Attribute scan: every `(entity, value)` pair of `stuff/name`.
    pub fn attribute_scan(&self) -> Result<usize> {
        let mut statement = self
            .connection
            .prepare_cached("SELECT of, val FROM facts WHERE the = ?1")?;
        let mut count = 0;
        let mut result = statement.query((NAME_ATTRIBUTE,))?;
        while let Some(found) = result.next()? {
            let _entity: String = found.get(0)?;
            let _value: String = found.get(1)?;
            count += 1;
        }
        Ok(count)
    }

    /// Two-attribute join on the shared entity: `(entity, name, role)`
    /// tuples — the SQL statement of the `query_join` concept query.
    pub fn join(&self) -> Result<usize> {
        let mut statement = self.connection.prepare_cached(
            "SELECT n.of, n.val, r.val
             FROM facts n JOIN facts r ON n.of = r.of
             WHERE n.the = ?1 AND r.the = ?2",
        )?;
        let mut count = 0;
        let mut result = statement.query((NAME_ATTRIBUTE, ROLE_ATTRIBUTE))?;
        while let Some(found) = result.next()? {
            let _entity: String = found.get(0)?;
            let _name: String = found.get(1)?;
            let _role: String = found.get(2)?;
            count += 1;
        }
        Ok(count)
    }
}

/// Where a dialog branch keeps its blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DialogMode {
    /// Volatile (in-memory) storage: the CPU-isolation signal.
    Memory,
    /// Native storage in the platform temp directory: the real-latency
    /// signal.
    Disk,
}

/// A dialog branch over either storage, exposed through one enum so
/// benches and examples treat both uniformly.
pub enum DialogFacts {
    /// Over volatile storage.
    Memory(repo::DialogRepo<Peer<VolatileSpace, Session>>),
    /// Over native storage in the platform temp directory.
    Disk(repo::DialogRepo<Peer<NativeTempSpace, Session>>),
}

/// Runs `$body` with `$repo` bound to whichever branch `$facts` holds.
macro_rules! with_repo {
    ($facts:expr, $repo:ident => $body:expr) => {
        match $facts {
            DialogFacts::Memory($repo) => $body,
            DialogFacts::Disk($repo) => $body,
        }
    };
}

impl DialogFacts {
    /// Open a fresh branch in the given mode.
    pub async fn open(mode: DialogMode) -> Result<Self> {
        Ok(match mode {
            DialogMode::Memory => Self::Memory(repo::DialogRepo::volatile().await?),
            DialogMode::Disk => Self::Disk(repo::DialogRepo::temp().await?),
        })
    }

    /// Commit each row as its own commit (the small-commit shape).
    pub async fn insert_per_row_transactions(&self, rows: &[FactRow]) -> Result<()> {
        with_repo!(self, repo => repo.insert_per_row_transactions(rows).await)
    }

    /// Commit every row in one commit (the bulk-load shape).
    pub async fn insert_one_transaction(&self, rows: &[FactRow]) -> Result<()> {
        with_repo!(self, repo => repo.insert_one_transaction(rows).await)
    }

    /// Replay the Stack Exchange log, one commit per transaction.
    pub async fn replay_se(&self, log: &se::SeLog) -> Result<()> {
        with_repo!(self, repo => repo.replay_se(log).await)
    }

    /// Point lookup: the value of `(entity, stuff/name)`.
    pub async fn point_get(&self, entity: &str) -> Result<Option<Value>> {
        with_repo!(self, repo => repo.point_get(entity).await)
    }

    /// Attribute scan: every `stuff/name` fact.
    pub async fn attribute_scan(&self) -> Result<usize> {
        with_repo!(self, repo => repo.attribute_scan().await)
    }

    /// Two-attribute hash join on the shared entity.
    pub async fn join(&self) -> Result<usize> {
        with_repo!(self, repo => repo.join().await)
    }

    /// The current title of a post.
    pub async fn se_title(&self, post: &str) -> Result<Option<Value>> {
        with_repo!(self, repo => repo.se_title(post).await)
    }

    /// All entities whose `se.post/kind` is `kind`.
    pub async fn se_by_kind(&self, kind: &str) -> Result<usize> {
        with_repo!(self, repo => repo.se_by_kind(kind).await)
    }

    /// Every fact matching `selector`, materialized.
    pub async fn collect(
        &self,
        selector: dialog_artifacts::ArtifactSelector<dialog_artifacts::selector::Constrained>,
    ) -> Result<Vec<Artifact>> {
        with_repo!(self, repo => repo.collect(selector).await)
    }
}

/// The two facts a row stands for: its entity's name and role.
pub(crate) fn artifacts_for(row: &FactRow) -> Result<[Artifact; 2]> {
    let entity = Entity::from_str(&row.entity)?;
    Ok([
        Artifact {
            the: Attribute::from_str(NAME_ATTRIBUTE)?,
            of: entity.clone(),
            is: Value::String(row.name.clone()),
            cause: None,
        },
        Artifact {
            the: Attribute::from_str(ROLE_ATTRIBUTE)?,
            of: entity,
            is: Value::String(row.role.clone()),
            cause: None,
        },
    ])
}

/// The instructions asserting every row's facts, in row order.
pub(crate) fn instructions_for(rows: &[FactRow]) -> Result<Vec<Instruction>> {
    let mut instructions = Vec::with_capacity(rows.len() * 2);
    for row in rows {
        instructions.extend(
            artifacts_for(row)?
                .map(|artifact| Instruction::Assert(artifact, dialog_artifacts::Policy::All)),
        );
    }
    Ok(instructions)
}

/// The changes `instructions` make, as a transaction integrates them: the
/// form a staged commit (and its amends) takes its writes in.
pub fn changes_of(instructions: impl IntoIterator<Item = Instruction>) -> Changes {
    let mut changes = Changes::new();
    for instruction in instructions {
        match instruction {
            Instruction::Assert(fact, policy) => {
                changes.associate(fact.the, fact.of, fact.is, policy)
            }
            Instruction::Retract(fact) => changes.dissociate(fact.the, fact.of, fact.is),
        }
    }
    changes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_roundtrip() -> Result<()> {
        let rows = generate_rows(10);
        let mut store = SqliteFacts::open(SqliteMode::Memory)?;
        store.insert_one_transaction(&rows)?;
        assert_eq!(
            store.point_get(&rows[3].entity)?,
            Some(rows[3].name.clone())
        );
        assert_eq!(store.attribute_scan()?, 10);
        assert_eq!(store.join()?, 10);
        Ok(())
    }

    #[tokio::test]
    async fn dialog_roundtrip() -> Result<()> {
        let rows = generate_rows(10);
        let store = DialogFacts::open(DialogMode::Memory).await?;
        store.insert_one_transaction(&rows).await?;
        assert_eq!(
            store.point_get(&rows[3].entity).await?,
            Some(Value::String(rows[3].name.clone()))
        );
        assert_eq!(store.attribute_scan().await?, 10);
        assert_eq!(store.join().await?, 10);
        Ok(())
    }

    #[tokio::test]
    async fn stores_agree() -> Result<()> {
        let rows = generate_rows(25);
        let mut sqlite = SqliteFacts::open(SqliteMode::Memory)?;
        sqlite.insert_per_row_transactions(&rows)?;
        let dialog = DialogFacts::open(DialogMode::Memory).await?;
        dialog.insert_per_row_transactions(&rows).await?;
        assert_eq!(sqlite.attribute_scan()?, dialog.attribute_scan().await?);
        assert_eq!(sqlite.join()?, dialog.join().await?);
        Ok(())
    }
}
