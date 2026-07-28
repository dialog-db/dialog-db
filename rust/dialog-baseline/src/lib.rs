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
//! - **dialog-db** through the public [`Artifacts`] fact-store API
//!   (`commit` / `select`), over both the in-memory and the native
//!   filesystem storage backends.
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

pub mod se;

use std::str::FromStr;

use anyhow::Result;
use base58::ToBase58;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactStoreMut, Artifacts, Attribute, Entity, Instruction, Value,
};
use dialog_storage::{Blake3Hash, FileSystemStorageBackend, MemoryStorageBackend};
use futures_util::{TryStreamExt, stream};
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
/// seeded the same way `dialog-operator`'s `generate_data` seeds its
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

/// Where a dialog [`Artifacts`] store keeps its blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DialogMode {
    /// In-memory storage backend — CPU-isolation signal.
    Memory,
    /// Native filesystem backend in a fresh temp directory.
    Disk,
}

/// A dialog fact store over either storage backend, exposed through one
/// enum so benches treat both uniformly.
pub enum DialogFacts {
    /// Backed by [`MemoryStorageBackend`].
    Memory(Artifacts<MemoryStorageBackend<Blake3Hash, Vec<u8>>>),
    /// Backed by [`FileSystemStorageBackend`]; the temp dir lives as long
    /// as the store.
    Disk(
        Artifacts<FileSystemStorageBackend<Blake3Hash, Vec<u8>>>,
        TempDir,
    ),
}

impl DialogFacts {
    /// Open a fresh store in the given mode.
    pub async fn open(mode: DialogMode) -> Result<Self> {
        match mode {
            DialogMode::Memory => {
                let backend = MemoryStorageBackend::default();
                Ok(Self::Memory(Artifacts::anonymous(backend).await?))
            }
            DialogMode::Disk => {
                let dir = TempDir::new()?;
                let backend = FileSystemStorageBackend::new(dir.path()).await?;
                Ok(Self::Disk(Artifacts::anonymous(backend).await?, dir))
            }
        }
    }

    fn artifacts_for(row: &FactRow) -> Result<[Artifact; 2]> {
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

    /// Commit each row as its own transaction (the small-commit shape).
    pub async fn insert_per_row_transactions(&mut self, rows: &[FactRow]) -> Result<()> {
        for row in rows {
            let instructions = stream::iter(Self::artifacts_for(row)?.map(Instruction::Assert));
            match self {
                Self::Memory(artifacts) => artifacts.commit(instructions).await?,
                Self::Disk(artifacts, _) => artifacts.commit(instructions).await?,
            };
        }
        Ok(())
    }

    /// Commit every row in one transaction (the bulk-load shape).
    pub async fn insert_one_transaction(&mut self, rows: &[FactRow]) -> Result<()> {
        let mut instructions = Vec::with_capacity(rows.len() * 2);
        for row in rows {
            instructions.extend(Self::artifacts_for(row)?.map(Instruction::Assert));
        }
        match self {
            Self::Memory(artifacts) => artifacts.commit(stream::iter(instructions)).await?,
            Self::Disk(artifacts, _) => artifacts.commit(stream::iter(instructions)).await?,
        };
        Ok(())
    }

    pub(crate) async fn collect(
        &self,
        selector: ArtifactSelector<dialog_artifacts::selector::Constrained>,
    ) -> Result<Vec<Artifact>> {
        Ok(match self {
            Self::Memory(artifacts) => artifacts.select(selector).try_collect().await?,
            Self::Disk(artifacts, _) => artifacts.select(selector).try_collect().await?,
        })
    }

    /// Point lookup: the value of `(entity, stuff/name)`.
    pub async fn point_get(&self, entity: &str) -> Result<Option<Value>> {
        let selector = ArtifactSelector::new()
            .the(Attribute::from_str(NAME_ATTRIBUTE)?)
            .of(Entity::from_str(entity)?);
        Ok(self.collect(selector).await?.pop().map(|found| found.is))
    }

    /// Attribute scan: every `stuff/name` fact.
    pub async fn attribute_scan(&self) -> Result<usize> {
        let selector = ArtifactSelector::new().the(Attribute::from_str(NAME_ATTRIBUTE)?);
        Ok(self.collect(selector).await?.len())
    }

    /// Two-attribute hash join on the shared entity, at the fact-store
    /// layer: one AEV scan per attribute, joined in memory. This is the
    /// storage-layer ceiling for the `query_join` engine benchmark — the
    /// gap between this number and `query_join` is engine overhead.
    pub async fn join(&self) -> Result<usize> {
        let names = self
            .collect(ArtifactSelector::new().the(Attribute::from_str(NAME_ATTRIBUTE)?))
            .await?;
        let roles = self
            .collect(ArtifactSelector::new().the(Attribute::from_str(ROLE_ATTRIBUTE)?))
            .await?;
        let names_by_entity: std::collections::HashMap<String, Value> = names
            .into_iter()
            .map(|artifact| (artifact.of.to_string(), artifact.is))
            .collect();
        let mut count = 0;
        for role in roles {
            if names_by_entity.contains_key(&role.of.to_string()) {
                count += 1;
            }
        }
        Ok(count)
    }
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
        let mut store = DialogFacts::open(DialogMode::Memory).await?;
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
        let mut dialog = DialogFacts::open(DialogMode::Memory).await?;
        dialog.insert_per_row_transactions(&rows).await?;
        assert_eq!(sqlite.attribute_scan()?, dialog.attribute_scan().await?);
        assert_eq!(sqlite.join()?, dialog.join().await?);
        Ok(())
    }
}
