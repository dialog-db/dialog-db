mod artifact;
pub use artifact::*;

mod revision;
pub use revision::*;

mod data;
pub use data::*;

mod instruction;
pub use instruction::*;

pub mod selector;
pub use selector::{ArtifactSelector, ValueBound};

mod query;
pub use query::{ArtifactStream, Select};

mod store;
pub use store::*;

mod update;
pub use update::{Change, ChangeStream, Changes, SortKey, Statement, Update, sort_key};

mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

mod ordkey;
pub use ordkey::*;

mod ordvalue;
pub use ordvalue::*;

mod cause;
pub use cause::*;

mod r#match;
pub use r#match::*;

use base58::ToBase58;
use rand::{Rng, distributions::Alphanumeric};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{Blake3Hash as NodeHash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_search_tree::{Buffer as TreeBuffer, ContentAddressedStorage as TreeStorage, Delta};
pub use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder, HashType,
    MemoryStorageBackend, Storage, StorageBackend,
};
use futures_util::{Stream, StreamExt, TryStreamExt, stream};
use std::sync::Arc;
use tokio::sync::RwLock;

#[cfg(feature = "csv")]
use async_stream::stream;

#[cfg(feature = "csv")]
use tokio::io::{AsyncRead, AsyncWrite};

#[cfg(feature = "csv")]
use crate::{EntityKey, KeyViewConstruct};

use crate::tree::{
    ArtifactTree, ArtifactTreeExt, SpillCache, TreeStorageBridge, fetch_spilled_cached, spill_cache,
};
use crate::{
    DialogArtifactsError, HASH_SIZE, Key, State, artifacts::selector::Constrained, make_reference,
};

/// An alias type that describes the search tree that is used for each
/// index in [`Artifacts`]. Kept generic-free: keys are raw fixed-size key
/// bytes and values are CBOR-encoded [`State`] blocks (see
/// [`crate::tree`]).
pub type Index = ArtifactTree;

/// Maximum number of concurrent block writes when persisting the index's
/// pending nodes. Blocks are content-addressed and independent, so their
/// write order doesn't matter; only completion before the root is
/// referenced does.
const FLUSH_CONCURRENCY: usize = 16;

/// [`Artifacts`] is an implementor of [`ArtifactStore`] and [`ArtifactStoreMut`].
/// Internally, [`Artifacts`] maintains indexes built from [`Tree`]s (that is,
/// prolly trees). These indexes are built up as new [`Artifact`]s are commited,
/// and they are chosen based on [`ArtifactSelector`] shapes when [`Artifact`]s are
/// queried.
///
/// [`Artifacts`] are backed by a concrete implementation of [`StorageBackend`].
/// The user-provided [`StorageBackend`] is paired with a [`BasicEncoder`] to
/// produce a [`ContentAddressedStorage`] that is suitable for storing and
/// retrieving facts.
///
/// See the crate-level documentation for an example of usage.
#[derive(Clone)]
pub struct Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    identifier: String,
    storage: Storage<CborEncoder, Backend>,
    index: Arc<RwLock<Index>>,
    /// Caches spilled value blocks across selects so a repeated read of the
    /// same large value skips the store fetch. Content-addressed, so it never
    /// serves stale bytes.
    spill_cache: SpillCache,
}

impl<Backend> Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    #[cfg(feature = "debug")]
    /// Get a reference-counted pointer to the internal entity index of the [`Artifacts`]
    pub fn index(&self) -> Arc<RwLock<Index>> {
        self.index.clone()
    }

    #[cfg(feature = "debug")]
    /// Get a clone of the storage used by this [`Artifacts`]
    pub fn storage(&self) -> Storage<CborEncoder, Backend> {
        self.storage.clone()
    }

    /// The name used to uniquely identify the data of this [`Artifacts`]
    /// instance
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Initialize a new [`Artifacts`] with the provided [`StorageBackend`].
    pub async fn open(identifier: String, backend: Backend) -> Result<Self, DialogArtifactsError> {
        let storage = Storage {
            encoder: CborEncoder,
            backend: backend.clone(),
        };

        // TODO: We probably want to enforce some namespacing within storage so
        // that generic K/V storage can go e.g., in a different IDB store or a
        // different folder on the FS
        let index = {
            let revision_block = storage.get(&make_reference(identifier.as_bytes())).await?;
            let revision = if let Some(revision_hash_bytes) = revision_block {
                // Check if the revision is NULL_REVISION_HASH
                if revision_hash_bytes == NULL_REVISION_HASH {
                    None
                } else {
                    // For actual revisions, read the revision from storage
                    let hash = Blake3Hash::try_from(revision_hash_bytes).map_err(|bytes| {
                        DialogArtifactsError::InvalidRevision(format!(
                            "Incorrect byte length (expected {HASH_SIZE}, received {})",
                            bytes.len()
                        ))
                    })?;

                    storage.read::<Revision>(&hash).await?
                }
            } else {
                None
            };

            if let Some(revision) = revision {
                ArtifactTree::from_hash(NodeHash::from(*revision.index()))
            } else {
                ArtifactTree::empty()
            }
        };

        Ok(Self {
            identifier,
            storage,
            index: Arc::new(RwLock::new(index)),
            spill_cache: spill_cache(),
        })
    }

    /// Initialize a new, empty [`Artifacts`] with a randomly generated
    /// identifier
    pub async fn anonymous(backend: Backend) -> Result<Self, DialogArtifactsError> {
        let identifier = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(32)
            .map(char::from)
            .collect();

        Self::open(identifier, backend).await
    }

    #[cfg(feature = "csv")]
    /// Export the database in CSV format. Each row will consist of the data
    /// from a single [`Artifact`], laid out in {attribute, entity, value,
    /// cause} order.
    // TODO: It would be cool if we could export (and maybe even import?) based
    // on pattern matching.
    pub async fn export<Write>(&self, write: &mut Write) -> Result<(), DialogArtifactsError>
    where
        Write: AsyncWrite + Unpin,
    {
        let mut csv = csv_async::AsyncSerializer::from_writer(write);

        let index = self.index.read().await;
        let range = <EntityKey<Key> as KeyViewConstruct>::min().into_key()
            ..=<EntityKey<Key> as KeyViewConstruct>::max().into_key();
        let tree_storage = TreeStorage::new(TreeStorageBridge(self.storage.clone()));
        let entity_stream = index.stream_range(range, &tree_storage);

        tokio::pin!(entity_stream);

        while let Some(entry) = entity_stream.try_next().await? {
            if let State::Added(datum) = &entry.value {
                let spilled =
                    fetch_spilled_cached(&self.storage, &self.spill_cache, &entry.key).await?;
                let artifact = Artifact::from_key_datum_with_value(&entry.key, datum, spilled)?;

                csv.serialize(artifact)
                    .await
                    .map_err(|error| DialogArtifactsError::Export(format!("{error}")))?;
            }
        }

        Ok(())
    }

    #[cfg(feature = "csv")]
    /// Import data from a CSV laid out like the one produced by
    /// [`Artifacts::export`]
    pub async fn import<Read>(&mut self, read: &mut Read) -> Result<(), DialogArtifactsError>
    where
        // bare-send-ok: csv_async bounds its readers on real Send on every target
        Read: AsyncRead + Unpin + Send,
    {
        let instructions = stream! {
            let mut reader = csv_async::AsyncReaderBuilder::new()
                .create_deserializer(read);

            let stream = reader.deserialize::<Artifact>();

            for await artifact in stream {
                match artifact {
                    Ok(artifact) => {
                        yield Instruction::Assert(artifact)
                    }
                    Err(error) => {
                        println!("Skipping invalid datum: {error}");
                    }
                }
            }
        };

        ArtifactStoreMut::commit(self, instructions).await?;

        Ok(())
    }

    /// Get the hash that represents the [`ArtifactStore`] at its current version.
    pub async fn revision(&self) -> Result<Blake3Hash, DialogArtifactsError> {
        let index = self.index.read().await;

        let root = index.root();
        Ok(if root == NULL_BLAKE3_HASH {
            NULL_REVISION_HASH
        } else {
            Revision::new(root.as_bytes()).as_reference().await?
        })
    }

    /// Reset the root of the database to `revision_hash` if provided, or else reset
    /// to the stored root if available, or else to an empty database.
    pub async fn reset(
        &mut self,
        revision_hash: Option<Blake3Hash>,
    ) -> Result<(), DialogArtifactsError> {
        // Determine target revision we are resetting to
        let required_hash = match revision_hash {
            // If a specific revision hash is provided, use it
            Some(hash) => hash,
            // Otherwise get current revision hash from storage
            None => {
                let block = self
                    .storage
                    .get(&make_reference(self.identifier().as_bytes()))
                    .await?;

                match block {
                    // If store has a revision, use it
                    Some(block_data) => {
                        // Check if the block data matches NULL_REVISION_HASH
                        if block_data == NULL_REVISION_HASH.to_vec() {
                            NULL_REVISION_HASH
                        } else {
                            Blake3Hash::try_from(block_data).map_err(|bytes| {
                                DialogArtifactsError::InvalidRevision(format!(
                                    "Incorrect byte length (expected {HASH_SIZE}, received {})",
                                    bytes.len()
                                ))
                            })?
                        }
                    }
                    // If no revision exists in storage, use NULL_REVISION_HASH
                    None => NULL_REVISION_HASH,
                }
            }
        };

        // Now get hashes for the indexes.
        let index_version =
            // The null revision does not actually exists it just represents
            // empty indexes so we set all versions to None's.
            if required_hash == NULL_REVISION_HASH {
                None
            } else {
                // Otherwise we hydrate revision info from the store.
                let revision = self
                    .storage
                    .read::<Revision>(&required_hash)
                    .await?
                    .ok_or_else(|| {
                        DialogArtifactsError::InvalidRevision(format!(
                            "Block ({}) not found in storage",
                            required_hash.to_base58()
                        ))
                    })?;

                Some(revision.index().to_owned())
            };

        // Update storage to point to the revision hash only if it was
        // explicitly provided. If it was not no point of updating because
        // we just read it from the store.
        if revision_hash.is_some() {
            self.storage
                .set(
                    make_reference(self.identifier.as_bytes()),
                    required_hash.to_vec(),
                )
                .await?;
        }

        // Finally update the index
        let mut index = self.index.write().await;
        *index = match index_version {
            Some(hash) => ArtifactTree::from_hash(NodeHash::from(hash)),
            None => ArtifactTree::empty(),
        };

        Ok(())
    }

    /// Get the currently asserted [`Datum`]s recorded for the given entity
    /// and attribute. Multiple data are possible for attributes with more
    /// than one asserted value.
    pub async fn select_data(
        &self,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Datum>, DialogArtifactsError> {
        let index = self.index.read().await;
        index.select_data(self.storage.clone(), of, the).await
    }

    /// Stream every [`Artifact`] matching `selector`.
    pub fn select(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static + ConditionalSend
    {
        let index = self.index.clone();
        let storage = self.storage.clone();
        let cache = self.spill_cache.clone();

        try_stream! {
            // Clone the tree under the read lock to "pin" it at a
            // version for the stream's lifetime, then hand off to the
            // shared `ArtifactTreeExt::scan` for EAV/AEV/VAE dispatch.
            let tree = index.read().await.clone();
            let scanned = tree.scan(storage, cache, selector);
            tokio::pin!(scanned);
            for await artifact in scanned {
                yield artifact?;
            }
        }
    }

    /// Commit the given instructions to the store's indexes.
    ///
    /// Data committed this way carries no [`Version`](crate::history::Version)
    /// tag and records no history — version-controlled writes go through the
    /// branch commit path in `dialog-repository` instead.
    async fn commit_instructions<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Instructions: Stream<Item = Instruction> + ConditionalSend,
    {
        let base_revision = self.revision().await?;

        let transaction_result = async {
            let mut index = self.index.write().await;

            // The per-instruction EAV/AEV/VAE key writes (and
            // cardinality-one supersession) are the shared
            // `ArtifactTreeExt::apply`. This method adds only
            // the surrounding transaction bookkeeping — base-revision
            // capture, revision persistence, pointer advance, and the
            // rollback below.
            let mut delta: Delta<NodeHash, TreeBuffer> = Delta::zero();
            index
                .apply(&mut self.storage, &mut delta, instructions)
                .await?;

            // Persist the tree's pending nodes before minting a revision;
            // a revision must only reference durable blocks.
            stream::iter(delta.flush().map(|(_, buffer)| buffer))
                .map(|buffer| {
                    let mut storage = self.storage.clone();
                    async move {
                        let digest = *buffer.blake3_hash().as_bytes();
                        storage.set(digest, buffer.into_vec()).await
                    }
                })
                .buffer_unordered(FLUSH_CONCURRENCY)
                .try_collect::<()>()
                .await?;

            let root = index.root();
            let next_revision = if root == NULL_BLAKE3_HASH {
                None
            } else {
                Some(Revision::new(root.as_bytes()))
            };

            let revision_hash = if let Some(revision) = &next_revision {
                self.storage.write(&revision).await?;
                revision.as_reference().await?
            } else {
                NULL_REVISION_HASH
            };

            // Advance the effective pointer to the latest version of this DB
            self.storage
                .set(
                    make_reference(self.identifier.as_bytes()),
                    revision_hash.to_vec(),
                )
                .await?;

            Ok(revision_hash) as Result<Blake3Hash, DialogArtifactsError>
        }
        .await;

        match transaction_result {
            Ok(revision) => Ok(revision),
            Err(error) => {
                self.reset(Some(base_revision)).await?;
                Err(error)
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> ArtifactStore for Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    fn select(
        &self,
        selector: ArtifactSelector<Constrained>,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + 'static + ConditionalSend
    {
        let index = self.index.clone();
        let storage = self.storage.clone();
        let cache = self.spill_cache.clone();

        try_stream! {
            // Clone the tree under the read lock to "pin" it at a
            // version for the stream's lifetime, then hand off to the
            // shared `ArtifactTreeExt::scan` for EAV/AEV/VAE dispatch.
            let tree = index.read().await.clone();
            let scanned = tree.scan(storage, cache, selector);
            tokio::pin!(scanned);
            for await artifact in scanned {
                yield artifact?;
            }
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> ArtifactStoreMut for Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    async fn commit<Instructions>(
        &mut self,
        instructions: Instructions,
    ) -> Result<Blake3Hash, DialogArtifactsError>
    where
        Instructions: Stream<Item = Instruction> + ConditionalSend,
    {
        self.commit_instructions(instructions).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeSet, iter::once, str::FromStr, sync::Arc};
    use tokio::io::{BufReader, BufWriter};

    use anyhow::Result;
    use dialog_storage::{
        MeasuredStorage, MemoryStorageBackend, StorageBackend, make_target_storage,
    };
    use futures_util::{StreamExt, TryStreamExt};
    use tokio::sync::Mutex;

    use crate::helpers::generate_data;
    use crate::{
        Artifact, ArtifactSelector, ArtifactStoreMutExt, Artifacts, Attribute,
        DialogArtifactsError, Entity, Instruction, NULL_REVISION_HASH, Value, make_reference,
    };

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// On-demand bug-tracker footprint: seeds the same 300 bugs the query
    /// benchmark uses (seven `squash.bug/*` facts each) into a disk-backed store
    /// wrapped in a byte counter, and reports the persisted size and the block
    /// (node) count + size distribution. Run on this revision and on `main` to
    /// compare formats: it answers both "how much smaller on disk" and "why the
    /// block-read count differs" (block count/size = tree shape). Gated on
    /// `DIALOG_BUG_FOOTPRINT`; native only.
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn it_measures_bug_footprint() -> anyhow::Result<()> {
        use std::str::FromStr;
        use std::sync::Arc;

        if std::env::var("DIALOG_BUG_FOOTPRINT").is_err() {
            eprintln!("DIALOG_BUG_FOOTPRINT not set; skipping bug footprint");
            return Ok(());
        }
        let count: usize = std::env::var("DIALOG_BUG_COUNT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);

        const STATUSES: &[&str] = &["done", "triage", "todo", "canceled", "in-progress"];
        const PRIORITIES: &[&str] = &["medium", "high", "low", "urgent"];
        const ASSIGNEES: &[&str] = &[
            "",
            "did:key:z6MkDQtgLHmp664Wf8wn32G9MT79GpKncnQkcJmLYYu6HEJz",
            "did:key:z6MkAoFSTzm7XMv6wc1X9H5iND4YSfEaHw2LYWiTR2xDPfu8",
            "did:key:z6MkGSesqrS3iyekKGrhMCmHyp82RxJaohuvnNMmdQXG9kza",
        ];
        // Seven facts per bug, matching the query bench's `Bug` concept.
        let field = |ns: &str, name: &str| Attribute::from_str(&format!("{ns}/{name}")).unwrap();
        let mut data = Vec::with_capacity(count * 7);
        for index in 0..count {
            let of = Entity::new()?;
            let push = |data: &mut Vec<Artifact>, the: Attribute, is: Value| {
                data.push(Artifact {
                    the,
                    of: of.clone(),
                    is,
                    cause: None,
                });
            };
            push(
                &mut data,
                field("squash.bug", "status"),
                Value::String(STATUSES[index % STATUSES.len()].to_string()),
            );
            push(
                &mut data,
                field("squash.bug", "priority"),
                Value::String(PRIORITIES[index % PRIORITIES.len()].to_string()),
            );
            push(
                &mut data,
                field("squash.bug", "assignee"),
                Value::String(ASSIGNEES[index % ASSIGNEES.len()].to_string()),
            );
            push(
                &mut data,
                field("squash.bug", "title"),
                Value::String(format!("Bug #{index}: something is off")),
            );
            // `detail` follows the real tonk data's shape: mostly a few hundred
            // chars, but roughly 1 in 25 is a long paste (>4096 bytes) that
            // spills to a content-addressed block. This is what makes the
            // measurement representative — a fixed short detail would miss the
            // spill path entirely.
            let detail_len = if index % 25 == 7 { 20_000 } else { 284 };
            push(
                &mut data,
                field("squash.bug", "detail"),
                Value::String("x".repeat(detail_len)),
            );
            push(
                &mut data,
                field("squash.bug", "ident"),
                Value::String(format!("SQ-{index:04}")),
            );
            push(
                &mut data,
                field("squash.bug", "ordering"),
                Value::Float(index as f64 * 1000.0),
            );
        }
        let fact_count = data.len();

        let root = tempfile::tempdir()?;
        let backend = dialog_storage::FileSystemStorageBackend::<crate::Blake3Hash, Vec<u8>>::new(
            root.path(),
        )
        .await?;
        let measured = Arc::new(tokio::sync::Mutex::new(
            dialog_storage::MeasuredStorage::new(backend),
        ));
        let mut facts = Artifacts::anonymous(measured.clone()).await?;
        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        // Walk the on-disk directory to size every persisted block and build a
        // size histogram (the tree's node bytes drive both footprint and reads).
        let mut sizes: Vec<u64> = Vec::new();
        for entry in std::fs::read_dir(root.path())? {
            let path = entry?.path();
            if path.is_file() {
                sizes.push(std::fs::metadata(&path)?.len());
            }
        }
        sizes.sort_unstable();
        let total: u64 = sizes.iter().sum();
        let blocks = sizes.len();
        let (write_bytes, writes) = {
            let storage = measured.lock().await;
            (storage.write_bytes(), storage.writes())
        };
        let median = sizes.get(blocks / 2).copied().unwrap_or(0);
        let max = sizes.last().copied().unwrap_or(0);
        // Big blocks are the index/leaf nodes; small ones are revisions/refs.
        let big = sizes.iter().filter(|&&s| s > 1024).count();

        eprintln!(
            "BUGFOOTPRINT bugs={count} facts={fact_count} \
             on_disk_bytes={total} blocks={blocks} \
             bytes/bug={:.1} bytes/fact={:.1} \
             write_bytes={write_bytes} writes={writes} \
             block_median={median} block_max={max} blocks_over_1k={big}",
            total as f64 / count as f64,
            total as f64 / fact_count as f64,
        );
        Ok(())
    }

    /// On-demand real-data footprint + query harness. Skipped unless
    /// `DIALOG_IMPORT_CSV` points at a `the,of,as,is,cause` CSV (produced by
    /// `tonk export`). Imports every row into a disk-backed store wrapped in a
    /// byte counter, reports the persisted size, and times an attribute scan —
    /// run it on this revision and on the old tag to compare formats on real
    /// data. Native only (it reads a file).
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    // A gated, on-demand measurement harness: fully-qualified std/storage paths
    // keep it self-contained without cluttering the test module's imports.
    #[allow(clippy::absolute_paths)]
    async fn it_reports_real_data_footprint() -> anyhow::Result<()> {
        use std::str::FromStr;
        use std::sync::Arc;

        let Ok(csv_path) = std::env::var("DIALOG_IMPORT_CSV") else {
            eprintln!("DIALOG_IMPORT_CSV not set; skipping real-data footprint harness");
            return Ok(());
        };

        // Parse the CSV into artifacts (the columns are the,of,as,is,cause;
        // `as` is the value type). A minimal RFC-4180 parser handling quoted
        // fields with embedded commas, doubled quotes, and newlines (the tonk
        // export puts multi-line HTML/CSS in the `is` column), so the harness
        // has no CSV dependency and runs unchanged on the old tag.
        fn parse_csv(text: &str) -> Vec<Vec<String>> {
            let mut records = Vec::new();
            let mut record = Vec::new();
            let mut field = String::new();
            let mut in_quotes = false;
            let mut chars = text.chars().peekable();
            while let Some(ch) = chars.next() {
                match ch {
                    '"' if in_quotes && chars.peek() == Some(&'"') => {
                        field.push('"');
                        chars.next();
                    }
                    '"' => in_quotes = !in_quotes,
                    ',' if !in_quotes => record.push(std::mem::take(&mut field)),
                    '\n' if !in_quotes => {
                        record.push(std::mem::take(&mut field));
                        records.push(std::mem::take(&mut record));
                    }
                    '\r' if !in_quotes => {}
                    _ => field.push(ch),
                }
            }
            if !field.is_empty() || !record.is_empty() {
                record.push(field);
                records.push(record);
            }
            records
        }

        let text = std::fs::read_to_string(&csv_path)?;
        let mut artifacts_in = Vec::new();
        for record in parse_csv(&text).into_iter().skip(1) {
            if record.len() < 4 {
                continue;
            }
            let Ok(the) = Attribute::from_str(&record[0]) else {
                continue;
            };
            let Ok(of) = Entity::from_str(&record[1]) else {
                continue;
            };
            let value_type = record[2].as_str();
            let raw = record[3].as_str();
            let parsed = match value_type {
                "text" => Some(Value::String(raw.to_owned())),
                "entity" => Entity::from_str(raw).ok().map(Value::Entity),
                "natural" => raw.parse().ok().map(Value::UnsignedInt),
                "integer" => raw.parse().ok().map(Value::SignedInt),
                "float" => raw.parse().ok().map(Value::Float),
                "boolean" => bool::from_str(raw).ok().map(Value::Boolean),
                "attribute" => Attribute::from_str(raw).ok().map(Value::Symbol),
                // Skip rows whose value type this minimal parser does not
                // handle (bytes/record are base58 and not needed for a size
                // comparison of the common text/entity/numeric mix).
                _ => None,
            };
            let Some(is) = parsed else {
                continue;
            };
            artifacts_in.push(Artifact {
                the,
                of,
                is,
                cause: None,
            });
        }
        let fact_count = artifacts_in.len();

        // Fresh, unique tempdir per run (tempfile::tempdir), so the on-disk
        // size reflects THIS import alone — not a reused directory or a
        // long-lived repo with accumulated history. Auto-removed on drop.
        let root = tempfile::tempdir()?;
        let backend = dialog_storage::FileSystemStorageBackend::<crate::Blake3Hash, Vec<u8>>::new(
            root.path(),
        )
        .await?;
        // Journaled(Measured(fs)): MeasuredStorage counts write/read bytes;
        // JournaledStorage records the block hash of every read, so a query's
        // reads can be named (and each block's on-disk size looked up) to show
        // exactly which extra blocks a format touches and why.
        let measured = Arc::new(tokio::sync::Mutex::new(
            dialog_storage::JournaledStorage::new(dialog_storage::MeasuredStorage::new(backend)),
        ));
        // Fixed identifier so the tree can be reopened for cold per-query
        // reads (see the query loop below).
        let mut facts = Artifacts::open("bench".to_owned(), measured.clone()).await?;

        // Commit incrementally so a malformed key names the artifact that
        // produced it (set DIALOG_IMPORT_BISECT to enable; otherwise commit in
        // one batch for the timing figure).
        let commit_start = std::time::Instant::now();
        if std::env::var("DIALOG_IMPORT_BISECT").is_ok() {
            for (index, artifact) in artifacts_in.iter().enumerate() {
                if let Err(error) = facts
                    .commit(std::iter::once(Instruction::Assert(artifact.clone())))
                    .await
                {
                    panic!(
                        "commit failed at row {index}: {error}\n  the={} of={} is={:?}",
                        artifact.the, artifact.of, artifact.is
                    );
                }
            }
        } else {
            facts
                .commit(artifacts_in.iter().cloned().map(Instruction::Assert))
                .await?;
        }
        let commit_elapsed = commit_start.elapsed();

        let (write_bytes, writes) = {
            let storage = measured.lock().await;
            (storage.backend().write_bytes(), storage.backend().writes())
        };

        // (1) TOTAL on-disk size after the import: recursively sum every file
        // under the storage root — index blocks, spilled archive blocks, refs,
        // everything the format persisted for this dataset. This is the size
        // number the format reduction is about; write_bytes is cumulative
        // bytes-written (counts overwrites), on_disk is the settled footprint.
        fn dir_size(path: &std::path::Path) -> std::io::Result<(u64, u64)> {
            let mut bytes = 0u64;
            let mut files = 0u64;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let meta = entry.metadata()?;
                if meta.is_dir() {
                    let (b, f) = dir_size(&entry.path())?;
                    bytes += b;
                    files += f;
                } else {
                    bytes += meta.len();
                    files += 1;
                }
            }
            Ok((bytes, files))
        }
        let (on_disk_bytes, on_disk_files) = dir_size(root.path())?;

        // Map a read (a block hash) back to its on-disk file size, so a
        // query's reads can be summarized by count AND bytes moved. The
        // FileSystemStorageBackend names each block file by its hash; the
        // journal records the Blake3Hash key of each read.
        let block_size_on_disk = |hash: &crate::Blake3Hash| -> u64 {
            use base58::ToBase58;
            let name = hash.to_base58();
            let mut found = 0u64;
            // Blocks live in per-store subdirectories (archive/index, ...);
            // walk to find the file named for this hash.
            fn walk(dir: &std::path::Path, name: &str, out: &mut u64) {
                let Ok(entries) = std::fs::read_dir(dir) else {
                    return;
                };
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.is_dir() {
                        walk(&path, name, out);
                    } else if path.file_name().and_then(|n| n.to_str()) == Some(name) {
                        *out = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    }
                }
            }
            walk(root.path(), &name, &mut found);
            found
        };

        // (2) + (3) Run realistic queries against the IMPORTED dataset on
        // disk, journalling each so we can name the blocks read and time it.
        // Pick the three most common attributes so the shapes are meaningful.
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for artifact in &artifacts_in {
            *counts.entry(artifact.the.to_string()).or_default() += 1;
        }
        let mut ranked: Vec<(String, usize)> = counts.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        eprintln!(
            "REALDATA facts={fact_count} \
             on_disk={on_disk_bytes}B ({on_disk_files} files, {:.1} B/fact, {:.1} B/entry) \
             write_bytes={write_bytes} writes={writes} \
             commit={commit_elapsed:?}",
            on_disk_bytes as f64 / fact_count as f64,
            on_disk_bytes as f64 / fact_count as f64 / 3.0,
        );

        for (attribute, count) in ranked.into_iter().take(3) {
            let the = Attribute::from_str(&attribute)?;
            // Reopen from the same on-disk storage so each query runs with a
            // COLD in-memory tree cache (only the persisted blocks are shared).
            // Otherwise the first scan warms the cache and later scans report
            // reads=0, making per-query attribution order-dependent.
            let facts = Artifacts::open("bench".to_owned(), measured.clone()).await?;
            {
                let storage = measured.lock().await;
                storage.clear_journal();
            }
            let scan_start = std::time::Instant::now();
            let selected: Vec<Artifact> = facts
                .select(ArtifactSelector::new().the(the))
                .try_collect()
                .await?;
            let scan_elapsed = scan_start.elapsed();

            // Name the blocks this query read and the bytes each moved, so a
            // read-count difference between formats is explained by the actual
            // blocks (count and size), not a guess.
            let reads = {
                let storage = measured.lock().await;
                storage.get_reads()
            };
            let mut unique: std::collections::BTreeMap<crate::Blake3Hash, usize> =
                std::collections::BTreeMap::new();
            for hash in &reads {
                *unique.entry(*hash).or_default() += 1;
            }
            let read_bytes: u64 = unique.keys().map(block_size_on_disk).sum();
            eprintln!(
                "  scan the={attribute} n={count} results={} \
                 time={scan_elapsed:?} reads={} unique_blocks={} read_bytes={read_bytes}",
                selected.len(),
                reads.len(),
                unique.len(),
            );
            for (hash, times) in &unique {
                use base58::ToBase58;
                eprintln!(
                    "    block {} size={}B reads={times}",
                    hash.to_base58(),
                    block_size_on_disk(hash)
                );
            }
        }

        Ok(())
    }

    /// A selector that constrains the entity, the attribute and the value
    /// pins every component of the index key, so the scan range collapses
    /// to a single exact key. Regression guard: the range must be treated
    /// inclusively or the entry is unreachable (the old prolly tree papered
    /// over this with a point-lookup special case for start == end ranges).
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_selects_fully_constrained_artifacts() -> anyhow::Result<()> {
        let (storage_backend, _temp) = make_target_storage().await?;
        let data = generate_data(4)?;
        let sample = data[0].clone();
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        artifacts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let selector = ArtifactSelector::new()
            .of(sample.of.clone())
            .the(sample.the.clone())
            .is(sample.is.clone());
        let results: Vec<Artifact> = artifacts
            .select(selector)
            .map(|artifact| artifact.unwrap())
            .collect()
            .await;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].of, sample.of);
        assert_eq!(results[0].the, sample.the);
        assert_eq!(results[0].is, sample.is);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_commits_and_selects_facts() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let entity_order = |l: &Artifact, r: &Artifact| l.of.cmp(&r.of);
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        let mut data = vec![
            Artifact {
                the: Attribute::from_str("profile/name")?,
                of: Entity::new()?,
                is: Value::String("Foo Bar".into()),
                cause: None,
            },
            Artifact {
                the: Attribute::from_str("profile/name")?,
                of: Entity::new()?,
                is: Value::String("Fizz Buzz".into()),
                cause: None,
            },
        ];

        data.sort_by(entity_order);

        facts
            .commit(data.clone().into_iter().map(Instruction::Assert))
            .await?;

        let fact_stream =
            facts.select(ArtifactSelector::new().the(Attribute::from_str("profile/name")?));

        let mut facts: Vec<Artifact> = fact_stream.map(|fact| fact.unwrap()).collect().await;
        facts.sort_by(entity_order);

        assert_eq!(data, facts);

        Ok(())
    }

    /// Regression: two separate commits (persist + reload between them) where
    /// the second entity's keys sort BEFORE the first's must keep both facts.
    /// This exact DID pair reproduced a drop of the first commit's fact.
    #[dialog_common::test]
    async fn it_keeps_prior_fact_when_second_commit_inserts_new_minimum() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        // alice sorts AFTER bob; alice is committed first, bob second.
        let alice = Artifact {
            the: Attribute::from_str("person/name")?,
            of: "did:key:z6MkQmQKzPsjyUz49pvaxYdiiZEuQXyNqeBkS88GTrvqnov".parse()?,
            is: Value::String("Alice".into()),
            cause: None,
        };
        let bob = Artifact {
            the: Attribute::from_str("person/name")?,
            of: "did:key:z6MkDiL3ZaJ4V7VSdQruLenZLA4RNbu6cErR5m8K5Wj99wTF".parse()?,
            is: Value::String("Bob".into()),
            cause: None,
        };

        facts
            .commit(vec![alice.clone()].into_iter().map(Instruction::Replace))
            .await?;
        facts
            .commit(vec![bob.clone()].into_iter().map(Instruction::Replace))
            .await?;

        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the(Attribute::from_str("person/name")?))
            .map(|fact| fact.unwrap())
            .collect()
            .await;

        assert_eq!(
            selected.len(),
            2,
            "both facts must survive two commits; got {:?}",
            selected
                .iter()
                .map(|a| a.of.to_string())
                .collect::<Vec<_>>()
        );

        Ok(())
    }

    /// Retracting a fact that was never asserted is a no-op: no tombstone is
    /// written, so the tree root (and therefore the branch revision) is
    /// unchanged. Otherwise an idle synced branch would push a revision whose
    /// only content is a tombstone for a fact that never existed.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_treats_retracting_a_missing_fact_as_a_noop() -> Result<()> {
        let (storage_backend, _temp) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        // Seed an unrelated fact so the tree is non-empty.
        let base_root = facts
            .commit(once(Instruction::Assert(Artifact {
                the: Attribute::from_str("user/name")?,
                of: Entity::new()?,
                is: Value::String("Alice".into()),
                cause: None,
            })))
            .await?;

        // Retract a fact that was never asserted.
        let after_root = facts
            .commit(once(Instruction::Retract(Artifact {
                the: Attribute::from_str("user/session")?,
                of: Entity::new()?,
                is: Value::String("ephemeral".into()),
                cause: None,
            })))
            .await?;

        assert_eq!(
            base_root, after_root,
            "retracting a fact that was never present must not change the tree"
        );
        Ok(())
    }

    /// Assert + retract of a fact in the same batch, when that fact had **no
    /// prior committed value**, leaves nothing behind: the assert and retract
    /// cancel at the tree, no tombstone is written, and the root is unchanged.
    /// This is the transient-command shape (a concept asserted then retracted
    /// in one commit) that used to churn the branch head on every occurrence.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_leaves_no_key_when_assert_and_retract_a_novel_fact_in_one_batch() -> Result<()> {
        let (storage_backend, _temp) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        let base_root = facts
            .commit(once(Instruction::Assert(Artifact {
                the: Attribute::from_str("user/name")?,
                of: Entity::new()?,
                is: Value::String("Alice".into()),
                cause: None,
            })))
            .await?;

        let session = Attribute::from_str("user/session")?;
        let transient = Artifact {
            the: session.clone(),
            of: Entity::new()?,
            is: Value::String("ephemeral".into()),
            cause: None,
        };
        let after_root = facts
            .commit(
                vec![
                    Instruction::Assert(transient.clone()),
                    Instruction::Retract(transient.clone()),
                ]
                .into_iter(),
            )
            .await?;

        // No tree churn: the novel fact left no key at all.
        assert_eq!(
            base_root, after_root,
            "assert+retract of a novel fact must leave the tree unchanged"
        );
        // And the fact is not queryable.
        let hits: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the(session))
            .try_collect()
            .await?;
        assert!(hits.is_empty(), "the transient fact must not be queryable");
        Ok(())
    }

    /// Retracting a fact whose value was **already committed** deletes it
    /// from the active indexes — no tombstone (deletion now travels as a
    /// history record; see `crate::merge`). The tree still changes, since
    /// the fact's keys are gone, and the fact is no longer queryable.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_deletes_from_the_indexes_when_retracting_a_committed_fact() -> Result<()> {
        let (storage_backend, _temp) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        let name = Attribute::from_str("user/name")?;
        let alice = Artifact {
            the: name.clone(),
            of: Entity::new()?,
            is: Value::String("Alice".into()),
            cause: None,
        };

        // Commit the fact so it has a durable prior.
        let committed_root = facts
            .commit(once(Instruction::Assert(alice.clone())))
            .await?;

        // Retract it in a later commit.
        let after_root = facts
            .commit(once(Instruction::Retract(alice.clone())))
            .await?;

        // The retraction changed the tree: the fact's keys were deleted
        // (and a retract record was appended to the history region).
        assert_ne!(
            committed_root, after_root,
            "retracting a committed fact removes it from the indexes (tree changes)"
        );
        // The fact is gone from queries.
        let hits: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the(name))
            .try_collect()
            .await?;
        assert!(
            hits.is_empty(),
            "the retracted fact must not be queryable after deletion"
        );
        Ok(())
    }

    /// An attribute-prefix selector ranges over the AEV index:
    /// attribute names are stored raw in the key, so the range is
    /// exact.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_selects_by_attribute_prefix() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;
        let alice = Entity::new()?;

        let data = vec![
            Artifact {
                the: Attribute::from_str("person/name")?,
                of: alice.clone(),
                is: Value::String("Alice".into()),
                cause: None,
            },
            Artifact {
                the: Attribute::from_str("person/age")?,
                of: alice.clone(),
                is: Value::UnsignedInt(40),
                cause: None,
            },
            Artifact {
                the: Attribute::from_str("group/name")?,
                of: alice,
                is: Value::String("Admins".into()),
                cause: None,
            },
        ];
        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the_starting_with("person/"))
            .try_collect()
            .await?;
        assert_eq!(selected.len(), 2, "two person/* facts");
        assert!(
            selected
                .iter()
                .all(|fact| String::from(&fact.the).starts_with("person/")),
            "every selected fact carries the prefix"
        );
        Ok(())
    }

    /// An entity-prefix selector ranges over the EAV index. The
    /// entity key stores only the first 32 URI bytes raw, so a
    /// prefix longer than that must be confirmed against the stored
    /// datum — the second half of this test diverges two URIs past
    /// byte 32 to force that path.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_selects_by_entity_prefix() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        let name = Attribute::from_str("person/name")?;
        let fact = |entity: Entity, value: &str| Artifact {
            the: name.clone(),
            of: entity,
            is: Value::String(value.into()),
            cause: None,
        };

        // Short prefixes (within the raw head) discriminate on key
        // bytes alone.
        let urn_alpha = Entity::from_str("urn:alpha:1")?;
        let urn_beta = Entity::from_str("urn:beta:1")?;
        // Long shared head: these two agree for well over 32 bytes
        // and diverge only in the hashed tail of the key.
        let shared = "urn:shared:0000000000000000000000000000:";
        let long_a = Entity::from_str(&format!("{shared}aaaa"))?;
        let long_b = Entity::from_str(&format!("{shared}bbbb"))?;

        facts
            .commit(
                [
                    fact(urn_alpha.clone(), "Alpha"),
                    fact(urn_beta, "Beta"),
                    fact(long_a.clone(), "LongA"),
                    fact(long_b, "LongB"),
                ]
                .into_iter()
                .map(Instruction::Assert),
            )
            .await?;

        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().of_starting_with("urn:alpha:"))
            .try_collect()
            .await?;
        assert_eq!(selected.len(), 1, "short prefix selects on key bytes");
        assert_eq!(selected[0].of, urn_alpha);

        let long_prefix = format!("{shared}aaaa");
        assert!(long_prefix.len() > 32, "the prefix outruns the raw head");
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().of_starting_with(long_prefix))
            .try_collect()
            .await?;
        assert_eq!(
            selected.len(),
            1,
            "the datum re-check discriminates beyond the raw head"
        );
        assert_eq!(selected[0].of, long_a);

        Ok(())
    }

    /// A value-prefix selector ranges over the VAE index. The M3
    /// value-in-key format stores a string value's bytes inline and
    /// order-preservingly, so a prefix scan brackets the value dimension
    /// directly and returns exactly the string values beginning with the
    /// prefix — across different attributes and entities, and excluding
    /// non-string values that cannot carry the prefix.
    #[dialog_common::test]
    async fn it_selects_by_value_prefix() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;
        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let data = vec![
            Artifact {
                the: Attribute::from_str("person/name")?,
                of: alice.clone(),
                is: Value::String("Alice".into()),
                cause: None,
            },
            Artifact {
                the: Attribute::from_str("person/city")?,
                of: alice.clone(),
                is: Value::String("Albuquerque".into()),
                cause: None,
            },
            Artifact {
                the: Attribute::from_str("person/name")?,
                of: bob.clone(),
                is: Value::String("Bob".into()),
                cause: None,
            },
            // A non-string value that must never match a string prefix.
            Artifact {
                the: Attribute::from_str("person/age")?,
                of: alice,
                is: Value::UnsignedInt(40),
                cause: None,
            },
        ];
        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        // "Al" spans two attributes (name + city) on the same entity.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_starting_with("Al"))
            .try_collect()
            .await?;
        assert_eq!(selected.len(), 2, "two values begin with Al");
        assert!(
            selected.iter().all(|fact| match &fact.is {
                Value::String(string) => string.starts_with("Al"),
                other => panic!("unexpected non-string match: {other:?}"),
            }),
            "every selected value carries the prefix"
        );

        // A narrower prefix isolates one value.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_starting_with("Ali"))
            .try_collect()
            .await?;
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].is, Value::String("Alice".into()));

        // A prefix that matches nothing returns nothing.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_starting_with("Zzz"))
            .try_collect()
            .await?;
        assert!(selected.is_empty(), "no value begins with Zzz");

        Ok(())
    }

    /// A float value's key must round-trip once it accumulates into a shared
    /// leaf. Regression guard for a value-tail width bug: `encode_f64` writes 8
    /// bytes but the key parser once claimed 16 for `Float`, so a float key
    /// over-read into the following components and split into fewer parts than
    /// its schema — every commit that packed such a key into an index leaf then
    /// failed. Commit enough float-valued facts to force a leaf, then read them
    /// back.
    #[dialog_common::test]
    async fn it_round_trips_many_float_values() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        let attribute = Attribute::from_str("measure/value")?;
        let mut data = Vec::new();
        for index in 0..400u64 {
            data.push(Artifact {
                the: attribute.clone(),
                of: Entity::new()?,
                // A mix of magnitudes and signs, including a large timestamp-
                // like integer stored as a float (the shape that first tripped
                // this in real data).
                is: Value::Float(index as f64 * 1.5 - 100.0),
                cause: None,
            });
        }
        data.push(Artifact {
            the: attribute.clone(),
            of: Entity::new()?,
            is: Value::Float(1783112056217.0),
            cause: None,
        });
        let expected = data.len();
        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the(attribute))
            .try_collect()
            .await?;
        assert_eq!(selected.len(), expected, "every float fact reads back");
        assert!(
            selected
                .iter()
                .all(|fact| matches!(fact.is, Value::Float(_))),
            "every value round-trips as a float"
        );
        Ok(())
    }

    /// Numeric value range scans over the VAE index. The M3 value-in-key
    /// format sorts numeric values order-preservingly within their type band,
    /// so `is_at_least`/`is_at_most`/`is_between` (and their exclusive
    /// variants) bracket the value dimension; exclusivity and the type band are
    /// enforced by the per-entry re-check.
    #[dialog_common::test]
    async fn it_selects_by_value_range() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let mut facts = Artifacts::anonymous(storage_backend).await?;

        let age = Attribute::from_str("person/age")?;
        let fact = |value: u128| -> anyhow::Result<Artifact> {
            Ok(Artifact {
                the: age.clone(),
                of: Entity::new()?,
                is: Value::UnsignedInt(value),
                cause: None,
            })
        };
        let mut data = Vec::new();
        for value in [10u128, 20, 30, 40, 50] {
            data.push(fact(value)?);
        }
        // A string value on the same attribute must never match a numeric range.
        data.push(Artifact {
            the: age.clone(),
            of: Entity::new()?,
            is: Value::String("not a number".into()),
            cause: None,
        });
        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let values = |facts: &[Artifact]| -> Vec<u128> {
            let mut out: Vec<u128> = facts
                .iter()
                .map(|fact| match fact.is {
                    Value::UnsignedInt(value) => value,
                    ref other => panic!("unexpected non-numeric match: {other:?}"),
                })
                .collect();
            out.sort_unstable();
            out
        };

        // Inclusive lower bound.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_at_least(Value::UnsignedInt(30)))
            .try_collect()
            .await?;
        assert_eq!(values(&selected), vec![30, 40, 50], ">= 30");

        // Exclusive lower bound drops the boundary value.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_greater_than(Value::UnsignedInt(30)))
            .try_collect()
            .await?;
        assert_eq!(values(&selected), vec![40, 50], "> 30");

        // Inclusive upper bound.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_at_most(Value::UnsignedInt(20)))
            .try_collect()
            .await?;
        assert_eq!(values(&selected), vec![10, 20], "<= 20");

        // A closed interval.
        let selected: Vec<Artifact> = facts
            .select(
                ArtifactSelector::new().is_between(Value::UnsignedInt(20), Value::UnsignedInt(40)),
            )
            .try_collect()
            .await?;
        assert_eq!(values(&selected), vec![20, 30, 40], "[20, 40]");

        // A range that spans nothing.
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().is_at_least(Value::UnsignedInt(1000)))
            .try_collect()
            .await?;
        assert!(selected.is_empty(), ">= 1000 matches nothing");

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_pins_a_stream_at_the_version_where_iteration_begins() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let data = generate_data(5)?;
        let mut artifacts = Artifacts::anonymous(storage_backend.clone()).await?;

        let entities = data
            .iter()
            .map(|artifact| artifact.of.clone())
            .collect::<BTreeSet<Entity>>();

        artifacts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let stream = artifacts.select(ArtifactSelector::new().the("item/id".parse()?));

        tokio::pin!(stream);

        let mut count = 0usize;

        while let Some(artifact) = stream.try_next().await? {
            artifacts
                .commit(generate_data(1)?.into_iter().map(Instruction::Assert))
                .await?;
            assert!(entities.contains(&artifact.of));
            count += 1;
        }

        assert_eq!(count, 5);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_export_to_and_import_from_csv() -> Result<()> {
        let (csv, expected_ids, expected_revision) = {
            let storage_backend = MemoryStorageBackend::default();
            let data = generate_data(1)?;
            let mut artifacts = Artifacts::anonymous(storage_backend.clone()).await?;

            artifacts
                .commit(data.into_iter().map(Instruction::Assert))
                .await?;

            let ids = artifacts
                .select(ArtifactSelector::new().the(Attribute::from_str("item/id")?))
                .map(|result| result.unwrap())
                .collect::<Vec<Artifact>>()
                .await;

            let mut csv = BufWriter::new(Vec::<u8>::new());
            artifacts.export(&mut csv).await?;
            (csv.into_inner(), ids, artifacts.revision().await)
        };

        println!("{}", String::from_utf8(csv.clone())?);

        let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;

        artifacts.import(&mut BufReader::new(csv.as_ref())).await?;

        let actual_ids = artifacts
            .select(ArtifactSelector::new().the(Attribute::from_str("item/id")?))
            .map(|result| result.unwrap())
            .collect::<Vec<Artifact>>()
            .await;

        assert_eq!(expected_ids, actual_ids);
        assert_eq!(expected_revision, artifacts.revision().await);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_query_efficiently_by_entity_and_value() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let storage_backend = Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));
        let data = generate_data(32)?;
        let attribute = Attribute::from_str("item/name")?;
        let name = Value::String("name18".into());
        let entity = data
            .iter()
            .find(|element| element.is == name)
            .unwrap()
            .of
            .clone();

        let mut facts = Artifacts::anonymous(storage_backend.clone()).await?;

        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let (initial_reads, initial_writes) = {
            let storage_backend = storage_backend.lock().await;
            (storage_backend.reads(), storage_backend.writes())
        };

        let fact_stream = facts.select(ArtifactSelector::new().of(entity.clone()).is(name.clone()));

        let results: Vec<Artifact> = fact_stream.map(|result| result.unwrap()).collect().await;

        assert_eq!(
            vec![Artifact {
                the: attribute,
                of: entity,
                is: name,
                cause: None
            }],
            results
        );

        let (net_reads, net_writes) = {
            let storage_backend = storage_backend.lock().await;
            (
                storage_backend.reads() - initial_reads,
                storage_backend.writes() - initial_writes,
            )
        };

        // The threshold-based geometric distribution gives an exact 1/m
        // split at every level, so the tree is flatter than the bit-batch
        // distribution (whose upper levels averaged only 2-4 children) and a
        // point query walks one fewer block to reach its leaf.
        assert_eq!(net_reads, 2);
        assert_eq!(net_writes, 0);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_query_efficiently_by_attribute_and_value() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let storage_backend = Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));
        let data = generate_data(32)?;
        let attribute = Attribute::from_str("item/name")?;
        let name = Value::String("name18".into());
        let entity = data
            .iter()
            .find(|element| element.is == name)
            .unwrap()
            .of
            .clone();

        let mut facts = Artifacts::anonymous(storage_backend.clone()).await?;

        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let (initial_reads, initial_writes) = {
            let storage_backend = storage_backend.lock().await;
            (storage_backend.reads(), storage_backend.writes())
        };

        let fact_stream = facts.select(
            ArtifactSelector::new()
                .the(attribute.clone())
                .is(name.clone()),
        );

        let results: Vec<Artifact> = fact_stream.map(|result| result.unwrap()).collect().await;

        assert_eq!(
            vec![Artifact {
                the: attribute,
                of: entity,
                is: name,
                cause: None
            }],
            results
        );

        let (net_reads, net_writes) = {
            let storage_backend = storage_backend.lock().await;
            (
                storage_backend.reads() - initial_reads,
                storage_backend.writes() - initial_writes,
            )
        };

        // The threshold-based geometric distribution gives an exact 1/m
        // split at every level, so the tree is flatter than the bit-batch
        // distribution (whose upper levels averaged only 2-4 children) and a
        // point query walks one fewer block to reach its leaf.
        assert_eq!(net_reads, 2);
        assert_eq!(net_writes, 0);

        Ok(())
    }

    /// Measures on-disk size and write amplification per fact. Not a pass/fail
    /// assertion of an exact number (that would be brittle across format
    /// tweaks); it prints the persisted bytes-per-fact so the M3 format epoch's
    /// size win can be tracked, and guards a loose upper bound so a regression
    /// that doubled the size would fail. `generate_data` is a realistic mixed
    /// workload (five attributes per entity, several value types).
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_measures_persisted_size_per_fact() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let entities = 512;
        let data = generate_data(entities)?;
        let fact_count = data.len();

        let storage_backend = Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));
        let mut facts = Artifacts::anonymous(storage_backend.clone()).await?;
        facts
            .commit(data.into_iter().map(Instruction::Assert))
            .await?;

        let (write_bytes, writes) = {
            let storage = storage_backend.lock().await;
            (storage.write_bytes(), storage.writes())
        };

        let bytes_per_fact = write_bytes as f64 / fact_count as f64;
        // Each logical fact writes three index entries (EAV/AEV/VAE); the
        // per-entry figure is the apples-to-apples comparison against the
        // pre-M3 baseline, which was measured per single index entry (~172
        // B/entry, where the payload stored the whole fact again).
        let bytes_per_entry = bytes_per_fact / 3.0;
        let blocks_per_fact = writes as f64 / fact_count as f64;
        println!(
            "SIZE {fact_count} facts: {write_bytes} bytes total, \
             {bytes_per_fact:.1} bytes/fact = {bytes_per_entry:.1} bytes/entry \
             (3 indexes), {blocks_per_fact:.3} blocks/fact"
        );

        // Regression guard. Three measured points on this fixture:
        //
        //   pre-M3 (value-hash key, whole fact in payload)  ~172 B/entry
        //   value-in-key alone (#393)                        118 B/entry
        //   value-in-key + version control (this branch)     186 B/entry
        //
        // Version control adds a history record and a coverage entry per
        // write, so it costs ~68 B/entry over #393 on this synthetic fixture
        // (every fact is a distinct entity, which is the worst case for the
        // history region: nothing shares a lineage). The guard tracks the
        // combined figure — the number that matters is the one users pay.
        assert!(
            bytes_per_entry < 200.0,
            "per-entry size regressed to {bytes_per_entry:.1} bytes/entry \
             (value-in-key alone is 118, with version control ~186)"
        );

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_indexes_to_optimize_reads() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(256)?.into_iter().map(Instruction::Assert);

        let storage_backend = Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));

        let mut facts = Artifacts::anonymous(storage_backend.clone()).await?;

        facts.commit(data).await?;

        let (initial_reads, initial_writes) = {
            let storage_backend = storage_backend.lock().await;
            (storage_backend.reads(), storage_backend.writes())
        };

        let fact_stream = facts.select(ArtifactSelector::new().is(Value::String("name64".into())));
        let results: Vec<Artifact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

        assert_eq!(results.len(), 1);

        let (net_reads, net_writes) = {
            let storage_backend = storage_backend.lock().await;
            (
                storage_backend.reads() - initial_reads,
                storage_backend.writes() - initial_writes,
            )
        };

        // The threshold-based geometric distribution gives an exact 1/m
        // split at every level, so the tree is flatter than the bit-batch
        // distribution (whose upper levels averaged only 2-4 children) and
        // reaching a leaf costs fewer block reads.
        assert_eq!(net_reads, 2);
        assert_eq!(net_writes, 0);

        let fact_stream =
            facts.select(ArtifactSelector::new().the(Attribute::from_str("item/id")?));

        let results: Vec<Artifact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

        assert_eq!(results.len(), 256);

        let (net_reads, net_writes) = {
            let storage_backend = storage_backend.lock().await;
            (
                storage_backend.reads() - initial_reads,
                storage_backend.writes() - initial_writes,
            )
        };

        // Cumulative reads across both queries in this test (the baseline is
        // captured once, before the first). The broad attribute scan is a
        // bounded descent, not a full-tree walk. Small values now inline in the
        // key as their order-preserving form rather than a fixed 32-byte
        // reference, which changes leaf boundaries and the tree's shape, so the
        // scan's bounded descent touches a few more blocks than the reference
        // layout did.
        assert_eq!(net_reads, 5);
        assert_eq!(net_writes, 0);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_completes_a_query_when_no_data_matches() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = [124u128; 3]
            .into_iter()
            .map(Value::UnsignedInt)
            .map(|value| Artifact {
                the: "test/attribute".parse().unwrap(),
                of: Entity::new().unwrap(),
                is: value,
                cause: None,
            })
            .map(Instruction::Assert);

        let mut artifacts = Artifacts::anonymous(storage_backend.clone()).await?;
        artifacts.commit(data).await?;

        let results = artifacts
            .select(ArtifactSelector::new().is(Value::UnsignedInt(123)))
            .map(|result| result.unwrap().of)
            .collect::<BTreeSet<Entity>>()
            .await;

        assert_eq!(results.len(), 0);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_distinguishes_same_value_across_different_entities() -> Result<()> {
        // NOTE: This covers a bug where we weren't aggregating entities in the value index properly
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = [123u128; 3]
            .into_iter()
            .map(Value::UnsignedInt)
            .map(|value| Artifact {
                the: "test/attribute".parse().unwrap(),
                of: Entity::new().unwrap(),
                is: value,
                cause: None,
            })
            .map(Instruction::Assert);

        let mut artifacts = Artifacts::anonymous(storage_backend.clone()).await?;
        artifacts.commit(data).await?;

        let data = generate_data(32)?.into_iter().map(Instruction::Assert);

        artifacts.commit(data).await?;

        let results = artifacts
            .select(ArtifactSelector::new().is(Value::UnsignedInt(123)))
            .map(|result| result.unwrap().of)
            .collect::<BTreeSet<Entity>>()
            .await;

        assert_eq!(results.len(), 3);
        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_produces_the_same_version_with_different_insertion_order() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(32)?;

        let mut reordered_data = data.clone();
        reordered_data.reverse();

        assert_ne!(data, reordered_data);

        let into_assert = |fact: Artifact| Instruction::Assert(fact);

        let data = data.into_iter().map(into_assert);
        let reordered_data = reordered_data.into_iter().map(into_assert);

        let storage_backend = Arc::new(Mutex::new(MeasuredStorage::new(storage_backend)));

        let mut facts_one = Artifacts::anonymous(storage_backend.clone()).await?;

        facts_one.commit(data).await?;

        let mut facts_two = Artifacts::anonymous(storage_backend.clone()).await?;

        facts_two.commit(reordered_data).await?;

        assert_eq!(facts_one.revision().await, facts_two.revision().await);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_restore_a_previously_commited_version() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(64)?;
        let into_assert = |fact: Artifact| Instruction::Assert(fact);
        let storage_backend = Arc::new(Mutex::new(storage_backend));

        let mut facts = Artifacts::anonymous(storage_backend.clone()).await?;
        let id = facts.identifier().to_owned();

        facts.commit(data.into_iter().map(into_assert)).await?;
        let revision = facts.revision().await;

        let restored_facts = Artifacts::open(id, storage_backend).await?;
        let restored_revision = restored_facts.revision().await;

        assert_eq!(revision, restored_revision);

        let fact_stream = facts.select(ArtifactSelector::new().is(Value::String("name10".into())));
        let results: Vec<Artifact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

        let restored_fact_stream =
            restored_facts.select(ArtifactSelector::new().is(Value::String("name10".into())));
        let restored_results: Vec<Artifact> = restored_fact_stream
            .map(|fact| fact.unwrap())
            .collect()
            .await;

        assert_eq!(results, restored_results);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_upsert_facts() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let storage_backend = Arc::new(Mutex::new(storage_backend));

        let mut artifacts = Artifacts::anonymous(storage_backend.clone()).await?;

        let attribute = Attribute::from_str("test/attribute")?;
        let entity = Entity::new()?;
        let artifact = Artifact {
            the: attribute.clone(),
            of: entity.clone(),
            is: Value::Boolean(false),
            cause: None,
        };

        artifacts.commit([Instruction::Assert(artifact)]).await?;

        // Replace supersedes priors at (entity, attribute) regardless of value.
        let updated_artifact = Artifact {
            the: attribute,
            of: entity.clone(),
            is: Value::Boolean(true),
            cause: None,
        };

        artifacts
            .commit([Instruction::Replace(updated_artifact.clone())])
            .await?;

        let results = artifacts
            .select(ArtifactSelector::new().of(entity))
            .collect::<Vec<Result<Artifact, DialogArtifactsError>>>()
            .await;

        assert_eq!(results, vec![Ok(updated_artifact)]);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_reset_to_an_earlier_version() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(16)?;

        let expected_entities = data
            .iter()
            .map(|datum| datum.of.clone())
            .collect::<BTreeSet<Entity>>();

        let mut artifacts = Artifacts::anonymous(storage_backend.clone()).await?;

        let revision = artifacts
            .commit(data.clone().into_iter().map(Instruction::Assert))
            .await?;

        let more_data = generate_data(16)?;

        artifacts
            .commit(more_data.into_iter().map(Instruction::Assert))
            .await?;

        artifacts.reset(Some(revision)).await?;

        let results = artifacts
            .select(ArtifactSelector::new().the("item/id".parse()?))
            .map(|result| result.unwrap())
            .collect::<Vec<Artifact>>()
            .await;

        assert_eq!(results.len(), 16);

        for result in results {
            assert!(expected_entities.contains(&result.of))
        }

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_reset_before_commit() -> Result<()> {
        let storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();

        let mut artifacts = Artifacts::anonymous(storage_backend).await?;
        artifacts.reset(None).await?;

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_stores_null_revision_hash_directly() -> Result<()> {
        // Use memory storage backend to avoid file system errors
        let storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();

        // Create an anonymous artifacts instance
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // Reset to NULL_REVISION_HASH explicitly
        artifacts.reset(Some(NULL_REVISION_HASH)).await?;

        // Verify that the revision reference was set to NULL_REVISION_HASH
        let reference_key = make_reference(artifacts.identifier().as_bytes());
        let stored_value = artifacts.storage.get(&reference_key).await?;

        assert!(stored_value.is_some(), "Reference should exist");
        assert_eq!(
            stored_value.unwrap(),
            NULL_REVISION_HASH.to_vec(),
            "Value should be NULL_REVISION_HASH"
        );

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_handles_existing_null_revision() -> Result<()> {
        // Use memory storage backend to avoid file system errors
        let storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();

        // Create an anonymous artifacts instance
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // Manually set the reference to NULL_REVISION_HASH
        let reference_key = make_reference(artifacts.identifier().as_bytes());
        artifacts
            .storage
            .set(reference_key, NULL_REVISION_HASH.to_vec())
            .await?;

        // Get the value before reset
        let before_value = artifacts.storage.get(&reference_key).await?;

        // Now reset with None (should assume NULL_REVISION_HASH)
        artifacts.reset(None).await?;

        // Get the value after reset - should be unchanged since None was passed
        let after_value = artifacts.storage.get(&reference_key).await?;
        assert_eq!(
            before_value, after_value,
            "Storage shouldn't change with reset(None)"
        );

        // Verify we can still add data after reset
        let entity = Entity::new()?;
        let attribute = Attribute::from_str("test/attribute")?;
        let value = Value::String("test value".into());

        artifacts
            .commit(vec![Instruction::Assert(Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is: value.clone(),
                cause: None,
            })])
            .await?;

        // Verify the data exists
        let results = artifacts
            .select(ArtifactSelector::new().the(attribute))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 1);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_treats_null_revision_as_empty() -> Result<()> {
        // Use memory storage backend to avoid file system errors
        let storage_backend = MemoryStorageBackend::default();

        // Create an anonymous artifacts instance
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // Add some data
        let entity = Entity::new()?;
        let attribute = Attribute::from_str("test/attribute")?;
        let value = Value::String("test value".into());

        artifacts
            .commit(vec![Instruction::Assert(Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is: value.clone(),
                cause: None,
            })])
            .await?;

        // Verify the data exists
        let results = artifacts
            .select(ArtifactSelector::new().the(attribute.clone()))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 1);

        // Reset to NULL_REVISION_HASH
        artifacts.reset(Some(NULL_REVISION_HASH)).await?;

        // Verify data is gone (empty state)
        let results = artifacts
            .select(ArtifactSelector::new().the(attribute))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 0);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_verifies_storage_operations_for_null_revision() -> Result<()> {
        // Test that null revision hash is stored correctly
        let storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();

        // Create artifacts instance
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // Reset to NULL_REVISION_HASH with an explicit parameter
        // (this should trigger a storage write)
        artifacts.reset(Some(NULL_REVISION_HASH)).await?;

        // Get the reference key and check what was stored
        let reference_key = make_reference(artifacts.identifier().as_bytes());
        let value = artifacts.storage.get(&reference_key).await?;

        // Verify NULL_REVISION_HASH was stored directly
        assert_eq!(value, Some(NULL_REVISION_HASH.to_vec()));

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_open_with_null_revision_hash() -> Result<()> {
        // Use memory storage backend to avoid file system errors
        let mut storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();

        // Create an identifier for testing
        let identifier = "test-artifacts".to_string();

        // Create a reference key for this identifier
        let reference_key = make_reference(identifier.as_bytes());

        // Set the NULL_REVISION_HASH directly in storage
        storage_backend
            .set(reference_key, NULL_REVISION_HASH.to_vec())
            .await?;

        // Open artifacts with this identifier - should successfully handle NULL_REVISION_HASH
        let artifacts = Artifacts::open(identifier, storage_backend).await?;

        // Verify we can use the artifacts instance
        let entity = Entity::new()?;
        let attribute = Attribute::from_str("test/attribute")?;
        let value = Value::String("test value".into());

        // Try to commit some data to verify it's working
        let mut artifacts_mut = artifacts;
        artifacts_mut
            .commit(vec![Instruction::Assert(Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is: value.clone(),
                cause: None,
            })])
            .await?;

        // Query the data to verify it was stored
        let results = artifacts_mut
            .select(ArtifactSelector::new().the(attribute))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;

        assert_eq!(
            results.len(),
            1,
            "Should be able to read data after opening with NULL_REVISION_HASH"
        );

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_avoids_unnecessary_storage_writes() -> Result<()> {
        // Use a memory storage backend so we can track writes
        let mut storage_backend = MemoryStorageBackend::<[u8; 32], Vec<u8>>::default();

        // Create a key we can track
        let test_key = [42u8; 32];
        let test_value = vec![1, 2, 3];

        // Set an initial value
        storage_backend.set(test_key, test_value.clone()).await?;

        // Create artifacts
        let mut artifacts = Artifacts::anonymous(storage_backend).await?;

        // First, establish a revision
        let entity = Entity::new()?;
        let attribute = Attribute::from_str("test/attribute")?;
        let value = Value::String("test value".into());

        artifacts
            .commit(vec![Instruction::Assert(Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is: value.clone(),
                cause: None,
            })])
            .await?;

        // Get the current revision (we don't need it, just ensure there is one)
        let _ = artifacts.revision().await;

        // Get the reference key
        let reference_key = make_reference(artifacts.identifier().as_bytes());
        let before_value = artifacts.storage.get(&reference_key).await?;

        // Reset with None (should not trigger a storage write)
        artifacts.reset(None).await?;

        // Check the value after reset
        let after_value = artifacts.storage.get(&reference_key).await?;

        // Values should be identical since we didn't trigger a write
        assert_eq!(
            before_value, after_value,
            "Storage value should not change when reset called with None"
        );

        Ok(())
    }

    /// A value larger than the inline threshold spills: its key carries a
    /// 32-byte reference, its bytes land as a content-addressed block in the
    /// store (keyed by that reference), and a select reconstructs the exact
    /// value by fetching the block. Inline values are unaffected.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_round_trips_a_spilled_value() -> Result<()> {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        let big = "s".repeat(inline_n + 1);
        let value = Value::String(big.clone());
        let reference = value.to_reference();

        let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
        let attribute = Attribute::from_str("doc/body")?;
        let entity = Entity::new()?;

        artifacts
            .commit(vec![Instruction::Assert(Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is: value.clone(),
                cause: None,
            })])
            .await?;

        // The value bytes live as a block keyed by the value reference.
        assert_eq!(
            artifacts.storage.get(&reference).await?,
            Some(value.to_bytes()),
            "spilled value bytes are stored as a block under the value reference"
        );

        // A select reconstructs the exact value by fetching the block.
        let results = artifacts
            .select(ArtifactSelector::new().the(attribute))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].is, value, "spilled value reconstructs exactly");

        Ok(())
    }

    /// The inline threshold is inclusive: a value whose encoded form is exactly
    /// `inline_n` bytes stays inline (no block written); one byte larger spills.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_spills_exactly_above_the_threshold() -> Result<()> {
        let inline_n = dialog_search_tree::Manifest::default().inline_n as usize;
        // `encode_value` of a String is the 0x00-escaped bytes plus a
        // terminator; for an all-ASCII string with no NULs that is len + 1. So
        // a string of `inline_n - 1` ASCII bytes encodes to exactly `inline_n`.
        let at = Value::String("a".repeat(inline_n - 1));
        let over = Value::String("a".repeat(inline_n));
        assert_eq!(
            crate::encode_value_owned(&at).len(),
            inline_n,
            "at boundary"
        );
        assert!(
            crate::encode_value_owned(&over).len() > inline_n,
            "over boundary"
        );

        for (value, should_spill) in [(at, false), (over, true)] {
            let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
            let entity = Entity::new()?;
            artifacts
                .commit(vec![Instruction::Assert(Artifact {
                    the: Attribute::from_str("doc/body")?,
                    of: entity.clone(),
                    is: value.clone(),
                    cause: None,
                })])
                .await?;
            let block = artifacts.storage.get(&value.to_reference()).await?;
            assert_eq!(
                block.is_some(),
                should_spill,
                "spill decision at the exact boundary is inclusive"
            );
            // Either way the value reconstructs.
            let results = artifacts
                .select(ArtifactSelector::new().of(entity))
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
                .await;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].is, value);
        }
        Ok(())
    }

    /// Every reconstructable spillable value type round-trips through a spill:
    /// String and Bytes (including a `0x00`-escape case) reconstruct exactly.
    /// `Record` reconstruction from raw bytes is unimplemented workspace-wide
    /// (see `Value::try_from`), which is orthogonal to spilling; it is not
    /// exercised here.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_spills_and_round_trips_every_value_type() -> Result<()> {
        let n = dialog_search_tree::Manifest::default().inline_n as usize + 64;
        let values = vec![
            Value::String("s".repeat(n)),
            Value::Bytes(vec![0xABu8; n]),
            Value::Bytes({
                let mut v = vec![0u8; n];
                v[0] = 0x00; // exercise the 0x00-escape in the encoding
                v
            }),
        ];
        for value in values {
            assert!(
                crate::encode_value_owned(&value).len()
                    > dialog_search_tree::Manifest::default().inline_n as usize,
                "value must spill: {value:?}"
            );
            let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
            let entity = Entity::new()?;
            artifacts
                .commit(vec![Instruction::Assert(Artifact {
                    the: Attribute::from_str("doc/body")?,
                    of: entity.clone(),
                    is: value.clone(),
                    cause: None,
                })])
                .await?;
            let results = artifacts
                .select(ArtifactSelector::new().of(entity))
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
                .await;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].is, value, "{value:?} round-trips through spill");
        }
        Ok(())
    }

    /// Replacing a fact whose prior value was spilled supersedes the prior
    /// (reconstructed via the block) and leaves exactly the new value, whether
    /// the new value is itself spilled or inline.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_replaces_a_spilled_prior() -> Result<()> {
        let n = dialog_search_tree::Manifest::default().inline_n as usize + 8;
        let spilled_prior = Value::String("p".repeat(n));

        for new_value in [Value::String("r".repeat(n)), Value::String("small".into())] {
            let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
            let entity = Entity::new()?;
            let attribute = Attribute::from_str("doc/body")?;
            let of_the = |is: Value| Artifact {
                the: attribute.clone(),
                of: entity.clone(),
                is,
                cause: None,
            };
            artifacts
                .commit(vec![Instruction::Replace(of_the(spilled_prior.clone()))])
                .await?;
            artifacts
                .commit(vec![Instruction::Replace(of_the(new_value.clone()))])
                .await?;

            let results = artifacts
                .select(ArtifactSelector::new().of(entity).the(attribute.clone()))
                .map(|r| r.unwrap())
                .collect::<Vec<_>>()
                .await;
            assert_eq!(results.len(), 1, "cardinality-one keeps one value");
            assert_eq!(
                results[0].is, new_value,
                "the new value supersedes the spilled prior"
            );
        }
        Ok(())
    }

    /// Retracting a fact whose value spilled removes it from the scan (a
    /// tombstone), and no fact is returned.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_retracts_a_spilled_fact() -> Result<()> {
        let n = dialog_search_tree::Manifest::default().inline_n as usize + 8;
        let value = Value::String("t".repeat(n));
        let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
        let entity = Entity::new()?;
        let fact = Artifact {
            the: Attribute::from_str("doc/body")?,
            of: entity.clone(),
            is: value.clone(),
            cause: None,
        };
        artifacts
            .commit(vec![Instruction::Assert(fact.clone())])
            .await?;
        artifacts.commit(vec![Instruction::Retract(fact)]).await?;

        let results = artifacts
            .select(ArtifactSelector::new().of(entity))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert!(
            results.is_empty(),
            "a retracted spilled fact is not returned"
        );
        Ok(())
    }

    /// Two facts with the same large value share one content-addressed block:
    /// the block is stored once under the shared reference, and both facts
    /// reconstruct it.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_dedups_a_shared_spilled_block() -> Result<()> {
        let n = dialog_search_tree::Manifest::default().inline_n as usize + 8;
        let value = Value::String("d".repeat(n));
        let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
        let attribute = Attribute::from_str("doc/body")?;
        let a = Entity::new()?;
        let b = Entity::new()?;
        artifacts
            .commit(vec![
                Instruction::Assert(Artifact {
                    the: attribute.clone(),
                    of: a,
                    is: value.clone(),
                    cause: None,
                }),
                Instruction::Assert(Artifact {
                    the: attribute.clone(),
                    of: b,
                    is: value.clone(),
                    cause: None,
                }),
            ])
            .await?;

        // One block under the shared reference; both facts read it.
        assert_eq!(
            artifacts.storage.get(&value.to_reference()).await?,
            Some(value.to_bytes())
        );
        let results = artifacts
            .select(ArtifactSelector::new().the(attribute))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(results.len(), 2, "two facts share one spilled block");
        assert!(results.iter().all(|r| r.is == value));
        Ok(())
    }

    /// A missing spilled block surfaces a clean error (not a panic) on read.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_errors_when_a_spilled_block_is_missing() -> Result<()> {
        use crate::EntityKey;
        use crate::tree::fetch_spilled;
        let n = dialog_search_tree::Manifest::default().inline_n as usize + 8;
        let value = Value::String("m".repeat(n));
        let artifact = Artifact {
            the: Attribute::from_str("doc/body")?,
            of: Entity::new()?,
            is: value.clone(),
            cause: None,
        };
        let key = EntityKey::from(&artifact).into_key();
        // A store that never had the block written.
        let empty = MemoryStorageBackend::<dialog_storage::Blake3Hash, Vec<u8>>::default();
        let result = fetch_spilled(&empty, &key).await;
        assert!(
            matches!(result, Err(DialogArtifactsError::InvalidValue(_))),
            "a missing spilled block is a clean error, got {result:?}"
        );
        Ok(())
    }

    /// A value-equality select on a spilled value returns exactly that fact:
    /// the selector's value reference matches the spilled key's reference.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_selects_by_a_spilled_value() -> Result<()> {
        let n = dialog_search_tree::Manifest::default().inline_n as usize + 8;
        let wanted = Value::String("w".repeat(n));
        let other = Value::String("o".repeat(n));
        let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;
        let attribute = Attribute::from_str("doc/body")?;
        for value in [wanted.clone(), other.clone()] {
            artifacts
                .commit(vec![Instruction::Assert(Artifact {
                    the: attribute.clone(),
                    of: Entity::new()?,
                    is: value,
                    cause: None,
                })])
                .await?;
        }
        let results = artifacts
            .select(ArtifactSelector::new().is(wanted.clone()))
            .map(|r| r.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(
            results.len(),
            1,
            "equality-by-spilled-value returns one fact"
        );
        assert_eq!(results[0].is, wanted);
        Ok(())
    }

    /// Real-data footprint on an on-disk backend. Gated on `DIALOG_IMPORT_CSV`.
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    #[cfg(not(target_arch = "wasm32"))]
    #[allow(clippy::absolute_paths)]
    async fn it_reports_real_data_footprint() -> anyhow::Result<()> {
        use std::str::FromStr;
        use std::sync::Arc;

        let Ok(csv_path) = std::env::var("DIALOG_IMPORT_CSV") else {
            eprintln!("DIALOG_IMPORT_CSV not set; skipping real-data footprint harness");
            return Ok(());
        };

        // Parse the CSV into artifacts (the columns are the,of,as,is,cause;
        // `as` is the value type). A minimal RFC-4180 parser handling quoted
        // fields with embedded commas, doubled quotes, and newlines (the tonk
        // export puts multi-line HTML/CSS in the `is` column), so the harness
        // has no CSV dependency and runs unchanged on the old tag.
        fn parse_csv(text: &str) -> Vec<Vec<String>> {
            let mut records = Vec::new();
            let mut record = Vec::new();
            let mut field = String::new();
            let mut in_quotes = false;
            let mut chars = text.chars().peekable();
            while let Some(ch) = chars.next() {
                match ch {
                    '"' if in_quotes && chars.peek() == Some(&'"') => {
                        field.push('"');
                        chars.next();
                    }
                    '"' => in_quotes = !in_quotes,
                    ',' if !in_quotes => record.push(std::mem::take(&mut field)),
                    '\n' if !in_quotes => {
                        record.push(std::mem::take(&mut field));
                        records.push(std::mem::take(&mut record));
                    }
                    '\r' if !in_quotes => {}
                    _ => field.push(ch),
                }
            }
            if !field.is_empty() || !record.is_empty() {
                record.push(field);
                records.push(record);
            }
            records
        }

        let text = std::fs::read_to_string(&csv_path)?;
        let mut artifacts_in = Vec::new();
        let mut reserved_skipped = 0usize;
        for record in parse_csv(&text).into_iter().skip(1) {
            if record.len() < 4 {
                continue;
            }
            // The `dialog.` namespace is reserved for version-control
            // records on this branch, so real-world facts under it cannot be
            // asserted through the public API. Skip them and report the count,
            // so the footprint is over the facts actually imported.
            if record[0].starts_with("dialog.") {
                reserved_skipped += 1;
                continue;
            }
            let Ok(the) = Attribute::from_str(&record[0]) else {
                continue;
            };
            let Ok(of) = Entity::from_str(&record[1]) else {
                continue;
            };
            let value_type = record[2].as_str();
            let raw = record[3].as_str();
            let parsed = match value_type {
                "text" => Some(Value::String(raw.to_owned())),
                "entity" => Entity::from_str(raw).ok().map(Value::Entity),
                "natural" => raw.parse().ok().map(Value::UnsignedInt),
                "integer" => raw.parse().ok().map(Value::SignedInt),
                "float" => raw.parse().ok().map(Value::Float),
                "boolean" => bool::from_str(raw).ok().map(Value::Boolean),
                "attribute" => Attribute::from_str(raw).ok().map(Value::Symbol),
                // Skip rows whose value type this minimal parser does not
                // handle (bytes/record are base58 and not needed for a size
                // comparison of the common text/entity/numeric mix).
                _ => None,
            };
            let Some(is) = parsed else {
                continue;
            };
            artifacts_in.push(Artifact {
                the,
                of,
                is,
                cause: None,
            });
        }
        let fact_count = artifacts_in.len();

        let root = tempfile::tempdir()?;
        let backend = dialog_storage::FileSystemStorageBackend::<crate::Blake3Hash, Vec<u8>>::new(
            root.path(),
        )
        .await?;
        let measured = Arc::new(tokio::sync::Mutex::new(
            dialog_storage::MeasuredStorage::new(backend),
        ));
        let mut facts = Artifacts::anonymous(measured.clone()).await?;

        // Commit incrementally so a malformed key names the artifact that
        // produced it (set DIALOG_IMPORT_BISECT to enable; otherwise commit in
        // one batch for the timing figure).
        let commit_start = std::time::Instant::now();
        if std::env::var("DIALOG_IMPORT_BISECT").is_ok() {
            for (index, artifact) in artifacts_in.iter().enumerate() {
                if let Err(error) = facts
                    .commit(std::iter::once(Instruction::Assert(artifact.clone())))
                    .await
                {
                    panic!(
                        "commit failed at row {index}: {error}\n  the={} of={} is={:?}",
                        artifact.the, artifact.of, artifact.is
                    );
                }
            }
        } else {
            facts
                .commit(artifacts_in.iter().cloned().map(Instruction::Assert))
                .await?;
        }
        let commit_elapsed = commit_start.elapsed();

        let (write_bytes, writes) = {
            let storage = measured.lock().await;
            (storage.write_bytes(), storage.writes())
        };

        // Time a scan over the most common attribute in the data set.
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for artifact in &artifacts_in {
            *counts.entry(artifact.the.to_string()).or_default() += 1;
        }
        let (top_attribute, top_count) = counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .unwrap_or_default();

        let scan_start = std::time::Instant::now();
        let selected: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the(Attribute::from_str(&top_attribute)?))
            .try_collect()
            .await?;
        let scan_elapsed = scan_start.elapsed();

        eprintln!(
            "REALDATA facts={fact_count} reserved_skipped={reserved_skipped} write_bytes={write_bytes} writes={writes} \
             bytes/fact={:.1} bytes/entry={:.1} blocks/fact={:.3} \
             commit={commit_elapsed:?} \
             scan(the={top_attribute} n={top_count})={scan_elapsed:?} results={}",
            write_bytes as f64 / fact_count as f64,
            write_bytes as f64 / fact_count as f64 / 3.0,
            writes as f64 / fact_count as f64,
            selected.len(),
        );

        Ok(())
    }
}
