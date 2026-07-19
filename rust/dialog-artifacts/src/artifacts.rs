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
        let base_revision = self.revision().await?;

        let transaction_result = async {
            let mut index = self.index.write().await;

            // The per-instruction EAV/AEV/VAE key writes (and
            // cardinality-one supersession) are the shared
            // `ArtifactTreeExt::apply`. `commit` adds only the
            // surrounding transaction bookkeeping — base-revision
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
        Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMutExt, Artifacts, Attribute,
        DialogArtifactsError, Entity, Instruction, NULL_REVISION_HASH, Value, make_reference,
    };

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

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

    /// Assert + retract of a fact whose value was **already committed** ends in
    /// a retraction: a `Removed` tombstone replaces the live value, so the tree
    /// changes (the removal must propagate on merge and beat a stale remote
    /// assert) and the fact is no longer queryable.
    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_tombstones_when_retracting_a_fact_that_had_a_committed_value() -> Result<()> {
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

        // The retraction changed the tree (a tombstone replaced the value).
        assert_ne!(
            committed_root, after_root,
            "retracting a committed fact must write a tombstone (tree changes)"
        );
        // The fact is gone from queries.
        let hits: Vec<Artifact> = facts
            .select(ArtifactSelector::new().the(name))
            .try_collect()
            .await?;
        assert!(
            hits.is_empty(),
            "the retracted fact must not be queryable after the tombstone"
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

        // Regression guard against the pre-M3 flat baseline of ~172 B/entry
        // (which duplicated the whole fact in the payload). After the value is
        // in the key and the payload is just `State<Cause>`, an entry is well
        // under that.
        assert!(
            bytes_per_entry < 172.0,
            "per-entry size regressed to {bytes_per_entry:.1} bytes/entry (pre-M3 was ~172)"
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
}
