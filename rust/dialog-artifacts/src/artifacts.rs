mod artifact;
pub use artifact::*;

mod revision;
pub use revision::*;

mod data;
pub use data::*;

mod instruction;
pub use instruction::*;

pub mod selector;
pub use selector::ArtifactSelector;

mod store;
pub use store::*;

mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

mod cause;
pub use cause::*;

mod r#match;
pub use r#match::*;

use base58::ToBase58;
use rand::{Rng, distributions::Alphanumeric};

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_prolly_tree::{Entry, GeometricDistribution, Tree};
pub use dialog_storage::{
    Blake3Hash, CborEncoder, ContentAddressedStorage, DialogStorageError, Encoder, Storage,
    StorageBackend,
};
use futures_util::{Stream, StreamExt};
use std::{ops::Range, sync::Arc};
use tokio::sync::RwLock;

#[cfg(feature = "csv")]
use futures_util::TryStreamExt;

#[cfg(feature = "csv")]
use async_stream::stream;

use crate::{
    AttributeKey, BRANCH_FACTOR, DialogArtifactsError, EntityKey, HASH_SIZE, State, ValueKey,
    artifacts::selector::Constrained, make_reference,
};

/// An alias type that describes the [`Tree`]-based prolly tree that is
/// used for each index in [`Artifacts`]
pub type Index<Key, Value, Backend> = Tree<
    BRANCH_FACTOR,
    HASH_SIZE,
    GeometricDistribution,
    Key,
    State<Value>,
    Blake3Hash,
    Storage<HASH_SIZE, CborEncoder, Backend>,
>;

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
    storage: Storage<HASH_SIZE, CborEncoder, Backend>,
    entity_index: Arc<RwLock<Index<EntityKey, Datum, Backend>>>,
    attribute_index: Arc<RwLock<Index<AttributeKey, Datum, Backend>>>,
    value_index: Arc<RwLock<Index<ValueKey, Datum, Backend>>>,
}

impl<Backend> Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
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
        let (entity_index, attribute_index, value_index) = {
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
                tokio::try_join!(
                    Tree::from_hash(revision.entity_index(), storage.clone()),
                    Tree::from_hash(revision.attribute_index(), storage.clone()),
                    Tree::from_hash(revision.value_index(), storage.clone())
                )?
            } else {
                (
                    Tree::new(storage.clone()),
                    Tree::new(storage.clone()),
                    Tree::new(storage.clone()),
                )
            }
        };

        Ok(Self {
            identifier,
            storage,
            entity_index: Arc::new(RwLock::new(entity_index)),
            attribute_index: Arc::new(RwLock::new(attribute_index)),
            value_index: Arc::new(RwLock::new(value_index)),
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
        Write: tokio::io::AsyncWrite + Unpin,
    {
        let mut csv = csv_async::AsyncSerializer::from_writer(write);

        let entity_index = self.entity_index.read().await;
        let entity_stream = entity_index.stream();

        tokio::pin!(entity_stream);

        while let Some(entry) = entity_stream.try_next().await? {
            let Entry { value, .. } = entry;

            if let State::Added(datum) = value {
                let artifact = Artifact::try_from(datum)?;

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
        Read: tokio::io::AsyncRead + Unpin + Send,
    {
        let instructions = stream! {
            let mut reader = csv_async::AsyncReaderBuilder::new()
                .create_deserializer(read);

            let stream = reader.deserialize::<Artifact>();

            for await artifact in stream {
                if let Ok(artifact) = artifact {
                    yield Instruction::Assert(artifact)
                }
            }
        };

        ArtifactStoreMut::commit(self, instructions).await?;

        Ok(())
    }

    /// Get the hash that represents the [`ArtifactStore`] at its current version.
    pub async fn revision(&self) -> Result<Blake3Hash, DialogArtifactsError> {
        let (entity_index, attribute_index, value_index) = tokio::join!(
            self.entity_index.read(),
            self.attribute_index.read(),
            self.value_index.read()
        );

        Ok(
            match (
                entity_index.hash(),
                attribute_index.hash(),
                value_index.hash(),
            ) {
                (Some(entity_version), Some(attribute_version), Some(value_version)) => {
                    Revision::from((
                        entity_version.to_owned(),
                        attribute_version.to_owned(),
                        value_version.to_owned(),
                    ))
                    .as_reference()
                    .await?
                }
                _ => NULL_REVISION_HASH,
            },
        )
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
        let (entity_version, attribute_version, value_version) =
            // The null revision does not actually exists it just represents
            // empty indexes so we set all versions to None's.
            if required_hash == NULL_REVISION_HASH {
                (None, None, None)
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
                (
                    Some(revision.entity_index().to_owned()),
                    Some(revision.attribute_index().to_owned()),
                    Some(revision.value_index().to_owned()),
                )
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

        // Finally update all the indexes
        let (mut entity_index, mut attribute_index, mut value_index) = tokio::join!(
            self.entity_index.write(),
            self.attribute_index.write(),
            self.value_index.write()
        );
        tokio::try_join!(
            entity_index.set_hash(entity_version),
            attribute_index.set_hash(attribute_version),
            value_index.set_hash(value_version),
        )?;

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
        let entity_index = self.entity_index.clone();
        let attribute_index = self.attribute_index.clone();
        let value_index = self.value_index.clone();

        try_stream! {
            // We clone to "pin" the indexes at a version for the lifetime of the stream
            let entity_index = entity_index.read().await.clone();
            let attribute_index = attribute_index.read().await.clone();
            let value_index = value_index.read().await.clone();

            if selector.entity().is_some() {
                let start = EntityKey::min().apply_selector(&selector);
                let end = EntityKey::max().apply_selector(&selector);

                let stream = entity_index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else if selector.value().is_some() {
                let start = ValueKey::min().apply_selector(&selector);
                let end = ValueKey::max().apply_selector(&selector);

                let stream = value_index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else if selector.attribute().is_some() {
                let start = AttributeKey::min().apply_selector(&selector);
                let end = AttributeKey::max().apply_selector(&selector);

                let stream = attribute_index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { value: State::Added(datum), .. } = entry {
                            yield Artifact::try_from(datum)?;
                        }
                    }
                }
            } else {
                unreachable!("ArtifactSelector will always have at least one field specified")
            };
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
            let (mut entity_index, mut attribute_index, mut value_index) = tokio::join!(
                self.entity_index.write(),
                self.attribute_index.write(),
                self.value_index.write()
            );

            tokio::pin!(instructions);

            while let Some(instruction) = instructions.next().await {
                match instruction {
                    Instruction::Assert(artifact) => {
                        let entity_key = EntityKey::from(&artifact);
                        let value_key = ValueKey::from(&entity_key);
                        let attribute_key = AttributeKey::from(&entity_key);

                        let datum = Datum::from(artifact);

                        if let Some(cause) = &datum.cause {
                            let ancestor_key = {
                                let search_start = EntityKey::min()
                                    .set_entity(entity_key.entity())
                                    .set_attribute(entity_key.attribute());
                                let search_end = EntityKey::max()
                                    .set_entity(entity_key.entity())
                                    .set_attribute(entity_key.attribute());

                                let search_stream =
                                    entity_index.stream_range(search_start..search_end);

                                let mut ancestor_key = None;

                                tokio::pin!(search_stream);

                                while let Some(candidate) = search_stream.try_next().await? {
                                    if let State::Added(current_element) = candidate.value {
                                        let current_artifact = Artifact::try_from(current_element)?;
                                        let current_artifact_reference =
                                            Cause::from(&current_artifact);

                                        if cause == &current_artifact_reference {
                                            ancestor_key = Some(candidate.key);
                                            break;
                                        }
                                    }
                                }

                                ancestor_key
                            };

                            if let Some(entity_key) = ancestor_key {
                                // Prune the old entry from the indexes
                                let value_key = ValueKey::from(&entity_key);
                                let attribute_key = AttributeKey::from(&entity_key);

                                tokio::try_join!(
                                    value_index.delete(&value_key),
                                    attribute_index.delete(&attribute_key),
                                    entity_index.delete(&entity_key),
                                )?;
                            }
                        }

                        tokio::try_join!(
                            attribute_index.set(attribute_key, State::Added(datum.clone())),
                            value_index.set(value_key, State::Added(datum.clone())),
                            entity_index.set(entity_key, State::Added(datum)),
                        )?;
                    }
                    Instruction::Retract(fact) => {
                        tokio::try_join!(
                            entity_index.set(EntityKey::from(&fact), State::Removed,),
                            attribute_index.set(AttributeKey::from(&fact), State::Removed),
                            value_index.set(ValueKey::from(&fact), State::Removed),
                        )?;
                    }
                }
            }

            let next_revision = match (
                entity_index.hash(),
                attribute_index.hash(),
                value_index.hash(),
            ) {
                (Some(entity_index), Some(attribute_index), Some(value_index)) => Some(
                    Revision::from((*entity_index, *attribute_index, *value_index)),
                ),
                _ => None,
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
    use std::{collections::BTreeSet, str::FromStr, sync::Arc};

    use anyhow::Result;
    use dialog_storage::{
        MeasuredStorage, MemoryStorageBackend, StorageBackend, make_target_storage,
    };
    use futures_util::{StreamExt, TryStreamExt};
    use tokio::sync::Mutex;

    use crate::{
        Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMutExt, Artifacts, Attribute,
        DialogArtifactsError, Entity, Instruction, NULL_REVISION_HASH, Value, generate_data,
        make_reference,
    };

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

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

            let mut csv = tokio::io::BufWriter::new(Vec::<u8>::new());
            artifacts.export(&mut csv).await?;
            (csv.into_inner(), ids, artifacts.revision().await)
        };

        println!("{}", String::from_utf8(csv.clone())?);

        let mut artifacts = Artifacts::anonymous(MemoryStorageBackend::default()).await?;

        artifacts
            .import(&mut tokio::io::BufReader::new(csv.as_ref()))
            .await?;

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

        assert_eq!(net_reads, 1);
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

        assert_eq!(net_reads, 2);
        assert_eq!(net_writes, 0);

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

        assert_eq!(net_reads, 3);
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

        assert_eq!(net_reads, 7);
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
            the: attribute,
            of: entity.clone(),
            is: Value::Boolean(false),
            cause: None,
        };

        artifacts
            .commit([Instruction::Assert(artifact.clone())])
            .await?;

        let updated_artifact = artifact.update(Value::Boolean(true));

        artifacts
            .commit([Instruction::Assert(updated_artifact.clone())])
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
}
