mod artifact;

pub use artifact::*;

mod revision;
use base58::ToBase58;
use rand::{Rng, distr::Alphanumeric};
pub use revision::*;

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

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_prolly_tree::{Entry, GeometricDistribution, Tree};
use dialog_storage::{
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
    AttributeKey, BRANCH_FACTOR, DialogArtifactsError, EntityDatum, EntityKey, HASH_SIZE, State,
    ValueDatum, ValueKey, ValueReferenceKeyPart, artifacts::selector::Constrained, make_reference,
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
    entity_index: Arc<RwLock<Index<EntityKey, ValueDatum, Backend>>>,
    attribute_index: Arc<RwLock<Index<AttributeKey, ValueDatum, Backend>>>,
    value_index: Arc<RwLock<Index<ValueKey, EntityDatum, Backend>>>,
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
        let (entity_index, attribute_index, value_index) =
            if let Some(revision) = storage.get(&make_reference(identifier.as_bytes())).await? {
                let revision = CborEncoder.decode::<Revision>(&revision).await?;

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
        let identifier = rand::rng()
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
            let Entry { key, value } = entry;

            if let State::Added(datum) = value {
                let artifact = Artifact::try_from((key, datum))?;

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
                _ => NULL_REVISION.as_reference().await?,
            },
        )
    }

    /// Reset the root of the database to `revision` if provided, or else reset
    /// to the stored root if available, or else to an empty database.
    pub async fn reset(
        &mut self,
        revision: Option<Blake3Hash>,
    ) -> Result<(), DialogArtifactsError> {
        let (mut entity_index, mut attribute_index, mut value_index) = tokio::join!(
            self.entity_index.write(),
            self.attribute_index.write(),
            self.value_index.write()
        );

        let revision = if let Some(revision) = revision {
            self.storage
                .read::<Revision>(&revision)
                .await?
                .ok_or_else(|| {
                    DialogArtifactsError::InvalidRevision(format!(
                        "Block ({}) not found in storage",
                        revision.to_base58()
                    ))
                })?
        } else {
            let block = self
                .storage
                .get(&make_reference(self.identifier().as_bytes()))
                .await?;
            if let Some(block) = block {
                CborEncoder.decode::<Revision>(&block).await?
            } else {
                NULL_REVISION.clone()
            }
        };

        self.storage
            .set(
                make_reference(self.identifier.as_bytes()),
                CborEncoder.encode(&revision).await?.1,
            )
            .await?;

        let entity_index_hash = Some(revision.entity_index().to_owned());
        let attribute_index_hash = Some(revision.attribute_index().to_owned());
        let value_index_hash = Some(revision.value_index().to_owned());

        tokio::try_join!(
            entity_index.set_hash(entity_index_hash),
            attribute_index.set_hash(attribute_index_hash),
            value_index.set_hash(value_index_hash),
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

            let entity = selector.entity();
            let attribute = selector.attribute();
            let value = selector.value();

            if let Some(entity) = entity {
                let mut start = EntityKey::min().set_entity(entity.into());
                let mut end = EntityKey::max().set_entity(entity.into());

                if let Some(attribute) = attribute {
                    start = start.set_attribute(attribute.into());
                    end = end.set_attribute(attribute.into());
                }

                let stream = entity_index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { key, value: State::Added(datum) } = entry {
                            yield Artifact::try_from((key, datum))?;
                        }
                    }
                }
            } else if let Some(value) = value {
                let value_reference = value.to_reference();

                let mut start = ValueKey::min()
                    .set_value_type(value.data_type())
                    .set_value_reference(ValueReferenceKeyPart(&value_reference));
                let mut end = ValueKey::max()
                    .set_value_type(value.data_type())
                    .set_value_reference(ValueReferenceKeyPart(&value_reference));


                if let Some(attribute) = attribute {
                    start = start.set_attribute(attribute.into());
                    end = end.set_attribute(attribute.into());
                }

                let stream = value_index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { key, value: State::Added(_) } = entry {
                                let key = EntityKey::default()
                                    .set_entity(key.entity())
                                    .set_attribute(key.attribute())
                                    .set_value_type(key.value_type());

                                let Some(State::Added(datum)) = entity_index.get(&key).await? else {
                                    return Err(DialogArtifactsError::MalformedIndex(format!("Missing datum for key {:?}", key)))?;
                                };
                                yield Artifact::try_from((key, datum))?;
                        }
                    }
                }
            } else if let Some(attribute) = attribute {
                let start = AttributeKey::min().set_attribute(attribute.into());
                let end = AttributeKey::max().set_attribute(attribute.into());

                let stream = attribute_index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { key, value: State::Added(datum) } = entry {
                            yield Artifact::try_from((key, datum))?;
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
    ) -> Result<Revision, DialogArtifactsError>
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
                        let value_datum =
                            ValueDatum::new(artifact.is.clone(), artifact.cause.clone());
                        let entity_key = EntityKey::from(&artifact);

                        if let Some(cause) = &artifact.cause {
                            if let Some(State::Added(current_element)) =
                                entity_index.get(&entity_key).await?
                            {
                                let current_artifact = Artifact::try_from((
                                    entity_key.clone(),
                                    current_element.clone(),
                                ))?;
                                let current_artifact_reference = Cause::from(&current_artifact);

                                if cause == &current_artifact_reference {
                                    // Prune the old entry from the value index
                                    let value_key = ValueKey::from(&current_artifact);
                                    value_index.delete(&value_key).await?;
                                }
                            }
                        }

                        tokio::try_join!(
                            entity_index.set(entity_key, State::Added(value_datum.clone())),
                            attribute_index
                                .set(AttributeKey::from(&artifact), State::Added(value_datum)),
                            value_index
                                .set(ValueKey::from(&artifact), State::Added(EntityDatum {})),
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
                (Some(entity_index), Some(attribute_index), Some(value_index)) => {
                    Revision::from((*entity_index, *attribute_index, *value_index))
                }
                _ => NULL_REVISION.clone(),
            };

            // Advance the effective pointer to the latest version of this DB
            self.storage
                .set(
                    make_reference(self.identifier.as_bytes()),
                    CborEncoder.encode(&next_revision).await?.1,
                )
                .await?;

            Ok(next_revision) as Result<Revision, DialogArtifactsError>
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
    use dialog_storage::{MeasuredStorage, MemoryStorageBackend, make_target_storage};
    use futures_util::{StreamExt, TryStreamExt};
    use tokio::sync::Mutex;

    use crate::{
        Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMutExt, Artifacts, Attribute,
        DialogArtifactsError, Entity, Instruction, Value, generate_data,
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
                of: Entity::new(),
                is: Value::String("Foo Bar".into()),
                cause: None,
            },
            Artifact {
                the: Attribute::from_str("profile/name")?,
                of: Entity::new(),
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

        assert_eq!(net_reads, 5);
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

        assert_eq!(net_reads, 5);
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

        assert_eq!(net_reads, 9);
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
                of: Entity::new(),
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
                of: Entity::new(),
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
        let entity = Entity::new();
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
}
