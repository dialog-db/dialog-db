mod artifact;
pub use artifact::*;

mod revision;
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
use dialog_storage::{CborEncoder, DialogStorageError, Storage, StorageBackend};
use futures_util::Stream;
use std::{ops::Range, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    AttributeKey, BRANCH_FACTOR, DialogArtifactsError, EntityDatum, EntityKey, EntityKeyPart,
    HASH_SIZE, State, ValueDatum, ValueKey, ValueReferenceKeyPart,
    artifacts::selector::Constrained,
};

/// The representation of the hash type (BLAKE3, in this case) that must be used
/// by a [`StorageBackend`] that may back an instance of [`Artifacts`].
pub type Blake3Hash = [u8; HASH_SIZE];

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
    /// Initialize a new [`Artifacts`] with the provided [`StorageBackend`].
    pub fn new(backend: Backend) -> Self {
        Self {
            entity_index: Arc::new(RwLock::new(Tree::new(Storage {
                encoder: CborEncoder,
                backend: backend.clone(),
            }))),
            attribute_index: Arc::new(RwLock::new(Tree::new(Storage {
                encoder: CborEncoder,
                backend: backend.clone(),
            }))),
            value_index: Arc::new(RwLock::new(Tree::new(Storage {
                encoder: CborEncoder,
                backend: backend.clone(),
            }))),
        }
    }

    /// Attempt to initialize the [`Artifacts`] at a specific [`Version`].
    pub async fn restore(
        version: Revision,
        backend: Backend,
    ) -> Result<Self, DialogArtifactsError> {
        // let entity_index = Tree::from_hash(
        //     version.entity(),
        //     Storage {
        //         encoder: CborEncoder,
        //         backend: backend.clone(),
        //     },
        // );

        let storage = Storage {
            encoder: CborEncoder,
            backend,
        };

        let (entity_index, attribute_index, value_index) = tokio::try_join!(
            Tree::from_hash(version.entity(), storage.clone()),
            Tree::from_hash(version.attribute(), storage.clone()),
            Tree::from_hash(version.value(), storage)
        )?;

        Ok(Self {
            entity_index: Arc::new(RwLock::new(
                entity_index as Index<EntityKey, ValueDatum, Backend>,
            )),
            attribute_index: Arc::new(RwLock::new(
                attribute_index as Index<AttributeKey, ValueDatum, Backend>,
            )),
            value_index: Arc::new(RwLock::new(
                value_index as Index<ValueKey, EntityDatum, Backend>,
            )),
        })
    }

    /// Get the hash that represents the [`ArtifactStore`] at its current version.
    pub async fn revision(&self) -> Option<Revision> {
        let (entity_index, attribute_index, value_index) = tokio::join!(
            self.entity_index.read(),
            self.attribute_index.read(),
            self.value_index.read()
        );

        match (
            entity_index.hash(),
            attribute_index.hash(),
            value_index.hash(),
        ) {
            (Some(entity_version), Some(attribute_version), Some(value_version)) => {
                Some(Revision::from((
                    entity_version.to_owned(),
                    attribute_version.to_owned(),
                    value_version.to_owned(),
                )))
            }
            _ => None,
        }
    }

    pub(crate) async fn reset(
        &self,
        revision: Option<Revision>,
    ) -> Result<(), DialogArtifactsError> {
        let (mut entity_index, mut attribute_index, mut value_index) = tokio::join!(
            self.entity_index.write(),
            self.attribute_index.write(),
            self.value_index.write()
        );

        let entity_index_hash = revision.as_ref().map(|revision| *revision.entity());
        let attribute_index_hash = revision.as_ref().map(|revision| *revision.attribute());
        let value_index_hash = revision.as_ref().map(|revision| *revision.value());

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

                let index = entity_index.read().await;
                let stream = index.stream_range(Range { start, end });

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

                let index = value_index.read().await;
                let stream = index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    let entry = item?;

                    if entry.matches_selector(&selector) {
                        if let Entry { key, value: State::Added(datum) } = entry {
                            let key = EntityKey::default()
                                .set_entity(EntityKeyPart(&datum))
                                .set_attribute(key.attribute())
                                .set_value_type(key.value_type());

                            let entity_index = entity_index.read().await;
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

                let index = attribute_index.read().await;
                let stream = index.stream_range(Range { start, end });

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
    async fn commit<I>(&mut self, instructions: I) -> Result<(), DialogArtifactsError>
    where
        I: IntoIterator<Item = Instruction> + ConditionalSend,
        I::IntoIter: ConditionalSend,
    {
        let base_revision = self.revision().await;

        let transaction_result = async {
            let (mut entity_index, mut attribute_index, mut value_index) = tokio::join!(
                self.entity_index.write(),
                self.attribute_index.write(),
                self.value_index.write()
            );

            for instruction in instructions {
                match instruction {
                    Instruction::Assert(fact) => {
                        let value_datum = ValueDatum::new(fact.is.clone(), fact.cause.clone());
                        let entity_key = EntityKey::from(&fact);

                        if let Some(cause) = &fact.cause {
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
                                .set(AttributeKey::from(&fact), State::Added(value_datum)),
                            value_index.set(
                                ValueKey::from(&fact),
                                State::Added(EntityDatum { entity: *fact.of })
                            ),
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

            Ok(()) as Result<(), DialogArtifactsError>
        }
        .await;

        if transaction_result.is_err() {
            // Rollback
            self.reset(base_revision).await?;
        }

        transaction_result
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use anyhow::Result;
    use dialog_storage::{MeasuredStorageBackend, make_target_storage};
    use futures_util::StreamExt;
    use tokio::sync::Mutex;

    use crate::{
        Artifact, ArtifactSelector, ArtifactStore, ArtifactStoreMut, Artifacts, Attribute,
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
        let mut facts = Artifacts::new(storage_backend);

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
    async fn it_can_query_efficiently_by_entity_and_value() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let storage_backend = Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));
        let data = generate_data(32)?;
        let attribute = Attribute::from_str("item/name")?;
        let name = Value::String("name18".into());
        let entity = data
            .iter()
            .find(|element| element.is == name)
            .unwrap()
            .of
            .clone();

        let mut facts = Artifacts::new(storage_backend.clone());

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
        let storage_backend = Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));
        let data = generate_data(32)?;
        let attribute = Attribute::from_str("item/name")?;
        let name = Value::String("name18".into());
        let entity = data
            .iter()
            .find(|element| element.is == name)
            .unwrap()
            .of
            .clone();

        let mut facts = Artifacts::new(storage_backend.clone());

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

        assert_eq!(net_reads, 4);
        assert_eq!(net_writes, 0);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_indexes_to_optimize_reads() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(256)?.into_iter().map(Instruction::Assert);

        let storage_backend = Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));

        let mut facts = Artifacts::new(storage_backend.clone());

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

        assert_eq!(net_reads, 4);
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
    async fn it_produces_the_same_version_with_different_insertion_order() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(32)?;

        let mut reordered_data = data.clone();
        reordered_data.sort_by(|l, r| l.of.cmp(&r.of));

        assert_ne!(data, reordered_data);

        let into_assert = |fact: Artifact| Instruction::Assert(fact);

        let data = data.into_iter().map(into_assert);
        let reordered_data = reordered_data.into_iter().map(into_assert);

        let storage_backend = Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));

        let mut facts_one = Artifacts::new(storage_backend.clone());

        facts_one.commit(data).await?;

        let mut facts_two = Artifacts::new(storage_backend.clone());

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

        let mut facts = Artifacts::new(storage_backend.clone());

        facts.commit(data.into_iter().map(into_assert)).await?;
        let revision = facts.revision().await.unwrap();

        let restored_facts = Artifacts::restore(revision.clone(), storage_backend).await?;
        let restored_revision = restored_facts.revision().await.unwrap();

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

        let mut artifacts = Artifacts::new(storage_backend.clone());

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
