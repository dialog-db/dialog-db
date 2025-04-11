mod revision;
pub use revision::*;

mod instruction;
pub use instruction::*;

mod selector;
pub use selector::*;

mod store;
pub use store::*;

mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

use async_stream::try_stream;
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_prolly_tree::{BasicEncoder, Entry, GeometricDistribution, Tree, ValueType};
use dialog_storage::{DialogStorageError, Storage, StorageBackend};
use futures_util::Stream;
use std::{ops::Range, sync::Arc};
use tokio::sync::RwLock;

use crate::{
    AttributeKey, BRANCH_FACTOR, DialogArtifactsError, EntityDatum, EntityKey, EntityKeyPart,
    HASH_SIZE, State, ValueDatum, ValueKey, ValueReferenceKeyPart,
};

/// The representation of the hash type (BLAKE3, in this case) that must be used
/// by a [`StorageBackend`] that may back an instance of [`Artifacts`].
pub type Blake3Hash = [u8; HASH_SIZE];

/// A [`Artifact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`FactStore`].
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Artifact {
    /// The [`Attribute`] of the [`Artifact`]; the predicate of the triple
    pub the: Attribute,
    /// The [`Entity`] of the [`Artifact`]; the subject of the triple
    pub of: Entity,
    /// The [`Value`] of the [`Artifact`]; the object of the triple
    pub is: Value,
}

/// An alias type that describes the [`Tree`]-based prolly tree that is
/// used for each index in [`Artifacts`]
pub type Index<Key, Value, Backend> = Arc<
    RwLock<
        Tree<
            BRANCH_FACTOR,
            HASH_SIZE,
            GeometricDistribution,
            Key,
            State<Value>,
            Blake3Hash,
            Storage<HASH_SIZE, BasicEncoder<Key, State<Value>>, Backend>,
        >,
    >,
>;

/// [`Artifacts`] is an implementor of [`FactStore`] and [`FactStoreMut`].
/// Internally, [`Artifacts`] maintains indexes built from [`Tree`]s (that is,
/// prolly trees). These indexes are built up as new [`Artifact`]s are commited,
/// and they are chosen based on [`FactSelector`] shapes when [`Artifact`]s are
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
    entity_index: Index<EntityKey, ValueDatum, Backend>,
    attribute_index: Index<AttributeKey, ValueDatum, Backend>,
    value_index: Index<ValueKey, EntityDatum, Backend>,
}

impl<Backend> Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    /// Initialize a new [`Artifacts`] with the provided [`StorageBackend`].
    pub async fn new(backend: Backend) -> Result<Self, DialogArtifactsError> {
        Ok(Self {
            entity_index: Arc::new(RwLock::new(Tree::new(Storage {
                encoder: BasicEncoder::<EntityKey, State<ValueDatum>>::default(),
                backend: backend.clone(),
            }))),
            attribute_index: Arc::new(RwLock::new(Tree::new(Storage {
                encoder: BasicEncoder::<AttributeKey, State<ValueDatum>>::default(),
                backend: backend.clone(),
            }))),
            value_index: Arc::new(RwLock::new(Tree::new(Storage {
                encoder: BasicEncoder::<ValueKey, State<EntityDatum>>::default(),
                backend: backend.clone(),
            }))),
        })
    }

    /// Attempt to initialize the [`Artifacts`] at a specific [`Version`].
    pub async fn restore(
        version: Revision,
        backend: Backend,
    ) -> Result<Self, DialogArtifactsError> {
        let (entity_index, attribute_index, value_index) = tokio::try_join!(
            Tree::from_hash(
                version.entity(),
                Storage {
                    encoder: BasicEncoder::<EntityKey, State<ValueDatum>>::default(),
                    backend: backend.clone(),
                },
            ),
            Tree::from_hash(
                version.attribute(),
                Storage {
                    encoder: BasicEncoder::<AttributeKey, State<ValueDatum>>::default(),
                    backend: backend.clone(),
                },
            ),
            Tree::from_hash(
                version.value(),
                Storage {
                    encoder: BasicEncoder::<ValueKey, State<EntityDatum>>::default(),
                    backend: backend.clone(),
                },
            )
        )?;

        Ok(Self {
            entity_index: Arc::new(RwLock::new(entity_index)),
            attribute_index: Arc::new(RwLock::new(attribute_index)),
            value_index: Arc::new(RwLock::new(value_index)),
        })
    }

    /// Get the hash that represents the [`FactStore`] at its current version.
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

    async fn reset(&self, revision: Option<Revision>) -> Result<(), DialogArtifactsError> {
        let (mut entity_index, mut attribute_index, mut value_index) = tokio::join!(
            self.entity_index.write(),
            self.attribute_index.write(),
            self.value_index.write()
        );

        let entity_index_hash = revision.as_ref().map(|revision| revision.entity().clone());
        let attribute_index_hash = revision
            .as_ref()
            .map(|revision| revision.attribute().clone());
        let value_index_hash = revision.as_ref().map(|revision| revision.value().clone());

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
impl<Backend> FactStore for Artifacts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync
        + 'static,
{
    fn select(
        &self,
        selector: FactSelector,
    ) -> impl Stream<Item = Result<Artifact, DialogArtifactsError>> + '_ + ConditionalSend {
        try_stream! {
            let FactSelector {
                entity, attribute, value
            } = &selector;

            if let Some(entity) = entity {
                let mut start = EntityKey::min().set_entity(entity.into());
                let mut end = EntityKey::max().set_entity(entity.into());

                if let Some(attribute) = attribute {
                    start = start.set_attribute(attribute.into());
                    end = end.set_attribute(attribute.into());
                }

                let index = self.entity_index.read().await;
                let stream = index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    if let Ok(Entry { key, value: State::Added(datum) }) = item {
                        yield Artifact {
                            the: Attribute::try_from(key.attribute())?,
                            of: Entity::from(key.entity()),
                            is: Value::try_from((key.value_type(), datum.to_vec()))?
                        }
                    }
                }
            } else if let Some(attribute) = attribute {
                let start = AttributeKey::min().set_attribute(attribute.into());
                let end = AttributeKey::max().set_attribute(attribute.into());

                let index = self.attribute_index.read().await;
                let stream = index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    if let Ok(Entry { key, value: State::Added(datum) }) = item {
                        yield Artifact {
                            the: Attribute::try_from(key.attribute())?,
                            of: Entity::from(key.entity()),
                            is: Value::try_from((key.value_type(), datum.to_vec()))?
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

                let index = self.value_index.read().await;
                let stream = index.stream_range(Range { start, end });

                tokio::pin!(stream);

                for await item in stream {
                    if let Ok(Entry { key, value: State::Added(datum) }) = item {
                        let key = EntityKey::default()
                            .set_entity(EntityKeyPart(&datum))
                            .set_attribute(key.attribute())
                            .set_value_type(key.value_type());

                        let entity_index = self.entity_index.read().await;
                        let Some(State::Added(datum)) = entity_index.get(&key).await? else {
                            return Err(DialogArtifactsError::MalformedIndex(format!("Missing datum for key {:?}", key)))?;
                        };

                        yield Artifact {
                            the: Attribute::try_from(key.attribute())?,
                            of: Entity::from(key.entity()),
                            is: Value::try_from((key.value_type(), datum.to_vec()))?
                        }
                    }
                }
            } else {
                // insanitywolf.webp
                let index = self.entity_index.read().await;
                let stream = index.stream();

                for await item in stream {
                    if let Ok(Entry { key, value: State::Added(datum) }) = item {
                        yield Artifact {
                            the: Attribute::try_from(key.attribute())?,
                            of: Entity::from(key.entity()),
                            is: Value::try_from((key.value_type(), datum.to_vec()))?
                        }
                    }
                }
            };
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> FactStoreMut for Artifacts<Backend>
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
                        tokio::try_join!(
                            entity_index.set(
                                EntityKey::from(&fact),
                                State::Added(ValueDatum {
                                    value: fact.is.to_bytes(),
                                }),
                            ),
                            attribute_index.set(
                                AttributeKey::from(&fact),
                                State::Added(ValueDatum {
                                    value: fact.is.to_bytes()
                                })
                            ),
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

        if let Err(_) = transaction_result {
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
        Artifact, Artifacts, Attribute, Entity, FactSelector, FactStore, FactStoreMut, Instruction,
        Value, generate_data,
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
        let mut facts = Artifacts::new(storage_backend).await?;

        let mut data = vec![
            Artifact {
                the: Attribute::from_str("profile/name")?,
                of: Entity::new(),
                is: Value::String("Foo Bar".into()),
            },
            Artifact {
                the: Attribute::from_str("profile/name")?,
                of: Entity::new(),
                is: Value::String("Fizz Buzz".into()),
            },
        ];

        data.sort_by(entity_order);

        facts
            .commit(data.clone().into_iter().map(Instruction::Assert))
            .await?;

        let fact_stream = facts.select(FactSelector::default());

        let mut facts: Vec<Artifact> = fact_stream.map(|fact| fact.unwrap()).collect().await;
        facts.sort_by(entity_order);

        assert_eq!(data, facts);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_uses_indexes_to_optimize_reads() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(256)?.into_iter().map(Instruction::Assert);

        let storage_backend = Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));

        let mut facts = Artifacts::new(storage_backend.clone()).await?;

        facts.commit(data).await?;

        let (initial_reads, initial_writes) = {
            let storage_backend = storage_backend.lock().await;
            (storage_backend.reads(), storage_backend.writes())
        };

        let fact_stream = facts.select(FactSelector::default().is(Value::String("name64".into())));
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
            facts.select(FactSelector::default().the(Attribute::from_str("item/id")?));

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

        let mut facts_one = Artifacts::new(storage_backend.clone()).await?;

        facts_one.commit(data).await?;

        let mut facts_two = Artifacts::new(storage_backend.clone()).await?;

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

        let mut facts = Artifacts::new(storage_backend.clone()).await?;

        facts.commit(data.into_iter().map(into_assert)).await?;
        let revision = facts.revision().await.unwrap();

        let restored_facts = Artifacts::restore(revision.clone(), storage_backend).await?;
        let restored_revision = restored_facts.revision().await.unwrap();

        assert_eq!(revision, restored_revision);

        let fact_stream = facts.select(FactSelector::default().is(Value::String("name10".into())));
        let results: Vec<Artifact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

        let restored_fact_stream =
            restored_facts.select(FactSelector::default().is(Value::String("name10".into())));
        let restored_results: Vec<Artifact> = restored_fact_stream
            .map(|fact| fact.unwrap())
            .collect()
            .await;

        assert_eq!(results, restored_results);

        Ok(())
    }
}
