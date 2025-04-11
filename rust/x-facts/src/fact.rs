mod version;
pub use version::*;

mod instruction;
pub use instruction::*;

mod selector;
pub use selector::*;

mod store;
pub use store::*;

use async_stream::try_stream;
use async_trait::async_trait;
use futures_util::Stream;
use std::{ops::Range, sync::Arc};
use tokio::sync::RwLock;
use x_common::{ConditionalSend, ConditionalSync};
use x_prolly_tree::{BasicEncoder, Entry, GeometricDistribution, Tree, ValueType};
use x_storage::{Storage, StorageBackend, XStorageError};

use crate::{
    AttributeKey, BRANCH_FACTOR, EntityDatum, EntityKey, EntityKeyPart, HASH_SIZE, State,
    ValueDatum, ValueKey, ValueReferenceKeyPart, XFactsError,
};

mod attribute;
pub use attribute::*;

mod entity;
pub use entity::*;

mod value;
pub use value::*;

/// The representation of the hash type (BLAKE3, in this case) that must be used
/// by a [`StorageBackend`] that may back an instance of [`Facts`].
pub type Blake3Hash = [u8; HASH_SIZE];

/// A [`Fact`] embodies a datum - a semantic triple - that may be stored in or
/// retrieved from a [`FactStore`].
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct Fact {
    /// The [`Attribute`] of the [`Fact`]; the predicate of the triple
    pub the: Attribute,
    /// The [`Entity`] of the [`Fact`]; the subject of the triple
    pub of: Entity,
    /// The [`Value`] of the [`Fact`]; the object of the triple
    pub is: Value,
}

/// An alias type that describes the [`Tree`]-based prolly tree that is
/// used for each index in [`Facts`]
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

/// [`Facts`] is an implementor of [`FactStore`] and [`FactStoreMut`].
/// Internally, [`Facts`] maintains indexes built from [`Tree`]s (that is,
/// prolly trees). These indexes are built up as new [`Fact`]s are commited,
/// and they are chosen based on [`FactSelector`] shapes when [`Fact`]s are
/// queried.
///
/// [`Facts`] are backed by a concrete implementation of [`StorageBackend`].
/// The user-provided [`StorageBackend`] is paired with a [`BasicEncoder`] to
/// produce a [`ContentAddressedStorage`] that is suitable for storing and
/// retrieving facts.
///
/// See the crate-level documentation for an example of usage.
#[derive(Clone)]
pub struct Facts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = XStorageError>
        + ConditionalSync
        + 'static,
{
    entity_index: Index<EntityKey, ValueDatum, Backend>,
    attribute_index: Index<AttributeKey, ValueDatum, Backend>,
    value_index: Index<ValueKey, EntityDatum, Backend>,
}

impl<Backend> Facts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = XStorageError>
        + ConditionalSync
        + 'static,
{
    /// Initialize a new [`Facts`] with the provided [`StorageBackend`].
    pub async fn new(backend: Backend) -> Result<Self, XFactsError> {
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

    /// Attempt to initialize the [`Facts`] at a specific [`Version`].
    pub async fn at(version: Version, backend: Backend) -> Result<Self, XFactsError> {
        Ok(Self {
            entity_index: Arc::new(RwLock::new(
                Tree::from_hash(
                    version.entity(),
                    Storage {
                        encoder: BasicEncoder::<EntityKey, State<ValueDatum>>::default(),
                        backend: backend.clone(),
                    },
                )
                .await?,
            )),
            attribute_index: Arc::new(RwLock::new(
                Tree::from_hash(
                    version.attribute(),
                    Storage {
                        encoder: BasicEncoder::<AttributeKey, State<ValueDatum>>::default(),
                        backend: backend.clone(),
                    },
                )
                .await?,
            )),
            value_index: Arc::new(RwLock::new(
                Tree::from_hash(
                    version.value(),
                    Storage {
                        encoder: BasicEncoder::<ValueKey, State<EntityDatum>>::default(),
                        backend: backend.clone(),
                    },
                )
                .await?,
            )),
        })
    }

    /// Get the hash that represents the [`FactStore`] at its current version.
    pub async fn version(&self) -> Option<Version> {
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
                Some(Version::from((
                    entity_version.to_owned(),
                    attribute_version.to_owned(),
                    value_version.to_owned(),
                )))
            }
            _ => None,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> FactStore for Facts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = XStorageError>
        + ConditionalSync
        + 'static,
{
    fn select(
        &self,
        selector: FactSelector,
    ) -> impl Stream<Item = Result<Fact, XFactsError>> + '_ + ConditionalSend {
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
                        yield Fact {
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
                        yield Fact {
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
                            return Err(XFactsError::MalformedIndex(format!("Missing datum for key {:?}", key)))?;
                        };

                        yield Fact {
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
                        yield Fact {
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
impl<Backend> FactStoreMut for Facts<Backend>
where
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = XStorageError>
        + ConditionalSync
        + 'static,
{
    async fn commit<I>(&mut self, instructions: I) -> Result<(), XFactsError>
    where
        I: IntoIterator<Item = Instruction> + ConditionalSend,
        I::IntoIter: ConditionalSend,
    {
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

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{str::FromStr, sync::Arc};

    use anyhow::Result;
    use futures_util::StreamExt;
    use tokio::sync::Mutex;
    use x_storage::{MeasuredStorageBackend, make_target_storage};

    use crate::{
        Attribute, Entity, Fact, FactSelector, FactStore, FactStoreMut, Facts, Instruction, Value,
        generate_data,
    };

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_commits_and_selects_facts() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let entity_order = |l: &Fact, r: &Fact| l.of.cmp(&r.of);
        let mut facts = Facts::new(storage_backend).await?;

        let mut data = vec![
            Fact {
                the: Attribute::from_str("profile/name")?,
                of: Entity::new(),
                is: Value::String("Foo Bar".into()),
            },
            Fact {
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

        let mut facts: Vec<Fact> = fact_stream.map(|fact| fact.unwrap()).collect().await;
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

        let mut facts = Facts::new(storage_backend.clone()).await?;

        facts.commit(data).await?;

        let (initial_reads, initial_writes) = {
            let storage_backend = storage_backend.lock().await;
            (storage_backend.reads(), storage_backend.writes())
        };

        let fact_stream = facts.select(FactSelector::default().is(Value::String("name64".into())));
        let results: Vec<Fact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

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

        let results: Vec<Fact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

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

        let into_assert = |fact: Fact| Instruction::Assert(fact);

        let data = data.into_iter().map(into_assert);
        let reordered_data = reordered_data.into_iter().map(into_assert);

        let storage_backend = Arc::new(Mutex::new(MeasuredStorageBackend::new(storage_backend)));

        let mut facts_one = Facts::new(storage_backend.clone()).await?;

        facts_one.commit(data).await?;

        let mut facts_two = Facts::new(storage_backend.clone()).await?;

        facts_two.commit(reordered_data).await?;

        assert_eq!(facts_one.version().await, facts_two.version().await);

        Ok(())
    }

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_can_restore_a_previously_commited_version() -> Result<()> {
        let (storage_backend, _temp_directory) = make_target_storage().await?;
        let data = generate_data(64)?;
        let into_assert = |fact: Fact| Instruction::Assert(fact);
        let storage_backend = Arc::new(Mutex::new(storage_backend));

        let mut facts = Facts::new(storage_backend.clone()).await?;

        facts.commit(data.into_iter().map(into_assert)).await?;
        let version = facts.version().await.unwrap();

        let restored_facts = Facts::at(version.clone(), storage_backend).await?;
        let restored_version = restored_facts.version().await.unwrap();

        assert_eq!(version, restored_version);

        let fact_stream = facts.select(FactSelector::default().is(Value::String("name10".into())));
        let results: Vec<Fact> = fact_stream.map(|fact| fact.unwrap()).collect().await;

        let restored_fact_stream =
            restored_facts.select(FactSelector::default().is(Value::String("name10".into())));
        let restored_results: Vec<Fact> = restored_fact_stream
            .map(|fact| fact.unwrap())
            .collect()
            .await;

        assert_eq!(results, restored_results);

        Ok(())
    }
}
