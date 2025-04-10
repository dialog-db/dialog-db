use async_stream::try_stream;
use async_trait::async_trait;
use futures_core::TryStream;
use std::{collections::BTreeMap, sync::Arc};
use tokio::{join, sync::RwLock};

use crate::{AevKey, EavKey, IndexKey, KeyPart, PrimaryKey, VaeKey, XQueryError};

use super::{Datum, State, TripleStore, TripleStoreMut, TripleStorePull};

/// A [MemoryStore] implements [TripleStore] and [TripleStoreMut] over indexes
/// built with the standard Rust [BTreeMap]. All facts are held in-memory, and no
/// effort is made to persist them.
#[derive(Default, Clone)]
pub struct MemoryStore {
    eav: Arc<RwLock<BTreeMap<EavKey, State>>>,
    aev: Arc<RwLock<BTreeMap<AevKey, State>>>,
    vae: Arc<RwLock<BTreeMap<VaeKey, State>>>,
}

#[async_trait]
impl TripleStore for MemoryStore {
    /// Given a key, return that datum associated with the key
    async fn read(&self, key: &PrimaryKey) -> Result<Option<Datum>, XQueryError> {
        Ok(self
            .eav
            .read()
            .await
            .get(key)
            .and_then(|state| match state {
                State::Added(datum) => Some(datum.clone()),
                State::Removed => None,
            }))
    }
}

#[async_trait]
impl TripleStoreMut for MemoryStore {
    async fn write(&mut self, key: PrimaryKey, state: State) -> Result<(), std::io::Error> {
        let aev_key = AevKey::from(key.clone());
        let vae_key = VaeKey::from(key.clone());

        let (mut eav, mut aev, mut vae) =
            join!(self.eav.write(), self.aev.write(), self.vae.write());

        eav.insert(key, state.clone());
        aev.insert(aev_key, state.clone());
        vae.insert(vae_key, state);

        Ok(())
    }
}

impl TripleStorePull for MemoryStore {
    /// Returns a stream that yields all entities that have a given attribute
    fn entities_with_attribute(
        &self,
        fragment: KeyPart,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send {
        let aev = self.aev.clone();

        try_stream! {
            let attribute = fragment.as_attribute_part()?;

            let min = <AevKey as IndexKey>::min().attribute_part(attribute.clone());
            let max = <AevKey as IndexKey>::max().attribute_part(attribute.clone());
            let aev = aev.read().await;

            for (key, _) in aev.range(min..max) {
                yield key.clone().into()
            }
        }
    }

    /// Returns a stream that yields all entities that have a given attribute
    fn entities_with_value(
        &self,
        fragment: KeyPart,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send {
        let vae = self.vae.clone();

        try_stream! {
            let value = fragment.as_value_part()?;

            let min = <VaeKey as IndexKey>::min().value_part(value.clone());
            let max = <VaeKey as IndexKey>::max().value_part(value.clone());
            let vae = vae.read().await;

            for (key, _) in vae.range(min..max) {
                yield key.clone().into()
            }
        }
    }

    /// Returns a stream that yields all attributes associated with a given entity
    fn attributes_of_entity(
        &self,
        fragment: KeyPart,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send {
        let eav = self.eav.clone();

        try_stream! {
            let entity = fragment.as_entity_part()?;

            let min = <EavKey as IndexKey>::min().entity_part(entity.clone());
            let max = <EavKey as IndexKey>::max().entity_part(entity.clone());
            let eav = eav.read().await;

            for (key, _) in eav.range(min..max) {
                yield key.clone().into()
            }
        }
    }

    /// Returns a stream that yields a [PrimaryKey] for every unique [Datum] in the store
    fn keys(&self) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send {
        let eav = self.eav.clone();

        try_stream! {
            let eav = eav.read().await;
            for key in eav.keys() {
                yield key.clone()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{Attribute, Entity, KeyPart, TripleStorePull};

    use super::{MemoryStore, TripleStoreMut};
    use anyhow::Result;
    use futures_util::{TryStreamExt, pin_mut};

    #[cfg(target_arch = "wasm32")]
    use wasm_bindgen_test::wasm_bindgen_test;
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[cfg_attr(target_arch = "wasm32", wasm_bindgen_test)]
    #[cfg_attr(not(target_arch = "wasm32"), tokio::test)]
    async fn it_streams_all_entities_with_an_attribute() -> Result<()> {
        let mut memory_store = MemoryStore::default();

        let name_attribute = Attribute::from_str("test/name")?;
        let color_attribute = Attribute::from_str("test/color")?;

        // Populate the store with 200 entities
        for _ in 0..100 {
            memory_store
                .assert(Entity::new(), name_attribute.clone(), vec![0])
                .await?;
            memory_store
                .assert(Entity::new(), color_attribute.clone(), vec![1])
                .await?;
        }

        // Stream only entities with a "test/name" attribute (100 entities expected)
        let entity_stream = memory_store.entities_with_attribute(KeyPart::from(name_attribute));

        pin_mut!(entity_stream);

        let mut count = 0;

        while let Ok(Some(_)) = entity_stream.try_next().await {
            count += 1;
        }

        assert_eq!(count, 100);

        Ok(())
    }
}
