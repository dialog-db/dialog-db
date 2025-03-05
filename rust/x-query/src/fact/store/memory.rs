use async_stream::try_stream;
use async_trait::async_trait;
use futures_core::TryStream;
use std::{collections::BTreeMap, sync::Arc};
use tokio::{join, sync::RwLock};
use x_common::ConditionalSend;

use crate::{AevKey, EavKey, Fragment, IndexKey, PrimaryKey, VaeKey, XQueryError};

use super::{Datum, State, TripleStore, TripleStoreMut};

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
    /// Returns a stream that yields all entities that have a given attribute
    fn entities_with_attribute(
        &self,
        fragment: Fragment,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + ConditionalSend {
        let aev = self.aev.clone();

        try_stream! {
            let attribute = fragment.as_attribute()?;

            let min = <AevKey as IndexKey>::min().attribute_reference(attribute.clone());
            let max = <AevKey as IndexKey>::max().attribute_reference(attribute.clone());
            let aev = aev.read().await;

            for (key, _) in aev.range(min..max) {
                yield key.clone().into()
            }
        }
    }

    /// Returns a stream that yields all entities that have a given attribute
    fn entities_with_value(
        &self,
        fragment: Fragment,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + ConditionalSend {
        let vae = self.vae.clone();

        try_stream! {
            let value = fragment.as_value()?;

            let min = <VaeKey as IndexKey>::min().value_reference(value.clone());
            let max = <VaeKey as IndexKey>::max().value_reference(value.clone());
            let vae = vae.read().await;

            for (key, _) in vae.range(min..max) {
                yield key.clone().into()
            }
        }
    }

    /// Returns a stream that yields all attributes associated with a given entity
    fn attributes_of_entity(
        &self,
        fragment: Fragment,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + ConditionalSend {
        let eav = self.eav.clone();

        try_stream! {
            let entity = fragment.as_entity()?;

            let min = <EavKey as IndexKey>::min().entity_reference(entity.clone());
            let max = <EavKey as IndexKey>::max().entity_reference(entity.clone());
            let eav = eav.read().await;

            for (key, _) in eav.range(min..max) {
                yield key.clone().into()
            }
        }
    }

    /// Returns a stream that yields a [PrimaryKey] for every unique [Datum] in the store
    fn keys(
        &self,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + ConditionalSend {
        let eav = self.eav.clone();

        try_stream! {
            let eav = eav.read().await;
            for key in eav.keys() {
                yield key.clone()
            }
        }
    }

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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::{Attribute, Entity, Fragment};

    use super::{MemoryStore, TripleStore, TripleStoreMut};
    use anyhow::Result;
    // use futures_core::{Stream, TryStream};
    use futures_util::{TryStreamExt, pin_mut};

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
        let entity_stream = memory_store.entities_with_attribute(Fragment::from(name_attribute));

        pin_mut!(entity_stream);

        let mut count = 0;

        while let Ok(Some(_)) = entity_stream.try_next().await {
            count += 1;
        }

        assert_eq!(count, 100);

        Ok(())
    }
}
