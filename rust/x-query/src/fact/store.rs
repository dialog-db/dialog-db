use async_trait::async_trait;
use futures_core::TryStream;
use serde::de::DeserializeOwned;
use x_common::ConditionalSend;

use crate::{Attribute, Codec, Entity, Value, XQueryError};

use super::PrimaryKey;

mod memory;
pub use memory::*;

#[derive(Debug, Clone)]
pub enum State {
    Added(Datum),
    Removed,
}

pub type Datum = (Entity, Attribute, Value);

#[async_trait]
pub trait TripleStore {
    /// Returns a stream that yields all entities that have a given attribute
    fn entities_with_attribute<A>(
        &self,
        attribute: A,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>>
    where
        A: Into<Attribute> + ConditionalSend;

    /// Returns a stream that yields all attributes associated with a given entity
    fn attributes_of_entity(
        &self,
        entity: &Entity,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>>;

    /// Returns a stream that yields unique keys in the store
    fn keys(&self) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>>;

    /// Given a key, return that datum associated with the key
    async fn read(&self, key: &PrimaryKey) -> Result<Option<Datum>, XQueryError>;

    /// Given a key, load the value associated with that key and attempt to
    /// deserialize it as the specified type
    async fn load<T, C>(&self, key: &PrimaryKey) -> Result<Option<T>, XQueryError>
    where
        T: DeserializeOwned + ConditionalSend,
        C: Codec,
    {
        match self.read(key).await? {
            Some((_, _, bytes)) => Ok(C::deserialize(bytes).map_err(|error| error.into())?),
            None => Ok(None),
        }
    }
}

#[async_trait]
pub trait TripleStoreMut: TripleStore {
    /// Given a fact, derive its key and commit it to the store
    async fn assert<A, V>(
        &mut self,
        entity: Entity,
        attribute: A,
        value: V,
    ) -> Result<PrimaryKey, std::io::Error>
    where
        A: Clone + ConditionalSend,
        Attribute: From<A>,
        V: AsRef<[u8]> + ConditionalSend,
    {
        let owned_value = value.as_ref().to_vec();
        let attribute = Attribute::from(attribute);
        let key = PrimaryKey::from((entity.clone(), attribute.clone(), value));
        self.write(key.clone(), State::Added((entity, attribute, owned_value)))
            .await?;
        Ok(key)
    }

    /// Given a fact state and its key, commit it to the store
    async fn write(&mut self, key: PrimaryKey, state: State) -> Result<(), std::io::Error>;
}
