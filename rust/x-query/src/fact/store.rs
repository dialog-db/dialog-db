use async_trait::async_trait;
use futures_core::TryStream;
use serde::de::DeserializeOwned;
use x_common::ConditionalSend;

use crate::{Attribute, Codec, Entity, Value, XQueryError};

use super::PrimaryKey;

pub type Datum = (Entity, Attribute, Value);

#[async_trait]
pub trait TripleStore {
    /// Returns a stream that yields unique entities in the store
    async fn entities(&self) -> impl TryStream<Item = Result<Entity, XQueryError>>;

    /// Returns a stream that yields unique attributes in the store
    async fn attributes(&self) -> impl TryStream<Item = Result<Attribute, XQueryError>>;

    /// Returns a stream that yields unique keys in the store
    async fn keys(&self) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>>;

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
        attribute: Attribute,
        value: Value,
    ) -> Result<(), std::io::Error>
    where
        A: Clone,
        Attribute: From<A>,
        V: AsRef<[u8]>,
    {
        self.write(
            PrimaryKey::from((entity, attribute.clone(), value.clone())),
            (entity, attribute.into(), value),
        )
        .await
    }

    /// Given a fact and its key, commit it to the store
    async fn write(&mut self, key: PrimaryKey, datum: Datum) -> Result<(), std::io::Error>;
}
