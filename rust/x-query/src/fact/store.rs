use async_trait::async_trait;
use futures_core::TryStream;
use tokio::sync::mpsc::Receiver;
use x_common::ConditionalSync;

use crate::{Attribute, Entity, Value, XQueryError};

use super::{KeyPart, PrimaryKey};

mod memory;
pub use memory::*;

#[derive(Debug, Clone)]
pub enum State {
    Added(Datum),
    Removed,
}

pub type Datum = (Entity, Attribute, Value);

#[async_trait]
pub trait TripleStore: Clone + ConditionalSync {
    /// Given a key, return that datum associated with the key
    async fn read(&self, key: &PrimaryKey) -> Result<Option<Datum>, XQueryError>;
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
        A: Clone + Send,
        V: Send,
        Attribute: From<A>,
        Value: From<V>,
    {
        let owned_value = Value::from(value);
        let attribute = Attribute::from(attribute);
        let key = PrimaryKey::from((entity.clone(), attribute.clone(), owned_value.clone()));
        self.write(key.clone(), State::Added((entity, attribute, owned_value)))
            .await?;
        Ok(key)
    }

    /// Given a fact state and its key, commit it to the store
    async fn write(&mut self, key: PrimaryKey, state: State) -> Result<(), std::io::Error>;
}

pub trait TripleStorePull: TripleStore {
    /// Returns a stream that yields all entities that have a given attribute
    fn entities_with_attribute(
        &self,
        fragment: KeyPart,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send;

    /// Returns a stream that yields all entities that have a given value
    fn entities_with_value(
        &self,
        fragment: KeyPart,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send;

    /// Returns a stream that yields all attributes associated with a given entity
    fn attributes_of_entity(
        &self,
        fragment: KeyPart,
    ) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send;

    /// Returns a stream that yields all unique keys in the store
    fn keys(&self) -> impl TryStream<Item = Result<PrimaryKey, XQueryError>> + 'static + Send;
}

pub trait TripleStorePush: TripleStore {
    /// Returns a channel that receives all entities that have a given attribute
    fn entities_by_attribute(&self, fragment: KeyPart) -> Receiver<PrimaryKey>;

    /// Returns a channel that receives all entities that have a given value
    fn entities_by_value(&self, fragment: KeyPart) -> Receiver<PrimaryKey>;

    /// Returns a channel that receives all attributes associated with a given entity
    fn attributes_by_entity(&self, fragment: KeyPart) -> Receiver<PrimaryKey>;

    /// Returns a channel that receives all unique keys in the store
    fn keys(&self) -> Receiver<PrimaryKey>;
}
