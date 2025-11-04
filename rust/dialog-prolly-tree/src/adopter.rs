use crate::{DialogProllyTreeError, Entry, KeyType, Node, Reference, ValueType};
use async_trait::async_trait;
use dialog_storage::{ContentAddressedStorage, HashType};
use nonempty::NonEmpty;

/// A helper trait implemented by [`Entry`], [`Reference`] and [`Node`] to
/// create new [`Node`]s.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Adopter<Key, Value, Hash>: Sized
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType,
{
    /// Adopt a collection of `children` into a new [`Node`]. Children data must
    /// be ordered and follow rank rules.
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<Hash = Hash>,
    ) -> Result<Node<Key, Value, Hash>, DialogProllyTreeError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, Hash> Adopter<Key, Value, Hash> for Entry<Key, Value>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<Hash = Hash>,
    ) -> Result<Node<Key, Value, Hash>, DialogProllyTreeError> {
        Node::segment(children, storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, Hash> Adopter<Key, Value, Hash> for Reference<Key, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<Hash = Hash>,
    ) -> Result<Node<Key, Value, Hash>, DialogProllyTreeError> {
        Node::branch(children, storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value, Hash> Adopter<Key, Value, Hash> for Node<Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
{
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<Hash = Hash>,
    ) -> Result<Node<Key, Value, Hash>, DialogProllyTreeError> {
        Node::branch(children.map(|node| node.reference().clone()), storage).await
    }
}
