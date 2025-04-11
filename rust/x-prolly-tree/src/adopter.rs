use crate::{Block, Entry, KeyType, Node, Reference, ValueType, XProllyTreeError};
use async_trait::async_trait;
use nonempty::NonEmpty;
use x_storage::{ContentAddressedStorage, HashType};

/// A helper trait implemented by [`Entry`], [`Reference`] and [`Node`] to
/// create new [`Node`]s.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Adopter<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>:
    Sized
where
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    /// Adopt a collection of `children` into a new [`Node`]. Children data must
    /// be ordered and follow rank rules.
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<
            HASH_SIZE,
            Block = Block<HASH_SIZE, Key, Value, Hash>,
            Hash = Hash,
        >,
    ) -> Result<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>, XProllyTreeError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
    Adopter<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash> for Entry<Key, Value>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<
            HASH_SIZE,
            Block = Block<HASH_SIZE, Key, Value, Hash>,
            Hash = Hash,
        >,
    ) -> Result<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>, XProllyTreeError> {
        Node::segment(children, storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
    Adopter<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash> for Reference<HASH_SIZE, Key, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<
            HASH_SIZE,
            Block = Block<HASH_SIZE, Key, Value, Hash>,
            Hash = Hash,
        >,
    ) -> Result<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>, XProllyTreeError> {
        Node::branch(children, storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Key, Value, Hash>
    Adopter<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>
    for Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>
where
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
{
    async fn adopt(
        children: NonEmpty<Self>,
        storage: &mut impl ContentAddressedStorage<
            HASH_SIZE,
            Block = Block<HASH_SIZE, Key, Value, Hash>,
            Hash = Hash,
        >,
    ) -> Result<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>, XProllyTreeError> {
        Node::branch(children.map(|node| node.reference().clone()), storage).await
    }
}
