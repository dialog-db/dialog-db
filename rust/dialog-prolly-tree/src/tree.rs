use std::{
    collections::BTreeMap,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, HashType};
use futures_core::Stream;
use nonempty::NonEmpty;

use crate::{Adopter, DialogProllyTreeError, Entry, KeyType, Node, ValueType};

/// A key-value store backed by a Ranked Prolly Tree with configurable storage,
/// encoding and rank distribution.
#[derive(Clone)]
pub struct Tree<
    const BRANCH_FACTOR: u32,
    const HASH_SIZE: usize,
    Distribution,
    Key,
    Value,
    Hash,
    Storage,
> where
    Distribution: crate::Distribution<BRANCH_FACTOR, HASH_SIZE, Key, Hash>,
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    storage: Storage,
    root: Option<Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>>,

    distribution_type: PhantomData<Distribution>,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
    hash_type: PhantomData<Hash>,
}

impl<const BRANCH_FACTOR: u32, const HASH_SIZE: usize, Distribution, Key, Value, Hash, Storage>
    Tree<BRANCH_FACTOR, HASH_SIZE, Distribution, Key, Value, Hash, Storage>
where
    Distribution: crate::Distribution<BRANCH_FACTOR, HASH_SIZE, Key, Hash>,
    Key: KeyType,
    Value: ValueType,
    Hash: HashType<HASH_SIZE>,
    Storage: ContentAddressedStorage<HASH_SIZE, Hash = Hash>,
{
    /// Creates a new [`Tree`] with provided [`ContentAddressedStorage`].
    pub fn new(storage: Storage) -> Self {
        Self {
            storage,
            root: None,

            distribution_type: PhantomData,
            key_type: PhantomData,
            value_type: PhantomData,
            hash_type: PhantomData,
        }
    }

    /// Hydrate a new [`Tree`] from a [`HashType`] that references a [`Node`].
    pub async fn from_hash(hash: &Hash, storage: Storage) -> Result<Self, DialogProllyTreeError> {
        let root = Node::from_hash(hash.clone(), &storage).await?;
        Ok(Self {
            storage,
            root: Some(root),

            distribution_type: PhantomData,
            key_type: PhantomData,
            value_type: PhantomData,
            hash_type: PhantomData,
        })
    }

    /// The [`ContentAddressedStorage`] used by this tree.
    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Returns the [`Node`] representing the root of this tree.
    ///
    /// Returns `None` if the tree is empty.
    pub fn root(&self) -> Option<&Node<BRANCH_FACTOR, HASH_SIZE, Key, Value, Hash>> {
        self.root.as_ref()
    }

    /// Changes the root (revision) of the tree to the node identified by the
    /// given [`HashType`]
    pub async fn set_hash(&mut self, hash: Option<Hash>) -> Result<(), DialogProllyTreeError> {
        self.root = if let Some(hash) = hash {
            Some(Node::from_hash(hash, &self.storage).await?)
        } else {
            None
        };
        Ok(())
    }

    /// Returns the [`HashType`] representing the root of this tree.
    ///
    /// Returns `None` if the tree is empty.
    pub fn hash(&self) -> Option<&Hash> {
        self.root().map(|root| root.hash())
    }

    /// Retrieves the value associated with `key` from the tree.
    pub async fn get(&self, key: &Key) -> Result<Option<Value>, DialogProllyTreeError> {
        match &self.root {
            Some(root) => match root.get_entry(key, &self.storage).await? {
                Some(entry) => Ok(Some(entry.value)),
                None => Ok(None),
            },
            None => Ok(None),
        }
    }

    /// Sets a `key`/`value` pair into the tree.
    pub async fn set(&mut self, key: Key, value: Value) -> Result<(), DialogProllyTreeError> {
        let entry = Entry { key, value };
        match &self.root {
            Some(root) => {
                let new_root = root
                    .insert::<Distribution, _>(entry, &mut self.storage)
                    .await?;
                self.root = Some(new_root);
            }
            None => {
                let segment = Entry::adopt(NonEmpty::singleton(entry), &mut self.storage).await?;
                self.root = Some(segment);
            }
        }
        Ok(())
    }

    /// Remove the `key`/`value` pair associated with `key` (if it is present)
    pub async fn delete(&mut self, key: &Key) -> Result<(), DialogProllyTreeError> {
        match &self.root {
            Some(root) => {
                self.root = root
                    .remove::<Distribution, _>(key, &mut self.storage)
                    .await?;
                Ok(())
            }
            None => Ok(()),
        }
    }

    /// Returns an async stream over all entries.
    pub fn stream(
        &self,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogProllyTreeError>> + '_ {
        self.stream_range(..)
    }

    /// Returns an async stream over entries with keys within the provided range.
    pub fn stream_range<'a, R>(
        &'a self,
        range: R,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogProllyTreeError>> + 'a
    where
        R: RangeBounds<Key> + 'a,
    {
        try_stream! {
            match (range.start_bound(), range.end_bound()) {
                // Handle the case where the start and end of the bounds are the
                // same key by looking up the key directly
                (
                    Bound::Included(start_key) | Bound::Excluded(start_key),
                    Bound::Included(end_key) | Bound::Excluded(end_key),
                ) if start_key == end_key => {
                    if let Some(value) = self.get(start_key).await? {
                        yield Entry {
                            key: start_key.clone(),
                            value,
                        };
                    }
                }
                _ => {
                    if let Some(root) = self.root.as_ref() {
                        let stream = root.get_range(range, &self.storage);
                        for await item in stream {
                            yield item?;
                        }
                    }
                }
            }
        }
    }

    /// Create a new [`Tree`] from a [`BTreeMap`].
    ///
    /// A more efficient method than iteratively adding values.
    pub async fn from_collection(
        collection: BTreeMap<Key, Value>,
        mut storage: Storage,
    ) -> Result<Self, DialogProllyTreeError> {
        let mut nodes = {
            let entries = collection
                .into_iter()
                .map(|(key, value)| {
                    let node = Entry { key, value };
                    let rank = Distribution::rank(&node.key);
                    (node, rank)
                })
                .collect();
            let entries = NonEmpty::from_vec(entries).ok_or_else(|| {
                DialogProllyTreeError::InvalidConstruction(
                    "Tree must have at least one child".into(),
                )
            })?;
            Node::join_with_rank(entries, 1, &mut storage).await?
        };
        let mut minimum_rank = 2;
        loop {
            nodes = Node::join_with_rank(nodes, minimum_rank, &mut storage).await?;
            if nodes.len() == 1 {
                break;
            }
            minimum_rank += 1;
        }
        Ok(Tree {
            storage,
            root: Some(nodes.head.0),

            distribution_type: PhantomData,
            key_type: PhantomData,
            value_type: PhantomData,
            hash_type: PhantomData,
        })
    }
}
