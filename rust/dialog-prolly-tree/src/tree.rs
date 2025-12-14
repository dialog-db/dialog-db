use std::{
    collections::BTreeMap,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use super::differential::Change;
use crate::{Adopter, DialogProllyTreeError, Entry, KeyType, Node, TreeDifference, ValueType};
use async_stream::try_stream;
use dialog_storage::{ContentAddressedStorage, Encoder, HashType};
use futures_core::Stream;
use nonempty::NonEmpty;

/// A hash representing an empty (usually newly created) `Tree`.
pub static EMPT_TREE_HASH: [u8; 32] = [0; 32];

/// A key-value store backed by a Ranked Prolly Tree with configurable storage,
/// encoding and rank distribution.
#[derive(Debug, Clone)]
pub struct Tree<Distribution, Key, Value, Hash, Storage>
where
    Distribution: crate::Distribution<Key, Hash>,
    Key: KeyType + 'static,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
{
    storage: Storage,
    root: Option<Node<Key, Value, Hash>>,

    distribution_type: PhantomData<Distribution>,
    key_type: PhantomData<Key>,
    value_type: PhantomData<Value>,
    hash_type: PhantomData<Hash>,
}

impl<Distribution, Key, Value, Hash, Storage> Tree<Distribution, Key, Value, Hash, Storage>
where
    Distribution: crate::Distribution<Key, Hash>,
    Key: KeyType,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash>,
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
        let root = if hash.as_ref() == EMPT_TREE_HASH {
            None
        } else {
            Some(Node::from_hash(hash.clone(), &storage).await?)
        };

        Ok(Self {
            storage,
            root,
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
    pub fn root(&self) -> Option<&Node<Key, Value, Hash>> {
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

    /// Returns a differential that produces changes to transform `self` into `other`.
    ///
    /// Usage: `self.integrate(self.differentiate(other))` will result in `other`.
    pub fn differentiate<'a>(
        &'a self,
        other: &'a Self,
    ) -> impl crate::differential::Differential<Key, Value> + 'a {
        try_stream! {
            let delta = TreeDifference::compute(self, other).await?;
            for await change in delta.changes() {
                yield change?;
            }
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

// Impl block for methods that require Encoder
impl<Distribution, Key, Value, Hash, Storage> Tree<Distribution, Key, Value, Hash, Storage>
where
    Distribution: crate::Distribution<Key, Hash>,
    Key: KeyType,
    Value: ValueType,
    Hash: HashType,
    Storage: ContentAddressedStorage<Hash = Hash> + Encoder,
{
    /// Integrates changes into this tree with deterministic conflict resolution.
    ///
    /// Applies a differential (stream of changes) with Last-Write-Wins conflict resolution
    /// based on value hashes. This ensures eventual consistency across replicas.
    ///
    /// # Conflict Resolution
    ///
    /// - **Add**: If key exists with different value, compare hashes - higher hash wins
    /// - **Remove**: Only removes if the exact entry (key+value) exists
    ///
    /// The operation is atomic - if any change fails, the entire integration is rolled back.
    pub async fn integrate<Changes>(
        &mut self,
        changes: Changes,
    ) -> Result<(), DialogProllyTreeError>
    where
        Changes: crate::differential::Differential<Key, Value>,
    {
        use futures_util::StreamExt;

        // Copy root here in case we fail integration and need to revert
        let root = self.root.clone();

        let result: Result<(), DialogProllyTreeError> = {
            futures_util::pin_mut!(changes);
            while let Some(change_result) = changes.next().await {
                let change = change_result?;
                match change {
                    Change::Add(entry) => {
                        // Check if key already exists
                        match self.get(&entry.key).await? {
                            None => {
                                // Key doesn't exist - insert it
                                self.set(entry.key, entry.value).await?;
                            }
                            Some(existing_value) => {
                                if existing_value == entry.value {
                                    // Same value - no-op (idempotent)
                                } else {
                                    // Different values - resolve conflict by comparing hashes

                                    let (existing_hash, _) = self
                                        .storage()
                                        .encode(&existing_value)
                                        .await
                                        .map_err(|e| e.into())?;
                                    let (new_hash, _) = self
                                        .storage()
                                        .encode(&entry.value)
                                        .await
                                        .map_err(|e| e.into())?;

                                    if new_hash.as_ref() > existing_hash.as_ref() {
                                        // New value wins - update
                                        self.set(entry.key, entry.value).await?;
                                    }
                                    // Else: existing wins, no-op
                                }
                            }
                        }
                    }
                    Change::Remove(entry) => {
                        // Check if key exists
                        match self.get(&entry.key).await? {
                            None => {
                                // Key doesn't exist - no-op (already removed)
                            }
                            Some(existing_value) => {
                                if existing_value == entry.value {
                                    // Same value - remove it
                                    self.delete(&entry.key).await?;
                                }
                                // Else: different value - no-op (concurrent update)
                            }
                        }
                    }
                }
            }
            Ok(())
        };

        // If integration fails we set the root back to the original
        // as this operation must be atomic.
        if result.is_err() {
            self.root = root;
        }

        result
    }
}
