use std::{
    collections::BTreeMap,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use super::differential::{Change, Differential};
use async_stream::{stream, try_stream};
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

    /// Returns a difference between this and the other tree. Applying returned
    /// differential onto `other` tree should produce this `tree`.
    pub fn differentiate(&self, other: Self) -> impl Differential<Key, Value> + '_ {
        stream! {
            match (self.root(), other.root()) {
                (None, None) => {
                    // Both trees are empty - no changes
                }
                // if we have a root but other does not
                // then difference simply adds everything
                (Some(_), None) => {
                    for await entry in self.stream() {
                        yield Ok(Change::Add(entry?));
                    }
                }
                (None, Some(_)) => {
                    for await entry in other.stream() {
                        yield Ok(Change::Remove(entry?));
                    }
                }
                (Some(after), Some(before)) => {
                    // Use the differential module to compute changes
                    let diff = crate::differential::differentiate(
                        before.clone(),
                        after.clone(),
                        &self.storage
                    );

                    for await change in diff {
                        yield change;
                    }
                }
            }
        }
    }

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
        Changes: IntoIterator<Item = Change<Key, Value>>,
        Value: AsRef<[u8]>,
    {
        // Copy root here in case we fail integration and need to revert
        let root = self.root.clone();

        let result: Result<(), DialogProllyTreeError> = {
            for change in changes {
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
                                    let existing_hash = existing_value.hash();
                                    let new_hash = entry.value.hash();

                                    if new_hash > existing_hash {
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
