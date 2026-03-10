use std::{marker::PhantomData, ops::RangeBounds};

use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Accessor, Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError, Entry, Key,
    SearchOptions, SearchResult, SymmetryWith, TreeShaper, TreeWalker, Value, into_owned,
};

/// A key-value store backed by a ranked prolly tree with content-addressed
/// storage.
///
/// The [`Tree`] represents an immutable, persistent data structure where each
/// modification creates a new version while sharing unchanged nodes through
/// structural sharing. The tree stores key-value pairs in sorted order and
/// provides efficient lookups, insertions, and range queries.
///
/// Nodes are stored in content-addressed storage using [`Blake3Hash`] as
/// identifiers, enabling deduplication and efficient versioning. The tree
/// maintains a [`Delta`] of pending changes that can be flushed to storage in
/// batches, and uses a [`Cache`] for frequently accessed nodes to minimize
/// storage I/O operations.
///
/// ## Clone Semantics
///
/// Cloning a [`Tree`] does **not** fork the internal state. Instead, it shares
/// the cache and delta via reference counting. Mutations
/// ([`insert`](Self::insert), [`delete`](Self::delete)) implicitly fork the
/// tree by returning a new instance with a branched [`Delta`] (independent
/// copy) and an updated root hash, while continuing to share the [`Cache`].
/// This enables efficient versioning where multiple tree versions share the
/// same cache for read operations, but maintain independent mutation state.
#[derive(Debug, Clone)]
pub struct Tree<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
{
    key: PhantomData<Key>,
    value: PhantomData<Value>,

    root: Blake3Hash,
    node_cache: Cache<Blake3Hash, Buffer>,

    delta: Delta<Blake3Hash, Buffer>,
}

impl<Key, Value> Tree<Key, Value>
where
    Key: self::Key
        + ConditionalSync
        + 'static
        + PartialOrd<Key::Archived>
        + PartialEq<Key::Archived>
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + ConditionalSync
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value
        + ConditionalSync
        + 'static
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
{
    /// Returns the [`Blake3Hash`] of the root node of this tree.
    ///
    /// The root hash uniquely identifies this version of the tree and can be
    /// used to reconstruct the tree from storage or to compare tree versions.
    pub fn root(&self) -> &Blake3Hash {
        &self.root
    }

    /// Creates a new empty [`Tree`] with no entries.
    ///
    /// The empty tree has a null root hash and contains no cached nodes or
    /// pending changes in its delta.
    pub fn empty() -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            root: NULL_BLAKE3_HASH.clone(),
            node_cache: Cache::new(),
            delta: Delta::zero(),
        }
    }

    /// Creates a [`Tree`] from a known root hash.
    ///
    /// This constructor is used to restore a tree to a previously persisted
    /// version. The tree will lazily load nodes from storage as they are
    /// accessed during operations.
    pub fn from_hash(root: Blake3Hash) -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            root,
            node_cache: Cache::new(),
            delta: Delta::zero(),
        }
    }

    /// Retrieves the value associated with `key` from the tree.
    ///
    /// This method performs a binary search through the tree hierarchy to
    /// locate the leaf segment containing the key, then searches within that
    /// segment for the specific entry.
    ///
    /// Returns `Ok(Some(value))` if the key exists, `Ok(None)` if the key is
    /// not found, or an error if the tree structure is invalid or storage
    /// access fails.
    pub async fn get<Backend>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Option<Value>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        if let Some(result) = self.search(key, storage, SearchOptions::default()).await? {
            if let Some(entry) = result.leaf.body()?.find_entry(key)? {
                into_owned(&entry.value).map(|value| Some(value))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Inserts a `key`/`value` pair into the tree, returning a new tree
    /// version.
    ///
    /// If the key already exists, its value is updated. If the key is new, it
    /// is inserted in sorted order within the appropriate leaf segment.
    pub async fn insert<Backend>(
        &self,
        key: Key,
        value: Value,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        let search_result = self.search(&key, storage, SearchOptions::default()).await?;
        let shaper = TreeShaper::new(self.root.clone(), self.delta.clone());
        let (next_root, delta) = shaper.insert(Entry { key, value }, search_result)?;

        Ok(self.advance(next_root, delta))
    }

    /// Removes the `key`/`value` pair associated with `key` from the tree.
    ///
    /// If the key does not exist, the operation completes successfully without
    /// modification.
    pub async fn delete<Backend>(
        &mut self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        let options = SearchOptions {
            prefetch_right_neighbor: true,
        };
        if let Some(search_result) = self.search(key, storage, options).await? {
            let shaper = TreeShaper::new(self.root.clone(), self.delta.clone());
            let (next_root, delta) = shaper.delete(key, search_result)?;
            Ok(self.advance(next_root, delta))
        } else {
            // Key not found in tree - nothing to delete
            Ok(self.clone())
        }
    }

    /// Returns an async stream over all entries in the tree.
    ///
    /// Entries are yielded in sorted order by key. This method traverses the
    /// tree from the leftmost leaf to the rightmost, streaming entries as they
    /// are encountered without loading the entire tree into memory.
    ///
    /// Internally, this calls [`stream_range`](Self::stream_range) with an
    /// unbounded range covering all possible keys.
    pub fn stream<'a, Backend>(
        &'a self,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        self.stream_range(
            <Key as self::Key>::min()..<Key as self::Key>::max(),
            storage,
        )
    }

    /// Returns an async stream over entries with keys within the provided
    /// range.
    ///
    /// The range can be bounded or unbounded on either end, following Rust's
    /// standard [`RangeBounds`] trait. Entries are yielded in sorted order.
    pub fn stream_range<R, Backend>(
        &self,
        range: R,
        storage: &ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend + 'static
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
        R: RangeBounds<Key> + ConditionalSend + 'static,
    {
        let accessor = Accessor::new(self.delta.clone(), self.node_cache.clone(), storage.clone());

        TreeWalker::new(self.root.clone()).stream(range, accessor)
    }

    /// Flushes all pending changes, returning an iterator of nodes to be
    /// written to storage.
    ///
    /// This method drains the accumulated delta and returns hash-buffer pairs
    /// representing the new or modified nodes that need to be persisted to
    /// storage. After flushing, the tree's delta is cleared.
    ///
    /// In cases where the caller wishes to access the modified tree in the
    /// future, they should persist the flushed changes.
    pub fn flush(&mut self) -> impl Iterator<Item = (Blake3Hash, Buffer)> {
        self.delta.flush()
    }

    /// Creates a new tree version with an updated root hash and delta.
    ///
    /// This is an internal method used to produce a new tree state after
    /// modifications. The new tree shares the same cache as the original,
    /// enabling efficient structural sharing.
    fn advance(&self, root: Blake3Hash, delta: Delta<Blake3Hash, Buffer>) -> Self {
        Tree {
            key: PhantomData,
            value: PhantomData,
            root,
            node_cache: self.node_cache.clone(),
            delta,
        }
    }

    /// Searches for the leaf segment that would contain `key`, recording the
    /// path taken through the tree.
    ///
    /// This method traverses from the root to a leaf segment, following the
    /// child references whose key ranges encompass the target key. The search
    /// returns a [`SearchResult`] containing:
    /// - The leaf segment where the key would be located
    /// - The complete path from root to leaf, including sibling references
    ///
    /// The path information is essential for tree modification operations, as
    /// it enables efficient reconstruction of the tree after changes.
    ///
    /// Returns `None` if the tree is empty.
    async fn search<Backend>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
        options: SearchOptions,
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync
            + 'static,
    {
        let accessor = Accessor::new(self.delta.clone(), self.node_cache.clone(), storage.clone());

        TreeWalker::new(self.root.clone())
            .search(key, accessor, options)
            .await
    }
}

impl<Key, Value> From<Blake3Hash> for Tree<Key, Value>
where
    Key: self::Key + ConditionalSync + 'static,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord + ConditionalSync,
    Key::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    fn from(root: Blake3Hash) -> Self {
        Self::from_hash(root)
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    use crate::{ContentAddressedStorage, Tree};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_retrieves_inserted_values() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert a range of values
        for i in 0..100u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify we can retrieve all inserted values
        for i in 0..100u32 {
            let value = tree.get(&i.to_le_bytes(), &storage).await?;
            assert_eq!(value, Some(i.to_le_bytes().to_vec()));
        }

        // Verify non-existent keys return None
        let missing = tree.get(&200u32.to_le_bytes(), &storage).await?;
        assert_eq!(missing, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_values() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert values
        for i in 0..50u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Delete some values
        for i in (0..50u32).step_by(2) {
            tree = tree.delete(&i.to_le_bytes(), &storage).await?;
        }

        // Flush deletions
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify deleted values are gone
        for i in (0..50u32).step_by(2) {
            let value = tree.get(&i.to_le_bytes(), &storage).await?;
            assert_eq!(value, None, "Key {} should be deleted", i);
        }

        // Verify non-deleted values still exist
        for i in (1..50u32).step_by(2) {
            let value = tree.get(&i.to_le_bytes(), &storage).await?;
            assert_eq!(
                value,
                Some(i.to_le_bytes().to_vec()),
                "Key {} should exist",
                i
            );
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_entries_in_order() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert values in non-sequential order
        let values = [5u32, 2, 8, 1, 9, 3, 7, 4, 6, 0];
        for &i in &values {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Stream all entries and verify they come out sorted
        let stream = tree.stream(&storage);
        futures_util::pin_mut!(stream);

        let mut prev: Option<u32> = None;
        let mut count = 0;

        while let Some(entry) = stream.next().await {
            let entry = entry?;
            let key = u32::from_le_bytes(entry.key);
            let value = u32::from_le_bytes(entry.value.as_slice().try_into()?);

            assert_eq!(key, value);

            if let Some(prev_key) = prev {
                assert!(key > prev_key, "Keys should be in sorted order");
            }

            prev = Some(key);
            count += 1;
        }

        assert_eq!(count, values.len());
        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_range_queries() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert values 0-99
        for i in 0..100u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Test inclusive range [10..20)
        let stream = tree.stream_range(10u32.to_le_bytes()..20u32.to_le_bytes(), &storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            let key = u32::from_le_bytes(entry.key);
            assert!(
                (10..20).contains(&key),
                "Key {} should be in range [10, 20)",
                key
            );
            count += 1;
        }
        assert_eq!(count, 10, "Should have 10 entries in range");

        // Test inclusive range [50..=54]
        let stream = tree.stream_range(50u32.to_le_bytes()..=54u32.to_le_bytes(), &storage);
        futures_util::pin_mut!(stream);

        let mut collected = Vec::new();
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            let key = u32::from_le_bytes(entry.key);
            collected.push(key);
        }
        assert_eq!(collected, vec![50, 51, 52, 53, 54]);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_empty_range() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert values 0-9 and 90-99
        for i in 0..10u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for i in 90..100u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Query range with no entries [50..60)
        let stream = tree.stream_range(50u32.to_le_bytes()..60u32.to_le_bytes(), &storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }

        assert_eq!(count, 0, "Empty range should yield no entries");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_empty_tree_operations() -> Result<()> {
        use futures_util::StreamExt;

        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Get on empty tree should return None
        let value = tree.get(&1u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, None);

        // Delete on empty tree should be no-op
        let tree_after_delete = tree.delete(&1u32.to_le_bytes(), &storage).await?;
        assert_eq!(tree_after_delete.root(), tree.root());

        // Stream on empty tree should yield no entries
        let stream = tree.stream(&storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        assert_eq!(count, 0);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_null_root_when_empty() -> Result<()> {
        use dialog_common::NULL_BLAKE3_HASH;

        let tree = Tree::<[u8; 4], Vec<u8>>::empty();
        assert_eq!(tree.root(), &NULL_BLAKE3_HASH.clone());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_keys() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert initial value
        tree = tree
            .insert(42u32.to_le_bytes(), vec![1, 2, 3], &storage)
            .await?;

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify initial value
        let value = tree.get(&42u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, Some(vec![1, 2, 3]));

        // Update with new value
        tree = tree
            .insert(42u32.to_le_bytes(), vec![4, 5, 6, 7], &storage)
            .await?;

        // Flush update
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify updated value
        let value = tree.get(&42u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, Some(vec![4, 5, 6, 7]));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_old_tree_after_insert() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree_v1 = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert into v1
        tree_v1 = tree_v1
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?;

        // Flush v1
        for (key, value) in tree_v1.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let v1_root = tree_v1.root().clone();

        // Create v2 by inserting into v1
        let mut tree_v2 = tree_v1
            .insert(2u32.to_le_bytes(), vec![2], &storage)
            .await?;

        // Flush v2
        for (key, value) in tree_v2.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify v1 is unchanged
        assert_eq!(tree_v1.root(), &v1_root);
        assert_eq!(
            tree_v1.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1])
        );
        assert_eq!(tree_v1.get(&2u32.to_le_bytes(), &storage).await?, None);

        // Verify v2 has both entries
        assert_ne!(tree_v2.root(), &v1_root);
        assert_eq!(
            tree_v2.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1])
        );
        assert_eq!(
            tree_v2.get(&2u32.to_le_bytes(), &storage).await?,
            Some(vec![2])
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_creates_independent_tree_versions() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut base = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert base data
        for i in 0..10u32 {
            base = base
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }

        // Flush base
        for (key, value) in base.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Create two independent branches
        let mut branch_a = base
            .insert(100u32.to_le_bytes(), vec![100], &storage)
            .await?;
        let mut branch_b = base
            .insert(200u32.to_le_bytes(), vec![200], &storage)
            .await?;

        // Flush both branches
        for (key, value) in branch_a.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }
        for (key, value) in branch_b.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify branch A
        assert_eq!(
            branch_a.get(&100u32.to_le_bytes(), &storage).await?,
            Some(vec![100])
        );
        assert_eq!(branch_a.get(&200u32.to_le_bytes(), &storage).await?, None);

        // Verify branch B
        assert_eq!(branch_b.get(&100u32.to_le_bytes(), &storage).await?, None);
        assert_eq!(
            branch_b.get(&200u32.to_le_bytes(), &storage).await?,
            Some(vec![200])
        );

        // Verify base is still unchanged
        assert_eq!(base.get(&100u32.to_le_bytes(), &storage).await?, None);
        assert_eq!(base.get(&200u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_changes_root_after_modification() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        let root_empty = tree.root().clone();

        // Insert changes root
        tree = tree.insert(1u32.to_le_bytes(), vec![1], &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let root_after_insert = tree.root().clone();
        assert_ne!(root_after_insert, root_empty);

        // Another insert changes root again
        tree = tree.insert(2u32.to_le_bytes(), vec![2], &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let root_after_second_insert = tree.root().clone();
        assert_ne!(root_after_second_insert, root_after_insert);

        // Delete changes root
        tree = tree.delete(&1u32.to_le_bytes(), &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let root_after_delete = tree.root().clone();
        assert_ne!(root_after_delete, root_after_second_insert);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_same_root_for_identical_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Build tree A
        let mut tree_a = Tree::<[u8; 4], Vec<u8>>::empty();
        for i in 0..10u32 {
            tree_a = tree_a
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree_a.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Build tree B with same data
        let mut tree_b = Tree::<[u8; 4], Vec<u8>>::empty();
        for i in 0..10u32 {
            tree_b = tree_b
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree_b.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Trees with identical content should have same root (content-addressed)
        assert_eq!(tree_a.root(), tree_b.root());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reads_unflushed_insertions() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert without flushing
        tree = tree
            .insert(1u32.to_le_bytes(), vec![1, 2, 3], &storage)
            .await?;

        // Should be able to read from delta
        let value = tree.get(&1u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, Some(vec![1, 2, 3]));

        // Insert more without flushing
        tree = tree
            .insert(2u32.to_le_bytes(), vec![4, 5, 6], &storage)
            .await?;

        // Should read both from delta
        assert_eq!(
            tree.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            tree.get(&2u32.to_le_bytes(), &storage).await?,
            Some(vec![4, 5, 6])
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reads_through_delta_and_storage() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert and flush first batch
        for i in 0..5u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Insert second batch WITHOUT flushing
        for i in 5..10u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }

        // Should read 0-4 from storage and 5-9 from delta
        for i in 0..10u32 {
            let value = tree.get(&i.to_le_bytes(), &storage).await?;
            assert_eq!(value, Some(vec![i as u8]));
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_single_entry_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert single entry
        tree = tree
            .insert(42u32.to_le_bytes(), vec![1, 2, 3], &storage)
            .await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Get should work
        assert_eq!(
            tree.get(&42u32.to_le_bytes(), &storage).await?,
            Some(vec![1, 2, 3])
        );

        // Delete should work
        tree = tree.delete(&42u32.to_le_bytes(), &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Should be empty again
        assert_eq!(tree.get(&42u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_nonexistent_key_in_empty_tree() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        let root_before = tree.root().clone();

        // Delete from empty tree should be no-op
        tree = tree.delete(&1u32.to_le_bytes(), &storage).await?;

        // Root should be unchanged
        assert_eq!(tree.root(), &root_before);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_nonexistent_key_in_populated_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert some data
        tree = tree.insert(1u32.to_le_bytes(), vec![1], &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let root_before = tree.root().clone();

        // Delete non-existent key should be no-op
        tree = tree.delete(&999u32.to_le_bytes(), &storage).await?;

        // Root should be unchanged
        assert_eq!(tree.root(), &root_before);

        // Original data should still exist
        assert_eq!(
            tree.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1])
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_mixed_operations() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert
        tree = tree.insert(1u32.to_le_bytes(), vec![1], &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        assert_eq!(
            tree.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1])
        );

        // Delete
        tree = tree.delete(&1u32.to_le_bytes(), &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        assert_eq!(tree.get(&1u32.to_le_bytes(), &storage).await?, None);

        // Re-insert same key with different value
        tree = tree
            .insert(1u32.to_le_bytes(), vec![2, 3], &storage)
            .await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        assert_eq!(
            tree.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![2, 3])
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_same_root_regardless_of_insertion_order() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Build tree A with one insertion order
        let mut tree_a = Tree::<[u8; 4], Vec<u8>>::empty();
        tree_a = tree_a.insert(1u32.to_le_bytes(), vec![1], &storage).await?;
        tree_a = tree_a.insert(2u32.to_le_bytes(), vec![2], &storage).await?;
        tree_a = tree_a.insert(3u32.to_le_bytes(), vec![3], &storage).await?;

        for (key, value) in tree_a.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Build tree B with different insertion order
        let mut tree_b = Tree::<[u8; 4], Vec<u8>>::empty();
        tree_b = tree_b.insert(3u32.to_le_bytes(), vec![3], &storage).await?;
        tree_b = tree_b.insert(1u32.to_le_bytes(), vec![1], &storage).await?;
        tree_b = tree_b.insert(2u32.to_le_bytes(), vec![2], &storage).await?;

        for (key, value) in tree_b.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Different insertion orders should produce same root hash
        assert_eq!(tree_a.root(), tree_b.root());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_to_null_root_after_deleting_all_entries() -> Result<()> {
        use dialog_common::NULL_BLAKE3_HASH;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert some entries
        tree = tree.insert(1u32.to_le_bytes(), vec![1], &storage).await?;
        tree = tree.insert(2u32.to_le_bytes(), vec![2], &storage).await?;
        tree = tree.insert(3u32.to_le_bytes(), vec![3], &storage).await?;

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify tree is not empty
        assert_ne!(tree.root(), &NULL_BLAKE3_HASH.clone());

        // Delete all entries
        tree = tree.delete(&1u32.to_le_bytes(), &storage).await?;
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        tree = tree.delete(&2u32.to_le_bytes(), &storage).await?;
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        tree = tree.delete(&3u32.to_le_bytes(), &storage).await?;
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Tree should be back to empty state with null root
        assert_eq!(tree.root(), &NULL_BLAKE3_HASH.clone());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_out_of_bounds_range_queries() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert values 10-20
        for i in 10..=20u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }

        // Flush
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Query completely below range (0-5)
        let stream = tree.stream_range(0u32.to_le_bytes()..5u32.to_le_bytes(), &storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        assert_eq!(count, 0, "Range below all entries should yield nothing");

        // Query completely above range (30-40)
        let stream = tree.stream_range(30u32.to_le_bytes()..40u32.to_le_bytes(), &storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        assert_eq!(count, 0, "Range above all entries should yield nothing");

        // Query with only start out of bounds (25..)
        let stream = tree.stream_range(25u32.to_le_bytes().., &storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        assert_eq!(count, 0, "Start beyond all entries should yield nothing");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_larger_random_dataset() -> Result<()> {
        use rand::{Rng, thread_rng};

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();
        let mut ledger = Vec::new();

        // Generate and insert random data
        for _ in 0..1024 {
            let key = thread_rng().r#gen::<u32>().to_le_bytes();
            let value = thread_rng().r#gen::<[u8; 16]>().to_vec();
            ledger.push((key, value.clone()));
            tree = tree.insert(key, value, &storage).await?;
        }

        // Flush to storage
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Verify all entries can be retrieved
        for (key, expected_value) in ledger {
            let value = tree.get(&key, &storage).await?;
            assert_eq!(value, Some(expected_value));
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_restores_tree_from_persisted_root_hash() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..100u32 {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        let root = tree.root().clone();
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Reconstruct from just the root hash — no shared delta or cache
        let restored = Tree::<[u8; 4], Vec<u8>>::from_hash(root);

        for i in 0..100u32 {
            let value = restored.get(&i.to_le_bytes(), &storage).await?;
            assert_eq!(
                value,
                Some(i.to_le_bytes().to_vec()),
                "Key {} should be retrievable from restored tree",
                i
            );
        }

        assert_eq!(restored.get(&200u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_unflushed_insertions() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert and flush a base set
        for i in 0..10u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Insert more WITHOUT flushing
        for i in 10..20u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }

        // Stream should include both flushed and unflushed entries
        let stream = tree.stream(&storage);
        futures_util::pin_mut!(stream);

        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }

        assert_eq!(
            count, 20,
            "Stream should yield all entries including unflushed"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_unflushed_deletions() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Build and flush
        for i in 0..20u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Delete some entries WITHOUT flushing
        for i in (0..20u32).step_by(2) {
            tree = tree.delete(&i.to_le_bytes(), &storage).await?;
        }

        // Stream should reflect deletions even without flush
        let stream = tree.stream(&storage);
        futures_util::pin_mut!(stream);

        let mut keys = Vec::new();
        while let Some(entry) = stream.next().await {
            let entry = entry?;
            keys.push(u32::from_le_bytes(entry.key));
        }

        let expected: Vec<u32> = (1..20u32).step_by(2).collect();
        assert_eq!(keys, expected, "Stream should exclude unflushed deletions");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_all_entries_after_boundary_deletion() -> Result<()> {
        use crate::distribution;
        use dialog_common::Blake3Hash;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let all_keys: Vec<u32> = (0..1000).collect();

        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();
        for &k in &all_keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Find boundary keys
        let boundaries: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&i| distribution::geometric::rank(&Blake3Hash::hash(&i.to_le_bytes())) > 1)
            .collect();

        for &bk in boundaries.iter().take(3) {
            let mut after_delete = tree.delete(&bk.to_le_bytes(), &storage).await?;
            for (h, b) in after_delete.flush() {
                storage.store(b.as_ref().to_vec(), &h).await?;
            }

            // Deleted key should be gone
            assert_eq!(
                after_delete.get(&bk.to_le_bytes(), &storage).await?,
                None,
                "Deleted boundary key {bk} should not be found"
            );

            // Every other key must still return the correct value
            for &k in &all_keys {
                if k == bk {
                    continue;
                }
                let value = after_delete.get(&k.to_le_bytes(), &storage).await?;
                assert_eq!(
                    value,
                    Some(k.to_le_bytes().to_vec()),
                    "Key {k} should still be accessible after deleting boundary {bk}"
                );
            }
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_to_null_root_after_sequential_deletion_of_many_entries() -> Result<()> {
        use dialog_common::NULL_BLAKE3_HASH;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..500u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Delete all entries one at a time
        for i in 0..500u32 {
            tree = tree.delete(&i.to_le_bytes(), &storage).await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        assert_eq!(
            tree.root(),
            &NULL_BLAKE3_HASH.clone(),
            "Tree should be empty after deleting all entries"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_root_when_upserting_identical_value() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..50u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let root_before = tree.root().clone();

        // Re-insert key 25 with the same value
        tree = tree
            .insert(25u32.to_le_bytes(), vec![25u8], &storage)
            .await?;

        assert_eq!(
            tree.root(),
            &root_before,
            "Upserting the same key+value should not change the root"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_entries_in_byte_lexicographic_order() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Insert keys whose byte and numeric orders differ.
        // 1u32 = [1,0,0,0], 256u32 = [0,1,0,0], 512u32 = [0,2,0,0]
        // Byte order: [0,1,0,0] < [0,2,0,0] < [1,0,0,0]
        // Numeric order: 1 < 256 < 512
        let numeric_keys: Vec<u32> = vec![1, 256, 512, 2, 257, 0];

        for &k in &numeric_keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        let stream = tree.stream(&storage);
        futures_util::pin_mut!(stream);

        let mut keys_out: Vec<[u8; 4]> = Vec::new();
        while let Some(entry) = stream.next().await {
            keys_out.push(entry?.key);
        }

        // Verify byte-lexicographic order
        for pair in keys_out.windows(2) {
            assert!(
                pair[0] < pair[1],
                "Keys should be in byte-lexicographic order: {:?} should precede {:?}",
                pair[0],
                pair[1]
            );
        }

        // Verify the specific expected order
        let mut expected = numeric_keys
            .iter()
            .map(|k| k.to_le_bytes())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(keys_out, expected);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_range_with_excluded_start() -> Result<()> {
        use futures_util::StreamExt;
        use std::ops::Bound;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..20u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Excluded start: should not include key 5
        let range = (
            Bound::Excluded(5u32.to_le_bytes()),
            Bound::Included(10u32.to_le_bytes()),
        );
        let stream = tree.stream_range(range, &storage);
        futures_util::pin_mut!(stream);

        let mut keys = Vec::new();
        while let Some(entry) = stream.next().await {
            keys.push(u32::from_le_bytes(entry?.key));
        }

        assert_eq!(
            keys,
            vec![6, 7, 8, 9, 10],
            "Excluded start should skip key 5"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_range_with_unbounded_end() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..20u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // start.. (unbounded end)
        let stream = tree.stream_range(15u32.to_le_bytes().., &storage);
        futures_util::pin_mut!(stream);

        let mut keys = Vec::new();
        while let Some(entry) = stream.next().await {
            keys.push(u32::from_le_bytes(entry?.key));
        }

        assert_eq!(keys, vec![15, 16, 17, 18, 19]);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_streams_single_point_range() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        for i in 0..20u32 {
            tree = tree
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in tree.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // key..=key should return exactly one entry
        let k = 10u32.to_le_bytes();
        let stream = tree.stream_range(k..=k, &storage);
        futures_util::pin_mut!(stream);

        let mut keys = Vec::new();
        while let Some(entry) = stream.next().await {
            keys.push(u32::from_le_bytes(entry?.key));
        }

        assert_eq!(keys, vec![10], "Single-point range should yield one entry");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_produces_canonical_structure_regardless_of_insertion_order() -> Result<()> {
        use crate::distribution;
        use dialog_common::Blake3Hash;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let keys: Vec<u32> = (0..1000).collect();

        // Sanity check: with 1000 keys we expect ~8 boundary entries
        // (rank > 1). If none exist, the test can't exercise the bug.
        let boundary_count = keys
            .iter()
            .filter(|&&k| distribution::geometric::rank(&Blake3Hash::hash(&k.to_le_bytes())) > 1)
            .count();
        assert!(
            boundary_count > 0,
            "Test requires at least one boundary key to be meaningful"
        );

        // Build tree A: forward insertion order
        let mut tree_a = Tree::<[u8; 4], Vec<u8>>::empty();
        for &k in &keys {
            tree_a = tree_a
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for (key, value) in tree_a.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Build tree B: reverse insertion order
        let mut tree_b = Tree::<[u8; 4], Vec<u8>>::empty();
        for &k in keys.iter().rev() {
            tree_b = tree_b
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for (key, value) in tree_b.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // A canonical prolly tree must produce the same structure — and
        // therefore the same root hash — for the same set of entries,
        // regardless of insertion order.
        assert_eq!(
            tree_a.root(),
            tree_b.root(),
            "Trees with identical entries should have the same root \
             regardless of insertion order ({boundary_count} boundary \
             keys in dataset)"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_interleaved_operations_across_tree_versions() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Create base tree
        let mut base = Tree::<[u8; 4], Vec<u8>>::empty();
        for i in 0..20u32 {
            base = base
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?;
        }
        for (key, value) in base.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Create two branches
        let mut branch_a = base.clone();
        let mut branch_b = base.clone();

        // Interleave operations
        branch_a = branch_a
            .insert(100u32.to_le_bytes(), vec![100], &storage)
            .await?;
        branch_b = branch_b.delete(&5u32.to_le_bytes(), &storage).await?;
        branch_a = branch_a
            .insert(101u32.to_le_bytes(), vec![101], &storage)
            .await?;
        branch_b = branch_b.delete(&10u32.to_le_bytes(), &storage).await?;

        // Flush both
        for (key, value) in branch_a.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }
        for (key, value) in branch_b.flush() {
            storage.store(value.as_ref().to_vec(), &key).await?;
        }

        // Branch A: base + 100, 101
        assert_eq!(
            branch_a.get(&100u32.to_le_bytes(), &storage).await?,
            Some(vec![100])
        );
        assert_eq!(
            branch_a.get(&101u32.to_le_bytes(), &storage).await?,
            Some(vec![101])
        );
        assert_eq!(
            branch_a.get(&5u32.to_le_bytes(), &storage).await?,
            Some(vec![5]),
            "Branch A should still have key 5"
        );

        // Branch B: base - 5, 10
        assert_eq!(branch_b.get(&5u32.to_le_bytes(), &storage).await?, None);
        assert_eq!(branch_b.get(&10u32.to_le_bytes(), &storage).await?, None);
        assert_eq!(branch_b.get(&100u32.to_le_bytes(), &storage).await?, None);

        // Base unchanged
        assert_eq!(
            base.get(&5u32.to_le_bytes(), &storage).await?,
            Some(vec![5])
        );
        assert_eq!(base.get(&100u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reads_unflushed_entries_after_boundary_delete() -> Result<()> {
        use crate::distribution;
        use dialog_common::Blake3Hash;
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let all_keys: Vec<u32> = (0..1000).collect();

        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();
        for &k in &all_keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for (h, b) in tree.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }

        let boundaries: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&i| distribution::geometric::rank(&Blake3Hash::hash(&i.to_le_bytes())) > 1)
            .collect();
        assert!(
            !boundaries.is_empty(),
            "test requires at least one boundary"
        );

        for &bk in boundaries.iter().take(3) {
            let after_delete = tree.delete(&bk.to_le_bytes(), &storage).await?;

            // Read every surviving key WITHOUT flushing. Any missing new
            // node in the delta surfaces here as a node-not-found error.
            for &k in &all_keys {
                if k == bk {
                    assert_eq!(after_delete.get(&k.to_le_bytes(), &storage).await?, None);
                    continue;
                }
                assert_eq!(
                    after_delete.get(&k.to_le_bytes(), &storage).await?,
                    Some(k.to_le_bytes().to_vec()),
                    "key {k} should be readable from unflushed tree after deleting boundary {bk}"
                );
            }

            // Stream the whole tree too; same guarantee, different path.
            let expected_count = all_keys.len() - 1;
            let stream = after_delete.stream(&storage);
            futures_util::pin_mut!(stream);
            let mut count = 0;
            while let Some(entry) = stream.next().await {
                entry?;
                count += 1;
            }
            assert_eq!(
                count, expected_count,
                "stream should yield {expected_count} entries from unflushed tree after deleting boundary {bk}"
            );
        }

        Ok(())
    }

    #[dialog_common::test]
    async fn it_removes_orphaned_hashes_from_delta_after_mutation() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // After a single insert into an empty tree the delta holds
        // exactly the root's new subtree (here: one level-1 index and
        // one leaf segment). Record its root hash.
        tree = tree.insert(1u32.to_le_bytes(), vec![1], &storage).await?;
        let root_after_first = tree.root().clone();
        assert!(
            tree.delta.get(&root_after_first).is_some(),
            "root of just-inserted tree should be in the delta"
        );

        // A second insert replaces the root and (because the insert
        // rewrites the only leaf) the old leaf segment too. The
        // pre-mutation root hash is unreachable from the new tree and
        // must not linger in the delta.
        tree = tree.insert(2u32.to_le_bytes(), vec![2], &storage).await?;
        assert!(
            tree.delta.get(&root_after_first).is_none(),
            "old root hash should be evicted from delta after follow-up insert",
        );

        // Update-in-place (same key, new value): the old leaf and old
        // root are replaced; both should be evicted.
        let root_before_update = tree.root().clone();
        tree = tree.insert(1u32.to_le_bytes(), vec![99], &storage).await?;
        assert!(
            tree.delta.get(&root_before_update).is_none(),
            "old root hash should be evicted from delta after update-in-place",
        );

        // Non-boundary delete: rewrites the leaf + root along the path,
        // previous root must be gone.
        let root_before_delete = tree.root().clone();
        tree = tree.delete(&2u32.to_le_bytes(), &storage).await?;
        assert!(
            tree.delta.get(&root_before_delete).is_none(),
            "old root hash should be evicted from delta after non-boundary delete",
        );

        // Delete-to-empty: the remaining leaf vanishes; tree root becomes
        // the null hash and the last lingering content root must go.
        let root_before_empty = tree.root().clone();
        tree = tree.delete(&1u32.to_le_bytes(), &storage).await?;
        assert!(
            tree.delta.get(&root_before_empty).is_none(),
            "old root hash should be evicted from delta after delete-to-empty",
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_keeps_delta_bounded_across_unflushed_mutations() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = Tree::<[u8; 4], Vec<u8>>::empty();

        // Build and flush a baseline so the delta starts empty.
        let base_size: u32 = 200;
        for i in 0..base_size {
            tree = tree
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        for (h, b) in tree.flush() {
            storage.store(b.as_ref().to_vec(), &h).await?;
        }
        assert_eq!(tree.delta.len(), 0);

        // Update each existing key in place many times. Entry count and
        // tree shape stay the same, so delta size should stabilize near
        // the affected-path cost (≈ tree height × a small constant per
        // modified key, with boundary keys possibly touching a few more
        // ancestors). If orphan-removal regresses, this grows linearly
        // with the number of updates.
        let mutations: u32 = 400;
        for i in 0..mutations {
            let key = (i % base_size).to_le_bytes();
            tree = tree.insert(key, vec![(i % 255) as u8], &storage).await?;
        }

        // Loose but informative upper bound: the entire live tree
        // (every segment plus every index) can't exceed `base_size`
        // nodes, since each node holds at least one entry. Any linear
        // accumulation would blow past this threshold for 400 updates
        // against 200 entries.
        let live_upper_bound = base_size as usize;
        assert!(
            tree.delta.len() <= live_upper_bound,
            "delta grew to {} after {mutations} updates against {base_size} entries; \
             expected ≤ {live_upper_bound} (tree's live node count)",
            tree.delta.len(),
        );

        Ok(())
    }
}
