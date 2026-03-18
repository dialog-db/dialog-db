use std::{marker::PhantomData, ops::RangeBounds};

use dialog_common::{Blake3Hash, NULL_BLAKE3_HASH};
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
    Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError, Entry, Key, Node,
    SearchResult, SymmetryWith, TreeShaper, TreeWalker, Value, into_owned,
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
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
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
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        if let Some(result) = self.search(key, storage).await? {
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
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        let search_result = self.search(&key, storage).await?;
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
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        if let Some(search_result) = self.search(key, storage).await? {
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
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
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
    pub fn stream_range<'a, R, Backend>(
        &'a self,
        range: R,
        storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
        R: RangeBounds<Key> + 'a,
    {
        TreeWalker::new(self.root.clone(), async |hash| {
            self.get_node(hash, storage).await
        })
        .stream(range)
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

    /// Retrieves a node from cache, delta, or storage.
    ///
    /// This method implements a multi-level lookup strategy:
    /// 1. Check the node cache for a previously loaded node
    /// 2. Check the delta for a recently created/modified node
    /// 3. Fetch from persistent storage as a last resort
    ///
    /// Fetched nodes are automatically added to the cache for future access.
    /// This layered approach minimizes expensive storage I/O operations.
    async fn get_node<Backend>(
        &self,
        hash: &Blake3Hash,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Node<Key, Value>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        self.node_cache
            .get_or_fetch(hash, async move |key| {
                if let Some(buffer) = self.delta.get(hash) {
                    Ok(Some(buffer))
                } else {
                    storage
                        .retrieve(key)
                        .await
                        .map(|maybe_bytes| maybe_bytes.map(Buffer::from))
                }
            })
            .await?
            .ok_or_else(|| {
                DialogSearchTreeError::Node(format!("Blob not found in storage: {}", hash))
            })
            .map(|buffer| Node::new(buffer))
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
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>,
    {
        TreeWalker::new(self.root.clone(), async |hash| {
            self.get_node(hash, storage).await
        })
        .search(key)
        .await
    }
}

impl<Key, Value> From<Blake3Hash> for Tree<Key, Value>
where
    Key: self::Key,
    Key: PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Key::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Key::Archived: Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Key: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
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
}
