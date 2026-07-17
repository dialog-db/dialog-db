mod transient;
pub use transient::*;

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
    Accessor, Buffer, Cache, ContentAddressedStorage, DialogSearchTreeError, Differential,
    Distribution, Entry, Geometric, Key, SearchOptions, SearchResult, TreeDifference, TreeWalker,
    Value, into_owned,
};

/// A key-value store backed by a ranked prolly tree with content-addressed
/// storage.
///
/// The [`PersistentTree`] represents an immutable, persistent data structure
/// where each modification creates a new version while sharing unchanged nodes
/// through structural sharing. The tree stores key-value pairs in sorted order
/// and provides efficient lookups and range queries. It is read-only;
/// modifications go through [`edit`](Self::edit), which opens a
/// [`TransientTree`] batch that seals back into a new [`PersistentTree`] via
/// [`persist`](TransientTree::persist).
///
/// Nodes are stored in content-addressed storage using [`Blake3Hash`] as
/// identifiers, enabling deduplication and efficient versioning. The tree
/// maintains a [`Delta`] of pending changes that can be flushed to storage in
/// batches, and uses a [`Cache`] for frequently accessed nodes to minimize
/// storage I/O operations.
///
/// ## Clone Semantics
///
/// Cloning a [`PersistentTree`] does **not** fork the internal state. Instead,
/// it shares the cache and delta via reference counting. An edit batch
/// implicitly forks the tree by producing a new instance with a branched
/// [`Delta`] (independent copy) and an updated root hash, while continuing to
/// share the [`Cache`]. This enables efficient versioning where multiple tree
/// versions share the same cache for read operations, but maintain independent
/// mutation state.
#[derive(Debug)]
pub struct PersistentTree<Key, Value, D = Geometric>
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    key: PhantomData<Key>,
    value: PhantomData<Value>,
    distribution: PhantomData<D>,

    root: Blake3Hash,
    node_cache: Cache<Blake3Hash, Buffer>,
}

// Manual impl: a derived `Clone` would demand `D: Clone`, but the
// distribution is a pure type-level strategy that is never instantiated.
impl<Key, Value, D> Clone for PersistentTree<Key, Value, D>
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    fn clone(&self) -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            distribution: PhantomData,
            root: self.root.clone(),
            node_cache: self.node_cache.clone(),
        }
    }
}

impl<Key, Value, D> PersistentTree<Key, Value, D>
where
    Key: self::Key + ConditionalSync + 'static,
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
    D: Distribution,
{
    /// Returns the [`Blake3Hash`] of the root node of this tree.
    ///
    /// The root hash uniquely identifies this version of the tree and can be
    /// used to reconstruct the tree from storage or to compare tree versions.
    pub fn root(&self) -> &Blake3Hash {
        &self.root
    }

    /// Creates a new empty [`PersistentTree`] with no entries.
    ///
    /// The empty tree has a null root hash and an empty node cache.
    pub fn empty() -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            distribution: PhantomData,
            root: NULL_BLAKE3_HASH.clone(),
            node_cache: Cache::new(),
        }
    }

    /// Creates a [`PersistentTree`] from a known root hash.
    ///
    /// This constructor is used to restore a tree to a previously persisted
    /// version. The tree will lazily load nodes from storage as they are
    /// accessed during operations.
    pub fn from_hash(root: Blake3Hash) -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            distribution: PhantomData,
            root,
            node_cache: Cache::new(),
        }
    }

    /// Creates a [`PersistentTree`] from a known root hash, reusing an existing
    /// node cache instead of allocating a fresh empty one.
    ///
    /// Nodes are content-addressed: a [`Blake3Hash`] always maps to the same
    /// bytes, so a cache may be shared freely across tree versions and revisions
    /// without ever serving a stale entry. Use this to keep a cache warm across
    /// successive reconstructions of a tree from a moving root (e.g. a branch
    /// that reuses one cache across every read).
    pub fn from_hash_with_cache(root: Blake3Hash, node_cache: Cache<Blake3Hash, Buffer>) -> Self {
        Self::seal(root, node_cache)
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
            + ConditionalSync,
    {
        // The search path is the copy-on-write frontier for an update; a read
        // ignores it and takes only the leaf. Building it is allocation-free
        // (each layer is an Arc-backed node plus a child index), so the read
        // pays nothing for the siblings an update would later decode.
        if let Some(result) = self.search(key, storage, SearchOptions::default()).await? {
            let segment = result.leaf.as_segment()?;
            if let Some(at) = segment.find::<Key>(key.as_ref())? {
                into_owned(segment.value_at(at)?).map(Some)
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
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
            + ConditionalSync,
    {
        self.stream_range(.., storage)
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
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        R: RangeBounds<Key> + ConditionalSend,
    {
        let accessor = Accessor::new(self.node_cache.clone(), storage.clone());

        TreeWalker::new(self.root.clone()).stream(range, accessor)
    }

    /// Returns a differential that produces changes to transform `self` into
    /// `other`.
    ///
    /// Usage: applying the changes to `self` (via
    /// [`integrate`](TransientTree::integrate) on an edit batch) results in
    /// `other`. Only blocks on differing paths are read; see
    /// [`TreeDifference`](crate::TreeDifference) for the frugality contract.
    pub fn differentiate<'a, Backend>(
        &'a self,
        other: &'a Self,
        self_storage: &'a ContentAddressedStorage<Backend>,
        other_storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Differential<Key, Value> + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        Value: PartialEq,
    {
        async_stream::try_stream! {
            let difference =
                TreeDifference::compute(self, other, self_storage, other_storage).await?;
            for await change in difference.changes() {
                yield change?;
            }
        }
    }

    /// Streams the changes *within `scope`* that transform this tree into
    /// `other`.
    ///
    /// Like [`differentiate`](Self::differentiate), but subtrees whose key
    /// span cannot intersect any scope range are dropped from the
    /// comparison without being loaded: reads are proportional to the
    /// differing regions within the scope, not to the full difference. On
    /// a partial replica this keeps the diff from fetching subtrees the
    /// caller never demanded.
    pub fn differentiate_within<'a, Backend>(
        &'a self,
        other: &'a Self,
        scope: &'a [core::ops::RangeInclusive<Key>],
        self_storage: &'a ContentAddressedStorage<Backend>,
        other_storage: &'a ContentAddressedStorage<Backend>,
    ) -> impl Differential<Key, Value> + ConditionalSend + 'a
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        // `Key`/`Value` bound `ConditionalSync` (not just the trait's
        // `ConditionalSend`) so that `&PersistentTree` — which the
        // returned `async_stream` captures — is `Send` on native.
        // `PersistentTree` holds `PhantomData<Key>`/`PhantomData<Value>`,
        // so its `Sync` (hence `&_: Send`) needs `Key: Sync`/`Value: Sync`.
        // Subscriptions poll this diff inside an axum handler future that
        // must be `Send`; without the bound the whole handler is `!Send`.
        Key: ConditionalSync,
        Value: PartialEq + ConditionalSync,
        D: ConditionalSync,
    {
        async_stream::try_stream! {
            let difference =
                TreeDifference::compute_within(self, other, self_storage, other_storage, scope)
                    .await?;
            for await change in difference.changes_within(scope) {
                yield change?;
            }
        }
    }

    /// Builds a persistent tree from a sealed root hash and a node cache. Used
    /// by [`TransientTree::persist`] to turn a finished edit batch back into a
    /// [`PersistentTree`] while carrying its cache forward. The batch's new nodes
    /// go into the caller's delta, not the tree.
    pub(crate) fn seal(root: Blake3Hash, node_cache: Cache<Blake3Hash, Buffer>) -> Self {
        PersistentTree {
            key: PhantomData,
            value: PhantomData,
            distribution: PhantomData,
            root,
            node_cache,
        }
    }

    /// Opens a batch of in-place edits over this tree.
    ///
    /// The returned [`TransientTree`] holds the tree's spine in transient form;
    /// apply [`insert`](TransientTree::insert) / [`delete`](TransientTree::delete)
    /// to mutate it and [`persist`](TransientTree::persist) to seal the batch
    /// back into a [`PersistentTree`]. A single batch and the equivalent sequence
    /// of one-operation batches each persisted in turn converge on the same root.
    ///
    /// Opening is synchronous and touches no storage: the root is loaded lazily
    /// by the first edit that descends into it. Equivalent to
    /// [`TransientTree::from`].
    pub fn edit(&self) -> TransientTree<Key, Value, D> {
        TransientTree::new(self.root.clone(), self.node_cache.clone())
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
            + ConditionalSync,
    {
        let accessor = Accessor::new(self.node_cache.clone(), storage.clone());

        TreeWalker::new(self.root.clone())
            .search(key, accessor, options)
            .await
    }
}

impl<Key, Value, D> From<Blake3Hash> for PersistentTree<Key, Value, D>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    fn from(root: Blake3Hash) -> Self {
        Self::from_hash(root)
    }
}

impl<Key, Value, D> From<&PersistentTree<Key, Value, D>> for TransientTree<Key, Value, D>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    fn from(tree: &PersistentTree<Key, Value, D>) -> Self {
        tree.edit()
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    use crate::{ContentAddressedStorage, Delta, PersistentTree};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    #[dialog_common::test]
    async fn it_retrieves_inserted_values() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert a range of values
        for i in 0..100u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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

    /// A key type whose component layout is dispatched by a leading tag byte,
    /// like the dialog artifact key's EAV/AEV/VAE orderings. Two layouts:
    /// tag 0 puts the 2-byte arena field first, tag 1 puts the 1-byte
    /// dictionary field first. Persisting keys of both tags into one tree and
    /// reading them all back proves the tag-dispatched columnar codec.
    mod tag_dispatched {
        use crate::{Component, DialogSearchTreeError, Key, Schema};

        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub(super) struct TaggedKey(pub [u8; 4]);

        // tag 0: tag(1) ++ arena(2) ++ dict(1)
        const LAYOUT0: &[Component] = &[
            Component::dictionary(1),
            Component::arena(2),
            Component::dictionary(1),
        ];
        // tag 1: tag(1) ++ dict(1) ++ arena(2)
        const LAYOUT1: &[Component] = &[
            Component::dictionary(1),
            Component::dictionary(1),
            Component::arena(2),
        ];

        impl AsRef<[u8]> for TaggedKey {
            fn as_ref(&self) -> &[u8] {
                &self.0
            }
        }

        impl Key for TaggedKey {
            fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
                bytes
                    .try_into()
                    .map(TaggedKey)
                    .map_err(|_| DialogSearchTreeError::Encoding("bad tagged key".into()))
            }

            fn min() -> Self {
                TaggedKey([0; 4])
            }

            fn max() -> Self {
                TaggedKey([u8::MAX; 4])
            }

            fn layout(&self) -> u8 {
                self.0[0]
            }

            fn schema(layout: u8) -> Schema {
                match layout {
                    0 => Schema::new(LAYOUT0),
                    _ => Schema::new(LAYOUT1),
                }
            }

            fn components<'a>(&'a self, out: &mut Vec<&'a [u8]>) {
                let widths: [usize; 3] = if self.0[0] == 0 { [1, 2, 1] } else { [1, 1, 2] };
                let mut at = 0;
                for width in widths {
                    out.push(&self.0[at..at + width]);
                    at += width;
                }
            }
        }
    }

    #[dialog_common::test]
    async fn it_round_trips_tag_dispatched_layouts() -> Result<()> {
        use tag_dispatched::TaggedKey;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<TaggedKey, Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Keys of both tags. Tag sorts first, so tag-0 keys form one run of
        // leaves and tag-1 keys another; each leaf is single-layout.
        let mut keys: Vec<TaggedKey> = Vec::new();
        for tag in 0u8..2 {
            for a in 0u8..16 {
                for b in 0u8..8 {
                    keys.push(TaggedKey([tag, a, b, (a ^ b) % 4]));
                }
            }
        }
        keys.sort();

        for key in &keys {
            tree = tree
                .edit()
                .insert(key.clone(), key.0.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Every key reads back under its own layout.
        for key in &keys {
            assert_eq!(
                tree.get(key, &storage).await?,
                Some(key.0.to_vec()),
                "tagged key {:?} must round-trip",
                key.0
            );
        }

        // A full scan yields every key in order, decoding both layouts.
        use futures_util::StreamExt;
        let scanned: Vec<TaggedKey> = tree
            .stream(&storage)
            .map(|entry| entry.map(|entry| entry.key))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(scanned, keys, "scan must yield all keys, both layouts");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_values() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert values
        for i in 0..50u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Delete some values
        for i in (0..50u32).step_by(2) {
            tree = tree
                .edit()
                .delete(&i.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert values in non-sequential order
        let values = [5u32, 2, 8, 1, 9, 3, 7, 4, 6, 0];
        for &i in &values {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert values 0-99
        for i in 0..100u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert values 0-9 and 90-99
        for i in 0..10u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        for i in 90..100u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();

        // Get on empty tree should return None
        let value = tree.get(&1u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, None);

        // Delete on empty tree should be no-op
        let mut delta = Delta::zero();
        let tree_after_delete = tree
            .edit()
            .delete(&1u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
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

        let tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        assert_eq!(tree.root(), &NULL_BLAKE3_HASH.clone());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_updates_existing_keys() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert initial value
        tree = tree
            .edit()
            .insert(42u32.to_le_bytes(), vec![1, 2, 3], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush to storage
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Verify initial value
        let value = tree.get(&42u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, Some(vec![1, 2, 3]));

        // Update with new value
        tree = tree
            .edit()
            .insert(42u32.to_le_bytes(), vec![4, 5, 6, 7], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush update
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Verify updated value
        let value = tree.get(&42u32.to_le_bytes(), &storage).await?;
        assert_eq!(value, Some(vec![4, 5, 6, 7]));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_preserves_old_tree_after_insert() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree_v1 = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_v1 = Delta::zero();

        // Insert into v1
        tree_v1 = tree_v1
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta_v1)?;

        // Flush v1
        for (_, buffer) in delta_v1.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let v1_root = tree_v1.root().clone();

        // Create v2 by inserting into v1
        let mut delta_v2 = Delta::zero();
        let tree_v2 = tree_v1
            .edit()
            .insert(2u32.to_le_bytes(), vec![2], &storage)
            .await?
            .persist(&mut delta_v2)?;

        // Flush v2
        for (_, buffer) in delta_v2.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
        let mut base = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_base = Delta::zero();

        // Insert base data
        for i in 0..10u32 {
            base = base
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta_base)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta_base.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Create two independent branches
        let mut delta_a = Delta::zero();
        let mut delta_b = Delta::zero();
        let branch_a = base
            .edit()
            .insert(100u32.to_le_bytes(), vec![100], &storage)
            .await?
            .persist(&mut delta_a)?;
        let branch_b = base
            .edit()
            .insert(200u32.to_le_bytes(), vec![200], &storage)
            .await?
            .persist(&mut delta_b)?;

        // Flush both branches
        for (_, buffer) in delta_a.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        for (_, buffer) in delta_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        let root_empty = tree.root().clone();

        // Insert changes root
        tree = tree
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let root_after_insert = tree.root().clone();
        assert_ne!(root_after_insert, root_empty);

        // Another insert changes root again
        tree = tree
            .edit()
            .insert(2u32.to_le_bytes(), vec![2], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let root_after_second_insert = tree.root().clone();
        assert_ne!(root_after_second_insert, root_after_insert);

        // Delete changes root
        tree = tree
            .edit()
            .delete(&1u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let root_after_delete = tree.root().clone();
        assert_ne!(root_after_delete, root_after_second_insert);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_has_same_root_for_identical_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Build tree A
        let mut tree_a = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_a = Delta::zero();
        for i in 0..10u32 {
            tree_a = tree_a
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta_a)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta_a.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Build tree B with same data
        let mut tree_b = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_b = Delta::zero();
        for i in 0..10u32 {
            tree_b = tree_b
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta_b)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta_b.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Trees with identical content should have same root (content-addressed)
        assert_eq!(tree_a.root(), tree_b.root());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_single_entry_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert single entry
        tree = tree
            .edit()
            .insert(42u32.to_le_bytes(), vec![1, 2, 3], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Get should work
        assert_eq!(
            tree.get(&42u32.to_le_bytes(), &storage).await?,
            Some(vec![1, 2, 3])
        );

        // Delete should work
        tree = tree
            .edit()
            .delete(&42u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Should be empty again
        assert_eq!(tree.get(&42u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_nonexistent_key_in_empty_tree() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();

        let root_before = tree.root().clone();

        // Delete from empty tree should be no-op
        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .delete(&1u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;

        // Root should be unchanged
        assert_eq!(tree.root(), &root_before);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_deletes_nonexistent_key_in_populated_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();

        // Insert some data
        let mut delta = Delta::zero();
        tree = tree
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let root_before = tree.root().clone();

        // Delete non-existent key should be no-op
        tree = tree
            .edit()
            .delete(&999u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;

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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert
        tree = tree
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        assert_eq!(
            tree.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1])
        );

        // Delete
        tree = tree
            .edit()
            .delete(&1u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        assert_eq!(tree.get(&1u32.to_le_bytes(), &storage).await?, None);

        // Re-insert same key with different value
        tree = tree
            .edit()
            .insert(1u32.to_le_bytes(), vec![2, 3], &storage)
            .await?
            .persist(&mut delta)?;

        // Flush
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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
        let mut tree_a = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_a = Delta::zero();
        tree_a = tree_a
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta_a)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_a.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        tree_a = tree_a
            .edit()
            .insert(2u32.to_le_bytes(), vec![2], &storage)
            .await?
            .persist(&mut delta_a)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_a.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        tree_a = tree_a
            .edit()
            .insert(3u32.to_le_bytes(), vec![3], &storage)
            .await?
            .persist(&mut delta_a)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_a.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Build tree B with different insertion order
        let mut tree_b = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_b = Delta::zero();
        tree_b = tree_b
            .edit()
            .insert(3u32.to_le_bytes(), vec![3], &storage)
            .await?
            .persist(&mut delta_b)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        tree_b = tree_b
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta_b)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        tree_b = tree_b
            .edit()
            .insert(2u32.to_le_bytes(), vec![2], &storage)
            .await?
            .persist(&mut delta_b)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Different insertion orders should produce same root hash
        assert_eq!(tree_a.root(), tree_b.root());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_to_null_root_after_deleting_all_entries() -> Result<()> {
        use dialog_common::NULL_BLAKE3_HASH;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert some entries
        tree = tree
            .edit()
            .insert(1u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        tree = tree
            .edit()
            .insert(2u32.to_le_bytes(), vec![2], &storage)
            .await?
            .persist(&mut delta)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        tree = tree
            .edit()
            .insert(3u32.to_le_bytes(), vec![3], &storage)
            .await?
            .persist(&mut delta)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Verify tree is not empty
        assert_ne!(tree.root(), &NULL_BLAKE3_HASH.clone());

        // Delete all entries
        tree = tree
            .edit()
            .delete(&1u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        tree = tree
            .edit()
            .delete(&2u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        tree = tree
            .edit()
            .delete(&3u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        // Tree should be back to empty state with null root
        assert_eq!(tree.root(), &NULL_BLAKE3_HASH.clone());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_out_of_bounds_range_queries() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert values 10-20
        for i in 10..=20u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        let mut ledger = Vec::new();

        // Generate and insert random data
        for _ in 0..1024 {
            let key = thread_rng().r#gen::<u32>().to_le_bytes();
            let value = thread_rng().r#gen::<[u8; 16]>().to_vec();
            ledger.push((key, value.clone()));
            tree = tree
                .edit()
                .insert(key, value, &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Verify all entries can be retrieved
        for (key, expected_value) in ledger {
            let value = tree.get(&key, &storage).await?;
            assert_eq!(value, Some(expected_value));
        }

        Ok(())
    }

    /// `get` descends with [`TreeWalker::find_leaf`], a read-only path that
    /// binary-searches each index node instead of walking and materializing the
    /// sibling links the way the insert/delete `search` does. This pins that the
    /// two descents agree on every key, including the boundary cases the binary
    /// search's `partition_point` must get right: a key below the minimum, keys
    /// that sit exactly on an index `upper_bound`, keys between stored keys, and
    /// a key above the maximum (which must fall through to the last child).
    #[dialog_common::test]
    async fn it_gets_present_and_absent_keys_across_index_boundaries() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();

        // Even keys only, so every odd key probes a gap; spread wide enough to
        // force a multi-level index whose boundaries the descent must navigate.
        let present: Vec<u32> = (0..4000u32).map(|i| i * 2).collect();
        let mut delta = Delta::zero();
        for &k in &present {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Every present (even) key resolves to its value.
        for &k in &present {
            assert_eq!(
                tree.get(&k.to_le_bytes(), &storage).await?,
                Some(k.to_le_bytes().to_vec()),
                "present key {k} should resolve"
            );
        }

        // Every absent (odd) key in range, plus one above the maximum, returns
        // None. The odd keys land between stored keys and on the far side of
        // index boundaries; the above-max key exercises the last-child fallback.
        for k in (1..8000u32).step_by(2) {
            assert_eq!(
                tree.get(&k.to_le_bytes(), &storage).await?,
                None,
                "absent key {k} should not resolve"
            );
        }
        assert_eq!(tree.get(&100_000u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_restores_tree_from_persisted_root_hash() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        for i in 0..100u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let root = tree.root().clone();

        // Reconstruct from just the root hash — no shared delta or cache
        let restored = PersistentTree::<[u8; 4], Vec<u8>>::from_hash(root);

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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Insert and flush a base set
        for i in 0..10u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Insert more entries
        for i in 10..20u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Stream should include all inserted entries
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        // Build and flush
        for i in 0..20u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Delete some entries
        for i in (0..20u32).step_by(2) {
            tree = tree
                .edit()
                .delete(&i.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Stream should reflect the deletions
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

        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for &k in &all_keys {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Find boundary keys
        let boundaries: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&i| distribution::geometric::rank(&Blake3Hash::hash(&i.to_le_bytes())) > 1)
            .collect();

        for &bk in boundaries.iter().take(3) {
            let after_delete = tree
                .edit()
                .delete(&bk.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        for i in 0..500u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Delete all entries one at a time
        for i in 0..500u32 {
            tree = tree
                .edit()
                .delete(&i.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        for i in 0..50u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let root_before = tree.root().clone();

        // Re-insert key 25 with the same value
        tree = tree
            .edit()
            .insert(25u32.to_le_bytes(), vec![25u8], &storage)
            .await?
            .persist(&mut delta)?;

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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();

        // Insert keys whose byte and numeric orders differ.
        // 1u32 = [1,0,0,0], 256u32 = [0,1,0,0], 512u32 = [0,2,0,0]
        // Byte order: [0,1,0,0] < [0,2,0,0] < [1,0,0,0]
        // Numeric order: 1 < 256 < 512
        let numeric_keys: Vec<u32> = vec![1, 256, 512, 2, 257, 0];

        let mut delta = Delta::zero();
        for &k in &numeric_keys {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        for i in 0..20u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        for i in 0..20u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();

        for i in 0..20u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut tree_a = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_a = Delta::zero();
        for &k in &keys {
            tree_a = tree_a
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta_a)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta_a.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Build tree B: reverse insertion order
        let mut tree_b = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_b = Delta::zero();
        for &k in keys.iter().rev() {
            tree_b = tree_b
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta_b)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta_b.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut base = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta_base = Delta::zero();
        for i in 0..20u32 {
            base = base
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta_base)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta_base.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        // Create two branches
        let mut branch_a = base.clone();
        let mut branch_b = base.clone();
        let mut delta_a = Delta::zero();
        let mut delta_b = Delta::zero();

        // Interleave operations
        branch_a = branch_a
            .edit()
            .insert(100u32.to_le_bytes(), vec![100], &storage)
            .await?
            .persist(&mut delta_a)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_a.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        branch_b = branch_b
            .edit()
            .delete(&5u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta_b)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        branch_a = branch_a
            .edit()
            .insert(101u32.to_le_bytes(), vec![101], &storage)
            .await?
            .persist(&mut delta_a)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_a.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        branch_b = branch_b
            .edit()
            .delete(&10u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta_b)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta_b.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
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

        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for &k in &all_keys {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
            let after_delete = tree
                .edit()
                .delete(&bk.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }

            // Read every surviving key. Any missing new node in storage
            // surfaces here as a node-not-found error.
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

    /// Builds a tree by inserting the given u32 keys (little-endian encoded)
    /// in order and flushing the result to storage.
    async fn build_and_flush_u32(
        keys: &[u32],
        storage: &mut ContentAddressedStorage<
            MemoryStorageBackend<dialog_common::Blake3Hash, Vec<u8>>,
        >,
    ) -> Result<PersistentTree<[u8; 4], Vec<u8>>> {
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for &k in keys {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        Ok(tree)
    }

    /// Deleting a boundary whose removal collapses the root must produce the
    /// same tree as building the remaining keys from scratch.
    ///
    /// Regression guard: in this dataset the tree has exactly two segments
    /// under the root and 69161 is the boundary of the first one. Deleting
    /// it makes the right segment adopt the orphans and the root's two
    /// children fuse into one. `let_right_neighbor_adopt_orphans` used to
    /// return the fused node at whatever level the fold reached (here: a
    /// bare segment as the tree root, where a canonical tree always has an
    /// index root) instead of collapsing to the canonical height.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_when_boundary_delete_collapses_the_root() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let keys: Vec<u32> = vec![69161, 101527, 102790, 164389, 171478, 193283];

        let tree = build_and_flush_u32(&keys, &mut storage).await?;
        let mut delta = Delta::zero();
        let after = tree
            .edit()
            .delete(&69161u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let remaining: Vec<u32> = keys.iter().copied().filter(|&k| k != 69161).collect();
        let scratch = build_and_flush_u32(&remaining, &mut storage).await?;

        assert_eq!(
            after.root(),
            scratch.root(),
            "boundary delete that collapses the root should be canonical"
        );
        Ok(())
    }

    /// Deleting the sole entry of a segment must leave every other entry
    /// readable and produce the canonical tree.
    ///
    /// Regression guard: in this dataset 198936 is a boundary that forms a
    /// single-entry segment in a tree of height two, so deleting it walks a
    /// multi-layer search path through the empty-segment repair.
    /// An earlier per-operation shaper processed the empty-segment repair
    /// layers in root-to-leaf order while the merge consumed them
    /// leaf-to-root, so the rebuilt subtrees came out in the wrong key order:
    /// keys 10554, 83265 and 167706 landed to the right of larger keys and
    /// became unreachable through search.
    #[dialog_common::test]
    async fn it_keeps_entries_readable_after_a_delete_empties_a_segment() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let keys: Vec<u32> = vec![
            10554, 28619, 40390, 43764, 45237, 48124, 64082, 66285, 67399, 67838, 81131, 83265,
            92896, 94186, 98645, 103270, 110189, 114100, 123267, 127869, 135162, 136309, 147808,
            153518, 154310, 156529, 161523, 167706, 172145, 172489, 176828, 187970, 189253, 198936,
        ];

        let tree = build_and_flush_u32(&keys, &mut storage).await?;
        let mut delta = Delta::zero();
        let after = tree
            .edit()
            .delete(&198936u32.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let mut unreadable = vec![];
        for &k in keys.iter().filter(|&&k| k != 198936) {
            if after.get(&k.to_le_bytes(), &storage).await?.is_none() {
                unreadable.push(k);
            }
        }
        assert!(
            unreadable.is_empty(),
            "live keys unreadable after emptying a segment: {unreadable:?}"
        );

        let remaining: Vec<u32> = keys.iter().copied().filter(|&k| k != 198936).collect();
        let scratch = build_and_flush_u32(&remaining, &mut storage).await?;
        assert_eq!(
            after.root(),
            scratch.root(),
            "emptying a segment should produce the canonical tree"
        );
        Ok(())
    }

    /// A range with an unbounded start bound must stream every entry below
    /// the end bound. Regression guard: `TreeWalker::stream` used to return
    /// immediately on `Bound::Unbounded`, yielding nothing.
    #[dialog_common::test]
    async fn it_streams_ranges_with_unbounded_start() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let tree = build_and_flush_u32(&(0..10).collect::<Vec<_>>(), &mut storage).await?;

        let stream = tree.stream_range(..5u32.to_le_bytes(), &storage);
        futures_util::pin_mut!(stream);
        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        assert_eq!(count, 5, "..end range should yield entries below end");
        Ok(())
    }

    /// Streaming the whole tree must include an entry whose key is the
    /// maximum key. Regression guard: `Tree::stream` used to delegate to
    /// `stream_range(Key::min()..Key::max())`, whose exclusive end bound
    /// dropped the maximum key.
    #[dialog_common::test]
    async fn it_streams_the_maximum_key() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut tree = PersistentTree::<[u8; 4], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for i in 0..5u32 {
            tree = tree
                .edit()
                .insert(i.to_le_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        tree = tree
            .edit()
            .insert([0xFF; 4], vec![0xFF], &storage)
            .await?
            .persist(&mut delta)?;
        // Flush after each persist so the next edit can load the nodes this persist created.
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let stream = tree.stream(&storage);
        futures_util::pin_mut!(stream);
        let mut count = 0;
        while let Some(entry) = stream.next().await {
            entry?;
            count += 1;
        }
        assert_eq!(count, 6, "stream should include the maximum key");
        Ok(())
    }

    /// After every mutation, the tree must be byte-identical to a tree built
    /// from scratch over the same logical content. This is the invariant the
    /// whole crate is built around: same entries, same root hash, regardless
    /// of the operation history. Random insert/delete sequences over random
    /// key sets exercise every mutation path (in-place redistribution,
    /// orphan adoption across parents, empty-segment removal, cascade
    /// collapse, root chain stripping) without hand-picking datasets.
    #[dialog_common::test]
    async fn it_converges_to_canonical_form_after_every_operation() -> Result<()> {
        use std::collections::BTreeSet;

        use rand::{Rng, SeedableRng, rngs::StdRng};

        let mut rng = StdRng::seed_from_u64(0x_D1A1_06DB);
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for trial in 0..2 {
            // Seed the tree with a random key set.
            let mut live = BTreeSet::new();
            let initial_size = rng.gen_range(40..120);
            while live.len() < initial_size {
                live.insert(rng.gen_range(0..200_000u32));
            }
            let initial: Vec<u32> = live.iter().copied().collect();
            let mut tree = build_and_flush_u32(&initial, &mut storage).await?;
            let mut delta = Delta::zero();

            for op in 0..120 {
                // Half deletes of present keys, half inserts of fresh keys
                // (which occasionally hit a present key and become updates).
                if !live.is_empty() && rng.gen_bool(0.5) {
                    let index = rng.gen_range(0..live.len());
                    let key = *live.iter().nth(index).expect("index is in range");
                    tree = tree
                        .edit()
                        .delete(&key.to_le_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?;
                    live.remove(&key);
                } else {
                    let key = rng.gen_range(0..200_000u32);
                    tree = tree
                        .edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?;
                    live.insert(key);
                }

                // Flush periodically so both the delta-resident and the
                // storage-resident read paths get exercised.
                if op % 10 == 9 {
                    for (_, buffer) in delta.flush() {
                        storage
                            .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                            .await?;
                    }
                }

                let content: Vec<u32> = live.iter().copied().collect();
                let scratch = build_and_flush_u32(&content, &mut storage).await?;
                assert_eq!(
                    tree.root(),
                    scratch.root(),
                    "trial {trial}, operation {op}: tree diverged from the \
                     canonical form of its {} entries",
                    content.len(),
                );
            }
        }

        Ok(())
    }
}
