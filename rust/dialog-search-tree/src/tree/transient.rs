//! In-place batched tree edits.
//!
//! [`TransientTree`] opens a [`PersistentTree`]'s spine and applies a sequence
//! of inserts and deletes by mutating that live structure with a copy-on-write
//! descent. Each operation descends from the root to the target leaf, lifting
//! only the nodes on the path from [`Node::Persistent`] references to editable
//! [`Node::Transient`] form, applies the change to the leaf, then re-shapes the
//! touched path so the tree is canonical again. Untouched siblings stay shared
//! by reference. The shape rules are the same the per-operation
//! [`TreeShaper`](crate::TreeShaper) applies, so an edit batch and the
//! equivalent sequence of [`PersistentTree::insert`] / [`PersistentTree::delete`]
//! calls converge on the same root, byte for byte, after every operation.
//!
//! [`persist`](TransientTree::persist) is a pure bottom-up serializer: it makes
//! no shape decisions, because the shape was already established at edit time.

use crate::{
    Accessor, BOTTOM_RANK, Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError,
    Distribution, Entry, Geometric, Key, Node, PersistentNode, PersistentTree, Rank, SymmetryWith,
    TransientIndex, TransientNode, TransientSegment, TreeWalker, Value, regroup_children,
    regroup_entries,
};
use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use futures_util::StreamExt;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use std::{marker::PhantomData, ops::RangeBounds};

/// The root of a [`TransientTree`].
///
/// An unedited root is just that hash (possibly `NULL_BLAKE3_HASH` for an
/// empty tree), loaded into a live [`TransientNode`] only by the first edit
/// that descends into it.
enum TransientRoot<Key, Value> {
    /// The durable root hash, not yet loaded. `NULL_BLAKE3_HASH` is an empty
    /// tree. Persisting an unloaded root returns this hash verbatim, touching
    /// no storage.
    Unloaded(Blake3Hash),
    /// The root loaded and being edited this batch.
    Loaded(TransientNode<Key, Value>),
}

/// A batch of in-place edits over a tree's [`Node`] spine.
///
/// The edit holds no storage handle: like [`PersistentTree`], every method that
/// may read from storage takes the [`ContentAddressedStorage`] as a parameter.
/// The edit retains only the in-memory transient spine, the node cache, and the
/// accumulating delta.
pub struct TransientTree<Key, Value, D = Geometric>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
    D: Distribution,
{
    /// The root, mirroring [`PersistentTree`]'s `root: Blake3Hash`: it starts as
    /// the same (possibly null) hash and is loaded lazily into a transient node
    /// only by the first edit that descends into it, so opening neither awaits
    /// nor touches storage.
    root: TransientRoot<Key, Value>,
    cache: Cache<Blake3Hash, Buffer>,
    delta: Delta<Blake3Hash, Buffer>,
    distribution: PhantomData<D>,
}

impl<Key, Value, D> TransientTree<Key, Value, D>
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
    D: Distribution,
{
    /// Creates an edit batch over the tree rooted at `root`, deferring the root
    /// load.
    ///
    /// The root is held as its (possibly null) hash and loaded lazily by the
    /// first edit that descends into it, so this is synchronous and touches no
    /// storage.
    pub fn new(
        root: Blake3Hash,
        cache: Cache<Blake3Hash, Buffer>,
        delta: Delta<Blake3Hash, Buffer>,
    ) -> Self {
        Self {
            root: TransientRoot::Unloaded(root),
            cache,
            delta,
            distribution: PhantomData,
        }
    }

    /// Loads the root into a transient node, returning `None` for an empty tree
    /// (a null root hash, which cannot be loaded).
    async fn load<Backend>(
        root: TransientRoot<Key, Value>,
        accessor: &Accessor<Backend>,
    ) -> Result<Option<TransientNode<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        match root {
            TransientRoot::Loaded(node) => Ok(Some(node)),
            TransientRoot::Unloaded(hash) if &hash == NULL_BLAKE3_HASH => Ok(None),
            TransientRoot::Unloaded(hash) => {
                let node: PersistentNode<Key, Value> = accessor.get_node(&hash).await?;
                Ok(Some(TransientNode::try_from(&node)?))
            }
        }
    }

    /// Inserts a key/value pair, mutating the transient tree in place.
    pub async fn insert<Backend>(
        mut self,
        key: Key,
        value: Value,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        let entry = Entry { key, value };
        let accessor = Accessor::new(self.delta.clone(), self.cache.clone(), storage.clone());

        let node = match Self::load(self.root, &accessor).await? {
            // The first entry of an empty tree becomes a lone segment wrapped in
            // a single-child index, matching the canonical root invariant that
            // the root is always an index.
            None => TransientNode::Index(TransientIndex {
                children: vec![Node::Transient(TransientNode::Segment(TransientSegment {
                    entries: vec![entry],
                }))],
            }),
            Some(root) => Edit::Upsert(entry)
                .apply::<Backend, D>(root, &accessor)
                .await?
                .expect("an insert never empties the tree"),
        };
        self.root = TransientRoot::Loaded(node);
        Ok(self)
    }

    /// Deletes a key, mutating the transient tree in place. A missing key is a
    /// no-op.
    pub async fn delete<Backend>(
        mut self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        let accessor = Accessor::new(self.delta.clone(), self.cache.clone(), storage.clone());

        let Some(root) = Self::load(self.root, &accessor).await? else {
            // Deleting from an empty tree is a no-op; leave it empty.
            self.root = TransientRoot::Unloaded(NULL_BLAKE3_HASH.clone());
            return Ok(self);
        };
        let edited = Edit::Delete(key.clone())
            .apply::<Backend, D>(root, &accessor)
            .await?;
        self.root = match edited {
            Some(node) => TransientRoot::Loaded(node),
            // The delete emptied the tree.
            None => TransientRoot::Unloaded(NULL_BLAKE3_HASH.clone()),
        };
        Ok(self)
    }

    /// Retrieves the value associated with `key` from the in-flight transient
    /// tree, reading exactly what [`persist`](Self::persist) would produce.
    ///
    /// Untouched subtrees are still [`Node::Persistent`] references and are
    /// fully persistent: a point lookup into one delegates to the same read
    /// path [`PersistentTree::get`] uses. Only the edited
    /// [`Node::Transient`] spine is descended in memory.
    pub async fn get<Backend>(
        &self,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Option<Value>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        let mut node = match &self.root {
            TransientRoot::Unloaded(hash) => {
                return self.persistent_get(hash, key, storage).await;
            }
            TransientRoot::Loaded(node) => node,
        };

        loop {
            match node {
                TransientNode::Index(index) => {
                    let at = child_for::<Key, Value>(&index.children, key);
                    match &index.children[at] {
                        Node::Persistent(link) => {
                            return self.persistent_get(&link.node, key, storage).await;
                        }
                        Node::Transient(child) => node = child,
                    }
                }
                TransientNode::Segment(segment) => {
                    return match segment.entries.binary_search_by(|entry| entry.key.cmp(key)) {
                        Ok(at) => Ok(Some(segment.entries[at].value.clone())),
                        Err(_) => Ok(None),
                    };
                }
            }
        }
    }

    /// Delegates a point lookup over a fully persistent subtree rooted at
    /// `hash` to [`PersistentTree::get`], so the transient read of an untouched
    /// subtree is byte-for-byte the persistent read.
    async fn persistent_get<Backend>(
        &self,
        hash: &Blake3Hash,
        key: &Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Option<Value>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        let subtree: PersistentTree<Key, Value, D> =
            PersistentTree::seal(hash.clone(), self.cache.clone(), self.delta.clone());
        subtree.get(key, storage).await
    }

    /// Streams the entries of the in-flight transient tree whose keys fall in
    /// `range`, in ascending key order, reading exactly what
    /// [`persist`](Self::persist) would produce.
    ///
    /// Untouched subtrees are still [`Node::Persistent`] references and are
    /// fully persistent: each streams through the same [`TreeWalker`] path
    /// [`PersistentTree::stream_range`] uses. Only the edited
    /// [`Node::Transient`] spine is traversed in memory.
    pub fn stream_range<R, Backend>(
        &self,
        range: R,
        storage: &ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        R: RangeBounds<Key> + ConditionalSend,
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        // The transient spine borrows `self`, but the returned stream must own
        // everything it touches. Snapshot the spine into an owned plan of steps
        // (persistent subtree hashes to stream, transient leaf entries to
        // yield), in ascending key order, before building the stream.
        // Persistent subtrees stay as hashes, so the snapshot copies no
        // untouched node.
        let cache = self.cache.clone();
        let delta = self.delta.clone();
        let storage = storage.clone();

        // Snapshot the range bounds to owned, cloneable form: the walker
        // consumes a range per persistent subtree, and the transient leaves are
        // filtered with the same bounds, so a borrow of the caller's range
        // cannot outlive this method.
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());

        let plan = match &self.root {
            TransientRoot::Unloaded(hash) => vec![StreamStep::Persistent(hash.clone())],
            TransientRoot::Loaded(node) => {
                let mut plan = Vec::new();
                collect_stream_plan(node, &mut plan);
                plan
            }
        };

        try_stream! {
            for step in plan {
                match step {
                    StreamStep::Persistent(hash) => {
                        let accessor =
                            Accessor::new(delta.clone(), cache.clone(), storage.clone());
                        let inner = TreeWalker::<Key, Value>::new(hash)
                            .stream(bounds.clone(), accessor);
                        futures_util::pin_mut!(inner);
                        while let Some(entry) = inner.next().await {
                            yield entry?;
                        }
                    }
                    StreamStep::Entry(entry) => {
                        if bounds.contains(&entry.key) {
                            yield entry;
                        }
                    }
                }
            }
        }
    }

    /// Serializes the edited tree bottom-up and returns it as a
    /// [`PersistentTree`], carrying the node cache and the accumulated delta
    /// forward. The root is empty (`NULL_BLAKE3_HASH`) when the batch left the
    /// tree empty.
    pub fn persist(mut self) -> Result<PersistentTree<Key, Value, D>, DialogSearchTreeError> {
        let root = match self.root {
            // An untouched root (including an empty tree's null hash) was never
            // loaded; its hash is already durable and is returned verbatim,
            // touching no storage.
            TransientRoot::Unloaded(hash) => hash,
            TransientRoot::Loaded(transient) => transient.persist(&mut self.delta)?.hash().clone(),
        };

        Ok(PersistentTree::seal(root, self.cache, self.delta))
    }
}

/// A single keyed edit applied to a [`TransientTree`].
enum Edit<Key, Value> {
    /// Insert the entry, or update the value if the key already exists.
    Upsert(Entry<Key, Value>),
    /// Remove the key if present.
    Delete(Key),
}

impl<Key, Value> Edit<Key, Value> {
    /// The key this edit targets, borrowed from the edit itself so the descent
    /// can route to its leaf without cloning a separate routing key.
    fn key(&self) -> &Key {
        match self {
            Edit::Upsert(entry) => &entry.key,
            Edit::Delete(key) => key,
        }
    }
}

impl<Key, Value> Edit<Key, Value>
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
    /// Applies this edit to the tree rooted at `root`, re-shaping it in place so
    /// the result is canonical. Returns `None` when a delete empties the tree.
    ///
    /// Done in two phases. First a copy-on-write descent (async) lifts every
    /// node on the path from the root to the target leaf to transient form,
    /// recording the child index taken at each level, and (for a boundary
    /// delete) lifts the left spine of the right-adjacent subtree so its
    /// leftmost leaf can fuse with the orphaned entries. Then a fully
    /// synchronous re-shape applies the edit to the leaf and re-groups the
    /// touched path bottom-up. Splitting the work this way keeps the synchronous
    /// re-shape free of borrows spanning awaits.
    async fn apply<Backend, D>(
        self,
        mut root: TransientNode<Key, Value>,
        accessor: &Accessor<Backend>,
    ) -> Result<Option<TransientNode<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        D: Distribution,
    {
        // Phase one: lift the path to the target leaf, recording the child index
        // chosen at each level. The routing key is borrowed from this edit, so
        // the descent clones no separate key.
        let key = self.key();
        let mut path = Vec::new();
        loop {
            let node = follow(&mut root, &path)?;
            match node {
                TransientNode::Index(index) => {
                    let at = child_for::<Key, Value>(&index.children, key);
                    lift(&mut index.children[at], accessor).await?;
                    path.push(at);
                }
                TransientNode::Segment(_) => break,
            }
        }

        let height = path.len() as Rank;

        // A boundary delete removes the segment's terminating boundary, so the
        // orphaned entries must fuse with the leftmost leaf of the right-adjacent
        // subtree. Detect it before anything else: a boundary key is always the
        // segment's last entry (it may be absent, a no-op delete, so confirm by
        // matching the last key). This case is NOT eligible for the fast path,
        // because after removal the segment looks locally canonical (an open run)
        // yet must still fuse rightward, which the local check cannot see.
        let is_boundary_delete = match (&self, follow(&mut root, &path)?) {
            (Edit::Delete(_), TransientNode::Segment(segment)) => segment
                .entries
                .last()
                .map(|e| &e.key == key)
                .unwrap_or(false),
            _ => false,
        };

        // Fast path (only when this is not a fusing boundary delete): if the edit
        // provably leaves the leaf canonical, apply it in place and return. A
        // canonical leaf is a run with no interior boundary (its boundary, if any,
        // is the last entry). The check is done WITHOUT mutating or cloning, so the
        // common case costs only a binary search plus the in-place Vec edit.
        //
        //   - Insert of a non-boundary key is canonical unless it lands after the
        //     leaf's terminating boundary (appended past a boundary last entry),
        //     which would leave that boundary interior. An inserted boundary key
        //     always splits, so it is never fast.
        //   - Delete of a non-boundary key never creates an interior boundary, so
        //     it is canonical unless it empties the leaf (which must remove the
        //     segment from its parent).
        //
        // Anything not provably canonical falls through to the re-shaping paths.
        if !is_boundary_delete {
            let TransientNode::Segment(segment) = follow(&mut root, &path)? else {
                return Err(DialogSearchTreeError::Node(
                    "Path did not reach a segment".into(),
                ));
            };
            if fast_path_keeps_canonical::<Key, Value, D>(&segment.entries, &self) {
                apply_to_segment(&mut segment.entries, self);
                return Ok(Some(root));
            }
        }

        let neighbor_path = if is_boundary_delete {
            lift_right_neighbor_spine(&mut root, &path, accessor).await?
        } else {
            None
        };

        // Phase two: synchronous re-shape. The whole touched region is transient, so
        // the re-shape needs no further loads and runs without any borrow spanning
        // an await.
        let replacement = match &neighbor_path {
            Some(neighbor_path) => {
                // A boundary delete: the LCA is the deepest level where the main
                // and neighbor paths diverge. Re-shape the shared prefix down to
                // the LCA, where the two child subtrees fuse. `self` is not moved
                // on this arm, so the boundary key is borrowed from it directly.
                let lca_depth = path
                    .iter()
                    .zip(neighbor_path.iter())
                    .position(|(a, b)| a != b)
                    .ok_or_else(|| {
                        DialogSearchTreeError::Node(
                            "Boundary delete had no diverging neighbor path".into(),
                        )
                    })?;
                reshape_fused::<Key, Value, D>(
                    &mut root,
                    &path,
                    neighbor_path,
                    lca_depth,
                    self.key(),
                    height,
                )?
            }
            None => reshape_path::<Key, Value, D>(&mut root, &path, self, height)?,
        };
        seal_root::<Key, Value, D>(replacement, height)
    }
}

/// Walks `root` down the recorded child indices in `path`, lifting the node at
/// the end of the path to transient form, and returns a mutable reference to it.
///
/// Each index in `path` was produced by a prior descent step, so every node
/// along the way is already transient. Re-walking from the root each step (the
/// path is at most the tree height) avoids holding a borrow across the lift's
/// await.
fn follow<'a, Key, Value>(
    root: &'a mut TransientNode<Key, Value>,
    path: &[usize],
) -> Result<&'a mut TransientNode<Key, Value>, DialogSearchTreeError> {
    let mut node = root;
    for &at in path {
        match node {
            TransientNode::Index(index) => match &mut index.children[at] {
                Node::Transient(child) => node = child,
                Node::Persistent(_) => {
                    return Err(DialogSearchTreeError::Node(
                        "Path descended into a node that was not lifted".into(),
                    ));
                }
            },
            TransientNode::Segment(_) => {
                return Err(DialogSearchTreeError::Node(
                    "Path descended through a segment".into(),
                ));
            }
        }
    }
    Ok(node)
}

/// Ensures `node` is transient, loading and opening it from storage if it is
/// still a persistent reference.
async fn lift<Key, Value, Backend>(
    node: &mut Node<Key, Value>,
    accessor: &Accessor<Backend>,
) -> Result<(), DialogSearchTreeError>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    if let Node::Persistent(link) = node {
        let persistent = accessor.get_node(&link.node).await?;
        *node = TransientNode::try_from(&persistent)?.into();
    }
    Ok(())
}

/// Lifts the left spine of the subtree immediately to the right of `path`, so a
/// boundary delete at the end of the leaf reached by `path` can fuse the
/// orphaned entries with that neighbor's leftmost leaf during the re-shape.
///
/// The neighbor is found by climbing `path` to the deepest ancestor that still
/// has a child after the one `path` descended into (the lowest common ancestor
/// of the two leaves), then walking that next child's leftmost edge down to its
/// leaf, lifting each node on the way. Returns the path to the neighbor's
/// leftmost leaf, or `None` when `path` already reaches the rightmost leaf and
/// there is no neighbor to fuse with.
async fn lift_right_neighbor_spine<Key, Value, Backend>(
    root: &mut TransientNode<Key, Value>,
    path: &[usize],
    accessor: &Accessor<Backend>,
) -> Result<Option<Vec<usize>>, DialogSearchTreeError>
where
    Key: self::Key
        + ConditionalSync
        + 'static
        + PartialOrd<Key::Archived>
        + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + ConditionalSync
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    // Find the deepest ancestor with a right sibling of the descended child, and
    // build the path to that sibling: the ancestor prefix, then the next index.
    let mut neighbor_path: Option<Vec<usize>> = None;
    for depth in (0..path.len()).rev() {
        let ancestor = follow(root, &path[..depth])?;
        if let TransientNode::Index(index) = ancestor
            && path[depth] + 1 < index.children.len()
        {
            let mut prefix = path[..depth].to_vec();
            prefix.push(path[depth] + 1);
            neighbor_path = Some(prefix);
            break;
        }
    }

    let Some(mut neighbor_path) = neighbor_path else {
        return Ok(None);
    };

    // Walk the neighbor subtree's leftmost edge to its leaf, lifting each node so
    // the whole spine is transient.
    loop {
        let node = follow(root, &neighbor_path)?;
        match node {
            TransientNode::Index(index) => {
                lift(&mut index.children[0], accessor).await?;
                neighbor_path.push(0);
            }
            TransientNode::Segment(_) => break,
        }
    }

    Ok(Some(neighbor_path))
}

/// Re-shapes the path from `node` down to the target leaf after applying `edit`,
/// returning the canonical run of nodes that should replace `node` in its
/// parent.
///
/// This recurses to the leaf, applies the edit to its entries, re-cuts the leaf
/// into segments by rank, then on the way back up splices each rebuilt run into
/// its parent's child list and re-cuts that list at the parent's level. An empty
/// run propagates a removal (an emptied segment, or an index left childless),
/// matching the sequential shaper's `remove_from_path`.
///
/// `height` is the height of `node` (0 for the leaf). A node at height `h`
/// groups its height-`h - 1` children with the level-`h` threshold.
fn reshape_path<Key, Value, D>(
    node: &mut TransientNode<Key, Value>,
    path: &[usize],
    edit: Edit<Key, Value>,
    height: Rank,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    match path.split_first() {
        None => {
            // The leaf: apply the edit and re-cut its entries into segments.
            let TransientNode::Segment(segment) = node else {
                return Err(DialogSearchTreeError::Node(
                    "Reshape reached an index where a leaf was expected".into(),
                ));
            };
            apply_to_segment(&mut segment.entries, edit);
            Ok(regroup_entries::<Key, Value, D>(std::mem::take(
                &mut segment.entries,
            )))
        }
        Some((&at, rest)) => {
            let child = node.child_mut(at)?;
            let replacement = reshape_path::<Key, Value, D>(child, rest, edit, height - 1)?;
            splice_and_regroup::<Key, Value, D>(
                &mut node.as_index_mut()?.children,
                at..at + 1,
                replacement,
                height,
            )
        }
    }
}

/// Re-shapes the shared prefix of a boundary delete down to the lowest common
/// ancestor (LCA) of the modified leaf and its right neighbour, fusing the two
/// child subtrees at the LCA.
///
/// Above the LCA the re-shape is identical to [`reshape_path`]: recurse, splice,
/// re-cut. At the LCA the children `path[lca_depth]` (the main subtree) and
/// `path[lca_depth] + 1` (the neighbour subtree) are fused by [`fuse_subtrees`]
/// into one canonical run that replaces both, then the LCA's child list is
/// re-cut. The returned run replaces `node` in its parent.
fn reshape_fused<Key, Value, D>(
    node: &mut TransientNode<Key, Value>,
    path: &[usize],
    neighbor_path: &[usize],
    lca_depth: usize,
    key: &Key,
    height: Rank,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    let at = path[0];
    if lca_depth == 0 {
        // We have reached the LCA: fuse the main child subtree (at `at`) with the
        // neighbour child subtree (at `at + 1`).
        let children = &mut node.as_index_mut()?.children;
        let main = take_transient(children, at)?;
        // After removing the main child the neighbour shifted left into `at`.
        let neighbor = take_transient(children, at)?;
        let fused = fuse_subtrees::<Key, Value, D>(main, neighbor, key, height - 1)?;
        return splice_and_regroup::<Key, Value, D>(children, at..at, fused, height);
    }

    // Above the LCA: recurse through the shared prefix, then splice and re-cut.
    let child = node.child_mut(at)?;
    let replacement = reshape_fused::<Key, Value, D>(
        child,
        &path[1..],
        &neighbor_path[1..],
        lca_depth - 1,
        key,
        height - 1,
    )?;
    splice_and_regroup::<Key, Value, D>(
        &mut node.as_index_mut()?.children,
        at..at + 1,
        replacement,
        height,
    )
}

/// Fuses the main subtree (whose rightmost leaf lost its boundary) with the
/// neighbour subtree (the right-adjacent one), returning the canonical run of
/// nodes that replaces both at their shared parent.
///
/// Both subtrees are equal height. The main subtree is the rightmost descendant
/// below the LCA, so on its spine every node is the last child and there are no
/// right siblings; the neighbour is the leftmost descendant, so on its spine
/// every node is the first child and there are no left siblings. Recursing down
/// both spines in lock-step, the boundary that the delete dissolved is fused at
/// the leaf (the orphaned entries concatenated with the neighbour's leftmost
/// leaf), then each level above folds the main spine's left siblings, the fused
/// run, and the neighbour spine's right siblings into one run re-cut at that
/// level. Mirrors the level-by-level fold in the shaper's
/// `let_right_neighbor_adopt_orphans`.
fn fuse_subtrees<Key, Value, D>(
    main: TransientNode<Key, Value>,
    neighbor: TransientNode<Key, Value>,
    key: &Key,
    height: Rank,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    match (main, neighbor) {
        (TransientNode::Segment(mut main), TransientNode::Segment(neighbor)) => {
            // The leaf level: drop the dissolved boundary key (the main leaf's
            // last entry), then concatenate the orphans with the neighbour's
            // entries and re-cut into segments.
            if main.entries.last().map(|e| &e.key == key).unwrap_or(false) {
                main.entries.pop();
            }
            let mut entries = main.entries;
            entries.extend(neighbor.entries);
            Ok(regroup_entries::<Key, Value, D>(entries))
        }
        (TransientNode::Index(mut main), TransientNode::Index(mut neighbor)) => {
            // The main spine descends through the last child; the neighbour
            // spine through the first. Fuse those, then splice the fused run
            // between the main's left siblings and the neighbour's right
            // siblings and re-cut at this level.
            let main_last = main.children.pop().ok_or_else(|| {
                DialogSearchTreeError::Node("Fused main index had no children".into())
            })?;
            let main_last = main_last.into_transient()?;
            let neighbor_first = remove_first(&mut neighbor.children)?;
            let neighbor_first = neighbor_first.into_transient()?;

            let fused = fuse_subtrees::<Key, Value, D>(main_last, neighbor_first, key, height - 1)?;

            let mut combined = main.children;
            combined.extend(fused);
            combined.extend(neighbor.children);
            regroup_children::<Key, Value, D>(combined, height)
        }
        _ => Err(DialogSearchTreeError::Node(
            "Fused subtrees had mismatched heights".into(),
        )),
    }
}

/// Replaces `children[range]` with `replacement` and re-cuts the resulting child
/// list into index nodes at `height`. Returns the run of index nodes (one or
/// more), or an empty run when the splice left no children (a removal that must
/// propagate one level up).
fn splice_and_regroup<Key, Value, D>(
    children: &mut Vec<Node<Key, Value>>,
    range: std::ops::Range<usize>,
    replacement: Vec<Node<Key, Value>>,
    height: Rank,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    children.splice(range, replacement);
    if children.is_empty() {
        return Ok(vec![]);
    }
    regroup_children::<Key, Value, D>(std::mem::take(children), height)
}

/// Turns the root's replacement run (the nodes that stand for the old root after
/// the re-shape) into a single canonical index root, or `None` when the tree was
/// emptied.
///
/// The canonical root is always an index, and never a single-child index whose
/// only child is another index (such chains arise when a delete dissolves the
/// boundary that demanded the upper levels). The run is wrapped into a parent
/// while it holds more than one node, then any single-child index-over-index
/// wrapper is stripped, leaving the multi-child index, or the lone index over a
/// single segment.
fn seal_root<Key, Value, D>(
    mut replacement: Vec<Node<Key, Value>>,
    height: Rank,
) -> Result<Option<TransientNode<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    if replacement.is_empty() {
        return Ok(None);
    }

    // Group the run into a parent level by rank until a single node remains,
    // raising the threshold one level each pass (the same loop the sequential
    // builder runs at the top). The canonical root is always an index, so a lone
    // surviving segment is wrapped once too.
    let mut level = height + 1;
    while replacement.len() > 1
        || matches!(
            replacement.first(),
            Some(Node::Transient(TransientNode::Segment(_)))
        )
    {
        replacement = regroup_children::<Key, Value, D>(replacement, level)?;
        level += 1;
    }

    let mut root = match replacement.pop() {
        Some(Node::Transient(transient)) => transient,
        // A lone persistent index already is the canonical root for this level;
        // re-open it so the root stays transient. This is unreachable in
        // practice because the touched root is always rebuilt into a transient
        // node, but it keeps the function total.
        Some(Node::Persistent(link)) => {
            return Ok(Some(TransientNode::Index(TransientIndex {
                children: vec![Node::Persistent(link)],
            })));
        }
        None => return Ok(None),
    };

    // Strip a non-canonical chain of single-child index nodes over indices.
    loop {
        let TransientNode::Index(index) = &mut root else {
            break;
        };
        if index.children.len() != 1 {
            break;
        }
        match &index.children[0] {
            Node::Transient(TransientNode::Index(_)) => {
                let child = index.children.pop().expect("single child present");
                let Node::Transient(child) = child else {
                    unreachable!("matched transient index above");
                };
                root = child;
            }
            _ => break,
        }
    }

    Ok(Some(root))
}

/// Removes the child at `at` from `children` and unwraps it to its transient
/// form. The child is on the lifted path, so it is always transient.
fn take_transient<Key, Value>(
    children: &mut Vec<Node<Key, Value>>,
    at: usize,
) -> Result<TransientNode<Key, Value>, DialogSearchTreeError> {
    if at >= children.len() {
        return Err(DialogSearchTreeError::Node(
            "Re-shape child index out of range".into(),
        ));
    }
    children.remove(at).into_transient()
}

/// Removes and returns the first child of a list, erroring on an empty list.
fn remove_first<Key, Value>(
    children: &mut Vec<Node<Key, Value>>,
) -> Result<Node<Key, Value>, DialogSearchTreeError> {
    if children.is_empty() {
        return Err(DialogSearchTreeError::Node(
            "Fused neighbour index had no children".into(),
        ));
    }
    Ok(children.remove(0))
}

/// Whether applying `edit` to this already-canonical segment would leave it
/// canonical, decided without mutating or cloning.
///
/// A canonical leaf has no interior boundary: its terminating boundary, if any,
/// is the last entry. The input segment is canonical (an invariant maintained
/// by every edit), so only the local effect of `edit` needs checking:
///
///   - Upsert of a key that already exists only replaces a value: shape-neutral.
///   - Upsert of a boundary key (rank > leaf threshold) always splits: not fast.
///   - Upsert of a non-boundary key is fine unless it is appended after the
///     segment's terminating boundary, leaving that boundary interior. That
///     happens only when the key sorts last and the current last entry is a
///     boundary.
///   - Remove of a present key is fine unless it empties the segment (which must
///     remove the segment from its parent). Removing any non-last entry, or the
///     last entry when it is non-boundary, keeps the leaf canonical; removing a
///     boundary last entry is handled earlier as a fusing boundary delete.
fn fast_path_keeps_canonical<Key, Value, D>(
    entries: &[Entry<Key, Value>],
    edit: &Edit<Key, Value>,
) -> bool
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    D: Distribution,
{
    match edit {
        Edit::Upsert(entry) => {
            let found = entries.binary_search_by(|e| e.key.cmp(&entry.key));
            if found.is_ok() {
                return true; // value update only, shape unchanged
            }
            if D::rank(entry.key.as_ref()) > BOTTOM_RANK {
                return false; // inserting a boundary splits the segment
            }
            let at = found.unwrap_err();
            // Appending after a boundary last entry would leave it interior.
            let appends_last = at == entries.len();
            let last_is_boundary = entries
                .last()
                .map(|e| D::rank(e.key.as_ref()) > BOTTOM_RANK)
                .unwrap_or(false);
            !(appends_last && last_is_boundary)
        }
        Edit::Delete(key) => match entries.binary_search_by(|e| e.key.cmp(key)) {
            // Removing the only entry empties the segment: not fast.
            Ok(_) => entries.len() > 1,
            // Key absent: no-op, trivially canonical.
            Err(_) => true,
        },
    }
}

/// Applies one edit to a sorted segment in place.
fn apply_to_segment<Key, Value>(entries: &mut Vec<Entry<Key, Value>>, edit: Edit<Key, Value>)
where
    Key: Ord,
{
    match edit {
        Edit::Upsert(entry) => match entries.binary_search_by(|e| e.key.cmp(&entry.key)) {
            Ok(at) => entries[at].value = entry.value,
            Err(at) => entries.insert(at, entry),
        },
        Edit::Delete(key) => {
            if let Ok(at) = entries.binary_search_by(|e| e.key.cmp(&key)) {
                entries.remove(at);
            }
        }
    }
}

/// Index of the child whose subtree covers `key`: the first child whose upper
/// bound is `>= key`, or the last child when the key exceeds every bound.
fn child_for<Key, Value>(children: &[Node<Key, Value>], key: &Key) -> usize
where
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    let mut at = 0usize;
    while at + 1 < children.len() {
        match children[at].upper_bound_ref() {
            Ok(bound) if bound < key => at += 1,
            _ => break,
        }
    }
    at
}

/// One step of an ascending traversal plan over a transient spine, captured so
/// the streamed read owns everything it touches.
///
/// A [`StreamStep::Persistent`] is an untouched, fully persistent subtree to
/// stream by hash; a [`StreamStep::Entry`] is an owned entry from an edited
/// (transient) leaf to yield directly. The plan lists these left to right, so
/// concatenating their outputs yields entries in ascending key order.
enum StreamStep<Key, Value> {
    /// A persistent subtree to stream by its root hash.
    Persistent(Blake3Hash),
    /// An owned entry from a transient leaf.
    Entry(Entry<Key, Value>),
}

/// Walks the transient `node` left to right, appending each persistent subtree
/// (as a hash) and each transient leaf entry (cloned) to `plan` in ascending
/// key order.
fn collect_stream_plan<Key, Value>(
    node: &TransientNode<Key, Value>,
    plan: &mut Vec<StreamStep<Key, Value>>,
) where
    Key: Clone,
    Value: Clone,
{
    match node {
        TransientNode::Index(index) => {
            for child in &index.children {
                match child {
                    Node::Persistent(link) => {
                        plan.push(StreamStep::Persistent(link.node.clone()));
                    }
                    Node::Transient(child) => collect_stream_plan(child, plan),
                }
            }
        }
        TransientNode::Segment(segment) => {
            for entry in &segment.entries {
                plan.push(StreamStep::Entry(entry.clone()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use dialog_storage::MemoryStorageBackend;

    use crate::{ContentAddressedStorage, PersistentTree};

    type TestTree = PersistentTree<[u8; 4], Vec<u8>>;
    type TestStorage = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Build a tree by inserting `keys` one at a time with [`Tree::insert`],
    /// then flush it to storage.
    async fn sequential(keys: &[u32], storage: &mut TestStorage) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        for &k in keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), storage)
                .await?;
        }
        for buffer in tree.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(tree)
    }

    #[dialog_common::test]
    async fn it_matches_sequential_inserts() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let keys: Vec<u32> = (0..500).collect();
        let expected = sequential(&keys, &mut storage).await?;

        let mut edit = TestTree::empty().edit();
        for &k in &keys {
            edit = edit
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        let tree = edit.persist()?;

        assert_eq!(
            tree.root(),
            expected.root(),
            "batched inserts must match sequential inserts"
        );
        Ok(())
    }

    /// A tiny deterministic xorshift PRNG so the property tests are reproducible
    /// without pulling in seeded-`rand` plumbing.
    struct Rng(u64);

    impl Rng {
        fn new(seed: u64) -> Self {
            Rng(seed ^ 0x9E3779B97F4A7C15)
        }

        fn next_u32(&mut self) -> u32 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            (x >> 32) as u32
        }

        /// Fisher-Yates shuffle.
        fn shuffle<T>(&mut self, items: &mut [T]) {
            for i in (1..items.len()).rev() {
                let j = (self.next_u32() as usize) % (i + 1);
                items.swap(i, j);
            }
        }
    }

    /// Applies `keys` to a fresh [`Edit`] in order, then persists and returns the
    /// root hash.
    async fn batched(keys: &[u32], storage: &TestStorage) -> Result<Blake3Hash> {
        let mut edit = TestTree::empty().edit();
        for &k in keys {
            edit = edit
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), storage)
                .await?;
        }
        let tree = edit.persist()?;
        Ok(tree.root().clone())
    }

    #[dialog_common::test]
    async fn it_matches_sequential_for_random_insert_order() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..200u64 {
            let mut keys: Vec<u32> = (0..300).collect();
            Rng::new(seed).shuffle(&mut keys);

            let expected = sequential(&keys, &mut storage).await?;
            let root = batched(&keys, &storage).await?;

            assert_eq!(
                &root,
                expected.root(),
                "seed {seed}: batched inserts in random order must match sequential"
            );
        }
        Ok(())
    }

    /// Build a tree from `keys` with [`Tree::insert`], delete `to_delete` with
    /// [`Tree::delete`], flush, and return the resulting tree.
    async fn sequential_with_deletes(
        keys: &[u32],
        to_delete: &[u32],
        storage: &mut TestStorage,
    ) -> Result<TestTree> {
        let mut tree = sequential(keys, storage).await?;
        for &k in to_delete {
            tree = tree.delete(&k.to_le_bytes(), storage).await?;
        }
        for buffer in tree.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(tree)
    }

    /// Every single-key boundary delete from a 0..N tree must match the shaper.
    /// This covers boundary keys at all levels, the case that needed the
    /// right-neighbor entry fusion.
    #[dialog_common::test]
    async fn it_matches_sequential_for_every_single_delete() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let keys: Vec<u32> = (0..200).collect();

        for &victim in &keys {
            let expected = sequential_with_deletes(&keys, &[victim], &mut storage).await?;
            let mut edit = TestTree::empty().edit();
            for &k in &keys {
                edit = edit
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            edit = edit.delete(&victim.to_le_bytes(), &storage).await?;
            let tree = edit.persist()?;
            assert_eq!(
                tree.root(),
                expected.root(),
                "deleting single key {victim} must match the shaper"
            );
        }
        Ok(())
    }

    /// Batched deletes (50 random of 300) must match the sequential shaper.
    #[dialog_common::test]
    async fn it_matches_sequential_with_deletes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..200u64 {
            let keys: Vec<u32> = (0..300).collect();
            let mut to_delete: Vec<u32> = (0..300).collect();
            let mut rng = Rng::new(seed);
            rng.shuffle(&mut to_delete);
            to_delete.truncate(50);

            let expected = sequential_with_deletes(&keys, &to_delete, &mut storage).await?;

            let mut edit = TestTree::empty().edit();
            for &k in &keys {
                edit = edit
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            for &k in &to_delete {
                edit = edit.delete(&k.to_le_bytes(), &storage).await?;
            }
            let tree = edit.persist()?;

            assert_eq!(
                tree.root(),
                expected.root(),
                "seed {seed}: batched delete must match sequential delete"
            );
        }
        Ok(())
    }

    /// A random interleaving of inserts and deletes in one batch must match the
    /// same operations applied one at a time through [`Tree`]. This is the
    /// strongest oracle: it exercises seams created and dissolved repeatedly
    /// within a single edit.
    #[dialog_common::test]
    async fn it_matches_sequential_for_random_interleaved_ops() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..200u64 {
            let mut rng = Rng::new(seed);

            // Build a randomized op stream over a small key domain so inserts
            // and deletes collide on the same keys, churning boundaries.
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..400 {
                let is_insert = !(rng.next_u32()).is_multiple_of(3); // ~2/3 inserts
                let key = rng.next_u32() % 150;
                ops.push((is_insert, key));
            }

            // Sequential reference through Tree.
            let mut tree = TestTree::empty();
            for &(is_insert, key) in &ops {
                tree = if is_insert {
                    tree.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    tree.delete(&key.to_le_bytes(), &storage).await?
                };
            }
            for buffer in tree.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }

            // Batched through Edit.
            let mut edit = TestTree::empty().edit();
            for &(is_insert, key) in &ops {
                edit = if is_insert {
                    edit.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    edit.delete(&key.to_le_bytes(), &storage).await?
                };
            }
            let tree = edit.persist()?;

            assert_eq!(
                tree.root(),
                tree.root(),
                "seed {seed}: interleaved batched ops must match sequential"
            );
        }
        Ok(())
    }

    /// The transient read path (`get` / `stream_range`) must see exactly the
    /// tree `persist` would produce. For many random insert/delete batches,
    /// build a transient tree WITHOUT persisting, then persist a clone of the
    /// same batch to a [`PersistentTree`]; the transient `get` of every probed
    /// key and the full ordered `stream_range(..)` must match the persistent
    /// tree's `get` / `stream_range` exactly.
    #[dialog_common::test]
    async fn it_reads_in_flight_edits_like_persist() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..200u64 {
            let mut rng = Rng::new(seed);

            // A randomized op stream over a small key domain so inserts and
            // deletes collide on the same keys, churning boundaries and leaving
            // a mix of transient and persistent subtrees in flight.
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..400 {
                let is_insert = !(rng.next_u32()).is_multiple_of(3); // ~2/3 inserts
                let key = rng.next_u32() % 150;
                ops.push((is_insert, key));
            }

            // Build the transient tree in flight, never persisting it here.
            let mut transient = TestTree::empty().edit();
            for &(is_insert, key) in &ops {
                transient = if is_insert {
                    transient
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    transient.delete(&key.to_le_bytes(), &storage).await?
                };
            }

            // Build the same batch again and persist it, flushing to storage so
            // the persistent reference reads are fully resolvable.
            let mut reference = TestTree::empty().edit();
            for &(is_insert, key) in &ops {
                reference = if is_insert {
                    reference
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    reference.delete(&key.to_le_bytes(), &storage).await?
                };
            }
            let mut persistent = reference.persist()?;
            for buffer in persistent.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }

            // Every key in the domain (present or absent) must read identically.
            for key in 0..160u32 {
                let from_transient = transient.get(&key.to_le_bytes(), &storage).await?;
                let from_persistent = persistent.get(&key.to_le_bytes(), &storage).await?;
                assert_eq!(
                    from_transient, from_persistent,
                    "seed {seed}: transient get of key {key} must match persisted get"
                );
            }

            // The full ordered stream must match entry for entry.
            let mut transient_entries = Vec::new();
            {
                let stream = transient.stream_range(.., &storage);
                futures_util::pin_mut!(stream);
                while let Some(entry) = stream.next().await {
                    let entry = entry?;
                    transient_entries.push((entry.key, entry.value));
                }
            }

            let mut persistent_entries = Vec::new();
            {
                let stream = persistent.stream_range(.., &storage);
                futures_util::pin_mut!(stream);
                while let Some(entry) = stream.next().await {
                    let entry = entry?;
                    persistent_entries.push((entry.key, entry.value));
                }
            }

            assert_eq!(
                transient_entries, persistent_entries,
                "seed {seed}: transient stream must match persisted stream"
            );
        }

        Ok(())
    }

    /// A bounded `stream_range` over the in-flight transient tree must match the
    /// same bounded range over the persisted tree, exercising the bound-clamping
    /// in both the transient leaves and the delegated persistent subtrees.
    #[dialog_common::test]
    async fn it_streams_bounded_ranges_like_persist() -> Result<()> {
        use futures_util::StreamExt;

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..50u64 {
            let mut keys: Vec<u32> = (0..300).collect();
            Rng::new(seed).shuffle(&mut keys);

            let mut transient = TestTree::empty().edit();
            for &k in &keys {
                transient = transient
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }

            let mut reference = TestTree::empty().edit();
            for &k in &keys {
                reference = reference
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            let mut persistent = reference.persist()?;
            for buffer in persistent.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }

            let range = 73u32.to_le_bytes()..210u32.to_le_bytes();

            let mut transient_entries = Vec::new();
            {
                let stream = transient.stream_range(range.clone(), &storage);
                futures_util::pin_mut!(stream);
                while let Some(entry) = stream.next().await {
                    transient_entries.push(entry?.key);
                }
            }

            let mut persistent_entries = Vec::new();
            {
                let stream = persistent.stream_range(range.clone(), &storage);
                futures_util::pin_mut!(stream);
                while let Some(entry) = stream.next().await {
                    persistent_entries.push(entry?.key);
                }
            }

            assert_eq!(
                transient_entries, persistent_entries,
                "seed {seed}: bounded transient stream must match persisted stream"
            );
        }

        Ok(())
    }
}
