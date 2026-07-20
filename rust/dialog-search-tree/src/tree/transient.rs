//! In-place batched tree edits.
//!
//! [`TransientTree`] opens a [`PersistentTree`]'s spine and applies a sequence
//! of inserts and deletes by mutating that live structure with a copy-on-write
//! descent. Each operation descends from the root to the target leaf, lifting
//! only the nodes on the path from [`Node::Persistent`] references to editable
//! [`Node::Transient`] form, applies the change to the leaf, then re-shapes the
//! touched path so the tree is canonical again. Untouched siblings stay shared
//! by reference. The shape rules are history-independent, so a batch of edits
//! and the equivalent sequence of single edits each persisted in turn converge
//! on the same root, byte for byte, after every operation.
//!
//! [`persist`](TransientTree::persist) is a pure bottom-up serializer: it makes
//! no shape decisions, because the shape was already established at edit time.

use crate::{
    Accessor, BOTTOM_RANK, Buffer, Cache, Change, ContentAddressedStorage, Delta,
    DialogSearchTreeError, Differential, Distribution, Entry, Geometric, Key, Manifest, Node,
    PersistentNode, PersistentTree, Rank, TransientIndex, TransientNode, TransientSegment,
    TreeWalker, Value, regroup_children, regroup_entries,
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
/// It retains only the in-memory transient spine and the node cache; the new
/// nodes a batch produces are written into a caller-owned [`Delta`] at
/// [`persist`](Self::persist) time, not held here.
pub struct TransientTree<Key, Value, D = Geometric>
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    /// The root, mirroring [`PersistentTree`]'s `root: Blake3Hash`: it starts as
    /// the same (possibly null) hash and is loaded lazily into a transient node
    /// only by the first edit that descends into it, so opening neither awaits
    /// nor touches storage.
    root: TransientRoot<Key, Value>,
    cache: Cache<Blake3Hash, Buffer>,
    /// The tree's format header, stamped into every node this batch persists
    /// and read by the boundary coin during reshaping. Every node in a tree
    /// carries the same manifest.
    // TODO: when editing an existing NON-default tree, adopt the manifest from
    // the loaded root node (via `PersistentNode::manifest`) so an edit
    // preserves the tree's format. That read is async (it loads the root),
    // which the synchronous `edit()`/`new` entry cannot do; today every tree
    // uses `Manifest::default()`, so this is correct until non-default trees
    // exist. See `PersistentTree::edit`.
    manifest: Manifest,
    distribution: PhantomData<D>,
}

impl<Key, Value, D> TransientTree<Key, Value, D>
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
    /// Creates an edit batch over the tree rooted at `root`, deferring the root
    /// load.
    ///
    /// The root is held as its (possibly null) hash and loaded lazily by the
    /// first edit that descends into it, so this is synchronous and touches no
    /// storage.
    pub fn new(root: Blake3Hash, cache: Cache<Blake3Hash, Buffer>) -> Self {
        Self {
            root: TransientRoot::Unloaded(root),
            cache,
            manifest: Manifest::default(),
            distribution: PhantomData,
        }
    }

    /// Loads the root into a transient node for editing, returning `None` for an
    /// empty tree (a null root hash, which cannot be loaded).
    ///
    /// The root's stored header must equal the edit's manifest: editing a tree
    /// under different format parameters would re-coin the touched spine with
    /// the wrong branching/length-guard settings and stamp mixed headers into
    /// one tree — silent shape divergence between replicas. Until an edit
    /// adopts the loaded root's manifest (see the TODO on `manifest`), a
    /// mismatch fails loudly instead. Every node of a well-formed tree carries
    /// the root's manifest, so the root check covers the tree.
    async fn load<Backend>(
        root: TransientRoot<Key, Value>,
        accessor: &Accessor<Backend>,
        manifest: &Manifest,
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
                let header = node.manifest()?;
                if header != *manifest {
                    return Err(DialogSearchTreeError::Node(format!(
                        "Tree manifest mismatch: the root was written under \
                         {header:?} but the edit runs under {manifest:?}"
                    )));
                }
                // The root's left edge is the tree's global leftmost seam,
                // whose separator is the empty string (negative infinity).
                Ok(Some(TransientNode::open(&node, Vec::new())?))
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
        let accessor = Accessor::new(self.cache.clone(), storage.clone());
        let manifest = self.manifest;

        let node = match Self::load(self.root, &accessor, &self.manifest).await? {
            // The first entry of an empty tree becomes a lone segment wrapped in
            // a single-child index, matching the canonical root invariant that
            // the root is always an index.
            None => {
                // The first segment of a tree sits at the global leftmost
                // seam. Its separator is seeded through the distribution's
                // own rule (re-derived over the empty floor) rather than
                // hardcoded: later min-moves refresh it with `reseparate`,
                // and a seed the rule would not itself reproduce makes the
                // tree's bytes depend on edit history. Under the production
                // rule the empty floor is a fixed point, so this IS the
                // empty separator there.
                let separator = D::reseparate(entry.key.as_ref(), &[]);
                TransientNode::Index(TransientIndex {
                    children: vec![Node::Transient(TransientNode::Segment(TransientSegment {
                        entries: vec![entry],
                        separator,
                    }))],
                })
            }
            Some(root) => Edit::Upsert(entry)
                .apply::<Backend, D>(root, &accessor, &manifest)
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
        let accessor = Accessor::new(self.cache.clone(), storage.clone());
        let manifest = self.manifest;

        let Some(root) = Self::load(self.root, &accessor, &self.manifest).await? else {
            // Deleting from an empty tree is a no-op; leave it empty.
            self.root = TransientRoot::Unloaded(NULL_BLAKE3_HASH.clone());
            return Ok(self);
        };
        let edited = Edit::Delete(key.clone())
            .apply::<Backend, D>(root, &accessor, &manifest)
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
                    let at = child_for::<Key, Value>(&index.children, key)?;
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
            PersistentTree::seal(hash.clone(), self.cache.clone());
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
                        let accessor = Accessor::new(cache.clone(), storage.clone());
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

    /// Serializes the edited tree bottom-up into `delta` and returns it as a
    /// [`PersistentTree`], carrying the node cache forward. The root is empty
    /// (`NULL_BLAKE3_HASH`) when the batch left the tree empty.
    ///
    /// The caller owns `delta`: it is the batch's output, an accumulator the
    /// caller may aggregate across many persists and flush on its own schedule.
    /// Only nodes this batch created are added; untouched subtrees stay in
    /// storage and are never re-serialized.
    pub fn persist(
        self,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<PersistentTree<Key, Value, D>, DialogSearchTreeError> {
        let root = match self.root {
            // An untouched root (including an empty tree's null hash) was never
            // loaded; its hash is already durable and is returned verbatim,
            // touching no storage.
            TransientRoot::Unloaded(hash) => hash,
            TransientRoot::Loaded(transient) => {
                transient.persist(delta, &self.manifest)?.hash().clone()
            }
        };

        Ok(PersistentTree::seal(root, self.cache))
    }

    /// Integrates a differential into this edit batch with deterministic
    /// last-write-wins conflict resolution, returning the edited batch.
    ///
    /// Each change is resolved against the batch's own in-flight writes (read
    /// via [`get`](Self::get)), so changes later in the stream see the effect of
    /// earlier ones:
    ///
    /// - **Add**: if the key exists with a different value, the value whose
    ///   blake3 hash (over its serialized rkyv form) is higher wins.
    /// - **Remove**: only removes when the exact entry (key and value) is
    ///   present, so a concurrent update is not clobbered by a stale removal.
    ///
    /// Atomicity is the caller's: on error the batch is dropped and never
    /// persisted, leaving the original tree untouched. The caller seals a
    /// successful integration with [`persist`](Self::persist).
    pub async fn integrate<Backend, Changes>(
        mut self,
        changes: Changes,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        Changes: Differential<Key, Value>,
        Value: PartialEq,
    {
        futures_util::pin_mut!(changes);
        while let Some(change) = changes.next().await {
            match change? {
                Change::Add(entry) => match self.get(&entry.key, storage).await? {
                    None => {
                        self = self.insert(entry.key, entry.value, storage).await?;
                    }
                    Some(existing) => {
                        if existing != entry.value {
                            let existing_hash = value_identity(&existing)?;
                            let new_hash = value_identity(&entry.value)?;
                            if new_hash.as_bytes() > existing_hash.as_bytes() {
                                self = self.insert(entry.key, entry.value, storage).await?;
                            }
                        }
                    }
                },
                Change::Remove(entry) => {
                    if let Some(existing) = self.get(&entry.key, storage).await?
                        && existing == entry.value
                    {
                        self = self.delete(&entry.key, storage).await?;
                    }
                }
            }
        }
        Ok(self)
    }
}

/// Computes the identity hash used for last-write-wins conflict resolution: the
/// blake3 hash of the value's serialized (rkyv) form, the same canonical bytes
/// the value has inside a node.
fn value_identity<Value>(value: &Value) -> Result<Blake3Hash, DialogSearchTreeError>
where
    Value: for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    >,
{
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value)
        .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
    Ok(Blake3Hash::hash(bytes.as_slice()))
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
        manifest: &Manifest,
    ) -> Result<Option<TransientNode<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        D: Distribution,
    {
        // The manifest supplies the branching parameter and the length-guard
        // bound to the rank coin, threaded down the reshape chain so every rank
        // decision uses the tree's own format parameters. It is the same
        // manifest stamped into every node this batch persists.
        let manifest = *manifest;

        // Phase one: lift the path to the target leaf, recording the child index
        // chosen at each level. The routing key is borrowed from this edit, so
        // the descent clones no separate key.
        let key = self.key();
        let mut path = Vec::new();
        loop {
            let node = follow(&mut root, &path)?;
            match node {
                TransientNode::Index(index) => {
                    let at = child_for::<Key, Value>(&index.children, key)?;
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
        // An orphan append: a non-boundary key that sorts after the segment's
        // terminating boundary. Under lower-bound routing a key in the gap
        // between two segments routes LEFT (it is below the right segment's
        // separator), so it lands past the boundary that closes the left
        // segment. No cut is justified after a rank-1 key, so the appended
        // entry belongs with the right neighbor's leftmost leaf: the same
        // rightward fusion a boundary delete needs for its orphaned tail.
        let is_orphan_append = match (&self, follow(&mut root, &path)?) {
            (Edit::Upsert(entry), TransientNode::Segment(segment)) => {
                // Cheapest test first: only a key sorting past the segment's
                // last entry can be an orphan append, so the two rank hashes
                // are paid only on true appends.
                match segment.entries.last() {
                    Some(last) if entry.key > last.key => {
                        D::rank(last.key.as_ref(), &manifest) > BOTTOM_RANK
                            && D::rank(entry.key.as_ref(), &manifest) <= BOTTOM_RANK
                    }
                    _ => false,
                }
            }
            _ => false,
        };

        // A min-move edit replaces the segment's first key (an insert sorting
        // before it, or a delete of it), which rewrites the separator at the
        // segment's left edge. `min_move` captures the (old, new) separator
        // pair for such an edit; `None` when the edit leaves the minimum in
        // place. A single-entry segment empties outright; its joined seam is
        // evaluated below, once the right neighbor's minimum is reachable.
        let min_move = match (&self, follow(&mut root, &path)?) {
            (Edit::Upsert(entry), TransientNode::Segment(segment)) => {
                match segment.entries.binary_search_by(|e| e.key.cmp(&entry.key)) {
                    Err(0) => Some((
                        segment.separator.clone(),
                        D::reseparate(entry.key.as_ref(), &segment.separator),
                    )),
                    _ => None,
                }
            }
            (Edit::Delete(key), TransientNode::Segment(segment)) => {
                match segment.entries.binary_search_by(|e| e.key.cmp(key)) {
                    Ok(0) if segment.entries.len() > 1 => Some((
                        segment.separator.clone(),
                        D::reseparate(segment.entries[1].key.as_ref(), &segment.separator),
                    )),
                    _ => None,
                }
            }
            _ => None,
        };

        // When the new separator's seam rank no longer sustains an index-level
        // cut the old one punched, that cut must dissolve, which merges the
        // edited subtree LEFTWARD across its parent seam: the local re-shape
        // cannot see the left sibling, so the left neighbor's spine must be
        // lifted and fused, mirroring the boundary-delete machinery on the
        // right edge. The global leftmost seam (the empty separator) is a
        // fixed point of the floor rule, so it can never trigger this.
        let dissolves_left_cut = min_move
            .as_ref()
            .map(|(old, new)| seam_cut_dissolves::<D>(old, new, &manifest))
            .unwrap_or(false);

        // The mirror image: a seam-rank RISE that starts punching cuts the old
        // separator did not. The new cut is a split realized by the local
        // regroup, so no neighbor is needed — but the fast path performs no
        // regroup at all, so it must be bypassed for the re-shape to see it.
        let raises_left_cut = min_move
            .as_ref()
            .map(|(old, new)| seam_cut_punches::<D>(old, new, &manifest))
            .unwrap_or(false);

        // Anything not provably canonical falls through to the re-shaping paths.
        if !is_boundary_delete && !is_orphan_append && !dissolves_left_cut && !raises_left_cut {
            let TransientNode::Segment(segment) = follow(&mut root, &path)? else {
                return Err(DialogSearchTreeError::Node(
                    "Path did not reach a segment".into(),
                ));
            };
            if fast_path_keeps_canonical::<Key, Value, D>(&segment.entries, &self, &manifest) {
                apply_to_segment(&mut segment.entries, self);
                // The seam at the segment's left edge moves with its first
                // key. Re-derive the separator from the new minimum against
                // the old separator as the floor; the rule is idempotent
                // when the minimum did not change.
                if let Some(first) = segment.entries.first() {
                    segment.separator = D::reseparate(first.key.as_ref(), &segment.separator);
                }
                return Ok(Some(root));
            }
        }

        let neighbor_path = if is_boundary_delete || is_orphan_append {
            lift_right_neighbor_spine(&mut root, &path, accessor).await?
        } else {
            None
        };

        // The right-LCA: the deepest level where the main and right-neighbor
        // paths diverge (for a boundary delete).
        let lca_depth = match &neighbor_path {
            Some(neighbor_path) => Some(
                path.iter()
                    .zip(neighbor_path.iter())
                    .position(|(a, b)| a != b)
                    .ok_or_else(|| {
                        DialogSearchTreeError::Node(
                            "Boundary delete had no diverging neighbor path".into(),
                        )
                    })?,
            ),
            None => None,
        };

        // A single-entry boundary delete removes the whole segment, joining
        // its left seam with the right neighbor's: the joined separator is
        // re-derived from the neighbor's minimum over the vanished segment's
        // floor, and its rank may likewise dissolve a punched cut.
        let dissolves_left_cut = dissolves_left_cut
            || match &neighbor_path {
                Some(neighbor_path) => {
                    let floor = match follow(&mut root, &path)? {
                        TransientNode::Segment(segment) if segment.entries.len() == 1 => {
                            Some(segment.separator.clone())
                        }
                        _ => None,
                    };
                    match floor {
                        Some(floor) => match follow(&mut root, neighbor_path)? {
                            TransientNode::Segment(neighbor) => match neighbor.entries.first() {
                                Some(first) => seam_cut_dissolves::<D>(
                                    &floor,
                                    &D::reseparate(first.key.as_ref(), &floor),
                                    &manifest,
                                ),
                                None => false,
                            },
                            _ => false,
                        },
                        None => false,
                    }
                }
                None => false,
            };

        // Lift the left neighbor's spine and locate the fusion depth: the
        // deepest ancestor where the edited subtree has a left sibling. For
        // a boundary delete, a divergence deeper than the right-LCA lies
        // INSIDE the fused window (the fusion regroups the main subtree's
        // own left siblings wholesale), so only a crossing at or above the
        // LCA needs the explicit left fusion.
        let left_fuse = if dissolves_left_cut {
            match lift_left_neighbor_spine(&mut root, &path, accessor).await? {
                Some(left_path) => {
                    let depth = path
                        .iter()
                        .zip(left_path.iter())
                        .position(|(a, b)| a != b)
                        .ok_or_else(|| {
                            DialogSearchTreeError::Node(
                                "Left fusion had no diverging neighbor path".into(),
                            )
                        })?;
                    match lca_depth {
                        Some(lca) if depth > lca => None,
                        _ => Some(depth),
                    }
                }
                None => None,
            }
        } else {
            None
        };

        // Phase two: synchronous re-shape. The whole touched region is transient, so
        // the re-shape needs no further loads and runs without any borrow spanning
        // an await.
        let replacement = match (&neighbor_path, lca_depth) {
            (Some(neighbor_path), Some(lca_depth)) => {
                // A rightward fusion: re-shape the shared prefix down to the
                // LCA, where the two child subtrees fuse. A boundary delete
                // pops its key at the facing leaves; an orphan append is
                // applied to the leaf up front and nothing is popped.
                let pop = match self {
                    Edit::Delete(key) => Some(key),
                    upsert @ Edit::Upsert(_) => {
                        let TransientNode::Segment(segment) = follow(&mut root, &path)? else {
                            return Err(DialogSearchTreeError::Node(
                                "Path did not reach a segment".into(),
                            ));
                        };
                        apply_to_segment(&mut segment.entries, upsert);
                        None
                    }
                };
                reshape_fused::<Key, Value, D>(
                    &mut root,
                    &path,
                    neighbor_path,
                    lca_depth,
                    pop.as_ref(),
                    height,
                    left_fuse,
                    &manifest,
                )?
            }
            _ => {
                reshape_path::<Key, Value, D>(&mut root, &path, self, height, left_fuse, &manifest)?
            }
        };
        seal_root::<Key, Value, D, _>(replacement, height, &manifest, accessor).await
    }
}

/// Whether replacing a seam's separator dissolves an index-level cut the old
/// separator punched: true when the old seam rank cut at least level 1 and
/// the new rank is lower. (Punched levels form the range `1..=rank - 2`, so
/// a drop from any rank of 3 or more removes cuts; a rise only adds them,
/// which the local regroup realizes as a split without neighbor content.)
fn seam_cut_dissolves<D>(old_separator: &[u8], new_separator: &[u8], manifest: &Manifest) -> bool
where
    D: Distribution,
{
    let old_rank = D::seam_rank(old_separator, manifest);
    let new_rank = D::seam_rank(new_separator, manifest);
    old_rank > BOTTOM_RANK + 1 && new_rank < old_rank
}

/// Whether replacing a seam's separator punches an index-level cut the old
/// separator did not: true when the new seam rank cuts at least level 1 and
/// rose above the old rank. The cut is realized as a split by the local
/// regroup (no neighbor content is needed), but only a re-shape runs that
/// regroup — the fast path must not swallow such an edit.
fn seam_cut_punches<D>(old_separator: &[u8], new_separator: &[u8], manifest: &Manifest) -> bool
where
    D: Distribution,
{
    let old_rank = D::seam_rank(old_separator, manifest);
    let new_rank = D::seam_rank(new_separator, manifest);
    new_rank > BOTTOM_RANK + 1 && new_rank > old_rank
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
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    if let Node::Persistent(link) = node {
        let persistent = accessor.get_node(&link.node).await?;
        // The link's separator is the seam at the opened subtree's left edge;
        // it moves onto the transient node (a segment stores it, an index
        // derives it from its first child's link).
        let separator = link.separator.clone();
        *node = TransientNode::open(&persistent, separator)?.into();
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
    Key: self::Key + ConditionalSync + 'static,
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

    // Lift the right sibling itself (the last index in `neighbor_path`) so the
    // leftmost descent below can follow into it; the descent only lifts the
    // first child at each deeper level, never the sibling it starts from.
    let sibling = neighbor_path[neighbor_path.len() - 1];
    let parent = follow(root, &neighbor_path[..neighbor_path.len() - 1])?;
    lift(&mut parent.as_index_mut()?.children[sibling], accessor).await?;

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

/// Lifts the right spine of the subtree immediately to the left of `path`, so
/// a left fusion (a min-move whose seam rank drop dissolves an index cut) can
/// merge the edited subtree into that neighbor during the re-shape.
///
/// The mirror of [`lift_right_neighbor_spine`]: the neighbor is found by
/// climbing `path` to the deepest ancestor whose descended child has a left
/// sibling, then walking that sibling's rightmost edge down to its leaf,
/// lifting each node on the way. Returns the path to the neighbor's rightmost
/// leaf, or `None` when `path` runs along the tree's leftmost edge.
async fn lift_left_neighbor_spine<Key, Value, Backend>(
    root: &mut TransientNode<Key, Value>,
    path: &[usize],
    accessor: &Accessor<Backend>,
) -> Result<Option<Vec<usize>>, DialogSearchTreeError>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    let mut neighbor_path: Option<Vec<usize>> = None;
    for depth in (0..path.len()).rev() {
        let ancestor = follow(root, &path[..depth])?;
        if let TransientNode::Index(_) = ancestor
            && path[depth] > 0
        {
            let mut prefix = path[..depth].to_vec();
            prefix.push(path[depth] - 1);
            neighbor_path = Some(prefix);
            break;
        }
    }

    let Some(mut neighbor_path) = neighbor_path else {
        return Ok(None);
    };

    // Lift the left sibling itself so the rightmost descent below can follow
    // into it.
    let sibling = neighbor_path[neighbor_path.len() - 1];
    let parent = follow(root, &neighbor_path[..neighbor_path.len() - 1])?;
    lift(&mut parent.as_index_mut()?.children[sibling], accessor).await?;

    // Walk the neighbor subtree's rightmost edge to its leaf, lifting each
    // node so the whole spine is transient.
    loop {
        let node = follow(root, &neighbor_path)?;
        match node {
            TransientNode::Index(index) => {
                let last = index.children.len() - 1;
                lift(&mut index.children[last], accessor).await?;
                neighbor_path.push(last);
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
///
/// `left_fuse` names the depth (relative to `node`) of the ancestor at which
/// the rebuilt run's head must fuse into its left sibling, when the edit
/// dissolved the cut at the edited subtree's left edge; see [`fuse_left_run`].
fn reshape_path<Key, Value, D>(
    node: &mut TransientNode<Key, Value>,
    path: &[usize],
    edit: Edit<Key, Value>,
    height: Rank,
    left_fuse: Option<usize>,
    manifest: &Manifest,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
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
            // The segment's separator becomes the floor for the regrouped
            // run: its first group re-derives against it, and an emptied
            // segment propagates its removal through the boundary-delete
            // paths, which have the neighbor's keys in memory.
            let floor = std::mem::take(&mut segment.separator);
            Ok(regroup_entries::<Key, Value, D>(
                std::mem::take(&mut segment.entries),
                floor,
                manifest,
            ))
        }
        Some((&at, rest)) => {
            let child = node.child_mut(at)?;
            let replacement = reshape_path::<Key, Value, D>(
                child,
                rest,
                edit,
                height - 1,
                left_fuse.and_then(|depth| depth.checked_sub(1)),
                manifest,
            )?;
            let children = &mut node.as_index_mut()?.children;
            if left_fuse == Some(0) {
                // The replacement's left-edge seam rank dropped: its head
                // must merge into the left sibling. The edited child is
                // consumed by its replacement either way.
                children.remove(at);
                fuse_left_run::<Key, Value, D>(children, at, replacement, height, manifest)
            } else {
                splice_and_regroup::<Key, Value, D>(
                    children,
                    at..at + 1,
                    replacement,
                    height,
                    manifest,
                )
            }
        }
    }
}

/// Splices `run` into `children` at `insert_at` after fusing the run's first
/// node with the child immediately to the left, then re-cuts the child list.
///
/// This realizes a dissolved left-edge cut: the seam at the run's left edge
/// no longer sustains its index-level boundary, so the neighboring subtrees'
/// contents are combined level by level ([`fuse_subtrees`] with nothing to
/// pop) and the pointwise regroup decides every cut afresh; if the seam's
/// new rank still punches some levels, the regroup simply recreates those
/// cuts, so an over-wide window is safe. An empty run degenerates to a plain
/// re-cut.
fn fuse_left_run<Key, Value, D>(
    children: &mut Vec<Node<Key, Value>>,
    insert_at: usize,
    mut run: Vec<Node<Key, Value>>,
    height: Rank,
    manifest: &Manifest,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    if insert_at == 0 {
        return Err(DialogSearchTreeError::Node(
            "Left fusion requires a left sibling".into(),
        ));
    }
    if run.is_empty() {
        return splice_and_regroup::<Key, Value, D>(
            children,
            insert_at..insert_at,
            run,
            height,
            manifest,
        );
    }

    let left_sibling = take_transient(children, insert_at - 1)?;
    let first = run.remove(0).into_transient()?;
    let mut fused =
        fuse_subtrees::<Key, Value, D>(left_sibling, first, None, height - 1, manifest)?;
    fused.extend(run);
    splice_and_regroup::<Key, Value, D>(
        children,
        (insert_at - 1)..(insert_at - 1),
        fused,
        height,
        manifest,
    )
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
///
/// `left_fuse` names the depth (relative to `node`) at which the rebuilt
/// run's head must additionally fuse into its LEFT sibling (a dissolved
/// left-edge cut); it is never deeper than the LCA, since divergences below
/// it lie inside the fused window, whose wholesale regroup re-decides those
/// cuts anyway.
// The fused-reshape path genuinely needs each of these distinct inputs (both
// descent paths, the LCA depth, the pop key, the height, the left-fuse index,
// and now the manifest for the coin); grouping them into a struct would only
// move the argument list, not simplify it.
#[allow(clippy::too_many_arguments)]
fn reshape_fused<Key, Value, D>(
    node: &mut TransientNode<Key, Value>,
    path: &[usize],
    neighbor_path: &[usize],
    lca_depth: usize,
    key: Option<&Key>,
    height: Rank,
    left_fuse: Option<usize>,
    manifest: &Manifest,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
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
        let fused = fuse_subtrees::<Key, Value, D>(main, neighbor, key, height - 1, manifest)?;
        return if left_fuse == Some(0) {
            fuse_left_run::<Key, Value, D>(children, at, fused, height, manifest)
        } else {
            splice_and_regroup::<Key, Value, D>(children, at..at, fused, height, manifest)
        };
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
        left_fuse.and_then(|depth| depth.checked_sub(1)),
        manifest,
    )?;
    let children = &mut node.as_index_mut()?.children;
    if left_fuse == Some(0) {
        children.remove(at);
        fuse_left_run::<Key, Value, D>(children, at, replacement, height, manifest)
    } else {
        splice_and_regroup::<Key, Value, D>(children, at..at + 1, replacement, height, manifest)
    }
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
    key: Option<&Key>,
    height: Rank,
    manifest: &Manifest,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
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
            // entries and re-cut into segments. The main segment's separator
            // is the floor for the fused run: the run's left seam is the main
            // segment's left seam (the neighbour's own seam dissolves and is
            // re-derived fresh if regrouping recreates it).
            let floor = std::mem::take(&mut main.separator);
            if let Some(key) = key
                && main.entries.last().map(|e| &e.key == key).unwrap_or(false)
            {
                main.entries.pop();
            }
            let mut entries = main.entries;
            entries.extend(neighbor.entries);
            Ok(regroup_entries::<Key, Value, D>(entries, floor, manifest))
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

            let fused = fuse_subtrees::<Key, Value, D>(
                main_last,
                neighbor_first,
                key,
                height - 1,
                manifest,
            )?;

            let mut combined = main.children;
            combined.extend(fused);
            combined.extend(neighbor.children);
            regroup_children::<Key, Value, D>(combined, height, manifest)
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
    manifest: &Manifest,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
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
    regroup_children::<Key, Value, D>(std::mem::take(children), height, manifest)
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
///
/// Stripping may need a load: when a delete empties the rightmost subtree, the
/// surviving sibling can still be a persistent link (its spine was never
/// lifted), and whether the wrapper above it is canonical depends on the
/// linked node's kind — an index child makes the wrapper a non-canonical
/// chain, a segment child makes it the canonical root. The child is lifted to
/// find out.
async fn seal_root<Key, Value, D, Backend>(
    mut replacement: Vec<Node<Key, Value>>,
    height: Rank,
    manifest: &Manifest,
    accessor: &Accessor<Backend>,
) -> Result<Option<TransientNode<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
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
        replacement = regroup_children::<Key, Value, D>(replacement, level, manifest)?;
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

    // Strip a non-canonical chain of single-child index nodes over indices. A
    // persistent single child is lifted first: its kind (index or segment)
    // decides whether the wrapper above it is canonical.
    loop {
        let TransientNode::Index(index) = &mut root else {
            break;
        };
        if index.children.len() != 1 {
            break;
        }
        if matches!(&index.children[0], Node::Persistent(_)) {
            lift(&mut index.children[0], accessor).await?;
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
    manifest: &Manifest,
) -> bool
where
    Key: self::Key,
    D: Distribution,
{
    match edit {
        Edit::Upsert(entry) => {
            let found = entries.binary_search_by(|e| e.key.cmp(&entry.key));
            if found.is_ok() {
                return true; // value update only, shape unchanged
            }
            if D::rank(entry.key.as_ref(), manifest) > BOTTOM_RANK {
                return false; // inserting a boundary splits the segment
            }
            let at = found.unwrap_err();
            // Appending after a boundary last entry would leave it interior.
            let appends_last = at == entries.len();
            let last_is_boundary = entries
                .last()
                .map(|e| D::rank(e.key.as_ref(), manifest) > BOTTOM_RANK)
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

/// Index of the child whose subtree covers `key`: the last child whose
/// separator is at or below the key (a probe equal to a separator belongs to
/// the seam's right side), clamped to the leftmost child when the key sits
/// below every separator. A child whose separator cannot be read is a
/// corrupt node: the error surfaces instead of silently routing the edit
/// into the wrong subtree.
fn child_for<Key, Value>(
    children: &[Node<Key, Value>],
    key: &Key,
) -> Result<usize, DialogSearchTreeError>
where
    Key: self::Key,
{
    let mut at = 0usize;
    while at + 1 < children.len() {
        if children[at + 1].separator()? <= key.as_ref() {
            at += 1;
        } else {
            break;
        }
    }
    Ok(at)
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

    use crate::{Buffer, ContentAddressedStorage, Delta, PersistentTree, Rank, distribution};

    type TestTree = PersistentTree<[u8; 4], Vec<u8>>;
    type TestStorage = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;

    /// The geometric rank of a `u32` key, hashed the same way the tree hashes it.
    fn rank_of(key: u32) -> Rank {
        distribution::geometric::rank(&Blake3Hash::hash(&key.to_le_bytes()))
    }

    /// The keys in `range` that act as segment boundaries (rank above the
    /// leaf threshold).
    fn boundary_keys(range: std::ops::Range<u32>) -> Vec<u32> {
        range.filter(|&i| rank_of(i) > 1).collect()
    }

    /// The keys in `range` that are interior (not segment boundaries).
    fn interior_keys(range: std::ops::Range<u32>) -> Vec<u32> {
        range.filter(|&i| rank_of(i) <= 1).collect()
    }

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// Build a tree by inserting `keys` one at a time, each in its own
    /// single-operation edit batch, then flush it to storage. Persisting after
    /// every operation is the history-independence baseline a single combined
    /// batch must reproduce.
    async fn sequential(keys: &[u32], storage: &mut TestStorage) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        let mut delta = Delta::zero();
        for &k in keys {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this
            // persist created: a persist writes new nodes only into the delta,
            // never into storage, so they must be stored before the following
            // edit descends into them.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
        let mut delta = Delta::zero();
        let tree = edit.persist(&mut delta)?;

        assert_eq!(
            tree.root(),
            expected.root(),
            "batched inserts must match sequential inserts"
        );
        Ok(())
    }

    /// Building and then editing a tree stamps the manifest into the root node
    /// both times: an edit re-persists with the same manifest, so the format
    /// is stable across edits and readable from any node.
    #[dialog_common::test]
    async fn it_stamps_the_manifest_into_the_root_across_edits() -> Result<()> {
        use crate::{Accessor, Cache, Manifest, PersistentNode};

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let built = sequential(&(0..200).collect::<Vec<u32>>(), &mut storage).await?;

        // A fresh cache reads the persisted nodes straight from storage.
        let accessor = Accessor::new(Cache::new(), storage.clone());
        let root: PersistentNode<[u8; 4], Vec<u8>> = accessor.get_node(built.root()).await?;
        assert_eq!(root.manifest()?, Manifest::default());

        // Edit the built tree and persist; the new root still carries the
        // manifest.
        let mut delta = Delta::zero();
        let edited = built
            .edit()
            .insert(9999u32.to_le_bytes(), vec![1], &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        let edited_root: PersistentNode<[u8; 4], Vec<u8>> =
            accessor.get_node(edited.root()).await?;
        assert_eq!(edited_root.manifest()?, Manifest::default());
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
        let mut delta = Delta::zero();
        let tree = edit.persist(&mut delta)?;
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

    /// Build a tree from `keys`, then delete `to_delete`, each operation in its
    /// own single-operation edit batch, flush, and return the resulting tree.
    /// This is the per-operation-persist baseline for the delete oracles.
    async fn sequential_with_deletes(
        keys: &[u32],
        to_delete: &[u32],
        storage: &mut TestStorage,
    ) -> Result<TestTree> {
        let mut tree = sequential(keys, storage).await?;
        let mut delta = Delta::zero();
        for &k in to_delete {
            tree = tree
                .edit()
                .delete(&k.to_le_bytes(), storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this
            // persist created.
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
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
            let mut delta = Delta::zero();
            let tree = edit.persist(&mut delta)?;
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
            let mut delta = Delta::zero();
            let tree = edit.persist(&mut delta)?;

            assert_eq!(
                tree.root(),
                expected.root(),
                "seed {seed}: batched delete must match sequential delete"
            );
        }
        Ok(())
    }

    /// A random interleaving of inserts and deletes in one batch must match the
    /// same operations applied one at a time, each persisted in its own batch.
    /// This is the strongest oracle: it exercises seams created and dissolved
    /// repeatedly within a single edit.
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
            let mut delta = Delta::zero();
            for &(is_insert, key) in &ops {
                tree = if is_insert {
                    tree.edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    tree.edit()
                        .delete(&key.to_le_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?
                };
                // Flush after each persist so the next edit can load the nodes
                // this persist created.
                for (_, buffer) in delta.flush() {
                    storage
                        .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                        .await?;
                }
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
            let mut batched_delta = Delta::zero();
            let batched = edit.persist(&mut batched_delta)?;

            assert_eq!(
                batched.root(),
                tree.root(),
                "seed {seed}: interleaved batched ops must match sequential"
            );
        }
        Ok(())
    }

    /// The transient read path (`get` / `stream_range`) must see exactly the
    /// An in-flight insert is readable from the same transient batch before any
    /// persist or flush: reads walk the live spine, not storage.
    #[dialog_common::test]
    async fn it_reads_unflushed_insertions() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut edit = TestTree::empty().edit();

        edit = edit
            .insert(1u32.to_le_bytes(), vec![1, 2, 3], &storage)
            .await?;
        assert_eq!(
            edit.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1, 2, 3]),
            "an in-flight insert should be readable before persisting"
        );

        edit = edit
            .insert(2u32.to_le_bytes(), vec![4, 5, 6], &storage)
            .await?;
        assert_eq!(
            edit.get(&1u32.to_le_bytes(), &storage).await?,
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            edit.get(&2u32.to_le_bytes(), &storage).await?,
            Some(vec![4, 5, 6])
        );

        Ok(())
    }

    /// A transient batch reads a mix of flushed-from-storage keys (an earlier,
    /// flushed tree the batch opened over) and its own in-flight keys.
    #[dialog_common::test]
    async fn it_reads_in_flight_and_stored_keys() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Build and flush a baseline of 0..5 so those keys live in storage.
        let base = sequential(&(0..5).collect::<Vec<_>>(), &mut storage).await?;

        // Open a batch over it and add 5..10 in flight, without persisting.
        // `sequential` stores each value as the key's little-endian bytes, so
        // the in-flight inserts use the same value form for a uniform assertion.
        let mut edit = base.edit();
        for i in 5..10u32 {
            edit = edit
                .insert(i.to_le_bytes(), i.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // 0..5 resolve through storage (untouched persistent subtrees), 5..10
        // through the live transient spine.
        for i in 0..10u32 {
            assert_eq!(
                edit.get(&i.to_le_bytes(), &storage).await?,
                Some(i.to_le_bytes().to_vec()),
                "key {i} should be readable from the in-flight batch"
            );
        }

        Ok(())
    }

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
            let mut delta = Delta::zero();
            let persistent = reference.persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
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
            let mut delta = Delta::zero();
            let persistent = reference.persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
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

    /// Flushes a delta's pending nodes into `storage`, so the persisted tree's
    /// root becomes reachable from storage alone. Used by the canonical-form
    /// tests, which compare a flushed delete result against an independent
    /// from-scratch rebuild.
    async fn flush_into(
        delta: &mut Delta<Blake3Hash, Buffer>,
        storage: &mut TestStorage,
    ) -> Result<()> {
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    /// Deleting a boundary key must yield the same root as building a tree from
    /// the surviving keys directly. The from-scratch rebuild is an independent
    /// canonical oracle: it never touches the delete path, so a delete that
    /// reshapes incorrectly cannot also corrupt the reference.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_a_boundary_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();
        let boundaries = boundary_keys(0..1000);
        assert!(
            boundaries.len() >= 2,
            "need at least 2 boundary keys for a meaningful test; got {}",
            boundaries.len()
        );

        let full_tree = sequential(&all_keys, &mut storage).await?;

        for &bk in boundaries.iter().take(5) {
            let mut delta = Delta::zero();
            let tree_via_delete = full_tree
                .edit()
                .delete(&bk.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            flush_into(&mut delta, &mut storage).await?;

            let remaining: Vec<u32> = all_keys.iter().copied().filter(|&k| k != bk).collect();
            let tree_from_scratch = sequential(&remaining, &mut storage).await?;

            assert_eq!(
                tree_via_delete.root(),
                tree_from_scratch.root(),
                "deleting boundary key {bk} should produce the same root \
                 as building from scratch without it"
            );
        }

        Ok(())
    }

    /// Deleting a non-boundary key must yield the same root as building from the
    /// surviving keys directly.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_a_non_boundary_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();
        let non_boundaries = interior_keys(0..1000);
        assert!(!non_boundaries.is_empty());

        let full_tree = sequential(&all_keys, &mut storage).await?;

        for &key in non_boundaries.iter().take(5) {
            let mut delta = Delta::zero();
            let tree_via_delete = full_tree
                .edit()
                .delete(&key.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            flush_into(&mut delta, &mut storage).await?;

            let remaining: Vec<u32> = all_keys.iter().copied().filter(|&k| k != key).collect();
            let tree_from_scratch = sequential(&remaining, &mut storage).await?;

            assert_eq!(
                tree_via_delete.root(),
                tree_from_scratch.root(),
                "deleting non-boundary key {key} should produce the same root \
                 as building from scratch without it"
            );
        }

        Ok(())
    }

    /// Building a tree then pruning the extra keys must converge to the same
    /// root as a direct build of the final key set.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_bulk_deletion() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let final_keys: Vec<u32> = (0..200).collect();
        let extra_keys: Vec<u32> = (200..400).collect();

        let mut all_keys = final_keys.clone();
        all_keys.extend(&extra_keys);

        let tree_direct = sequential(&final_keys, &mut storage).await?;

        let mut tree_pruned = sequential(&all_keys, &mut storage).await?;
        let mut delta = Delta::zero();
        for &ek in &extra_keys {
            tree_pruned = tree_pruned
                .edit()
                .delete(&ek.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this
            // persist created.
            flush_into(&mut delta, &mut storage).await?;
        }

        assert_eq!(
            tree_direct.root(),
            tree_pruned.root(),
            "build-then-prune must converge to the same root as a direct build"
        );

        Ok(())
    }

    /// Deleting a key then re-inserting it must restore the original root,
    /// confirming an edit and its inverse cancel exactly.
    #[dialog_common::test]
    async fn it_restores_original_root_after_delete_then_reinsert() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..500).collect();
        let original = sequential(&all_keys, &mut storage).await?;

        // A mix of boundary and non-boundary keys.
        let test_keys: Vec<u32> = {
            let mut keys = boundary_keys(0..500);
            keys.extend(interior_keys(0..500).into_iter().take(3));
            keys.truncate(6);
            keys
        };

        for &key in &test_keys {
            let mut delete_delta = Delta::zero();
            let after_delete = original
                .edit()
                .delete(&key.to_le_bytes(), &storage)
                .await?
                .persist(&mut delete_delta)?;
            flush_into(&mut delete_delta, &mut storage).await?;

            let mut restore_delta = Delta::zero();
            let restored = after_delete
                .edit()
                .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut restore_delta)?;
            flush_into(&mut restore_delta, &mut storage).await?;

            assert_eq!(
                original.root(),
                restored.root(),
                "delete then re-insert of key {key} should restore the original root"
            );
        }

        Ok(())
    }

    /// An insert-only history and an insert-then-delete history that end with the
    /// same entry set must converge to the same root (history independence).
    #[dialog_common::test]
    async fn it_converges_to_same_root_regardless_of_operation_history() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // History A: insert 0..100 directly.
        let tree_a = sequential(&(0..100).collect::<Vec<_>>(), &mut storage).await?;

        // History B: insert 0..200, then delete 100..200.
        let mut tree_b = sequential(&(0..200).collect::<Vec<_>>(), &mut storage).await?;
        let mut delta = Delta::zero();
        for i in 100..200u32 {
            tree_b = tree_b
                .edit()
                .delete(&i.to_le_bytes(), &storage)
                .await?
                .persist(&mut delta)?;
            // Flush after each persist so the next edit can load the nodes this
            // persist created.
            flush_into(&mut delta, &mut storage).await?;
        }

        assert_eq!(
            tree_a.root(),
            tree_b.root(),
            "insert-only vs insert-then-delete must converge for the same entry set"
        );

        Ok(())
    }

    /// Keys longer than `max_separator` are permanently rank 0 (the
    /// length-guarded coin), so a set of only oversized keys can never cut a
    /// boundary: edits must still terminate, produce one open segment, read
    /// back completely, and converge with a from-scratch rebuild after
    /// deletes. This is the history-region band (512..4096-byte keys).
    #[dialog_common::test]
    async fn it_handles_a_band_of_permanently_rank_zero_keys() -> Result<()> {
        const OVERSIZED: usize = 600; // above the default max_separator of 512

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        type BandTree = PersistentTree<[u8; OVERSIZED], Vec<u8>>;

        let make_key = |i: u32| {
            let mut key = [0u8; OVERSIZED];
            key[..4].copy_from_slice(&i.to_be_bytes());
            key
        };

        let mut delta = Delta::zero();
        let mut tree = BandTree::empty();
        for i in 0..60u32 {
            tree = tree
                .edit()
                .insert(make_key(i), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        for i in 0..60u32 {
            let found = tree.get(&make_key(i), &storage).await?;
            assert!(found.is_some(), "oversized key {i} reads back");
        }

        // Delete a third and compare against the from-scratch build.
        for i in (0..60u32).step_by(3) {
            tree = tree
                .edit()
                .delete(&make_key(i), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        let mut scratch = BandTree::empty();
        for i in 0..60u32 {
            if i % 3 == 0 {
                continue;
            }
            scratch = scratch
                .edit()
                .insert(make_key(i), i.to_le_bytes().to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }
        assert_eq!(
            tree.root(),
            scratch.root(),
            "the all-rank-0 band converges under deletes"
        );
        Ok(())
    }

    /// Editing a tree whose root was written under a different manifest must
    /// fail loudly: re-coining the touched spine under other format
    /// parameters (and stamping mixed headers into one tree) would silently
    /// break shape convergence between replicas. This pins the tripwire until
    /// edits adopt the loaded root's manifest.
    #[dialog_common::test]
    async fn it_rejects_editing_a_tree_with_a_mismatched_manifest() -> Result<()> {
        use crate::{Manifest, PersistentNodeBody};

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let foreign = Manifest {
            fanout_n: 4,
            ..Manifest::default()
        };
        let entries = vec![crate::Entry {
            key: 7u32.to_le_bytes(),
            value: 7u32.to_le_bytes().to_vec(),
        }];
        let body = PersistentNodeBody::segment_from_entries(entries, foreign)?;
        let buffer = Buffer::from(body.as_bytes()?);
        let root = buffer.blake3_hash();
        storage.store(buffer.as_ref().to_vec(), root).await?;

        let tree = TestTree::from_hash(root.clone());
        let result = tree
            .edit()
            .insert(9u32.to_le_bytes(), 9u32.to_le_bytes().to_vec(), &storage)
            .await;
        assert!(
            result.is_err(),
            "editing under a mismatched manifest must fail, not silently re-coin"
        );
        Ok(())
    }

    /// Deleting a random subset must converge to the from-scratch build of
    /// the survivors. The rebuild is an oracle independent of the delete
    /// path, so any delete that reshapes non-canonically fails here even if
    /// batched and sequential deletes agree with each other.
    #[dialog_common::test]
    async fn it_matches_scratch_rebuild_for_random_deletes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..25u64 {
            let mut rng = Rng::new(seed);
            let keys: Vec<u32> = (0..300).collect();
            let mut tree = sequential(&keys, &mut storage).await?;

            let doomed: Vec<u32> = keys
                .iter()
                .copied()
                .filter(|_| rng.next_u32().is_multiple_of(3))
                .collect();

            let mut delta = Delta::zero();
            for key in &doomed {
                tree = tree
                    .edit()
                    .delete(&key.to_le_bytes(), &storage)
                    .await?
                    .persist(&mut delta)?;
                flush_into(&mut delta, &mut storage).await?;
            }

            let survivors: Vec<u32> = keys
                .iter()
                .copied()
                .filter(|k| !doomed.contains(k))
                .collect();
            let scratch = sequential(&survivors, &mut storage).await?;

            assert_eq!(
                tree.root(),
                scratch.root(),
                "seed {seed}: deleting {} keys must match the scratch rebuild",
                doomed.len()
            );
        }
        Ok(())
    }

    /// A fast-path delete of a segment's minimum rewrites the separator in
    /// place; when the re-derived separator's seam rank RISES enough to punch
    /// an index-level cut the old separator did not, the tree must regroup to
    /// create that cut, exactly as a from-scratch build of the surviving keys
    /// would. The simulator bakes the ranks into the key bytes: deleting `b`
    /// promotes `c` (seam rank 3) to segment minimum, so the seam must now
    /// punch a level-1 cut.
    #[dialog_common::test]
    async fn it_recreates_index_cuts_when_min_delete_raises_seam_rank() -> Result<()> {
        use crate::helpers::{DistributionSimulator, SpecKey, encode_key, test_storage};
        type SpecTree = PersistentTree<SpecKey, Vec<u8>, DistributionSimulator>;

        let mut storage = test_storage();
        let a = encode_key(b"a", 2, 1); // leaf boundary, quiet seam
        let b = encode_key(b"b", 1, 1); // interior, quiet seam
        let c = encode_key(b"c", 1, 3); // interior, seam punches level 1

        let mut delta = Delta::zero();
        let mut tree = SpecTree::empty();
        for key in [a, b, c] {
            tree = tree
                .edit()
                .insert(key, key.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let deleted = tree
            .edit()
            .delete(&b, &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let mut scratch = SpecTree::empty();
        for key in [a, c] {
            scratch = scratch
                .edit()
                .insert(key, key.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        assert_eq!(
            deleted.root(),
            scratch.root(),
            "a min-delete that raises the seam rank must recreate the index cut"
        );
        Ok(())
    }

    /// Deleting the sole key of the rightmost subtree, when the surviving
    /// sibling subtree was persisted in an earlier batch, must not leave a
    /// single-child index-over-index root: the result must match the
    /// from-scratch build of the surviving keys.
    #[dialog_common::test]
    async fn it_strips_a_persistent_single_child_root_after_rightmost_delete() -> Result<()> {
        use crate::helpers::{DistributionSimulator, SpecKey, encode_key, test_storage};
        type SpecTree = PersistentTree<SpecKey, Vec<u8>, DistributionSimulator>;

        let mut storage = test_storage();
        let a = encode_key(b"a", 2, 1); // leaf boundary, quiet seam
        let b = encode_key(b"b", 1, 3); // interior, seam punches level 1

        let mut delta = Delta::zero();
        let mut tree = SpecTree::empty();
        for key in [a, b] {
            tree = tree
                .edit()
                .insert(key, key.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        let deleted = tree
            .edit()
            .delete(&b, &storage)
            .await?
            .persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        let mut scratch = SpecTree::empty();
        for key in [a] {
            scratch = scratch
                .edit()
                .insert(key, key.to_vec(), &storage)
                .await?
                .persist(&mut delta)?;
            for (_, buffer) in delta.flush() {
                storage
                    .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                    .await?;
            }
        }

        assert_eq!(
            deleted.root(),
            scratch.root(),
            "emptying the rightmost subtree must strip the leftover root level"
        );
        Ok(())
    }

    /// Deleting the sole entry of a single-entry segment (a boundary whose
    /// segment holds only itself) must still produce a canonical tree.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_emptying_a_segment() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Construct the single-entry segment deterministically: take the first
        // two boundaries in byte order and EXCLUDE every key strictly between
        // them from the fixture, so the second boundary's segment holds only
        // itself. This holds under any coin — no dependence on two boundaries
        // happening to be byte-adjacent in a dense range.
        let boundaries = boundary_keys(0..2000);
        let mut byte_boundaries: Vec<(u32, [u8; 4])> =
            boundaries.iter().map(|&k| (k, k.to_le_bytes())).collect();
        byte_boundaries.sort_by(|a, b| a.1.cmp(&b.1));
        assert!(
            byte_boundaries.len() >= 2,
            "fixture must contain two boundaries"
        );
        let (_, first_bytes) = byte_boundaries[0];
        let (solo_key, solo_bytes) = byte_boundaries[1];

        let all_keys: Vec<u32> = (0..2000u32)
            .filter(|&k| {
                let kb = k.to_le_bytes();
                !(kb > first_bytes && kb < solo_bytes)
            })
            .collect();

        let full_tree = sequential(&all_keys, &mut storage).await?;

        let mut delta = Delta::zero();
        let tree_via_delete = full_tree
            .edit()
            .delete(&solo_key.to_le_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        flush_into(&mut delta, &mut storage).await?;

        let remaining: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&k| k != solo_key)
            .collect();
        let tree_from_scratch = sequential(&remaining, &mut storage).await?;

        assert_eq!(
            tree_via_delete.root(),
            tree_from_scratch.root(),
            "deleting sole entry in segment (key {solo_key}) should produce a canonical tree"
        );

        Ok(())
    }

    /// Deleting the first entry (smallest key in byte order) must produce a
    /// canonical tree.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_first_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();

        let mut sorted: Vec<[u8; 4]> = all_keys.iter().map(|k| k.to_le_bytes()).collect();
        sorted.sort();
        let first_key = sorted[0];
        let first_u32 = u32::from_le_bytes(first_key);

        let full_tree = sequential(&all_keys, &mut storage).await?;

        let mut delta = Delta::zero();
        let tree_via_delete = full_tree
            .edit()
            .delete(&first_key, &storage)
            .await?
            .persist(&mut delta)?;
        flush_into(&mut delta, &mut storage).await?;

        let remaining: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&k| k != first_u32)
            .collect();
        let tree_from_scratch = sequential(&remaining, &mut storage).await?;

        assert_eq!(
            tree_via_delete.root(),
            tree_from_scratch.root(),
            "deleting first entry (key {first_u32}) should produce a canonical tree"
        );

        Ok(())
    }

    /// Deleting the last entry (largest key in byte order) must produce a
    /// canonical tree. The rightmost segment is always a tail, so this verifies
    /// tails at the end are left intact.
    #[dialog_common::test]
    async fn it_produces_canonical_tree_after_deleting_last_entry() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let all_keys: Vec<u32> = (0..1000).collect();

        let mut sorted: Vec<[u8; 4]> = all_keys.iter().map(|k| k.to_le_bytes()).collect();
        sorted.sort();
        let last_key = *sorted.last().unwrap();
        let last_u32 = u32::from_le_bytes(last_key);

        let full_tree = sequential(&all_keys, &mut storage).await?;

        let mut delta = Delta::zero();
        let tree_via_delete = full_tree
            .edit()
            .delete(&last_key, &storage)
            .await?
            .persist(&mut delta)?;
        flush_into(&mut delta, &mut storage).await?;

        let remaining: Vec<u32> = all_keys
            .iter()
            .copied()
            .filter(|&k| k != last_u32)
            .collect();
        let tree_from_scratch = sequential(&remaining, &mut storage).await?;

        assert_eq!(
            tree_via_delete.root(),
            tree_from_scratch.root(),
            "deleting last entry (key {last_u32}) should produce a canonical tree"
        );

        Ok(())
    }

    /// The future of a recursive invariant walk, resolving to the subtree's
    /// (min, max) leaf keys.
    type LeafBounds<'a> =
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<([u8; 4], [u8; 4])>> + 'a>>;

    /// Walks a persisted subtree verifying the separator invariants, and
    /// returns the subtree's (min, max) leaf keys:
    ///
    /// - a link's separator equals the canonical shortest-distinguishing
    ///   string of its seam (the shortest prefix of its own subtree's minimum
    ///   leaf key that sorts strictly above the left-adjacent subtree's
    ///   maximum), and
    /// - a node's first link carries the separator the node itself carries in
    ///   its parent (propagation), with the tree's global leftmost chain
    ///   carrying the empty separator.
    fn assert_separator_invariants<'a>(
        hash: &'a Blake3Hash,
        expected_leftmost: &'a [u8],
        storage: &'a TestStorage,
    ) -> LeafBounds<'a> {
        Box::pin(async move {
            use crate::{ArchivedNodeBody, Buffer, PersistentNode, distribution};

            let bytes = storage
                .retrieve(hash)
                .await?
                .ok_or_else(|| anyhow::anyhow!("node {hash} missing from storage"))?;
            let node: PersistentNode<[u8; 4], Vec<u8>> = PersistentNode::new(Buffer::from(bytes));

            match node.body()? {
                ArchivedNodeBody::Segment(segment) => {
                    let first: [u8; 4] = segment.first_key::<[u8; 4]>()?.as_slice().try_into()?;
                    let last: [u8; 4] = segment.last_key::<[u8; 4]>()?.as_slice().try_into()?;
                    Ok((first, last))
                }
                ArchivedNodeBody::Index(index) => {
                    let mut previous_max: Option<[u8; 4]> = None;
                    let mut bounds: Option<([u8; 4], [u8; 4])> = None;
                    for (at, link) in index.links()?.into_iter().enumerate() {
                        let child: Blake3Hash = link.node;
                        let separator: Vec<u8> = link.separator;
                        let expected_child_leftmost = if at == 0 {
                            expected_leftmost.to_vec()
                        } else {
                            separator.clone()
                        };
                        let (child_min, child_max) =
                            assert_separator_invariants(&child, &expected_child_leftmost, storage)
                                .await?;

                        match previous_max {
                            None => assert_eq!(
                                separator, expected_leftmost,
                                "a node's first link must carry the node's own separator"
                            ),
                            Some(previous_max) => assert_eq!(
                                separator,
                                distribution::shortest_separator(&previous_max, &child_min),
                                "separator at seam {previous_max:02x?}|{child_min:02x?} \
                                 must be the canonical shortest-distinguishing string"
                            ),
                        }

                        previous_max = Some(child_max);
                        bounds = Some((bounds.map(|(min, _)| min).unwrap_or(child_min), child_max));
                    }
                    bounds.ok_or_else(|| anyhow::anyhow!("index node had no children"))
                }
            }
        })
    }

    /// Every link in a built tree stores the canonical shortest-distinguishing
    /// separator of its seam, first links propagate their node's own
    /// separator, and the global leftmost chain is empty. This pins the
    /// stored bytes, not just the root hash: two wrong separators that
    /// happen to agree across build orders would pass the canonical-form
    /// tests but fail here.
    #[dialog_common::test]
    async fn it_stores_canonical_shortest_separators() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let keys: Vec<u32> = (0..600).collect();
        let tree = sequential(&keys, &mut storage).await?;

        assert_separator_invariants(tree.root(), &[], &storage).await?;

        Ok(())
    }

    /// A variable-length opaque key for exercising the variable-length insert
    /// paths (the fixed `[u8; N]` keys never share a long common prefix, so
    /// they cannot reproduce a new-minimum split within one leaf).
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct VarKey(Vec<u8>);

    impl AsRef<[u8]> for VarKey {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }

    impl crate::Key for VarKey {
        fn try_from_bytes(bytes: &[u8]) -> Result<Self, crate::DialogSearchTreeError> {
            Ok(VarKey(bytes.to_vec()))
        }
        fn min() -> Self {
            VarKey(Vec::new())
        }
        fn max() -> Self {
            VarKey(vec![u8::MAX; 64])
        }
    }

    type VarTree = PersistentTree<VarKey, Vec<u8>>;

    /// The geometric rank of a variable-length key.
    fn var_rank(key: &[u8]) -> Rank {
        distribution::geometric::rank(&Blake3Hash::hash(key))
    }

    /// Regression: inserting a NEW MINIMUM variable-length key into a
    /// single-entry tree must not drop the existing entry. Mirrors the
    /// artifact two-commit bug where a second entity whose key sorts before
    /// the first wiped the first. Fixed-width `[u8;4]` tests never exercise a
    /// new-minimum split because equal-length keys share no long prefix.
    #[dialog_common::test]
    async fn it_keeps_prior_when_inserting_new_minimum_variable_key() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Sweep suffixes so the new-minimum `low` lands on a boundary rank at
        // least once (a boundary new-minimum insert is the trigger).
        let prefix = b"prefix/shared/".to_vec();
        let mut high_bytes = prefix.clone();
        high_bytes.extend_from_slice(b"zzzzzzzzzzzzzzzzzzzz");
        let high = VarKey(high_bytes);

        for n in 0..300u32 {
            let mut low_bytes = prefix.clone();
            low_bytes.extend_from_slice(format!("aaaa-{n:04}").as_bytes());
            let low = VarKey(low_bytes);
            assert!(low < high, "low must sort before high");

            // Commit 1: insert `high` and persist.
            let mut delta = Delta::zero();
            let tree = VarTree::empty()
                .edit()
                .insert(high.clone(), high.0.clone(), &storage)
                .await?
                .persist(&mut delta)?;
            flush_into(&mut delta, &mut storage).await?;

            // Commit 2: insert `low` (the new minimum) over the persisted tree.
            let mut delta = Delta::zero();
            let tree = tree
                .edit()
                .insert(low.clone(), low.0.clone(), &storage)
                .await?
                .persist(&mut delta)?;
            flush_into(&mut delta, &mut storage).await?;

            let got_high = tree.get(&high, &storage).await?;
            let got_low = tree.get(&low, &storage).await?;
            if got_low.is_none() || got_high.is_none() {
                panic!(
                    "n={n} rank(low)={} rank(high)={}: low_present={} high_present={}",
                    var_rank(low.as_ref()),
                    var_rank(high.as_ref()),
                    got_low.is_some(),
                    got_high.is_some(),
                );
            }
        }

        Ok(())
    }
}
