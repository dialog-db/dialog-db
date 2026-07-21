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
    Novelty, NoveltyEntry, NoveltyOp, PersistentNode, PersistentTree, Rank, TransientIndex,
    TransientNode, TransientSegment, TreeWalker, Value, distribution::cap, link_bounds,
    regroup_children, regroup_entries,
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
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds, RangeInclusive},
};

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

/// The unwrapped root of a [`TransientTree`], exposed to the crate so the
/// hitchhiker tree can take ownership of a finished batch's live spine without
/// serializing it.
pub(crate) enum TransientRootParts<Key, Value> {
    /// The durable root hash (an unedited or emptied batch). `NULL_BLAKE3_HASH`
    /// is an empty tree.
    Unloaded(Blake3Hash),
    /// The live transient node the batch edited.
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
    ///
    /// [`PersistentTree::edit_with_manifest`] recovers the edited tree's own
    /// manifest from its root node and passes it to
    /// [`with_manifest`](Self::with_manifest), so an edit preserves the tree's
    /// format. The synchronous [`new`](Self::new) cannot perform that (async)
    /// root read and defaults it; see its documentation for when that is sound.
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
    /// load, under the *default* format [`Manifest`].
    ///
    /// The root is held as its (possibly null) hash and loaded lazily by the
    /// first edit that descends into it, so this is synchronous and touches no
    /// storage. Recovering the edited tree's real manifest would mean loading
    /// its root, which is async, so this entry cannot and defaults it: it is
    /// sound only when the tree's manifest IS [`Manifest::default`]. Use
    /// [`with_manifest`](Self::with_manifest), or the
    /// [`PersistentTree::edit_with_manifest`] that feeds it, to preserve a
    /// non-default tree's format.
    pub fn new(root: Blake3Hash, cache: Cache<Blake3Hash, Buffer>) -> Self {
        Self::with_manifest(root, cache, Manifest::default())
    }

    /// Creates an edit batch over the tree rooted at `root` under an explicit
    /// format `manifest`, deferring the root load.
    ///
    /// The manifest must be the one the tree's existing nodes carry, or the
    /// batch will re-shape and re-stamp the touched path under a format the
    /// untouched siblings do not share. [`PersistentTree::edit_with_manifest`]
    /// reads it from the root for exactly this reason.
    pub fn with_manifest(
        root: Blake3Hash,
        cache: Cache<Blake3Hash, Buffer>,
        manifest: Manifest,
    ) -> Self {
        Self {
            root: TransientRoot::Unloaded(root),
            cache,
            manifest,
            distribution: PhantomData,
        }
    }

    /// Creates an edit batch over an already-loaded transient `node`, sharing
    /// `cache`. Used by the hitchhiker tree to replay leaf-bound ops directly on
    /// its live spine, with no serialization round-trip.
    ///
    /// The caller hands over a live transient node, not a hash, so there is no
    /// persisted root to read a format header back from here: `manifest` must
    /// be the one the spine's nodes carry, which the hitchhiker captures when
    /// it first loads the tree's root (see
    /// [`HitchhikerTree::persist`](crate::HitchhikerTree::persist)). A tree
    /// born empty has no stored header and passes the default.
    pub(crate) fn from_loaded(
        node: TransientNode<Key, Value>,
        cache: Cache<Blake3Hash, Buffer>,
        manifest: Manifest,
    ) -> Self {
        Self {
            root: TransientRoot::Loaded(node),
            cache,
            manifest,
            distribution: PhantomData,
        }
    }

    /// Unwraps the batch into its root: the live transient node when one was
    /// loaded (the common case after edits), or the durable root hash when the
    /// batch was never edited or left the tree empty.
    pub(crate) fn into_root(self) -> TransientRootParts<Key, Value> {
        match self.root {
            TransientRoot::Loaded(node) => TransientRootParts::Loaded(node),
            TransientRoot::Unloaded(hash) => TransientRootParts::Unloaded(hash),
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
                    novelty: Novelty::new(),
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
                    // Ops buffered here are newer than anything below: a write
                    // lands in a node's buffer and only reaches a leaf when that
                    // buffer overflows. Resolving them on the way down is what
                    // makes a read of a lifted node agree with a read of the
                    // same node in its stored form. Ops ride the link that
                    // routes them, so the descended link's buffer is the only
                    // one that can cover the key.
                    let at = child_for::<Key, Value>(&index.children, key)?;
                    if let Some(op) = index.novelty.resolve::<Key>(at, key.as_ref())? {
                        return Ok(match op {
                            NoveltyOp::Assert(value) => Some(value),
                            NoveltyOp::Retract => None,
                        });
                    }
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
                collect_stream_plan(node, &bounds, &mut plan);
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
                            // Two different values contending for one key:
                            // ask the value type first (it can encode
                            // semantics the bytes cannot — a tombstone
                            // beating any concurrent assertion), and fall
                            // back to the deterministic last-write-wins
                            // hash race. Both paths are antisymmetric, so
                            // replicas integrating in opposite directions
                            // pick the same winner and converge — and the
                            // loser is FUSED into the winner rather than
                            // dropped, so value types that aggregate
                            // (collapsed claim versions) survive the
                            // contest on both sides identically.
                            let replaces = match entry.value.prevails_over(&existing) {
                                Some(verdict) => verdict,
                                None => {
                                    let existing_hash = value_identity(&existing)?;
                                    let new_hash = value_identity(&entry.value)?;
                                    new_hash.as_bytes() > existing_hash.as_bytes()
                                }
                            };
                            let fused = if replaces {
                                Value::fuse(entry.value, &existing)
                            } else {
                                Value::fuse(existing.clone(), &entry.value)
                            };
                            if fused != existing {
                                self = self.insert(entry.key, fused, storage).await?;
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

    /// Builds a tree whose content is the concatenation of `pieces`, reusing
    /// the interior of every [`Piece::Range`] structurally.
    ///
    /// Pieces must be given in ascending, non-overlapping key order: every key
    /// a piece contributes must sort strictly after every key of the pieces
    /// before it. The result is canonical: [`persist`](Self::persist)ing it
    /// yields the same root, byte for byte, as building a tree from scratch
    /// over the union of the pieces' entries.
    ///
    /// Reads are bounded by the piece seams. Carving a range loads only the
    /// nodes on the two spines covering its bounds, and joining two adjacent
    /// pieces loads only their facing edge spines. Interior nodes of a range
    /// stay [`Node::Persistent`] links, are never fetched, and are re-emitted
    /// verbatim at persist time, so the persist delta scales with the seams,
    /// not the entry count.
    pub async fn stitch<Backend>(
        pieces: Vec<Piece<'_, Key, Value, D>>,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        // One cache serves the whole stitch. Nodes are content-addressed, so
        // sharing the first source's cache (when there is one) is always safe
        // and keeps its warm entries; the stitched tree carries it forward.
        let cache = pieces
            .iter()
            .find_map(|piece| match piece {
                Piece::Range { source, .. } => Some(source.node_cache.clone()),
                Piece::Entries(_) => None,
            })
            .unwrap_or_else(Cache::new);
        let accessor = Accessor::new(cache.clone(), storage.clone());

        // The stitched tree keeps its sources' format. A manifest lives in the
        // nodes, so it is read from the source roots — and every source must
        // AGREE: grafting subtrees written under one format into a tree
        // stamped with another would mix headers and diverge silently from
        // either side's canonical shape, so a mismatch fails loudly here,
        // exactly as the edit path's `load` does. A stitch of nothing but
        // loose entries has no source to inherit from and takes the default
        // format.
        let mut manifest: Option<Manifest> = None;
        for piece in &pieces {
            if let Piece::Range { source, .. } = piece {
                let root = source.root().clone();
                if &root != NULL_BLAKE3_HASH {
                    let node: PersistentNode<Key, Value> = accessor.get_node(&root).await?;
                    let header = node.manifest()?;
                    match &manifest {
                        None => manifest = Some(header),
                        Some(first) if *first == header => {}
                        Some(first) => {
                            return Err(DialogSearchTreeError::Node(format!(
                                "Stitch manifest mismatch: one source was written under \
                                 {first:?} and another under {header:?}"
                            )));
                        }
                    }
                }
            }
        }
        let manifest = manifest.unwrap_or_default();

        // Carve every piece into a part: a subtree of known height whose
        // interior children are still persistent links. A part also remembers
        // its source root when carving left the source untouched, enabling the
        // single-piece fast path below.
        let mut parts: Vec<(TransientNode<Key, Value>, Rank, Option<Blake3Hash>)> = Vec::new();
        for piece in pieces {
            match piece {
                Piece::Range { source, range } => {
                    if let Some((node, height, trim)) =
                        carve(source.root().clone(), &range, &accessor).await?
                    {
                        let whole = match trim {
                            Trim::Unchanged => Some(source.root().clone()),
                            _ => None,
                        };
                        parts.push((node, height, whole));
                    }
                }
                Piece::Entries(entries) => {
                    if !entries.is_empty() {
                        let run = regroup_entries::<Key, Value, D>(entries, Vec::new(), &manifest);
                        parts.push((
                            TransientNode::Index(TransientIndex {
                                children: run,
                                novelty: Novelty::new(),
                            }),
                            1,
                            None,
                        ));
                    }
                }
            }
        }

        // A single piece covering its whole source IS that source: hand its
        // root back by hash so persisting writes nothing at all. The result
        // carries the source's manifest, not the default — a later edit of
        // the stitched tree must run under the format its nodes were
        // written with.
        if let [(_, _, Some(root))] = parts.as_slice() {
            return Ok(TransientTree::with_manifest(root.clone(), cache, manifest));
        }

        // A canonical tree opens its leftmost seam at the empty separator
        // (negative infinity), and every index level derives its left edge
        // from its leftmost segment. A start-trimmed carve leaves the
        // surviving leftmost segment carrying the separator its parent link
        // held in the SOURCE tree (derived from a left neighbor the trim
        // dropped), and nothing downstream recomputes it — joins re-cut only
        // right-hand seams — so the stale string would propagate into every
        // leftmost link of the persisted result and break the byte-for-byte
        // canonicality the stitch promises. Re-floor the first part's left
        // edge; untouched whole-source parts are already canonical.
        if let Some((node, _, whole)) = parts.first_mut()
            && whole.is_none()
        {
            refloor_leftmost(node)?;
        }

        // Fold the parts left to right. Each join lifts the two facing edge
        // spines (the only loads it performs), levels the shorter part with
        // single-child wrappers that the join dismantles again, and re-cuts
        // the seam level by level; everything off the seam is carried over
        // untouched.
        let mut merged: Option<(TransientNode<Key, Value>, Rank)> = None;
        for (node, height, _) in parts {
            merged = Some(match merged {
                None => (node, height),
                Some((left, left_height)) => {
                    let target = left_height.max(height);
                    let mut left = raise(left, left_height, target);
                    let mut right = raise(node, height, target);
                    lift_boundary_spine(&mut left, false, &accessor).await?;
                    lift_boundary_spine(&mut right, true, &accessor).await?;
                    let (mut run, seam_novelty) =
                        concat_levels::<Key, Value, D>(left, right, target, &manifest)?;
                    if run.len() == 1 {
                        // Keep the accumulator's nominal height tight so later
                        // joins pad as little as possible.
                        let mut node = run.pop().expect("run has one node").into_transient()?;
                        // Ops the join lifted off the seam are pending against
                        // this run; re-attach them to the node that covers it,
                        // each op routed to its link.
                        if !seam_novelty.is_empty() {
                            match &mut node {
                                TransientNode::Index(index) => {
                                    let bounds = link_bounds(&index.children)?;
                                    index.novelty.route::<Key>(&bounds, seam_novelty)?;
                                }
                                // A segment cannot hold a buffer, so wrap it in
                                // an index that can; a single link takes every
                                // op.
                                TransientNode::Segment(_) => {
                                    let mut novelty = Novelty::new();
                                    novelty.route::<Key>(&[], seam_novelty)?;
                                    node = TransientNode::Index(TransientIndex {
                                        children: vec![node.into()],
                                        novelty,
                                    });
                                }
                            }
                        }
                        (node, target)
                    } else {
                        let mut index = TransientIndex {
                            children: run,
                            novelty: Novelty::new(),
                        };
                        let bounds = link_bounds(&index.children)?;
                        index.novelty.route::<Key>(&bounds, seam_novelty)?;
                        (TransientNode::Index(index), target + 1)
                    }
                }
            });
        }

        let root = match merged {
            None => {
                return Ok(TransientTree::with_manifest(
                    NULL_BLAKE3_HASH.clone(),
                    cache,
                    manifest,
                ));
            }
            // A lone segment can only arise from degenerate single-leaf
            // sources; hand it to the leveling loop as a height-0 run so it
            // gains its canonical index root.
            Some((node @ TransientNode::Segment(_), _)) => {
                seal_root::<Key, Value, D, _>(vec![node.into()], 0, &manifest, &accessor).await?
            }
            Some((TransientNode::Index(mut index), height)) => {
                // `seal_root` reshapes the children into a canonical root and
                // strips single-child chains, so the node holding this buffer
                // may not survive. Carry the ops onto whatever root it returns:
                // they are pending against this whole subtree, and the root is
                // the one node guaranteed to cover it.
                let novelty = index.novelty.take_all::<Key>()?;
                let sealed =
                    seal_root::<Key, Value, D, _>(index.children, height - 1, &manifest, &accessor)
                        .await?;
                match sealed {
                    Some(TransientNode::Index(mut root)) if !novelty.is_empty() => {
                        let bounds = link_bounds(&root.children)?;
                        root.novelty.route::<Key>(&bounds, novelty)?;
                        Some(TransientNode::Index(root))
                    }
                    other => other,
                }
            }
        };

        Ok(TransientTree {
            root: match root {
                Some(node) => TransientRoot::Loaded(node),
                None => TransientRoot::Unloaded(NULL_BLAKE3_HASH.clone()),
            },
            cache,
            manifest,
            distribution: PhantomData,
        })
    }
}

/// One contiguous piece of a [`stitch`](TransientTree::stitch): a key range
/// carved out of an existing tree, or a run of explicit entries.
///
/// Pieces are given to [`stitch`](TransientTree::stitch) in ascending,
/// non-overlapping key order: every key a piece contributes must sort strictly
/// after every key of the pieces before it.
pub enum Piece<'a, Key, Value, D = Geometric>
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    /// The entries of `source` whose keys fall within `range`, both bounds
    /// inclusive. A range that contains none of the source's keys contributes
    /// nothing.
    Range {
        /// The tree the range is taken from. It must be flushed: its nodes are
        /// read from the storage handed to [`stitch`](TransientTree::stitch).
        source: &'a PersistentTree<Key, Value, D>,
        /// The inclusive key range to take.
        range: RangeInclusive<Key>,
    },
    /// Explicit entries, sorted ascending by key.
    Entries(Vec<Entry<Key, Value>>),
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
                    // This edit is newer than anything buffered for the same key
                    // on the way down, and it is about to write the key's value
                    // into the leaf. A stale op left in an ancestor's buffer
                    // would keep shadowing that leaf on every read (buffered ops
                    // win over stored entries), so the write would be invisible.
                    // Drop it: the edit supersedes it, and after this descent the
                    // leaf is the only place the key lives. Ops ride the link
                    // that routes them, so only the descended link's buffer can
                    // hold one.
                    let at = child_for::<Key, Value>(&index.children, key)?;
                    index.novelty.remove_key::<Key>(at, key.as_ref())?;
                    lift(&mut index.children[at], accessor).await?;
                    path.push(at);
                }
                TransientNode::Segment(_) => break,
            }
        }

        let height = path.len() as Rank;

        // The backstop widening: forced seams split an over-target vetoed
        // stretch — or an over-ceiling frame — into pieces whose anchors
        // are a function of the WHOLE run, so a membership-changing edit
        // into any piece must re-shape the complete run. Value-only updates
        // and no-op deletes cannot move an anchor (anchors are key-derived)
        // and skip it. Unlike the retired hard cap this widening reaches
        // only force-split runs — the scan joins nothing unless a
        // neighboring separator exceeds `max_separator`, which the weight
        // coin's natural cuts never store — so the common edit path pays two
        // separator length reads and nothing else.
        let changes_membership = if manifest.max_segment == 0 {
            false
        } else {
            match (&self, follow(&mut root, &path)?) {
                (Edit::Upsert(entry), TransientNode::Segment(segment)) => segment
                    .entries
                    .binary_search_by(|e| e.key.cmp(&entry.key))
                    .is_err(),
                (Edit::Delete(key), TransientNode::Segment(segment)) => {
                    segment.entries.binary_search_by(|e| e.key.cmp(key)).is_ok()
                }
                _ => true,
            }
        };
        let widened = if changes_membership {
            merge_forced_run::<Key, Value, Backend>(&mut root, &mut path, accessor, &manifest)
                .await?
        } else {
            false
        };

        // The frame ceiling's fast-path gate: a membership-changing edit
        // that leaves its segment over the ceiling must reach the regroup,
        // which force-splits the frame — the in-place fast path would let
        // the segment grow past the hard bound. The weight sum is paid only
        // when a ceiling is armed and membership changes; an upsert of an
        // existing key never gets here (weights are key-derived).
        let over_ceiling = if !changes_membership || manifest.frame_ceiling() == 0 {
            false
        } else {
            match follow(&mut root, &path)? {
                TransientNode::Segment(segment) => {
                    let weight: usize = segment
                        .entries
                        .iter()
                        .map(|entry| cap::entry_weight(entry.key.as_ref()))
                        .sum();
                    let weight = match &self {
                        Edit::Upsert(entry) => weight + cap::entry_weight(entry.key.as_ref()),
                        Edit::Delete(_) => weight,
                    };
                    weight > manifest.frame_ceiling()
                }
                _ => false,
            }
        };

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
        // An orphan append: a key that sorts after the segment's terminating
        // boundary but earns no cut of its own. Under lower-bound routing a
        // key in the gap between two segments routes LEFT (it is below the
        // right segment's separator), so it lands past the boundary that
        // closes the left segment; with no cut after it, the appended entry
        // belongs with the right neighbor's leftmost leaf — the same
        // rightward fusion a boundary delete needs for its orphaned tail.
        //
        // What "earns no cut" means depends on the coin. Under the
        // entry-counted coin (`max_segment == 0`) it is the two rank tests
        // of old. Under the banked weight coin the appended key becomes the
        // seam's new left partner and its decision is `leaf_cut` with the
        // bank the seam inherits: zero when the old boundary's seam to the
        // newcomer is accepted (the bank resets there), or the trailing
        // vetoed stretch's whole weight (old boundary included) when the
        // newcomer extends that stretch — in which case the old boundary's
        // own cut is vetoed away and the moved terminal decision may still
        // come up cut (no fusion; the in-window regroup realizes the moved
        // boundary) or not (fusion). Either way the fused window regroups
        // pair-aware and settles every case.
        let is_orphan_append = match (&self, follow(&mut root, &path)?) {
            (Edit::Upsert(entry), TransientNode::Segment(segment)) => {
                // Cheapest test first: only a key sorting past the segment's
                // last entry can be an orphan append, so the coin hashes are
                // paid only on true appends.
                match segment.entries.last() {
                    Some(last) if entry.key > last.key => {
                        if manifest.max_segment == 0 {
                            D::rank(last.key.as_ref(), &manifest) > BOTTOM_RANK
                                && D::rank(entry.key.as_ref(), &manifest) <= BOTTOM_RANK
                        } else if D::vetoes(last.key.as_ref(), entry.key.as_ref(), &manifest) {
                            let bank = trailing_stretch_weight::<Key, Value, D>(
                                &segment.entries,
                                &manifest,
                            );
                            !D::leaf_cut(entry.key.as_ref(), bank, &manifest)
                        } else {
                            !D::leaf_cut(entry.key.as_ref(), 0, &manifest)
                        }
                    }
                    _ => false,
                }
            }
            _ => false,
        };

        // Under the banked coin a segment's terminating cut is funded by its
        // trailing vetoed stretch: deleting a stretch member (or splitting
        // the stretch so the bank resets closer to the end) can defund the
        // cut, dissolving the stored boundary even though the deleted key
        // is interior. Detect it by re-running the terminal decision over
        // the post-delete tail; a defunded boundary takes the rightward
        // fusion path, exactly like a boundary delete, with the deletion
        // applied up front instead of popped at the seam. The veto gate
        // keeps this off the common path: a delete with no vetoed seam
        // beside it cannot change any bank.
        let dissolves_terminal_cut = if manifest.max_segment == 0 || is_boundary_delete {
            false
        } else {
            match (&self, follow(&mut root, &path)?) {
                (Edit::Delete(key), TransientNode::Segment(segment)) => {
                    match segment.entries.binary_search_by(|e| e.key.cmp(key)) {
                        Ok(at) if at + 1 < segment.entries.len() => {
                            let entries = &segment.entries;
                            let beside_stretch = (at > 0
                                && D::vetoes(
                                    entries[at - 1].key.as_ref(),
                                    entries[at].key.as_ref(),
                                    &manifest,
                                ))
                                || D::vetoes(
                                    entries[at].key.as_ref(),
                                    entries[at + 1].key.as_ref(),
                                    &manifest,
                                );
                            if beside_stretch {
                                let bank = trailing_stretch_weight_skipping::<Key, Value, D>(
                                    entries, at, &manifest,
                                );
                                let last = entries
                                    .last()
                                    .expect("segment with a found key is non-empty");
                                !D::leaf_cut(last.key.as_ref(), bank, &manifest)
                            } else {
                                false
                            }
                        }
                        _ => false,
                    }
                }
                _ => false,
            }
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

        // Anything not provably canonical falls through to the re-shaping
        // paths. A widened window always re-shapes: the merge left the
        // stretch as one oversized segment that only the regroup re-splits.
        if !is_boundary_delete
            && !is_orphan_append
            && !dissolves_terminal_cut
            && !dissolves_left_cut
            && !raises_left_cut
            && !widened
            && !over_ceiling
        {
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
                // A moved separator moves a link boundary in the deepest
                // ancestor where this leaf is not on the leftmost edge, and
                // buffered ops are grouped BY those boundaries: re-home the
                // ops the move re-ranged, or the grouping goes stale and a
                // read or flush of the wrong link misses them. The re-shape
                // paths re-route buffers wholesale, so only this in-place
                // fast path needs it.
                if min_move.is_some() {
                    reroute_moved_seam::<Key, Value>(&mut root, &path)?;
                }
                return Ok(Some(root));
            }
        }

        let neighbor_path = if is_boundary_delete || is_orphan_append || dissolves_terminal_cut {
            lift_right_neighbor_spine(&mut root, &path, accessor).await?
        } else {
            None
        };

        // A rightward fusion hands the neighbor's leftmost leaf new
        // predecessors, and if that leaf is the head of a force-split
        // vetoed stretch its anchors are a function of the whole stretch:
        // widen the neighbor across its forced seams too, so the fusion
        // regroups against complete membership. For any non-cluster
        // neighbor the scan reads one separator length and joins nothing.
        let neighbor_path = match neighbor_path {
            Some(mut neighbor_path) if manifest.max_segment > 0 => {
                merge_forced_run::<Key, Value, Backend>(
                    &mut root,
                    &mut neighbor_path,
                    accessor,
                    &manifest,
                )
                .await?;
                Some(neighbor_path)
            }
            other => other,
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
                // pops its key at the facing leaves; an orphan append — and
                // an interior delete that defunded the terminal cut — is
                // applied to the leaf up front and nothing is popped (the
                // pop mechanism removes only a leaf's LAST entry, which an
                // interior delete is not).
                let pop = match self {
                    Edit::Delete(key) if is_boundary_delete => Some(key),
                    edit => {
                        let TransientNode::Segment(segment) = follow(&mut root, &path)? else {
                            return Err(DialogSearchTreeError::Node(
                                "Path did not reach a segment".into(),
                            ));
                        };
                        apply_to_segment(&mut segment.entries, edit);
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

/// Re-homes buffered ops after a fast-path min-move edit changed the
/// separator of the leaf at `path`.
///
/// A leaf's separator is the separator of every ancestor it is the leftmost
/// leaf of, so the moved seam surfaces as a link boundary exactly once: in
/// the deepest ancestor where the remaining path leaves the leftmost edge.
/// Per-link buffers are grouped by those boundaries, so the ops around the
/// moved bound must be re-checked against it ([`Novelty`]'s reroute). A path
/// on the tree's global leftmost edge moved no interior boundary and needs
/// nothing.
fn reroute_moved_seam<Key, Value>(
    root: &mut TransientNode<Key, Value>,
    path: &[usize],
) -> Result<(), DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
{
    // The deepest depth whose position leaves the leftmost edge: below it the
    // path is all zeros, so the leaf's separator IS the boundary at this
    // ancestor's descended link.
    let mut depth = path.len();
    while depth > 0 && path[depth - 1] == 0 {
        depth -= 1;
    }
    if depth == 0 {
        return Ok(());
    }
    let at = path[depth - 1];
    let ancestor = follow(root, &path[..depth - 1])?.as_index_mut()?;
    // The descended child's separator is the leaf's new separator, threaded
    // up through leftmost-first-child derivation.
    let bound = ancestor.children[at].separator()?.to_vec();
    ancestor.novelty.reroute_boundary::<Key>(at, &bound)
}

/// The weight bank an entry appended after this segment's last entry
/// inherits when its seam to that entry is vetoed: the summed entry weight
/// of the maximal trailing vetoed stretch, the last entry included (under
/// the bank rule every stretch key, the old terminal included, is the left
/// partner of a vetoed seam once the newcomer extends the stretch, so each
/// banks its weight). The appended key's own weight is not counted here;
/// `Distribution::leaf_cut` adds it.
fn trailing_stretch_weight<Key, Value, D>(
    entries: &[Entry<Key, Value>],
    manifest: &Manifest,
) -> usize
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    let mut at = entries.len() - 1;
    let mut weight = cap::entry_weight(entries[at].key.as_ref());
    while at > 0
        && D::vetoes(
            entries[at - 1].key.as_ref(),
            entries[at].key.as_ref(),
            manifest,
        )
    {
        at -= 1;
        weight += cap::entry_weight(entries[at].key.as_ref());
    }
    weight
}

/// The weight bank this segment's terminal seam would see after removing
/// `entries[skip]`: the summed entry weight of the post-delete trailing
/// vetoed stretch, EXCLUDING the last entry (whose own weight
/// `Distribution::leaf_cut` adds). Adjacency is evaluated over the
/// post-delete sequence, so a delete that splits the stretch (the joined
/// seam around the gap turning accepted) resets the bank at the split, as
/// the regroup would.
fn trailing_stretch_weight_skipping<Key, Value, D>(
    entries: &[Entry<Key, Value>],
    skip: usize,
    manifest: &Manifest,
) -> usize
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    let mut bank = 0usize;
    let survivors: Vec<usize> = (0..entries.len()).filter(|at| *at != skip).collect();
    for pair in survivors.windows(2).rev() {
        let [prev, cur] = pair else { break };
        if D::vetoes(
            entries[*prev].key.as_ref(),
            entries[*cur].key.as_ref(),
            manifest,
        ) {
            bank += cap::entry_weight(entries[*prev].key.as_ref());
        } else {
            break;
        }
    }
    bank
}

/// Widens an edit's window to the whole force-split run around the leaf at
/// `path` — a vetoed stretch's pieces, an over-ceiling frame's pieces, or
/// both — merging them into a single segment carrying the run's own left
/// separator, updating `path`'s leaf position, and returning whether any
/// merge happened.
///
/// Forced anchors (`cap::forced_cut_positions`, `cap::frame_cut_positions`)
/// are pure functions of a whole run's keys, so an edit that changes any
/// piece's membership must re-shape the complete run, or differently
/// ordered edits would anchor against different subsets and diverge. A
/// run's pieces are found from stored structure alone: a separator longer
/// than `max_separator` is the self-identifying mark of a forced seam (both
/// anchor kinds store one; see `cap::forced_seam_separator`) and joins a
/// piece to its left sibling — no natural cut ever stores such a
/// separator, so the mark is exact for canonically built trees. Forced
/// seams never punch index cuts (the seam coin's length guard ranks them
/// 0), so a run's pieces are always contiguous children of one parent
/// index node and the scan never crosses a node boundary. A stitch can
/// still leave an over-long natural separator at a piece seam; that only
/// widens the window, and a wider regroup reproduces the same canonical
/// shape.
///
/// This is the arm-1 window machinery rescoped to where it is cheap: the
/// hard cap ran it for every over-target run (the common case in the SE
/// workload — a 7x regression); this widening reaches only force-split
/// runs, which are rare (vetoed clusters, or the coin's `e^(-ceiling/S)`
/// tail) and bounded by their own extent.
async fn merge_forced_run<Key, Value, Backend>(
    root: &mut TransientNode<Key, Value>,
    path: &mut [usize],
    accessor: &Accessor<Backend>,
    manifest: &Manifest,
) -> Result<bool, DialogSearchTreeError>
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
    if path.is_empty() {
        return Ok(false);
    }
    let at = path[path.len() - 1];
    let parent_path = path[..path.len() - 1].to_vec();
    let bound = manifest.max_separator as usize;

    let (lo, hi) = {
        let children = &follow(root, &parent_path)?.as_index()?.children;
        let mut lo = at;
        while lo > 0 && children[lo].separator()?.len() > bound {
            lo -= 1;
        }
        let mut hi = at;
        while hi + 1 < children.len() && children[hi + 1].separator()?.len() > bound {
            hi += 1;
        }
        (lo, hi)
    };
    if lo == hi {
        return Ok(false);
    }

    // Untouched pieces may still be persistent links; lift each so the
    // merge below can consume their entries.
    for member in lo..=hi {
        let parent = follow(root, &parent_path)?.as_index_mut()?;
        lift(&mut parent.children[member], accessor).await?;
    }

    // Merge the pieces into one segment carrying the stretch's left
    // separator. The parent's buffered ops are grouped by link boundaries,
    // which the merge just changed, so they are re-routed onto the new
    // layout.
    let parent = follow(root, &parent_path)?.as_index_mut()?;
    let pending = parent.novelty.take_all::<Key>()?;
    let members: Vec<Node<Key, Value>> = parent
        .children
        .splice(lo..=hi, std::iter::empty())
        .collect();
    let mut entries = Vec::new();
    let mut separator = Vec::new();
    for (offset, member) in members.into_iter().enumerate() {
        let TransientNode::Segment(segment) = member.into_transient()? else {
            return Err(DialogSearchTreeError::Node(
                "Vetoed-stretch window member was not a leaf segment".into(),
            ));
        };
        if offset == 0 {
            separator = segment.separator;
        }
        entries.extend(segment.entries);
    }
    parent.children.insert(
        lo,
        Node::Transient(TransientNode::Segment(TransientSegment {
            entries,
            separator,
        })),
    );
    if !pending.is_empty() {
        let bounds = link_bounds(&parent.children)?;
        parent.novelty.route::<Key>(&bounds, pending)?;
    }
    let leaf = path.len() - 1;
    path[leaf] = lo;
    Ok(true)
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
            // Regrouping replaces `node` with a run of new nodes, so the ops
            // buffered here have nowhere to live unless they are carried over:
            // they are pending against this subtree, and the run covers exactly
            // that subtree.
            let carried = node.as_index_mut()?.novelty.take_all::<Key>()?;
            let children = &mut node.as_index_mut()?.children;
            let (run, mut lifted) = if left_fuse == Some(0) {
                // The replacement's left-edge seam rank dropped: its head
                // must merge into the left sibling. The edited child is
                // consumed by its replacement either way.
                children.remove(at);
                fuse_left_run::<Key, Value, D>(children, at, replacement, height, manifest)?
            } else {
                (
                    splice_and_regroup::<Key, Value, D>(
                        children,
                        at..at + 1,
                        replacement,
                        height,
                        manifest,
                    )?,
                    Vec::new(),
                )
            };
            // Ops lifted off fused child-level nodes are deeper (older) than
            // this node's own buffer, so they precede it; `carry_novelty`'s
            // stable re-sort then keeps the newest op for a key last.
            lifted.extend(carried);
            carry_novelty::<Key, Value>(run, lifted)
        }
    }
}

/// Re-attaches ops to a run of nodes that replaced the node holding them.
///
/// A node's `novelty` is pending against the subtree it roots, so when a
/// reshape dismantles that node the ops must land on nodes still covering their
/// keys. The run covers exactly the range the dismantled node did, so each op
/// goes to the node whose range contains it.
///
/// Separators are lower bounds, so the owning node is the last one whose
/// separator is at or below the key; a key below every separator belongs to the
/// leftmost node, matching how routing clamps. Within the chosen node the op is
/// then routed to its link, keeping the per-link grouping fresh.
///
/// A run of segments cannot hold ops at all, so those are wrapped in an index
/// that can. Dropping them instead would lose pending writes.
fn carry_novelty<Key, Value>(
    mut run: Vec<Node<Key, Value>>,
    novelty: Vec<NoveltyEntry<Value>>,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    if novelty.is_empty() || run.is_empty() {
        return Ok(run);
    }

    // Partition the ops across the run first (preserving their order, which
    // carries the precedence: older ops arrive before newer ones and the
    // stable per-link sort keeps the newest op for a key last), then attach
    // each bucket with one routed merge.
    let mut buckets: Vec<Vec<NoveltyEntry<Value>>> = run.iter().map(|_| Vec::new()).collect();
    for entry in novelty {
        // The node covering this key, by the same lower-bound rule routing
        // uses: the last node whose separator is at or below the key.
        let mut at = 0usize;
        while at + 1 < run.len() {
            match run[at + 1].separator() {
                Ok(separator) if separator <= entry.key.as_slice() => at += 1,
                _ => break,
            }
        }
        buckets[at].push(entry);
    }

    for (at, bucket) in buckets.into_iter().enumerate() {
        if bucket.is_empty() {
            continue;
        }
        match &mut run[at] {
            Node::Transient(TransientNode::Index(index)) => {
                let bounds = link_bounds(&index.children)?;
                index.novelty.route::<Key>(&bounds, bucket)?;
            }
            // Leaves and persistent links cannot carry a buffer, so wrap the
            // node in an index that can. The wrapper keeps the same key range,
            // so routing is unchanged; its single link takes every op.
            other => {
                let placeholder = Node::Transient(TransientNode::Segment(TransientSegment {
                    entries: Vec::new(),
                    separator: Vec::new(),
                }));
                let wrapped = std::mem::replace(other, placeholder);
                let mut novelty = Novelty::new();
                novelty.route::<Key>(&[], bucket)?;
                *other = Node::Transient(TransientNode::Index(TransientIndex {
                    children: vec![wrapped],
                    novelty,
                }));
            }
        }
    }
    Ok(run)
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
///
/// Returns the spliced run together with the buffered ops the fusion lifted
/// off the dismantled nodes (see [`fuse_subtrees`]); the caller must
/// re-attach them to a node covering the run.
fn fuse_left_run<Key, Value, D>(
    children: &mut Vec<Node<Key, Value>>,
    insert_at: usize,
    mut run: Vec<Node<Key, Value>>,
    height: Rank,
    manifest: &Manifest,
) -> Result<JoinedRun<Key, Value>, DialogSearchTreeError>
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
        return Ok((
            splice_and_regroup::<Key, Value, D>(
                children,
                insert_at..insert_at,
                run,
                height,
                manifest,
            )?,
            Vec::new(),
        ));
    }

    let left_sibling = take_transient(children, insert_at - 1)?;
    let first = run.remove(0).into_transient()?;
    let (mut fused, lifted) =
        fuse_subtrees::<Key, Value, D>(left_sibling, first, None, height - 1, manifest)?;
    fused.extend(run);
    Ok((
        splice_and_regroup::<Key, Value, D>(
            children,
            (insert_at - 1)..(insert_at - 1),
            fused,
            height,
            manifest,
        )?,
        lifted,
    ))
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
        // Regrouping below replaces this node, so its buffer is carried onto
        // the run that takes its place (see `carry_novelty`), together with
        // the buffers the fusion lifts off the two dismantled children.
        let carried = node.as_index_mut()?.novelty.take_all::<Key>()?;
        let children = &mut node.as_index_mut()?.children;
        let main = take_transient(children, at)?;
        // After removing the main child the neighbour shifted left into `at`.
        let neighbor = take_transient(children, at)?;
        let (fused, mut lifted) =
            fuse_subtrees::<Key, Value, D>(main, neighbor, key, height - 1, manifest)?;
        let run = if left_fuse == Some(0) {
            let (run, more) =
                fuse_left_run::<Key, Value, D>(children, at, fused, height, manifest)?;
            lifted.extend(more);
            run
        } else {
            splice_and_regroup::<Key, Value, D>(children, at..at, fused, height, manifest)?
        };
        // The lifted ops came from child level or below, so they are older
        // than this node's own buffer and precede it; `carry_novelty`'s
        // stable re-sort keeps the newest op for a key last.
        lifted.extend(carried);
        return carry_novelty::<Key, Value>(run, lifted);
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
    let carried = node.as_index_mut()?.novelty.take_all::<Key>()?;
    let children = &mut node.as_index_mut()?.children;
    let (run, mut lifted) = if left_fuse == Some(0) {
        children.remove(at);
        fuse_left_run::<Key, Value, D>(children, at, replacement, height, manifest)?
    } else {
        (
            splice_and_regroup::<Key, Value, D>(
                children,
                at..at + 1,
                replacement,
                height,
                manifest,
            )?,
            Vec::new(),
        )
    };
    // Lifted ops are deeper (older) than this node's own buffer; see the
    // ordering note in `reshape_path`.
    lifted.extend(carried);
    carry_novelty::<Key, Value>(run, lifted)
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
///
/// Returns the fused run together with the buffered ops lifted off the index
/// nodes the fusion dismantled, the same contract as [`concat_levels`]: those
/// ops are pending writes with no home until the caller re-attaches them to a
/// node covering the run, and dropping them loses data. The lifted list is
/// ordered deepest first (a seam level's ops precede the two fused nodes'
/// own), so the append-then-stable-sort re-attachment in [`carry_novelty`]
/// keeps the newest op for a key last.
fn fuse_subtrees<Key, Value, D>(
    main: TransientNode<Key, Value>,
    neighbor: TransientNode<Key, Value>,
    key: Option<&Key>,
    height: Rank,
    manifest: &Manifest,
) -> Result<JoinedRun<Key, Value>, DialogSearchTreeError>
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
            // re-derived fresh if regrouping recreates it). Leaves buffer
            // nothing, so there is nothing to lift here.
            let floor = std::mem::take(&mut main.separator);
            if let Some(key) = key
                && main.entries.last().map(|e| &e.key == key).unwrap_or(false)
            {
                main.entries.pop();
            }
            let mut entries = main.entries;
            entries.extend(neighbor.entries);
            Ok((
                regroup_entries::<Key, Value, D>(entries, floor, manifest),
                Vec::new(),
            ))
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

            let (fused, seam_novelty) = fuse_subtrees::<Key, Value, D>(
                main_last,
                neighbor_first,
                key,
                height - 1,
                manifest,
            )?;

            // Both destructured nodes' buffers are pending against the run
            // being built, and the regroup discards the nodes that held them.
            // Lift them out for the caller, deepest first: the seam ops come
            // from one level further down and are older than this level's own.
            // The two fused nodes cover disjoint ranges, so their relative
            // order carries no precedence.
            let mut novelty = seam_novelty;
            novelty.extend(main.novelty.take_all::<Key>()?);
            novelty.extend(neighbor.novelty.take_all::<Key>()?);

            let mut combined = main.children;
            combined.extend(fused);
            combined.extend(neighbor.children);

            Ok((
                regroup_children::<Key, Value, D>(combined, height, manifest)?,
                novelty,
            ))
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
                novelty: Novelty::new(),
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
                let novelty = index.novelty.take_all::<Key>()?;
                let child = index.children.pop().expect("single child present");
                let Node::Transient(mut child) = child else {
                    unreachable!("matched transient index above");
                };
                // The stripped wrapper's buffer is pending against exactly
                // the subtree the surviving child roots (they cover the same
                // range), so it moves onto the child, each op routed to its
                // link. The wrapper sat one level shallower, so its ops are
                // the newer: the routed merge appends them after the child's
                // own for equal keys, keeping the newest op for a key last,
                // the position last-op-wins resolution reads.
                if !novelty.is_empty() {
                    let child_index = child.as_index_mut()?;
                    let bounds = link_bounds(&child_index.children)?;
                    child_index.novelty.route::<Key>(&bounds, novelty)?;
                }
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

/// The outcome of trimming a carved subtree to its piece's range.
enum Trim {
    /// Nothing within the range remained; the subtree contributes no entries.
    Empty,
    /// Every entry already lay within the range; the subtree is byte-for-byte
    /// the original.
    Unchanged,
    /// Entries outside the range were removed from the boundary spines.
    Trimmed,
}

/// Carves out of the persisted tree rooted at `root` the subtree holding the
/// entries within `range`, loading only the two boundary spines.
///
/// The carved subtree keeps the source's height and stays canonical without
/// any re-cutting: the cut rule is per key, so removing a prefix or a suffix
/// of a canonically cut node leaves every surviving cut in place, and the
/// open-ended edge nodes it creates are exactly the leftmost/rightmost nodes
/// of the piece where open runs are canonical. Children strictly inside the
/// range remain [`Node::Persistent`] links and are never loaded. Returns the
/// carved root, its height, and the trim outcome, or `None` when the tree is
/// empty or no entry falls within the range.
async fn carve<Key, Value, Backend>(
    root: Blake3Hash,
    range: &RangeInclusive<Key>,
    accessor: &Accessor<Backend>,
) -> Result<Option<(TransientNode<Key, Value>, Rank, Trim)>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    if &root == NULL_BLAKE3_HASH {
        return Ok(None);
    }
    let node: PersistentNode<Key, Value> = accessor.get_node(&root).await?;
    // The carved root stands at the source tree's left edge for the purposes
    // of this carve, so it opens with the empty separator (negative infinity),
    // exactly as `load` opens a tree root.
    let mut root = TransientNode::open(&node, Vec::new())?;

    // Lift the spine covering each range bound so the trim below can edit the
    // boundary nodes in place. The two descents share a prefix until they
    // diverge; every child off these spines stays a persistent link.
    let mut height: Rank = 0;
    for bound in [range.start(), range.end()] {
        let mut path = Vec::new();
        loop {
            let node = follow(&mut root, &path)?;
            match node {
                TransientNode::Index(index) => {
                    let at = child_for::<Key, Value>(&index.children, bound)?;
                    lift(&mut index.children[at], accessor).await?;
                    path.push(at);
                }
                TransientNode::Segment(_) => break,
            }
        }
        height = path.len() as Rank;
    }

    match trim_to_range(&mut root, range, true, true)? {
        Trim::Empty => Ok(None),
        trim => Ok(Some((root, height, trim))),
    }
}

/// Trims `node` in place to the entries within `range`, descending only the
/// boundary spines that [`carve`] lifted.
///
/// `trim_start` and `trim_end` say which bounds can still cut into this
/// subtree; they follow the start and end boundary spines respectively, and
/// both hold only while the two spines coincide. Children wholly outside the
/// range are dropped, children wholly inside are kept untouched (persistent
/// links included), and the at most two straddling children are trimmed
/// recursively. An emptied node reports [`Trim::Empty`] so its parent removes
/// it in turn.
fn trim_to_range<Key, Value>(
    node: &mut TransientNode<Key, Value>,
    range: &RangeInclusive<Key>,
    trim_start: bool,
    trim_end: bool,
) -> Result<Trim, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    match node {
        TransientNode::Segment(segment) => {
            let before = segment.entries.len();
            segment.entries.retain(|entry| {
                (!trim_start || &entry.key >= range.start())
                    && (!trim_end || &entry.key <= range.end())
            });
            Ok(if segment.entries.is_empty() {
                Trim::Empty
            } else if segment.entries.len() == before {
                Trim::Unchanged
            } else {
                Trim::Trimmed
            })
        }
        TransientNode::Index(index) => {
            // A node's buffer is part of its content, so a trim has to cut it to
            // the range the way it cuts a segment's entries. Ops outside the
            // range belong to the pieces being carved away; ops inside must
            // survive, or the carved piece silently loses every write still
            // pending against it. The per-link grouping is positional against
            // the child list this trim is about to shorten, so take the ops
            // flat first and route the survivors back over whatever children
            // remain at the end.
            let buffered = index.novelty.take_all::<Key>()?;
            let buffered_before = buffered.len();
            let buffered: Vec<NoveltyEntry<Value>> = buffered
                .into_iter()
                .filter(|entry| {
                    (!trim_start || entry.key.as_slice() >= range.start().as_ref())
                        && (!trim_end || entry.key.as_slice() <= range.end().as_ref())
                })
                .collect();
            let buffered_trimmed = buffered.len() != buffered_before;

            let children = &mut index.children;
            let mut changed = buffered_trimmed;

            // Index children carry separators, not full keys: under the
            // lower-bound convention `children[i].separator()` is the smallest
            // key reachable in child `i`, so child `i` spans
            // `[sep(i), sep(i+1))` and the last child runs to `+infinity`.
            // Both bounds are therefore tested against separators, never
            // against an upper bound (an index node has none to give).
            if trim_end {
                // Child `i` lies wholly beyond the range end iff its own
                // lower bound already exceeds it. Keep every child before the
                // first such one.
                let end = range.end().as_ref();
                let mut keep = children.len();
                for (at, child) in children.iter().enumerate() {
                    if child.separator()? > end {
                        keep = at;
                        break;
                    }
                }
                // The first child always stays: its separator is the node's
                // own left edge, which cannot exceed a range the node
                // straddles.
                let keep = keep.max(1);
                if keep < children.len() {
                    children.truncate(keep);
                    changed = true;
                }
            }
            if trim_start {
                // Child `i` lies wholly before the range start iff the NEXT
                // child's lower bound is still at or below it (so nothing in
                // child `i` can reach the start). The last child has no
                // successor and always stays.
                let start = range.start().as_ref();
                let mut below = 0;
                while below + 1 < children.len() && children[below + 1].separator()? <= start {
                    below += 1;
                }
                if below > 0 {
                    children.drain(..below);
                    changed = true;
                }
            }
            if children.is_empty() {
                return Ok(Trim::Empty);
            }

            // Recurse into the children that may straddle a bound: the last
            // kept child for the end bound, the first for the start bound, or
            // one child for both when a single child remains. These are
            // exactly the children the carve descent lifted.
            let both = trim_start && trim_end && children.len() == 1;
            if trim_end {
                let last = children.len() - 1;
                match trim_to_range(lifted_child(&mut children[last])?, range, both, true)? {
                    Trim::Empty => {
                        children.remove(last);
                        changed = true;
                    }
                    Trim::Trimmed => changed = true,
                    Trim::Unchanged => {}
                }
            }
            if trim_start && !both && !children.is_empty() {
                match trim_to_range(lifted_child(&mut children[0])?, range, true, false)? {
                    Trim::Empty => {
                        children.remove(0);
                        changed = true;
                    }
                    Trim::Trimmed => changed = true,
                    Trim::Unchanged => {}
                }
            }
            if children.is_empty() {
                return Ok(Trim::Empty);
            }

            // Route the surviving ops over the trimmed child list. Every
            // surviving key is within the range, and the surviving children
            // cover the whole range (below-all keys clamp into the new
            // leftmost child, the new last child runs open-ended), so no op
            // is stranded.
            let bounds = link_bounds(children)?;
            index.novelty.route::<Key>(&bounds, buffered)?;

            Ok(if changed {
                Trim::Trimmed
            } else {
                Trim::Unchanged
            })
        }
    }
}

/// Resets the leftmost seam of a stitch's FIRST part to the empty separator,
/// walking first children down to the leftmost segment (index levels derive
/// their left edge from it, so nothing else needs touching). The carve lifts
/// the whole start-bound spine, so on a start-trimmed part the walk stays on
/// transient nodes all the way down; a persistent leftmost child can only
/// mean the part kept its source's own left edge, whose stored separators
/// are already the canonical empty string, so the walk stops there.
fn refloor_leftmost<Key, Value>(
    part: &mut TransientNode<Key, Value>,
) -> Result<(), DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
{
    let mut node = part;
    loop {
        match node {
            TransientNode::Segment(segment) => {
                segment.separator = Vec::new();
                return Ok(());
            }
            TransientNode::Index(index) => {
                let first = index.children.first_mut().ok_or_else(|| {
                    DialogSearchTreeError::Node("Index was unexpectedly empty".into())
                })?;
                match first {
                    Node::Transient(child) => node = child,
                    Node::Persistent(_) => return Ok(()),
                }
            }
        }
    }
}

/// Unwraps a child on a carve boundary spine to its lifted transient form.
/// The carve descent lifted every child it routed through, so a persistent
/// child here is an invariant violation, not a normal case.
fn lifted_child<Key, Value>(
    child: &mut Node<Key, Value>,
) -> Result<&mut TransientNode<Key, Value>, DialogSearchTreeError> {
    match child {
        Node::Transient(node) => Ok(node),
        Node::Persistent(_) => Err(DialogSearchTreeError::Node(
            "Range trim descended into a child that was not lifted".into(),
        )),
    }
}

/// Lifts an edge spine of a stitched part to transient form: the leftmost
/// spine when `leftmost`, otherwise the rightmost. A join re-cuts exactly
/// these spines, so they are the only nodes it needs loaded; nodes that are
/// already transient cost nothing.
async fn lift_boundary_spine<Key, Value, Backend>(
    root: &mut TransientNode<Key, Value>,
    leftmost: bool,
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
    let mut path = Vec::new();
    loop {
        let node = follow(root, &path)?;
        match node {
            TransientNode::Index(index) => {
                if index.children.is_empty() {
                    return Err(DialogSearchTreeError::Node(
                        "Stitched index was unexpectedly empty".into(),
                    ));
                }
                let at = if leftmost {
                    0
                } else {
                    index.children.len() - 1
                };
                lift(&mut index.children[at], accessor).await?;
                path.push(at);
            }
            TransientNode::Segment(_) => break,
        }
    }
    Ok(())
}

/// Levels a stitched part up to height `to` by wrapping it in single-child
/// index nodes. The wrappers exist purely to align two parts for a join: the
/// join pops each one apart again at its level, so no wrapper node survives
/// into the result.
fn raise<Key, Value>(
    node: TransientNode<Key, Value>,
    from: Rank,
    to: Rank,
) -> TransientNode<Key, Value> {
    (from..to).fold(node, |node, _| {
        TransientNode::Index(TransientIndex {
            children: vec![node.into()],
            novelty: Novelty::new(),
        })
    })
}

/// Joins two adjacent, equal-height subtrees (every key of `left` sorts
/// before every key of `right`) into the canonical run of nodes covering
/// both, re-cutting only the seam.
///
/// Mirrors [`fuse_subtrees`]: recurse down `left`'s rightmost spine and
/// `right`'s leftmost spine in lock-step (both lifted by the caller), join the
/// facing leaves by concatenating their entries, then on the way up splice
/// each joined run between the remaining children and re-cut at that level.
/// Children off the seam pass through untouched, persistent links included,
/// because a canonical cut depends only on each child's own upper-bound rank:
/// cuts away from the seam cannot move.
///
/// Returns the joined run together with the buffered ops lifted off the nodes
/// the join dismantled. Regrouping discards the index nodes that held those
/// buffers, so the caller must re-attach them to a node covering the run; they
/// are pending writes, and dropping them loses data.
/// A joined run of nodes together with the buffered ops lifted off the nodes
/// the join dismantled. The ops are pending writes with no home until the
/// caller re-attaches them to a node covering the run.
type JoinedRun<Key, Value> = (Vec<Node<Key, Value>>, Vec<NoveltyEntry<Value>>);

fn concat_levels<Key, Value, D>(
    left: TransientNode<Key, Value>,
    right: TransientNode<Key, Value>,
    height: Rank,
    manifest: &Manifest,
) -> Result<JoinedRun<Key, Value>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    match (left, right) {
        (TransientNode::Segment(mut left), TransientNode::Segment(right)) => {
            // Leaves buffer nothing.
            let floor = left.separator.clone();
            left.entries.extend(right.entries);
            Ok((
                regroup_entries::<Key, Value, D>(left.entries, floor, manifest),
                Vec::new(),
            ))
        }
        (TransientNode::Index(mut left), TransientNode::Index(mut right)) => {
            let left_last = left
                .children
                .pop()
                .ok_or_else(|| {
                    DialogSearchTreeError::Node("Stitched left index had no children".into())
                })?
                .into_transient()?;
            let right_first = remove_first(&mut right.children)?.into_transient()?;

            let (seam, seam_novelty) =
                concat_levels::<Key, Value, D>(left_last, right_first, height - 1, manifest)?;

            // Both joined nodes' buffers are pending against the run being
            // built, and regrouping discards the nodes that held them. Hand
            // them back so the caller re-attaches them to a node that covers
            // the run; dropping them here loses every write still buffered at
            // a seam. Deepest first, as in `fuse_subtrees`: the seam ops come
            // from one level further down and are older than the two joined
            // nodes' own, so the stable sort here and at every re-attachment
            // keeps the newest op for a key last. The two joined nodes cover
            // disjoint ranges, so their relative order carries no precedence.
            let mut novelty = seam_novelty;
            novelty.extend(left.novelty.take_all::<Key>()?);
            novelty.extend(right.novelty.take_all::<Key>()?);
            novelty.sort_by(|a, b| a.key.cmp(&b.key));

            let mut combined = left.children;
            combined.extend(seam);
            combined.extend(right.children);

            Ok((
                regroup_children::<Key, Value, D>(combined, height, manifest)?,
                novelty,
            ))
        }
        _ => Err(DialogSearchTreeError::Node(
            "Stitched subtrees had mismatched heights".into(),
        )),
    }
}

/// Whether applying `edit` to this already-canonical segment would leave it
/// canonical, decided without mutating or cloning.
///
/// A canonical leaf has no interior cut: no interior entry both holds a
/// high coin AND has an un-vetoed seam to its successor (an interior
/// high-rank entry may exist when the seam to its neighbor is vetoed). The
/// input segment is canonical (an invariant maintained by every edit), so
/// only the local effect of `edit` needs checking:
///
///   - Upsert of a key that already exists only replaces a value: both the
///     coin and the veto read keys alone, so shape is untouched.
///   - Upsert of a high-rank key may split the segment: not fast. (Under the
///     veto it splits only when the successor seam survives; falling through
///     to the re-shape either way is correct, just unhurried, and keeps this
///     check successor-free at the window's right edge.)
///   - Upsert of a low-rank key cannot change a neighbor's cut either: the
///     inserted key re-partners the seam after its predecessor, but a key
///     between two keys shares at least their common prefix, so a seam the
///     veto rejected stays rejected against the newcomer (the downward
///     closure on [`Distribution::vetoes`]) — no re-check needed. Not fast
///     only when appended after a high-rank last entry (the boundary would
///     be left interior; the orphan-append machinery owns that case).
///   - Remove of a present key is fine unless it empties the segment (which
///     must remove the segment from its parent) or joins its two neighbors
///     into an un-vetoed seam: the predecessor may hold a high coin vetoed
///     against the removed key, and the successor it now faces may diverge
///     early, so a cut appears. Removing a boundary last entry is handled
///     earlier as a fusing boundary delete.
///   - Under the backstop (`max_segment` non-zero), any membership change
///     inside a vetoed stretch may move the stretch's forced anchors, so
///     inserts and deletes whose adjacent seam is vetoed always re-shape.
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
                return false; // inserting a high coin may split the segment
            }
            let at = found.unwrap_err();
            // Appending after a boundary last entry would leave it interior.
            let appends_last = at == entries.len();
            let last_is_boundary = entries
                .last()
                .map(|e| D::rank(e.key.as_ref(), manifest) > BOTTOM_RANK)
                .unwrap_or(false);
            if appends_last && last_is_boundary {
                return false;
            }
            // Under the backstop, joining a vetoed stretch changes the
            // stretch's weight, and its forced anchors are a function of
            // the whole stretch: re-shape so the regroup re-derives them.
            // The check fails cheaply (a length test) for every ordinary
            // key; by downward closure one adjacent seam suffices to
            // detect membership.
            if manifest.max_segment > 0 {
                let joins_stretch = (at > 0
                    && D::vetoes(entries[at - 1].key.as_ref(), entry.key.as_ref(), manifest))
                    || (at < entries.len()
                        && D::vetoes(entry.key.as_ref(), entries[at].key.as_ref(), manifest));
                if joins_stretch {
                    return false;
                }
            }
            true
        }
        Edit::Delete(key) => match entries.binary_search_by(|e| e.key.cmp(key)) {
            Ok(at) => {
                // Removing the only entry empties the segment: not fast.
                if entries.len() <= 1 {
                    return false;
                }
                // Removing an interior entry joins its neighbors' seams: a
                // high-rank predecessor whose cut the removed key's seam
                // vetoed may now face an early-diverging successor, so a
                // cut appears. (The joined seam can only be MORE
                // distinguishable than either removed seam, so an existing
                // cut never dissolves this way.) The old-seam veto test runs
                // first: it fails cheaply everywhere outside near-duplicate
                // stretches, so the predecessor's rank hash is paid only at
                // a genuine un-veto joint.
                if at > 0 && at + 1 < entries.len() {
                    let predecessor = entries[at - 1].key.as_ref();
                    if D::vetoes(predecessor, entries[at].key.as_ref(), manifest)
                        && !D::vetoes(predecessor, entries[at + 1].key.as_ref(), manifest)
                        && D::rank(predecessor, manifest) > BOTTOM_RANK
                    {
                        return false;
                    }
                }
                // Under the backstop, removing a vetoed stretch's member
                // changes the stretch's weight and may move its forced
                // anchors: re-shape so the regroup re-derives them.
                if manifest.max_segment > 0 {
                    let leaves_stretch = (at > 0
                        && D::vetoes(
                            entries[at - 1].key.as_ref(),
                            entries[at].key.as_ref(),
                            manifest,
                        ))
                        || (at + 1 < entries.len()
                            && D::vetoes(
                                entries[at].key.as_ref(),
                                entries[at + 1].key.as_ref(),
                                manifest,
                            ));
                    if leaves_stretch {
                        return false;
                    }
                }
                true
            }
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
///
/// Children whose key span cannot intersect `bounds` are pruned: a child's
/// span is `[its separator, the next sibling's separator)` under lower-bound
/// routing, so a subtree entirely below the range start or at/above the range
/// end contributes nothing and costs neither a clone (transient) nor a root
/// fetch and descent (persistent). A sibling whose separator cannot be read
/// is treated as unbounded: pruning degrades, correctness does not.
fn collect_stream_plan<Key, Value>(
    node: &TransientNode<Key, Value>,
    bounds: &(Bound<Key>, Bound<Key>),
    plan: &mut Vec<StreamStep<Key, Value>>,
) where
    Key: self::Key,
    Value: Clone,
{
    // Whether a child spanning `[span_start, span_end)` (separator bytes;
    // `None` when unknown) could hold a key inside `bounds`.
    let intersects = |span_start: Option<&[u8]>, span_end: Option<&[u8]>| {
        let below = match (&bounds.0, span_end) {
            // Keys are < span_end <= start: every key sorts below the range.
            (Bound::Included(start) | Bound::Excluded(start), Some(end)) => end <= start.as_ref(),
            _ => false,
        };
        let above = match (&bounds.1, span_start) {
            // Keys are >= span_start > end (or >= end when exclusive).
            (Bound::Included(end), Some(start)) => start > end.as_ref(),
            (Bound::Excluded(end), Some(start)) => start >= end.as_ref(),
            _ => false,
        };
        !below && !above
    };

    match node {
        TransientNode::Index(index) => {
            for (at, child) in index.children.iter().enumerate() {
                let span_start = child.separator().ok();
                let span_end = index
                    .children
                    .get(at + 1)
                    .and_then(|sibling| sibling.separator().ok());
                if !intersects(span_start, span_end) {
                    continue;
                }
                match child {
                    Node::Persistent(link) => {
                        plan.push(StreamStep::Persistent(link.node.clone()));
                    }
                    Node::Transient(child) => collect_stream_plan(child, bounds, plan),
                }
            }
        }
        TransientNode::Segment(segment) => {
            for entry in &segment.entries {
                if bounds.contains(&entry.key) {
                    plan.push(StreamStep::Entry(entry.clone()));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use std::collections::HashSet;

    use crate::{Distribution, Geometric, Manifest};
    use anyhow::Result;
    use dialog_common::{Blake3Hash, NULL_BLAKE3_HASH};
    use dialog_storage::MemoryStorageBackend;

    use crate::{
        Accessor, Buffer, Cache, ContentAddressedStorage, Delta, Entry, PersistentNode,
        PersistentTree, Piece, Rank, TransientTree, distribution,
    };

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
            let mut sequential = TestTree::empty();
            let mut delta = Delta::zero();
            for &(is_insert, key) in &ops {
                sequential = if is_insert {
                    sequential
                        .edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    sequential
                        .edit()
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
                sequential.root(),
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

    /// A bounded stream over a lightly-edited tree reads only blocks on the
    /// range's own path: persistent siblings whose separator spans cannot
    /// intersect the bounds are pruned from the stream plan. Regression
    /// guard — the plan once listed EVERY persistent subtree, so a small
    /// interior range over a tree edited at both ends paid a root fetch and
    /// descent per untouched subtree.
    #[dialog_common::test]
    async fn it_prunes_out_of_range_subtrees_from_bounded_streams() -> Result<()> {
        use crate::helpers::test_storage;
        use futures_util::StreamExt as _;

        fn make_key(i: u32) -> [u8; 8] {
            let mut key = [0u8; 8];
            key[..4].copy_from_slice(&i.to_be_bytes());
            key
        }

        let mut storage = test_storage();
        let mut tree = PersistentTree::<[u8; 8], Vec<u8>>::empty();
        let mut delta = Delta::zero();
        for i in 0..5_000u32 {
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

        // Reopen cold (fresh node cache) so every touched block is a real
        // read, then edit both ends WITHOUT persisting: the spine is
        // transient at the edges and the middle stays persistent.
        let tree = PersistentTree::<[u8; 8], Vec<u8>>::from_hash(tree.root().clone());
        let edit = tree
            .edit()
            .insert(make_key(0), vec![9], &storage)
            .await?
            .insert(make_key(4_999), vec![9], &storage)
            .await?;

        storage.backend().clear_journal();
        let stream = edit.stream_range(make_key(2_400)..make_key(2_420), &storage);
        futures_util::pin_mut!(stream);
        let mut yielded = 0usize;
        while let Some(entry) = stream.next().await {
            entry?;
            yielded += 1;
        }
        assert_eq!(yielded, 20, "the bounded stream yields exactly the range");
        let reads = storage.backend().get_reads().len();
        assert!(
            reads <= 2,
            "reads stay proportional to the range's path, got {reads}"
        );
        Ok(())
    }

    /// A band of near-duplicate keys — every adjacent pair agreeing past
    /// `max_separator`, so every seam between them is vetoed — can never cut
    /// a boundary among themselves, whatever the coin says: edits must still
    /// terminate, produce one open segment, read back completely, and
    /// converge with a from-scratch rebuild after deletes. This is the veto
    /// analogue of the retired demotion band (where LENGTH alone glued keys
    /// together); the keys here differ only in their trailing bytes, past
    /// the separator bound.
    #[dialog_common::test]
    async fn it_handles_a_band_of_fully_vetoed_seams() -> Result<()> {
        const OVERSIZED: usize = 600; // above the default max_separator of 512

        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        type BandTree = PersistentTree<[u8; OVERSIZED], Vec<u8>>;

        // The counter sits at the TAIL: adjacent keys share the 596-byte
        // prefix, far beyond `max_separator = 512`, so their shortest
        // separator exceeds the bound and the veto rejects every seam.
        let make_key = |i: u32| {
            let mut key = [0xAAu8; OVERSIZED];
            key[OVERSIZED - 4..].copy_from_slice(&i.to_be_bytes());
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
            "the fully vetoed band converges under deletes"
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
        byte_boundaries.sort_by_key(|boundary| boundary.1);
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

    /// Keys longer than `max_separator` are ranked by the coin like any
    /// other key — the retired length guard demoted them all to rank 0,
    /// which is exactly what glued long-key runs into unbounded segments.
    /// These keys diverge in their leading bytes, so every seam between
    /// them survives the veto and the run splits wherever the coin says;
    /// the tree must round-trip every entry.
    ///
    /// This is the case the fixed-width key tests cannot reach, and the one
    /// the history region is closest to: a history key carries a 40-byte
    /// version prefix on top of entity + attribute + value, so it crosses the
    /// separator bound sooner than the fact key for the same fact.
    #[dialog_common::test]
    async fn it_round_trips_keys_longer_than_the_separator_bound() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = Manifest::default();

        // Keys comfortably above `max_separator`; each is ranked by its
        // hash alone, pinning that the length guard is gone.
        let width = manifest.max_separator as usize + 64;
        let mut keys: Vec<VarKey> = Vec::new();
        for n in 0..64u32 {
            let mut bytes = format!("{n:08}").into_bytes();
            bytes.resize(width, b'x');
            assert_eq!(
                <Geometric as Distribution>::rank(&bytes, &manifest),
                var_rank(&bytes),
                "an oversized key is ranked by the coin alone; the separator \
                 bound is enforced per seam by the veto, not by demotion"
            );
            keys.push(VarKey(bytes));
        }

        let mut tree = VarTree::empty();
        let mut delta = Delta::zero();
        for key in &keys {
            tree = tree
                .edit()
                .insert(key.clone(), key.0.clone(), &storage)
                .await?
                .persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();
        }

        for key in &keys {
            let found = tree.get(key, &storage).await?;
            assert_eq!(
                found.as_deref(),
                Some(key.0.as_slice()),
                "every oversized key must still be readable"
            );
        }
        Ok(())
    }

    /// The manifest the veto and weight-coin convergence tests run under: a
    /// separator bound small enough that every in-cluster seam below is
    /// vetoed, and a `max_segment` target small enough that the weight coin
    /// paces the short keys visibly (a few dozen cluster keys still exceed
    /// it, which is what the step-3 backstop is for).
    fn capped_manifest() -> Manifest {
        Manifest {
            fanout_n: 4,
            max_separator: 24,
            max_segment: 512,
            ..Manifest::default()
        }
    }

    /// A key set exercising the veto and the segment cap: three
    /// near-duplicate clusters (each key 34 bytes sharing a 30-byte cluster
    /// prefix, past the 24-byte separator bound, so every in-cluster seam is
    /// vetoed AND trips the forced-candidate rule) plus short keys under the
    /// natural coin.
    fn capped_keys() -> Vec<VarKey> {
        let mut keys = Vec::new();
        for cluster in 0..3u8 {
            for n in 0..60u32 {
                let mut bytes = vec![b'L', b'0' + cluster];
                bytes.extend(vec![b'p'; 28]);
                bytes.extend(format!("{n:04}").into_bytes());
                keys.push(VarKey(bytes));
            }
        }
        for n in 0..40u32 {
            keys.push(VarKey(format!("s{n:03}").into_bytes()));
        }
        keys
    }

    /// Builds a tree by inserting `keys` one at a time under `manifest`,
    /// persisting and flushing after every edit: the incremental path whose
    /// convergence the capped-run gates pin.
    async fn build_incremental(
        keys: &[VarKey],
        manifest: Manifest,
        storage: &mut TestStorage,
    ) -> Result<VarTree> {
        let mut tree: Option<VarTree> = None;
        let mut delta = Delta::zero();
        for key in keys {
            let transient = match &tree {
                None => {
                    TransientTree::with_manifest(NULL_BLAKE3_HASH.clone(), Cache::new(), manifest)
                }
                Some(tree) => tree.edit_with_manifest(storage).await?,
            };
            let next = transient
                .insert(key.clone(), key.0.clone(), storage)
                .await?
                .persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();
            tree = Some(next);
        }
        tree.ok_or_else(|| anyhow::anyhow!("build_incremental needs at least one key"))
    }

    /// Deletes `keys` one at a time from `tree`, persisting and flushing
    /// after every edit.
    async fn delete_incremental(
        mut tree: VarTree,
        keys: &[VarKey],
        storage: &mut TestStorage,
    ) -> Result<VarTree> {
        let mut delta = Delta::zero();
        for key in keys {
            tree = tree
                .edit_with_manifest(storage)
                .await?
                .delete(key, storage)
                .await?
                .persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();
        }
        Ok(tree)
    }

    /// Gate 1 of the boundary-policy experiment under a non-zero
    /// `max_segment` (the weight-paced leaf coin): the coin is a pure
    /// per-key function, so builds that insert the same keys in different
    /// orders, persisting after every edit, must converge on byte-identical
    /// roots — with no window machinery at all, unlike the retired hard
    /// cap, whose run-scoped decisions needed the edit path to merge whole
    /// runs to stay convergent.
    #[dialog_common::test]
    async fn it_converges_on_weight_paced_trees_across_insertion_orders() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        let sorted = {
            let mut keys = capped_keys();
            keys.sort();
            keys
        };
        let reversed: Vec<VarKey> = sorted.iter().rev().cloned().collect();
        // A deterministic "shuffle": order by each key's blake3 hash, which
        // interleaves clusters and short keys thoroughly.
        let hashed = {
            let mut keys = sorted.clone();
            keys.sort_by_key(|key| *Blake3Hash::hash(&key.0).as_bytes());
            keys
        };

        let a = build_incremental(&sorted, manifest, &mut storage).await?;
        let b = build_incremental(&reversed, manifest, &mut storage).await?;
        let c = build_incremental(&hashed, manifest, &mut storage).await?;

        assert_eq!(
            a.root(),
            b.root(),
            "sorted and reverse insertion orders must converge"
        );
        assert_eq!(
            a.root(),
            c.root(),
            "sorted and hash-shuffled insertion orders must converge"
        );

        for key in &sorted {
            assert_eq!(
                a.get(key, &storage).await?.as_deref(),
                Some(key.0.as_slice()),
                "every key must be readable from the capped tree"
            );
        }

        // The weight coin must actually govern the shape: the same keys
        // under `max_segment: 0` (the geometric coin) group differently.
        // Compared as BOUNDARY SETS, not roots — the manifest is stamped
        // into every node, so root hashes differ whenever the manifest
        // does, coin switched or not.
        let uncapped = Manifest {
            max_segment: 0,
            ..manifest
        };
        let plain = build_incremental(&sorted, uncapped, &mut storage).await?;
        assert_ne!(
            leaf_boundaries(a.root(), &storage).await?,
            leaf_boundaries(plain.root(), &storage).await?,
            "the weight-paced build must differ from the geometric one, or the coin never switched"
        );

        Ok(())
    }

    /// Gate 1's delete oracle under the weight coin: deleting keys one at a
    /// time (including a whole vetoed cluster, so runs shrink, merge and
    /// dissolve) must land on the same bytes as building the surviving key
    /// set from scratch.
    #[dialog_common::test]
    async fn it_matches_rebuild_after_deletes_under_the_weight_coin() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        let sorted = {
            let mut keys = capped_keys();
            keys.sort();
            keys
        };

        // Delete every 7th key plus the whole middle cluster.
        let doomed: Vec<VarKey> = sorted
            .iter()
            .enumerate()
            .filter(|(at, key)| at % 7 == 0 || key.0.starts_with(b"L1"))
            .map(|(_, key)| key.clone())
            .collect();
        let survivors: Vec<VarKey> = sorted
            .iter()
            .filter(|key| !doomed.contains(key))
            .cloned()
            .collect();
        assert!(!doomed.is_empty() && !survivors.is_empty());

        let full = build_incremental(&sorted, manifest, &mut storage).await?;
        let pruned = delete_incremental(full, &doomed, &mut storage).await?;
        let rebuilt = build_incremental(&survivors, manifest, &mut storage).await?;

        assert_eq!(
            pruned.root(),
            rebuilt.root(),
            "incremental deletes must converge on the fresh build of the survivors"
        );

        for key in &survivors {
            assert_eq!(
                pruned.get(key, &storage).await?.as_deref(),
                Some(key.0.as_slice()),
                "survivor must remain readable"
            );
        }
        for key in doomed.iter().take(5) {
            assert_eq!(
                pruned.get(key, &storage).await?,
                None,
                "deleted key must be gone"
            );
        }

        Ok(())
    }

    /// The last key of every persisted leaf segment except the global last —
    /// the set of leaf seam boundaries — collected left to right by walking
    /// the stored nodes. The shape probe the veto tests read.
    async fn leaf_boundaries(
        root: &Blake3Hash,
        storage: &ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut boundaries: Vec<Vec<u8>> = Vec::new();
        if root == NULL_BLAKE3_HASH {
            return Ok(boundaries);
        }
        let accessor = Accessor::new(Cache::new(), storage.clone());
        let mut frontier = vec![root.clone()];
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for hash in &frontier {
                let node: PersistentNode<VarKey, Vec<u8>> = accessor.get_node(hash).await?;
                if let Ok(index) = node.as_index() {
                    for at in 0..index.len() {
                        next.push(index.hash_at(at)?.clone());
                    }
                } else {
                    boundaries.push(node.as_segment()?.last_key::<VarKey>()?);
                }
            }
            frontier = next;
        }
        // The global last key terminates the tree, not a seam.
        boundaries.pop();
        Ok(boundaries)
    }

    /// The veto's own convergence gate (no segment cap): every in-cluster
    /// seam of the near-duplicate clusters is vetoed, so a cluster rides in
    /// one segment whatever the coin says about its keys, and insertion
    /// order must not leak into the shape. Also pins that the veto is what
    /// governs it: the same keys under a bound wide enough to accept every
    /// seam shape differently.
    #[dialog_common::test]
    async fn it_converges_on_vetoed_clusters_across_insertion_orders() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = Manifest {
            max_segment: 0,
            ..capped_manifest()
        };

        let sorted = {
            let mut keys = capped_keys();
            keys.sort();
            keys
        };
        let reversed: Vec<VarKey> = sorted.iter().rev().cloned().collect();
        let hashed = {
            let mut keys = sorted.clone();
            keys.sort_by_key(|key| *Blake3Hash::hash(&key.0).as_bytes());
            keys
        };

        let a = build_incremental(&sorted, manifest, &mut storage).await?;
        let b = build_incremental(&reversed, manifest, &mut storage).await?;
        let c = build_incremental(&hashed, manifest, &mut storage).await?;

        assert_eq!(
            a.root(),
            b.root(),
            "sorted and reverse insertion orders must converge under the veto"
        );
        assert_eq!(
            a.root(),
            c.root(),
            "sorted and hash-shuffled insertion orders must converge under the veto"
        );

        for key in &sorted {
            assert_eq!(
                a.get(key, &storage).await?.as_deref(),
                Some(key.0.as_slice()),
                "every key must be readable from the vetoed tree"
            );
        }

        // No seam inside a cluster survives: a boundary carrying a cluster
        // prefix may only be the cluster's own last key (its seam partner is
        // the next cluster or a short key, diverging in byte 1).
        let boundaries = leaf_boundaries(a.root(), &storage).await?;
        for boundary in &boundaries {
            if boundary.starts_with(b"L") {
                let cluster_max = sorted
                    .iter()
                    .filter(|key| key.0.starts_with(&boundary[..2]))
                    .max()
                    .expect("cluster keys exist");
                assert_eq!(
                    boundary, &cluster_max.0,
                    "an in-cluster seam escaped the veto"
                );
            }
        }

        // The veto must be what shaped the tree: with the bound widened past
        // every in-cluster separator, the coin's cuts inside the clusters
        // are accepted and the grouping differs. Compared as BOUNDARY SETS,
        // not roots — the manifest is stamped into every node, so root
        // hashes differ whenever the manifest does, veto fired or not.
        let unvetoed = Manifest {
            max_separator: 512,
            ..manifest
        };
        let wide = build_incremental(&sorted, unvetoed, &mut storage).await?;
        assert_ne!(
            leaf_boundaries(a.root(), &storage).await?,
            leaf_boundaries(wide.root(), &storage).await?,
            "the vetoed build must differ from the wide-bound one, or the veto never fired"
        );

        Ok(())
    }

    /// The veto's delete oracle (no segment cap): deleting keys one at a
    /// time — including a whole cluster, so vetoed stretches join across
    /// the vanished keys and formerly vetoed seams re-partner into accepted
    /// ones — must land on the same bytes as building the survivors from
    /// scratch.
    #[dialog_common::test]
    async fn it_matches_rebuild_after_deletes_in_vetoed_clusters() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = Manifest {
            max_segment: 0,
            ..capped_manifest()
        };

        let sorted = {
            let mut keys = capped_keys();
            keys.sort();
            keys
        };
        let doomed: Vec<VarKey> = sorted
            .iter()
            .enumerate()
            .filter(|(at, key)| at % 7 == 0 || key.0.starts_with(b"L1"))
            .map(|(_, key)| key.clone())
            .collect();
        let survivors: Vec<VarKey> = sorted
            .iter()
            .filter(|key| !doomed.contains(key))
            .cloned()
            .collect();
        assert!(!doomed.is_empty() && !survivors.is_empty());

        let full = build_incremental(&sorted, manifest, &mut storage).await?;
        let pruned = delete_incremental(full, &doomed, &mut storage).await?;
        let rebuilt = build_incremental(&survivors, manifest, &mut storage).await?;

        assert_eq!(
            pruned.root(),
            rebuilt.root(),
            "incremental deletes must converge on the fresh build of the survivors"
        );

        Ok(())
    }

    /// The leaf pieces of a persisted tree, left to right: each segment's
    /// separator length (read from its parent link, where separators live
    /// in stored form) and first key. Piece heads whose separator exceeds
    /// `max_separator` are backstop anchors (forced seams are
    /// self-identifying by their long separators).
    async fn leaf_piece_heads(
        root: &Blake3Hash,
        storage: &ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    ) -> Result<Vec<(usize, Vec<u8>)>> {
        let mut pieces: Vec<(usize, Vec<u8>)> = Vec::new();
        if root == NULL_BLAKE3_HASH {
            return Ok(pieces);
        }
        let accessor = Accessor::new(Cache::new(), storage.clone());
        let mut frontier: Vec<(Blake3Hash, usize)> = vec![(root.clone(), 0)];
        while !frontier.is_empty() {
            let mut next = Vec::new();
            for (hash, separator) in &frontier {
                let node: PersistentNode<VarKey, Vec<u8>> = accessor.get_node(hash).await?;
                if let Ok(index) = node.as_index() {
                    for at in 0..index.len() {
                        next.push((index.hash_at(at)?.clone(), index.separator(at)?.len()));
                    }
                } else {
                    pieces.push((*separator, node.as_segment()?.first_key::<VarKey>()?));
                }
            }
            frontier = next;
        }
        Ok(pieces)
    }

    /// The backstop splits a fully vetoed over-target stretch at the cap
    /// scale: the cluster's pieces each weigh in under `max_segment`, every
    /// interior piece head carries the long-form forced separator (the
    /// self-identifying mark that keeps forced seams out of index levels
    /// and lets edits rejoin the stretch), and the split tree still reads
    /// back completely.
    #[dialog_common::test]
    async fn it_backstops_fully_vetoed_stretches_at_the_target() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        // One fully vetoed cluster several times the 512 target, plus two
        // short keys bracketing it.
        let cluster_key = |n: u32| {
            let mut bytes = vec![b'F'];
            bytes.extend(vec![b'r'; 29]);
            bytes.extend(format!("{n:04}").into_bytes());
            VarKey(bytes)
        };
        let mut keys: Vec<VarKey> = (0..48u32).map(cluster_key).collect();
        keys.push(VarKey(b"A000".to_vec()));
        keys.push(VarKey(b"z000".to_vec()));
        keys.sort();

        let tree = build_incremental(&keys, manifest, &mut storage).await?;
        let pieces = leaf_piece_heads(tree.root(), &storage).await?;
        let bound = manifest.max_separator as usize;
        let anchors: Vec<&(usize, Vec<u8>)> = pieces
            .iter()
            .filter(|(separator, _)| *separator > bound)
            .collect();
        assert!(
            anchors.len() >= 3,
            "an over-target vetoed stretch must be force-split, got {} anchors",
            anchors.len()
        );
        for (_, head) in &anchors {
            assert!(
                head.starts_with(b"F"),
                "forced anchors live inside the cluster"
            );
        }

        // Each cluster piece weighs in under the target: forced cuts land
        // where the recursion leaves every piece within `max_segment`.
        let cap = manifest.max_segment as usize;
        let cluster: Vec<&VarKey> = keys.iter().filter(|key| key.0.starts_with(b"F")).collect();
        let anchor_heads: Vec<&[u8]> = anchors.iter().map(|(_, head)| head.as_slice()).collect();
        let mut piece_weight = 0usize;
        for key in &cluster {
            if anchor_heads.contains(&key.0.as_slice()) {
                assert!(
                    piece_weight <= cap,
                    "a cluster piece weighed {piece_weight}, over the {cap} target"
                );
                piece_weight = 0;
            }
            piece_weight += distribution::cap::entry_weight(&key.0);
        }
        assert!(piece_weight <= cap, "the final piece must fit the target");

        for key in &keys {
            assert_eq!(
                tree.get(key, &storage).await?.as_deref(),
                Some(key.0.as_slice()),
                "every key must read back from the split cluster"
            );
        }

        Ok(())
    }

    /// The anchor flap rate: rendezvous placement keeps forced boundaries
    /// still under churn. Inserts into and deletes from a force-split
    /// cluster relocate an anchor only when the edit adds a new local
    /// hash minimum, removes an anchor key itself, or moves enough weight
    /// to add or drop a cut — never gratuitously.
    #[dialog_common::test]
    async fn it_keeps_forced_anchors_stable_under_churn() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        let cluster_key = |n: u32| {
            let mut bytes = vec![b'F'];
            bytes.extend(vec![b'r'; 29]);
            bytes.extend(format!("{n:04}").into_bytes());
            VarKey(bytes)
        };
        // Even numbers seed the cluster; odd inserts land BETWEEN existing
        // members, the interior churn that would shift positional anchors.
        let seed: Vec<VarKey> = (0..80u32).map(|n| cluster_key(2 * n)).collect();
        let mut tree = build_incremental(&seed, manifest, &mut storage).await?;

        let bound = manifest.max_separator as usize;
        let anchors = |pieces: &[(usize, Vec<u8>)]| -> HashSet<Vec<u8>> {
            pieces
                .iter()
                .filter(|(separator, _)| *separator > bound)
                .map(|(_, head)| head.clone())
                .collect()
        };
        let mut before = anchors(&leaf_piece_heads(tree.root(), &storage).await?);
        assert!(!before.is_empty(), "the seeded cluster must be force-split");

        let mut edits: Vec<(bool, VarKey)> = Vec::new();
        for n in (1..40u32).step_by(3) {
            edits.push((true, cluster_key(n)));
        }
        for n in (0..80u32).step_by(7) {
            edits.push((false, cluster_key(2 * n)));
        }

        let mut delta = Delta::zero();
        let mut flaps = 0usize;
        let mut flapping_edits = 0usize;
        let total = edits.len();
        for (insert, key) in edits {
            let transient = tree.edit_with_manifest(&storage).await?;
            let transient = if insert {
                transient
                    .insert(key.clone(), key.0.clone(), &storage)
                    .await?
            } else {
                transient.delete(&key, &storage).await?
            };
            tree = transient.persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();

            let after = anchors(&leaf_piece_heads(tree.root(), &storage).await?);
            let moved = before
                .symmetric_difference(&after)
                .filter(|anchor| anchor.as_slice() != key.0.as_slice())
                .count();
            if moved > 0 {
                flapping_edits += 1;
            }
            flaps += moved;
            before = after;
        }

        eprintln!(
            "ANCHOR-FLAP: {flaps} anchor moves across {total} edits ({flapping_edits} edits moved any)"
        );
        // Near-zero: most edits leave every pre-existing anchor in place.
        assert!(
            flapping_edits * 4 <= total,
            "anchors flapped on {flapping_edits} of {total} edits"
        );

        Ok(())
    }

    /// The capped manifest with a frame ceiling armed at `factor` times the
    /// 512-byte target, and the anchor selector chosen explicitly.
    fn ceiling_manifest(factor: u32, selector: u32) -> Manifest {
        Manifest {
            frame_ceiling_factor: factor,
            anchor_selector: selector,
            ..capped_manifest()
        }
    }

    /// The first `want` keys of the form `{prefix}{n:04}` whose bank-zero
    /// weight coin comes up tails under `manifest` — a deterministic
    /// all-tails run: every seam accepted (short keys diverge in their
    /// digits), no coin cut anywhere, the natural exponential tail's shape
    /// seeded by key choice.
    fn tails_keys(prefix: &str, want: usize, manifest: &Manifest) -> Vec<VarKey> {
        let mut keys = Vec::new();
        let mut n = 0u32;
        while keys.len() < want {
            let key = VarKey(format!("{prefix}{n:04}").into_bytes());
            if <Geometric as Distribution>::rank(&key.0, manifest) <= 1 {
                keys.push(key);
            }
            n += 1;
        }
        keys
    }

    /// The summed entry weight of each leaf, in tree order, derived by
    /// partitioning the sorted key set at the persisted leaf boundaries.
    async fn leaf_weights(
        sorted: &[VarKey],
        root: &Blake3Hash,
        storage: &ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>,
    ) -> Result<Vec<usize>> {
        let boundaries: HashSet<Vec<u8>> =
            leaf_boundaries(root, storage).await?.into_iter().collect();
        let mut weights = Vec::new();
        let mut current = 0usize;
        for key in sorted {
            current += distribution::cap::entry_weight(&key.0);
            if boundaries.contains(&key.0) {
                weights.push(current);
                current = 0;
            }
        }
        if current > 0 {
            weights.push(current);
        }
        Ok(weights)
    }

    /// Step-5 convergence gate: the frame ceiling's forced cuts are a pure
    /// function of the key set (frames are delimited by coin cuts only, and
    /// anchors are chosen by the selector over candidate accepted seams),
    /// so an all-tails run over the ceiling must build byte-identical trees
    /// across insertion orders, and incremental deletes must land on the
    /// survivors' fresh build.
    #[dialog_common::test]
    async fn it_converges_on_over_ceiling_frames_across_insertion_orders() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = ceiling_manifest(2, 0);

        let sorted = tails_keys("n", 60, &manifest);
        let reversed: Vec<VarKey> = sorted.iter().rev().cloned().collect();
        let hashed = {
            let mut keys = sorted.clone();
            keys.sort_by_key(|key| *Blake3Hash::hash(&key.0).as_bytes());
            keys
        };

        let a = build_incremental(&sorted, manifest, &mut storage).await?;
        let b = build_incremental(&reversed, manifest, &mut storage).await?;
        let c = build_incremental(&hashed, manifest, &mut storage).await?;
        assert_eq!(a.root(), b.root(), "orders must converge over the ceiling");
        assert_eq!(a.root(), c.root(), "orders must converge over the ceiling");

        // The ceiling must be what shaped it: factor 0 groups differently.
        // Compared as BOUNDARY SETS, not roots — the manifest is stamped
        // into every node, so root hashes differ whenever the manifest
        // does, ceiling fired or not.
        let unbounded = build_incremental(&sorted, ceiling_manifest(0, 0), &mut storage).await?;
        assert_ne!(
            leaf_boundaries(a.root(), &storage).await?,
            leaf_boundaries(unbounded.root(), &storage).await?,
            "the ceiling never fired on an over-ceiling all-tails run"
        );

        // Delete oracle: every 5th key, one edit at a time.
        let doomed: Vec<VarKey> = sorted.iter().step_by(5).cloned().collect();
        let survivors: Vec<VarKey> = sorted
            .iter()
            .filter(|key| !doomed.contains(key))
            .cloned()
            .collect();
        let pruned = delete_incremental(a, &doomed, &mut storage).await?;
        let rebuilt = build_incremental(&survivors, manifest, &mut storage).await?;
        assert_eq!(
            pruned.root(),
            rebuilt.root(),
            "incremental deletes must converge on the rebuild under the ceiling"
        );

        Ok(())
    }

    /// The hard bound itself: on an all-tails run (the coin never cuts) the
    /// ceiling still holds — no persisted leaf weighs more than the ceiling
    /// plus one entry, at either candidate factor.
    #[dialog_common::test]
    async fn it_bounds_frames_at_the_ceiling() -> Result<()> {
        for factor in [2u32, 3] {
            for selector in [0u32, 1] {
                let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
                let manifest = ceiling_manifest(factor, selector);
                let sorted = tails_keys("n", 90, &manifest);
                let tree = build_incremental(&sorted, manifest, &mut storage).await?;

                let ceiling = manifest.frame_ceiling();
                let slack = sorted
                    .iter()
                    .map(|key| distribution::cap::entry_weight(&key.0))
                    .max()
                    .expect("keys exist");
                for weight in leaf_weights(&sorted, tree.root(), &storage).await? {
                    assert!(
                        weight <= ceiling + slack,
                        "leaf weighs {weight}, over the {ceiling} ceiling \
                         (factor {factor}, selector {selector})"
                    );
                }
            }
        }
        Ok(())
    }

    /// Step-5 edit locality: frames are delimited by coin cuts, so an edit
    /// inside one frame re-anchors only that frame; boundaries in other
    /// frames — including the coin cut between them — stay byte-identical.
    #[dialog_common::test]
    async fn it_keeps_ceiling_effects_inside_the_touched_frames() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = ceiling_manifest(2, 0);

        // Two over-ceiling all-tails frames separated by one heads key (a
        // coin cut), found deterministically.
        let frame_a = tails_keys("a", 40, &manifest);
        let heads = {
            let mut n = 0u32;
            loop {
                let key = VarKey(format!("m{n:04}").into_bytes());
                if <Geometric as Distribution>::rank(&key.0, &manifest) > 1 {
                    break key;
                }
                n += 1;
            }
        };
        let frame_b = tails_keys("z", 40, &manifest);
        let mut sorted: Vec<VarKey> = frame_a.clone();
        sorted.push(heads.clone());
        sorted.extend(frame_b.clone());
        sorted.sort();

        let mut tree = build_incremental(&sorted, manifest, &mut storage).await?;
        let mut before: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
            .await?
            .into_iter()
            .collect();
        assert!(
            before.contains(&heads.0),
            "the heads key must cut between the frames"
        );

        // One insert into frame B and one delete from it: every boundary
        // change stays inside frame B's key range.
        let insert = tails_keys("z", 41, &manifest)
            .pop()
            .expect("a 41st tails key exists");
        let edits: Vec<(bool, VarKey)> = vec![(true, insert), (false, frame_b[7].clone())];
        let mut delta = Delta::zero();
        for (is_insert, key) in edits {
            let transient = tree.edit_with_manifest(&storage).await?;
            let transient = if is_insert {
                transient
                    .insert(key.clone(), key.0.clone(), &storage)
                    .await?
            } else {
                transient.delete(&key, &storage).await?
            };
            tree = transient.persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();

            let after: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
                .await?
                .into_iter()
                .collect();
            for moved in before.symmetric_difference(&after) {
                assert!(
                    moved.starts_with(b"z"),
                    "an edit inside frame B moved the boundary {:?}",
                    String::from_utf8_lossy(moved)
                );
            }
            before = after;
        }

        Ok(())
    }

    /// Constraint 5: in a composite frame — accepted-seam runs around a
    /// vetoed cluster — the ceiling anchors only at accepted seams; the
    /// vetoed cluster's interior is never cut by the frame machinery,
    /// whatever the weight distribution.
    #[dialog_common::test]
    async fn it_anchors_frames_only_at_accepted_seams() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = ceiling_manifest(2, 0);

        // A small vetoed cluster (under the 512 stretch target, so the
        // stretch backstop stays out of the picture) whose terminal seam
        // coin is tails, found deterministically over cluster tags.
        let cluster = {
            let mut tag = b'a';
            loop {
                let make = |n: u32| {
                    let mut bytes = vec![b'm', tag];
                    bytes.extend(vec![b'q'; 28]);
                    bytes.extend(format!("{n:04}").into_bytes());
                    VarKey(bytes)
                };
                let keys: Vec<VarKey> = (0..5u32).map(make).collect();
                let bank: usize = keys[..4]
                    .iter()
                    .map(|key| distribution::cap::entry_weight(&key.0))
                    .sum();
                let last = &keys[4];
                if !<Geometric as Distribution>::leaf_cut(&last.0, bank, &manifest) {
                    break keys;
                }
                tag += 1;
            }
        };

        let mut sorted: Vec<VarKey> = tails_keys("e", 10, &manifest);
        sorted.extend(cluster.clone());
        sorted.extend(tails_keys("x", 10, &manifest));
        sorted.sort();

        let tree = build_incremental(&sorted, manifest, &mut storage).await?;
        let pieces = leaf_piece_heads(tree.root(), &storage).await?;
        let bound = manifest.max_separator as usize;
        let anchors: Vec<&Vec<u8>> = pieces
            .iter()
            .filter(|(separator, _)| *separator > bound)
            .map(|(_, head)| head)
            .collect();
        assert!(
            !anchors.is_empty(),
            "the composite frame exceeds the ceiling and must be force-split"
        );
        let cluster_interior: HashSet<&[u8]> =
            cluster[1..].iter().map(|key| key.0.as_slice()).collect();
        for anchor in anchors {
            assert!(
                !cluster_interior.contains(anchor.as_slice()),
                "a frame anchor landed inside the vetoed cluster: {:?}",
                String::from_utf8_lossy(anchor)
            );
        }

        Ok(())
    }

    /// The step-5 churn and evenness measurement, one run per (ceiling
    /// factor, anchor selector) config over the same all-tails fixture:
    /// builds the tree, reports piece evenness (leaf weight min/mean/max),
    /// then applies the shared edit protocol (interior inserts, then
    /// deletes) counting boundary moves per edit — excluding the edited
    /// key itself — split into insert-driven and delete-driven. The
    /// numbers land in the experiment note; the assertion only pins that
    /// churn stays bounded (no edit moves more than a handful of
    /// boundaries).
    #[dialog_common::test]
    async fn it_reports_boundary_churn_across_configs() -> Result<()> {
        let probe = capped_manifest();
        let all = tails_keys("f", 200, &probe);
        let seed: Vec<VarKey> = all.iter().step_by(2).cloned().collect();
        let inserts: Vec<VarKey> = all.iter().skip(1).step_by(8).cloned().collect();
        let deletes: Vec<VarKey> = seed.iter().skip(3).step_by(9).cloned().collect();

        for (factor, selector) in [(0u32, 0u32), (2, 0), (2, 1), (3, 0), (3, 1)] {
            let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
            let manifest = ceiling_manifest(factor, selector);
            let mut tree = build_incremental(&seed, manifest, &mut storage).await?;

            let weights = leaf_weights(&seed, tree.root(), &storage).await?;
            let total: usize = weights.iter().sum();
            eprintln!(
                "CHURN-EVENNESS factor={factor} selector={selector}: leaves={} weight min={} mean={} max={}",
                weights.len(),
                weights.iter().min().expect("leaves exist"),
                total / weights.len(),
                weights.iter().max().expect("leaves exist"),
            );

            let mut before: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
                .await?
                .into_iter()
                .collect();
            let mut delta = Delta::zero();
            let mut insert_moves = 0usize;
            let mut delete_moves = 0usize;
            let edits: Vec<(bool, VarKey)> = inserts
                .iter()
                .map(|key| (true, key.clone()))
                .chain(deletes.iter().map(|key| (false, key.clone())))
                .collect();
            for (is_insert, key) in edits {
                let transient = tree.edit_with_manifest(&storage).await?;
                let transient = if is_insert {
                    transient
                        .insert(key.clone(), key.0.clone(), &storage)
                        .await?
                } else {
                    transient.delete(&key, &storage).await?
                };
                tree = transient.persist(&mut delta)?;
                for (hash, buffer) in delta.flush() {
                    storage.store(buffer.as_ref().to_vec(), &hash).await?;
                }
                delta = Delta::zero();

                let after: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
                    .await?
                    .into_iter()
                    .collect();
                let moved = before
                    .symmetric_difference(&after)
                    .filter(|boundary| boundary.as_slice() != key.0.as_slice())
                    .count();
                assert!(
                    moved <= 4,
                    "an edit moved {moved} boundaries (factor {factor}, selector {selector})"
                );
                if is_insert {
                    insert_moves += moved;
                } else {
                    delete_moves += moved;
                }
                before = after;
            }
            eprintln!(
                "CHURN factor={factor} selector={selector}: insert_moves={insert_moves}/{} delete_moves={delete_moves}/{}",
                inserts.len(),
                deletes.len(),
            );
        }

        Ok(())
    }

    /// A vetoed cluster with two sub-cluster boundaries whose shortest
    /// separators (25 bytes) are strictly shorter than every in-sub seam's
    /// (32+): the hybrid selector's semantic anchors. Three sub-clusters
    /// of 15 near-duplicate keys sharing a 24-byte cluster prefix.
    fn semantic_cluster() -> Vec<VarKey> {
        let mut keys = Vec::new();
        for sub in [b'A', b'B', b'C'] {
            for n in 0..15u32 {
                let mut bytes = vec![b'W'];
                bytes.extend(vec![b'q'; 23]);
                bytes.push(sub);
                bytes.extend(vec![b'r'; 6]);
                bytes.extend(format!("{n:03}").into_bytes());
                keys.push(VarKey(bytes));
            }
        }
        keys
    }

    /// Sandwich property, insert half: for sorted `p < q < k`, `q` shares
    /// `p` and `k`'s common prefix, so an insert can never mint a strictly
    /// shorter separator — under the hybrid selector the semantic anchors
    /// (the sub-cluster boundaries) stay put through inserts elsewhere in
    /// the stretch.
    #[dialog_common::test]
    async fn it_keeps_semantic_anchors_still_under_inserts() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = ceiling_manifest(0, 1);

        let sorted = semantic_cluster();
        let mut tree = build_incremental(&sorted, manifest, &mut storage).await?;
        // A semantic anchor's stored separator carries exactly lcp + 1 = 25
        // bytes (the sub-boundary divergence, one past the 24-byte cluster
        // prefix); in-sub anchors carry 32 or more.
        let bound = manifest.max_separator as usize;
        let semantic = |pieces: &[(usize, Vec<u8>)]| -> Vec<Vec<u8>> {
            pieces
                .iter()
                .filter(|(separator, _)| *separator == bound + 1)
                .map(|(_, head)| head.clone())
                .collect()
        };
        let anchors = semantic(&leaf_piece_heads(tree.root(), &storage).await?);
        let sub_b_first = &sorted[15];
        let sub_c_first = &sorted[30];
        assert!(
            anchors.contains(&sub_b_first.0) && anchors.contains(&sub_c_first.0),
            "the hybrid selector must anchor at the sub-cluster boundaries"
        );

        // Insert new members inside sub-cluster B (deep seams only): the
        // semantic anchors must not move.
        let mut delta = Delta::zero();
        for n in [500u32, 501, 502] {
            let mut bytes = vec![b'W'];
            bytes.extend(vec![b'q'; 23]);
            bytes.push(b'B');
            bytes.extend(vec![b'r'; 6]);
            bytes.extend(format!("{n:03}").into_bytes());
            let key = VarKey(bytes);
            tree = tree
                .edit_with_manifest(&storage)
                .await?
                .insert(key.clone(), key.0.clone(), &storage)
                .await?
                .persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();
        }
        let after = semantic(&leaf_piece_heads(tree.root(), &storage).await?);
        assert!(
            after.contains(&sub_b_first.0) && after.contains(&sub_c_first.0),
            "inserts inside a sub-cluster moved a semantic anchor"
        );

        Ok(())
    }

    /// Sandwich property, delete half: deleting the semantic anchor key
    /// merges its seam with the next, and the merged seam keeps the shallow
    /// divergence — the anchor legitimately re-anchors at the merged seam's
    /// right key (the sub-cluster's next member), not anywhere else.
    #[dialog_common::test]
    async fn it_reanchors_when_the_semantic_anchor_is_deleted() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = ceiling_manifest(0, 1);

        let sorted = semantic_cluster();
        let tree = build_incremental(&sorted, manifest, &mut storage).await?;
        let sub_b_first = sorted[15].clone();
        let sub_b_second = sorted[16].clone();

        let mut delta = Delta::zero();
        let tree = tree
            .edit_with_manifest(&storage)
            .await?
            .delete(&sub_b_first, &storage)
            .await?
            .persist(&mut delta)?;
        for (hash, buffer) in delta.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        let bound = manifest.max_separator as usize;
        let anchors: Vec<Vec<u8>> = leaf_piece_heads(tree.root(), &storage)
            .await?
            .into_iter()
            .filter(|(separator, _)| *separator > bound)
            .map(|(_, head)| head)
            .collect();
        assert!(
            !anchors.contains(&sub_b_first.0),
            "the deleted anchor key must be gone"
        );
        assert!(
            anchors.contains(&sub_b_second.0),
            "the merged seam must re-anchor at the sub-cluster's next member"
        );

        Ok(())
    }

    /// A composite stretch: `clusters` near-duplicate clusters (34-byte
    /// keys sharing a 30-byte in-cluster prefix, so every in-cluster seam
    /// is vetoed under the capped manifest's 24-byte bound) separated by
    /// one short "glue" key per cluster (sorting after its cluster, before
    /// the next, diverging at byte 3 — an accepted seam). This is the
    /// SE-shaped input the per-key weight coin could not pace: only glue
    /// seams can cut, and each used to flip a single key-sized coin.
    fn composite_keys(clusters: u32, per_cluster: u32) -> Vec<VarKey> {
        let mut keys = Vec::new();
        for cluster in 0..clusters {
            for n in 0..per_cluster {
                let mut bytes = format!("G{cluster:02}").into_bytes();
                bytes.extend(vec![b'w'; 27]);
                bytes.extend(format!("{n:04}").into_bytes());
                keys.push(VarKey(bytes));
            }
            keys.push(VarKey(format!("G{cluster:02}zz").into_bytes()));
        }
        keys
    }

    /// Step-4 convergence gate: the banked coin is a pure function of the
    /// key sequence (the bank is "weight since the last accepted seam", a
    /// structural property, never a cut outcome), so composite stretches
    /// must build byte-identical trees across insertion orders, and
    /// incremental deletes must land on the fresh build of the survivors.
    #[dialog_common::test]
    async fn it_converges_on_composite_stretches_across_insertion_orders() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        let sorted = {
            let mut keys = composite_keys(4, 20);
            keys.sort();
            keys
        };
        let reversed: Vec<VarKey> = sorted.iter().rev().cloned().collect();
        let hashed = {
            let mut keys = sorted.clone();
            keys.sort_by_key(|key| *Blake3Hash::hash(&key.0).as_bytes());
            keys
        };

        let a = build_incremental(&sorted, manifest, &mut storage).await?;
        let b = build_incremental(&reversed, manifest, &mut storage).await?;
        let c = build_incremental(&hashed, manifest, &mut storage).await?;
        assert_eq!(
            a.root(),
            b.root(),
            "sorted and reverse insertion orders must converge on composite stretches"
        );
        assert_eq!(
            a.root(),
            c.root(),
            "sorted and hash-shuffled insertion orders must converge on composite stretches"
        );

        // The delete oracle: drop a whole cluster plus every 7th key, one
        // edit at a time, and land on the survivors' fresh build.
        let doomed: Vec<VarKey> = sorted
            .iter()
            .enumerate()
            .filter(|(at, key)| at % 7 == 0 || key.0.starts_with(b"G01"))
            .map(|(_, key)| key.clone())
            .collect();
        let survivors: Vec<VarKey> = sorted
            .iter()
            .filter(|key| !doomed.contains(key))
            .cloned()
            .collect();
        let pruned = delete_incremental(a, &doomed, &mut storage).await?;
        let rebuilt = build_incremental(&survivors, manifest, &mut storage).await?;
        assert_eq!(
            pruned.root(),
            rebuilt.root(),
            "incremental deletes through composite stretches must converge on the rebuild"
        );

        for key in &survivors {
            assert_eq!(
                pruned.get(key, &storage).await?.as_deref(),
                Some(key.0.as_slice()),
                "survivor must remain readable"
            );
        }

        Ok(())
    }

    /// Step-4 edit locality: the bank resets at every accepted seam, so an
    /// edit inside one vetoed cluster can only change decisions within that
    /// cluster's stretch (its forced anchors and its terminating seam);
    /// every boundary outside the enclosing accepted-seam window stays
    /// byte-for-byte in place.
    #[dialog_common::test]
    async fn it_keeps_bank_effects_inside_the_enclosing_stretch() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        let sorted = {
            let mut keys = composite_keys(4, 20);
            keys.sort();
            keys
        };
        let mut tree = build_incremental(&sorted, manifest, &mut storage).await?;
        let mut before: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
            .await?
            .into_iter()
            .collect();

        // One insert extending cluster G02's stretch at its tail, then one
        // delete from its middle. A stretch member is 34 bytes and starts
        // with the cluster tag; the glue key is short — decisions outside
        // the G02 stretch may not move.
        let inside_stretch =
            |boundary: &Vec<u8>| boundary.len() == 34 && boundary.starts_with(b"G02");
        let mut edits: Vec<(bool, VarKey)> = Vec::new();
        {
            let mut bytes = b"G02".to_vec();
            bytes.extend(vec![b'w'; 27]);
            bytes.extend(b"0050".to_vec());
            edits.push((true, VarKey(bytes)));
        }
        {
            let mut bytes = b"G02".to_vec();
            bytes.extend(vec![b'w'; 27]);
            bytes.extend(b"0010".to_vec());
            edits.push((false, VarKey(bytes)));
        }

        let mut delta = Delta::zero();
        for (insert, key) in edits {
            let transient = tree.edit_with_manifest(&storage).await?;
            let transient = if insert {
                transient
                    .insert(key.clone(), key.0.clone(), &storage)
                    .await?
            } else {
                transient.delete(&key, &storage).await?
            };
            tree = transient.persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();

            let after: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
                .await?
                .into_iter()
                .collect();
            for moved in before.symmetric_difference(&after) {
                assert!(
                    inside_stretch(moved),
                    "an edit inside cluster G02 moved the boundary {:?}",
                    String::from_utf8_lossy(moved)
                );
            }
            before = after;
        }

        Ok(())
    }

    /// The composite-stretch shape itself, the step-4 payoff: every glue
    /// seam behind an over-target cluster cuts with certainty (the bank
    /// carries the whole cluster's weight, and probability saturates at
    /// one), so leaves break at the cluster boundaries instead of gluing
    /// into one giant run. Under the per-key coin the same input left each
    /// glue seam a ~13% key-sized flip and the clusters merged.
    #[dialog_common::test]
    async fn it_cuts_composite_stretches_at_their_glue_seams() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = capped_manifest();

        let sorted = {
            let mut keys = composite_keys(8, 20);
            keys.sort();
            keys
        };
        let tree = build_incremental(&sorted, manifest, &mut storage).await?;
        let boundaries: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
            .await?
            .into_iter()
            .collect();

        // Each cluster weighs 20 * 66 = 1320 >= the 512 target, so the seam
        // after each cluster's last key is a certain cut. The final
        // cluster's glue key ends the tree, so only the first seven glue
        // seams are interior.
        for cluster in 0..7u32 {
            let mut last = format!("G{cluster:02}").into_bytes();
            last.extend(vec![b'w'; 27]);
            last.extend(b"0019".to_vec());
            assert!(
                boundaries.contains(&last),
                "the glue seam after cluster {cluster} must cut: the bank funds it with the whole cluster"
            );
        }

        Ok(())
    }

    /// Deleting the last key of a vetoed stretch can un-veto the seam its
    /// high-coin predecessor was suppressed by: the predecessor re-partners
    /// with the removed key's successor, and an early divergence there frees
    /// the cut. This is the one veto transition the in-place fast path could
    /// swallow (the edit looks like a plain interior delete), so it pins the
    /// fast-path bypass: the delete must re-shape and split, landing on the
    /// same bytes as a fresh build of the survivors.
    #[dialog_common::test]
    async fn it_recreates_a_cut_when_a_delete_unvetoes_a_seam() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = Manifest {
            max_segment: 0,
            ..capped_manifest()
        };

        // 34-byte keys sharing a 30-byte prefix: past the 24-byte bound, so
        // every in-cluster seam is vetoed.
        let cluster_key = |n: u32| {
            let mut bytes = vec![b'C'];
            bytes.extend(vec![b'q'; 29]);
            bytes.extend(format!("{n:04}").into_bytes());
            VarKey(bytes)
        };
        // The tree's own coin (the capped manifest's branch factor, not the
        // default's): find a high-coin key immediately followed by a
        // low-coin key, deterministically.
        let coin = |key: &VarKey| <Geometric as Distribution>::rank(&key.0, &manifest);
        let mut found = None;
        for n in 0..500u32 {
            if coin(&cluster_key(n)) > 1 && coin(&cluster_key(n + 1)) <= 1 {
                found = Some(n);
                break;
            }
        }
        let high = found.expect("a high/low coin pair exists among 500 cluster keys");

        // Cluster keys up to and including the low-coin follower, plus a
        // short tail: the follower's seam to the tail diverges in byte one
        // but its coin is low, so nothing cuts and everything rides in one
        // segment.
        let mut keys: Vec<VarKey> = (0..=high + 1).map(cluster_key).collect();
        keys.push(VarKey(b"z000".to_vec()));

        let full = build_incremental(&keys, manifest, &mut storage).await?;
        assert!(
            leaf_boundaries(full.root(), &storage).await?.is_empty(),
            "the vetoed stretch must ride in one segment"
        );

        // Deleting the low-coin follower re-partners the high coin's seam
        // with the short tail: the veto lifts and the cut must appear.
        let doomed = vec![cluster_key(high + 1)];
        let survivors: Vec<VarKey> = keys
            .iter()
            .filter(|key| !doomed.contains(key))
            .cloned()
            .collect();
        let pruned = delete_incremental(full, &doomed, &mut storage).await?;
        let rebuilt = build_incremental(&survivors, manifest, &mut storage).await?;
        assert_eq!(
            pruned.root(),
            rebuilt.root(),
            "the un-vetoing delete must converge on the fresh build"
        );
        assert_eq!(
            leaf_boundaries(pruned.root(), &storage).await?,
            vec![cluster_key(high).0],
            "the freed high coin must now cut after its key"
        );

        Ok(())
    }

    /// Seam-flip sensitivity under the veto (no segment cap): one edit may
    /// change the status of at most the two seams adjacent to the edited
    /// key, because a seam's separator — and with it the veto and the coin
    /// it guards — is a pure function of its two partner keys, and an edit
    /// re-partners only the seams it touches. Measured, not assumed: this
    /// is the edit-sensitivity fear that once killed a veto variant.
    #[dialog_common::test]
    async fn it_flips_at_most_adjacent_seams_per_edit() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let manifest = Manifest {
            max_segment: 0,
            ..capped_manifest()
        };

        let sorted = {
            let mut keys = capped_keys();
            keys.sort();
            keys
        };
        let mut tree = build_incremental(&sorted, manifest, &mut storage).await?;
        let mut before: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
            .await?
            .into_iter()
            .collect();

        // A deterministic mix of edits: near-duplicate inserts into cluster
        // L0 (both partners vetoed), deletes of cluster keys (seams join
        // across the gap), and short-key inserts and deletes (the natural
        // coin's territory).
        let mut edits: Vec<(bool, VarKey)> = Vec::new();
        for n in 0..12u32 {
            let mut bytes = vec![b'L', b'0'];
            bytes.extend(vec![b'p'; 28]);
            bytes.extend(format!("{:04}", 100 + n).into_bytes());
            edits.push((true, VarKey(bytes)));
        }
        for n in (0..60u32).step_by(9) {
            let mut bytes = vec![b'L', b'2'];
            bytes.extend(vec![b'p'; 28]);
            bytes.extend(format!("{n:04}").into_bytes());
            edits.push((false, VarKey(bytes)));
        }
        for n in 0..8u32 {
            edits.push((true, VarKey(format!("t{n:03}").into_bytes())));
        }
        for n in (0..40u32).step_by(11) {
            edits.push((false, VarKey(format!("s{n:03}").into_bytes())));
        }

        let mut delta = Delta::zero();
        for (insert, key) in edits {
            let transient = tree.edit_with_manifest(&storage).await?;
            let transient = if insert {
                transient
                    .insert(key.clone(), key.0.clone(), &storage)
                    .await?
            } else {
                transient.delete(&key, &storage).await?
            };
            tree = transient.persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
            delta = Delta::zero();

            let after: HashSet<Vec<u8>> = leaf_boundaries(tree.root(), &storage)
                .await?
                .into_iter()
                .collect();
            let flips = before
                .symmetric_difference(&after)
                .filter(|boundary| boundary.as_slice() != key.0.as_slice())
                .count();
            assert!(
                flips <= 2,
                "an edit of {:?} flipped {flips} pre-existing seams",
                String::from_utf8_lossy(&key.0)
            );
            before = after;
        }

        Ok(())
    }

    /// A tree built under a NON-default manifest keeps that manifest across an
    /// edit opened with [`PersistentTree::edit_with_manifest`], and the format
    /// constants a reader recovers are the tree's own, not the defaults. The
    /// synchronous [`PersistentTree::edit`] is shown to lose them, which is
    /// exactly the boundary its documentation draws.
    #[dialog_common::test]
    async fn it_preserves_a_non_default_manifest_across_an_edit() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        // Non-default in every field: a smaller branching parameter, a
        // separator bound low enough that the keys below straddle it, and a
        // spill threshold between the default and the sizes used here.
        let manifest = Manifest {
            fanout_n: 4,
            max_separator: 16,
            inline_n: 64,
            ..Manifest::default()
        };
        assert_ne!(manifest, Manifest::default());

        let key_at = |n: u32| {
            let mut bytes = format!("key-{n:03}").into_bytes();
            // Straddle the tree's 16-byte separator bound so shaping under it
            // differs from shaping under the default 512.
            bytes.resize(8 + (n as usize % 24), b'p');
            VarKey(bytes)
        };

        let mut delta = Delta::zero();
        let first = key_at(0);
        let mut tree: VarTree =
            TransientTree::with_manifest(NULL_BLAKE3_HASH.clone(), Cache::new(), manifest)
                .insert(first.clone(), first.0.clone(), &storage)
                .await?
                .persist(&mut delta)?;
        for (hash, buffer) in delta.flush() {
            storage.store(buffer.as_ref().to_vec(), &hash).await?;
        }

        assert_eq!(
            tree.manifest(&storage).await?,
            manifest,
            "the first persist must stamp the tree's own manifest"
        );

        for n in 1..64u32 {
            let key = key_at(n);
            tree = tree
                .edit_with_manifest(&storage)
                .await?
                .insert(key.clone(), key.0.clone(), &storage)
                .await?
                .persist(&mut delta)?;
            for (hash, buffer) in delta.flush() {
                storage.store(buffer.as_ref().to_vec(), &hash).await?;
            }
        }

        let after = tree.manifest(&storage).await?;
        assert_eq!(
            after, manifest,
            "an edit opened with the tree's manifest must not reformat it"
        );
        // Spelled out per field: `inline_n` is the value-spill threshold the
        // artifact key builders read off the tree, and it is the tree's 64,
        // not the default 4096.
        assert_eq!(after.inline_n, 64);
        assert_eq!(after.max_separator, 16);
        assert_eq!(after.fanout_n, 4);

        // Every entry still reads back, so preserving the format did not
        // corrupt the structure those constants shape.
        for n in 0..64u32 {
            let key = key_at(n);
            assert_eq!(tree.get(&key, &storage).await?, Some(key.0.clone()));
        }

        // The synchronous entry cannot read the root, so it runs under the
        // default manifest. Against a non-default tree that disagreement is
        // now REFUSED at load rather than silently re-coining the touched path
        // under the wrong format: the failure is loud and the tree is left
        // intact. Pinned here rather than left implicit.
        let last = key_at(64);
        let reformatted = tree
            .edit()
            .insert(last.clone(), last.0.clone(), &storage)
            .await;
        assert!(
            reformatted.is_err(),
            "the synchronous edit must refuse a non-default-manifest tree"
        );
        assert_eq!(
            tree.manifest(&storage).await?,
            manifest,
            "the refused edit must leave the tree's format untouched"
        );

        Ok(())
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

    /// Encodes a key big-endian so numeric order matches the byte-wise
    /// lexicographic order the tree sorts by, letting the stitch tests express
    /// range pieces as plain numeric bands.
    fn bkey(k: u32) -> [u8; 4] {
        k.to_be_bytes()
    }

    /// The geometric rank of a big-endian key, hashed the way the tree hashes
    /// it. The stitch tests encode keys big-endian, so their boundary/interior
    /// classification must too.
    fn rank_of_be(key: u32) -> Rank {
        distribution::geometric::rank(&Blake3Hash::hash(&key.to_be_bytes()))
    }

    /// Builds a tree over big-endian `keys` in one batch and flushes it to
    /// storage, so a stitch can read its nodes back.
    async fn stitch_source(keys: &[u32], storage: &mut TestStorage) -> Result<TestTree> {
        let mut edit = TestTree::empty().edit();
        for &k in keys {
            edit = edit.insert(bkey(k), bkey(k).to_vec(), storage).await?;
        }
        let mut delta = Delta::zero();
        let tree = edit.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(tree)
    }

    /// The from-scratch oracle for a stitch: the root of a tree built by
    /// inserting the union of the pieces' entries directly.
    async fn stitch_oracle(keys: &[u32], storage: &TestStorage) -> Result<Blake3Hash> {
        let mut edit = TestTree::empty().edit();
        for &k in keys {
            edit = edit.insert(bkey(k), bkey(k).to_vec(), storage).await?;
        }
        let mut delta = Delta::zero();
        Ok(edit.persist(&mut delta)?.root().clone())
    }

    /// Stitches `pieces`, persists the result, and returns its root hash along
    /// with the number of nodes the persist wrote into the delta.
    async fn stitched(
        pieces: Vec<Piece<'_, [u8; 4], Vec<u8>>>,
        storage: &TestStorage,
    ) -> Result<(Blake3Hash, usize)> {
        let tree = TransientTree::stitch(pieces, storage).await?;
        let mut delta = Delta::zero();
        let tree = tree.persist(&mut delta)?;
        Ok((tree.root().clone(), delta.flush().count()))
    }

    /// Draws up to `count` distinct keys from the half-open `range`.
    fn random_band(rng: &mut Rng, range: std::ops::Range<u32>, count: usize) -> Vec<u32> {
        let width = range.end - range.start;
        let mut keys: Vec<u32> = (0..count * 2)
            .map(|_| range.start + rng.next_u32() % width)
            .collect();
        keys.sort_unstable();
        keys.dedup();
        keys.truncate(count);
        keys
    }

    /// Stitches whole-range pieces over the trees built from `a_keys` and
    /// `b_keys` (ascending, in disjoint bands) and asserts the result equals
    /// the from-scratch build over their union.
    async fn assert_disjoint_stitch(
        a_keys: &[u32],
        b_keys: &[u32],
        storage: &mut TestStorage,
        label: &str,
    ) -> Result<()> {
        let a = stitch_source(a_keys, storage).await?;
        let b = stitch_source(b_keys, storage).await?;
        let boundary = b_keys[0];
        let pieces = vec![
            Piece::Range {
                source: &a,
                range: bkey(0)..=bkey(boundary - 1),
            },
            Piece::Range {
                source: &b,
                range: bkey(boundary)..=bkey(u32::MAX),
            },
        ];
        let (root, _) = stitched(pieces, storage).await?;
        let union: Vec<u32> = a_keys.iter().chain(b_keys).copied().collect();
        let expected = stitch_oracle(&union, storage).await?;
        assert_eq!(
            root, expected,
            "{label}: stitching two disjoint trees must match the union build"
        );
        Ok(())
    }

    /// A stitch whose LEFTMOST piece start-trims its source must still be
    /// canonical. The surviving leftmost segment carries the separator its
    /// parent link held in the source (derived from a left neighbor the
    /// trim dropped), and nothing downstream recomputes the left edge —
    /// joins re-cut only right-hand seams — so without a re-floor the whole
    /// leftmost spine of the persisted result differs byte-for-byte from a
    /// from-scratch build over the same entries: replica divergence in the
    /// convergent-merge use case the stitch exists for.
    #[dialog_common::test]
    async fn it_stitches_a_start_trimmed_leftmost_piece_canonically() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let source = stitch_source(&(0..500).collect::<Vec<u32>>(), &mut storage).await?;

        // Single start-trimmed piece.
        let (root, _) = stitched(
            vec![Piece::Range {
                source: &source,
                range: bkey(300)..=bkey(u32::MAX),
            }],
            &storage,
        )
        .await?;
        let expected = stitch_oracle(&(300..500).collect::<Vec<u32>>(), &storage).await?;
        assert_eq!(
            root, expected,
            "a start-trimmed leftmost piece keeps the canonical left edge"
        );

        // The same left edge through the fold path (a second piece joins).
        let tail = stitch_source(&(1000..1100).collect::<Vec<u32>>(), &mut storage).await?;
        let (root, _) = stitched(
            vec![
                Piece::Range {
                    source: &source,
                    range: bkey(300)..=bkey(999),
                },
                Piece::Range {
                    source: &tail,
                    range: bkey(1000)..=bkey(u32::MAX),
                },
            ],
            &storage,
        )
        .await?;
        let union: Vec<u32> = (300..500).chain(1000..1100).collect();
        let expected = stitch_oracle(&union, &storage).await?;
        assert_eq!(
            root, expected,
            "the fold path keeps the canonical left edge too"
        );
        Ok(())
    }

    /// Stitching two disjoint trees, each taken whole, must produce exactly
    /// the tree a from-scratch build over the union of their entries produces.
    #[dialog_common::test]
    async fn it_stitches_two_disjoint_trees_canonically() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // A fixed case first: evens in 0..600 next to the band 1000..1300.
        let a_keys: Vec<u32> = (0..600).step_by(2).collect();
        let b_keys: Vec<u32> = (1000..1300).collect();
        assert_disjoint_stitch(&a_keys, &b_keys, &mut storage, "fixed").await?;

        // Then random key sets in disjoint bands.
        for seed in 0..50u64 {
            let mut rng = Rng::new(seed);
            let a_keys = random_band(&mut rng, 0..100_000, 150);
            let b_keys = random_band(&mut rng, 1_000_000..1_100_000, 150);
            assert_disjoint_stitch(&a_keys, &b_keys, &mut storage, &format!("seed {seed}")).await?;
        }
        Ok(())
    }

    /// A prefix range of a tree, a band of explicit entries, and a suffix
    /// range of the same tree stitch into the canonical tree over the union;
    /// the source's middle band is dropped and replaced by the entries.
    #[dialog_common::test]
    async fn it_stitches_ranges_of_one_tree_with_explicit_entries() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let source_keys: Vec<u32> = (0..500).collect();
        let source = stitch_source(&source_keys, &mut storage).await?;

        for seed in 0..30u64 {
            let mut rng = Rng::new(seed);
            let k = 50 + rng.next_u32() % 150;
            let m = 300 + rng.next_u32() % 150;
            let band = random_band(&mut rng, k + 1..m, 60);
            let entries: Vec<Entry<[u8; 4], Vec<u8>>> = band
                .iter()
                .map(|&x| Entry {
                    key: bkey(x),
                    value: bkey(x).to_vec(),
                })
                .collect();
            let pieces = vec![
                Piece::Range {
                    source: &source,
                    range: bkey(0)..=bkey(k),
                },
                Piece::Entries(entries),
                Piece::Range {
                    source: &source,
                    range: bkey(m)..=bkey(u32::MAX),
                },
            ];
            let (root, _) = stitched(pieces, &storage).await?;

            let union: Vec<u32> = source_keys
                .iter()
                .copied()
                .filter(|&x| x <= k)
                .chain(band.iter().copied())
                .chain(source_keys.iter().copied().filter(|&x| x >= m))
                .collect();
            let expected = stitch_oracle(&union, &storage).await?;
            assert_eq!(
                root, expected,
                "seed {seed}: prefix + entries + suffix stitch must match the union build"
            );
        }
        Ok(())
    }

    /// Alternating range pieces from two trees over interleaved bands: every
    /// piece boundary falls strictly inside both sources, so each seam trims
    /// and re-joins mid-tree. This is the seam-churn case.
    #[dialog_common::test]
    async fn it_stitches_alternating_ranges_from_two_trees() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..20u64 {
            let mut rng = Rng::new(seed);
            let width = 16 + (seed as u32 % 4) * 40;
            let bands = 8u32;

            // A owns the even bands, B the odd ones, each holding a random
            // subset of its bands' keys.
            let mut a_keys = Vec::new();
            let mut b_keys = Vec::new();
            for band in 0..bands {
                let owner = if band % 2 == 0 {
                    &mut a_keys
                } else {
                    &mut b_keys
                };
                for key in band * width..(band + 1) * width {
                    if rng.next_u32() & 1 == 0 {
                        owner.push(key);
                    }
                }
            }
            let a = stitch_source(&a_keys, &mut storage).await?;
            let b = stitch_source(&b_keys, &mut storage).await?;

            let pieces: Vec<Piece<'_, [u8; 4], Vec<u8>>> = (0..bands)
                .map(|band| Piece::Range {
                    source: if band % 2 == 0 { &a } else { &b },
                    range: bkey(band * width)..=bkey((band + 1) * width - 1),
                })
                .collect();
            let (root, _) = stitched(pieces, &storage).await?;

            let mut union = a_keys.clone();
            union.extend(&b_keys);
            union.sort_unstable();
            let expected = stitch_oracle(&union, &storage).await?;
            assert_eq!(
                root, expected,
                "seed {seed} width {width}: alternating range stitch must match the union build"
            );
        }
        Ok(())
    }

    /// Stitching two large disjoint trees writes only seam nodes: the persist
    /// delta stays at spine scale, nowhere near the entry count, proving the
    /// interiors were reused as persistent links rather than rebuilt.
    #[dialog_common::test]
    async fn it_reuses_interior_nodes_when_stitching() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let a_keys: Vec<u32> = (0..1000).collect();
        let b_keys: Vec<u32> = (1_000_000..1_001_000).collect();
        let a = stitch_source(&a_keys, &mut storage).await?;
        let b = stitch_source(&b_keys, &mut storage).await?;

        let pieces = vec![
            Piece::Range {
                source: &a,
                range: bkey(0)..=bkey(999_999),
            },
            Piece::Range {
                source: &b,
                range: bkey(1_000_000)..=bkey(u32::MAX),
            },
        ];
        let (root, written) = stitched(pieces, &storage).await?;
        assert!(
            (1..=40).contains(&written),
            "persist wrote {written} nodes; a stitch of 2000 entries must write \
             only the seam, not the interior"
        );

        let union: Vec<u32> = a_keys.iter().chain(&b_keys).copied().collect();
        let expected = stitch_oracle(&union, &storage).await?;
        assert_eq!(root, expected, "the reused stitch must still be canonical");
        Ok(())
    }

    /// A single piece covering its source's whole key range IS that source:
    /// the stitch hands back the same root and persisting writes nothing.
    #[dialog_common::test]
    async fn it_stitches_a_single_full_range_piece_to_the_source_root() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let keys: Vec<u32> = (0..400).collect();
        let source = stitch_source(&keys, &mut storage).await?;

        let pieces = vec![Piece::Range {
            source: &source,
            range: bkey(0)..=bkey(u32::MAX),
        }];
        let (root, written) = stitched(pieces, &storage).await?;
        assert_eq!(&root, source.root(), "a whole-source stitch is the source");
        assert_eq!(written, 0, "a whole-source stitch must write no nodes");
        Ok(())
    }

    /// Degenerate stitches: no pieces, an empty entries piece, and a range
    /// that contains none of its source's keys all produce the empty tree.
    #[dialog_common::test]
    async fn it_stitches_empty_pieces_to_the_empty_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let empty_root = TestTree::empty().root().clone();

        let (root, written) = stitched(vec![], &storage).await?;
        assert_eq!(root, empty_root, "no pieces stitch to the empty tree");
        assert_eq!(written, 0);

        let (root, _) = stitched(vec![Piece::Entries(Vec::new())], &storage).await?;
        assert_eq!(
            root, empty_root,
            "an empty entries piece contributes nothing"
        );

        let source = stitch_source(&(0..100).collect::<Vec<u32>>(), &mut storage).await?;
        let (root, _) = stitched(
            vec![Piece::Range {
                source: &source,
                range: bkey(500)..=bkey(900),
            }],
            &storage,
        )
        .await?;
        assert_eq!(
            root, empty_root,
            "a range holding no keys contributes nothing"
        );
        Ok(())
    }

    /// Splitting one source into two adjacent range pieces must stitch back to
    /// the identical root, whether the split lands inside a leaf (an interior
    /// key) or exactly on a segment boundary.
    #[dialog_common::test]
    async fn it_reassembles_a_source_split_at_any_key() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let keys: Vec<u32> = (0..500).collect();
        let source = stitch_source(&keys, &mut storage).await?;

        let interior = (200..300)
            .find(|&k| rank_of_be(k) <= 1)
            .expect("an interior key in 200..300");
        let boundary = (100..400).find(|&k| rank_of_be(k) > 1);

        for split in std::iter::once(interior).chain(boundary) {
            let pieces = vec![
                Piece::Range {
                    source: &source,
                    range: bkey(0)..=bkey(split),
                },
                Piece::Range {
                    source: &source,
                    range: bkey(split + 1)..=bkey(u32::MAX),
                },
            ];
            let (root, _) = stitched(pieces, &storage).await?;
            assert_eq!(
                &root,
                source.root(),
                "splitting at {split} and stitching back must reproduce the source root"
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod buffer_edit_interaction_tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use dialog_storage::MemoryStorageBackend;

    use crate::helpers::{
        DistributionSimulator, SpecKey, TestStorage as SpecStorage, encode_key, test_storage,
    };
    use crate::{
        ArchivedNodeBody, Buffer, Change, ContentAddressedStorage, Delta, Entry, HitchhikerTree,
        NoveltyEntry, NoveltyOp, PersistentNode, PersistentTree, Piece, TransientTree, tree_spec,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Store = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
    type Tree = PersistentTree<[u8; 4], Vec<u8>>;

    async fn settle(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut Store) -> Result<()> {
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    /// Buffering a retract of a BOUNDARY key must produce the same tree as
    /// retracting it canonically.
    ///
    /// A boundary key terminates its leaf, so removing it forces that leaf to
    /// fuse with the right-adjacent one. The canonical delete path detects this
    /// and re-shapes; a buffered retract defers the delete, so the fuse has to
    /// happen when the op finally reaches the leaf. If it does not, the tree
    /// keeps a shape no canonical build would produce, and two replicas holding
    /// the same facts disagree on their roots.
    #[dialog_common::test]
    async fn it_fuses_leaves_when_a_buffered_retract_removes_a_boundary() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Build a tree and find a real boundary key: one that terminates a leaf.
        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..600u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Discover a boundary: walk the root's links; each link's upper_bound is
        // the last key of that child, i.e. a boundary key.
        let boundary = {
            use crate::{ArchivedNodeBody, PersistentNode};
            let bytes = dialog_storage::StorageBackend::get(storage.backend(), base.root())
                .await?
                .unwrap();
            let node: PersistentNode<[u8; 4], Vec<u8>> =
                PersistentNode::new(crate::Buffer::from(bytes));
            match node.body()? {
                ArchivedNodeBody::Index(index) => {
                    // Separators are lower bounds, so the second child's
                    // separator IS the boundary key that ends the first child.
                    let separator = index.separator(1)?;
                    <[u8; 4]>::try_from(separator.as_slice())
                        .expect("separator is a whole four-byte key")
                }
                ArchivedNodeBody::Segment(_) => panic!("expected an index root"),
            }
        };

        // Canonical: delete the boundary through the edit path.
        let mut delta = Delta::zero();
        let canonical = base
            .edit()
            .delete(&boundary, &storage)
            .await?
            .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;

        // Buffered: retract the same boundary, then canonicalize so the op
        // reaches the leaf and any fuse must happen.
        let buffered = HitchhikerTree::open(&base)
            .with_op_buf_size(1_000_000)
            .delete(boundary, &storage)
            .await?;
        let mut delta = Delta::zero();
        let flushed = buffered.canonicalize(&storage, &mut delta).await?;
        settle(&mut delta, &mut storage).await?;

        assert_eq!(
            flushed.root(),
            canonical.root(),
            "a buffered boundary retract must fuse leaves like the canonical delete"
        );
        Ok(())
    }

    /// The same boundary fuse, but reached by OVERFLOW rather than an explicit
    /// canonicalize: a small buffer makes the retract cascade to the leaf as a
    /// side effect of later writes. The resulting tree must still match a
    /// canonical build of the surviving fact set.
    #[dialog_common::test]
    async fn it_fuses_leaves_when_an_overflowing_retract_removes_a_boundary() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..600u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        let boundary = {
            use crate::{ArchivedNodeBody, PersistentNode};
            let bytes = dialog_storage::StorageBackend::get(storage.backend(), base.root())
                .await?
                .unwrap();
            let node: PersistentNode<[u8; 4], Vec<u8>> =
                PersistentNode::new(crate::Buffer::from(bytes));
            match node.body()? {
                ArchivedNodeBody::Index(index) => {
                    // Separators are lower bounds, so the second child's
                    // separator IS the boundary key that ends the first child.
                    let separator = index.separator(1)?;
                    <[u8; 4]>::try_from(separator.as_slice())
                        .expect("separator is a whole four-byte key")
                }
                ArchivedNodeBody::Segment(_) => panic!("expected an index root"),
            }
        };

        // Reference: the same fact set built canonically (boundary deleted, plus
        // the extra keys the buffered run writes).
        let extras: Vec<u32> = (700..716).collect();
        let mut canonical = base.clone();
        let mut delta = Delta::zero();
        canonical = canonical
            .edit()
            .delete(&boundary, &storage)
            .await?
            .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        for key in &extras {
            canonical = canonical
                .edit()
                .insert(key.to_be_bytes(), vec![9], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Buffered: tiny buffer, so the retract cascades on overflow.
        let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(4);
        buffered = buffered.delete(boundary, &storage).await?;
        for key in &extras {
            buffered = buffered
                .insert(key.to_be_bytes(), vec![9], &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let flushed = buffered.canonicalize(&storage, &mut delta).await?;
        settle(&mut delta, &mut storage).await?;

        assert_eq!(
            flushed.root(),
            canonical.root(),
            "an overflow-cascaded boundary retract must fuse leaves like the canonical delete"
        );
        Ok(())
    }

    /// Buffered inserts that create new boundaries must split leaves exactly as
    /// canonical inserts do.
    ///
    /// A key's rank decides whether it terminates a leaf, so an insert can split
    /// a node. Deferring that insert through a buffer must not defer the split
    /// away: once the op reaches the leaf, the shape must match a canonical
    /// build, or two replicas with the same facts hold different roots.
    ///
    /// Sweeps seeds so the random keys actually hit boundary ranks.
    #[dialog_common::test]
    async fn it_splits_leaves_like_canonical_for_buffered_inserts() -> Result<()> {
        for seed in 0..25u64 {
            let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

            let base_keys: Vec<u32> = (0..400).map(|_| next() % 100_000).collect();
            let mut base = Tree::empty();
            let mut delta = Delta::zero();
            for key in &base_keys {
                base = base
                    .edit()
                    .insert(key.to_be_bytes(), vec![1], &storage)
                    .await?
                    .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
            }

            // A mix of inserts and retracts, including keys already present, so
            // both splits and fuses are exercised.
            let ops: Vec<(bool, u32)> = (0..40)
                .map(|_| {
                    let insert = !next().is_multiple_of(3);
                    let key = if next().is_multiple_of(2) {
                        base_keys[(next() as usize) % base_keys.len()]
                    } else {
                        next() % 100_000
                    };
                    (insert, key)
                })
                .collect();

            let mut canonical = base.clone();
            let mut delta = Delta::zero();
            for (insert, key) in &ops {
                canonical = if *insert {
                    canonical
                        .edit()
                        .insert(key.to_be_bytes(), vec![2], &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    canonical
                        .edit()
                        .delete(&key.to_be_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?
                };
                settle(&mut delta, &mut storage).await?;
            }

            // Same ops through buffers, at two cascade depths.
            for op_buf in [4usize, 1_000_000] {
                let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(op_buf);
                for (insert, key) in &ops {
                    buffered = if *insert {
                        buffered
                            .insert(key.to_be_bytes(), vec![2], &storage)
                            .await?
                    } else {
                        buffered.delete(key.to_be_bytes(), &storage).await?
                    };
                }
                let mut delta = Delta::zero();
                let flushed = buffered.canonicalize(&storage, &mut delta).await?;
                settle(&mut delta, &mut storage).await?;

                assert_eq!(
                    flushed.root(),
                    canonical.root(),
                    "seed {seed}, op_buf {op_buf}: buffered writes must reshape like canonical ones"
                );
            }
        }
        Ok(())
    }

    /// Stitching pieces of buffered trees must preserve their buffered ops.
    ///
    /// `stitch` assembles a tree from whole subtree ranges, which is how the
    /// graft merge adopts a side's content without walking it. Those subtrees
    /// can carry buffered ops, and dropping them loses writes: the graft path
    /// reports the adopted bulk as missing.
    #[dialog_common::test]
    async fn it_preserves_buffered_ops_through_stitch() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..400u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // A buffered write held at the root, plus one flushed for contrast.
        let buffered_key = 900u32.to_be_bytes();
        let buffered_tree = {
            let tree = HitchhikerTree::open(&base)
                .with_op_buf_size(1_000_000)
                .insert(buffered_key, vec![42], &storage)
                .await?;
            let mut delta = Delta::zero();
            let root = tree.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            Tree::from_hash_with_cache(root, Default::default())
        };
        assert_eq!(
            buffered_tree.get(&buffered_key, &storage).await?,
            Some(vec![42]),
            "precondition: the buffered write reads back before stitching"
        );

        // Stitch the whole tree as a single range piece: the result must hold
        // exactly what the source held.
        let mut delta = Delta::zero();
        let stitched = TransientTree::<[u8; 4], Vec<u8>>::stitch(
            vec![Piece::Range {
                source: &buffered_tree,
                range: [0u8; 4]..=[0xFFu8; 4],
            }],
            &storage,
        )
        .await?
        .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;

        assert_eq!(
            stitched.get(&buffered_key, &storage).await?,
            Some(vec![42]),
            "a stitched piece must keep the buffered ops its source held"
        );
        Ok(())
    }

    /// The case the graft merge actually exercises: stitching PARTIAL ranges,
    /// which go through `carve` and rebuild the trimmed spine. A whole-range
    /// piece short-circuits to the source root, so only partial ranges test
    /// whether carving preserves buffered ops.
    #[dialog_common::test]
    async fn it_preserves_buffered_ops_through_partial_stitch() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..400u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Buffered writes inside the range that will be carved.
        let low = 100u32;
        let high = 300u32;
        let buffered_keys = [150u32, 200, 250];
        let buffered_tree = {
            let mut tree = HitchhikerTree::open(&base).with_op_buf_size(1_000_000);
            for key in buffered_keys {
                tree = tree.insert(key.to_be_bytes(), vec![42], &storage).await?;
            }
            let mut delta = Delta::zero();
            let root = tree.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            Tree::from_hash_with_cache(root, Default::default())
        };

        // Carve the middle out: a partial range, so the spine is rebuilt.
        let mut delta = Delta::zero();
        let stitched = TransientTree::<[u8; 4], Vec<u8>>::stitch(
            vec![Piece::Range {
                source: &buffered_tree,
                range: low.to_be_bytes()..=high.to_be_bytes(),
            }],
            &storage,
        )
        .await?
        .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;

        for key in buffered_keys {
            assert_eq!(
                stitched.get(&key.to_be_bytes(), &storage).await?,
                Some(vec![42]),
                "a carved piece must keep the buffered op at {key}"
            );
        }
        Ok(())
    }

    /// A carve must keep buffered ops for keys ABOVE the stored upper bound of
    /// the subtree holding them.
    ///
    /// `upper_bound` describes stored content only, but `child_for` clamps: a
    /// buffered write for a key past the last child's bound lands in that
    /// child's buffer. If the trim decides which children to drop from stored
    /// bounds alone, it discards children whose buffers hold in-range keys, and
    /// the carved piece silently loses those writes.
    #[dialog_common::test]
    async fn it_preserves_buffered_ops_above_the_stored_bound_through_carve() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..400u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Buffered writes for keys past everything the tree stores.
        let buffered_keys = [10_000u32, 20_000, 30_000];
        let buffered_tree = {
            let mut tree = HitchhikerTree::open(&base).with_op_buf_size(1_000_000);
            for key in buffered_keys {
                tree = tree.insert(key.to_be_bytes(), vec![42], &storage).await?;
            }
            let mut delta = Delta::zero();
            let root = tree.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            Tree::from_hash_with_cache(root, Default::default())
        };
        for key in buffered_keys {
            assert_eq!(
                buffered_tree.get(&key.to_be_bytes(), &storage).await?,
                Some(vec![42]),
                "precondition: the buffered write at {key} reads back before carving"
            );
        }

        // Carve a partial range whose start is inside the stored content and
        // whose end reaches past it, so the buffered keys are in range.
        let mut delta = Delta::zero();
        let stitched = TransientTree::<[u8; 4], Vec<u8>>::stitch(
            vec![Piece::Range {
                source: &buffered_tree,
                range: 200u32.to_be_bytes()..=[0xFFu8; 4],
            }],
            &storage,
        )
        .await?
        .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;

        for key in buffered_keys {
            assert_eq!(
                stitched.get(&key.to_be_bytes(), &storage).await?,
                Some(vec![42]),
                "a carve must keep the buffered op at {key}, above the stored bound"
            );
        }
        Ok(())
    }

    /// Stitching pieces from buffered sources must preserve every op in range,
    /// across many key layouts and both cascade depths.
    ///
    /// The graft merge assembles a tree from several `Piece::Range`s taken from
    /// two buffered sides, so ops can sit at any level of any piece. Single
    /// hand-written cases miss layout-dependent losses; this sweeps seeds and
    /// compares the stitched result against the same content built canonically.
    #[dialog_common::test]
    async fn it_preserves_buffered_ops_across_stitched_pieces() -> Result<()> {
        for seed in 0..30u64 {
            let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

            let base_keys: Vec<u32> = (0..500).map(|_| next() % 50_000).collect();
            let mut base = Tree::empty();
            let mut delta = Delta::zero();
            for key in &base_keys {
                base = base
                    .edit()
                    .insert(key.to_be_bytes(), vec![1], &storage)
                    .await?
                    .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
            }

            for op_buf in [4usize, 1_000_000] {
                // Buffered writes scattered across the key space.
                let writes: Vec<u32> = (0..24).map(|_| next() % 50_000).collect();
                let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(op_buf);
                for key in &writes {
                    buffered = buffered
                        .insert(key.to_be_bytes(), vec![9], &storage)
                        .await?;
                }
                let mut delta = Delta::zero();
                let root = buffered.persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
                let source = Tree::from_hash_with_cache(root, Default::default());

                // Split the key space into three adjacent pieces and stitch them
                // back together: the result must equal the source exactly.
                let cut_a = (16_000u32).to_be_bytes();
                let cut_b = (33_000u32).to_be_bytes();
                let mut delta = Delta::zero();
                let stitched = TransientTree::<[u8; 4], Vec<u8>>::stitch(
                    vec![
                        Piece::Range {
                            source: &source,
                            range: [0u8; 4]..=cut_a,
                        },
                        Piece::Range {
                            source: &source,
                            range: next_key(cut_a)..=cut_b,
                        },
                        Piece::Range {
                            source: &source,
                            range: next_key(cut_b)..=[0xFFu8; 4],
                        },
                    ],
                    &storage,
                )
                .await?
                .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;

                // Every buffered write must read back through the stitched tree.
                for key in &writes {
                    assert_eq!(
                        stitched.get(&key.to_be_bytes(), &storage).await?,
                        Some(vec![9]),
                        "seed {seed}, op_buf {op_buf}: stitched pieces lost the write at {key}"
                    );
                }
                // And every base key not overwritten must survive.
                for key in &base_keys {
                    if writes.contains(key) {
                        continue;
                    }
                    assert_eq!(
                        stitched.get(&key.to_be_bytes(), &storage).await?,
                        Some(vec![1]),
                        "seed {seed}, op_buf {op_buf}: stitched pieces lost base key {key}"
                    );
                }
            }
        }
        Ok(())
    }

    /// The immediate successor of a fixed-width key, for building adjacent
    /// half-open ranges out of inclusive ones.
    fn next_key(mut key: [u8; 4]) -> [u8; 4] {
        for byte in key.iter_mut().rev() {
            if *byte == 0xFF {
                *byte = 0;
            } else {
                *byte += 1;
                break;
            }
        }
        key
    }

    /// Integrating a differential into a buffered tree must land the same
    /// content as integrating into its canonical form.
    ///
    /// `integrate` is how a merge applies screened changes, and it resolves
    /// each change against the batch's own in-flight state (via `get`). With
    /// buffering that state includes ops at any level, so a mis-resolution
    /// silently drops or resurrects facts during a pull.
    #[dialog_common::test]
    async fn it_integrates_into_buffered_like_canonical() -> Result<()> {
        for seed in 0..30u64 {
            let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

            let base_keys: Vec<u32> = (0..400).map(|_| next() % 40_000).collect();
            let mut base = Tree::empty();
            let mut delta = Delta::zero();
            for key in &base_keys {
                base = base
                    .edit()
                    .insert(key.to_be_bytes(), vec![1], &storage)
                    .await?
                    .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
            }

            // Buffered writes, then a differential integrated on top.
            let writes: Vec<u32> = (0..20).map(|_| next() % 40_000).collect();
            let changes: Vec<u32> = (0..20).map(|_| next() % 40_000).collect();

            for op_buf in [4usize, 1_000_000] {
                let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(op_buf);
                for key in &writes {
                    buffered = buffered
                        .insert(key.to_be_bytes(), vec![9], &storage)
                        .await?;
                }
                let mut delta = Delta::zero();
                let root = buffered.persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
                let buffered_tree = Tree::from_hash_with_cache(root, Default::default());

                // The canonical counterpart of the same content.
                let mut flushed = HitchhikerTree::open(&base).with_op_buf_size(op_buf);
                for key in &writes {
                    flushed = flushed.insert(key.to_be_bytes(), vec![9], &storage).await?;
                }
                let mut delta = Delta::zero();
                let canonical_tree = flushed.canonicalize(&storage, &mut delta).await?;
                settle(&mut delta, &mut storage).await?;

                // Integrate the same change stream into both.
                let stream = || {
                    futures_util::stream::iter(changes.iter().map(|key| {
                        Ok(Change::Add(Entry {
                            key: key.to_be_bytes(),
                            value: vec![7],
                        }))
                    }))
                };

                let mut delta = Delta::zero();
                let into_buffered = buffered_tree
                    .edit()
                    .integrate(stream(), &storage)
                    .await?
                    .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;

                let mut delta = Delta::zero();
                let into_canonical = canonical_tree
                    .edit()
                    .integrate(stream(), &storage)
                    .await?
                    .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;

                // Compare content key by key over everything touched.
                let mut probes: Vec<u32> = base_keys.clone();
                probes.extend(writes.iter().copied());
                probes.extend(changes.iter().copied());
                probes.sort_unstable();
                probes.dedup();
                for key in probes {
                    let left = into_buffered.get(&key.to_be_bytes(), &storage).await?;
                    let right = into_canonical.get(&key.to_be_bytes(), &storage).await?;
                    assert_eq!(
                        left, right,
                        "seed {seed}, op_buf {op_buf}: integrate disagreed at key {key}"
                    );
                }
            }
        }
        Ok(())
    }

    /// Editing one key must not disturb ops buffered for OTHER keys.
    ///
    /// An edit descends and reshapes (splitting or regrouping nodes), and the
    /// nodes it dismantles may hold buffered ops for unrelated keys. Those ops
    /// must survive: only the edited key's own pending op is superseded.
    #[dialog_common::test]
    async fn it_keeps_sibling_buffered_ops_across_an_edit() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..400u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![1], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Several buffered writes to keys absent from the base.
        let buffered_keys: Vec<u32> = vec![10_001, 10_002, 10_003, 10_004];
        let tree = {
            let mut t = HitchhikerTree::open(&base).with_op_buf_size(1_000_000);
            for key in &buffered_keys {
                t = t.insert(key.to_be_bytes(), vec![9], &storage).await?;
            }
            let mut delta = Delta::zero();
            let root = t.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            Tree::from_hash_with_cache(root, Default::default())
        };

        // Edit ONE unrelated key through the canonical path.
        let mut delta = Delta::zero();
        let edited = tree
            .edit()
            .insert(50_000u32.to_be_bytes(), vec![5], &storage)
            .await?
            .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;

        for key in &buffered_keys {
            assert_eq!(
                edited.get(&key.to_be_bytes(), &storage).await?,
                Some(vec![9]),
                "an edit to another key must not drop the buffered op at {key}"
            );
        }
        Ok(())
    }

    /// An edit that RESHAPES THE ROOT must not discard the root's own buffer.
    ///
    /// A reshaping edit rebuilds the root from its regrouped children via
    /// `seal_root`. The children carry their own buffers along as nodes, but the
    /// old root node is replaced, so ops buffered AT the root are dropped unless
    /// they are carried across explicitly. The commit path buffers at the root,
    /// so a single reshaping commit silently wipes every write still pending
    /// there.
    #[dialog_common::test]
    async fn it_keeps_the_root_buffer_when_an_edit_reshapes_the_root() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // A base big enough that inserts reshape the upper levels.
        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..600u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![1], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Buffer a batch of writes at the root (a large buffer keeps them there).
        let buffered_keys: Vec<u32> = (0..32).map(|i| 700_000 + i * 37).collect();
        let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(1_000_000);
        for &key in &buffered_keys {
            buffered = buffered
                .insert(key.to_be_bytes(), vec![7], &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        let tree = Tree::from_hash_with_cache(root, Default::default());

        for &key in &buffered_keys {
            assert_eq!(
                tree.get(&key.to_be_bytes(), &storage).await?,
                Some(vec![7]),
                "precondition: the root-buffered write at {key} reads back"
            );
        }

        // Now drive canonical edits until one reshapes the root. Every
        // root-buffered write must still be readable afterwards.
        let mut edited = tree;
        for i in 0..64u32 {
            let mut delta = Delta::zero();
            edited = edited
                .edit()
                .insert((900_000 + i).to_be_bytes(), vec![8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;

            for &key in &buffered_keys {
                assert_eq!(
                    edited.get(&key.to_be_bytes(), &storage).await?,
                    Some(vec![7]),
                    "after edit {i}, the root-buffered write at {key} must survive"
                );
            }
        }
        Ok(())
    }

    /// Counts the leaf segments in a tree, so a test can assert that a
    /// structural split or join actually happened rather than trusting that
    /// equal roots imply it did.
    async fn segment_count(tree: &Tree, storage: &Store) -> Result<usize> {
        use crate::{ArchivedNodeBody, PersistentNode};
        let mut frontier = vec![tree.root().clone()];
        let mut segments = 0usize;
        while let Some(hash) = frontier.pop() {
            if &hash == dialog_common::NULL_BLAKE3_HASH {
                continue;
            }
            let bytes = dialog_storage::StorageBackend::get(storage.backend(), &hash)
                .await?
                .expect("node present");
            let node: PersistentNode<[u8; 4], Vec<u8>> = PersistentNode::new(Buffer::from(bytes));
            match node.body()? {
                ArchivedNodeBody::Index(index) => {
                    for at in 0..index.len() {
                        frontier.push(index.hash_at(at)?.clone());
                    }
                }
                ArchivedNodeBody::Segment(_) => segments += 1,
            }
        }
        Ok(segments)
    }

    /// A key whose rank makes it a segment boundary: inserting it splits the
    /// leaf that would hold it, and removing it forces that leaf to fuse with
    /// its right neighbour.
    fn boundary_key(from: u32, avoid: &[u32]) -> u32 {
        (from..from + 200_000)
            .find(|candidate| {
                !avoid.contains(candidate)
                    && <crate::Geometric as crate::Distribution>::rank(
                        &candidate.to_be_bytes(),
                        &crate::Manifest::default(),
                    ) > crate::BOTTOM_RANK
            })
            .expect("a boundary-ranked key exists in range")
    }

    /// A fact that arrives via novelty and, when flushed all the way to the
    /// leaves, SPLITS the segment it lands in.
    ///
    /// The split is what makes this worth pinning separately from ordinary
    /// buffered inserts: the flush has to re-cut the leaf, and a buffer that
    /// merely delivered its op without reshaping would leave a tree no
    /// canonical build produces.
    #[dialog_common::test]
    async fn it_splits_a_segment_when_buffered_novelty_is_flushed() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // A base of non-boundary keys, so the leaf the split lands in is a plain
        // open run.
        let base_keys: Vec<u32> = (0..400u32)
            .filter(|k| {
                <crate::Geometric as crate::Distribution>::rank(
                    &k.to_be_bytes(),
                    &crate::Manifest::default(),
                ) <= crate::BOTTOM_RANK
            })
            .collect();
        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for key in &base_keys {
            base = base
                .edit()
                .insert(key.to_be_bytes(), vec![1], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }
        let before = segment_count(&base, &storage).await?;

        // A boundary-ranked key inside the base range: inserting it must split.
        let splitter = boundary_key(1, &base_keys);
        assert!(
            splitter < 400,
            "the splitter must land inside the base range, got {splitter}"
        );

        // Canonical reference.
        let mut delta = Delta::zero();
        let canonical = base
            .edit()
            .insert(splitter.to_be_bytes(), vec![9], &storage)
            .await?
            .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        let after_canonical = segment_count(&canonical, &storage).await?;
        assert!(
            after_canonical > before,
            "precondition: the key must split a segment canonically ({before} -> {after_canonical})"
        );

        // Same write through the buffer, flushed all the way.
        let buffered = HitchhikerTree::open(&base)
            .with_op_buf_size(1_000_000)
            .insert(splitter.to_be_bytes(), vec![9], &storage)
            .await?;
        let mut delta = Delta::zero();
        let flushed = buffered.canonicalize(&storage, &mut delta).await?;
        settle(&mut delta, &mut storage).await?;

        assert_eq!(
            segment_count(&flushed, &storage).await?,
            after_canonical,
            "a flushed buffered insert must split the segment like the canonical one"
        );
        assert_eq!(
            flushed.root(),
            canonical.root(),
            "and land on the identical canonical tree"
        );
        Ok(())
    }

    /// A retraction that arrives via novelty and, when flushed all the way,
    /// JOINS two segments.
    ///
    /// Removing a boundary key leaves its leaf without a terminator, so the
    /// orphaned entries must fuse with the right-adjacent leaf. A buffered
    /// retract defers that, so the fuse has to happen when the op finally lands.
    #[dialog_common::test]
    async fn it_joins_segments_when_a_buffered_retraction_is_flushed() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let base_keys: Vec<u32> = (0..400u32).collect();
        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for key in &base_keys {
            base = base
                .edit()
                .insert(key.to_be_bytes(), vec![1], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }
        let before = segment_count(&base, &storage).await?;

        // A boundary key that is actually present, and not the last one (so it
        // has a right neighbour to fuse with).
        let boundary = base_keys
            .iter()
            .copied()
            .find(|k| {
                *k < 399
                    && <crate::Geometric as crate::Distribution>::rank(
                        &k.to_be_bytes(),
                        &crate::Manifest::default(),
                    ) > crate::BOTTOM_RANK
            })
            .expect("the base contains a non-final boundary key");

        // Canonical reference.
        let mut delta = Delta::zero();
        let canonical = base
            .edit()
            .delete(&boundary.to_be_bytes(), &storage)
            .await?
            .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        let after_canonical = segment_count(&canonical, &storage).await?;
        assert!(
            after_canonical < before,
            "precondition: removing the boundary must join segments ({before} -> {after_canonical})"
        );

        // Same retraction through the buffer, flushed all the way.
        let buffered = HitchhikerTree::open(&base)
            .with_op_buf_size(1_000_000)
            .delete(boundary.to_be_bytes(), &storage)
            .await?;
        let mut delta = Delta::zero();
        let flushed = buffered.canonicalize(&storage, &mut delta).await?;
        settle(&mut delta, &mut storage).await?;

        assert_eq!(
            segment_count(&flushed, &storage).await?,
            after_canonical,
            "a flushed buffered retract must join the segments like the canonical delete"
        );
        assert_eq!(
            flushed.root(),
            canonical.root(),
            "and land on the identical canonical tree"
        );
        Ok(())
    }

    /// A canonical edit to a key that is currently shadowed by a buffered op
    /// must win: the edit is the newer write.
    ///
    /// An edit descends to the leaf a key belongs in and writes there, but the
    /// key's live value may sit in an ancestor's buffer. If the edit does not
    /// displace that op, the stale buffered value keeps shadowing the write on
    /// every read, and the edit is silently invisible.
    #[dialog_common::test]
    async fn it_lets_an_edit_override_a_buffered_op() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut base = Tree::empty();
        let mut delta = Delta::zero();
        for i in 0..500u32 {
            base = base
                .edit()
                .insert(i.to_be_bytes(), vec![i as u8], &storage)
                .await?
                .persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
        }

        // Buffer a write high in the tree (big buffer: it stays at the root).
        let key = 42u32.to_be_bytes();
        let buffered = HitchhikerTree::open(&base)
            .with_op_buf_size(1_000_000)
            .insert(key, vec![111], &storage)
            .await?;
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;
        let tree = Tree::from_hash_with_cache(root, Default::default());

        assert_eq!(
            tree.get(&key, &storage).await?,
            Some(vec![111]),
            "buffered write reads back"
        );

        // Now edit the SAME key through the canonical path, as a merge does.
        let mut delta = Delta::zero();
        let edited = tree
            .edit()
            .insert(key, vec![222], &storage)
            .await?
            .persist(&mut delta)?;
        settle(&mut delta, &mut storage).await?;

        assert_eq!(
            edited.get(&key, &storage).await?,
            Some(vec![222]),
            "a canonical edit must override the buffered op it shadows"
        );
        Ok(())
    }

    type SpecTree = PersistentTree<SpecKey, Vec<u8>, DistributionSimulator>;

    async fn flush_spec(
        delta: &mut Delta<Blake3Hash, Buffer>,
        storage: &mut SpecStorage,
    ) -> Result<()> {
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    async fn load_spec_node(
        storage: &SpecStorage,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<SpecKey, Vec<u8>>> {
        let bytes = storage
            .retrieve(hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("node {hash} missing from storage"))?;
        Ok(PersistentNode::new(Buffer::from(bytes)))
    }

    /// The buffered ops sealed into a stored node, owned.
    fn sealed_novelty(
        node: &PersistentNode<SpecKey, Vec<u8>>,
    ) -> Result<Vec<NoveltyEntry<Vec<u8>>>> {
        Ok(match node.body()? {
            ArchivedNodeBody::Index(index) => index.all_novelty::<SpecKey>()?,
            ArchivedNodeBody::Segment(_) => Vec::new(),
        })
    }

    /// Buffered ops parked on the two index subtrees flanking a leaf boundary
    /// must survive the boundary delete that fuses those subtrees at their
    /// LCA: `fuse_subtrees` dismantles both index spines, and the buffers they
    /// carry are pending writes, not shape. (The existing fuse tests fuse
    /// SEGMENTS, which hold no buffers, so they cannot see this.)
    #[dialog_common::test]
    async fn it_keeps_buffered_ops_through_an_lca_fusion() -> Result<()> {
        let spec = tree_spec![
            [                   ..p]
            [        ..h,       ..p]
            [..d, ..h, ..l, ..p]
        ]
        .build(test_storage())
        .await
        .expect("the fixture spec is valid");
        let mut storage = spec.storage().clone();
        let base = SpecTree::from_hash(spec.tree().root().clone());

        let left_parked = encode_key(b"bb", 1, 1);
        let right_parked = encode_key(b"mm", 1, 1);
        let boundary = encode_key(b"h", 2, 1);

        // Park one op on each mid-level index flanking the `h` boundary: the
        // first write buffers at the root, the second overflows the one-op
        // buffer and cascades each op into the mid-level node covering it.
        let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(1);
        buffered = buffered.insert(left_parked, vec![1], &storage).await?;
        buffered = buffered.insert(right_parked, vec![2], &storage).await?;
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        flush_spec(&mut delta, &mut storage).await?;

        // The fixture must actually park both ops BELOW the root.
        let root_node = load_spec_node(&storage, &root).await?;
        assert!(
            sealed_novelty(&root_node)?.is_empty(),
            "fixture: the root buffer must be empty after the cascade"
        );
        let mut parked_count = 0;
        for link in root_node.as_index()?.links()? {
            let child = load_spec_node(&storage, &link.node).await?;
            parked_count += sealed_novelty(&child)?.len();
        }
        assert_eq!(
            parked_count, 2,
            "fixture: both ops must sit in mid-level index buffers"
        );

        let parked: SpecTree = PersistentTree::from_hash(root);
        assert_eq!(
            parked.get(&left_parked, &storage).await?,
            Some(vec![1]),
            "control: the parked op reads back before the fusion"
        );

        // Deleting the boundary key fuses the two mid-level subtrees at the
        // root, their LCA.
        let mut delta = Delta::zero();
        let fused = parked
            .edit()
            .delete(&boundary, &storage)
            .await?
            .persist(&mut delta)?;
        flush_spec(&mut delta, &mut storage).await?;

        assert_eq!(
            fused.get(&left_parked, &storage).await?,
            Some(vec![1]),
            "the left fused subtree's buffered op must survive the fusion"
        );
        assert_eq!(
            fused.get(&right_parked, &storage).await?,
            Some(vec![2]),
            "the right fused subtree's buffered op must survive the fusion"
        );
        assert_eq!(fused.get(&boundary, &storage).await?, None);

        // And survive the full flush.
        let mut delta = Delta::zero();
        let canonical = HitchhikerTree::open(&fused)
            .canonicalize(&storage, &mut delta)
            .await?;
        flush_spec(&mut delta, &mut storage).await?;
        assert_eq!(
            canonical.get(&left_parked, &storage).await?,
            Some(vec![1]),
            "the left parked op must survive canonicalize"
        );
        assert_eq!(
            canonical.get(&right_parked, &storage).await?,
            Some(vec![2]),
            "the right parked op must survive canonicalize"
        );
        assert_eq!(canonical.get(&boundary, &storage).await?, None);
        Ok(())
    }

    /// A delete that strips a single-child root wrapper must carry the
    /// wrapper's buffer onto the surviving root instead of dropping it.
    /// Shaped like
    /// `it_strips_a_persistent_single_child_root_after_rightmost_delete`,
    /// but with an op parked in the root buffer before the delete.
    #[dialog_common::test]
    async fn it_keeps_the_root_buffer_when_a_delete_strips_the_root() -> Result<()> {
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
            flush_spec(&mut delta, &mut storage).await?;
        }

        // Park an op in the ROOT buffer (the default buffer is big enough
        // that nothing cascades).
        let parked = encode_key(b"aa", 1, 1);
        let buffered = HitchhikerTree::open(&tree)
            .insert(parked, vec![7], &storage)
            .await?;
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        flush_spec(&mut delta, &mut storage).await?;

        let root_node = load_spec_node(&storage, &root).await?;
        assert_eq!(
            sealed_novelty(&root_node)?.len(),
            1,
            "fixture: the op must sit in the root buffer"
        );

        // Deleting `b` empties the rightmost subtree, leaving a single-child
        // index-over-index root that `seal_root` strips.
        let reopened: SpecTree = PersistentTree::from_hash(root);
        let mut delta = Delta::zero();
        let stripped = reopened
            .edit()
            .delete(&b, &storage)
            .await?
            .persist(&mut delta)?;
        flush_spec(&mut delta, &mut storage).await?;

        assert_eq!(
            stripped.get(&parked, &storage).await?,
            Some(vec![7]),
            "the stripped wrapper's buffered op must move onto the surviving root"
        );
        assert_eq!(stripped.get(&a, &storage).await?, Some(a.to_vec()));

        let mut delta = Delta::zero();
        let canonical = HitchhikerTree::open(&stripped)
            .canonicalize(&storage, &mut delta)
            .await?;
        flush_spec(&mut delta, &mut storage).await?;
        assert_eq!(
            canonical.get(&parked, &storage).await?,
            Some(vec![7]),
            "the parked op must survive canonicalize"
        );
        assert_eq!(canonical.get(&a, &storage).await?, Some(a.to_vec()));
        Ok(())
    }

    /// For a key buffered at two depths of a stitch source the SHALLOWEST op
    /// is the newest: writes land in the root buffer and a flush only moves
    /// ops downward, so deeper always means older. `concat_levels` lifts the
    /// buffers off the nodes it dismantles at the seam, and the ops lifted by
    /// its recursion come from one level further down than the two joined
    /// nodes' own buffers. Re-attaching them after the joined nodes' own ops
    /// lets the stable re-sort leave the deeper, older op to win a same-key
    /// collision, resurrecting a superseded value in the stitched tree.
    #[dialog_common::test]
    async fn it_resolves_the_shallowest_op_across_a_stitch_seam() -> Result<()> {
        let spec = tree_spec![
            [                   ..p]
            [        ..h,       ..p]
            [..d, ..h, ..l, ..p]
        ]
        .build(test_storage())
        .await
        .expect("the fixture spec is valid");
        let mut storage = spec.storage().clone();
        let base = SpecTree::from_hash(spec.tree().root().clone());

        // The probe lives under the RIGHTMOST mid-level index, the spine the
        // stitch seam below dismantles. The far key lives under the other
        // mid-level index and exists only to overflow the one-op root buffer,
        // cascading the probe's first op down into the mid node covering it.
        let probe = encode_key(b"k", 1, 1);
        let far = encode_key(b"b", 1, 1);

        let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(1);
        buffered = buffered.insert(probe, vec![1], &storage).await?;
        buffered = buffered.insert(far, vec![9], &storage).await?;
        buffered = buffered.insert(probe, vec![2], &storage).await?;
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        flush_spec(&mut delta, &mut storage).await?;

        // The fixture must genuinely hold the probe's ops at two levels: the
        // newer op in the root buffer, the older one sealed into the rightmost
        // mid-level index. A fixture with both ops at one depth pins nothing.
        let root_node = load_spec_node(&storage, &root).await?;
        assert!(
            sealed_novelty(&root_node)?
                .iter()
                .any(|entry| entry.key == probe.to_vec() && entry.op == NoveltyOp::Assert(vec![2])),
            "fixture: the newer op must sit in the root buffer"
        );
        let links = root_node.as_index()?.links()?;
        let seam_child = links.last().expect("the fixture root has children");
        let seam_node = load_spec_node(&storage, &seam_child.node).await?;
        assert!(
            sealed_novelty(&seam_node)?
                .iter()
                .any(|entry| entry.key == probe.to_vec() && entry.op == NoveltyOp::Assert(vec![1])),
            "fixture: the older op must sit in the rightmost mid-level buffer"
        );

        // Stitch the whole buffered source next to a run of entries past its
        // last key: the join runs down the source's rightmost spine, lifting
        // both the root's buffer and the mid node's seam buffer.
        let source: SpecTree = PersistentTree::from_hash(root);
        let after = encode_key(b"t", 1, 1);
        let stitched = TransientTree::stitch(
            vec![
                Piece::Range {
                    source: &source,
                    range: [0u8; 8]..=[0xffu8; 8],
                },
                Piece::Entries(vec![Entry {
                    key: after,
                    value: vec![7],
                }]),
            ],
            &storage,
        )
        .await?;
        let mut delta = Delta::zero();
        let stitched = stitched.persist(&mut delta)?;
        flush_spec(&mut delta, &mut storage).await?;

        assert_eq!(
            stitched.get(&probe, &storage).await?,
            Some(vec![2]),
            "the stitch must resolve the shallower (newer) op at the seam"
        );
        assert_eq!(stitched.get(&far, &storage).await?, Some(vec![9]));
        assert_eq!(stitched.get(&after, &storage).await?, Some(vec![7]));
        Ok(())
    }
}
