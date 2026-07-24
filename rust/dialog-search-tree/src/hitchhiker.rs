//! Hitchhiker (fractal / Bε) tree write buffering over the search tree.
//!
//! A [`HitchhikerTree`] holds the same content-addressed spine a
//! [`PersistentTree`] does, but every index node may carry a bounded buffer of
//! pending ops (its `novelty`). A write appends an op to the root's buffer;
//! when a buffer overflows it cascades one level down toward the leaves. The
//! expensive canonical rebuild is amortized across many writes instead of paid
//! eagerly on every one.
//!
//! The algorithm mirrors the reference hitchhiker tree's `enqueue`, with the
//! buffer grouped per child link (see [`crate::Novelty`]):
//!
//! - A **leaf** does not buffer. Ops that reach a leaf are deferred and applied
//!   through the canonical [`TransientTree`] insert/delete path, which reshapes
//!   exactly as a sequential edit would.
//! - An **index** routes each arriving op to the child link covering it (one
//!   binary search over the separators, the same rule stored routing uses) and
//!   merges it into that link's buffer, newest op for a key last.
//! - An **index that overflows** cascades one level down: each child receives
//!   its own link's buffer verbatim (the grouping already happened at enqueue,
//!   so there is no partition step) and the node's buffers are left empty.
//!
//! [`canonicalize`](HitchhikerTree::canonicalize) forces a full flush: every
//! buffer is pushed all the way to the leaves, leaving all `novelty` empty. The
//! result is the deterministic, history-independent canonical tree, byte for
//! byte identical to a sequential build of the same fact set.

use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};

use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_util::Stream;
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
    Accessor, ArchivedNodeBody, Buffer, Cache, ContentAddressedStorage, Delta,
    DialogSearchTreeError, Distribution, Entry, Geometric, Key, Manifest, Node, NoveltyEntry,
    NoveltyOp, PersistentNode, PersistentTree, TransientNode, TransientRootParts, TransientSegment,
    TransientTree, Value, link_bounds,
};

/// The default per-node novelty capacity.
///
/// Calibrated on the on-disk bug-tracker benchmark (300 bugs, six-field concept
/// join): 64 -> 66ms, 256 -> 57ms, 1024 -> 111ms on the all-bugs query. Too
/// small and writes cascade constantly; too large and every read that crosses a
/// buffered node pays to project a big buffer over the leaves beneath it.
///
/// Hitchhiker trees run buffers several times the fan-out so most writes touch
/// only the upper buffers. The base fan-out here (the geometric distribution's
/// expected children per node) is around 254; a multiple of that keeps the
/// amortization a hitchhiker buffer is meant to provide. Tunable per tree via
/// [`HitchhikerTree::with_op_buf_size`].
pub const DEFAULT_OP_BUF_SIZE: usize = 256;

/// A boxed future returning an edited [`TransientNode`], the shape `enqueue`
/// returns so its recursion can be expressed as a plain `async fn` body inside a
/// `Box::pin`.
/// A future carrying the platform's conditional send-ness.
///
/// [`ConditionalSend`] cannot appear in a `dyn` bound directly (it is not an
/// auto trait), so it is lifted into a supertrait that can: the blanket impl
/// below makes every qualifying future one of these, and on wasm
/// `ConditionalSend` is vacuous, so the bound disappears exactly as it should.
pub trait ConditionalSendFuture: std::future::Future + ConditionalSend {}

impl<F> ConditionalSendFuture for F where F: std::future::Future + ConditionalSend {}

type NodeFuture<'a, Key, Value> = std::pin::Pin<
    Box<
        dyn ConditionalSendFuture<Output = Result<TransientNode<Key, Value>, DialogSearchTreeError>>
            + 'a,
    >,
>;

/// A boxed unit future, the shape the recursive novelty drain returns.
type UnitFuture<'a> =
    std::pin::Pin<Box<dyn ConditionalSendFuture<Output = Result<(), DialogSearchTreeError>> + 'a>>;

/// A boxed boolean future, the shape the recursive novelty probe returns.
type BoolFuture<'a> = std::pin::Pin<
    Box<dyn ConditionalSendFuture<Output = Result<bool, DialogSearchTreeError>> + 'a>,
>;

/// How far an overflowing buffer cascades, selecting the tree's write behavior.
///
/// One core mechanism (route ops into novelty, flush on overflow) yields three
/// behaviors by choosing the cascade depth:
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlushPolicy {
    /// True hitchhiker buffering: an overflowing buffer flushes one level down,
    /// so work is amortized across levels. The write-optimal default.
    #[default]
    Amortized,
    /// An overflowing buffer flushes straight through to the leaves rather than
    /// one level at a time, keeping the tree close to canonical with shallower
    /// buffering. Equivalent to canonicalizing on each flush trigger.
    Recursive,
    /// Never buffer: every write goes straight to the canonical tree, i.e. the
    /// unbuffered behavior. A baseline and the trivial degenerate policy.
    Immediate,
}

/// What makes a node's buffer flush.
///
/// The trigger is independent of [`FlushPolicy`], which says how far a flush
/// cascades once it starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlushTrigger {
    /// Flush when the buffer's total occupancy would exceed its capacity.
    ///
    /// Simple, but blind to how the ops are distributed: a full buffer whose ops
    /// scatter across every child pays a rewrite per child to move a couple of
    /// ops each.
    #[default]
    Capacity,
    /// Flush when the ops bound for any single child reach `capacity / children`.
    ///
    /// Sizes the trigger to the node's own fan-out, which varies enormously by
    /// level (measured: ~4 at the root of a large tree, ~185 one level down).
    /// A node with few children tolerates far more buffering before flushing,
    /// since a flush there scatters into few targets and is cheap to defer; a
    /// wide node flushes as soon as one target has a batch worth writing.
    ///
    /// Under uniformly scattered keys this behaves much like [`Capacity`], since
    /// a child reaches its share about when the buffer fills. The two diverge
    /// under skew, which is exactly when flushing early is worth it: the ops are
    /// concentrated, so one edge carries a real batch.
    ///
    /// `floor` keeps the per-child threshold from collapsing on wide nodes,
    /// where `capacity / children` can fall to one or two ops and flush on
    /// nearly every write.
    PerChild {
        /// Smallest per-child batch that may trigger a flush.
        floor: usize,
    },
}

/// The root of a [`HitchhikerTree`], loaded lazily like a [`TransientTree`]'s.
enum HitchhikerRoot<Key, Value> {
    /// The durable root hash, not yet loaded. `NULL_BLAKE3_HASH` is an empty
    /// tree.
    Unloaded(Blake3Hash),
    /// The root loaded into a live, buffered transient node.
    Loaded(TransientNode<Key, Value>),
}

/// A write-buffered search tree.
///
/// Like [`PersistentTree`] it is backed by content-addressed storage and a node
/// [`Cache`], but it keeps a live spine across writes so buffered ops accumulate
/// in node `novelty` and cascade lazily. Seal it back into a canonical
/// [`PersistentTree`] with [`canonicalize`](Self::canonicalize), or serialize
/// the buffered form as-is with [`persist`](Self::persist).
pub struct HitchhikerTree<Key, Value, D = Geometric>
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    root: HitchhikerRoot<Key, Value>,
    cache: Cache<Blake3Hash, Buffer>,
    op_buf_size: usize,
    policy: FlushPolicy,
    trigger: FlushTrigger,
    /// The tree's format header, captured from the root node the first time a
    /// write or canonicalize loads it (opening is synchronous and cannot read
    /// it). `None` until then, and forever for a tree born empty, whose first
    /// canonical write stamps the default format. Threaded into every replay
    /// and persist so the buffered path re-shapes and re-stamps under the
    /// tree's own format, mirroring the guard `TransientTree::load` enforces
    /// on the canonical edit path.
    manifest: Option<Manifest>,
    distribution: PhantomData<D>,
}

impl<Key, Value, D> HitchhikerTree<Key, Value, D>
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
    /// Opens a buffered tree over the root of `tree`, sharing its node cache.
    ///
    /// The root is held as its (possibly null) hash and loaded lazily by the
    /// first write that descends into it, so this is synchronous and touches no
    /// storage.
    pub fn open(tree: &PersistentTree<Key, Value, D>) -> Self {
        Self {
            root: HitchhikerRoot::Unloaded(tree.root().clone()),
            cache: tree.node_cache(),
            op_buf_size: DEFAULT_OP_BUF_SIZE,
            policy: FlushPolicy::default(),
            trigger: FlushTrigger::default(),
            manifest: None,
            distribution: PhantomData,
        }
    }

    /// Opens an empty buffered tree.
    pub fn empty() -> Self {
        Self {
            root: HitchhikerRoot::Unloaded(NULL_BLAKE3_HASH.clone()),
            cache: Cache::new(),
            op_buf_size: DEFAULT_OP_BUF_SIZE,
            policy: FlushPolicy::default(),
            trigger: FlushTrigger::default(),
            manifest: None,
            distribution: PhantomData,
        }
    }

    /// Sets the per-node novelty capacity (the write-amplification knob).
    pub fn with_op_buf_size(mut self, op_buf_size: usize) -> Self {
        self.op_buf_size = op_buf_size.max(1);
        self
    }

    /// Sets the [`FlushPolicy`] selecting how far an overflowing buffer cascades.
    pub fn with_flush_policy(mut self, policy: FlushPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Selects what makes a buffer flush (see [`FlushTrigger`]).
    pub fn with_flush_trigger(mut self, trigger: FlushTrigger) -> Self {
        self.trigger = trigger;
        self
    }

    /// Buffers an insert (or value update) of `key` into the tree.
    pub async fn insert<Backend>(
        self,
        key: Key,
        value: Value,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.write(
            vec![NoveltyEntry {
                key: key.as_ref().to_vec(),
                op: NoveltyOp::Assert(value),
            }],
            storage,
        )
        .await
    }

    /// Buffers a delete (tombstone) of `key` into the tree.
    pub async fn delete<Backend>(
        self,
        key: Key,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.write(
            vec![NoveltyEntry {
                key: key.as_ref().to_vec(),
                op: NoveltyOp::Retract,
            }],
            storage,
        )
        .await
    }

    /// Enqueues a batch of ops into the tree, cascading buffers one level on
    /// overflow.
    ///
    /// Mirrors the reference `enqueue`: the ops are routed into the root's
    /// novelty buffer (or cascaded down on overflow), and any ops that reach a
    /// leaf are collected and applied afterward through the canonical
    /// [`TransientTree`] insert/delete path so leaf landings reshape exactly as
    /// a sequential edit would.
    async fn write<Backend>(
        mut self,
        msgs: Vec<NoveltyEntry<Value>>,
        storage: &ContentAddressedStorage<Backend>,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        let accessor = Accessor::new(self.cache.clone(), storage.clone());

        // An empty tree has no node to buffer into; every op goes straight to
        // the (initially empty) canonical tree.
        let loaded = match self.root {
            HitchhikerRoot::Loaded(node) => Some(node),
            HitchhikerRoot::Unloaded(ref hash) if hash == NULL_BLAKE3_HASH => None,
            HitchhikerRoot::Unloaded(ref hash) => {
                let node: PersistentNode<Key, Value> = accessor.get_node(hash).await?;
                // Capture the tree's format header at the first root load,
                // where storage is in hand: the synchronous `open` cannot read
                // it, and every replay and persist below must run under the
                // tree's own format rather than the default.
                self.manifest = Some(node.manifest()?);
                // Measurement-only (uncommitted, env-gated) lift breadcrumb.
                dialog_storage::dup_audit::note_lift(hash.as_bytes(), "root_open");
                // The root's left edge is the tree's global leftmost seam,
                // whose separator is the empty string (negative infinity).
                Some(TransientNode::open(&node, Vec::new())?)
            }
        };

        let mut deferred = Vec::new();
        let node = match loaded {
            // Immediate never buffers: every op goes straight to the canonical
            // edit path, the unbuffered baseline behavior.
            Some(node) if self.policy == FlushPolicy::Immediate => {
                deferred = msgs;
                Some(node)
            }
            Some(node) => Some(
                enqueue::<Key, Value, D, Backend>(
                    node,
                    msgs,
                    self.op_buf_size,
                    self.policy,
                    self.trigger,
                    &mut deferred,
                    &accessor,
                )
                .await?,
            ),
            None => {
                // No spine to buffer into yet: defer everything to the leaf path.
                deferred = msgs;
                None
            }
        };

        // Apply the deferred (leaf-bound) ops to the live spine in memory through
        // the canonical edit path, with no serialization round-trip. The buffered
        // upper nodes ride along untouched; only the leaves the ops reach are
        // reshaped, exactly as a sequential edit would reshape them.
        let edit = match node {
            Some(node) => TransientTree::<Key, Value, D>::from_loaded(
                node,
                self.cache.clone(),
                self.manifest.unwrap_or_default(),
            ),
            None => {
                TransientTree::<Key, Value, D>::new(NULL_BLAKE3_HASH.clone(), self.cache.clone())
            }
        };
        let edit = replay_ops(edit, deferred, storage).await?;
        self.root = match edit.into_root() {
            TransientRootParts::Loaded(node) => HitchhikerRoot::Loaded(node),
            TransientRootParts::Unloaded(hash) => HitchhikerRoot::Unloaded(hash),
        };
        Ok(self)
    }

    /// Flushes every buffer to the leaves and seals the result into a canonical
    /// [`PersistentTree`], writing new nodes into `delta`.
    ///
    /// `canonicalize` is a forced full flush: it drains all `novelty` through
    /// the canonical edit path so the returned tree is deterministic and
    /// history-independent, the same root a sequential build of the surviving
    /// fact set produces. The buffered tree is consumed.
    pub async fn canonicalize<Backend>(
        self,
        storage: &ContentAddressedStorage<Backend>,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<PersistentTree<Key, Value, D>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        // Collect every buffered op across the whole spine in key order, clearing
        // the buffers as we go, then replay them as canonical edits onto the
        // de-buffered base in memory. A canonical edit reshapes exactly as a
        // sequential insert/delete, so the result is the canonical tree.
        //
        // The drain is post-order (children before their parent's own buffer),
        // so for any key the deepest op comes first. Ops flow root to leaf and
        // deeper therefore means older, so after the stable sort by key the
        // SHALLOWEST (newest) op for a key sits last, and last-write-wins
        // replay lets exactly that op stand.
        let accessor = Accessor::new(self.cache.clone(), storage.clone());
        let edit = match self.root {
            HitchhikerRoot::Unloaded(hash) => {
                // A cold reopen of a persisted buffered tree arrives here with
                // its buffers sealed in the stored bytes, so the root must be
                // inspected rather than passed through verbatim. A tree with
                // no novelty anywhere is already canonical (an empty buffer is
                // byte-identical to a canonical node's) and keeps its hash
                // with no rewrite.
                if &hash == NULL_BLAKE3_HASH
                    || !subtree_has_novelty::<Key, Value, Backend>(&hash, &accessor).await?
                {
                    TransientTree::<Key, Value, D>::new(hash, self.cache.clone())
                } else {
                    let root: PersistentNode<Key, Value> = accessor.get_node(&hash).await?;
                    // The replay must re-shape and re-stamp under the tree's
                    // own format, read from the root it is inlined into.
                    let manifest = root.manifest()?;
                    let mut node = TransientNode::open(&root, Vec::new())?;
                    let mut ops = Vec::new();
                    drain_novelty(&mut node, &mut ops, &accessor).await?;
                    ops.sort_by(|a, b| a.key.cmp(&b.key));
                    let edit = TransientTree::<Key, Value, D>::from_loaded(
                        node,
                        self.cache.clone(),
                        manifest,
                    );
                    replay_ops(edit, ops, storage).await?
                }
            }
            HitchhikerRoot::Loaded(mut node) => {
                let mut ops = Vec::new();
                drain_novelty(&mut node, &mut ops, &accessor).await?;
                ops.sort_by(|a, b| a.key.cmp(&b.key));
                let edit = TransientTree::<Key, Value, D>::from_loaded(
                    node,
                    self.cache.clone(),
                    self.manifest.unwrap_or_default(),
                );
                replay_ops(edit, ops, storage).await?
            }
        };
        edit.persist(delta)
    }

    /// Retrieves the value associated with `key`, merging buffered ops over the
    /// stored leaves.
    ///
    /// The descent reads exactly what [`canonicalize`](Self::canonicalize) would
    /// produce. Ops flow root to leaf, so a buffered op closer to the root is
    /// more recent: at each index node on the path the node's `novelty` is
    /// consulted first, and the highest covering op wins. A buffered
    /// [`Retract`](NoveltyOp::Retract) hides the underlying value; a buffered
    /// [`Assert`](NoveltyOp::Assert) shadows it. With no covering buffered op the
    /// stored leaf value stands, read through the same path
    /// [`PersistentTree::get`] uses for untouched subtrees.
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
            HitchhikerRoot::Unloaded(hash) => {
                return self.persistent_get(hash, key, storage).await;
            }
            HitchhikerRoot::Loaded(node) => node,
        };

        loop {
            match node {
                TransientNode::Index(index) => {
                    // A higher buffer holds the most recent op, so a covering op
                    // here wins over anything deeper. Ops ride the link that
                    // routes them, so the descended link's buffer is the only
                    // one that can cover the key.
                    let at = child_index::<Key, Value>(&index.children, key.as_ref())?;
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

    /// Streams the entries in `range` in key order, merging buffered ops over
    /// the stored leaves.
    ///
    /// The novelty-aware counterpart of [`PersistentTree::stream_range`], and
    /// the scan a read-modify-write caller must use: a slot scan blind to the
    /// buffers would miss a prior that a recent write has only buffered, and so
    /// would fail to supersede it.
    ///
    /// Ops flow root to leaf, so for any key the covering op nearest the root is
    /// the most recent. The spine's buffers are collected in one descent (they
    /// are bounded by `op_buf_size` per node and by the tree's height), reduced
    /// to one winning op per key, and merged over the persistent stream: an
    /// [`Assert`](NoveltyOp::Assert) replaces or inserts an entry, a
    /// [`Retract`](NoveltyOp::Retract) hides one. The result is exactly what the
    /// same range would yield after [`canonicalize`](Self::canonicalize).
    pub fn stream_range<R, Backend>(
        &self,
        range: R,
        storage: &ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        R: RangeBounds<Key> + ConditionalSend,
        Key: Clone,
        Value: Clone,
    {
        // The winning buffered op per key across the whole spine, in key order.
        // Collected up front (not inside the stream) so the returned stream owns
        // everything it touches and borrows nothing from `self`. A decode
        // error (a malformed sealed buffer) is carried into the stream and
        // surfaced as its first item.
        let mut pending: Vec<NoveltyEntry<Value>> = Vec::new();
        let collected: Result<(), DialogSearchTreeError> = match &self.root {
            HitchhikerRoot::Loaded(node) => {
                collect_novelty_in_range::<Key, Value, R>(node, &range, &mut pending).map(|_| {
                    pending.sort_by(|left, right| left.key.cmp(&right.key));
                    // Root-most wins, and `collect_novelty_in_range` pushes in
                    // root-to-leaf order, so the FIRST op for a key is the
                    // winner.
                    pending.dedup_by(|later, earlier| later.key == earlier.key);
                })
            }
            HitchhikerRoot::Unloaded(_) => Ok(()),
        };

        let base = self.persistent_range(range, storage);

        async_stream::try_stream! {
            futures_util::pin_mut!(base);

            collected?;
            let mut buffered = pending.into_iter().peekable();
            let mut stored = futures_util::StreamExt::next(&mut base).await.transpose()?;

            loop {
                // Emit whichever side is next in key order; where both hold the
                // same key the buffered op wins and the stored entry is dropped.
                // A buffered op carries raw key bytes and a stored entry a typed
                // key; `Key`'s order agrees with its bytes, so they compare
                // through `as_ref`.
                let take_buffered = match (buffered.peek(), &stored) {
                    (None, _) => false,
                    (Some(_), None) => true,
                    (Some(op), Some(entry)) => op.key.as_slice() <= entry.key.as_ref(),
                };

                if take_buffered {
                    let op = buffered.next().expect("peeked");
                    if stored.as_ref().is_some_and(|entry| entry.key.as_ref() == op.key.as_slice()) {
                        stored = futures_util::StreamExt::next(&mut base).await.transpose()?;
                    }
                    if let NoveltyOp::Assert(value) = op.op {
                        yield Entry { key: Key::try_from_bytes(&op.key)?, value };
                    }
                } else {
                    match stored.take() {
                        Some(entry) => {
                            yield entry;
                            stored = futures_util::StreamExt::next(&mut base).await.transpose()?;
                        }
                        None => break,
                    }
                }
            }
        }
    }

    /// Streams `range` over this tree's stored entries, ignoring every buffer.
    ///
    /// The spine is handed to a [`TransientTree`], which already knows how to
    /// stream a half-loaded tree (persistent subtrees by hash, live leaves from
    /// memory) and is blind to `novelty` by construction, so this yields exactly
    /// the pre-buffer state that [`stream_range`](Self::stream_range) merges
    /// buffered ops over.
    fn persistent_range<R, Backend>(
        &self,
        range: R,
        storage: &ContentAddressedStorage<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        R: RangeBounds<Key> + ConditionalSend,
    {
        // Snapshot the spine into an owned plan (persistent subtrees as hashes,
        // live leaf entries cloned) so the stream borrows nothing from `self`.
        // Untouched subtrees stay hashes, so nothing below the spine is copied.
        let cache = self.cache.clone();
        let storage = storage.clone();
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());

        let plan = match &self.root {
            HitchhikerRoot::Unloaded(hash) => vec![StoredStep::Subtree(hash.clone())],
            HitchhikerRoot::Loaded(node) => {
                let mut plan = Vec::new();
                collect_stored_plan(node, &bounds, &mut plan);
                plan
            }
        };

        async_stream::try_stream! {
            for step in plan {
                match step {
                    StoredStep::Subtree(hash) => {
                        let accessor = Accessor::new(cache.clone(), storage.clone());
                        let inner = crate::TreeWalker::<Key, Value>::new(hash)
                            .stream(bounds.clone(), accessor);
                        futures_util::pin_mut!(inner);
                        while let Some(entry) = futures_util::StreamExt::next(&mut inner).await {
                            yield entry?;
                        }
                    }
                    StoredStep::Entry(entry) => {
                        if bounds.contains(&entry.key) {
                            yield entry;
                        }
                    }
                }
            }
        }
    }

    /// Delegates a point lookup over a fully persistent subtree to
    /// [`PersistentTree::get`], so reads of an untouched subtree match the
    /// persistent read exactly.
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

    /// Serializes the buffered tree as-is (buffers intact) into `delta`,
    /// returning its root hash. The result is not canonical until
    /// [`canonicalize`](Self::canonicalize) has flushed the buffers.
    pub fn persist(
        self,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<Blake3Hash, DialogSearchTreeError> {
        match self.root {
            HitchhikerRoot::Unloaded(hash) => Ok(hash),
            // Every node carries the tree's format header. A loaded root means
            // a write loaded it, and that load captured the tree's own
            // manifest; a tree born empty in this process has no stored header
            // yet and takes the default, which is exactly what its first
            // canonical write would stamp.
            HitchhikerRoot::Loaded(node) => Ok(node
                .persist(delta, &self.manifest.unwrap_or_default())?
                .hash()
                .clone()),
        }
    }
}

/// Routes `msgs` into the subtree rooted at `node`, cascading one level on
/// overflow, collecting leaf-bound ops into `deferred`.
///
/// This is the faithful counterpart of the reference hitchhiker `enqueue`,
/// over the per-link grouped buffer:
///
/// - **Leaf**: a segment buffers nothing; its `msgs` are appended to `deferred`
///   for the caller to apply through the canonical edit path.
/// - **Index with room**: each op in `msgs` is routed to the child link
///   covering it (child `at` takes `[sep(at), sep(at + 1))`, the last child
///   runs open-ended, and a key below every separator clamps into the
///   leftmost child, matching how the read descent routes) and merged into
///   that link's buffer, kept sorted with the newest op for a key last.
/// - **Index overflow**: each child receives its own link's buffer verbatim
///   via a recursive one-level `enqueue`. The grouping already happened at
///   enqueue, so a flush partitions nothing.
fn enqueue<'a, Key, Value, D, Backend>(
    node: TransientNode<Key, Value>,
    msgs: Vec<NoveltyEntry<Value>>,
    op_buf_size: usize,
    policy: FlushPolicy,
    trigger: FlushTrigger,
    deferred: &'a mut Vec<NoveltyEntry<Value>>,
    accessor: &'a Accessor<Backend>,
) -> NodeFuture<'a, Key, Value>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    D: Distribution,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    Box::pin(async move {
        let mut node = node;
        let index = match &mut node {
            // A leaf buffers nothing: defer its ops to the canonical edit path.
            TransientNode::Segment(_) => {
                deferred.extend(msgs);
                return Ok(node);
            }
            TransientNode::Index(index) => index,
        };

        // Route the arriving ops to their links up front: this is the one
        // grouping the design has, shared by reads, flushes, and the stored
        // form, so it happens exactly once per op.
        {
            let bounds = link_bounds(&index.children)?;
            index.novelty.route::<Key>(&bounds, msgs)?;
        }

        // Does this node flush? `Capacity` asks whether the buffer
        // overflowed; `PerChild` asks whether any one child now has a batch
        // worth writing, which scales the decision to this node's own
        // fan-out. Both read lengths the grouped buffer already tracks.
        let flushes = match trigger {
            FlushTrigger::Capacity => index.novelty.len() > op_buf_size,
            FlushTrigger::PerChild { floor } => {
                // Never exceed the buffer's capacity, whatever the per-child
                // threshold works out to.
                if index.novelty.len() > op_buf_size {
                    true
                } else {
                    let children = index.children.len().max(1);
                    let threshold = (op_buf_size / children).max(floor).max(1);
                    index.novelty.peak() >= threshold
                }
            }
        };

        // Room: the ops stay where routing put them.
        if !flushes {
            return Ok(node);
        }

        // Overflow: cascade one level into the children — lifting ON
        // DEMAND, only where a non-empty buffer actually descends. A child
        // with nothing to take stays `Node::Persistent`: at persist,
        // `into_link` passes its link through with no decode, no re-encode,
        // no re-hash, and no delta entry, where lifting every child up
        // front turned each untouched sibling into a byte-identical
        // re-store (the measured 24-26% duplicate-block share of sealed
        // bytes; see bead dialog-db-59). Provenance is the cheap, exact
        // signal here — store identity is not observable at persist time.
        let child_count = index.children.len();
        for at in 0..child_count {
            let took = index.novelty.take_link::<Key>(at)?;
            if took.is_empty() {
                continue;
            }
            lift_child(&mut index.children[at], accessor).await?;

            let child = std::mem::replace(
                &mut index.children[at],
                Node::Transient(TransientNode::Segment(TransientSegment {
                    entries: Vec::new(),
                    separator: Vec::new(),
                })),
            )
            .into_transient()?;
            // Amortized cascades one level: the child buffers normally. Recursive
            // flushes through to the leaves: the child gets a zero-size buffer so
            // it overflows immediately and keeps pushing down until the ops land
            // in leaves.
            let child_buf_size = match policy {
                FlushPolicy::Recursive => 0,
                _ => op_buf_size,
            };
            let updated = enqueue::<Key, Value, D, Backend>(
                child,
                took,
                child_buf_size,
                policy,
                trigger,
                deferred,
                accessor,
            )
            .await?;
            index.children[at] = Node::Transient(updated);
        }

        Ok(node)
    })
}

/// Lifts `child` from a [`Node::Persistent`] reference into editable transient
/// form, loading it from storage; a transient child is left untouched.
async fn lift_child<Key, Value, Backend>(
    child: &mut Node<Key, Value>,
    accessor: &Accessor<Backend>,
) -> Result<(), DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    if let Node::Persistent(link) = child {
        let persistent = accessor.get_node(&link.node).await?;
        // The link carries the seam at the child's left edge; a lifted segment
        // must keep it, since it is the ground truth every level above derives
        // its separators from.
        let separator = link.separator.clone();
        *child = TransientNode::open(&persistent, separator)?.into();
    }
    Ok(())
}

/// Appends every buffered op whose key falls in `range` to `pending`, walking
/// the live spine root to leaf.
///
/// Push order is root-most first, which is the precedence order: ops flow
/// downward, so a shallower buffer holds the more recent op for a key. Within a
/// single node's buffer the last entry wins; the per-link collection walks the
/// links in child order, which is key order, and a sealed link decodes only its
/// winners. Persistent children are skipped: their buffers, if any, were
/// sealed into the stored bytes and are read back by
/// [`persistent_range`](HitchhikerTree::persistent_range) as part of the base.
fn collect_novelty_in_range<Key, Value, R>(
    node: &TransientNode<Key, Value>,
    range: &R,
    pending: &mut Vec<NoveltyEntry<Value>>,
) -> Result<(), DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value + Clone,
    R: RangeBounds<Key>,
{
    let TransientNode::Index(index) = node else {
        return Ok(());
    };

    // A buffered key is raw bytes and a range bound is a typed key; `Key`'s
    // order agrees with its bytes, so the two compare through `as_ref`.
    let start = match range.start_bound() {
        Bound::Included(start) => Bound::Included(start.as_ref()),
        Bound::Excluded(start) => Bound::Excluded(start.as_ref()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let end = match range.end_bound() {
        Bound::Included(end) => Bound::Included(end.as_ref()),
        Bound::Excluded(end) => Bound::Excluded(end.as_ref()),
        Bound::Unbounded => Bound::Unbounded,
    };
    index
        .novelty
        .collect_winners_in_range::<Key>(start, end, pending)?;

    // Descend only into children whose span can intersect the range, so the
    // walk costs the span rather than the spine.
    //
    // Separators are lower bounds: child `at` spans `[sep(at), sep(at + 1))`
    // and the last child runs open-ended.
    for (at, child) in index.children.iter().enumerate() {
        let lower = child.separator().ok();
        let upper = index
            .children
            .get(at + 1)
            .and_then(|next| next.separator().ok());

        // An open-ended right edge always reaches the range's start; otherwise
        // the range must begin strictly below the next child's separator.
        let above_start = match (upper, range.start_bound()) {
            (Some(upper), Bound::Included(start)) => start.as_ref() < upper,
            (Some(upper), Bound::Excluded(start)) => start.as_ref() < upper,
            _ => true,
        };
        // The child begins at its own separator. An unreadable separator is
        // kept rather than silently dropped.
        let below_end = match (lower, range.end_bound()) {
            (Some(lower), Bound::Included(end)) => lower <= end.as_ref(),
            (Some(lower), Bound::Excluded(end)) => lower < end.as_ref(),
            _ => true,
        };

        if above_start
            && below_end
            && let Node::Transient(child) = child
        {
            collect_novelty_in_range(child, range, pending)?;
        }
    }
    Ok(())
}

/// One step of a stored-entry stream over a live spine.
enum StoredStep<Key, Value> {
    /// A persistent subtree to stream by its root hash.
    Subtree(Blake3Hash),
    /// An owned entry cloned from a live leaf.
    Entry(Entry<Key, Value>),
}

/// Walks the live spine left to right, appending each persistent subtree (as a
/// hash) and each live leaf entry (cloned) to `plan` in ascending key order.
///
/// Buffers are deliberately skipped: this produces the tree's *stored* state,
/// which [`stream_range`](HitchhikerTree::stream_range) merges buffered ops over.
fn collect_stored_plan<Key, Value, R>(
    node: &TransientNode<Key, Value>,
    bounds: &R,
    plan: &mut Vec<StoredStep<Key, Value>>,
) where
    Key: self::Key + Clone,
    Value: self::Value + Clone,
    R: RangeBounds<Key>,
{
    match node {
        TransientNode::Index(index) => {
            // Skip whole subtrees that cannot intersect the range. Without this
            // a bounded scan still enumerates every node in the tree, so its
            // cost grows with the tree rather than with the range: a slot scan
            // over one (entity, attribute) pair ends up linear in history.
            //
            // Separators are lower bounds, so child `at` spans
            // `[sep(at), sep(at + 1))` and the last child runs open-ended.
            for (at, child) in index.children.iter().enumerate() {
                let lower = child.separator().ok();
                let upper = index
                    .children
                    .get(at + 1)
                    .and_then(|next| next.separator().ok());

                let above_start = match (upper, bounds.start_bound()) {
                    (Some(upper), Bound::Included(start)) => start.as_ref() < upper,
                    (Some(upper), Bound::Excluded(start)) => start.as_ref() < upper,
                    _ => true,
                };
                // An unreadable separator is kept rather than silently dropped.
                let below_end = match (lower, bounds.end_bound()) {
                    (Some(lower), Bound::Included(end)) => lower <= end.as_ref(),
                    (Some(lower), Bound::Excluded(end)) => lower < end.as_ref(),
                    _ => true,
                };

                if above_start && below_end {
                    match child {
                        Node::Persistent(link) => plan.push(StoredStep::Subtree(link.node.clone())),
                        Node::Transient(child) => collect_stored_plan(child, bounds, plan),
                    }
                }
            }
        }
        TransientNode::Segment(segment) => {
            for entry in &segment.entries {
                if bounds.contains(&entry.key) {
                    plan.push(StoredStep::Entry(entry.clone()));
                }
            }
        }
    }
}

/// Index of the child whose subtree covers `key`: separators are lower bounds,
/// so it is the LAST child whose separator is at or below the key, and a key
/// sorting below every separator clamps to the leftmost child.
///
/// Mirrors the transient tree's routing (and `carry_novelty`'s re-attachment
/// rule) so a buffered read descends the same way a canonical edit would.
fn child_index<Key, Value>(
    children: &[Node<Key, Value>],
    key: &[u8],
) -> Result<usize, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
{
    let mut at = 0usize;
    while at + 1 < children.len() {
        if children[at + 1].separator()? <= key {
            at += 1;
        } else {
            break;
        }
    }
    Ok(at)
}

/// Replays a list of buffered ops as canonical inserts/deletes on an edit batch.
async fn replay_ops<Key, Value, D, Backend>(
    mut edit: TransientTree<Key, Value, D>,
    ops: Vec<NoveltyEntry<Value>>,
    storage: &ContentAddressedStorage<Backend>,
) -> Result<TransientTree<Key, Value, D>, DialogSearchTreeError>
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
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    for entry in ops {
        // A buffered op carries raw key bytes; the canonical edit path takes a
        // typed key, so reconstruct it here (the same round trip a leaf read
        // makes).
        let key = Key::try_from_bytes(&entry.key)?;
        edit = match entry.op {
            NoveltyOp::Assert(value) => edit.insert(key, value, storage).await?,
            NoveltyOp::Retract => edit.delete(&key, storage).await?,
        };
    }
    Ok(edit)
}

/// Whether any node in the stored subtree rooted at `hash` carries buffered
/// ops (sealed `novelty`).
///
/// Novelty lives only in index nodes, so the probe walks the subtree's index
/// spine and never touches a leaf. It is what lets the drain lift exactly the
/// subtrees that need rewriting, leaving every clean subtree's hash untouched.
fn subtree_has_novelty<'a, Key, Value, Backend>(
    hash: &'a Blake3Hash,
    accessor: &'a Accessor<Backend>,
) -> BoolFuture<'a>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    Box::pin(async move {
        let node: PersistentNode<Key, Value> = accessor.get_node(hash).await?;
        let links = match node.body()? {
            ArchivedNodeBody::Segment(_) => return Ok(false),
            ArchivedNodeBody::Index(index) => {
                if !index.novelty.is_empty() {
                    return Ok(true);
                }
                index.links()?
            }
        };
        for link in links {
            if subtree_has_novelty::<Key, Value, Backend>(&link.node, accessor).await? {
                return Ok(true);
            }
        }
        Ok(false)
    })
}

/// Drains every `novelty` buffer in the subtree rooted at `node` into `ops`,
/// leaving all buffers empty. Persistent index children whose subtree still
/// carries sealed novelty are lifted so their buffers drain too; subtrees with
/// no novelty anywhere below them are never lifted, so untouched regions keep
/// their hashes through structural sharing.
///
/// The walk is POST-order: a node's children are drained before its own
/// buffer is appended. Ops flow root to leaf, so along any path deeper means
/// older; post-order therefore appends the oldest ops first and the root's
/// newest last, and the caller's stable sort by key preserves that, leaving
/// the shallowest (newest) op for every key in the last position for
/// last-write-wins replay. Within one node the buffer is already sorted with
/// the newest op for a key last, so appending it whole keeps that order too.
fn drain_novelty<'a, Key, Value, Backend>(
    node: &'a mut TransientNode<Key, Value>,
    ops: &'a mut Vec<NoveltyEntry<Value>>,
    accessor: &'a Accessor<Backend>,
) -> UnitFuture<'a>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    Box::pin(async move {
        if let TransientNode::Index(index) = node {
            for child in &mut index.children {
                if let Node::Persistent(link) = child {
                    if !subtree_has_novelty::<Key, Value, Backend>(&link.node, accessor).await? {
                        continue;
                    }
                    // Measurement-only (uncommitted, env-gated) lift breadcrumb.
                    dialog_storage::dup_audit::note_lift(link.node.as_bytes(), "drain_novelty");
                    lift_child(child, accessor).await?;
                }
                if let Node::Transient(child) = child {
                    drain_novelty(child, ops, accessor).await?;
                }
            }
            ops.extend(index.novelty.take_all::<Key>()?);
        }
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::{Blake3Hash, NULL_BLAKE3_HASH};
    use dialog_storage::MemoryStorageBackend;

    use super::{FlushPolicy, FlushTrigger, HitchhikerTree};
    use crate::helpers::{
        DistributionSimulator, SpecKey, TestStorage as SpecStorage, encode_key, test_storage,
    };
    use crate::{
        ArchivedNodeBody, Buffer, Cache, Change, ContentAddressedStorage, Delta, Manifest, Node,
        NoveltyEntry, NoveltyOp, PersistentNode, PersistentTree, TransientNode, TransientTree,
        tree_spec,
    };

    /// The three flush policies, so an oracle can assert behavior is identical
    /// across all of them (the canonical form is policy-independent).
    const POLICIES: [FlushPolicy; 3] = [
        FlushPolicy::Amortized,
        FlushPolicy::Recursive,
        FlushPolicy::Immediate,
    ];

    type TestStorage = ContentAddressedStorage<MemoryStorageBackend<Blake3Hash, Vec<u8>>>;
    type TestTree = PersistentTree<[u8; 4], Vec<u8>>;
    type TestHitchhiker = HitchhikerTree<[u8; 4], Vec<u8>>;

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A tiny deterministic xorshift PRNG, matching the one the transient tree
    /// tests use, so the property tests are reproducible without seeded `rand`.
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

        fn shuffle<T>(&mut self, items: &mut [T]) {
            for i in (1..items.len()).rev() {
                let j = (self.next_u32() as usize) % (i + 1);
                items.swap(i, j);
            }
        }
    }

    /// Builds a canonical reference tree by inserting `keys` one at a time, each
    /// in its own edit batch, flushing after each. This is the history-independent
    /// canonical oracle a canonicalized hitchhiker tree must reproduce.
    async fn sequential(keys: &[u32], storage: &mut TestStorage) -> Result<TestTree> {
        let mut tree = TestTree::empty();
        let mut delta = Delta::zero();
        for &k in keys {
            tree = tree
                .edit()
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), storage)
                .await?
                .persist(&mut delta)?;
            flush(&mut delta, storage).await?;
        }
        Ok(tree)
    }

    async fn flush(delta: &mut Delta<Blake3Hash, Buffer>, storage: &mut TestStorage) -> Result<()> {
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }
        Ok(())
    }

    /// An overflow cascade that distributes buffered ops to a SUBSET of a
    /// node's children must leave the untouched children as persistent
    /// links: no decode, no re-encode, no re-hash, and — the observable
    /// contract — no delta entry for their (unchanged) blocks at persist.
    /// The retired lift-every-child loop turned each untouched sibling into
    /// a byte-identical re-store, the measured 24-26% duplicate-block share
    /// of sealed bytes (bead dialog-db-59).
    #[dialog_common::test]
    async fn it_keeps_untouched_children_persistent_through_a_cascade() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // A persisted canonical tree wide enough that one key's cascade
        // reaches a strict subset of the root's children.
        let keys: Vec<u32> = (0..2000).collect();
        let tree = sequential(&keys, &mut storage).await?;
        let root_hashes: Vec<Blake3Hash> = {
            let accessor = crate::Accessor::new(crate::Cache::new(), storage.clone());
            let node: crate::PersistentNode<[u8; 4], Vec<u8>> =
                accessor.get_node(tree.root()).await?;
            let index = node.as_index()?;
            (0..index.len())
                .map(|at| index.hash_at(at).cloned())
                .collect::<Result<_, _>>()?
        };
        assert!(
            root_hashes.len() >= 2,
            "the fixture needs several children to distinguish touched from untouched"
        );

        // A zero-size buffer with the capacity trigger forces the recursive
        // cascade on the first write; the written key routes into exactly
        // one child's range.
        let buffered = TestHitchhiker::open(&tree)
            .with_op_buf_size(1)
            .with_flush_policy(FlushPolicy::Recursive)
            .with_flush_trigger(FlushTrigger::Capacity);
        let buffered = buffered
            .insert(7u32.to_le_bytes(), vec![0xAB], &storage)
            .await?
            .insert(8u32.to_le_bytes(), vec![0xCD], &storage)
            .await?;
        let mut delta = Delta::zero();
        buffered.persist(&mut delta)?;
        let written: std::collections::HashSet<Blake3Hash> =
            delta.flush().map(|(hash, _)| hash).collect();

        let touched: Vec<&Blake3Hash> = root_hashes
            .iter()
            .filter(|hash| written.contains(hash))
            .collect();
        assert!(
            touched.is_empty(),
            "unchanged children re-entered the delta as byte-identical re-stores: {touched:?}"
        );
        assert!(
            !written.is_empty(),
            "the touched child's rewrite must reach the delta"
        );

        Ok(())
    }

    /// Canonicalizing an empty buffered tree yields the null (empty) root.
    #[dialog_common::test]
    async fn it_canonicalizes_empty_to_null_root() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
        let mut delta = Delta::zero();
        let canonical = TestHitchhiker::empty()
            .canonicalize(&storage, &mut delta)
            .await?;
        assert_eq!(canonical.root(), TestTree::empty().root());
        Ok(())
    }

    /// With a buffer large enough to never overflow, every write lands in the
    /// root buffer and canonicalize flushes them in one pass: the result must
    /// equal a sequential build.
    #[dialog_common::test]
    async fn it_canonicalizes_root_buffered_writes_to_sequential() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let keys: Vec<u32> = (0..200).collect();
        let expected = sequential(&keys, &mut storage).await?;

        // A large buffer keeps every op in the root: no cascade until canonicalize.
        let mut tree = TestHitchhiker::empty().with_op_buf_size(100_000);
        for &k in &keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(&storage, &mut delta).await?;

        assert_eq!(
            canonical.root(),
            expected.root(),
            "root-buffered writes must canonicalize to the sequential root"
        );
        Ok(())
    }

    /// With a small buffer that forces repeated one-level cascades, canonicalize
    /// must still reproduce the sequential canonical root.
    #[dialog_common::test]
    async fn it_canonicalizes_cascaded_writes_to_sequential() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for &buf in &[1usize, 4, 16, 64] {
            let keys: Vec<u32> = (0..500).collect();
            let expected = sequential(&keys, &mut storage).await?;

            let mut tree = TestHitchhiker::empty().with_op_buf_size(buf);
            for &k in &keys {
                tree = tree
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            let mut delta = Delta::zero();
            let canonical = tree.canonicalize(&storage, &mut delta).await?;

            assert_eq!(
                canonical.root(),
                expected.root(),
                "op_buf_size {buf}: cascaded writes must canonicalize to the sequential root"
            );
        }
        Ok(())
    }

    /// Every write must survive when each batch PERSISTS and RE-OPENS the tree.
    ///
    /// This is what the commit path does: a commit opens a hitchhiker over the
    /// current root, writes its batch, seals the spine back to a root, and the
    /// next commit re-opens from that root. The long-lived-tree tests never
    /// exercise the reseal boundary, so a cascade that corrupts the spine as it
    /// is sealed shows up only here — as a tree that silently drops the writes
    /// committed before it.
    #[dialog_common::test]
    async fn it_retains_writes_across_persist_and_reopen_cycles() -> Result<()> {
        for &buf in &[1usize, 4, 16, 64] {
            let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

            // Each batch writes several scattered keys, the way a commit
            // touches three key orderings plus a history record.
            let mut rng = Rng::new(buf as u64);
            let mut keys: Vec<u32> = Vec::new();
            let mut tree = TestTree::empty();
            for batch in 0..200u32 {
                let batch_keys: Vec<u32> = (0..4).map(|_| rng.next_u32() % 1_000_000).collect();
                let mut hitchhiker = HitchhikerTree::open(&tree).with_op_buf_size(buf);
                for &k in &batch_keys {
                    hitchhiker = hitchhiker
                        .insert(k.to_be_bytes(), k.to_be_bytes().to_vec(), &storage)
                        .await?;
                }
                let mut delta = Delta::zero();
                let root = hitchhiker.persist(&mut delta)?;
                flush(&mut delta, &mut storage).await?;
                tree = TestTree::from_hash_with_cache(root, Default::default());
                keys.extend(batch_keys);
                let _ = batch;
            }

            for &k in &keys {
                assert_eq!(
                    tree.get(&k.to_be_bytes(), &storage).await?,
                    Some(k.to_be_bytes().to_vec()),
                    "op_buf_size {buf}: the write at {k} must survive later persist/reopen cycles"
                );
            }
        }
        Ok(())
    }

    /// Buffered writes over a previously flushed persistent base, then
    /// canonicalize, must match a sequential build of the union of both key sets.
    #[dialog_common::test]
    async fn it_canonicalizes_writes_over_a_persistent_base() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let base_keys: Vec<u32> = (0..300).collect();
        let base = sequential(&base_keys, &mut storage).await?;

        let extra_keys: Vec<u32> = (300..600).collect();
        let mut all = base_keys.clone();
        all.extend(&extra_keys);
        let expected = sequential(&all, &mut storage).await?;

        let mut tree = TestHitchhiker::open(&base).with_op_buf_size(8);
        for &k in &extra_keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(&storage, &mut delta).await?;

        assert_eq!(
            canonical.root(),
            expected.root(),
            "buffered writes over a persistent base must canonicalize to the union build"
        );
        Ok(())
    }

    /// Random insert order through a cascading buffer must canonicalize to the
    /// same root as the sequential build, confirming history independence.
    #[dialog_common::test]
    async fn it_canonicalizes_random_order_to_sequential() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..50u64 {
            let mut keys: Vec<u32> = (0..300).collect();
            let sorted = keys.clone();
            Rng::new(seed).shuffle(&mut keys);

            let expected = sequential(&sorted, &mut storage).await?;

            let mut tree = TestHitchhiker::empty().with_op_buf_size(8);
            for &k in &keys {
                tree = tree
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            let mut delta = Delta::zero();
            let canonical = tree.canonicalize(&storage, &mut delta).await?;

            assert_eq!(
                canonical.root(),
                expected.root(),
                "seed {seed}: random-order buffered inserts must canonicalize to sequential"
            );
        }
        Ok(())
    }

    /// Buffered deletes (tombstones) must cancel buffered or based inserts:
    /// canonicalize of insert-all-then-delete-some equals a sequential build of
    /// the survivors.
    #[dialog_common::test]
    async fn it_canonicalizes_with_buffered_deletes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..50u64 {
            let keys: Vec<u32> = (0..300).collect();
            let mut to_delete: Vec<u32> = (0..300).collect();
            let mut rng = Rng::new(seed);
            rng.shuffle(&mut to_delete);
            to_delete.truncate(80);

            let survivors: Vec<u32> = keys
                .iter()
                .copied()
                .filter(|k| !to_delete.contains(k))
                .collect();
            let expected = sequential(&survivors, &mut storage).await?;

            let mut tree = TestHitchhiker::empty().with_op_buf_size(16);
            for &k in &keys {
                tree = tree
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            for &k in &to_delete {
                tree = tree.delete(k.to_le_bytes(), &storage).await?;
            }
            let mut delta = Delta::zero();
            let canonical = tree.canonicalize(&storage, &mut delta).await?;

            assert_eq!(
                canonical.root(),
                expected.root(),
                "seed {seed}: buffered deletes must canonicalize to the survivor build"
            );
        }
        Ok(())
    }

    /// A random interleaving of buffered inserts and deletes over a small key
    /// domain (so ops collide on keys) must canonicalize to the same root as the
    /// same op stream applied sequentially through the canonical edit path.
    #[dialog_common::test]
    async fn it_canonicalizes_interleaved_ops_to_sequential() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..50u64 {
            let mut rng = Rng::new(seed);
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..400 {
                let is_insert = !(rng.next_u32()).is_multiple_of(3);
                let key = rng.next_u32() % 150;
                ops.push((is_insert, key));
            }

            // Sequential reference through the canonical edit path.
            let mut reference = TestTree::empty();
            let mut delta = Delta::zero();
            for &(is_insert, key) in &ops {
                reference = if is_insert {
                    reference
                        .edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    reference
                        .edit()
                        .delete(&key.to_le_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?
                };
                flush(&mut delta, &mut storage).await?;
            }

            // Buffered through the hitchhiker tree, with a cascading buffer.
            let mut tree = TestHitchhiker::empty().with_op_buf_size(8);
            for &(is_insert, key) in &ops {
                tree = if is_insert {
                    tree.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    tree.delete(key.to_le_bytes(), &storage).await?
                };
            }
            let mut canon_delta = Delta::zero();
            let canonical = tree.canonicalize(&storage, &mut canon_delta).await?;

            assert_eq!(
                canonical.root(),
                reference.root(),
                "seed {seed}: interleaved buffered ops must canonicalize to sequential"
            );
        }
        Ok(())
    }

    /// A buffered `get` must read exactly what canonicalize would produce: the
    /// merge of buffered ops over the stored leaves. For a random insert/delete
    /// stream over a small domain (so ops collide), every key's buffered `get`
    /// must match the same key's `get` on the canonical reference tree, both for
    /// present and absent keys.
    #[dialog_common::test]
    async fn it_reads_buffered_ops_like_canonicalized() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..50u64 {
            let mut rng = Rng::new(seed);
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..400 {
                let is_insert = !(rng.next_u32()).is_multiple_of(3);
                let key = rng.next_u32() % 150;
                ops.push((is_insert, key));
            }

            // Reference: replay the ops canonically and flush, so its reads are
            // fully resolvable from storage.
            let mut reference = TestTree::empty();
            let mut delta = Delta::zero();
            for &(is_insert, key) in &ops {
                reference = if is_insert {
                    reference
                        .edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    reference
                        .edit()
                        .delete(&key.to_le_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?
                };
                flush(&mut delta, &mut storage).await?;
            }

            // Buffered: the same ops through a cascading hitchhiker tree, never
            // canonicalized; reads merge the live buffers over the leaves.
            let mut tree = TestHitchhiker::empty().with_op_buf_size(8);
            for &(is_insert, key) in &ops {
                tree = if is_insert {
                    tree.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    tree.delete(key.to_le_bytes(), &storage).await?
                };
            }

            for key in 0..160u32 {
                let buffered = tree.get(&key.to_le_bytes(), &storage).await?;
                let canonical = reference.get(&key.to_le_bytes(), &storage).await?;
                assert_eq!(
                    buffered, canonical,
                    "seed {seed}: buffered get of key {key} must match the canonical read"
                );
            }
        }
        Ok(())
    }

    /// A buffered range scan must yield exactly what the canonical tree yields.
    ///
    /// This is the property the artifact layer's read-modify-write instructions
    /// depend on: `Replace` scans an (entity, attribute) slot to find the priors
    /// it must supersede, so a scan blind to the buffers would miss a prior that
    /// a recent write only buffered.
    #[dialog_common::test]
    async fn it_scans_ranges_like_canonicalized() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..25u64 {
            let mut rng = Rng::new(seed);
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..400 {
                let is_insert = !(rng.next_u32()).is_multiple_of(3);
                ops.push((is_insert, rng.next_u32() % 150));
            }

            let mut reference = TestTree::empty();
            let mut delta = Delta::zero();
            for &(is_insert, key) in &ops {
                reference = if is_insert {
                    reference
                        .edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    reference
                        .edit()
                        .delete(&key.to_le_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?
                };
                flush(&mut delta, &mut storage).await?;
            }

            // A small buffer forces cascades, so ops end up spread across several
            // levels of novelty and the scan has to merge all of them.
            let mut tree = TestHitchhiker::empty().with_op_buf_size(8);
            for &(is_insert, key) in &ops {
                tree = if is_insert {
                    tree.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                } else {
                    tree.delete(key.to_le_bytes(), &storage).await?
                };
            }

            // Full range, plus sub-ranges that start and end inside the domain.
            for (low, high) in [(0u32, 200u32), (10, 40), (75, 76), (140, 200)] {
                let range = low.to_le_bytes()..=high.to_le_bytes();

                let buffered: Vec<_> = {
                    let stream = tree.stream_range(range.clone(), &storage);
                    futures_util::pin_mut!(stream);
                    let mut collected = Vec::new();
                    while let Some(entry) = futures_util::StreamExt::next(&mut stream)
                        .await
                        .transpose()?
                    {
                        collected.push((entry.key, entry.value));
                    }
                    collected
                };

                let canonical: Vec<_> = {
                    let stream = reference.stream_range(range, &storage);
                    futures_util::pin_mut!(stream);
                    let mut collected = Vec::new();
                    while let Some(entry) = futures_util::StreamExt::next(&mut stream)
                        .await
                        .transpose()?
                    {
                        collected.push((entry.key, entry.value));
                    }
                    collected
                };

                assert_eq!(
                    buffered, canonical,
                    "seed {seed}: buffered scan of [{low}, {high}] must match the canonical scan"
                );
            }
        }
        Ok(())
    }

    /// The scan must see a buffered delete of a key that is still live in the
    /// persistent base. This is the case that makes a blind scan unsafe: a
    /// `Replace` that missed it would leave the superseded value live at a
    /// cardinality-one slot.
    #[dialog_common::test]
    async fn it_hides_a_buffered_delete_from_a_range_scan() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let base_keys: Vec<u32> = (0..300).collect();
        let base = sequential(&base_keys, &mut storage).await?;

        // A buffer large enough that neither op leaves the root's novelty.
        let mut tree = HitchhikerTree::open(&base).with_op_buf_size(100_000);
        tree = tree.delete(150u32.to_le_bytes(), &storage).await?;
        tree = tree
            .insert(151u32.to_le_bytes(), vec![9, 9], &storage)
            .await?;

        let scanned: Vec<_> = {
            let stream = tree.stream_range(148u32.to_le_bytes()..=153u32.to_le_bytes(), &storage);
            futures_util::pin_mut!(stream);
            let mut collected = Vec::new();
            while let Some(entry) = futures_util::StreamExt::next(&mut stream)
                .await
                .transpose()?
            {
                collected.push((u32::from_le_bytes(entry.key), entry.value));
            }
            collected
        };

        assert_eq!(
            scanned,
            vec![
                (148u32, 148u32.to_le_bytes().to_vec()),
                (149, 149u32.to_le_bytes().to_vec()),
                // 150 was deleted in the buffer and must not appear.
                (151, vec![9, 9]),
                (152, 152u32.to_le_bytes().to_vec()),
                (153, 153u32.to_le_bytes().to_vec()),
            ],
            "the scan must hide a buffered delete and surface a buffered update"
        );

        Ok(())
    }

    /// A later buffered write to a key must shadow an earlier one on read
    /// (last-op-wins), including a delete shadowing a prior insert and a
    /// re-insert shadowing a prior delete, all while the ops sit in buffers.
    #[dialog_common::test]
    async fn it_reads_last_op_wins_for_buffered_writes() -> Result<()> {
        let storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // A large buffer keeps every op in the root buffer, so all collisions on
        // a key resolve purely within one node's novelty.
        let mut tree = TestHitchhiker::empty().with_op_buf_size(100_000);

        tree = tree.insert(7u32.to_le_bytes(), vec![1], &storage).await?;
        assert_eq!(
            tree.get(&7u32.to_le_bytes(), &storage).await?,
            Some(vec![1])
        );

        tree = tree.insert(7u32.to_le_bytes(), vec![2], &storage).await?;
        assert_eq!(
            tree.get(&7u32.to_le_bytes(), &storage).await?,
            Some(vec![2]),
            "a later assert must shadow an earlier one"
        );

        tree = tree.delete(7u32.to_le_bytes(), &storage).await?;
        assert_eq!(
            tree.get(&7u32.to_le_bytes(), &storage).await?,
            None,
            "a buffered retract must hide a prior assert"
        );

        tree = tree.insert(7u32.to_le_bytes(), vec![3], &storage).await?;
        assert_eq!(
            tree.get(&7u32.to_le_bytes(), &storage).await?,
            Some(vec![3]),
            "a re-insert must shadow a prior retract"
        );

        Ok(())
    }

    /// A buffered op must shadow a value stored in the persistent base: opening
    /// over a flushed tree and buffering an update or delete to a based key must
    /// be visible to `get` before any flush.
    #[dialog_common::test]
    async fn it_reads_buffered_ops_over_a_persistent_base() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let base_keys: Vec<u32> = (0..300).collect();
        let base = sequential(&base_keys, &mut storage).await?;

        let mut tree = TestHitchhiker::open(&base).with_op_buf_size(8);

        // Update a based key and delete another, both buffered.
        tree = tree
            .insert(42u32.to_le_bytes(), vec![0xFF], &storage)
            .await?;
        tree = tree.delete(100u32.to_le_bytes(), &storage).await?;

        assert_eq!(
            tree.get(&42u32.to_le_bytes(), &storage).await?,
            Some(vec![0xFF]),
            "a buffered update must shadow the based value"
        );
        assert_eq!(
            tree.get(&100u32.to_le_bytes(), &storage).await?,
            None,
            "a buffered delete must hide the based value"
        );
        // An untouched based key still reads from storage.
        assert_eq!(
            tree.get(&200u32.to_le_bytes(), &storage).await?,
            Some(200u32.to_le_bytes().to_vec()),
            "an untouched based key reads through to storage"
        );
        // An absent key is absent.
        assert_eq!(tree.get(&999u32.to_le_bytes(), &storage).await?, None);

        Ok(())
    }

    /// All three flush policies are correctness-equivalent: under each, a random
    /// insert/delete stream through a cascading buffer must canonicalize to the
    /// same sequential root, and reads before canonicalize must match the
    /// canonical reference. They differ only in how far overflow cascades, not in
    /// the fact set they represent.
    #[dialog_common::test]
    async fn it_is_correctness_equivalent_across_flush_policies() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        for seed in 0..30u64 {
            let mut rng = Rng::new(seed);
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..300 {
                let is_insert = !(rng.next_u32()).is_multiple_of(3);
                let key = rng.next_u32() % 150;
                ops.push((is_insert, key));
            }

            // Canonical reference via the sequential edit path, flushed.
            let mut reference = TestTree::empty();
            let mut delta = Delta::zero();
            for &(is_insert, key) in &ops {
                reference = if is_insert {
                    reference
                        .edit()
                        .insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?
                } else {
                    reference
                        .edit()
                        .delete(&key.to_le_bytes(), &storage)
                        .await?
                        .persist(&mut delta)?
                };
                flush(&mut delta, &mut storage).await?;
            }

            for policy in POLICIES {
                // A small buffer forces cascades for Amortized and Recursive; for
                // Immediate the buffer is never used.
                let mut tree = TestHitchhiker::empty()
                    .with_op_buf_size(8)
                    .with_flush_policy(policy);
                for &(is_insert, key) in &ops {
                    tree = if is_insert {
                        tree.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), &storage)
                            .await?
                    } else {
                        tree.delete(key.to_le_bytes(), &storage).await?
                    };
                }

                // Reads before canonicalize must match the canonical reference.
                for key in 0..160u32 {
                    let buffered = tree.get(&key.to_le_bytes(), &storage).await?;
                    let canonical = reference.get(&key.to_le_bytes(), &storage).await?;
                    assert_eq!(
                        buffered, canonical,
                        "seed {seed}, policy {policy:?}: buffered get of key {key} \
                         must match the canonical read"
                    );
                }

                // Canonicalize must reproduce the sequential root under any policy.
                let mut canon_delta = Delta::zero();
                let canonical = tree.canonicalize(&storage, &mut canon_delta).await?;
                assert_eq!(
                    canonical.root(),
                    reference.root(),
                    "seed {seed}, policy {policy:?}: canonicalize must match the sequential root"
                );
            }
        }
        Ok(())
    }

    /// The Immediate policy keeps the tree canonical at all times: its persisted
    /// root after every write equals the sequential root, with no canonicalize
    /// step needed. This pins that Immediate is the unbuffered baseline.
    #[dialog_common::test]
    async fn it_keeps_immediate_policy_canonical_without_flush() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let keys: Vec<u32> = (0..200).collect();
        let expected = sequential(&keys, &mut storage).await?;

        let mut tree = TestHitchhiker::empty().with_flush_policy(FlushPolicy::Immediate);
        for &k in &keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }

        // persist (buffers intact) already equals the canonical root, because
        // Immediate never buffers.
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        assert_eq!(
            &root,
            expected.root(),
            "Immediate persist must equal the sequential root without canonicalize"
        );
        Ok(())
    }

    // --- Reconcile scenarios (two diverged buffered replicas syncing) ---
    //
    // These probe whether two buffered hitchhiker trees can be reconciled. The
    // bar is the one the user named: after reconcile, canonicalizing both sides
    // yields byte-identical roots representing the same fact set, with no
    // resurrected deletes. They model the three regimes the user called out:
    // both replicas overflow, neither overflows alone but the merge does, and a
    // flushed delete on one replica colliding with the other's catch-up.

    /// Persists a hitchhiker tree (novelty intact) and flushes it to storage,
    /// returning the materialized root tree. Models a replica pushing its
    /// buffered state to the shared store without canonicalizing.
    async fn persist_buffered(tree: TestHitchhiker, storage: &mut TestStorage) -> Result<TestTree> {
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        flush(&mut delta, storage).await?;
        Ok(TestTree::from_hash(root))
    }

    /// Canonicalizes a hitchhiker tree and flushes it, returning the canonical
    /// root tree. Models a replica that flushes all novelty to leaves at the
    /// sync boundary.
    async fn canonicalize_flushed(
        tree: TestHitchhiker,
        storage: &mut TestStorage,
    ) -> Result<TestTree> {
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(storage, &mut delta).await?;
        flush(&mut delta, storage).await?;
        Ok(canonical)
    }

    /// Reconciles `local` toward `other` relative to their common `base`, the way
    /// the repository pull path does: differentiate base against other, integrate
    /// those changes into an edit over local. Returns the merged root tree.
    async fn reconcile(
        base: &TestTree,
        local: &TestTree,
        other: &TestTree,
        storage: &mut TestStorage,
    ) -> Result<TestTree> {
        let differential = base.differentiate(other, storage, storage);
        let mut delta = Delta::zero();
        let merged = local
            .edit()
            .integrate(differential, storage)
            .await?
            .persist(&mut delta)?;
        flush(&mut delta, storage).await?;
        Ok(merged)
    }

    /// Builds a hitchhiker tree over `base` applying `ops` (true = insert key as
    /// its own value, false = delete), with the given buffer size.
    async fn buffered_replica(
        base: &TestTree,
        ops: &[(bool, u32)],
        op_buf_size: usize,
        storage: &TestStorage,
    ) -> Result<TestHitchhiker> {
        let mut tree = TestHitchhiker::open(base).with_op_buf_size(op_buf_size);
        for &(is_insert, key) in ops {
            tree = if is_insert {
                tree.insert(key.to_le_bytes(), key.to_le_bytes().to_vec(), storage)
                    .await?
            } else {
                tree.delete(key.to_le_bytes(), storage).await?
            };
        }
        Ok(tree)
    }

    /// Both replicas overflow their root buffer (disjoint key ranges), then
    /// reconcile via canonicalize-at-sync. The merged canonical root must equal a
    /// sequential build of the union of both replicas' fact sets.
    #[dialog_common::test]
    async fn it_reconciles_when_both_replicas_overflow() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Shared base of 0..200.
        let base = sequential(&(0..200u32).collect::<Vec<_>>(), &mut storage).await?;

        // Each replica adds enough disjoint keys to overflow a small buffer.
        let a_ops: Vec<(bool, u32)> = (1000..1100u32).map(|k| (true, k)).collect();
        let b_ops: Vec<(bool, u32)> = (2000..2100u32).map(|k| (true, k)).collect();

        let a = buffered_replica(&base, &a_ops, 8, &storage).await?;
        let b = buffered_replica(&base, &b_ops, 8, &storage).await?;

        // Canonicalize both at the sync boundary, then reconcile B toward A.
        let a_canon = canonicalize_flushed(a, &mut storage).await?;
        let b_canon = canonicalize_flushed(b, &mut storage).await?;
        let merged = reconcile(&base, &b_canon, &a_canon, &mut storage).await?;

        // Oracle: a sequential build of the union.
        let mut union: Vec<u32> = (0..200).collect();
        union.extend(1000..1100);
        union.extend(2000..2100);
        let expected = sequential(&union, &mut storage).await?;

        assert_eq!(
            merged.root(),
            expected.root(),
            "both-overflow reconcile must equal the union build"
        );
        Ok(())
    }

    /// Neither replica overflows alone, but their combined novelty would. After
    /// canonicalize-at-sync the merge must still equal the union build (the merge
    /// triggering a flush is the point of interest).
    #[dialog_common::test]
    async fn it_reconciles_when_only_the_merge_overflows() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let base = sequential(&(0..200u32).collect::<Vec<_>>(), &mut storage).await?;

        // Buffer of 64; each replica adds 40 (fits alone), union adds 80 (would
        // overflow a single root buffer).
        let a_ops: Vec<(bool, u32)> = (1000..1040u32).map(|k| (true, k)).collect();
        let b_ops: Vec<(bool, u32)> = (2000..2040u32).map(|k| (true, k)).collect();

        let a = buffered_replica(&base, &a_ops, 64, &storage).await?;
        let b = buffered_replica(&base, &b_ops, 64, &storage).await?;

        let a_canon = canonicalize_flushed(a, &mut storage).await?;
        let b_canon = canonicalize_flushed(b, &mut storage).await?;
        let merged = reconcile(&base, &b_canon, &a_canon, &mut storage).await?;

        let mut union: Vec<u32> = (0..200).collect();
        union.extend(1000..1040);
        union.extend(2000..2040);
        let expected = sequential(&union, &mut storage).await?;

        assert_eq!(
            merged.root(),
            expected.root(),
            "merge-overflow reconcile must equal the union build"
        );
        Ok(())
    }

    /// The delete-resurrection scenario. Replica A buffers and overflows a delete
    /// of a key K that exists in the base (so the delete flushes to a leaf), then
    /// pushes. Replica B made an unrelated buffered change. When B catches up to
    /// A via canonicalize-at-sync reconcile, K must STAY deleted: the canonical
    /// merged root must equal a sequential build of (base + B's change) minus K.
    #[dialog_common::test]
    async fn it_does_not_resurrect_a_flushed_delete_on_catch_up() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // Base contains K = 50 along with 0..200.
        let base = sequential(&(0..200u32).collect::<Vec<_>>(), &mut storage).await?;
        let victim = 50u32;

        // Replica A: delete K plus enough other ops to overflow the root buffer,
        // forcing the delete to flush down toward the leaf.
        let mut a_ops: Vec<(bool, u32)> = vec![(false, victim)];
        a_ops.extend((1000..1100u32).map(|k| (true, k)));
        let a = buffered_replica(&base, &a_ops, 8, &storage).await?;

        // Replica B: an unrelated buffered insert, no overflow.
        let b_ops: Vec<(bool, u32)> = vec![(true, 3000u32)];
        let b = buffered_replica(&base, &b_ops, 8, &storage).await?;

        let a_canon = canonicalize_flushed(a, &mut storage).await?;
        let b_canon = canonicalize_flushed(b, &mut storage).await?;
        let merged = reconcile(&base, &b_canon, &a_canon, &mut storage).await?;

        // K must not come back. Oracle: union of A's and B's effects.
        let mut union: Vec<u32> = (0..200).filter(|&k| k != victim).collect();
        union.extend(1000..1100);
        union.push(3000);
        let expected = sequential(&union, &mut storage).await?;

        assert_eq!(
            merged.root(),
            expected.root(),
            "a flushed delete of {victim} must not be resurrected on catch-up"
        );

        // And explicitly: K resolves to absent in the merged tree.
        assert_eq!(
            merged.get(&victim.to_le_bytes(), &storage).await?,
            None,
            "deleted key {victim} must remain absent after reconcile"
        );
        Ok(())
    }

    /// Reconciling buffered (non-canonicalized) trees directly is safe: the
    /// differential reads index novelty, so a key living only in a replica's
    /// root buffer is visible to it.
    ///
    /// This was previously the opposite: the differential saw only flushed leaf
    /// entries, so an unflushed insert was invisible and a reconcile silently
    /// dropped it. Canonicalizing before every sync was the workaround. Now the
    /// flush is an optimization rather than a correctness requirement, which is
    /// what lets a replica keep its writes buffered across syncs.
    #[dialog_common::test]
    async fn it_reconciles_a_buffered_replica_without_canonicalizing() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let base = sequential(&(0..200u32).collect::<Vec<_>>(), &mut storage).await?;

        // Replica A buffers a single insert that stays in the root novelty (large
        // buffer, no overflow), so it never reaches a leaf.
        let a = buffered_replica(&base, &[(true, 5000u32)], 100_000, &storage).await?;
        let a_buffered = persist_buffered(a, &mut storage).await?;

        // Reconcile the base toward A's buffered tree directly (no canonicalize).
        let merged = reconcile(&base, &base, &a_buffered, &mut storage).await?;

        assert_eq!(
            merged.get(&5000u32.to_le_bytes(), &storage).await?,
            Some(5000u32.to_le_bytes().to_vec()),
            "a reconcile must see an op that lives only in novelty"
        );

        // Canonicalizing first must reach the same result: whether the ops were
        // flushed before the differential ran cannot change what it reports.
        let a2 = buffered_replica(&base, &[(true, 5000u32)], 100_000, &storage).await?;
        let a_canon = canonicalize_flushed(a2, &mut storage).await?;
        let merged_safe = reconcile(&base, &base, &a_canon, &mut storage).await?;
        assert_eq!(
            merged_safe.get(&5000u32.to_le_bytes(), &storage).await?,
            Some(5000u32.to_le_bytes().to_vec()),
            "canonicalizing first must reach the same result"
        );
        Ok(())
    }

    /// The flush trigger must not change what the tree contains, only when work
    /// happens. Canonicalizing under either trigger must reach the identical
    /// canonical tree for the same op stream.
    #[dialog_common::test]
    async fn it_is_correctness_equivalent_across_flush_triggers() -> Result<()> {
        for seed in 0..20u64 {
            let mut rng = Rng::new(seed);
            let mut ops: Vec<(bool, u32)> = Vec::new();
            for _ in 0..600 {
                ops.push((!rng.next_u32().is_multiple_of(3), rng.next_u32() % 3_000));
            }

            let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());
            let mut roots = Vec::new();

            for trigger in [
                FlushTrigger::Capacity,
                FlushTrigger::PerChild { floor: 1 },
                FlushTrigger::PerChild { floor: 16 },
            ] {
                let mut tree = TestHitchhiker::empty()
                    .with_op_buf_size(64)
                    .with_flush_trigger(trigger);
                for (insert, key) in &ops {
                    tree = if *insert {
                        tree.insert(key.to_be_bytes(), key.to_be_bytes().to_vec(), &storage)
                            .await?
                    } else {
                        tree.delete(key.to_be_bytes(), &storage).await?
                    };
                }
                let mut delta = Delta::zero();
                let canonical = tree.canonicalize(&storage, &mut delta).await?;
                flush(&mut delta, &mut storage).await?;
                roots.push(canonical.root().clone());
            }

            assert!(
                roots.windows(2).all(|pair| pair[0] == pair[1]),
                "seed {seed}: flush triggers must reach the same canonical tree"
            );
        }
        Ok(())
    }

    type SpecTree = PersistentTree<SpecKey, Vec<u8>, DistributionSimulator>;
    type SpecHitchhiker = HitchhikerTree<SpecKey, Vec<u8>, DistributionSimulator>;

    /// The probe key: an interior key of the spec fixture's base, which the
    /// precedence pins buffer at TWO depths of the spine.
    fn probe() -> SpecKey {
        encode_key(b"f", 1, 1)
    }

    /// A key covered by the OTHER top-level subtree; writing it overflows the
    /// one-op root buffer and cascades the probe's first op into the mid-level
    /// index that covers the probe.
    fn far_key() -> SpecKey {
        encode_key(b"mm", 1, 1)
    }

    async fn spec_flush(
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

    /// Applies the three writes that leave the probe key pending at two
    /// depths: (probe, [1]) parked in a mid-level index buffer by the cascade,
    /// then `root_op` for the same key freshly buffered at the root.
    async fn apply_probe_writes(
        base: &SpecTree,
        root_op: &NoveltyOp<Vec<u8>>,
        storage: &SpecStorage,
    ) -> Result<SpecHitchhiker> {
        let mut tree = SpecHitchhiker::open(base).with_op_buf_size(1);
        tree = tree.insert(probe(), vec![1], storage).await?;
        tree = tree.insert(far_key(), vec![9], storage).await?;
        Ok(match root_op {
            NoveltyOp::Assert(value) => tree.insert(probe(), value.clone(), storage).await?,
            NoveltyOp::Retract => tree.delete(probe(), storage).await?,
        })
    }

    /// Verifies the two-level fixture actually pins what it claims: the older
    /// op for the probe sits in a NON-ROOT index buffer while the newer
    /// `root_op` sits at the root. A fixture that leaves the key at one depth
    /// would pin nothing.
    async fn assert_probe_spans_two_levels(
        root: &Blake3Hash,
        storage: &SpecStorage,
        root_op: &NoveltyOp<Vec<u8>>,
    ) -> Result<()> {
        let root_node = load_spec_node(storage, root).await?;
        assert!(
            sealed_novelty(&root_node)?
                .iter()
                .any(|entry| entry.key == probe().to_vec() && &entry.op == root_op),
            "fixture: the newer op must sit in the root buffer"
        );
        let mut parked = false;
        for link in root_node.as_index()?.links()? {
            let child = load_spec_node(storage, &link.node).await?;
            if sealed_novelty(&child)?.iter().any(|entry| {
                entry.key == probe().to_vec() && entry.op == NoveltyOp::Assert(vec![1])
            }) {
                parked = true;
            }
        }
        assert!(
            parked,
            "fixture: the older op must sit in a mid-level index buffer"
        );
        Ok(())
    }

    /// Builds a three-level base (root, two mid-level indexes, four leaves)
    /// and buffers the probe key at two depths of the spine: `[1]` in the
    /// mid-level index covering it, `root_op` (newer) at the root. A twin of
    /// the returned tree is persisted first purely to verify the placement.
    async fn buffered_at_two_levels(
        root_op: NoveltyOp<Vec<u8>>,
    ) -> Result<(SpecHitchhiker, SpecTree, SpecStorage)> {
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

        // The writes are deterministic, so a persisted twin proves where the
        // returned (still live) tree holds its ops.
        let twin = apply_probe_writes(&base, &root_op, &storage).await?;
        let mut delta = Delta::zero();
        let twin_root = twin.persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        assert_probe_spans_two_levels(&twin_root, &storage, &root_op).await?;

        let tree = apply_probe_writes(&base, &root_op, &storage).await?;
        Ok((tree, base, storage))
    }

    /// For a key buffered at two depths the SHALLOWEST op is the newest:
    /// writes land in the root buffer and a flush only moves ops downward, so
    /// deeper always means older. The live read resolves this correctly (the
    /// control); canonicalize must agree with it instead of letting the
    /// deeper, older op replay last.
    #[dialog_common::test]
    async fn it_resolves_the_shallowest_op_across_levels() -> Result<()> {
        let (tree, base, mut storage) = buffered_at_two_levels(NoveltyOp::Assert(vec![2])).await?;

        assert_eq!(
            tree.get(&probe(), &storage).await?,
            Some(vec![2]),
            "control: the live buffered read resolves the root-most op"
        );

        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(&storage, &mut delta).await?;
        spec_flush(&mut delta, &mut storage).await?;

        assert_eq!(
            canonical.get(&probe(), &storage).await?,
            Some(vec![2]),
            "canonicalize must let the shallowest (newest) op win"
        );

        let mut delta = Delta::zero();
        let expected = base
            .edit_with_manifest(&storage)
            .await?
            .insert(far_key(), vec![9], &storage)
            .await?
            .insert(probe(), vec![2], &storage)
            .await?
            .persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        assert_eq!(
            canonical.root(),
            expected.root(),
            "canonicalize must converge on the canonical build of the surviving facts"
        );
        Ok(())
    }

    /// The retract flavor of the cross-level precedence pin: a root-buffered
    /// retract must hide the older assert parked one level down.
    #[dialog_common::test]
    async fn it_resolves_a_shallow_retract_over_a_deep_assert() -> Result<()> {
        let (tree, base, mut storage) = buffered_at_two_levels(NoveltyOp::Retract).await?;

        assert_eq!(
            tree.get(&probe(), &storage).await?,
            None,
            "control: the live buffered read resolves the root-most retract"
        );

        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(&storage, &mut delta).await?;
        spec_flush(&mut delta, &mut storage).await?;

        assert_eq!(
            canonical.get(&probe(), &storage).await?,
            None,
            "canonicalize must let the shallowest retract hide the deeper assert"
        );

        let mut delta = Delta::zero();
        let expected = base
            .edit_with_manifest(&storage)
            .await?
            .insert(far_key(), vec![9], &storage)
            .await?
            .delete(&probe(), &storage)
            .await?
            .persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        assert_eq!(
            canonical.root(),
            expected.root(),
            "canonicalize must converge on the canonical build without the probe"
        );
        Ok(())
    }

    /// After a buffered persist and a cold reopen, the stored tree's readers
    /// (point read and range scan) must also resolve the shallowest op.
    #[dialog_common::test]
    async fn it_reads_the_shallowest_op_after_a_buffered_persist() -> Result<()> {
        let (tree, _base, mut storage) = buffered_at_two_levels(NoveltyOp::Assert(vec![2])).await?;

        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        assert_probe_spans_two_levels(&root, &storage, &NoveltyOp::Assert(vec![2])).await?;

        let reopened = SpecTree::from_hash(root);
        assert_eq!(
            reopened.get(&probe(), &storage).await?,
            Some(vec![2]),
            "the point read must resolve the root-most op"
        );

        let mut scanned: Vec<(SpecKey, Vec<u8>)> = Vec::new();
        {
            let stream = reopened.stream_range(.., &storage);
            futures_util::pin_mut!(stream);
            while let Some(entry) = futures_util::StreamExt::next(&mut stream)
                .await
                .transpose()?
            {
                scanned.push((entry.key, entry.value));
            }
        }
        let probed = scanned
            .iter()
            .find(|(key, _)| key == &probe())
            .map(|(_, value)| value.clone());
        assert_eq!(
            probed,
            Some(vec![2]),
            "the range scan must resolve the root-most op"
        );
        Ok(())
    }

    /// The retract flavor of the reopened-reader pin: a sealed root-level
    /// retract must hide the older assert sealed one level down.
    #[dialog_common::test]
    async fn it_hides_a_shallow_retract_after_a_buffered_persist() -> Result<()> {
        let (tree, _base, mut storage) = buffered_at_two_levels(NoveltyOp::Retract).await?;

        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        assert_probe_spans_two_levels(&root, &storage, &NoveltyOp::Retract).await?;

        let reopened = SpecTree::from_hash(root);
        assert_eq!(
            reopened.get(&probe(), &storage).await?,
            None,
            "the point read must let the root-most retract hide the deeper assert"
        );

        let mut scanned: Vec<SpecKey> = Vec::new();
        {
            let stream = reopened.stream_range(.., &storage);
            futures_util::pin_mut!(stream);
            while let Some(entry) = futures_util::StreamExt::next(&mut stream)
                .await
                .transpose()?
            {
                scanned.push(entry.key);
            }
        }
        assert!(
            !scanned.contains(&probe()),
            "the range scan must let the root-most retract hide the deeper assert"
        );
        Ok(())
    }

    /// The differential must surface the shallowest op's value for a key
    /// pending at two depths, not the deeper, older one.
    #[dialog_common::test]
    async fn it_differentiates_the_shallowest_op_across_levels() -> Result<()> {
        let (tree, base, mut storage) = buffered_at_two_levels(NoveltyOp::Assert(vec![2])).await?;

        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        let reopened = SpecTree::from_hash(root);

        let mut adds: Vec<(SpecKey, Vec<u8>)> = Vec::new();
        {
            let differential = base.differentiate(&reopened, &storage, &storage);
            futures_util::pin_mut!(differential);
            while let Some(change) = futures_util::StreamExt::next(&mut differential).await {
                if let Change::Add(entry) = change? {
                    adds.push((entry.key, entry.value));
                }
            }
        }

        let probe_adds: Vec<Vec<u8>> = adds
            .iter()
            .filter(|(key, _)| key == &probe())
            .map(|(_, value)| value.clone())
            .collect();
        assert_eq!(
            probe_adds,
            vec![vec![2]],
            "the difference must carry the newest (shallowest) op's value"
        );
        Ok(())
    }

    /// The retract flavor of the differential pin: with a root-level retract
    /// over a deeper assert, the difference must report a removal for the
    /// probe and no addition.
    #[dialog_common::test]
    async fn it_differentiates_a_shallow_retract_over_a_deep_assert() -> Result<()> {
        let (tree, base, mut storage) = buffered_at_two_levels(NoveltyOp::Retract).await?;

        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        let reopened = SpecTree::from_hash(root);

        let mut probe_adds: Vec<Vec<u8>> = Vec::new();
        let mut probe_removed = false;
        {
            let differential = base.differentiate(&reopened, &storage, &storage);
            futures_util::pin_mut!(differential);
            while let Some(change) = futures_util::StreamExt::next(&mut differential).await {
                match change? {
                    Change::Add(entry) if entry.key == probe() => {
                        probe_adds.push(entry.value);
                    }
                    Change::Remove(entry) if entry.key == probe() => {
                        probe_removed = true;
                    }
                    _ => {}
                }
            }
        }

        assert!(
            probe_adds.is_empty(),
            "the root-most retract must hide the deeper assert from the difference, \
             but it reported adds: {probe_adds:?}"
        );
        assert!(
            probe_removed,
            "the difference must report the probe's removal"
        );
        Ok(())
    }

    /// A persisted-but-never-canonicalized tree, reopened cold, must still
    /// canonicalize: its buffers travel in the stored bytes, so canonicalize
    /// has to load the root rather than pass the unloaded hash through
    /// verbatim.
    #[dialog_common::test]
    async fn it_canonicalizes_a_reopened_buffered_tree() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let keys: Vec<u32> = (0..300).collect();
        let expected = sequential(&keys, &mut storage).await?;

        // A giant buffer keeps every op (after the first) in the root buffer,
        // so the persisted tree is unambiguously non-canonical.
        let mut tree = TestHitchhiker::empty().with_op_buf_size(100_000);
        for &k in &keys {
            tree = tree
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        flush(&mut delta, &mut storage).await?;

        let reopened = TestTree::from_hash(root);
        let mut delta = Delta::zero();
        let canonical = HitchhikerTree::open(&reopened)
            .canonicalize(&storage, &mut delta)
            .await?;
        flush(&mut delta, &mut storage).await?;
        assert_eq!(
            canonical.root(),
            expected.root(),
            "a reopened buffered tree must canonicalize to the sequential root"
        );

        // Idempotence: canonicalizing the canonical result changes nothing.
        let mut delta = Delta::zero();
        let again = HitchhikerTree::open(&canonical)
            .canonicalize(&storage, &mut delta)
            .await?;
        assert_eq!(
            again.root(),
            canonical.root(),
            "canonicalize must be idempotent"
        );
        Ok(())
    }

    /// Sealed novelty BELOW a loaded root must be drained too: a later write
    /// loads the root, but the mid-level buffers persisted earlier still sit
    /// in `Node::Persistent` children, which the drain must lift.
    #[dialog_common::test]
    async fn it_canonicalizes_sealed_novelty_below_a_loaded_root() -> Result<()> {
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

        // Park one new key in each mid-level buffer via the one-op cascade,
        // then persist without canonicalizing.
        let left = encode_key(b"bb", 1, 1);
        let right = encode_key(b"mm", 1, 1);
        let mut tree = SpecHitchhiker::open(&base).with_op_buf_size(1);
        tree = tree.insert(left, vec![1], &storage).await?;
        tree = tree.insert(right, vec![9], &storage).await?;
        let mut delta = Delta::zero();
        let root = tree.persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;

        // A later write loads the root; its children stay persistent links
        // carrying the sealed buffers.
        let late = encode_key(b"cc", 1, 1);
        let reopened = SpecTree::from_hash(root);
        let tree = SpecHitchhiker::open(&reopened)
            .insert(late, vec![7], &storage)
            .await?;
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(&storage, &mut delta).await?;
        spec_flush(&mut delta, &mut storage).await?;

        assert_eq!(
            canonical.get(&left, &storage).await?,
            Some(vec![1]),
            "a sealed mid-level op must survive canonicalize"
        );
        assert_eq!(
            canonical.get(&right, &storage).await?,
            Some(vec![9]),
            "a sealed mid-level op must survive canonicalize"
        );
        assert_eq!(
            canonical.get(&late, &storage).await?,
            Some(vec![7]),
            "the live root op must survive canonicalize"
        );

        let mut delta = Delta::zero();
        let expected = base
            .edit_with_manifest(&storage)
            .await?
            .insert(left, vec![1], &storage)
            .await?
            .insert(right, vec![9], &storage)
            .await?
            .insert(late, vec![7], &storage)
            .await?
            .persist(&mut delta)?;
        spec_flush(&mut delta, &mut storage).await?;
        assert_eq!(
            canonical.root(),
            expected.root(),
            "canonicalize must equal the canonical build of all surviving facts"
        );
        Ok(())
    }

    /// A buffered persist over a tree written under a NON-default format must
    /// stamp that tree's manifest into the nodes it writes, not the default.
    #[dialog_common::test]
    async fn it_stamps_the_trees_manifest_on_buffered_persist() -> Result<()> {
        let custom = Manifest {
            fanout_n: 2,
            ..Manifest::default()
        };
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut edit = TransientTree::<[u8; 4], Vec<u8>>::with_manifest(
            NULL_BLAKE3_HASH.clone(),
            Cache::new(),
            custom,
        );
        for k in 0..50u32 {
            edit = edit
                .insert(k.to_be_bytes(), k.to_be_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let base = edit.persist(&mut delta)?;
        flush(&mut delta, &mut storage).await?;

        let tree = HitchhikerTree::open(&base)
            .insert(999u32.to_be_bytes(), vec![2], &storage)
            .await?;
        let mut delta = Delta::zero();
        tree.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            let node = PersistentNode::<[u8; 4], Vec<u8>>::new(buffer);
            assert_eq!(
                node.manifest()?,
                custom,
                "a buffered persist must stamp the tree's manifest, not the default"
            );
        }
        Ok(())
    }

    /// Canonicalizing buffered writes over a non-default-format tree must
    /// replay and stamp under that tree's manifest: the result must equal a
    /// canonical build of the union under the same format.
    #[dialog_common::test]
    async fn it_canonicalizes_under_the_trees_manifest() -> Result<()> {
        let custom = Manifest {
            fanout_n: 2,
            ..Manifest::default()
        };
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut edit = TransientTree::<[u8; 4], Vec<u8>>::with_manifest(
            NULL_BLAKE3_HASH.clone(),
            Cache::new(),
            custom,
        );
        for k in 0..40u32 {
            edit = edit
                .insert(k.to_be_bytes(), k.to_be_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let base = edit.persist(&mut delta)?;
        flush(&mut delta, &mut storage).await?;

        let mut tree = HitchhikerTree::open(&base).with_op_buf_size(4);
        for k in 40..80u32 {
            tree = tree
                .insert(k.to_be_bytes(), k.to_be_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let canonical = tree.canonicalize(&storage, &mut delta).await?;
        flush(&mut delta, &mut storage).await?;

        let mut oracle = TransientTree::<[u8; 4], Vec<u8>>::with_manifest(
            NULL_BLAKE3_HASH.clone(),
            Cache::new(),
            custom,
        );
        for k in 0..80u32 {
            oracle = oracle
                .insert(k.to_be_bytes(), k.to_be_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let expected = oracle.persist(&mut delta)?;
        flush(&mut delta, &mut storage).await?;

        assert_eq!(
            canonical.root(),
            expected.root(),
            "canonicalize must replay and stamp under the tree's own manifest"
        );
        Ok(())
    }

    /// Lifts every sealed link buffer on the live spine, discarding all cached
    /// stored encodings, so the next persist re-encodes every buffer from its
    /// decoded ops.
    fn lift_all_buffers(node: &mut TransientNode<[u8; 4], Vec<u8>>) -> Result<()> {
        if let TransientNode::Index(index) = node {
            index.novelty.lift_all::<[u8; 4]>()?;
            for child in &mut index.children {
                if let Node::Transient(child) = child {
                    lift_all_buffers(child)?;
                }
            }
        }
        Ok(())
    }

    /// Counts the link buffers on the live spine still carrying their sealed
    /// stored encoding.
    fn count_sealed_buffers(node: &TransientNode<[u8; 4], Vec<u8>>) -> usize {
        match node {
            TransientNode::Segment(_) => 0,
            TransientNode::Index(index) => {
                index.novelty.sealed_links()
                    + index
                        .children
                        .iter()
                        .map(|child| match child {
                            Node::Transient(child) => count_sealed_buffers(child),
                            Node::Persistent(_) => 0,
                        })
                        .sum::<usize>()
            }
        }
    }

    /// The persist path that reuses sealed buffer encodings must produce a
    /// node byte-identical (same root hash) to the path that re-encodes every
    /// buffer from its decoded ops. This is THE cache-correctness pin: the
    /// stored bytes may never depend on whether a link's buffer was touched.
    #[dialog_common::test]
    async fn it_persists_sealed_buffers_byte_identical_to_fresh_encodes() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(MemoryStorageBackend::default());

        // A tree with buffered novelty across several links: build a broad
        // base, then buffer scattered writes at the root and seal them. Pin a
        // small segment target so the base branches into several links (the
        // shipped ~64 KiB default would pack these into one leaf, leaving no
        // sealed sibling buffers to exercise). `HitchhikerTree::open` reads
        // this manifest back from the base, so every path below stays
        // consistent.
        let manifest = Manifest {
            max_segment: 512,
            frame_ceiling_factor: 0,
            ..Manifest::default()
        };
        let keys: Vec<u32> = (0..500).collect();
        let mut base = TestTree::empty();
        {
            let mut delta = Delta::zero();
            for &k in &keys {
                base =
                    TransientTree::with_manifest(base.root().clone(), base.node_cache(), manifest)
                        .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                        .await?
                        .persist(&mut delta)?;
                flush(&mut delta, &mut storage).await?;
            }
        }
        let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(100_000);
        for k in (500..560u32).step_by(3) {
            buffered = buffered
                .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                .await?;
        }
        let mut delta = Delta::zero();
        let root = buffered.persist(&mut delta)?;
        flush(&mut delta, &mut storage).await?;

        // Both paths reopen the sealed tree and write the same small batch,
        // which touches at most a couple of links and leaves the rest sealed.
        let batch: Vec<u32> = vec![700, 701];
        let mut roots = Vec::new();
        for fresh in [false, true] {
            let sealed: TestTree = PersistentTree::seal(root.clone(), Cache::new());
            let mut tree = HitchhikerTree::open(&sealed).with_op_buf_size(100_000);
            for &k in &batch {
                tree = tree
                    .insert(k.to_le_bytes(), k.to_le_bytes().to_vec(), &storage)
                    .await?;
            }
            let super::HitchhikerRoot::Loaded(node) = &mut tree.root else {
                panic!("a written tree must hold a loaded root");
            };
            if fresh {
                lift_all_buffers(node)?;
                assert_eq!(
                    count_sealed_buffers(node),
                    0,
                    "the fresh path must have no sealed buffer left"
                );
            } else {
                assert!(
                    count_sealed_buffers(node) > 0,
                    "the cached path must actually exercise sealed buffers"
                );
            }
            let mut delta = Delta::zero();
            roots.push(tree.persist(&mut delta)?);
        }

        assert_eq!(
            roots[0], roots[1],
            "reused sealed encodings must persist byte-identical to fresh encodes"
        );
        Ok(())
    }
}
