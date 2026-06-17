//! Hitchhiker (fractal / Bε) tree write buffering over the search tree.
//!
//! A [`HitchhikerTree`] holds the same content-addressed spine a
//! [`PersistentTree`] does, but every index node may carry a bounded buffer of
//! pending ops (its `novelty`). A write appends an op to the root's buffer;
//! when a buffer overflows it cascades one level down toward the leaves. The
//! expensive canonical rebuild is amortized across many writes instead of paid
//! eagerly on every one.
//!
//! The algorithm mirrors the reference hitchhiker tree's `enqueue`:
//!
//! - A **leaf** does not buffer. Ops that reach a leaf are deferred and applied
//!   through the canonical [`TransientTree`] insert/delete path, which reshapes
//!   exactly as a sequential edit would.
//! - An **index with room** (`novelty.len() + msgs.len() <= op_buf_size`)
//!   appends the ops to its `novelty`, keeping it sorted by key.
//! - An **index that overflows** takes `novelty ++ msgs`, stable-sorts by key,
//!   partitions the ops by which child's key range they fall into, and recurses
//!   one level down into each child. Its own `novelty` is then cleared.
//!
//! [`canonicalize`](HitchhikerTree::canonicalize) forces a full flush: every
//! buffer is pushed all the way to the leaves, leaving all `novelty` empty. The
//! result is the deterministic, history-independent canonical tree, byte for
//! byte identical to a sequential build of the same fact set.

use std::marker::PhantomData;

use dialog_common::{Blake3Hash, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
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
    Accessor, Buffer, Cache, ContentAddressedStorage, Delta, DialogSearchTreeError, Distribution,
    Geometric, Key, Node, NoveltyEntry, NoveltyOp, PersistentNode, PersistentTree, SymmetryWith,
    TransientNode, TransientRootParts, TransientSegment, TransientTree, Value,
};

/// The default per-node novelty capacity.
///
/// Hitchhiker trees run buffers several times the fan-out so most writes touch
/// only the upper buffers. The base fan-out here (the geometric distribution's
/// expected children per node) is around 254; a multiple of that keeps the
/// amortization a hitchhiker buffer is meant to provide. Tunable per tree via
/// [`HitchhikerTree::with_op_buf_size`].
pub const DEFAULT_OP_BUF_SIZE: usize = 1024;

/// A boxed future returning an edited [`TransientNode`], the shape `enqueue`
/// returns so its recursion can be expressed as a plain `async fn` body inside a
/// `Box::pin`.
type NodeFuture<'a, Key, Value> = std::pin::Pin<
    Box<
        dyn std::future::Future<Output = Result<TransientNode<Key, Value>, DialogSearchTreeError>>
            + 'a,
    >,
>;

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
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
    D: Distribution,
{
    root: HitchhikerRoot<Key, Value>,
    cache: Cache<Blake3Hash, Buffer>,
    op_buf_size: usize,
    distribution: PhantomData<D>,
}

impl<Key, Value, D> HitchhikerTree<Key, Value, D>
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
            distribution: PhantomData,
        }
    }

    /// Opens an empty buffered tree.
    pub fn empty() -> Self {
        Self {
            root: HitchhikerRoot::Unloaded(NULL_BLAKE3_HASH.clone()),
            cache: Cache::new(),
            op_buf_size: DEFAULT_OP_BUF_SIZE,
            distribution: PhantomData,
        }
    }

    /// Sets the per-node novelty capacity (the write-amplification knob).
    pub fn with_op_buf_size(mut self, op_buf_size: usize) -> Self {
        self.op_buf_size = op_buf_size.max(1);
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
                key,
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
                key,
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
        msgs: Vec<NoveltyEntry<Key, Value>>,
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
                Some(TransientNode::try_from(&node)?)
            }
        };

        let mut deferred = Vec::new();
        let node = match loaded {
            Some(node) => Some(
                enqueue::<Key, Value, D, Backend>(
                    node,
                    msgs,
                    self.op_buf_size,
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
            Some(node) => TransientTree::<Key, Value, D>::from_loaded(node, self.cache.clone()),
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
        let edit = match self.root {
            HitchhikerRoot::Unloaded(hash) => {
                TransientTree::<Key, Value, D>::new(hash, self.cache.clone())
            }
            HitchhikerRoot::Loaded(mut node) => {
                let mut ops = Vec::new();
                drain_novelty(&mut node, &mut ops);
                // A higher node's buffer interleaves with lower ones, so order
                // the full set by key before replay (stable, last op last).
                ops.sort_by(|a, b| a.key.cmp(&b.key));
                let edit = TransientTree::<Key, Value, D>::from_loaded(node, self.cache.clone());
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
                    // here wins over anything deeper.
                    if let Some(op) = novelty_lookup(&index.novelty, key) {
                        return Ok(match op {
                            NoveltyOp::Assert(value) => Some(value.clone()),
                            NoveltyOp::Retract => None,
                        });
                    }
                    let at = child_index::<Key, Value>(&index.children, key)?;
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
            HitchhikerRoot::Loaded(node) => Ok(node.persist(delta)?.hash().clone()),
        }
    }
}

/// Routes `msgs` into the subtree rooted at `node`, cascading one level on
/// overflow, collecting leaf-bound ops into `deferred`.
///
/// This is the faithful counterpart of the reference hitchhiker `enqueue`:
///
/// - **Leaf**: a segment buffers nothing; its `msgs` are appended to `deferred`
///   for the caller to apply through the canonical edit path.
/// - **Index with room**: `msgs` are merged into `node.novelty` (kept sorted)
///   and the node is returned.
/// - **Index overflow**: `node.novelty ++ msgs` is stable-sorted by key and
///   partitioned by each child's upper bound; each child receives the ops in its
///   range via a recursive one-level `enqueue`, and `node.novelty` is cleared.
///   The last child absorbs every remaining op (keys beyond all bounds route to
///   the rightmost child, matching the read descent's last-child fallback).
fn enqueue<'a, Key, Value, D, Backend>(
    node: TransientNode<Key, Value>,
    msgs: Vec<NoveltyEntry<Key, Value>>,
    op_buf_size: usize,
    deferred: &'a mut Vec<NoveltyEntry<Key, Value>>,
    accessor: &'a Accessor<Backend>,
) -> NodeFuture<'a, Key, Value>
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
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
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

        // Room: merge into this node's novelty and stop.
        if index.novelty.len() + msgs.len() <= op_buf_size {
            merge_sorted(&mut index.novelty, msgs);
            return Ok(node);
        }

        // Overflow: combine the buffered ops with the incoming ones, stable-sort
        // by key, and cascade one level into the children.
        let mut combined = std::mem::take(&mut index.novelty);
        combined.extend(msgs);
        combined.sort_by(|a, b| a.key.cmp(&b.key));

        // Lift every child so the recursion can descend into editable nodes.
        for child in &mut index.children {
            lift_child(child, accessor).await?;
        }

        // Partition the ops by child upper bound, left to right; the last child
        // takes everything that remains.
        let child_count = index.children.len();
        let mut rest = combined.into_iter().peekable();
        for at in 0..child_count {
            let took: Vec<NoveltyEntry<Key, Value>> = if at + 1 == child_count {
                rest.by_ref().collect()
            } else {
                let bound = index.children[at].upper_bound()?;
                let mut took = Vec::new();
                while let Some(entry) = rest.peek() {
                    if entry.key <= bound {
                        took.push(rest.next().expect("peeked"));
                    } else {
                        break;
                    }
                }
                took
            };

            if took.is_empty() {
                continue;
            }

            let child = std::mem::replace(
                &mut index.children[at],
                Node::Transient(TransientNode::Segment(TransientSegment {
                    entries: Vec::new(),
                })),
            )
            .into_transient()?;
            let updated =
                enqueue::<Key, Value, D, Backend>(child, took, op_buf_size, deferred, accessor)
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
    Key: self::Key + PartialOrd<Key::Archived> + PartialEq<Key::Archived>,
    Key::Archived: PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord
        + for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>,
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    if let Node::Persistent(link) = child {
        let persistent = accessor.get_node(&link.node).await?;
        *child = TransientNode::try_from(&persistent)?.into();
    }
    Ok(())
}

/// Merges `incoming` into the already-sorted `novelty`, keeping it sorted by key
/// and stable within equal keys (so the last op for a key remains last, which
/// last-op-wins resolution relies on).
fn merge_sorted<Key, Value>(
    novelty: &mut Vec<NoveltyEntry<Key, Value>>,
    incoming: Vec<NoveltyEntry<Key, Value>>,
) where
    Key: Ord,
{
    novelty.extend(incoming);
    novelty.sort_by(|a, b| a.key.cmp(&b.key));
}

/// Finds the authoritative buffered op for `key` in a node's (sorted) novelty,
/// or `None` if the key is not buffered here.
///
/// Within a key the last entry wins (last-op-wins), so the search returns the
/// last entry whose key equals `key`. The buffer is sorted by key and stable
/// within equal keys, so the run of equal-key entries is contiguous and its last
/// element is the most recent op.
fn novelty_lookup<'a, Key, Value>(
    novelty: &'a [NoveltyEntry<Key, Value>],
    key: &Key,
) -> Option<&'a NoveltyOp<Value>>
where
    Key: Ord,
{
    let at = novelty.partition_point(|entry| entry.key < *key);
    if at < novelty.len() && novelty[at].key == *key {
        // Walk to the last entry with this key (last op wins).
        let mut last = at;
        while last + 1 < novelty.len() && novelty[last + 1].key == *key {
            last += 1;
        }
        Some(&novelty[last].op)
    } else {
        None
    }
}

/// Index of the child whose subtree covers `key`: the first child whose upper
/// bound is `>= key`, or the last child when the key exceeds every bound.
///
/// Mirrors the transient tree's routing so a buffered read descends the same way
/// a canonical edit would.
fn child_index<Key, Value>(
    children: &[Node<Key, Value>],
    key: &Key,
) -> Result<usize, DialogSearchTreeError>
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
        if children[at].upper_bound_ref()? < key {
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
    ops: Vec<NoveltyEntry<Key, Value>>,
    storage: &ContentAddressedStorage<Backend>,
) -> Result<TransientTree<Key, Value, D>, DialogSearchTreeError>
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
    Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
        + ConditionalSync,
{
    for entry in ops {
        edit = match entry.op {
            NoveltyOp::Assert(value) => edit.insert(entry.key, value, storage).await?,
            NoveltyOp::Retract => edit.delete(&entry.key, storage).await?,
        };
    }
    Ok(edit)
}

/// Drains every `novelty` buffer in the subtree rooted at `node` into `ops`,
/// in ascending key order, leaving all buffers empty.
///
/// A pre-order walk that prepends each index node's (sorted) buffer before its
/// children would not yield global key order, so instead the buffers are
/// gathered and the caller sorts. Within one node the buffer is already sorted;
/// across nodes a higher buffer's ops may interleave with a lower one's, so the
/// caller (canonicalize) stable-sorts the full set by key before replay.
fn drain_novelty<Key, Value>(
    node: &mut TransientNode<Key, Value>,
    ops: &mut Vec<NoveltyEntry<Key, Value>>,
) where
    Key: Clone,
    Value: Clone,
{
    if let TransientNode::Index(index) = node {
        ops.append(&mut index.novelty);
        for child in &mut index.children {
            if let Node::Transient(child) = child {
                drain_novelty(child, ops);
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

    use super::HitchhikerTree;
    use crate::{Buffer, ContentAddressedStorage, Delta, PersistentTree};

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
}
