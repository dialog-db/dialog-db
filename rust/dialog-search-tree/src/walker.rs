use std::{
    future::Future,
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    task::Poll,
};

use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use nonempty::NonEmpty;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use std::sync::Arc;

use crate::{
    Accessor, ArchivedNodeBody, DecodedKeys, DialogSearchTreeError, Entry, Key, Link, NoveltyOp,
    PersistentNode, Value, into_owned,
};

/// How many sibling reads a range scan keeps in flight while it walks.
///
/// A scan reads a whole run of siblings, one after another, and each read that
/// misses locally can cost a round trip. Reading ahead of the walk turns a run
/// of round trips into an overlapping few, and bounding it keeps a scan that
/// stops early from having fetched much it never looked at.
const PREFETCH_CONCURRENCY: usize = 16;

/// How [`TreeWalker::stream`] materializes the keys of the entries it yields.
///
/// The typed instantiation (any [`Key`]) rebuilds the tree's key from the
/// entry bytes — one allocation and copy per yielded entry. The
/// [`KeyHandle`] instantiation instead hands out handles into the leaf's
/// memoized decoded-keys arena, so a warm leaf's entries yield with NO
/// per-entry copy; consumers that only need the key BYTES (the artifact
/// scan, which re-splits them itself) read through `as_ref`.
pub trait ScanKey: Sized + ConditionalSend {
    /// Build a yielded key from plain bytes: a buffered novelty op, or a
    /// cold leaf's streaming decode (whose per-key buffer is transient).
    fn from_entry_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError>;

    /// Build a yielded key from a warm leaf's memoized arena. Defaults to
    /// copying out via [`from_entry_bytes`](Self::from_entry_bytes).
    fn from_arena(keys: &Arc<DecodedKeys>, at: usize) -> Result<Self, DialogSearchTreeError> {
        Self::from_entry_bytes(keys.get(at).ok_or_else(|| {
            DialogSearchTreeError::Operation(format!("decoded key index {at} out of range"))
        })?)
    }
}

/// The typed instantiation of [`ScanKey`]: rebuilds the tree's own [`Key`]
/// from the entry bytes. A wrapper rather than a blanket impl so it cannot
/// conflict with [`KeyHandle`]'s impl under coherence.
struct TypedKey<K>(K);

impl<K: Key + ConditionalSend> ScanKey for TypedKey<K> {
    fn from_entry_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        Ok(TypedKey(K::try_from_bytes(bytes)?))
    }
}

/// A yielded entry's key bytes, without the typed-key copy: either a handle
/// into a warm leaf's memoized [`DecodedKeys`] arena (cloning clones the
/// `Arc`; the arena stays alive as long as any handle does), or an owned
/// copy for the sources that have no arena — buffered novelty ops and cold
/// leaves' streaming decodes.
///
/// Order note: handles compare/read only through [`as_ref`](AsRef), so the
/// tree's byte order is preserved exactly.
#[derive(Clone, Debug)]
pub enum KeyHandle {
    /// An owned copy (novelty op or cold streaming decode).
    Owned(Vec<u8>),
    /// A borrowed slice of a warm leaf's decoded arena. `at` is validated
    /// against the arena on construction ([`ScanKey::from_arena`]).
    Arena {
        /// The leaf's decoded keys, shared with the node's memo.
        keys: Arc<DecodedKeys>,
        /// This entry's index in the arena.
        at: usize,
    },
}

impl AsRef<[u8]> for KeyHandle {
    fn as_ref(&self) -> &[u8] {
        match self {
            KeyHandle::Owned(bytes) => bytes,
            // In range by construction: `from_arena` validated `at`.
            KeyHandle::Arena { keys, at } => keys.get(*at).expect("arena handle in range"),
        }
    }
}

impl ScanKey for KeyHandle {
    fn from_entry_bytes(bytes: &[u8]) -> Result<Self, DialogSearchTreeError> {
        Ok(Self::Owned(bytes.to_vec()))
    }

    fn from_arena(keys: &Arc<DecodedKeys>, at: usize) -> Result<Self, DialogSearchTreeError> {
        if keys.get(at).is_none() {
            return Err(DialogSearchTreeError::Operation(format!(
                "decoded key index {at} out of range"
            )));
        }
        Ok(Self::Arena {
            keys: Arc::clone(keys),
            at,
        })
    }
}

/// A buffered op that won its key during [`pending_for_leaf`]'s collection
/// pass, located by position instead of decoded: `level` names the path layer
/// whose descended link buffers it, `at` its entry index there, and `slot` its
/// value-table slot (tracked while streaming, so decoding it later costs no
/// polarity re-scan). Values are decoded only for the winners that survive
/// every narrowing and shadowing step, in the final decode pass.
struct PendingWinner {
    key: Vec<u8>,
    level: usize,
    at: usize,
    slot: usize,
}

/// Merges two key-sorted winner lists, keeping the `shallow` entry where both
/// hold a key: across the path the root-most layer's op is the newest. Runs
/// linear in the combined length, replacing the per-key membership scan that
/// made accumulation quadratic in the buffered-op count.
fn merge_winners(shallow: Vec<PendingWinner>, deeper: Vec<PendingWinner>) -> Vec<PendingWinner> {
    if deeper.is_empty() {
        return shallow;
    }
    if shallow.is_empty() {
        return deeper;
    }
    let mut merged = Vec::with_capacity(shallow.len() + deeper.len());
    let mut left = shallow.into_iter().peekable();
    let mut right = deeper.into_iter().peekable();
    loop {
        match (left.peek(), right.peek()) {
            (Some(l), Some(r)) => match l.key.cmp(&r.key) {
                std::cmp::Ordering::Less => merged.push(left.next().expect("peeked")),
                std::cmp::Ordering::Greater => merged.push(right.next().expect("peeked")),
                std::cmp::Ordering::Equal => {
                    merged.push(left.next().expect("peeked"));
                    right.next();
                }
            },
            (Some(_), None) => merged.push(left.next().expect("peeked")),
            (None, Some(_)) => merged.push(right.next().expect("peeked")),
            (None, None) => break,
        }
    }
    merged
}

/// Whether `key` lies below `start`, i.e. before the walk's range begins.
fn below_start(start: &Bound<&[u8]>, key: &[u8]) -> bool {
    match start {
        Bound::Included(bound) => key < *bound,
        Bound::Excluded(bound) => key <= *bound,
        Bound::Unbounded => false,
    }
}

/// Whether `key` lies past `end`, i.e. beyond the walk's range.
fn past_end_bytes(end: &Bound<&[u8]>, key: &[u8]) -> bool {
    match end {
        Bound::Included(bound) => key > *bound,
        Bound::Excluded(bound) => key >= *bound,
        Bound::Unbounded => false,
    }
}

/// The buffered ops covering the leaf a walk currently sits on, resolved to one
/// winning op per key and sorted by key, restricted to the walk's `[start, end]`
/// byte bounds.
///
/// `path` is the walk's ancestor stack, root first, each entry paired with the
/// index of the child it descended into. Novelty is stored per child link, so
/// each level contributes exactly the descended link's buffer; what a
/// shallower level contributed is narrowed to the descended link's share at
/// each deeper level by the same partition rule a flush uses (child `at`
/// takes `[sep(at), sep(at + 1))`, child 0 also takes everything below its
/// own separator, the last child runs open-ended). Successive narrowing
/// leaves, at the leaf, exactly the ops a flush would deliver to it — no
/// cross-level span inheritance exists to get wrong.
///
/// The range restriction is sound because the walker only ever surfaces a
/// buffered op after an in-range check: an op outside the bounds can never
/// yield, so collecting it (let alone decoding its value) is pure waste — and
/// for a point read it was most of the buffered-path cost, since the root
/// buffer holds ops for the whole subtree while the probe wants one key.
///
/// Precedence: WITHIN one link's buffer the last entry for a key is the newest
/// and wins; ACROSS the path the first (root-most) layer holding the key wins,
/// because writes land in the root buffer and a flush only moves ops downward,
/// so deeper always means older.
#[allow(clippy::type_complexity)]
fn pending_for_leaf<Key, Value>(
    path: &[(PersistentNode<Key, Value>, Option<usize>)],
    start: Bound<&[u8]>,
    end: Bound<&[u8]>,
) -> Result<Vec<(Vec<u8>, NoveltyOp<Value>)>, DialogSearchTreeError>
where
    Key: self::Key + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
{
    let mut winners: Vec<PendingWinner> = Vec::new();
    for (level, (node, descended)) in path.iter().enumerate() {
        let ArchivedNodeBody::Index(index) = node.body() else {
            continue;
        };
        let Some(at) = *descended else { continue };

        // Narrow what shallower levels handed down to the descended link's
        // share. Ops already collected sit within this node's range, so the
        // partition against this node's own separators is all it takes.
        if !winners.is_empty() {
            let lower = if at == 0 {
                None
            } else {
                Some(index.separator(at)?)
            };
            let upper = if at + 1 < index.len() {
                Some(index.separator(at + 1)?)
            } else {
                None
            };
            winners.retain(|winner| {
                lower
                    .as_ref()
                    .is_none_or(|lower| winner.key.as_slice() >= lower.as_slice())
                    && upper
                        .as_ref()
                        .is_none_or(|upper| winner.key.as_slice() < upper.as_slice())
            });
        }

        // This level's buffer is deeper (older) than everything accumulated,
        // so on a shared key the shallower layer's op wins the merge. Within
        // the buffer equal keys are contiguous and the last op of a run is
        // the newest. One streaming pass collects the in-range run winners
        // by position; no key outside the walk's bounds is materialized and
        // no value is decoded here at all.
        let Some(buffer) = index.buffer_for(at) else {
            continue;
        };
        let mut runs: Vec<PendingWinner> = Vec::new();
        // `keys()` validates the buffer (count vs polarity vs value tables),
        // so the polarity reads below cannot misread a well-formed buffer.
        let mut keys = buffer.keys::<Key>()?;
        let mut asserts = 0usize;
        while let Some((entry_at, key)) = keys.next_key()? {
            let slot = asserts;
            if buffer.polarity.get(entry_at).copied() == Some(1) {
                asserts += 1;
            }
            // Keys stream in sorted order: everything past the end bound
            // stays out of range for the rest of the buffer.
            if past_end_bytes(&end, key) {
                break;
            }
            if below_start(&start, key) {
                continue;
            }
            match runs.last_mut() {
                Some(last) if last.key.as_slice() == key => {
                    last.at = entry_at;
                    last.slot = slot;
                }
                _ => runs.push(PendingWinner {
                    key: key.to_vec(),
                    level,
                    at: entry_at,
                    slot,
                }),
            }
        }
        winners = merge_winners(winners, runs);
    }

    // Decode values only for the ops that actually won: everything narrowed
    // away or shadowed by a shallower layer cost a position, not a decode.
    winners
        .into_iter()
        .map(|winner| {
            let (node, descended) = &path[winner.level];
            let at = descended.ok_or_else(|| {
                DialogSearchTreeError::Node("pending winner on an undescended layer".into())
            })?;
            let buffer = node.as_index()?.buffer_for(at).ok_or_else(|| {
                DialogSearchTreeError::Node("pending winner on a bufferless link".into())
            })?;
            let op = buffer.op_with_slot(winner.at, winner.slot)?;
            Ok((winner.key, op))
        })
        .collect()
}

/// The winning buffered op for `key` along a root-to-leaf search path, or
/// `None` when no ancestor has one pending.
///
/// A write lands in a node's buffer and only reaches a leaf when that buffer
/// overflows, so a read that consults the leaf alone misses every recent write
/// to that key. Novelty rides the child links, and an op routes to exactly the
/// link the search descends (routing and enqueue share one rule), so only the
/// descended link's buffer is consulted at each layer. Within one buffer the
/// last op for a key wins (matching how a flush replays it); across the path
/// the FIRST layer holding the key wins, because ops flow root to leaf and
/// deeper therefore means older.
pub fn pending_for_key<Key, Value>(
    path: &[TreeLayer<Key, Value>],
    key: &[u8],
) -> Result<Option<NoveltyOp<Value>>, DialogSearchTreeError>
where
    Key: self::Key + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
{
    for layer in path {
        let ArchivedNodeBody::Index(index) = layer.host.body() else {
            continue;
        };
        let Some(buffer) = index.buffer_for(layer.index) else {
            continue;
        };
        // One streaming pass per buffer: the walk stops at the first key past
        // the probe, the winner's value-table slot is tracked as the polarity
        // column is walked, and only a winning op's value is decoded.
        if let Some(op) = buffer.resolve::<Key>(key)? {
            // The path is root first, so this is the shallowest layer holding
            // the key: its op is the newest, and any deeper hit is an older
            // copy a flush pushed down before this one was buffered.
            return Ok(Some(op));
        }
    }
    Ok(None)
}

/// A traversal mechanism for walking through a tree structure.
pub struct TreeWalker<Key, Value>
where
    Key: self::Key,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + ConditionalSync,
{
    root: Blake3Hash,

    key: PhantomData<Key>,
    value: PhantomData<Value>,
}

impl<Key, Value> TreeWalker<Key, Value>
where
    Key: self::Key + ConditionalSync + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
{
    /// Creates a new [`TreeWalker`] with the given root hash and node fetcher.
    pub fn new(root: Blake3Hash) -> Self {
        Self {
            root,

            key: PhantomData,
            value: PhantomData,
        }
    }

    /// Returns a stream of entries within the specified key range.
    pub fn stream<R, Backend>(
        self,
        range: R,
        accessor: Accessor<Backend>,
    ) -> impl Stream<Item = Result<Entry<Key, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        R: RangeBounds<Key> + ConditionalSend,
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        // A thin adapter, not another generator: wrapping the walk in a
        // second `try_stream!` layer measurably bloats every future that
        // embeds a walk (clippy's `large_futures` catches it downstream).
        futures_util::TryStreamExt::map_ok(
            self.stream_scan::<R, Backend, TypedKey<Key>>(range, accessor),
            |entry| Entry {
                key: entry.key.0,
                value: entry.value,
            },
        )
    }

    /// [`stream`](Self::stream), yielding each entry's key as a
    /// [`KeyHandle`] instead of the typed [`Key`]: a warm leaf's entries
    /// borrow the memoized decoded-keys arena with NO per-entry copy, and
    /// only novelty ops and cold streaming decodes copy. For consumers that
    /// work on the raw key bytes.
    pub fn stream_handles<R, Backend>(
        self,
        range: R,
        accessor: Accessor<Backend>,
    ) -> impl Stream<Item = Result<Entry<KeyHandle, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        R: RangeBounds<Key> + ConditionalSend,
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        self.stream_scan::<R, Backend, KeyHandle>(range, accessor)
    }

    /// The walk shared by [`stream`](Self::stream) and
    /// [`stream_handles`](Self::stream_handles); `Out` decides how yielded
    /// keys materialize (see [`ScanKey`]).
    fn stream_scan<R, Backend, Out>(
        self,
        range: R,
        accessor: Accessor<Backend>,
    ) -> impl Stream<Item = Result<Entry<Out, Value>, DialogSearchTreeError>> + ConditionalSend
    where
        R: RangeBounds<Key> + ConditionalSend,
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
        Out: ScanKey + 'static,
    {
        try_stream! {
            // Get the start key. Included/Excluded ranges are identical here,
            // the check if key is in range is below, and this will at most read
            // one unnecessary segment iff `Bound::Excluded(K)` and `K` is a
            // boundary node. An unbounded start begins at the leftmost leaf,
            // which searching for the minimum key descends to.
            let start_key = match range.start_bound() {
                Bound::Included(start) => start.clone(),
                Bound::Excluded(start) => start.clone(),
                Bound::Unbounded => <Key as self::Key>::min(),
            };
            let Some(search_result) = self
                .search(&start_key, accessor.clone(), SearchOptions::default())
                .await?
            else {
                return;
            };
            let mut search_path = search_result.into_indexed();
            let mut entered_range = false;
            let mut warming = FuturesUnordered::new();

            while let Some((node, maybe_index)) = search_path.pop() {
                let body = node.body();
                let is_segment = matches!(body, ArchivedNodeBody::Segment(_));
                if !is_segment {
                    let ArchivedNodeBody::Index(index) = body else {
                        unreachable!("checked above")
                    };
                    let child_index = if let Some(index) = maybe_index {
                        index + 1
                    } else {
                        0
                    };

                    if child_index < index.len() {
                        // The scan will walk this node's remaining children in
                        // turn, so start reading them now and let them land in
                        // the cache while the walk descends into the first of
                        // them.
                        for sibling in (child_index + 1)..index.len() {
                            if warming.len() >= PREFETCH_CONCURRENCY {
                                break;
                            }
                            warming.push(accessor.warm(index.hash_at(sibling)?.clone()));
                        }

                        // The one read that may join an in-flight warm:
                        // `while_warming` keeps polling the warms while
                        // this read waits, so a joined warm always publishes.
                        let next_node = while_warming(
                            accessor.get_node_joining(index.hash_at(child_index)?),
                            &mut warming,
                        )
                        .await?;
                        search_path.push((node, Some(child_index)));
                        search_path.push((next_node, None));
                    } else {
                        // Parent needs to check next sibling
                        continue;
                    }
                    continue;
                }

                // Ops buffered on the ancestors of this leaf are part of the
                // tree's content: a write lands in a node's buffer and only
                // reaches a leaf when that buffer overflows, so a walk that
                // reads segments alone misses every recent write. Merge the
                // covering ops over the stored entries, exactly as a flush
                // would resolve them — restricted to the walk's own range,
                // since an out-of-range op can never yield (`Key`'s order
                // agrees with its bytes, so bounds compare through `as_ref`).
                let start_bytes = match range.start_bound() {
                    Bound::Included(bound) => Bound::Included(bound.as_ref()),
                    Bound::Excluded(bound) => Bound::Excluded(bound.as_ref()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let end_bytes = match range.end_bound() {
                    Bound::Included(bound) => Bound::Included(bound.as_ref()),
                    Bound::Excluded(bound) => Bound::Excluded(bound.as_ref()),
                    Bound::Unbounded => Bound::Unbounded,
                };
                let pending = pending_for_leaf::<Key, Value>(&search_path, start_bytes, end_bytes)?;
                let mut buffered = pending.into_iter().peekable();

                // A leaf re-touched across selects (a join re-selects the same
                // branch once per outer binding, landing on the same leaves)
                // reuses a decode memoized on the node buffer; a leaf touched
                // once (a single range scan) streams its keys without paying to
                // materialize a cache it would never reuse. `should_memoize_keys`
                // returns `false` on the first touch, `true` from the second on.
                //
                // Both arms resolve buffered ops identically; only how the
                // stored keys are obtained differs.
                if node.should_memoize_keys() {
                    let keys = node.memoized_keys()?;
                    // The memoized decode has random access, so enter the leaf
                    // at the range's partition point instead of visiting every
                    // entry before it — the difference between O(leaf) and
                    // O(log leaf + hits) per point-shaped read. Buffered ops
                    // are already range-restricted, so none sort below the
                    // entry point's range.
                    let start_at = match &start_bytes {
                        Bound::Included(bound) | Bound::Excluded(bound) => {
                            keys.lower_bound(bound)
                        }
                        Bound::Unbounded => 0,
                    };
                    // Resolve the segment at most once per leaf, and only when
                    // an entry actually yields: `body()` is a full bytecheck
                    // validation of the node buffer, so resolving per yielded
                    // entry costs O(entries × node size) on the memoized
                    // (join) hot path, while resolving eagerly taxes leaves
                    // the range never enters.
                    let mut segment = None;
                    for at in start_at..keys.len() {
                        let key = keys.get(at).expect("index in range");
                        // Buffered inserts sorting before this entry.
                        while let Some((buffered_key, _)) = buffered.peek() {
                            if buffered_key.as_slice() >= key {
                                break;
                            }
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                entered_range = true;
                                let entry_key = Out::from_entry_bytes(&buffered_key)?;
                                yield Entry { key: entry_key, value };
                            }
                        }

                        // A covering op supersedes the stored entry.
                        if matches!(buffered.peek(), Some((buffered_key, _)) if buffered_key.as_slice() == key) {
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                entered_range = true;
                                let entry_key = Out::from_entry_bytes(&buffered_key)?;
                                yield Entry { key: entry_key, value };
                            }
                            continue;
                        }

                        // Byte-level range check: `Key`'s order agrees with
                        // its bytes, so the typed key is only materialized
                        // for entries that actually yield.
                        if !below_start(&start_bytes, key) && !past_end_bytes(&end_bytes, key) {
                            entered_range = true;
                            let segment = match &segment {
                                Some(segment) => segment,
                                None => {
                                    let ArchivedNodeBody::Segment(resolved) = node.body() else {
                                        unreachable!("segment checked above")
                                    };
                                    segment.insert(resolved)
                                }
                            };
                            let value = into_owned(segment.value_at(at)?)?;
                            // The memoized arena outlives the yield, so a
                            // `KeyHandle` consumer borrows it copy-free; the
                            // typed consumer copies out, as before.
                            let entry_key = Out::from_arena(&keys, at)?;
                            yield Entry { key: entry_key, value };
                        // Entries only ascend, so a key past the range's end
                        // ends the walk. The `past_end_bytes` half must NOT be
                        // gated on `entered_range`: a scan whose range hits no
                        // stored entry would otherwise never exit and would
                        // walk the rest of the tree, making an empty lookup
                        // cost the size of the database.
                        } else if entered_range || past_end_bytes(&end_bytes, key) {
                            return;
                        }
                    }
                } else {
                    let ArchivedNodeBody::Segment(segment) = node.body() else {
                        unreachable!("segment checked above")
                    };
                    let mut keys = segment.keys::<Key>()?;
                    while let Some((at, key)) = keys.next_key()? {
                        // Buffered inserts sorting before this entry.
                        while let Some((buffered_key, _)) = buffered.peek() {
                            if buffered_key.as_slice() >= key {
                                break;
                            }
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                entered_range = true;
                                let entry_key = Out::from_entry_bytes(&buffered_key)?;
                                yield Entry { key: entry_key, value };
                            }
                        }

                        // A covering op supersedes the stored entry.
                        if matches!(buffered.peek(), Some((buffered_key, _)) if buffered_key.as_slice() == key) {
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                entered_range = true;
                                let entry_key = Out::from_entry_bytes(&buffered_key)?;
                                yield Entry { key: entry_key, value };
                            }
                            continue;
                        }

                        // Byte-level range check, as in the memoized arm.
                        if !below_start(&start_bytes, key) && !past_end_bytes(&end_bytes, key) {
                            entered_range = true;
                            let value = into_owned(segment.value_at(at)?)?;
                            let entry_key = Out::from_entry_bytes(key)?;
                            yield Entry { key: entry_key, value };
                        // See the memoized arm above: the `past_end_bytes`
                        // half must not be gated on `entered_range`, or a
                        // range matching no stored entry walks the rest of
                        // the tree.
                        } else if entered_range || past_end_bytes(&end_bytes, key) {
                            return;
                        }
                    }
                }

                // Buffered inserts past the last stored entry of this leaf.
                // Already range-restricted by `pending_for_leaf`, so every
                // assert yields.
                for (buffered_key, op) in buffered {
                    if let NoveltyOp::Assert(value) = op {
                        entered_range = true;
                        let entry_key = Out::from_entry_bytes(&buffered_key)?;
                        yield Entry { key: entry_key, value };
                    }
                }
            }
        }
    }

    /// Searches for the leaf segment that would contain the given key.
    pub async fn search<Backend>(
        &self,
        key: &Key,
        accessor: Accessor<Backend>,
        options: SearchOptions,
    ) -> Result<Option<SearchResult<Key, Value>>, DialogSearchTreeError>
    where
        Backend: StorageBackend<Key = Blake3Hash, Value = Vec<u8>, Error = DialogStorageError>
            + ConditionalSync,
    {
        if &self.root == NULL_BLAKE3_HASH {
            return Ok(None);
        }

        // Depth scales logarithmically with number of entries, so 32 is truly
        // overkill here
        const MAXIMUM_TREE_DEPTH: usize = 32;

        let mut next_node = self.root.clone();
        let mut path = vec![];

        loop {
            if path.len() > MAXIMUM_TREE_DEPTH {
                return Err(DialogSearchTreeError::Operation(format!(
                    "Tree depth exceded the soft maximum ({MAXIMUM_TREE_DEPTH})"
                )));
            }

            let node = accessor.get_node(&next_node).await?;

            match node.body() {
                ArchivedNodeBody::Index(index) => {
                    // Descend into the last child whose separator is at or
                    // below the key (a probe equal to a separator belongs to
                    // the seam's right side), clamping to the leftmost child
                    // when the key sits below every separator.
                    let child_index = index.route(key.as_ref())?;

                    next_node = index.hash_at(child_index)?.clone();

                    path.push(TreeLayer {
                        host: node.clone(),
                        index: child_index,
                    });
                }
                ArchivedNodeBody::Segment(_) => {
                    let right_neighbor = if options.prefetch_right_neighbor {
                        prefetch_right_neighbor(key, &node, &path, accessor).await?
                    } else {
                        None
                    };
                    return Ok(Some(SearchResult {
                        leaf: node,
                        path,
                        right_neighbor,
                    }));
                }
            }
        }
    }
}

/// Polls `read` to completion, making progress on queued cache-warming reads
/// whenever it is not ready.
///
/// The queued reads only populate the cache; nothing waits on them and their
/// outcomes are discarded, so this changes neither what `read` resolves to nor
/// when its caller observes it. Reads still queued when the caller is dropped
/// are dropped with it.
async fn while_warming<Read, Warm>(read: Read, warming: &mut FuturesUnordered<Warm>) -> Read::Output
where
    Read: Future,
    Warm: Future<Output = ()>,
{
    let mut read = std::pin::pin!(read);

    std::future::poll_fn(move |context| {
        if let Poll::Ready(output) = read.as_mut().poll(context) {
            return Poll::Ready(output);
        }

        while let Poll::Ready(Some(())) = warming.poll_next_unpin(context) {}

        Poll::Pending
    })
    .await
}

/// Walks the narrow "overflow" path for [`RightNeighbor`] prefetching.
///
/// Called when [`TreeWalker::search`] lands on a leaf whose last entry matches
/// the searched key — a necessary condition for boundary-delete overflow. If
/// the search path contains any layer with a right sibling, we follow the
/// leftmost descent from the first such sibling down to the next leaf. This
/// lets a boundary delete fold orphan entries into that leaf in one
/// pass when the deleted entry turns out to be the segment boundary.
///
/// Returns `None` when either the key is not the leaf's last entry or the leaf
/// has no right-adjacent neighbor (the leaf is the rightmost segment in the
/// tree).
async fn prefetch_right_neighbor<Key, Value, Backend>(
    key: &Key,
    leaf: &PersistentNode<Key, Value>,
    path: &[TreeLayer<Key, Value>],
    accessor: Accessor<Backend>,
) -> Result<Option<RightNeighbor<Key, Value>>, DialogSearchTreeError>
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
    // Only prefetch when the caller's key matches the leaf's last entry;
    // boundary-delete overflow can't happen otherwise.
    let Some(leaf_upper_bound) = leaf.upper_bound()? else {
        return Ok(None);
    };
    if key.as_ref() != leaf_upper_bound.as_slice() {
        return Ok(None);
    }

    // Find the deepest ancestor with a right sibling: that's the lowest common
    // ancestor of the main descent and the right-adjacent descent.
    let Some(lca_depth) = path.iter().rposition(|layer| layer.has_right_siblings()) else {
        // The leaf is the rightmost segment in the tree; nothing to prefetch.
        return Ok(None);
    };

    // The right-descent starts at the LCA's first right sibling (the child just
    // past the one the main descent took).
    let lca = &path[lca_depth];
    let mut next_hash: Blake3Hash = lca.host.as_index()?.hash_at(lca.index + 1)?.clone();
    let mut diverged_path: Vec<TreeLayer<Key, Value>> = Vec::new();

    let right_leaf = loop {
        let node: PersistentNode<Key, Value> = accessor.get_node(&next_hash).await?;
        match node.body() {
            ArchivedNodeBody::Index(index) => {
                if index.is_empty() {
                    return Err(DialogSearchTreeError::Node(
                        "Empty index node during right-neighbor descent".into(),
                    ));
                }
                // The right-adjacent descent is leftmost, so it always takes
                // child 0; the remaining children are its right siblings.
                let child_hash = index.hash_at(0)?.clone();
                diverged_path.push(TreeLayer {
                    host: node.clone(),
                    index: 0,
                });
                next_hash = child_hash;
            }
            ArchivedNodeBody::Segment(_) => break node,
        }
    };

    Ok(Some(RightNeighbor {
        lca_depth,
        diverged_path,
        leaf: right_leaf,
    }))
}

/// Options controlling the behavior of [`TreeWalker::search`].
///
/// `prefetch_right_neighbor` is only consumed by a boundary delete to
/// resolve boundary-delete overflow. All other call sites (reads, inserts,
/// range streams) should leave it at its default of `false` to avoid the extra
/// leftmost descent that the prefetch can trigger.
#[derive(Debug, Default, Clone, Copy)]
pub struct SearchOptions {
    /// When `true`, [`TreeWalker::search`] will additionally descend to the
    /// leaf immediately right-adjacent to the found leaf when the searched key
    /// matches that leaf's last entry, populating
    /// [`SearchResult::right_neighbor`].
    pub prefetch_right_neighbor: bool,
}

/// A layer in the tree traversal path: the index node descended through and the
/// position of the child the descent took.
///
/// [`TreeWalker::search`] assembles a path of these as the copy-on-write
/// frontier for an update: each layer names a node an update rebuilds and the
/// child slot within it that changes. A layer is cheap to hold: `host` is an
/// [`Arc`]-backed [`Node`] that shares its buffer when cloned, and `index` is a
/// `usize`. The host's other children stay encoded in its buffer; a read leaves
/// them there, and a write decodes the ones it needs on demand through
/// [`left_siblings`](Self::left_siblings) /
/// [`right_siblings`](Self::right_siblings) when it rebuilds the level.
///
/// [`Arc`]: std::sync::Arc
/// [`Node`]: crate::Node
pub struct TreeLayer<Key, Value> {
    /// The index node at this layer of the tree.
    pub host: PersistentNode<Key, Value>,
    /// Position within the host's children of the child the descent followed.
    pub index: usize,
}

impl<Key, Value> TreeLayer<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Whether the descended child has any sibling to its left. Cheap: a length
    /// comparison, no decoding.
    pub fn has_left_siblings(&self) -> bool {
        self.index > 0
    }

    /// Whether the descended child has any sibling to its right. Cheap: a length
    /// comparison, no decoding.
    pub fn has_right_siblings(&self) -> bool {
        self.host
            .as_index()
            .map(|index| self.index + 1 < index.len())
            .unwrap_or(false)
    }

    /// The host's children strictly to the left of the descended child, decoded
    /// to owned links. Materialized on demand: only an update that rebuilds this
    /// level calls it.
    pub fn left_siblings(&self) -> Result<Option<NonEmpty<Link>>, DialogSearchTreeError> {
        self.siblings(0, self.index)
    }

    /// The host's children strictly to the right of the descended child, decoded
    /// to owned links. Materialized on demand: only an update that rebuilds this
    /// level calls it.
    pub fn right_siblings(&self) -> Result<Option<NonEmpty<Link>>, DialogSearchTreeError> {
        let links = self.host.as_index()?.len();
        self.siblings(self.index + 1, links)
    }

    fn siblings(
        &self,
        start: usize,
        end: usize,
    ) -> Result<Option<NonEmpty<Link>>, DialogSearchTreeError> {
        let index = self.host.as_index()?;
        let owned = (start..end)
            .map(|at| index.link_at(at))
            .collect::<Result<Vec<Link>, _>>()?;
        Ok(NonEmpty::from_vec(owned))
    }
}

/// The path taken from the root to a leaf during a tree search.
pub type SearchPath<Key, Value> = Vec<TreeLayer<Key, Value>>;

/// An indexed path with nodes and their child indices.
pub type IndexedPath<Key, Value> = Vec<(PersistentNode<Key, Value>, Option<usize>)>;

/// The result of a tree search, containing the leaf node and the path taken to
/// reach it.
pub struct SearchResult<Key, Value> {
    /// The leaf node found by the search.
    pub leaf: PersistentNode<Key, Value>,
    /// The path from root to leaf.
    pub path: SearchPath<Key, Value>,
    /// Prefetched right-adjacent segment, populated when the searched key
    /// matched the leaf's last entry and a right neighbor exists. Used by
    /// a boundary delete to resolve boundary-delete overflow in one pass.
    pub right_neighbor: Option<RightNeighbor<Key, Value>>,
}

/// Prefetched information about the leaf segment immediately to the right of a
/// [`SearchResult::leaf`].
///
/// This is populated by [`TreeWalker::search`] only when the search key lands
/// on the main leaf's last entry (a boundary-delete candidate) and a
/// right-adjacent leaf exists. Its shape captures where the right-adjacent
/// descent diverges from the main descent so a boundary delete can rebuild
/// both subtrees and stitch them together at the lowest common ancestor.
///
/// For the common "same-parent" overflow case (the right-adjacent leaf shares
/// a parent with the main leaf), `lca_depth == SearchResult.path.len() - 1`
/// and `diverged_path` is empty. For cross-parent overflow, `lca_depth` points
/// deeper in the shared ancestor chain and `diverged_path` records the
/// leftmost descent from there down to `leaf`'s parent.
pub struct RightNeighbor<Key, Value> {
    /// Depth in the main search path at which the right-adjacent descent
    /// diverges. Main and right-adjacent descents share hosts at depths
    /// `0..=lca_depth` (this depth's host is the same node in both
    /// descents, but they descend to different children).
    pub lca_depth: usize,
    /// Tree layers traversed during the leftmost descent from the first right
    /// sibling at `lca_depth` down to `leaf`'s parent. Empty when the main
    /// leaf and the right-adjacent leaf share a parent.
    pub diverged_path: Vec<TreeLayer<Key, Value>>,
    /// The right-adjacent leaf segment.
    pub leaf: PersistentNode<Key, Value>,
}

impl<Key, Value> SearchResult<Key, Value> {
    /// Converts this search result into a root-to-leaf path of
    /// `(node, child index)` pairs, where the leaf carries `None` and each index
    /// node carries the slot of the child the search descended into.
    pub fn into_indexed(mut self) -> IndexedPath<Key, Value> {
        let mut path = Vec::new();
        path.push((self.leaf, None));

        while let Some(layer) = self.path.pop() {
            path.push((layer.host, Some(layer.index)));
        }

        path.reverse();
        path
    }
}

#[cfg(test)]
mod walker_novelty_tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use dialog_storage::MemoryStorageBackend;
    use futures_util::StreamExt as _;

    use crate::{Buffer, ContentAddressedStorage, Delta, HitchhikerTree, PersistentTree};

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

    /// Successive buffered writes must all survive: a commit buffers, the next
    /// commit opens over the *published* root and buffers again, and every
    /// earlier write must still be readable. This is the shape the repository
    /// commit path produces.
    #[dialog_common::test]
    async fn it_accumulates_across_successive_buffered_writes() -> Result<()> {
        let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

        let mut tree = Tree::empty();
        let mut expected: Vec<([u8; 4], Vec<u8>)> = Vec::new();

        // 50 successive "commits", each buffering one write over the last
        // published root, exactly as the commit path now does.
        for i in 0..50u32 {
            let key = (i * 37 % 500).to_be_bytes();
            let value = vec![i as u8];

            let buffered = HitchhikerTree::open(&tree)
                .with_op_buf_size(8)
                .insert(key, value.clone(), &storage)
                .await?;
            let mut delta = Delta::zero();
            let root = buffered.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            tree = Tree::from_hash_with_cache(root, Default::default());

            expected.retain(|(k, _)| *k != key);
            expected.push((key, value));

            // Every write so far must be readable, by scan and by point read.
            expected.sort_by_key(|(a, _)| *a);
            let mut seen = Vec::new();
            {
                let stream = tree.stream_range(.., &storage);
                futures_util::pin_mut!(stream);
                while let Some(entry) = stream.next().await {
                    let entry = entry?;
                    seen.push((entry.key, entry.value));
                }
            }
            assert_eq!(
                seen,
                expected,
                "after {} commits the scan must see every write",
                i + 1
            );

            for (key, value) in &expected {
                assert_eq!(
                    tree.get(key, &storage).await?.as_ref(),
                    Some(value),
                    "after {} commits the point read must see key {key:?}",
                    i + 1
                );
            }
        }
        Ok(())
    }

    /// Several writes in ONE buffered batch, repeated across batches. The
    /// artifact layer writes 3+ keys per fact (EAV/AEV/VAE orderings) plus
    /// history records, so a commit buffers many keys at once and the next
    /// commit buffers many more over it.
    #[dialog_common::test]
    async fn it_accumulates_multi_key_buffered_batches() -> Result<()> {
        for seed in 0..20u64 {
            let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());
            let mut tree = Tree::empty();
            let mut expected: std::collections::BTreeMap<[u8; 4], Vec<u8>> = Default::default();

            for batch in 0..20u32 {
                // Scattered keys, like content-hashed artifact keys.
                let keys: Vec<u32> = (0..6).map(|_| next() % 100_000).collect();

                let mut buffered = HitchhikerTree::open(&tree).with_op_buf_size(8);
                for key in &keys {
                    let value = vec![batch as u8];
                    buffered = buffered
                        .insert(key.to_be_bytes(), value.clone(), &storage)
                        .await?;
                    expected.insert(key.to_be_bytes(), value);
                }
                let mut delta = Delta::zero();
                let root = buffered.persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
                tree = Tree::from_hash_with_cache(root, Default::default());

                let mut seen = Vec::new();
                {
                    let stream = tree.stream_range(.., &storage);
                    futures_util::pin_mut!(stream);
                    while let Some(entry) = stream.next().await {
                        let entry = entry?;
                        seen.push((entry.key, entry.value));
                    }
                }
                let want: Vec<_> = expected.iter().map(|(k, v)| (*k, v.clone())).collect();
                assert_eq!(
                    seen, want,
                    "seed {seed}, batch {batch}: scan must see every buffered write"
                );
            }
        }
        Ok(())
    }

    /// A buffered range scan must return exactly what the canonical tree
    /// returns, for every sub-range, across many random key layouts.
    ///
    /// The walker merges ops from the ancestors on its search path, scoped to
    /// the leaf it is sitting on; getting that scoping wrong drops or duplicates
    /// entries only for particular layouts, which is why this sweeps seeds.
    #[dialog_common::test]
    async fn it_scans_buffered_like_canonical_across_layouts() -> Result<()> {
        for seed in 0..40u64 {
            let mut rng = 0x9E3779B97F4A7C15u64 ^ seed;
            let mut next = || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                (rng >> 32) as u32
            };

            let mut storage: Store = ContentAddressedStorage::new(MemoryStorageBackend::default());

            // Random base, random keys (big-endian so byte order is key order).
            let base_keys: Vec<u32> = (0..300).map(|_| next() % 4000).collect();
            let mut base = Tree::empty();
            let mut delta = Delta::zero();
            for key in &base_keys {
                base = base
                    .edit()
                    .insert(key.to_be_bytes(), key.to_be_bytes().to_vec(), &storage)
                    .await?
                    .persist(&mut delta)?;
                settle(&mut delta, &mut storage).await?;
            }

            // Random ops, small buffer so they cascade across levels.
            let ops: Vec<(bool, u32)> = (0..60)
                .map(|_| (!next().is_multiple_of(3), next() % 4000))
                .collect();

            let mut buffered = HitchhikerTree::open(&base).with_op_buf_size(8);
            let mut canonical = HitchhikerTree::open(&base).with_op_buf_size(8);
            for (insert, key) in &ops {
                if *insert {
                    buffered = buffered
                        .insert(key.to_be_bytes(), vec![7], &storage)
                        .await?;
                    canonical = canonical
                        .insert(key.to_be_bytes(), vec![7], &storage)
                        .await?;
                } else {
                    buffered = buffered.delete(key.to_be_bytes(), &storage).await?;
                    canonical = canonical.delete(key.to_be_bytes(), &storage).await?;
                }
            }

            let mut delta = Delta::zero();
            let buffered_root = buffered.persist(&mut delta)?;
            settle(&mut delta, &mut storage).await?;
            let buffered_tree = Tree::from_hash_with_cache(buffered_root, Default::default());

            let mut delta = Delta::zero();
            let canonical_tree = canonical.canonicalize(&storage, &mut delta).await?;
            settle(&mut delta, &mut storage).await?;

            for (low, high) in [
                (0u32, 4000u32),
                (0, 100),
                (500, 1500),
                (3000, 4000),
                (77, 78),
            ] {
                let range = low.to_be_bytes()..=high.to_be_bytes();

                let mut from_buffered = Vec::new();
                {
                    let stream = buffered_tree.stream_range(range.clone(), &storage);
                    futures_util::pin_mut!(stream);
                    while let Some(entry) = stream.next().await {
                        let entry = entry?;
                        from_buffered.push((entry.key, entry.value));
                    }
                }

                let mut from_canonical = Vec::new();
                {
                    let stream = canonical_tree.stream_range(range, &storage);
                    futures_util::pin_mut!(stream);
                    while let Some(entry) = stream.next().await {
                        let entry = entry?;
                        from_canonical.push((entry.key, entry.value));
                    }
                }

                assert_eq!(
                    from_buffered, from_canonical,
                    "seed {seed}: buffered scan of [{low}, {high}] must match canonical"
                );
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod prefetch_tests {
    #![allow(unexpected_cfgs)]

    use std::collections::HashSet;

    use anyhow::Result;
    use dialog_common::Blake3Hash;
    use futures_util::TryStreamExt as _;

    use crate::{
        ArchivedNodeBody, Buffer, ContentAddressedStorage, Delta, PersistentNode, PersistentTree,
        helpers::ObservingBackend,
    };

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    type Tree = PersistentTree<[u8; 4], Vec<u8>>;
    type Storage = ContentAddressedStorage<ObservingBackend>;

    /// Enough entries that the leaf-seam coin (one seam expected every
    /// `2^fanout_n = 256` keys) reliably cuts the run into many sibling
    /// leaves, the shape prefetch exists for. Values do not count toward
    /// segment weight in the datum-as-key format, so fanning the tree out
    /// takes keys, not bulk.
    const ENTRIES: u32 = 8192;

    fn value_of(entry: u32) -> Vec<u8> {
        entry.to_be_bytes().to_vec()
    }

    /// Builds a tree of [`ENTRIES`] entries and hands back a handle to it whose
    /// node cache is cold, so that reads reach the backend.
    async fn built_tree(storage: &mut Storage) -> Result<Tree> {
        let mut delta = Delta::zero();
        let mut edit = Tree::empty().edit();

        for entry in 0..ENTRIES {
            edit = edit
                .insert(entry.to_be_bytes(), value_of(entry), storage)
                .await?;
        }

        let tree = edit.persist(&mut delta)?;
        for (_, buffer) in delta.flush() {
            storage
                .store(buffer.as_ref().to_vec(), buffer.blake3_hash())
                .await?;
        }

        Ok(Tree::from_hash(tree.root().clone()))
    }

    async fn load(
        storage: &Storage,
        hash: &Blake3Hash,
    ) -> Result<PersistentNode<[u8; 4], Vec<u8>>> {
        let bytes = storage
            .retrieve(hash)
            .await?
            .ok_or_else(|| anyhow::anyhow!("Node not stored"))?;

        Ok(PersistentNode::try_from(Buffer::from(bytes))?)
    }

    /// A reader that needs a node a read-ahead has claimed must not depend
    /// on that read-ahead being driven. The read-ahead here is polled
    /// exactly once — enough to claim the node and start its read — and
    /// then never again, the shape of a walk parked between two yields. A
    /// point read of a key under that node used to join the claim and wait
    /// for an outcome only further polling of the read-ahead could produce.
    // Native only: the bound is tokio's timer, which has no wasm runtime.
    #[cfg(not(target_arch = "wasm32"))]
    #[dialog_common::test]
    async fn it_serves_a_claimed_node_to_a_reader_the_read_ahead_cannot_reach() -> Result<()> {
        use std::task::{Context, Poll};

        let mut storage = ContentAddressedStorage::new(ObservingBackend::new());
        let tree = built_tree(&mut storage).await?;
        let accessor = crate::Accessor::new(tree.node_cache(), storage.clone());

        // The root's second child, and a key from its leftmost leaf.
        let root = load(&storage, tree.root()).await?;
        let ArchivedNodeBody::Index(index) = root.body() else {
            anyhow::bail!("the built tree has a single leaf; nothing to warm")
        };
        let sibling = index.hash_at(1)?.clone();
        let mut hash = sibling.clone();
        let key: [u8; 4] = loop {
            let node = load(&storage, &hash).await?;
            match node.body() {
                ArchivedNodeBody::Index(index) => hash = index.hash_at(0)?.clone(),
                ArchivedNodeBody::Segment(segment) => {
                    break segment.first_key::<[u8; 4]>()?.as_slice().try_into()?;
                }
            }
        };

        let mut warm = Box::pin(accessor.warm(sibling));
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(warm.as_mut().poll(&mut context), Poll::Pending));

        let read = tree.get(&key, &storage);
        let value = tokio::time::timeout(std::time::Duration::from_secs(2), read)
            .await
            .expect("a reader must not wait on a read-ahead nobody drives")?;
        assert_eq!(value, Some(value_of(u32::from_be_bytes(key))));
        drop(warm);
        Ok(())
    }

    #[dialog_common::test]
    async fn it_warms_sibling_nodes_during_a_range_scan() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(ObservingBackend::new());
        let tree = built_tree(&mut storage).await?;
        let backend = storage.backend().clone();
        backend.reset();

        let entries: Vec<_> = tree.stream(&storage).try_collect().await?;

        assert_eq!(entries.len() as u32, ENTRIES);
        assert!(
            entries.windows(2).all(|pair| pair[0].key < pair[1].key),
            "the scan yields entries in key order"
        );

        let reads = backend.read_log();
        let distinct = reads.iter().collect::<HashSet<_>>();

        assert!(reads.len() > 1, "the scan reads more than the root");
        assert_eq!(
            reads.len(),
            distinct.len(),
            "no node was read from the backend twice"
        );
        assert!(
            backend.peak_reads_in_flight() > 1,
            "sibling reads overlap the read the scan is waiting on"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_does_not_prefetch_on_point_lookups() -> Result<()> {
        let mut storage = ContentAddressedStorage::new(ObservingBackend::new());
        let tree = built_tree(&mut storage).await?;
        let backend = storage.backend().clone();
        backend.reset();

        let found = tree.get(&257u32.to_be_bytes(), &storage).await?;

        assert_eq!(found, Some(value_of(257)));

        let path = backend.read_log();
        assert!(path.len() > 1, "the tree has at least one index level");
        assert_eq!(path.first(), Some(tree.root()));
        assert_eq!(backend.peak_reads_in_flight(), 1);

        // Every read but the last is an index node holding the read that
        // follows it: the lookup descended a single root-to-leaf path and
        // read nothing beside it.
        for step in path.windows(2) {
            let node = load(&storage, &step[0]).await?;
            match node.body() {
                ArchivedNodeBody::Index(index) => assert!(
                    index.contains_hash(&step[1]),
                    "a read that is not a child of the read before it"
                ),
                ArchivedNodeBody::Segment(_) => panic!("a segment cannot hold a further read"),
            }
        }

        let leaf = load(&storage, path.last().expect("a read")).await?;
        assert!(matches!(leaf.body(), ArchivedNodeBody::Segment(_)));

        Ok(())
    }
}
