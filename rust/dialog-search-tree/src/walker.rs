use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

use async_stream::try_stream;
use dialog_common::{Blake3Hash, ConditionalSend, ConditionalSync, NULL_BLAKE3_HASH};
use dialog_storage::{DialogStorageError, StorageBackend};
use futures_core::Stream;
use nonempty::NonEmpty;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Accessor, ArchivedNodeBody, DialogSearchTreeError, Entry, Key, Link, NoveltyOp, PersistentNode,
    Value, into_owned,
};

/// Whether `key` lies at or above `range`'s upper bound: once one does, no
/// later key (keys stream in order) can be in range, so a walk can stop even
/// if the range was never entered. Without this, a range lying entirely below
/// a subtree's keys would walk every leaf to the subtree's right edge
/// yielding nothing.
fn past_end<Key: Ord, R: RangeBounds<Key>>(range: &R, key: &Key) -> bool {
    match range.end_bound() {
        Bound::Included(end) => key > end,
        Bound::Excluded(end) => key >= end,
        Bound::Unbounded => false,
    }
}

/// The buffered ops covering the leaf a walk currently sits on, resolved to one
/// winning op per key and sorted by key.
///
/// `path` is the walk's ancestor stack, root first, each entry paired with the
/// index of the child it descended into. That gives both the ops (each index
/// node's `novelty`) and the leaf's span: the child link's upper bound, and its
/// predecessor's as the exclusive lower bound. Ops outside the span belong to
/// sibling leaves and are skipped, so each op is resolved at exactly the one
/// leaf whose range covers it, the same leaf a flush would route it to.
///
/// The rightmost leaf of the rightmost path is open-ended: an op sorting past
/// every key belongs to it, matching the flush rule that the last child takes
/// whatever remains.
///
/// Precedence: WITHIN one node's buffer the last entry for a key is the newest
/// and wins; ACROSS the path the first (root-most) layer holding the key wins,
/// because writes land in the root buffer and a flush only moves ops downward,
/// so deeper always means older.
#[allow(clippy::type_complexity)]
fn pending_for_leaf<Key, Value>(
    path: &[(PersistentNode<Key, Value>, Option<usize>)],
) -> Result<Vec<(Vec<u8>, NoveltyOp<Value>)>, DialogSearchTreeError>
where
    Key: self::Key + 'static,
    Value: self::Value + ConditionalSync + 'static,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>
        + ConditionalSync,
{
    // Pass one: establish the leaf's span from the separator table alone,
    // touching no buffered entry. The span is what makes the second pass
    // cheap, so it has to be known before any op is looked at.
    //
    // Separators are lower bounds: child `at` spans `[sep(at), sep(at + 1))`,
    // and the last child of the rightmost path runs open-ended, matching the
    // flush rule that the last child takes whatever remains.
    let mut lower: Option<Vec<u8>> = None;
    let mut upper: Option<Vec<u8>> = None;

    for (node, descended) in path {
        let ArchivedNodeBody::Index(index) = node.body()? else {
            continue;
        };
        let Some(at) = descended else { continue };

        if *at < index.len() {
            let separator = index.separator(*at)?;
            if !separator.is_empty() {
                lower = Some(separator);
            }
            upper = if *at + 1 < index.len() {
                Some(index.separator(*at + 1)?)
            } else {
                // Rightmost child at this level: it inherits whatever bound
                // an ancestor imposed, so leave `upper` as the ancestors set it.
                upper
            };
        }
    }

    // Pass two: collect only the ops that fall in this leaf's span.
    //
    // A buffer holds ops for its whole subtree, so most of them belong to
    // sibling leaves. Deciding that from the archived key skips the
    // deserialization for every op that is not ours, which is the bulk of them.
    //
    // Buffers are sorted by key, so the in-range ops form a contiguous run and
    // the scan can stop at the first key past the span rather than walking the
    // tail.
    let mut winners: Vec<(Vec<u8>, NoveltyOp<Value>)> = Vec::new();
    for (node, descended) in path {
        let ArchivedNodeBody::Index(index) = node.body()? else {
            continue;
        };
        if descended.is_none() {
            continue;
        }

        // Winners recorded before this layer came from shallower nodes and are
        // newer; only entries from THIS layer's buffer may replace each other.
        let layer_start = winners.len();
        for entry in index.novelty.iter() {
            let key: &[u8] = &entry.key;
            // Below the span: skip without decoding.
            if lower.as_ref().is_some_and(|lower| key < lower.as_slice()) {
                continue;
            }
            // At or past the exclusive upper bound: the rest of this sorted
            // buffer is too, so stop.
            if upper.as_ref().is_some_and(|upper| key >= upper.as_slice()) {
                break;
            }

            match winners.iter().position(|(candidate, _)| candidate == key) {
                // Same layer: a later entry in one buffer is the newer op.
                Some(at) if at >= layer_start => winners[at].1 = into_owned(&entry.op)?,
                // A shallower layer already holds the newer op for this key;
                // this deeper (older) one loses without being decoded.
                Some(_) => {}
                None => winners.push((key.to_vec(), into_owned(&entry.op)?)),
            }
        }
    }

    winners.sort_by(|(left, _), (right, _)| left.cmp(right));
    Ok(winners)
}

/// The winning buffered op for `key` along a root-to-leaf search path, or
/// `None` when no ancestor has one pending.
///
/// A write lands in a node's buffer and only reaches a leaf when that buffer
/// overflows, so a read that consults the leaf alone misses every recent write
/// to that key. Within one node's buffer the last op wins (matching how a
/// flush replays it); across the path the FIRST layer holding the key wins,
/// because ops flow root to leaf and deeper therefore means older.
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
        let ArchivedNodeBody::Index(index) = layer.host.body()? else {
            continue;
        };
        // Buffers are sorted by key, so the run of entries for this key is
        // found by binary search rather than by scanning the whole buffer. A
        // node's buffer holds ops for its entire subtree, so scanning it per
        // read is the difference between constant and linear work in the buffer
        // size on every point read.
        let at = index
            .novelty
            .partition_point(|entry| entry.key.as_slice() < key);
        // Within a key the last op wins, and equal keys are contiguous.
        let mut found = None;
        for entry in index.novelty[at..].iter() {
            if entry.key.as_slice() != key {
                break;
            }
            found = Some(entry);
        }
        if let Some(entry) = found {
            // The path is root first, so this is the shallowest layer holding
            // the key: its op is the newest, and any deeper hit is an older
            // copy a flush pushed down before this one was buffered.
            return Ok(Some(into_owned::<NoveltyOp<Value>>(&entry.op)?));
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

            while let Some((node, maybe_index)) = search_path.pop() {
                let body = node.body()?;
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
                        let next_node = accessor.get_node(index.hash_at(child_index)?).await?;
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
                // would resolve them.
                let pending = pending_for_leaf::<Key, Value>(&search_path)?;
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
                    // Resolve the segment at most once per leaf, and only when
                    // an entry actually yields: `body()` is a full bytecheck
                    // validation of the node buffer, so resolving per yielded
                    // entry costs O(entries × node size) on the memoized
                    // (join) hot path, while resolving eagerly taxes leaves
                    // the range never enters.
                    let mut segment = None;
                    for (at, key) in keys.iter().enumerate() {
                        // Buffered inserts sorting before this entry.
                        while let Some((buffered_key, _)) = buffered.peek() {
                            if buffered_key.as_slice() >= key {
                                break;
                            }
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                let entry_key = Key::try_from_bytes(&buffered_key)?;
                                if range.contains(&entry_key) {
                                    entered_range = true;
                                    yield Entry { key: entry_key, value };
                                }
                            }
                        }

                        // A covering op supersedes the stored entry.
                        if matches!(buffered.peek(), Some((buffered_key, _)) if buffered_key.as_slice() == key) {
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                let entry_key = Key::try_from_bytes(&buffered_key)?;
                                if range.contains(&entry_key) {
                                    entered_range = true;
                                    yield Entry { key: entry_key, value };
                                }
                            }
                            continue;
                        }

                        let entry_key = Key::try_from_bytes(key)?;
                        if range.contains(&entry_key) {
                            entered_range = true;
                            let segment = match &segment {
                                Some(segment) => segment,
                                None => {
                                    let ArchivedNodeBody::Segment(resolved) = node.body()? else {
                                        unreachable!("segment checked above")
                                    };
                                    segment.insert(resolved)
                                }
                            };
                            let value = into_owned(segment.value_at(at)?)?;
                            yield Entry { key: entry_key, value };
                        // Entries only ascend, so a key past the range's end
                        // ends the walk. The `past_end` half must NOT be gated
                        // on `entered_range`: a scan whose range hits no stored
                        // entry would otherwise never exit and would walk the
                        // rest of the tree, making an empty lookup cost the
                        // size of the database.
                        } else if entered_range || past_end(&range, &entry_key) {
                            return;
                        }
                    }
                } else {
                    let ArchivedNodeBody::Segment(segment) = node.body()? else {
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
                                let entry_key = Key::try_from_bytes(&buffered_key)?;
                                if range.contains(&entry_key) {
                                    entered_range = true;
                                    yield Entry { key: entry_key, value };
                                }
                            }
                        }

                        // A covering op supersedes the stored entry.
                        if matches!(buffered.peek(), Some((buffered_key, _)) if buffered_key.as_slice() == key) {
                            let (buffered_key, op) = buffered.next().expect("peeked");
                            if let NoveltyOp::Assert(value) = op {
                                let entry_key = Key::try_from_bytes(&buffered_key)?;
                                if range.contains(&entry_key) {
                                    entered_range = true;
                                    yield Entry { key: entry_key, value };
                                }
                            }
                            continue;
                        }

                        let entry_key = Key::try_from_bytes(key)?;
                        if range.contains(&entry_key) {
                            entered_range = true;
                            let value = into_owned(segment.value_at(at)?)?;
                            yield Entry { key: entry_key, value };
                        // See the memoized arm above: the `past_end` half must
                        // not be gated on `entered_range`, or a range matching
                        // no stored entry walks the rest of the tree.
                        } else if entered_range || past_end(&range, &entry_key) {
                            return;
                        }
                    }
                }

                // Buffered inserts past the last stored entry of this leaf.
                for (buffered_key, op) in buffered {
                    if let NoveltyOp::Assert(value) = op {
                        let entry_key = Key::try_from_bytes(&buffered_key)?;
                        if range.contains(&entry_key) {
                            entered_range = true;
                            yield Entry { key: entry_key, value };
                        }
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

            match node.body()? {
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
        match node.body()? {
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
            expected.sort_by(|(a, _), (b, _)| a.cmp(b));
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
