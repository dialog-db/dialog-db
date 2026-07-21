use dialog_common::Blake3Hash;
use rkyv::{
    Deserialize, Serialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::{
    ArchivedIndex, ArchivedNodeBody, Buffer, Delta, DialogSearchTreeError, Distribution, Entry,
    Key, Link, Manifest, Node, NoveltyBuffer, NoveltyEntry, NoveltyOp, PersistentNode,
    PersistentNodeBody, Rank, Value, distribution::cap, into_owned, resolve_pending,
};

/// The rank threshold for grouping entries into leaf segments (level 0). Every
/// key has rank >= 1; a child whose rank exceeds the level threshold ends the
/// node it belongs to.
pub(crate) const BOTTOM_RANK: Rank = 1;

/// A tree node held in live, editable form prior to serialization.
///
/// Unlike a [`PersistentNode`](crate::PersistentNode), a [`TransientNode`] keeps
/// its structure as owned, mutable collections of native keys and values and
/// has no content hash. It is the working representation produced while editing
/// a tree; serializing it (bottom-up) yields the durable
/// [`PersistentNode`](crate::PersistentNode) form.
#[derive(Debug)]
pub enum TransientNode<Key, Value> {
    /// An index node holding child nodes in either representation.
    Index(TransientIndex<Key, Value>),
    /// A leaf segment holding key-value entries.
    Segment(TransientSegment<Key, Value>),
}

/// An index node holding live child nodes and a novelty buffer.
///
/// An index carries no separator of its own: its separator is by definition
/// its first child's separator (the seam at any node's left edge is the seam
/// at its leftmost leaf's left edge), so it is derived on demand via
/// [`TransientNode::separator`].
#[derive(Debug)]
pub struct TransientIndex<Key, Value> {
    /// The child nodes, each persistent or transient.
    pub children: Vec<Node<Key, Value>>,
    /// Ops pending against this subtree, grouped per child link (the node's
    /// novelty), mirroring the stored form.
    ///
    /// A canonical edit (insert/delete) never introduces novelty, so on that
    /// path this is always empty and flows through reshape untouched. It
    /// becomes non-empty only on the hitchhiker write/flush path, where every
    /// op routes to its link when it is enqueued (the same lower-bound rule
    /// stored routing uses), so a flush hands each child its link's buffer
    /// verbatim and no later partition step exists.
    ///
    /// Any structural change to `children` (a splice, a regroup, a fusion)
    /// must first take the buffered ops out via
    /// [`Novelty::take_all`] and re-route them onto whatever nodes replace
    /// this one; the reshape paths do exactly that through `carry_novelty`.
    pub novelty: Novelty<Value>,
}

/// One child link's buffered ops in transient form.
///
/// The two variants are the cache discipline made structural: a link is
/// either **sealed**, carrying exactly the stored columnar encoding it was
/// opened with and untouched by any write since, or **open**, lifted to
/// decoded entries because a write reached it. A sealed buffer is embedded
/// into the next persist verbatim (no decode at open, no re-encode at
/// persist); only an open buffer pays a fresh encode. There is no separate
/// dirty flag to forget: mutating requires lifting, and lifting discards the
/// sealed encoding.
#[derive(Debug)]
enum LinkNovelty<Value> {
    /// The stored encoding, exactly as persisted, untouched since open.
    Sealed(NoveltyBuffer<Value>),
    /// Decoded ops, sorted by key with the newest op for a key last.
    Open(Vec<NoveltyEntry<Value>>),
}

impl<Value> LinkNovelty<Value>
where
    Value: self::Value,
{
    /// The number of ops buffered at this link.
    fn len(&self) -> usize {
        match self {
            LinkNovelty::Sealed(buffer) => buffer.count as usize,
            LinkNovelty::Open(entries) => entries.len(),
        }
    }

    /// Lifts this link to its decoded entries for mutation, decoding a sealed
    /// buffer. The sealed encoding is discarded: from here on this link's
    /// buffer is re-encoded at persist, which is exactly the invalidation the
    /// cache needs.
    fn lift<K>(&mut self) -> Result<&mut Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        if let LinkNovelty::Sealed(buffer) = self {
            *self = LinkNovelty::Open(buffer.entries::<K>()?);
        }
        match self {
            LinkNovelty::Open(entries) => Ok(entries),
            LinkNovelty::Sealed(_) => unreachable!("sealed buffer was lifted above"),
        }
    }

    /// Takes this link's ops, leaving it empty.
    fn take<K>(&mut self) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        match std::mem::replace(self, LinkNovelty::Open(Vec::new())) {
            LinkNovelty::Sealed(buffer) => buffer.entries::<K>(),
            LinkNovelty::Open(entries) => Ok(entries),
        }
    }

    /// Whether this link buffers an op for `key`.
    #[cfg(debug_assertions)]
    fn contains<K>(&self, key: &[u8]) -> Result<bool, DialogSearchTreeError>
    where
        K: self::Key,
    {
        match self {
            LinkNovelty::Sealed(buffer) => Ok(buffer.resolve::<K>(key)?.is_some()),
            LinkNovelty::Open(entries) => Ok(resolve_pending(entries, key).is_some()),
        }
    }
}

/// A transient index node's buffered ops, grouped per child link.
///
/// Grouping happens when an op is enqueued (one binary search over the
/// children's separators, the same rule [`ArchivedIndex::route`] applies to
/// stored nodes), and each link's buffer is held as a [`LinkNovelty`]:
/// sealed while untouched, lifted to decoded entries by the first write that
/// reaches it. Links are indexed positionally against the node's children; a
/// missing tail entry is an empty buffer.
///
/// Within one buffer the entries are sorted by key and the newest op for a
/// key is last; across links the concatenation in child order is the flat
/// sorted op list, since links partition the key space in order.
#[derive(Debug)]
pub struct Novelty<Value> {
    /// One buffer per child link, positionally aligned with the node's
    /// children; the vec may be shorter than the child list (absent tail
    /// buffers are empty).
    links: Vec<LinkNovelty<Value>>,
    /// Total buffered ops across every link, so capacity triggers read a
    /// number instead of scanning.
    total: usize,
}

impl<Value> Default for Novelty<Value> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Value> Novelty<Value> {
    /// An empty novelty set.
    pub fn new() -> Self {
        Self {
            links: Vec::new(),
            total: 0,
        }
    }

    /// Total buffered ops across every link.
    pub fn len(&self) -> usize {
        self.total
    }

    /// Whether no link buffers anything.
    pub fn is_empty(&self) -> bool {
        self.total == 0
    }
}

impl<Value> Novelty<Value>
where
    Value: self::Value,
{
    /// The largest number of ops buffered at any single link: the quantity
    /// the `PerChild` flush trigger thresholds on, read from per-link lengths
    /// rather than re-routing every op.
    pub(crate) fn peak(&self) -> usize {
        self.links.iter().map(LinkNovelty::len).max().unwrap_or(0)
    }

    /// How many links still carry their sealed stored encoding.
    #[cfg(test)]
    pub(crate) fn sealed_links(&self) -> usize {
        self.links
            .iter()
            .filter(|link| matches!(link, LinkNovelty::Sealed(_)))
            .count()
    }

    /// Lifts every sealed link to decoded entries, discarding all cached
    /// encodings, so a persist after this re-encodes every buffer from
    /// scratch. Exists for the byte-identity pin: the cached path and the
    /// fresh path must produce the same node bytes.
    #[cfg(test)]
    pub(crate) fn lift_all<K>(&mut self) -> Result<(), DialogSearchTreeError>
    where
        K: self::Key,
    {
        for link in &mut self.links {
            link.lift::<K>()?;
        }
        Ok(())
    }

    /// The link buffer at `at`, growing the positional vec with empty
    /// buffers as needed.
    fn link_mut(&mut self, at: usize) -> &mut LinkNovelty<Value> {
        if self.links.len() <= at {
            self.links
                .resize_with(at + 1, || LinkNovelty::Open(Vec::new()));
        }
        &mut self.links[at]
    }

    /// Routes `incoming` ops into their link buffers by the children's
    /// bounds (see [`link_bounds`]): one binary search per op, the same
    /// lower-bound rule stored routing uses, with a key below every bound
    /// clamping into link 0. Only the touched links are lifted and re-sorted;
    /// the stable sort keeps existing ops before incoming ones for equal
    /// keys, so the newest op for a key stays last.
    pub(crate) fn route<K>(
        &mut self,
        bounds: &[&[u8]],
        incoming: Vec<NoveltyEntry<Value>>,
    ) -> Result<(), DialogSearchTreeError>
    where
        K: self::Key,
    {
        if incoming.is_empty() {
            return Ok(());
        }
        let mut buckets: BTreeMap<usize, Vec<NoveltyEntry<Value>>> = BTreeMap::new();
        for entry in incoming {
            let at = bounds.partition_point(|separator| *separator <= entry.key.as_slice());
            buckets.entry(at).or_default().push(entry);
        }
        for (at, bucket) in buckets {
            self.total += bucket.len();
            let entries = self.link_mut(at).lift::<K>()?;
            entries.extend(bucket);
            entries.sort_by(|left, right| left.key.cmp(&right.key));
        }
        Ok(())
    }

    /// The winning buffered op for `key` at link `at` (the link that routes
    /// the key), or `None` when the key is not buffered there. A sealed link
    /// resolves against its encoded columns without lifting anything.
    pub(crate) fn resolve<K>(
        &self,
        at: usize,
        key: &[u8],
    ) -> Result<Option<NoveltyOp<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        match self.links.get(at) {
            None => Ok(None),
            Some(LinkNovelty::Sealed(buffer)) => buffer.resolve::<K>(key),
            Some(LinkNovelty::Open(entries)) => Ok(resolve_pending(entries, key).cloned()),
        }
    }

    /// Takes link `at`'s ops, leaving that buffer empty: what a flush hands
    /// the child at `at`, verbatim: the grouping already happened at
    /// enqueue, so there is no partition step here.
    pub(crate) fn take_link<K>(
        &mut self,
        at: usize,
    ) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        match self.links.get_mut(at) {
            None => Ok(Vec::new()),
            Some(link) => {
                let taken = link.take::<K>()?;
                self.total -= taken.len();
                Ok(taken)
            }
        }
    }

    /// Takes every buffered op, concatenated in link order (the flat sorted
    /// op list, since links partition the key space in order and each buffer
    /// is sorted), leaving the set empty. The form the reshape paths carry
    /// and re-route, and the drain a canonicalize replays.
    pub(crate) fn take_all<K>(&mut self) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        let mut out = Vec::with_capacity(self.total);
        for link in &mut self.links {
            out.extend(link.take::<K>()?);
        }
        self.links.clear();
        self.total = 0;
        Ok(out)
    }

    /// Drops every op buffered for `key` at link `at` (the link that routes
    /// the key). A canonical edit descending past this node supersedes any op
    /// it buffers for the same key; a link that does not buffer the key is
    /// left untouched (sealed stays sealed).
    pub(crate) fn remove_key<K>(
        &mut self,
        at: usize,
        key: &[u8],
    ) -> Result<(), DialogSearchTreeError>
    where
        K: self::Key,
    {
        #[cfg(debug_assertions)]
        for (other, link) in self.links.iter().enumerate() {
            if other != at {
                debug_assert!(
                    !link.contains::<K>(key).unwrap_or(false),
                    "an op for a key may live only at the link that routes it"
                );
            }
        }
        let Some(link) = self.links.get_mut(at) else {
            return Ok(());
        };
        let present = match link {
            LinkNovelty::Sealed(buffer) => buffer.resolve::<K>(key)?.is_some(),
            LinkNovelty::Open(entries) => resolve_pending(entries, key).is_some(),
        };
        if !present {
            return Ok(());
        }
        let entries = link.lift::<K>()?;
        let before = entries.len();
        entries.retain(|entry| entry.key.as_slice() != key);
        self.total -= before - entries.len();
        Ok(())
    }

    /// Re-homes ops after the separator at link `at`'s left edge moved to
    /// `bound` (a min-move edit in the subtree below). A rise strands ops in
    /// link `at` whose keys now sort below the bound: they belong to link
    /// `at - 1`, and every such key sorts after everything already buffered
    /// there (those keys sat below the OLD separator), so appending keeps
    /// that buffer sorted. A drop (possible under distributions whose floor
    /// rule is not monotone, like the test spec's) strands ops in link
    /// `at - 1` whose keys now sort at or above the bound: they belong to
    /// link `at` and sort before everything already buffered there, so they
    /// prepend. The two moved ranges are disjoint from their destinations'
    /// keys, so no precedence question arises, and links with nothing to
    /// move stay sealed.
    pub(crate) fn reroute_boundary<K>(
        &mut self,
        at: usize,
        bound: &[u8],
    ) -> Result<(), DialogSearchTreeError>
    where
        K: self::Key,
    {
        if at == 0 {
            return Ok(());
        }

        // A risen bound: the leading ops of link `at` fall below it now.
        let strays_below = match self.links.get(at) {
            None => false,
            Some(LinkNovelty::Open(entries)) => entries
                .first()
                .is_some_and(|entry| entry.key.as_slice() < bound),
            Some(LinkNovelty::Sealed(buffer)) => match buffer.keys::<K>()?.next_key()? {
                Some((_, key)) => key < bound,
                None => false,
            },
        };
        if strays_below {
            let entries = self.links[at].lift::<K>()?;
            let split = entries.partition_point(|entry| entry.key.as_slice() < bound);
            let moved: Vec<NoveltyEntry<Value>> = entries.drain(..split).collect();
            let left = self.link_mut(at - 1).lift::<K>()?;
            left.extend(moved);
            debug_assert!(
                left.windows(2).all(|pair| pair[0].key <= pair[1].key),
                "re-homed ops must keep the left buffer sorted"
            );
            return Ok(());
        }

        // A dropped bound: the trailing ops of link `at - 1` reach it now.
        let strays_above = match self.links.get(at - 1) {
            None => false,
            Some(LinkNovelty::Open(entries)) => entries
                .last()
                .is_some_and(|entry| entry.key.as_slice() >= bound),
            Some(LinkNovelty::Sealed(buffer)) => {
                let mut keys = buffer.keys::<K>()?;
                let mut last = None;
                while let Some((_, key)) = keys.next_key()? {
                    last = Some(key.to_vec());
                }
                last.is_some_and(|key| key.as_slice() >= bound)
            }
        };
        if strays_above {
            let entries = self.links[at - 1].lift::<K>()?;
            let split = entries.partition_point(|entry| entry.key.as_slice() < bound);
            let moved: Vec<NoveltyEntry<Value>> = entries.drain(split..).collect();
            let right = self.link_mut(at).lift::<K>()?;
            right.splice(0..0, moved);
            debug_assert!(
                right.windows(2).all(|pair| pair[0].key <= pair[1].key),
                "re-homed ops must keep the right buffer sorted"
            );
        }
        Ok(())
    }

    /// Appends the winning op per key whose key falls within the bounds, per
    /// link in link order (ascending key order). Within one buffer the last
    /// op for a key wins; a sealed link streams its keys and decodes only the
    /// winners' values.
    pub(crate) fn collect_winners_in_range<K>(
        &self,
        start: Bound<&[u8]>,
        end: Bound<&[u8]>,
        out: &mut Vec<NoveltyEntry<Value>>,
    ) -> Result<(), DialogSearchTreeError>
    where
        K: self::Key,
    {
        for link in &self.links {
            match link {
                LinkNovelty::Open(entries) => {
                    // Buffers are sorted by key, so the in-range ops are a
                    // contiguous run: seek to its start rather than walking
                    // the whole buffer.
                    let from = match start {
                        Bound::Included(bound) => {
                            entries.partition_point(|entry| entry.key.as_slice() < bound)
                        }
                        Bound::Excluded(bound) => {
                            entries.partition_point(|entry| entry.key.as_slice() <= bound)
                        }
                        Bound::Unbounded => 0,
                    };
                    let mut at = from;
                    while at < entries.len() {
                        match end {
                            Bound::Included(bound) if entries[at].key.as_slice() > bound => break,
                            Bound::Excluded(bound) if entries[at].key.as_slice() >= bound => break,
                            _ => {}
                        }
                        let mut last = at;
                        while last + 1 < entries.len() && entries[last + 1].key == entries[at].key {
                            last += 1;
                        }
                        out.push(entries[last].clone());
                        at = last + 1;
                    }
                }
                LinkNovelty::Sealed(buffer) => {
                    let mut keys = buffer.keys::<K>()?;
                    // The pending winner: the last-seen index for the current
                    // key, flushed when the key changes or the scan ends.
                    let mut winner: Option<(usize, Vec<u8>)> = None;
                    while let Some((at, key)) = keys.next_key()? {
                        let after_start = match start {
                            Bound::Included(bound) => key >= bound,
                            Bound::Excluded(bound) => key > bound,
                            Bound::Unbounded => true,
                        };
                        if !after_start {
                            continue;
                        }
                        let in_range = match end {
                            Bound::Included(bound) => key <= bound,
                            Bound::Excluded(bound) => key < bound,
                            Bound::Unbounded => true,
                        };
                        if !in_range {
                            break;
                        }
                        match &mut winner {
                            Some((winning, current)) if current.as_slice() == key => *winning = at,
                            _ => {
                                if let Some((winning, current)) = winner.take() {
                                    out.push(NoveltyEntry {
                                        key: current,
                                        op: buffer.op_at(winning)?,
                                    });
                                }
                                winner = Some((at, key.to_vec()));
                            }
                        }
                    }
                    if let Some((winning, current)) = winner {
                        out.push(NoveltyEntry {
                            key: current,
                            op: buffer.op_at(winning)?,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Converts the set into the stored per-link buffers for persist, in
    /// ascending child order: a sealed buffer is reused verbatim (only its
    /// child index is restamped, since siblings may have shifted it) and only
    /// an open buffer pays a fresh encode. Ops buffered beyond the node's
    /// links mark a broken grouping invariant and error rather than dropping
    /// writes.
    pub(crate) fn into_buffers<K>(
        self,
        links: &[Link],
    ) -> Result<Vec<NoveltyBuffer<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
        Value: for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    {
        let mut buffers = Vec::new();
        for (at, link) in self.links.into_iter().enumerate() {
            if at >= links.len() {
                if link.len() == 0 {
                    continue;
                }
                return Err(DialogSearchTreeError::Node(
                    "Novelty was buffered beyond the node's links".into(),
                ));
            }
            let buffer = match link {
                LinkNovelty::Open(entries) => {
                    if entries.is_empty() {
                        continue;
                    }
                    NoveltyBuffer::from_entries::<K>(at as u32, entries)?
                }
                LinkNovelty::Sealed(mut sealed) => {
                    sealed.child = at as u32;
                    // The cache's whole contract: the sealed bytes must be
                    // exactly what a fresh encode of the same ops produces.
                    // Verified on every debug persist, pinned by test in
                    // release.
                    #[cfg(debug_assertions)]
                    {
                        let fresh =
                            NoveltyBuffer::from_entries::<K>(sealed.child, sealed.entries::<K>()?)?;
                        let sealed_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&sealed)
                            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                        let fresh_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&fresh)
                            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                        debug_assert_eq!(
                            sealed_bytes.as_slice(),
                            fresh_bytes.as_slice(),
                            "a sealed buffer must persist byte-identical to a fresh encode"
                        );
                    }
                    sealed
                }
            };
            #[cfg(debug_assertions)]
            debug_assert_grouped::<K, Value>(&buffer, at, links)?;
            buffers.push(buffer);
        }
        Ok(buffers)
    }
}

impl<Value> Novelty<Value>
where
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Opens a stored index node's novelty into the transient grouped form.
    ///
    /// Each stored buffer stays SEALED: its encoded columns and values are
    /// carried over as one bulk copy, with no columnar decode, no key
    /// reconstruction, and no per-entry allocation. A buffer is decoded only
    /// if and when a write touches its link; an untouched buffer flows back
    /// into the next persist byte-identical, never re-encoded.
    pub(crate) fn open(index: &ArchivedIndex<Value>) -> Result<Self, DialogSearchTreeError> {
        let mut links: Vec<LinkNovelty<Value>> = Vec::new();
        let mut total = 0usize;
        let mut previous: Option<usize> = None;
        for buffer in index.novelty.iter() {
            let child = buffer.child.to_native() as usize;
            // Strictly ascending child order and in-range children, the same
            // validation the flat decode performed; a violation marks the
            // node corrupt.
            if previous.is_some_and(|previous| child <= previous) || child >= index.len() {
                return Err(DialogSearchTreeError::Encoding(
                    "Novelty buffers are not in ascending child order".into(),
                ));
            }
            previous = Some(child);
            total += buffer.checked_count()?;
            let sealed: NoveltyBuffer<Value> = rkyv::deserialize::<_, rkyv::rancor::Error>(buffer)
                .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))?;
            if links.len() <= child {
                links.resize_with(child + 1, || LinkNovelty::Open(Vec::new()));
            }
            links[child] = LinkNovelty::Sealed(sealed);
        }
        Ok(Self { links, total })
    }
}

/// Verifies (debug only) that a persisted buffer's keys lie within its
/// link's range `[sep(at), sep(at + 1))`: the grouping invariant enqueue-time
/// routing maintains and every reader relies on.
#[cfg(debug_assertions)]
fn debug_assert_grouped<K, Value>(
    buffer: &NoveltyBuffer<Value>,
    at: usize,
    links: &[Link],
) -> Result<(), DialogSearchTreeError>
where
    K: self::Key,
    Value: self::Value,
{
    let mut keys = buffer.keys::<K>()?;
    let mut first: Option<Vec<u8>> = None;
    let mut last: Option<Vec<u8>> = None;
    while let Some((_, key)) = keys.next_key()? {
        if first.is_none() {
            first = Some(key.to_vec());
        }
        last = Some(key.to_vec());
    }
    if at > 0
        && let Some(first) = &first
    {
        debug_assert!(
            first.as_slice() >= links[at].separator.as_slice(),
            "a link buffer's keys must not sort below the link's separator"
        );
    }
    if at + 1 < links.len()
        && let Some(last) = &last
    {
        debug_assert!(
            last.as_slice() < links[at + 1].separator.as_slice(),
            "a link buffer's keys must sort below the next link's separator"
        );
    }
    Ok(())
}

/// The routing bounds of an index's children: the separators of every child
/// after the first, in child order. Child `at` covers `[sep(at), sep(at + 1))`
/// under the lower-bound convention, so the number of bounds at or below a key
/// is the child covering it, with a key below every bound clamping into child
/// 0, the same rule [`ArchivedIndex::route`] applies to stored nodes.
pub(crate) fn link_bounds<Key, Value>(
    children: &[Node<Key, Value>],
) -> Result<Vec<&[u8]>, DialogSearchTreeError> {
    children
        .iter()
        .skip(1)
        .map(|child| child.separator())
        .collect()
}

/// A leaf segment holding live key-value entries.
#[derive(Debug)]
pub struct TransientSegment<Key, Value> {
    /// The key-value entries stored in this segment.
    pub entries: Vec<Entry<Key, Value>>,
    /// The separator at this segment's left edge: the shortest byte string
    /// above everything left of the seam and at or below this segment's
    /// first key. Empty for the tree's global leftmost segment. This is the
    /// ground truth every index level above derives its separators from.
    pub separator: Vec<u8>,
}

impl<Key, Value> TransientNode<Key, Value> {
    /// The separator at this node's left edge.
    ///
    /// A segment stores it; an index derives it from its first child (the
    /// seam at a node's left edge is its leftmost leaf's seam, so the same
    /// string propagates upward unchanged). Errors on an empty index, which
    /// violates the node invariant.
    ///
    /// Stored content only: a node's novelty is deliberately excluded, since
    /// a separator is both a routing key and a rank input, so letting a
    /// pending op move it would reshape the tree as a side effect of
    /// buffering.
    pub fn separator(&self) -> Result<&[u8], DialogSearchTreeError> {
        match self {
            TransientNode::Segment(segment) => Ok(segment.separator.as_slice()),
            TransientNode::Index(index) => index
                .children
                .first()
                .ok_or_else(|| DialogSearchTreeError::Node("Index was unexpectedly empty".into()))?
                .separator(),
        }
    }
}

impl<Key, Value> TransientNode<Key, Value> {
    /// Interprets this node as an index, erroring if it is a segment. Mirrors
    /// [`PersistentNode::as_index`](crate::PersistentNode::as_index).
    pub fn as_index(&self) -> Result<&TransientIndex<Key, Value>, DialogSearchTreeError> {
        match self {
            TransientNode::Index(index) => Ok(index),
            TransientNode::Segment(_) => Err(DialogSearchTreeError::Node(
                "Attempted to interpret a segment node as an index node".into(),
            )),
        }
    }

    /// Interprets this node as an index for mutation, erroring if it is a
    /// segment.
    pub fn as_index_mut(
        &mut self,
    ) -> Result<&mut TransientIndex<Key, Value>, DialogSearchTreeError> {
        match self {
            TransientNode::Index(index) => Ok(index),
            TransientNode::Segment(_) => Err(DialogSearchTreeError::Node(
                "Attempted to interpret a segment node as an index node".into(),
            )),
        }
    }

    /// Interprets this node as a segment, erroring if it is an index. Mirrors
    /// [`PersistentNode::as_segment`](crate::PersistentNode::as_segment).
    pub fn as_segment(&self) -> Result<&TransientSegment<Key, Value>, DialogSearchTreeError> {
        match self {
            TransientNode::Segment(segment) => Ok(segment),
            TransientNode::Index(_) => Err(DialogSearchTreeError::Node(
                "Attempted to interpret an index node as a segment node".into(),
            )),
        }
    }

    /// Interprets this node as a segment for mutation, erroring if it is an
    /// index.
    pub fn as_segment_mut(
        &mut self,
    ) -> Result<&mut TransientSegment<Key, Value>, DialogSearchTreeError> {
        match self {
            TransientNode::Segment(segment) => Ok(segment),
            TransientNode::Index(_) => Err(DialogSearchTreeError::Node(
                "Attempted to interpret an index node as a segment node".into(),
            )),
        }
    }

    /// Returns a mutable reference to this index node's child at `at`, already
    /// lifted to transient form.
    ///
    /// Errors if this node is a segment, the index is out of range, or the child
    /// is still a [`Node::Persistent`] reference (it should have been lifted by
    /// the descent that reached it).
    pub fn child_mut(
        &mut self,
        at: usize,
    ) -> Result<&mut TransientNode<Key, Value>, DialogSearchTreeError> {
        match self.as_index_mut()?.children.get_mut(at) {
            Some(Node::Transient(child)) => Ok(child),
            Some(Node::Persistent(_)) => Err(DialogSearchTreeError::Node(
                "Re-shape path descended into a node that was not lifted".into(),
            )),
            None => Err(DialogSearchTreeError::Node(
                "Re-shape path child index out of range".into(),
            )),
        }
    }
}

impl<Key, Value> TransientNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Opens an index [`PersistentNode`] one level into a [`TransientIndex`].
    ///
    /// The index's links become children, each held as a [`Node::Persistent`]
    /// reference (the grandchildren stay serialized and shared until an edit
    /// reaches them). Only one level is opened; deeper nodes are opened lazily
    /// as edits descend. A segment node is opened directly into a
    /// [`TransientSegment`] by the caller from its decoded entries.
    pub fn open_index(
        node: &PersistentNode<Key, Value>,
    ) -> Result<TransientIndex<Key, Value>, DialogSearchTreeError> {
        let index = node.as_index()?;
        let children = index
            .links()?
            .into_iter()
            .map(Node::Persistent)
            .collect::<Vec<Node<Key, Value>>>();
        // Carry the node's novelty across to the transient form so a flush or
        // canonicalize can act on it. The stored form is already grouped per
        // child link, and the grouping survives verbatim: every buffer stays
        // sealed (its encoded bytes bulk-copied, nothing decoded) until a
        // write touches its link, so an untouched buffer costs no decode here
        // and no re-encode at the next persist.
        let novelty = Novelty::open(index)?;
        Ok(TransientIndex { children, novelty })
    }
}

impl<Key, Value> TransientNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// Opens a [`PersistentNode`] one level into its editable
    /// [`TransientNode`] form: an index becomes a [`TransientIndex`] whose
    /// children stay [`Node::Persistent`] references (their links carry their
    /// separators), and a segment becomes a [`TransientSegment`] with its
    /// entries decoded to owned form. Deeper nodes are opened lazily as edits
    /// descend.
    ///
    /// `separator` is the seam at the opened node's left edge, taken from the
    /// link the caller followed to reach it (the empty separator for a root).
    /// A segment stores it; an index needs none of its own, since its
    /// separator is derived from its first child.
    pub fn open(
        node: &PersistentNode<Key, Value>,
        separator: Vec<u8>,
    ) -> Result<Self, DialogSearchTreeError> {
        match node.body()? {
            ArchivedNodeBody::Index(_) => {
                Ok(TransientNode::Index(TransientNode::open_index(node)?))
            }
            ArchivedNodeBody::Segment(segment) => {
                let mut entries = Vec::with_capacity(segment.len());
                let mut keys = segment.keys::<Key>()?;
                while let Some((at, key)) = keys.next_key()? {
                    entries.push(Entry {
                        key: Key::try_from_bytes(key)?,
                        value: into_owned(segment.value_at(at)?)?,
                    });
                }
                Ok(TransientNode::Segment(TransientSegment {
                    entries,
                    separator,
                }))
            }
        }
    }
}

impl<Key, Value> TransientNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Serializes this transient node into a [`PersistentNode`] bottom-up,
    /// recording every newly created node in `delta` by hash.
    ///
    /// For a segment, the entries are encoded directly. For an index, each
    /// child is resolved to a [`Link`] first (a [`Node::Transient`] child
    /// recurses and serializes; a [`Node::Persistent`] child already is a
    /// link), and the node's novelty, already grouped per child link at
    /// enqueue time, is embedded buffer by buffer: a sealed buffer's stored
    /// encoding is reused verbatim, and only the links a write touched are
    /// freshly encoded with the segment codec. This makes no shape decisions:
    /// the children, novelty, and entries are encoded exactly as the edits
    /// left them.
    pub fn persist(
        self,
        delta: &mut Delta<Blake3Hash, Buffer>,
        manifest: &Manifest,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        let body = match self {
            TransientNode::Segment(segment) => {
                PersistentNodeBody::segment_from_entries(segment.entries, *manifest)?
            }
            TransientNode::Index(TransientIndex { children, novelty }) => {
                let links = children
                    .into_iter()
                    .map(|child| child.into_link(delta, manifest))
                    .collect::<Result<Vec<Link>, DialogSearchTreeError>>()?;
                let buffers = novelty.into_buffers::<Key>(&links)?;
                PersistentNodeBody::index_from_buffers(links, buffers, *manifest)?
            }
        };

        let node = PersistentNode::new(Buffer::from(body.as_bytes()?));
        delta.add(node.hash().clone(), node.buffer().clone());
        Ok(node)
    }
}

impl<Key, Value> TransientSegment<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// Returns the upper bound key of this segment, the key of its last entry.
    ///
    /// Errors if the segment is empty, which violates the node invariant.
    pub fn upper_bound(&self) -> Result<&Key, DialogSearchTreeError> {
        self.entries
            .last()
            .map(|entry| &entry.key)
            .ok_or_else(|| DialogSearchTreeError::Node("Segment was unexpectedly empty".into()))
    }
}

/// Regroups an ordered list of child nodes into index nodes by the canonical
/// cut rule for the given `level` (its threshold is `BOTTOM_RANK + level`).
///
/// Each child is ranked by the seam coin over its separator; a child whose
/// rank exceeds the threshold starts a new group (the cut falls on the seam
/// at the child's left edge). Because a node's separator equals its leftmost
/// leaf seam's separator, the same string is ranked at every level a seam
/// punches through, which is the rank recursion fed separator strings instead
/// of full keys. Returns one [`Node::Transient`] index per group; each
/// group's own separator is derived from its first child, so regrouping
/// never recomputes a separator.
pub(crate) fn regroup_children<Key, Value, D>(
    children: Vec<Node<Key, Value>>,
    level: Rank,
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
    let threshold = BOTTOM_RANK + level;
    let mut groups: Vec<Node<Key, Value>> = vec![];
    let mut pending: Vec<Node<Key, Value>> = vec![];

    // Reshaping produces canonical index nodes: novelty lives on the nodes
    // the write path buffered it at, and a regrouped node is a fresh one, so
    // it starts empty. A flush is what moves buffered ops downward.
    for child in children {
        let rank = D::seam_rank(child.separator()?, manifest);
        if rank > threshold && !pending.is_empty() {
            groups.push(
                TransientNode::Index(TransientIndex {
                    children: std::mem::take(&mut pending),
                    novelty: Novelty::new(),
                })
                .into(),
            );
        }
        pending.push(child);
    }

    if !pending.is_empty() {
        groups.push(
            TransientNode::Index(TransientIndex {
                children: pending,
                novelty: Novelty::new(),
            })
            .into(),
        );
    }

    Ok(groups)
}

/// Regroups an ordered list of entries into leaf segments by the canonical cut
/// rule at level 0 (threshold [`BOTTOM_RANK`]): a segment ends at an entry
/// whose leaf-coin rank exceeds the threshold AND whose seam to the successor
/// entry survives the veto ([`Distribution::vetoes`]) — a vetoed seam keeps
/// the two keys in one segment at every level. Which leaf coin flips is the
/// manifest's choice (`max_segment`: zero keeps the entry-counted geometric
/// coin, non-zero paces cuts by entry weight; see
/// [`weight_paced_rank`](crate::distribution::weight_paced_rank)) — either
/// way the coin decision is per key. A non-zero `max_segment` additionally
/// arms the backstop: a fully vetoed stretch whose weight exceeds the
/// target — the one shape no coin is allowed to cut — is force-split at
/// rendezvous anchors ([`cap::forced_cut_positions`]), so the caller must
/// hand this whole stretches (the edit path widens its window across the
/// self-identifying forced seams; see `merge_vetoed_stretch` in the
/// transient tree).
///
/// The window's last entry proposes no cut: its seam partner (the tree-wide
/// successor key) lies beyond the window, and that seam's status cannot have
/// drifted — a seam's separator is invariant under every edit that keeps
/// both partner keys (the edit-stability note on [`Distribution::vetoes`]),
/// and the edits that remove a partner (boundary deletes, orphan appends)
/// widen their window across the seam before regrouping.
///
/// `floor` is the separator at the left edge of the run (the edited
/// segment's previous separator). The first produced segment re-derives its
/// separator from its (possibly changed) first key against that floor; every
/// interior seam is fresh, with both adjacent keys in hand, so its separator
/// is computed directly. Returns one [`Node::Transient`] segment per group;
/// an empty entry list produces no groups (the caller propagates the removal,
/// and with it the floor, per the boundary-delete paths).
pub(crate) fn regroup_entries<Key, Value, D>(
    entries: Vec<Entry<Key, Value>>,
    floor: Vec<u8>,
    manifest: &Manifest,
) -> Vec<Node<Key, Value>>
where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    let mut groups: Vec<Node<Key, Value>> = vec![];
    let mut pending: Vec<Entry<Key, Value>> = vec![];
    // The last key of the previously sealed group; None while sealing the
    // first group, whose separator comes from the floor instead.
    let mut previous_last: Option<Key> = None;

    // Pair-aware cuts, decided before the entries move: the coin proposes a
    // boundary after an entry, and the veto rejects the proposal when the
    // seam to the successor cannot be told apart within the separator
    // bound. Both partner keys are needed, so the decisions are computed
    // over the borrowed list first. The veto verdicts are kept: they
    // delimit the stretches the backstop below scans.
    //
    // The weight bank rides the same walk: a vetoed seam banks its left
    // key's weight (no cut is possible there), and every ACCEPTED seam
    // spends the bank into its cut decision and resets it — reset on every
    // accepted seam, cut or no cut, so the bank is "weight since the last
    // accepted seam" (a structural property of the key sequence) and never
    // "weight since the last cut" (which would cascade decisions off coin
    // outcomes and break convergence). See `Distribution::leaf_cut`.
    let count = entries.len();
    let mut vetoed = vec![false; count.saturating_sub(1)];
    let mut cut_after = vec![false; count];
    let mut bank = 0usize;
    for at in 0..count.saturating_sub(1) {
        let key = entries[at].key.as_ref();
        vetoed[at] = D::vetoes(key, entries[at + 1].key.as_ref(), manifest);
        if vetoed[at] {
            // The coin is skipped entirely for vetoed seams: the veto
            // overrides whatever it would say, and the weight moves into
            // the bank instead.
            bank += cap::entry_weight(key);
        } else {
            cut_after[at] = D::leaf_cut(key, bank, manifest);
            bank = 0;
        }
    }

    // The frame partition is the COIN's verdicts alone, snapshotted before
    // any forced overlay: forced cuts (either backstop) never feed back
    // into frame definition, so there is no cascade.
    let coin_cut = cut_after.clone();

    // The stretch backstop: a maximal stretch of vetoed seams is uncuttable
    // by any coin, so when its summed entry weight exceeds `max_segment` it
    // is force-split at the anchors `cap::forced_cut_positions` chooses. A
    // group starting at a forced anchor carries the long-form forced
    // separator (`cap::forced_seam_separator`), which keeps the seam out
    // of every index level (the seam coin's length guard) and marks the
    // pieces as one run in stored form, so an edit can rejoin them.
    // Stretch extents never cross the window: a vetoed seam exists in
    // stored form only as a forced seam, and the edit path widens its
    // window across those before regrouping.
    let mut forced_start = vec![false; count];
    if manifest.max_segment > 0 {
        let mut at = 0usize;
        while at < vetoed.len() {
            if !vetoed[at] {
                at += 1;
                continue;
            }
            let start = at;
            while at < vetoed.len() && vetoed[at] {
                at += 1;
            }
            // The stretch covers keys `start..=at` (the last vetoed seam
            // joins keys `at - 1` and `at`).
            let keys: Vec<&Key> = entries[start..=at].iter().map(|entry| &entry.key).collect();
            for cut in cap::forced_cut_positions(&keys, manifest) {
                cut_after[start + cut - 1] = true;
                forced_start[start + cut] = true;
            }
        }
    }

    // The frame ceiling: a frame (the entries between coin-decided cuts)
    // over `frame_ceiling_factor * max_segment` is force-split at accepted
    // seams (`cap::frame_cut_positions`), bounding the weight coin's
    // natural exponential tail. Same stored form and same window contract
    // as the stretch backstop: forced seams are self-identifying and the
    // edit path widens across them, so a frame is always regrouped whole.
    if manifest.frame_ceiling() > 0 {
        let mut start = 0usize;
        for end in 0..count {
            let closes_frame = coin_cut[end] || end + 1 == count;
            if !closes_frame {
                continue;
            }
            if end > start {
                let keys: Vec<&Key> = entries[start..=end]
                    .iter()
                    .map(|entry| &entry.key)
                    .collect();
                let seams = &vetoed[start..end];
                for cut in cap::frame_cut_positions(&keys, seams, manifest) {
                    cut_after[start + cut - 1] = true;
                    forced_start[start + cut] = true;
                }
            }
            start = end + 1;
        }
    }

    let mut group_start = 0usize;
    for (at, entry) in entries.into_iter().enumerate() {
        pending.push(entry);
        if cut_after[at] {
            seal::<Key, Value, D>(
                &mut pending,
                &mut previous_last,
                &mut groups,
                &floor,
                forced_start[group_start],
                manifest,
            );
            group_start = at + 1;
        }
    }

    if !pending.is_empty() {
        seal::<Key, Value, D>(
            &mut pending,
            &mut previous_last,
            &mut groups,
            &floor,
            forced_start[group_start],
            manifest,
        );
    }

    groups
}

/// Seals one group of entries into a segment, deriving its left-edge
/// separator: from the floor for the very first group of a regroup, the
/// long forced form when the group starts at a backstop anchor, and the
/// canonical shortest-distinguishing prefix against the previous group's
/// last key everywhere else.
fn seal<Key, Value, D>(
    pending: &mut Vec<Entry<Key, Value>>,
    previous_last: &mut Option<Key>,
    groups: &mut Vec<Node<Key, Value>>,
    floor: &[u8],
    forced: bool,
    manifest: &Manifest,
) where
    Key: self::Key,
    Value: self::Value,
    D: Distribution,
{
    let entries = std::mem::take(pending);
    let first = entries
        .first()
        .expect("groups are sealed only when non-empty")
        .key
        .clone();
    let last = entries
        .last()
        .expect("groups are sealed only when non-empty")
        .key
        .clone();
    let separator = match previous_last.as_ref() {
        None => D::reseparate(first.as_ref(), floor),
        Some(previous) if forced => {
            cap::forced_seam_separator(previous.as_ref(), first.as_ref(), manifest)
        }
        Some(previous) => D::separator(previous.as_ref(), first.as_ref()),
    };
    *previous_last = Some(last);
    groups.push(TransientNode::Segment(TransientSegment { entries, separator }).into());
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;

    use super::{BOTTOM_RANK, regroup_entries};
    use crate::{Entry, Geometric, Manifest, Rank, distribution};

    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// The geometric rank of a `u32` key, hashed the same way the tree hashes it.
    fn rank_of(key: u32) -> Rank {
        distribution::geometric::rank(&Blake3Hash::hash(&key.to_le_bytes()))
    }

    /// One regrouped leaf segment's worth of entries.
    type Segment = Vec<Entry<[u8; 4], Vec<u8>>>;

    /// Regroups `keys` (as little-endian `[u8; 4]` entries) into leaf segments
    /// with the geometric distribution, returning each segment's entries.
    fn segments_of(keys: &[u32]) -> Result<Vec<Segment>> {
        let entries: Vec<Entry<[u8; 4], Vec<u8>>> = keys
            .iter()
            .map(|&i| Entry {
                key: i.to_le_bytes(),
                value: vec![i as u8],
            })
            .collect();

        regroup_entries::<[u8; 4], Vec<u8>, Geometric>(entries, Vec::new(), &Manifest::default())
            .into_iter()
            .map(|node| Ok(node.into_transient()?.as_segment()?.entries.clone()))
            .collect()
    }

    /// Regrouping cuts a new segment exactly at every boundary entry (rank above
    /// the leaf threshold): each segment but the last ends on a boundary, and no
    /// boundary sits in a segment's interior. A trailing run with no terminating
    /// boundary forms the final open segment.
    #[dialog_common::test]
    async fn it_partitions_entries_at_rank_boundaries() -> Result<()> {
        let keys: Vec<u32> = (0..1000).collect();
        let boundary_count = keys.iter().filter(|&&k| rank_of(k) > BOTTOM_RANK).count();
        assert!(boundary_count > 0, "need at least one boundary in 0..1000");

        // The entries are byte-lexicographically ordered, the order regrouping
        // cuts in, so derive the expected count over the same order.
        let mut sorted = keys.clone();
        sorted.sort_by_key(|k| k.to_le_bytes());
        let last_is_boundary = sorted
            .last()
            .map(|&k| rank_of(k) > BOTTOM_RANK)
            .unwrap_or(false);

        let segments = segments_of(&sorted)?;

        // One segment per boundary, plus a trailing open segment unless the very
        // last entry is itself a boundary.
        let expected = if last_is_boundary {
            boundary_count
        } else {
            boundary_count + 1
        };
        assert_eq!(segments.len(), expected, "wrong number of segments");

        for (i, segment) in segments.iter().enumerate() {
            // Every segment ends on a boundary except the trailing open segment,
            // which exists only when the last entry overall is not a boundary.
            let is_trailing_open = i == segments.len() - 1 && !last_is_boundary;
            for (j, entry) in segment.iter().enumerate() {
                let at_end = j == segment.len() - 1;
                let key = u32::from_le_bytes(entry.key);
                if at_end && !is_trailing_open {
                    assert!(
                        rank_of(key) > BOTTOM_RANK,
                        "segment {i} must end on a boundary, key {key} has rank {}",
                        rank_of(key)
                    );
                } else {
                    assert!(
                        rank_of(key) <= BOTTOM_RANK,
                        "interior key {key} of segment {i} must not be a boundary"
                    );
                }
            }
        }

        Ok(())
    }

    /// Regrouping preserves key order: entries are sorted within every segment,
    /// and segments are in ascending, non-overlapping key order.
    #[dialog_common::test]
    async fn it_preserves_entry_order_within_and_across_segments() -> Result<()> {
        let mut keys: Vec<u32> = (0..500).collect();
        keys.sort_by_key(|k| k.to_le_bytes());

        let segments = segments_of(&keys)?;

        let mut prev_upper: Option<[u8; 4]> = None;
        for segment in &segments {
            for pair in segment.windows(2) {
                assert!(
                    pair[0].key < pair[1].key,
                    "entries within a segment must be sorted"
                );
            }
            if let (Some(prev), Some(first)) = (prev_upper, segment.first()) {
                assert!(prev < first.key, "segments must be in ascending key order");
            }
            if let Some(last) = segment.last() {
                prev_upper = Some(last.key);
            }
        }

        Ok(())
    }

    /// Regrouping conserves entries: the segments together hold exactly the input
    /// entries, none dropped or duplicated.
    #[dialog_common::test]
    async fn it_preserves_total_entry_count_across_segments() -> Result<()> {
        let n = 1000u32;
        let keys: Vec<u32> = (0..n).collect();

        let segments = segments_of(&keys)?;
        let total: usize = segments.iter().map(|segment| segment.len()).sum();

        assert_eq!(
            total, n as usize,
            "every entry must land in exactly one segment"
        );

        Ok(())
    }
}
