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
    /// Decoded ops WITH their current encoding alongside, the state a
    /// non-consuming persist leaves an open link in: the next persist
    /// re-embeds `buffer` verbatim (like a sealed link), while the next
    /// mutation takes `entries` directly and drops the stale encoding (like
    /// an open link) — so a link that ops keep landing in never re-decodes
    /// its accumulated buffer, it only re-encodes it at each persist.
    Cached {
        /// The decoded ops, identical in content to `buffer`.
        entries: Vec<NoveltyEntry<Value>>,
        /// The encoding the last persist embedded.
        buffer: NoveltyBuffer<Value>,
    },
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
            LinkNovelty::Cached { entries, .. } => entries.len(),
        }
    }

    /// Lifts this link to its decoded entries for mutation, decoding a sealed
    /// buffer. The encoding (sealed or cached) is discarded: from here on
    /// this link's buffer is re-encoded at persist, which is exactly the
    /// invalidation the cache needs.
    fn lift<K>(&mut self) -> Result<&mut Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        match self {
            LinkNovelty::Sealed(buffer) => {
                *self = LinkNovelty::Open(buffer.entries::<K>()?);
            }
            LinkNovelty::Cached { entries, .. } => {
                *self = LinkNovelty::Open(std::mem::take(entries));
            }
            LinkNovelty::Open(_) => {}
        }
        match self {
            LinkNovelty::Open(entries) => Ok(entries),
            _ => unreachable!("the buffer was lifted above"),
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
            LinkNovelty::Cached { entries, .. } => Ok(entries),
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
            LinkNovelty::Open(entries) | LinkNovelty::Cached { entries, .. } => {
                Ok(resolve_pending(entries, key).is_some())
            }
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
    /// Total buffered WEIGHT across every link (key bytes + value payload
    /// weight per op, the same metering the frame ceiling uses), for the
    /// byte-capped flush trigger. `None` until first asked for (a spine
    /// opened from stored buffers computes it lazily by streaming the
    /// sealed columns); once computed, every mutator keeps it exact.
    weight: Option<usize>,
}

/// The weight one buffered op contributes toward the buffer byte cap: its
/// key bytes plus its value's payload weight (a retract carries no value
/// and is charged like [`State::Removed`]'s footprint).
fn novelty_entry_weight<Value>(entry: &NoveltyEntry<Value>) -> usize
where
    Value: self::Value,
{
    entry.key.len()
        + crate::entry::ENTRY_ENCODING_OVERHEAD
        + match &entry.op {
            NoveltyOp::Assert(value) => value.payload_weight(),
            NoveltyOp::Retract => 16,
        }
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
            weight: Some(0),
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
            if let Some(weight) = self.weight.as_mut() {
                *weight += bucket.iter().map(novelty_entry_weight).sum::<usize>();
            }
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
            Some(LinkNovelty::Open(entries)) | Some(LinkNovelty::Cached { entries, .. }) => {
                Ok(resolve_pending(entries, key).cloned())
            }
        }
    }

    /// Total buffered weight across every link (the byte-cap trigger's
    /// quantity), computed lazily on first ask — a spine opened from stored
    /// buffers streams their sealed columns once — and kept exact by every
    /// mutator afterwards.
    pub(crate) fn weight<K>(&mut self) -> Result<usize, DialogSearchTreeError>
    where
        K: self::Key,
    {
        if let Some(weight) = self.weight {
            return Ok(weight);
        }
        let mut weight = 0usize;
        for link in &self.links {
            weight += match link {
                LinkNovelty::Sealed(buffer) => buffer.weight::<K>()?,
                LinkNovelty::Open(entries) | LinkNovelty::Cached { entries, .. } => {
                    entries.iter().map(novelty_entry_weight).sum()
                }
            };
        }
        self.weight = Some(weight);
        Ok(weight)
    }

    /// Per-link `(link, weight, ops)` for every non-empty link buffer, the
    /// quantities a selective flush orders its shedding by. A sealed link
    /// streams its encoded columns; nothing is lifted.
    pub(crate) fn link_measures<K>(
        &self,
    ) -> Result<Vec<(usize, usize, usize)>, DialogSearchTreeError>
    where
        K: self::Key,
    {
        let mut measures = Vec::new();
        for (at, link) in self.links.iter().enumerate() {
            let ops = link.len();
            if ops == 0 {
                continue;
            }
            let weight = match link {
                LinkNovelty::Sealed(buffer) => buffer.weight::<K>()?,
                LinkNovelty::Open(entries) | LinkNovelty::Cached { entries, .. } => {
                    entries.iter().map(novelty_entry_weight).sum()
                }
            };
            measures.push((at, weight, ops));
        }
        Ok(measures)
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
                if let Some(weight) = self.weight.as_mut() {
                    *weight -= taken.iter().map(novelty_entry_weight).sum::<usize>();
                }
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
        self.weight = Some(0);
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
            LinkNovelty::Open(entries) | LinkNovelty::Cached { entries, .. } => {
                resolve_pending(entries, key).is_some()
            }
        };
        if !present {
            return Ok(());
        }
        let entries = link.lift::<K>()?;
        let before = entries.len();
        let mut removed_weight = 0usize;
        entries.retain(|entry| {
            let keep = entry.key.as_slice() != key;
            if !keep {
                removed_weight += novelty_entry_weight(entry);
            }
            keep
        });
        self.total -= before - entries.len();
        if let Some(weight) = self.weight.as_mut() {
            *weight -= removed_weight;
        }
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
            Some(LinkNovelty::Open(entries)) | Some(LinkNovelty::Cached { entries, .. }) => entries
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
            Some(LinkNovelty::Open(entries)) | Some(LinkNovelty::Cached { entries, .. }) => entries
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
                LinkNovelty::Open(entries) | LinkNovelty::Cached { entries, .. } => {
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
                    // The pending winner: the last-seen (index, value slot)
                    // for the current key, flushed when the key changes or
                    // the scan ends. The slot is tracked as the polarity
                    // column is walked (`keys()` validated it), so decoding
                    // a winner costs no per-op polarity re-scan.
                    let mut winner: Option<(usize, usize, Vec<u8>)> = None;
                    let mut asserts = 0usize;
                    while let Some((at, key)) = keys.next_key()? {
                        let slot = asserts;
                        if buffer.polarity.get(at).copied() == Some(1) {
                            asserts += 1;
                        }
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
                            Some((winning, winning_slot, current)) if current.as_slice() == key => {
                                *winning = at;
                                *winning_slot = slot;
                            }
                            _ => {
                                if let Some((winning, winning_slot, current)) = winner.take() {
                                    out.push(NoveltyEntry {
                                        key: current,
                                        op: buffer.op_with_slot(winning, winning_slot)?,
                                    });
                                }
                                winner = Some((at, slot, key.to_vec()));
                            }
                        }
                    }
                    if let Some((winning, winning_slot, current)) = winner {
                        out.push(NoveltyEntry {
                            key: current,
                            op: buffer.op_with_slot(winning, winning_slot)?,
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
                LinkNovelty::Cached {
                    buffer: mut sealed, ..
                }
                | LinkNovelty::Sealed(mut sealed) => {
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

    /// The stored per-link buffers for a NON-consuming persist, leaving the
    /// set intact so the live spine survives the persist and keeps taking
    /// writes.
    ///
    /// The borrowed twin of [`into_buffers`](Self::into_buffers), with one
    /// addition: an open link is encoded once and then held as
    /// [`LinkNovelty::Cached`] — the decoded ops stay live for the next
    /// append (no re-decode of the accumulated buffer) while the encoding
    /// is re-embedded verbatim by any persist that arrives before the next
    /// mutation. Encoding is a pure function of the op list, so a cached
    /// buffer is bit-identical to what a fresh open of the persisted node
    /// would carry.
    pub(crate) fn persist_buffers<K>(
        &mut self,
        links: &[Link],
    ) -> Result<Vec<NoveltyBuffer<Value>>, DialogSearchTreeError>
    where
        K: self::Key,
        Value: for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    {
        let mut buffers = Vec::new();
        for (at, link) in self.links.iter_mut().enumerate() {
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
                    let buffer = NoveltyBuffer::from_entries_ref::<K>(at as u32, entries)?;
                    let emitted = buffer.clone();
                    *link = LinkNovelty::Cached {
                        entries: std::mem::take(entries),
                        buffer,
                    };
                    emitted
                }
                LinkNovelty::Cached { entries, buffer } => {
                    // `entries` backs the debug-only identity check below;
                    // release builds embed the cached encoding without it.
                    #[cfg(not(debug_assertions))]
                    let _ = entries;
                    // Siblings may have shifted this buffer's child index;
                    // restamp the retained copy too so it stays in sync with
                    // what was just persisted.
                    buffer.child = at as u32;
                    #[cfg(debug_assertions)]
                    {
                        let fresh = NoveltyBuffer::from_entries_ref::<K>(buffer.child, entries)?;
                        let cached_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(buffer)
                            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                        let fresh_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&fresh)
                            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                        debug_assert_eq!(
                            cached_bytes.as_slice(),
                            fresh_bytes.as_slice(),
                            "a cached buffer must persist byte-identical to a fresh encode"
                        );
                    }
                    buffer.clone()
                }
                LinkNovelty::Sealed(sealed) => {
                    sealed.child = at as u32;
                    #[cfg(debug_assertions)]
                    {
                        let fresh =
                            NoveltyBuffer::from_entries::<K>(sealed.child, sealed.entries::<K>()?)?;
                        let sealed_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(sealed)
                            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                        let fresh_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&fresh)
                            .map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))?;
                        debug_assert_eq!(
                            sealed_bytes.as_slice(),
                            fresh_bytes.as_slice(),
                            "a sealed buffer must persist byte-identical to a fresh encode"
                        );
                    }
                    sealed.clone()
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
        Ok(Self {
            links,
            total,
            weight: None,
        })
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
    /// The key-value entries stored in this segment. Private so every
    /// mutation flows through a method that keeps the cached total weight
    /// exact; readers borrow through [`TransientSegment::entries`].
    entries: Vec<Entry<Key, Value>>,
    /// The separator at this segment's left edge: the shortest byte string
    /// above everything left of the seam and at or below this segment's
    /// first key. Empty for the tree's global leftmost segment. This is the
    /// ground truth every index level above derives its separators from.
    pub separator: Vec<u8>,
    /// Cached sum of the entries' weights ([`Entry::weight`]), `None` until
    /// first queried or after a wholesale mutation invalidated it. The edit
    /// path's frame-ceiling gate reads this once per edit; without the cache
    /// it re-summed the whole leaf on every membership-changing edit, which
    /// made batch commits quadratic in leaf size. The cached value must be
    /// exact whenever it is `Some`: an under-report would let a segment
    /// exceed the frame ceiling and change tree shape.
    weight: Option<usize>,
}

impl<Key, Value> TransientSegment<Key, Value> {
    /// Builds a segment from its entries and left-edge separator.
    pub fn new(entries: Vec<Entry<Key, Value>>, separator: Vec<u8>) -> Self {
        Self {
            entries,
            separator,
            weight: None,
        }
    }

    /// The key-value entries stored in this segment.
    pub fn entries(&self) -> &[Entry<Key, Value>] {
        &self.entries
    }

    /// Mutable access to the entries for wholesale edits (trims, pops).
    /// Invalidates the cached total weight; the targeted edit methods
    /// ([`upsert`](Self::upsert), [`delete`](Self::delete)) maintain it
    /// incrementally instead and should be preferred on hot paths.
    pub fn entries_mut(&mut self) -> &mut Vec<Entry<Key, Value>> {
        self.weight = None;
        &mut self.entries
    }

    /// Consumes the segment, returning its entries.
    pub fn into_entries(self) -> Vec<Entry<Key, Value>> {
        self.entries
    }

    /// Consumes the segment, returning its entries and separator.
    pub fn into_parts(self) -> (Vec<Entry<Key, Value>>, Vec<u8>) {
        (self.entries, self.separator)
    }

    /// Takes the entries out of the segment, leaving it empty.
    pub fn take_entries(&mut self) -> Vec<Entry<Key, Value>> {
        self.weight = None;
        std::mem::take(&mut self.entries)
    }
}

impl<Key, Value> TransientSegment<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
{
    /// The exact sum of the entries' weights ([`Entry::weight`]): the number
    /// the frame-ceiling gate compares against `Manifest::frame_ceiling`.
    /// Computed once per segment and maintained incrementally by
    /// [`upsert`](Self::upsert) and [`delete`](Self::delete), so repeated
    /// edits into the same leaf pay O(1) here instead of O(entries).
    pub fn total_weight(&mut self) -> usize {
        match self.weight {
            Some(weight) => weight,
            None => {
                let weight = self.entries.iter().map(Entry::weight).sum();
                self.weight = Some(weight);
                weight
            }
        }
    }

    /// Inserts or replaces the entry for `entry.key`, keeping the cached
    /// total weight exact: a replacement adjusts by the weight delta, an
    /// insert adds the entry's weight.
    pub fn upsert(&mut self, entry: Entry<Key, Value>) {
        match self.entries.binary_search_by(|e| e.key.cmp(&entry.key)) {
            Ok(at) => {
                if let Some(weight) = self.weight.as_mut() {
                    *weight = *weight + entry.weight() - self.entries[at].weight();
                }
                self.entries[at].value = entry.value;
            }
            Err(at) => {
                if let Some(weight) = self.weight.as_mut() {
                    *weight += entry.weight();
                }
                self.entries.insert(at, entry);
            }
        }
    }

    /// Removes the entry for `key` (a missing key is a no-op), keeping the
    /// cached total weight exact.
    pub fn delete(&mut self, key: &Key) {
        if let Ok(at) = self.entries.binary_search_by(|e| e.key.cmp(key)) {
            if let Some(weight) = self.weight.as_mut() {
                *weight -= self.entries[at].weight();
            }
            self.entries.remove(at);
        }
    }
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
                Ok(TransientNode::Segment(TransientSegment::new(
                    entries, separator,
                )))
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
        // Measurement-only (uncommitted, env-gated): classify the node being
        // sealed so a duplicate store downstream can be attributed to a kind.
        let audit_kind = if dialog_storage::dup_audit::enabled() {
            Some(match &self {
                TransientNode::Segment(_) => "segment",
                TransientNode::Index(index) if index.novelty.is_empty() => "index-quiet",
                TransientNode::Index(_) => "index-buffered",
            })
        } else {
            None
        };
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
        crate::distribution::audit::node(node.buffer().as_ref().len());
        if let Some(kind) = audit_kind {
            dialog_storage::dup_audit::note_seal(node.hash().as_bytes(), kind);
        }
        delta.add(node.hash().clone(), node.buffer().clone());
        Ok(node)
    }

    /// Serializes this transient node into a [`PersistentNode`] WITHOUT
    /// consuming it, so the live spine survives the persist and the next
    /// write appends to it instead of re-opening the frame from bytes.
    ///
    /// Semantically identical to [`persist`](Self::persist) — same codec,
    /// same bytes, same hash — with two retention rules: transient CHILDREN
    /// are persisted and collapsed back to [`Node::Persistent`] links
    /// (children are touched only by amortized cascades, so keeping them
    /// live would re-encode untouched frames on every subsequent persist),
    /// and open novelty buffers are re-sealed in place with the encoding
    /// they just produced (see [`Novelty::persist_buffers`]), so only links a
    /// later write touches pay a fresh encode next time.
    pub fn persist_mut(
        &mut self,
        delta: &mut Delta<Blake3Hash, Buffer>,
        manifest: &Manifest,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        let audit_kind = if dialog_storage::dup_audit::enabled() {
            Some(match &self {
                TransientNode::Segment(_) => "segment",
                TransientNode::Index(index) if index.novelty.is_empty() => "index-quiet",
                TransientNode::Index(_) => "index-buffered",
            })
        } else {
            None
        };
        let body = match self {
            TransientNode::Segment(segment) => {
                PersistentNodeBody::segment_from_entries(segment.entries().to_vec(), *manifest)?
            }
            TransientNode::Index(TransientIndex { children, novelty }) => {
                // Collapse any live (cascade-touched) child back to its
                // persisted link; an untouched persistent child passes
                // through with no re-encode, exactly as in `persist`.
                for child in children.iter_mut() {
                    if matches!(child, Node::Transient(_)) {
                        let lifted = std::mem::replace(
                            child,
                            Node::Transient(TransientNode::Segment(TransientSegment::new(
                                Vec::new(),
                                Vec::new(),
                            ))),
                        );
                        *child = Node::Persistent(lifted.into_link(delta, manifest)?);
                    }
                }
                let links = children
                    .iter()
                    .map(|child| match child {
                        Node::Persistent(link) => Ok(link.clone()),
                        Node::Transient(_) => Err(DialogSearchTreeError::Node(
                            "A transient child survived link collapse".into(),
                        )),
                    })
                    .collect::<Result<Vec<Link>, DialogSearchTreeError>>()?;
                let buffers = novelty.persist_buffers::<Key>(&links)?;
                PersistentNodeBody::index_from_buffers(links, buffers, *manifest)?
            }
        };

        let node = PersistentNode::new(Buffer::from(body.as_bytes()?));
        crate::distribution::audit::node(node.buffer().as_ref().len());
        if let Some(kind) = audit_kind {
            dialog_storage::dup_audit::note_seal(node.hash().as_bytes(), kind);
        }
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
    regroup_children_reusing::<Key, Value, D>(children, level, manifest, &[])
}

/// The provenance of one untouched index piece inside a widened frame regroup:
/// the child-index range it contributed to the merged frame and the stored
/// link it came from. When the regroup reproduces exactly that range as a
/// group (and derives the same separator), the piece's persistent link is
/// passed through verbatim — no re-encode, no re-hash, no re-store of a
/// byte-identical index block, and no delta entry.
///
/// This is the index-level twin of [`PieceOrigin`]: the frame ceiling force-
/// splits an over-ceiling INDEX frame by re-grouping the whole widened frame,
/// and the widening (`merge_forced_index_runs`) concatenates every piece's
/// children before the regroup re-cuts them. Most pieces come back
/// boundary-and-child-identical; without provenance each was rebuilt into a
/// fresh transient index and re-stored as a byte-identical block. Provenance
/// is the only sound signal here — store identity is not observable at persist
/// time.
///
/// Only QUIET pieces (empty sealed novelty) are eligible: the widening drains
/// every piece's buffer and re-routes it over the merged children, so a piece
/// that carried ops no longer owns them and its original link would double-
/// count. In the canonical edit path every lifted index node is quiet, so this
/// covers the whole force-split frame; a buffered piece simply rebuilds as
/// before.
#[derive(Clone, Debug)]
pub(crate) struct IndexPieceOrigin {
    /// First child index (inclusive) this piece contributed to the merged frame.
    pub start: usize,
    /// One past the last child index this piece contributed.
    pub end: usize,
    /// The stored link the piece was opened from.
    pub link: Link,
}

/// [`regroup_children`] with piece provenance: any produced group that exactly
/// reproduces an untouched origin piece — same child range, same derived
/// separator — is emitted as its original persistent link instead of a fresh
/// transient index. See [`IndexPieceOrigin`].
pub(crate) fn regroup_children_reusing<Key, Value, D>(
    children: Vec<Node<Key, Value>>,
    level: Rank,
    manifest: &Manifest,
    origins: &[IndexPieceOrigin],
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

    // Coin pass: a child whose seam rank exceeds the level threshold starts
    // a new group. Decisions are collected first so the ceiling overlay can
    // see the whole frame structure before any child moves.
    let mut cut_before = vec![false; children.len()];
    for (at, child) in children.iter().enumerate() {
        cut_before[at] = at > 0 && D::seam_rank(child.separator()?, manifest) > threshold;
    }

    // The index-level frame ceiling: under byte pacing, a frame (the run of
    // children between this level's coin cuts) whose summed link weight
    // exceeds the ceiling is force-split at elected seams, exactly like the
    // leaf machinery — bounding the seam ladder's own exponential tail (a
    // root that drew no cuts would otherwise accumulate every link).
    //
    // Unlike leaf anchors, index anchors need NO stored mark: whether a
    // stored seam is a coin cut is a pure function of its separator (the
    // ladder), so a forced seam self-identifies as "split here, but the
    // ladder says no cut" and can never be promoted at the next level —
    // quietness and piece contiguity are derived, and any later regroup of
    // the parent's children (always a whole-frame window, since frames stay
    // contiguous under one parent) re-derives the same anchors.
    if manifest.max_segment > 0 && manifest.frame_ceiling() > 0 {
        let ceiling = manifest.frame_ceiling();
        let mut weights = Vec::with_capacity(children.len());
        for child in &children {
            weights.push(cap::link_weight(child.separator()?));
        }
        let mut separators: Vec<&[u8]> = Vec::with_capacity(children.len());
        for child in &children {
            separators.push(child.separator()?);
        }
        let mut start = 0usize;
        for end in 0..children.len() {
            let closes_frame = end + 1 == children.len() || cut_before[end + 1];
            if !closes_frame {
                continue;
            }
            if end > start {
                for cut in cap::index_frame_cut_positions(
                    &separators[start..=end],
                    &weights[start..=end],
                    ceiling,
                    manifest,
                ) {
                    cut_before[start + cut] = true;
                }
            }
            start = end + 1;
        }
    }

    let mut groups: Vec<Node<Key, Value>> = vec![];
    let mut pending: Vec<Node<Key, Value>> = vec![];

    let origin_for = |start: usize, end: usize| -> Option<&Link> {
        origins
            .iter()
            .find(|origin| origin.start == start && origin.end == end)
            .map(|origin| &origin.link)
    };
    // A group that exactly reproduces an untouched origin piece — same child
    // range and same derived separator (the first child's) — passes the
    // original link straight through: `into_link` hands it on with no encode,
    // no hash, and no delta entry. Otherwise a fresh canonical index node is
    // built; its novelty starts empty (a regrouped node is fresh, and a flush
    // is what moves buffered ops downward).
    let mut seal = |start: usize, end: usize, pending: Vec<Node<Key, Value>>| {
        if let Some(link) = origin_for(start, end)
            && pending
                .first()
                .and_then(|child| child.separator().ok())
                .is_some_and(|separator| separator == link.separator.as_slice())
        {
            groups.push(Node::Persistent(link.clone()));
            return;
        }
        groups.push(
            TransientNode::Index(TransientIndex {
                children: pending,
                novelty: Novelty::new(),
            })
            .into(),
        );
    };

    let mut group_start = 0usize;
    for (at, child) in children.into_iter().enumerate() {
        if cut_before[at] && !pending.is_empty() {
            seal(group_start, at, std::mem::take(&mut pending));
            group_start = at;
        }
        pending.push(child);
    }

    if !pending.is_empty() {
        let end = group_start + pending.len();
        seal(group_start, end, pending);
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
/// The provenance of one untouched piece inside a widened regroup window:
/// the entry range it contributed to the merged window and the stored link
/// it came from. When the regroup reproduces exactly that range as a group
/// (and derives the same separator), the piece's persistent link is passed
/// through verbatim — no re-encode, no re-hash, no re-store of a
/// byte-identical block. Provenance is the only sound signal here: store
/// identity is not observable at persist time.
#[derive(Clone, Debug)]
pub(crate) struct PieceOrigin {
    /// First entry index (inclusive) this piece contributed.
    pub start: usize,
    /// One past the last entry index this piece contributed.
    pub end: usize,
    /// The stored link the piece was opened from.
    pub link: Link,
}

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
    regroup_entries_reusing::<Key, Value, D>(entries, floor, manifest, &[])
}

/// [`regroup_entries`] with piece provenance: any group that exactly
/// reproduces an untouched origin piece — same entry range, same derived
/// separator — is emitted as its original persistent link instead of a
/// fresh transient segment. See [`PieceOrigin`].
pub(crate) fn regroup_entries_reusing<Key, Value, D>(
    entries: Vec<Entry<Key, Value>>,
    floor: Vec<u8>,
    manifest: &Manifest,
    origins: &[PieceOrigin],
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
    // Every pacing decision below meters entries by their full weight (key
    // bytes plus the value's payload weight), computed once per window.
    let weights: Vec<usize> = if manifest.max_segment == 0 {
        Vec::new()
    } else {
        entries.iter().map(Entry::weight).collect()
    };
    let mut vetoed = vec![false; count.saturating_sub(1)];
    let mut cut_after = vec![false; count];
    let mut bank = 0usize;
    // The pacing-pressure ramp (prototype, see
    // `Manifest::pacing_ramp_threshold`): the weight of the frame built so
    // far — everything since the last coin-accepted cut — in excess of the
    // threshold rides every subsequent coin, so P(cut) approaches 1 before
    // the frame can reach the ceiling. This is deliberately
    // outcome-dependent context (the frame resets at cuts), unlike the
    // bank, which is seam-structural; the edit path's fusion machinery is
    // what re-decides across a boundary this moves.
    let ramp = manifest.pacing_ramp_threshold();
    let mut frame_weight = 0usize;
    for at in 0..count.saturating_sub(1) {
        let key = entries[at].key.as_ref();
        vetoed[at] = D::vetoes(key, entries[at + 1].key.as_ref(), manifest);
        if manifest.max_segment > 0 {
            frame_weight += weights[at];
        }
        if vetoed[at] {
            // The coin is skipped entirely for vetoed seams: the veto
            // overrides whatever it would say, and the weight moves into
            // the bank instead.
            if manifest.max_segment > 0 {
                bank += weights[at];
            }
        } else {
            let weight = if manifest.max_segment == 0 {
                0
            } else {
                let excess = if ramp > 0 {
                    frame_weight.saturating_sub(ramp)
                } else {
                    0
                };
                bank + weights[at] + excess
            };
            cut_after[at] = D::leaf_cut(key, weight, manifest);
            bank = 0;
            if cut_after[at] {
                frame_weight = 0;
            }
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
            for cut in cap::forced_cut_positions(&keys, &weights[start..=at], manifest) {
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
                for cut in cap::frame_cut_positions(&keys, &weights[start..=end], seams, manifest) {
                    cut_after[start + cut - 1] = true;
                    forced_start[start + cut] = true;
                }
            }
            start = end + 1;
        }
    }

    let origin_for = |start: usize, end: usize| -> Option<&Link> {
        origins
            .iter()
            .find(|origin| origin.start == start && origin.end == end)
            .map(|origin| &origin.link)
    };
    // The per-entry weights are already in hand, so each sealed group's
    // exact total rides into the segment's weight cache: a freshly
    // regrouped leaf then answers the edit path's frame-ceiling gate
    // without re-summing its entries.
    let group_weight = |start: usize, end: usize| -> Option<usize> {
        (manifest.max_segment > 0).then(|| weights[start..end].iter().sum())
    };
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
                origin_for(group_start, at + 1),
                group_weight(group_start, at + 1),
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
            origin_for(group_start, count),
            group_weight(group_start, count),
        );
    }

    groups
}

/// Seals one group of entries into a segment, deriving its left-edge
/// separator: from the floor for the very first group of a regroup, the
/// long forced form when the group starts at a backstop anchor, and the
/// canonical shortest-distinguishing prefix against the previous group's
/// last key everywhere else.
///
/// `weight`, when given, must be the exact sum of the group's entry weights
/// (the caller sums the same per-entry weights its cut decisions read); it
/// seeds the sealed segment's weight cache.
#[allow(clippy::too_many_arguments)]
fn seal<Key, Value, D>(
    pending: &mut Vec<Entry<Key, Value>>,
    previous_last: &mut Option<Key>,
    groups: &mut Vec<Node<Key, Value>>,
    floor: &[u8],
    forced: bool,
    manifest: &Manifest,
    origin: Option<&Link>,
    weight: Option<usize>,
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
        // A window-start floor that is itself a forced mark (longer than
        // the separator bound) is preserved verbatim: it is a valid
        // separator for any minimum the window can hold (routing keeps
        // `min >= floor`), and re-deriving through `reseparate` would
        // collapse it to the short natural prefix — stripping the
        // self-identifying mark that lets an edit rejoin the force-split
        // run. Reachable when a non-membership edit (a value update)
        // re-shapes a forced piece locally, which the widening does not
        // intercept.
        None if floor.len() > manifest.max_separator as usize => floor.to_vec(),
        None => D::reseparate(first.as_ref(), floor),
        Some(previous) if forced => {
            cap::forced_seam_separator(previous.as_ref(), first.as_ref(), manifest)
        }
        Some(previous) => D::separator(previous.as_ref(), first.as_ref()),
    };
    *previous_last = Some(last);
    // A group that reproduces an untouched origin piece byte-for-byte —
    // same entries (the exact-range match) and same separator — passes the
    // original link through: `into_link` will hand it on with no encode,
    // no hash, and no delta entry.
    if let Some(link) = origin
        && link.separator == separator
    {
        groups.push(Node::Persistent(link.clone()));
        return;
    }
    #[cfg(test)]
    if let Some(weight) = weight {
        debug_assert_eq!(
            weight,
            entries.iter().map(Entry::weight).sum::<usize>(),
            "a sealed group's cached weight must equal the sum of its entry weights"
        );
    }
    groups.push(
        TransientNode::Segment(TransientSegment {
            entries,
            separator,
            weight,
        })
        .into(),
    );
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

        // Byte pacing off: this pins the pure geometric coin's segment cuts,
        // independent of the shipped `max_segment` default (which would pack
        // these tiny entries into one segment and hide the boundaries).
        let manifest = Manifest {
            max_segment: 0,
            frame_ceiling_factor: 0,
            ..Manifest::default()
        };
        regroup_entries::<[u8; 4], Vec<u8>, Geometric>(entries, Vec::new(), &manifest)
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
