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

use crate::{
    ArchivedNodeBody, Buffer, Delta, DialogSearchTreeError, Distribution, Entry, Key, Link, Node,
    PersistentNode, PersistentNodeBody, Rank, SymmetryWith, Value, into_owned,
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

/// An index node holding live child nodes.
#[derive(Debug)]
pub struct TransientIndex<Key, Value> {
    /// The child nodes, each persistent or transient.
    pub children: Vec<Node<Key, Value>>,
}

/// A leaf segment holding live key-value entries.
#[derive(Debug)]
pub struct TransientSegment<Key, Value> {
    /// The key-value entries stored in this segment.
    pub entries: Vec<Entry<Key, Value>>,
}

impl<Key, Value> TransientIndex<Key, Value>
where
    Key: self::Key,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Returns the upper bound key of this index, the bound of its last child.
    ///
    /// Errors if the index is empty (which violates the node invariant) or if a
    /// persistent child's bound cannot be recovered.
    pub fn upper_bound(&self) -> Result<Key, DialogSearchTreeError> {
        self.children
            .last()
            .ok_or_else(|| DialogSearchTreeError::Node("Index was unexpectedly empty".into()))?
            .upper_bound()
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
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
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
        let children = node
            .as_index()?
            .links
            .iter()
            .map(|link| Ok(Node::Persistent(into_owned::<Link<Key>>(link)?)))
            .collect::<Result<Vec<Node<Key, Value>>, DialogSearchTreeError>>()?;
        Ok(TransientIndex { children })
    }
}

/// Opens a [`PersistentNode`] one level into its editable [`TransientNode`]
/// form: an index becomes a [`TransientIndex`] whose children stay
/// [`Node::Persistent`] references, and a segment becomes a [`TransientSegment`]
/// with its entries decoded to owned form. Deeper nodes are opened lazily as
/// edits descend.
impl<Key, Value> TryFrom<&PersistentNode<Key, Value>> for TransientNode<Key, Value>
where
    Key: self::Key,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    type Error = DialogSearchTreeError;

    fn try_from(node: &PersistentNode<Key, Value>) -> Result<Self, Self::Error> {
        match node.body()? {
            ArchivedNodeBody::Index(_) => {
                Ok(TransientNode::Index(TransientNode::open_index(node)?))
            }
            ArchivedNodeBody::Segment(segment) => {
                let entries = segment
                    .entries
                    .iter()
                    .map(into_owned)
                    .collect::<Result<Vec<Entry<Key, Value>>, DialogSearchTreeError>>()?;
                Ok(TransientNode::Segment(TransientSegment { entries }))
            }
        }
    }
}

impl<Key, Value> TransientNode<Key, Value>
where
    Key: self::Key
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
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
    /// link), so the index's body holds only links. This makes no shape
    /// decisions: the children and entries are encoded exactly as the edits
    /// left them.
    pub fn persist(
        self,
        delta: &mut Delta<Blake3Hash, Buffer>,
    ) -> Result<PersistentNode<Key, Value>, DialogSearchTreeError> {
        let body = match self {
            TransientNode::Segment(segment) => PersistentNodeBody::try_from(segment.entries)?,
            TransientNode::Index(index) => {
                let links = index
                    .children
                    .into_iter()
                    .map(|child| child.into_link(delta))
                    .collect::<Result<Vec<Link<Key>>, DialogSearchTreeError>>()?;
                PersistentNodeBody::try_from(links)?
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
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
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

/// Groups a flat, ordered list of children into nodes by the canonical cut
/// rule: a group ends at the first child whose rank exceeds `level_threshold`.
///
/// `level_threshold` is `BOTTOM_RANK + level` for the level being built. This
/// is the canonical cut rule applied while shaping a level; here it returns the
/// children partitioned into groups (the leftmost-to-rightmost runs) rather than
/// serialized nodes, so callers can wrap each run in the appropriate transient
/// node.
pub(crate) fn group_by_rank<Child>(
    children: Vec<(Child, Rank)>,
    level_threshold: Rank,
) -> Vec<Vec<Child>> {
    let mut groups: Vec<Vec<Child>> = vec![];
    let mut pending: Vec<Child> = vec![];

    for (child, rank) in children {
        pending.push(child);
        if rank > level_threshold {
            groups.push(std::mem::take(&mut pending));
        }
    }

    if !pending.is_empty() {
        groups.push(pending);
    }

    groups
}

/// Computes the rank of a child node from its upper-bound key.
pub(crate) fn rank_of_node<Key, Value, D>(
    node: &Node<Key, Value>,
) -> Result<Rank, DialogSearchTreeError>
where
    Key: self::Key,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    let bound = node.upper_bound()?;
    Ok(D::rank(bound.as_ref()))
}

/// Regroups an ordered list of child nodes into index nodes by the canonical
/// cut rule for the given `level` (its threshold is `BOTTOM_RANK + level`).
///
/// Each child is ranked by its upper-bound key; a group ends at the first child
/// whose rank exceeds the threshold. Returns one [`Node::Transient`] index per
/// group. This is the index-level analogue of regrouping a segment's entries,
/// applied after an edit changes a node's child list.
pub(crate) fn regroup_children<Key, Value, D>(
    children: Vec<Node<Key, Value>>,
    level: Rank,
) -> Result<Vec<Node<Key, Value>>, DialogSearchTreeError>
where
    Key: self::Key,
    Key::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Key, Strategy<Pool, rkyv::rancor::Error>>
        + PartialOrd<Key>
        + PartialEq<Key>
        + SymmetryWith<Key>
        + Ord,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    D: Distribution,
{
    let ranked = children
        .into_iter()
        .map(|child| {
            let rank = rank_of_node::<Key, Value, D>(&child)?;
            Ok((child, rank))
        })
        .collect::<Result<Vec<(Node<Key, Value>, Rank)>, DialogSearchTreeError>>()?;

    Ok(group_by_rank(ranked, BOTTOM_RANK + level)
        .into_iter()
        .map(|group| TransientNode::Index(TransientIndex { children: group }).into())
        .collect())
}

/// Regroups an ordered list of entries into leaf segments by the canonical cut
/// rule at level 0 (threshold [`BOTTOM_RANK`]).
///
/// Each entry is ranked by its key; a group ends at the first entry whose rank
/// exceeds the threshold. Returns one [`Node::Transient`] segment per group.
pub(crate) fn regroup_entries<Key, Value, D>(
    entries: Vec<Entry<Key, Value>>,
) -> Vec<Node<Key, Value>>
where
    Key: self::Key,
    Key::Archived: PartialOrd<Key> + PartialEq<Key> + SymmetryWith<Key> + Ord,
    Value: self::Value,
    D: Distribution,
{
    let ranked = entries
        .into_iter()
        .map(|entry| {
            let rank = D::rank(entry.key.as_ref());
            (entry, rank)
        })
        .collect::<Vec<(Entry<Key, Value>, Rank)>>();

    group_by_rank(ranked, BOTTOM_RANK)
        .into_iter()
        .map(|group| TransientNode::Segment(TransientSegment { entries: group }).into())
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(unexpected_cfgs)]

    use anyhow::Result;
    use dialog_common::Blake3Hash;

    use super::{BOTTOM_RANK, regroup_entries};
    use crate::{Entry, Geometric, Rank, distribution};

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

        regroup_entries::<[u8; 4], Vec<u8>, Geometric>(entries)
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
