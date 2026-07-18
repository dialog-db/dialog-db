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
    ArchivedNodeBody, Buffer, Delta, DialogSearchTreeError, Distribution, Entry, Key, Link,
    Manifest, Node, PersistentNode, PersistentNodeBody, Rank, Value, into_owned,
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
///
/// An index carries no separator of its own: its separator is by definition
/// its first child's separator (the seam at any node's left edge is the seam
/// at its leftmost leaf's left edge), so it is derived on demand via
/// [`TransientNode::separator`].
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
            .links()?
            .into_iter()
            .map(Node::Persistent)
            .collect::<Vec<Node<Key, Value>>>();
        Ok(TransientIndex { children })
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
                        key: Key::try_from_bytes(&key)?,
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
                    .collect::<Result<Vec<Link>, DialogSearchTreeError>>()?;
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

    for child in children {
        let rank = D::seam_rank(child.separator()?, manifest);
        if rank > threshold && !pending.is_empty() {
            groups.push(
                TransientNode::Index(TransientIndex {
                    children: std::mem::take(&mut pending),
                })
                .into(),
            );
        }
        pending.push(child);
    }

    if !pending.is_empty() {
        groups.push(TransientNode::Index(TransientIndex { children: pending }).into());
    }

    Ok(groups)
}

/// Regroups an ordered list of entries into leaf segments by the canonical cut
/// rule at level 0 (threshold [`BOTTOM_RANK`]): a segment ends at the first
/// entry whose leaf-coin rank exceeds the threshold.
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

    let seal = |pending: &mut Vec<Entry<Key, Value>>,
                previous_last: &mut Option<Key>,
                groups: &mut Vec<Node<Key, Value>>| {
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
            None => D::reseparate(first.as_ref(), &floor),
            Some(previous) => D::separator(previous.as_ref(), first.as_ref()),
        };
        *previous_last = Some(last);
        groups.push(TransientNode::Segment(TransientSegment { entries, separator }).into());
    };

    for entry in entries {
        let rank = D::rank(entry.key.as_ref(), manifest);
        pending.push(entry);
        if rank > BOTTOM_RANK {
            seal(&mut pending, &mut previous_last, &mut groups);
        }
    }

    if !pending.is_empty() {
        seal(&mut pending, &mut previous_last, &mut groups);
    }

    groups
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
