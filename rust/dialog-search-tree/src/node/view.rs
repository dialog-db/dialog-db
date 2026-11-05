//! What the tree reads a node through, whichever way the node is held: as
//! its archived block, or as rows a store keeps it in ([`NodeRows`]).
//!
//! Every view answers the same questions both ways. Over a block it reads
//! the archived index or segment in the block's bytes; over rows it reads
//! the store's rows in place: keys part by part, values as archives where
//! the store keeps them, children by the hash the store names.

use std::cmp::Ordering;
use std::ops::{Bound, Range};

use dialog_common::Blake3Hash;
use rkyv::{
    Deserialize,
    bytecheck::CheckBytes,
    de::Pool,
    rancor::Strategy,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::node::columnar::StreamingLeaf;
use crate::node::rows::{NodeRows, archived, out_of_range};
use crate::{
    ArchivedIndex, ArchivedNoveltyBuffer, ArchivedSegment, DialogSearchTreeError, Key, Link,
    NoveltyEntry, NoveltyOp, Scale, Value, into_owned,
};

/// A streaming decoder over keys in entry order, yielding each key's
/// position and bytes. Each key is written into one buffer the cursor
/// reuses, so a scan allocates nothing per key.
pub enum KeyCursor<'a> {
    /// Keys of a block's columnar segment or buffer.
    Columnar(StreamingLeaf<'a>),
    /// Keys a store keeps as parts, put together one at a time.
    Rows {
        /// Where the keys are.
        rows: &'a dyn NodeRows,
        /// The positions left to read.
        positions: Range<usize>,
        /// What the cursor takes off each position it reports: a buffer's
        /// first operation is its position 0.
        start: usize,
        /// Whether the positions are buffered operations' rather than
        /// entries'.
        ops: bool,
        /// The key just read.
        scratch: Vec<u8>,
    },
}

impl<'a> KeyCursor<'a> {
    /// A cursor over `positions` of `rows`, reporting each position less
    /// `start`.
    fn rows(rows: &'a dyn NodeRows, positions: Range<usize>, start: usize, ops: bool) -> Self {
        KeyCursor::Rows {
            rows,
            start,
            positions,
            ops,
            scratch: Vec::new(),
        }
    }

    /// The next key and its position, or `None` past the last.
    pub fn next_key(&mut self) -> Result<Option<(usize, &[u8])>, DialogSearchTreeError> {
        match self {
            KeyCursor::Columnar(leaf) => leaf.next_key(),
            KeyCursor::Rows {
                rows,
                positions,
                start,
                ops,
                scratch,
            } => {
                let Some(at) = positions.next() else {
                    return Ok(None);
                };
                let key = if *ops { rows.op_key(at) } else { rows.key(at) };
                key.write_into(scratch);
                Ok(Some((at - *start, scratch.as_slice())))
            }
        }
    }
}

/// An index node, read from its block or from rows.
pub enum IndexView<'a, Value: rkyv::Archive> {
    /// The archived index in a block.
    Archived(&'a ArchivedIndex<Value>),
    /// An index a store keeps as rows.
    Rows(&'a dyn NodeRows),
}

impl<Value: rkyv::Archive> Clone for IndexView<'_, Value> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Value: rkyv::Archive> Copy for IndexView<'_, Value> {}

/// A segment node, read from its block or from rows.
pub enum SegmentView<'a, Value: rkyv::Archive> {
    /// The archived segment in a block.
    Archived(&'a ArchivedSegment<Value>),
    /// A segment a store keeps as rows.
    Rows(&'a dyn NodeRows),
}

impl<Value: rkyv::Archive> Clone for SegmentView<'_, Value> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<Value: rkyv::Archive> Copy for SegmentView<'_, Value> {}

/// The buffer of operations riding one link of an index.
pub enum BufferView<'a, Value: rkyv::Archive> {
    /// The archived buffer in a block.
    Archived(&'a ArchivedNoveltyBuffer<Value>),
    /// A buffer a store keeps as rows: the link it rides and its
    /// operations' positions.
    Rows {
        /// Where the operations are.
        rows: &'a dyn NodeRows,
        /// The link the buffer rides.
        child: u32,
        /// Its operations' positions.
        ops: Range<usize>,
    },
}

impl<Value: rkyv::Archive> Clone for BufferView<'_, Value> {
    fn clone(&self) -> Self {
        match self {
            BufferView::Archived(buffer) => BufferView::Archived(buffer),
            BufferView::Rows { rows, child, ops } => BufferView::Rows {
                rows: *rows,
                child: *child,
                ops: ops.clone(),
            },
        }
    }
}

impl<'a, Value: rkyv::Archive> IndexView<'a, Value> {
    /// Number of children in this index.
    pub fn len(&self) -> usize {
        match self {
            IndexView::Archived(index) => index.len(),
            IndexView::Rows(rows) => rows.len(),
        }
    }

    /// Whether this index holds no children.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn check(&self, at: usize) -> Result<(), DialogSearchTreeError> {
        if at < self.len() {
            Ok(())
        } else {
            Err(out_of_range("Index child"))
        }
    }

    /// The full separator of the child at `at`.
    pub fn separator(&self, at: usize) -> Result<Vec<u8>, DialogSearchTreeError> {
        match self {
            IndexView::Archived(index) => index.separator(at),
            IndexView::Rows(rows) => {
                self.check(at)?;
                Ok(rows.separator(at).to_vec())
            }
        }
    }

    /// The content hash of the child at `at`.
    pub fn hash_at(&self, at: usize) -> Result<&'a Blake3Hash, DialogSearchTreeError> {
        match self {
            IndexView::Archived(index) => index.hash_at(at),
            IndexView::Rows(rows) => {
                self.check(at)?;
                Ok(rows.child(at))
            }
        }
    }

    /// The rough size of the child subtree at `at`, for query planning.
    pub fn scale_at(&self, at: usize) -> Result<Scale, DialogSearchTreeError> {
        match self {
            IndexView::Archived(index) => index.scale_at(at),
            IndexView::Rows(rows) => {
                self.check(at)?;
                Ok(Scale::from_u8(rows.scale(at)))
            }
        }
    }

    /// Every child's scale, in child order.
    pub fn scales(&self) -> impl Iterator<Item = Scale> + 'a {
        let view = *self;
        (0..view.len()).map(move |at| view.scale_at(at).expect("a child in range"))
    }

    /// The child at `at`, materialized as an owned [`Link`].
    pub fn link_at(&self, at: usize) -> Result<Link, DialogSearchTreeError> {
        Ok(Link {
            separator: self.separator(at)?.into(),
            node: self.hash_at(at)?.clone(),
            scale: self.scale_at(at)?,
        })
    }

    /// All children, materialized as owned [`Link`]s in child order.
    pub fn links(&self) -> Result<Vec<Link>, DialogSearchTreeError> {
        (0..self.len()).map(|at| self.link_at(at)).collect()
    }

    /// Whether any child's content hash equals `hash`.
    pub fn contains_hash(&self, hash: &Blake3Hash) -> bool {
        match self {
            IndexView::Archived(index) => index.contains_hash(hash),
            IndexView::Rows(rows) => (0..rows.len()).any(|at| rows.child(at) == hash),
        }
    }

    /// Number of children of `rows` whose separator is at or below `key`.
    fn children_at_or_below(rows: &dyn NodeRows, key: &[u8]) -> usize {
        let (mut low, mut high) = (0, rows.len());
        while low < high {
            let middle = (low + high) / 2;
            if rows.separator(middle) <= key {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    /// Index of the child whose subtree covers `key`: the last child whose
    /// separator is at or below the key, clamped to the leftmost.
    pub fn route(&self, key: &[u8]) -> Result<usize, DialogSearchTreeError> {
        match self {
            IndexView::Archived(index) => index.route(key),
            IndexView::Rows(rows) => Ok(Self::children_at_or_below(*rows, key).saturating_sub(1)),
        }
    }

    /// The range of child indices whose subtrees can hold a key in the
    /// half-open range `[lower, upper)`: a conservative superset (see
    /// [`ArchivedIndex::children_spanning`]).
    pub fn children_spanning(
        &self,
        lower: &[u8],
        upper: &[u8],
    ) -> Result<Range<usize>, DialogSearchTreeError> {
        match self {
            IndexView::Archived(index) => index.children_spanning(lower, upper),
            IndexView::Rows(rows) => {
                if self.is_empty() || upper <= lower {
                    return Ok(0..0);
                }
                let start = self.route(lower)?;
                let below_upper = Self::children_at_or_below(*rows, upper);
                let end = if rows.separator(below_upper.saturating_sub(1)) == upper {
                    below_upper.saturating_sub(1)
                } else {
                    below_upper
                };
                Ok(start..end.max(start + 1))
            }
        }
    }

    /// The exclusive index of the last child a scan bounded by `end` can
    /// visit.
    pub fn children_within(&self, end: Bound<&[u8]>) -> Result<usize, DialogSearchTreeError> {
        match self {
            IndexView::Archived(index) => index.children_within(end),
            IndexView::Rows(rows) => Ok(match end {
                Bound::Unbounded => self.len(),
                Bound::Included(end) => Self::children_at_or_below(*rows, end),
                Bound::Excluded(end) => {
                    let below = Self::children_at_or_below(*rows, end);
                    if below > 0 && rows.separator(below - 1) == end {
                        below - 1
                    } else {
                        below
                    }
                }
            }),
        }
    }

    /// A rough estimate of how many entries in this node's subtrees fall in
    /// `[lower, upper)`, read from this node alone.
    pub fn range_scale(&self, lower: &[u8], upper: &[u8]) -> Result<Scale, DialogSearchTreeError> {
        let children = self.children_spanning(lower, upper)?;
        let mut scales = Vec::with_capacity(children.len());
        for at in children {
            scales.push(self.scale_at(at)?);
        }
        Ok(Scale::total(scales))
    }

    /// Every buffer of operations, in child order.
    pub fn buffers(&self) -> impl Iterator<Item = BufferView<'a, Value>> + 'a {
        let view = *self;
        let count = match view {
            IndexView::Archived(index) => index.novelty.len(),
            IndexView::Rows(rows) => rows.buffers(),
        };
        (0..count).map(move |which| match view {
            IndexView::Archived(index) => BufferView::Archived(&index.novelty[which]),
            IndexView::Rows(rows) => {
                let (child, ops) = rows.buffer(which);
                BufferView::Rows { rows, child, ops }
            }
        })
    }

    /// Total number of buffered ops across every link's buffer.
    pub fn novelty_len(&self) -> usize {
        match self {
            IndexView::Archived(index) => index.novelty_len(),
            IndexView::Rows(rows) => (0..rows.buffers())
                .map(|which| rows.buffer(which).1.len())
                .sum(),
        }
    }

    /// The buffer riding the child link at `at`, if any.
    pub fn buffer_for(&self, at: usize) -> Option<BufferView<'a, Value>> {
        match self {
            IndexView::Archived(index) => index.buffer_for(at).map(BufferView::Archived),
            IndexView::Rows(_) => self.buffers().find(|buffer| buffer.child() as usize == at),
        }
    }

    /// Whether any buffered key satisfies `predicate`.
    pub fn any_novelty_key<Key>(
        &self,
        mut predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<bool, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        match self {
            IndexView::Archived(index) => index.any_novelty_key::<Key>(predicate),
            IndexView::Rows(_) => {
                for buffer in self.buffers() {
                    if buffer.any_key::<Key>(&mut predicate)? {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }
}

impl<'a, Value> IndexView<'a, Value>
where
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// The decoded ops riding the child link at `at`, in buffer order.
    pub fn link_novelty<Key>(
        &self,
        at: usize,
    ) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        match self.buffer_for(at) {
            Some(buffer) => buffer.entries::<Key>(),
            None => Ok(Vec::new()),
        }
    }

    /// Every buffered op of this node, concatenated in child order.
    pub fn all_novelty<Key>(&self) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        match self {
            IndexView::Archived(index) => index.all_novelty::<Key>(),
            IndexView::Rows(_) => {
                let mut out = Vec::with_capacity(self.novelty_len());
                for buffer in self.buffers() {
                    buffer.append_entries::<Key>(&mut out)?;
                }
                Ok(out)
            }
        }
    }
}

impl<'a, Value: rkyv::Archive> BufferView<'a, Value> {
    /// The link this buffer rides.
    pub fn child(&self) -> u32 {
        match self {
            BufferView::Archived(buffer) => buffer.child.to_native(),
            BufferView::Rows { child, .. } => *child,
        }
    }

    /// The number of buffered ops.
    pub fn count(&self) -> usize {
        match self {
            BufferView::Archived(buffer) => buffer.count.to_native() as usize,
            BufferView::Rows { ops, .. } => ops.len(),
        }
    }

    /// The op count, checked against the buffer's tables.
    pub fn checked_count(&self) -> Result<usize, DialogSearchTreeError> {
        match self {
            BufferView::Archived(buffer) => buffer.checked_count(),
            BufferView::Rows { ops, .. } => Ok(ops.len()),
        }
    }

    /// Whether op `at` asserts a value (rather than retracting).
    pub fn is_assert(&self, at: usize) -> bool {
        match self {
            BufferView::Archived(buffer) => buffer.polarity.get(at).copied() == Some(1),
            BufferView::Rows { rows, ops, .. } => {
                at < ops.len() && rows.op_value(ops.start + at).is_some()
            }
        }
    }

    /// A streaming decoder over this buffer's keys, in entry order.
    pub fn keys<Key: self::Key>(&self) -> Result<KeyCursor<'a>, DialogSearchTreeError> {
        match self {
            BufferView::Archived(buffer) => Ok(KeyCursor::Columnar(buffer.keys::<Key>()?)),
            BufferView::Rows { rows, ops, .. } => {
                Ok(KeyCursor::rows(*rows, ops.clone(), ops.start, true))
            }
        }
    }

    /// Whether any key in this buffer satisfies `predicate`.
    pub fn any_key<Key>(
        &self,
        mut predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<bool, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        let mut keys = self.keys::<Key>()?;
        while let Some((_, key)) = keys.next_key()? {
            if predicate(key) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl<'a, Value> BufferView<'a, Value>
where
    Value: self::Value,
    Value::Archived: for<'b> CheckBytes<
            Strategy<Validator<ArchiveValidator<'b>, SharedValidator>, rkyv::rancor::Error>,
        > + Deserialize<Value, Strategy<Pool, rkyv::rancor::Error>>,
{
    /// The op a rows buffer holds at `at`, decoded to owned form.
    fn rows_op(rows: &dyn NodeRows, at: usize) -> Result<NoveltyOp<Value>, DialogSearchTreeError> {
        Ok(match rows.op_value(at) {
            None => NoveltyOp::Retract,
            // SAFETY: a `NodeRows` vouches for every value it hands out, and
            // the node checked its values are `Value`s when it was built.
            Some(bytes) => NoveltyOp::Assert(into_owned::<Value>(unsafe {
                archived::<Value::Archived>(bytes)
            })?),
        })
    }

    /// The op at entry `at`, decoded to owned form.
    pub fn op_at(&self, at: usize) -> Result<NoveltyOp<Value>, DialogSearchTreeError> {
        match self {
            BufferView::Archived(buffer) => buffer.op_at(at),
            BufferView::Rows { rows, ops, .. } => {
                if at >= ops.len() {
                    return Err(out_of_range("Novelty entry"));
                }
                Self::rows_op(*rows, ops.start + at)
            }
        }
    }

    /// The op at entry `at` given its value-table `slot` (the number of
    /// asserts before it), for readers that track the slot while streaming.
    pub(crate) fn op_with_slot(
        &self,
        at: usize,
        slot: usize,
    ) -> Result<NoveltyOp<Value>, DialogSearchTreeError> {
        match self {
            BufferView::Archived(buffer) => buffer.op_with_slot(at, slot),
            BufferView::Rows { .. } => self.op_at(at),
        }
    }

    /// The winning op for `key` in this buffer, or `None` when the key is
    /// not buffered here: within a key the last op wins.
    pub fn resolve<Key: self::Key>(
        &self,
        key: &[u8],
    ) -> Result<Option<NoveltyOp<Value>>, DialogSearchTreeError> {
        match self {
            BufferView::Archived(buffer) => buffer.resolve::<Key>(key),
            BufferView::Rows { rows, ops, .. } => {
                let mut winner = None;
                for at in ops.clone() {
                    match rows.op_key(at).cmp_bytes(key) {
                        Ordering::Less => {}
                        Ordering::Equal => winner = Some(at),
                        Ordering::Greater => break,
                    }
                }
                winner.map(|at| Self::rows_op(*rows, at)).transpose()
            }
        }
    }

    /// Decodes the whole buffer to owned entries, in entry order.
    pub fn entries<Key: self::Key>(
        &self,
    ) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError> {
        let mut out = Vec::with_capacity(self.count());
        self.append_entries::<Key>(&mut out)?;
        Ok(out)
    }

    /// Decodes the whole buffer to owned entries, appending onto `out`.
    pub fn append_entries<Key: self::Key>(
        &self,
        out: &mut Vec<NoveltyEntry<Value>>,
    ) -> Result<(), DialogSearchTreeError> {
        match self {
            BufferView::Archived(buffer) => buffer.append_entries::<Key>(out),
            BufferView::Rows { rows, ops, .. } => {
                for at in ops.clone() {
                    out.push(NoveltyEntry {
                        key: rows.op_key(at).to_vec(),
                        op: Self::rows_op(*rows, at)?,
                    });
                }
                Ok(())
            }
        }
    }
}

impl<'a, Value: self::Value> SegmentView<'a, Value> {
    /// Number of entries in this segment.
    pub fn len(&self) -> usize {
        match self {
            SegmentView::Archived(segment) => segment.len(),
            SegmentView::Rows(rows) => rows.len(),
        }
    }

    /// Whether this segment holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The entry count the segment claims.
    pub fn count(&self) -> usize {
        match self {
            SegmentView::Archived(segment) => segment.count.to_native() as usize,
            SegmentView::Rows(rows) => rows.len(),
        }
    }

    /// The archived value of the entry at `at`, read in place.
    pub fn value_at(&self, at: usize) -> Result<&'a Value::Archived, DialogSearchTreeError> {
        match self {
            SegmentView::Archived(segment) => segment.value_at(at),
            SegmentView::Rows(rows) => {
                if at >= rows.len() {
                    return Err(out_of_range("Segment entry"));
                }
                // SAFETY: a `NodeRows` vouches for every value it hands out,
                // and the node checked its values are `Value`s when it was
                // built.
                Ok(unsafe { archived::<Value::Archived>(rows.value(at)) })
            }
        }
    }

    /// A streaming decoder over this segment's keys, in entry order.
    pub fn keys<Key: self::Key>(&self) -> Result<KeyCursor<'a>, DialogSearchTreeError> {
        match self {
            SegmentView::Archived(segment) => Ok(KeyCursor::Columnar(segment.keys::<Key>()?)),
            SegmentView::Rows(rows) => Ok(KeyCursor::rows(*rows, 0..rows.len(), 0, false)),
        }
    }

    /// A streaming decoder over this segment's keys from the first at or
    /// above `probe`, in entry order, reporting each key's position in the
    /// segment. Over rows the cursor enters the segment by a binary search
    /// in place; over a block's columnar keys, which stream only from the
    /// start, it starts at the first entry, and the caller skips what sorts
    /// below `probe`.
    pub fn keys_from<Key: self::Key>(
        &self,
        probe: &[u8],
    ) -> Result<KeyCursor<'a>, DialogSearchTreeError> {
        match self {
            SegmentView::Archived(_) => self.keys::<Key>(),
            SegmentView::Rows(rows) => {
                let start = self.lower_bound(probe).unwrap_or(0);
                Ok(KeyCursor::rows(*rows, start..rows.len(), 0, false))
            }
        }
    }

    /// Visit this segment's entries in order, handing each key and value to
    /// `visit`.
    pub fn for_each_entry<Key, Visit>(&self, mut visit: Visit) -> Result<(), DialogSearchTreeError>
    where
        Key: self::Key,
        Visit: FnMut(&[u8], &Value::Archived) -> Result<(), DialogSearchTreeError>,
    {
        let mut keys = self.keys::<Key>()?;
        while let Some((at, key)) = keys.next_key()? {
            let value = self.value_at(at)?;
            visit(key, value)?;
        }
        Ok(())
    }

    /// The first (minimum) key of this segment.
    pub fn first_key<Key: self::Key>(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        match self {
            SegmentView::Archived(segment) => segment.first_key::<Key>(),
            SegmentView::Rows(rows) => {
                if rows.is_empty() {
                    return Err(DialogSearchTreeError::Encoding(
                        "Segment was unexpectedly empty".into(),
                    ));
                }
                Ok(rows.key(0).to_vec())
            }
        }
    }

    /// The last (maximum) key of this segment.
    pub fn last_key<Key: self::Key>(&self) -> Result<Vec<u8>, DialogSearchTreeError> {
        match self {
            SegmentView::Archived(segment) => segment.last_key::<Key>(),
            SegmentView::Rows(rows) => {
                if rows.is_empty() {
                    return Err(DialogSearchTreeError::Encoding(
                        "Segment was unexpectedly empty".into(),
                    ));
                }
                Ok(rows.key(rows.len() - 1).to_vec())
            }
        }
    }

    /// Position of the first entry whose key is at or above `probe`.
    pub fn lower_bound(&self, probe: &[u8]) -> Option<usize> {
        let SegmentView::Rows(rows) = self else {
            return None;
        };
        let (mut low, mut high) = (0, rows.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if rows.key(middle).cmp_bytes(probe) == Ordering::Less {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Some(low)
    }

    /// Position of the entry whose key equals `probe`, or `None`. Over rows
    /// this is a binary search, comparing each key part by part in place.
    pub fn find<Key: self::Key>(
        &self,
        probe: &[u8],
    ) -> Result<Option<usize>, DialogSearchTreeError> {
        match self {
            SegmentView::Archived(segment) => segment.find::<Key>(probe),
            SegmentView::Rows(rows) => {
                let at = self.lower_bound(probe).unwrap_or(0);
                Ok(
                    (at < rows.len() && rows.key(at).cmp_bytes(probe) == Ordering::Equal)
                        .then_some(at),
                )
            }
        }
    }
}
