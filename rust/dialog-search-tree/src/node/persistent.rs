use dialog_common::Blake3Hash;
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::CheckBytes,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use crate::{
    Buffer, DialogSearchTreeError, Entry, Key, Link, Manifest, Value,
    node::codec::{common_prefix, encode_keys},
    node::columnar::{ColumnData, encode_columns},
};
use std::marker::PhantomData;
use std::sync::Arc;

/// A leaf segment's decoded keys in entry order, stored as one flat arena with
/// per-entry end offsets rather than a `Vec<Vec<u8>>`.
///
/// Decoding a leaf then costs two allocations (arena + offsets), not one per
/// key, so memoizing the decode (see [`PersistentNode::decoded_keys`]) stays as
/// allocation-frugal as the streaming decoder on the common single-touch scan
/// while letting a re-touched leaf reuse the decode.
#[derive(Debug)]
pub struct DecodedKeys {
    arena: Vec<u8>,
    ends: Vec<usize>,
}

impl DecodedKeys {
    /// The number of keys.
    pub fn len(&self) -> usize {
        self.ends.len()
    }

    /// Whether there are no keys.
    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    /// The key at `index`, borrowed from the arena.
    pub fn get(&self, index: usize) -> Option<&[u8]> {
        let end = *self.ends.get(index)?;
        let start = if index == 0 { 0 } else { self.ends[index - 1] };
        self.arena.get(start..end)
    }

    /// Iterates the keys in entry order, each borrowed from the arena.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        (0..self.len()).map(|index| self.get(index).expect("index in range"))
    }
}

/// A tree node in its serialized, content-addressed form.
///
/// A [`PersistentNode`] holds the serialized [`PersistentNodeBody`] as bytes in
/// a [`Buffer`] and is identified by its [`Blake3Hash`]. The structured
/// contents are recovered as a zero-copy [`ArchivedNodeBody`] view via
/// [`body`](PersistentNode::body).
#[derive(Clone, Debug)]
pub struct PersistentNode<Key, Value> {
    key: PhantomData<Key>,
    value: PhantomData<Value>,

    buffer: Buffer,
}

impl<Key, Value> PersistentNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Creates a new node from a serialized buffer.
    pub fn new(buffer: Buffer) -> Self {
        Self {
            buffer,
            key: PhantomData,
            value: PhantomData,
        }
    }

    /// Returns the content hash of this node.
    pub fn hash(&self) -> &Blake3Hash {
        self.buffer.blake3_hash()
    }

    /// Returns the underlying buffer containing serialized node data.
    pub fn buffer(&self) -> &Buffer {
        &self.buffer
    }

    /// Converts this node into a [`Link`] referencing it, carrying the
    /// separator at the subtree's left edge.
    ///
    /// The separator is a seam property, not derivable from the node's own
    /// body (it depends on the left-adjacent subtree), so the caller threads
    /// it in from the context that knows the seam.
    pub fn to_link(&self, separator: Vec<u8>) -> Result<Link, DialogSearchTreeError> {
        Ok(Link {
            separator,
            node: self.buffer.blake3_hash().clone(),
        })
    }

    /// Returns the upper bound (last) key of this segment node, decoded to
    /// its bytes, if it has one.
    ///
    /// Index nodes carry no full keys (their table holds separators), so this
    /// returns `None` for an index; full bounds exist only in leaves.
    pub fn upper_bound(&self) -> Result<Option<Vec<u8>>, DialogSearchTreeError> {
        match self.body()? {
            ArchivedNodeBody::Index(_) => Ok(None),
            ArchivedNodeBody::Segment(segment) => segment.last_key::<Key>().map(Some),
        }
    }

    /// Accesses the deserialized body of this node.
    pub fn body(&self) -> Result<&ArchivedNodeBody<Value>, DialogSearchTreeError> {
        rkyv::access::<_, rkyv::rancor::Error>(self.buffer.as_ref())
            .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))
    }

    /// Whether a scan over this leaf should reuse a memoized decode
    /// ([`memoized_keys`](Self::memoized_keys)) rather than stream it fresh.
    ///
    /// The columnar leaf must be decoded (front-decode + dictionary resolve)
    /// before its keys can be compared against a scan range. A leaf touched only
    /// once (a single range scan visits each leaf once) gains nothing from a
    /// cached decode and would only pay to materialize it, so the first touch
    /// returns `false` (the walker streams the keys) and only from the second
    /// touch on does this return `true` — a join re-selects the same branch once
    /// per outer binding and lands on the same few leaves each time, and those
    /// repeat touches reuse one decode memoized on the node's [`Buffer`] instead
    /// of re-decoding the leaf once per select.
    pub fn should_memoize_keys(&self) -> bool {
        self.buffer.should_memoize()
    }

    /// This segment's keys as a memoized flat-arena decode, shared via `Arc`.
    /// Populates the memo on the first call and reuses it thereafter. Use only
    /// once [`should_memoize_keys`](Self::should_memoize_keys) has returned
    /// `true`; a single-touch scan streams instead (see the walker).
    pub fn memoized_keys(&self) -> Result<Arc<DecodedKeys>, DialogSearchTreeError> {
        self.buffer
            .memoize_decode(|| self.materialize_keys())?
            .ok_or_else(|| {
                DialogSearchTreeError::Access("node buffer memoized a different decode".to_string())
            })
    }

    /// Decodes this segment's keys into the flat-arena form. Used both to
    /// populate the memo and, on a first (un-memoized) touch, transiently.
    fn materialize_keys(&self) -> Result<DecodedKeys, DialogSearchTreeError> {
        match self.body()? {
            ArchivedNodeBody::Segment(segment) => {
                let mut keys = segment.keys::<Key>()?;
                let mut arena = Vec::new();
                let mut ends = Vec::new();
                while let Some((_, key)) = keys.next_key()? {
                    arena.extend_from_slice(key);
                    ends.push(arena.len());
                }
                Ok(DecodedKeys { arena, ends })
            }
            ArchivedNodeBody::Index(_) => Err(DialogSearchTreeError::Access(
                "decoded_keys called on an index node".to_string(),
            )),
        }
    }

    /// The tree's format header carried by this node.
    ///
    /// Every node embeds the same [`Manifest`], so reading it from any node
    /// (in particular a root) recovers the tree's format constants (branching
    /// parameter, separator bound, value inline-vs-spill threshold) without a
    /// side channel: any node hash is a complete, self-describing tree root.
    pub fn manifest(&self) -> Result<Manifest, DialogSearchTreeError> {
        let header = match self.body()? {
            ArchivedNodeBody::Index(index) => &index.header,
            ArchivedNodeBody::Segment(segment) => &segment.header,
        };
        rkyv::deserialize::<Manifest, rkyv::rancor::Error>(header)
            .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))
    }

    /// Interprets this node as an index node, returning an error if it's a
    /// segment.
    pub fn as_index(&self) -> Result<&ArchivedIndex<Value>, DialogSearchTreeError> {
        self.body().and_then(|body| match body {
            ArchivedNodeBody::Index(index) => Ok(index),
            ArchivedNodeBody::Segment(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a segment node as an index node".to_string(),
            )),
        })
    }

    /// Interprets this node as a segment node, returning an error if it's an
    /// index.
    pub fn as_segment(&self) -> Result<&ArchivedSegment<Value>, DialogSearchTreeError> {
        self.body().and_then(|body| match body {
            ArchivedNodeBody::Segment(segment) => Ok(segment),
            ArchivedNodeBody::Index(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a index node as an segment node".to_string(),
            )),
        })
    }
}

/// A pending operation buffered at an index node (the node's novelty).
///
/// An insert or update is an [`Assert`](NoveltyOp::Assert) carrying the value;
/// a delete is a [`Retract`](NoveltyOp::Retract) tombstone. Both flow down the
/// tree with a flush and are resolved against the leaf segment; within a key the
/// last op wins.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNoveltyOp)]
pub enum NoveltyOp<Value> {
    /// Assert (insert or update) the value.
    Assert(Value),
    /// Retract (delete) the key.
    Retract,
}

/// A single buffered op together with the key it applies to.
///
/// The key is the raw byte string, matching the front-coded separator table:
/// under the value-in-key format a key IS its bytes, so a buffered op needs no
/// key type of its own.
///
/// This is the DECODED (transient) form of a buffered op. The stored form is
/// [`NoveltyBuffer`], which encodes a whole per-link buffer with the segment
/// codec rather than one rkyv record per op.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNoveltyEntry)]
pub struct NoveltyEntry<Value> {
    /// The key this op applies to.
    pub key: Vec<u8>,
    /// The buffered op.
    pub op: NoveltyOp<Value>,
}

/// One child link's buffered ops in stored form, encoded with the SAME
/// columnar codec leaf segments use.
///
/// The keys are split into their schema components and stored one column per
/// component (front-coded arenas for large mostly-distinct components,
/// per-buffer dictionaries for small repeated ones), exactly as
/// [`PersistentSegment`] stores leaf keys. Buffered ops repeat entities and
/// attributes heavily, so the same dictionary and front-coding compression
/// that shrank leaves shrinks the buffer bytes, and hash cost is proportional
/// to bytes. Op polarity (assert/retract) is one more small column; values
/// ride in a table aligned with the assert entries.
///
/// A buffer is range-scoped to its link, so away from the top of the tree it
/// is tag-homogeneous and the full columnar schema applies; a buffer that
/// genuinely straddles a layout boundary falls back to the opaque whole-key
/// schema under [`MIXED_LAYOUT`], the same rule segments use.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNoveltyBuffer)]
pub struct NoveltyBuffer<Value> {
    /// Index of the child link this buffer is pending against. Buffers are
    /// stored sparsely (links without pending ops store nothing), in strictly
    /// ascending child order.
    pub child: u32,
    /// Number of buffered ops.
    pub count: u32,
    /// The layout id shared by every key in this buffer, or [`MIXED_LAYOUT`]
    /// when the buffer straddles a layout boundary.
    pub layout: u8,
    /// One encoded column per key-schema component, in schema order.
    pub columns: Vec<ColumnData>,
    /// Op polarity per entry, in entry order: 1 is an assert, 0 a retract.
    pub polarity: Vec<u8>,
    /// Values of the assert entries, in entry order (a retract carries none).
    pub values: Vec<Value>,
}

impl<Value> NoveltyBuffer<Value>
where
    Value: self::Value,
{
    /// Encodes one link's buffered ops (sorted by key, newest op for a key
    /// last) into the columnar stored form. Encoding is a pure function of
    /// the op list, so equal buffers serialize to identical bytes.
    pub fn from_entries<Key: self::Key>(
        child: u32,
        entries: Vec<NoveltyEntry<Value>>,
    ) -> Result<Self, DialogSearchTreeError> {
        let count = entries.len() as u32;
        if entries.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to encode an empty novelty buffer".into(),
            ));
        }

        // Classify the buffer by layout from the raw bytes alone
        // ([`Key::layout_of`]): a buffer that straddles layouts (common near
        // the root, whose range spans every key region) takes the opaque
        // whole-key fallback, which needs no component split — so the typed
        // parse is skipped entirely there and paid only where the schema
        // split actually applies.
        let first_layout = Key::layout_of(&entries[0].key)?;
        let mut uniform = true;
        for entry in &entries[1..] {
            if Key::layout_of(&entry.key)? != first_layout {
                uniform = false;
                break;
            }
        }

        let (layout, columns) = if uniform {
            let schema = Key::schema(first_layout);
            // Buffered keys are raw bytes; the schema split needs the typed
            // key. Every buffered key was produced by `Key::as_ref`, so this
            // parse is a round trip and a failure marks corrupt state, not
            // bad input.
            let keys = entries
                .iter()
                .map(|entry| Key::try_from_bytes(&entry.key))
                .collect::<Result<Vec<Key>, DialogSearchTreeError>>()?;
            let mut rows: Vec<Vec<&[u8]>> = Vec::with_capacity(keys.len());
            for key in &keys {
                let mut row = Vec::with_capacity(schema.len());
                key.components(&mut row);
                // The same `Key` contract check the segment encoder makes: a
                // surplus slice would be silently dropped by the column
                // encoder, which is data loss in a content-addressed node.
                if row.len() != schema.len() {
                    return Err(DialogSearchTreeError::Node(format!(
                        "Key split into {} components for a schema of {}",
                        row.len(),
                        schema.len()
                    )));
                }
                if row.iter().map(|slice| slice.len()).sum::<usize>() != key.as_ref().len() {
                    return Err(DialogSearchTreeError::Node(
                        "Key components do not cover the key's bytes".into(),
                    ));
                }
                rows.push(row);
            }
            (first_layout, encode_columns(&schema, &rows)?)
        } else {
            // The opaque schema is a single whole-key arena column; encode it
            // directly from the key slices rather than through the per-row
            // component table (which would allocate a one-slice row per op).
            let keys: Vec<&[u8]> = entries.iter().map(|entry| entry.key.as_slice()).collect();
            let (prefix, stream) = encode_keys(&keys);
            (MIXED_LAYOUT, vec![ColumnData::Arena { prefix, stream }])
        };

        let mut polarity = Vec::with_capacity(entries.len());
        let mut values = Vec::new();
        for entry in entries {
            match entry.op {
                NoveltyOp::Assert(value) => {
                    polarity.push(1);
                    values.push(value);
                }
                NoveltyOp::Retract => polarity.push(0),
            }
        }

        Ok(Self {
            child,
            count,
            layout,
            columns,
            polarity,
            values,
        })
    }
}

/// Groups a node-wide buffer (sorted by key) into per-link buffers by the
/// SAME rule routing and a flush use: child `at` takes the ops in
/// `[sep(at), sep(at + 1))`, the last child takes whatever remains, and a key
/// below every separator clamps into child 0. Each op lands in exactly one
/// link's buffer, so the reader that descends a link takes exactly that
/// link's ops with no span derivation.
fn group_novelty<Key, Value>(
    links: &[Link],
    novelty: Vec<NoveltyEntry<Value>>,
) -> Result<Vec<NoveltyBuffer<Value>>, DialogSearchTreeError>
where
    Key: self::Key,
    Value: self::Value,
{
    if novelty.is_empty() {
        return Ok(Vec::new());
    }
    let mut buffers = Vec::new();
    let mut rest = novelty.into_iter().peekable();
    for at in 0..links.len() {
        let took: Vec<NoveltyEntry<Value>> = if at + 1 == links.len() {
            rest.by_ref().collect()
        } else {
            let bound: &[u8] = &links[at + 1].separator;
            let mut took = Vec::new();
            while let Some(entry) = rest.peek() {
                if entry.key.as_slice() < bound {
                    took.push(rest.next().expect("peeked"));
                } else {
                    break;
                }
            }
            took
        };
        if !took.is_empty() {
            buffers.push(NoveltyBuffer::from_entries::<Key>(at as u32, took)?);
        }
    }
    Ok(buffers)
}

/// An index node holding its children as a front-coded separator table.
///
/// Each child contributes its lower-bound separator (see [`Link`]); the table
/// stores the longest common prefix of all separators once and each
/// separator's remaining suffix contiguously. Routing compares a probe
/// against the prefix once, then against suffix slices, reconstructing
/// nothing.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedIndex)]
pub struct PersistentIndex<Value> {
    /// The tree's format header, carried by every node so any node hash is a
    /// complete, self-describing tree root. Identical across a tree's nodes,
    /// so structural sharing stores it once in practice.
    pub header: Manifest,
    /// Longest common prefix of all child separators, stored once.
    pub prefix: Vec<u8>,
    /// Concatenated separator suffixes (each separator minus `prefix`), in
    /// child order.
    pub suffixes: Vec<u8>,
    /// End offset of each child's suffix within `suffixes`; one per child,
    /// monotonically nondecreasing, the last equal to `suffixes.len()`.
    pub ends: Vec<u32>,
    /// Child node content hashes, in child order.
    pub hashes: Vec<Blake3Hash>,
    /// Ops pending against this node's subtrees, grouped per child link and
    /// encoded with the segment codec (the node's novelty). Sparse: only
    /// links with pending ops store a buffer, in ascending child order, each
    /// buffer sorted by key with the newest op for a key last.
    ///
    /// Logically each child is `{separator, hash, novelty}`; physically the
    /// separator table stays columnar and the buffers ride here with a
    /// per-child index. An empty `novelty` makes this node byte-identical to
    /// a canonical (fully flushed) index, so
    /// [`canonicalize`](crate::HitchhikerTree::canonicalize) reproduces the
    /// canonical tree exactly. The buffers are deliberately excluded from the
    /// separator table: separators are routing keys and rank inputs, so
    /// letting a pending op move one would reshape the tree as a side effect
    /// of buffering.
    pub novelty: Vec<NoveltyBuffer<Value>>,
}

impl<Value> PersistentIndex<Value> {
    /// Builds the separator table from child links, in order.
    ///
    /// The table layout is a pure function of the links: the prefix is the
    /// longest common prefix of the first and last separator (separators are
    /// sorted), so identical link lists yield identical bytes.
    pub fn from_links(links: Vec<Link>, header: Manifest) -> Self {
        let prefix_length = match (links.first(), links.last()) {
            (Some(first), Some(last)) => common_prefix(&first.separator, &last.separator),
            _ => 0,
        };
        let prefix = links
            .first()
            .map(|link| link.separator[..prefix_length].to_vec())
            .unwrap_or_default();

        let mut suffixes = Vec::new();
        let mut ends = Vec::with_capacity(links.len());
        let mut hashes = Vec::with_capacity(links.len());
        for link in links {
            // Sorted separators make the first/last LCP a prefix of every
            // middle separator (any middle string is sandwiched between them
            // and must share it); an unsorted caller breaks the tree invariant
            // upstream, so surface it as a debug failure and degrade to a
            // saturated slice rather than panicking at persist time.
            debug_assert!(
                link.separator.len() >= prefix_length && link.separator.starts_with(&prefix),
                "index links must be sorted: separator {:02x?} does not carry the prefix {prefix:02x?}",
                link.separator
            );
            let at = prefix_length.min(link.separator.len());
            suffixes.extend_from_slice(&link.separator[at..]);
            ends.push(suffixes.len() as u32);
            hashes.push(link.node);
        }

        Self {
            header,
            prefix,
            suffixes,
            ends,
            hashes,
            novelty: Vec::new(),
        }
    }
}

/// Layout id marking a leaf that straddles a layout boundary and so holds
/// keys of more than one layout. Such a leaf is encoded under the opaque
/// whole-key schema rather than any single layout's columnar schema. Chosen
/// as `u8::MAX` so it never collides with a real layout id (which are small
/// tag-derived values).
pub const MIXED_LAYOUT: u8 = u8::MAX;

/// A leaf segment holding entries columnar: one column per key component
/// (see [`Schema`](crate::Schema)) plus an index-aligned value table.
///
/// Each key is split into its schema components and each component stored in
/// the column that fits it: large mostly-distinct components (entity, value)
/// in front-coded byte arenas, small highly-repeated components (namespace,
/// name, value type) in per-leaf content-derived dictionaries. A key type
/// with no finer structure reports a single whole-key arena column, under
/// which this degrades to a single front-coded key stream. Values stay
/// individually archived, index-aligned with the entries.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedSegment)]
pub struct PersistentSegment<Value> {
    /// The tree's format header, carried by every node so any node hash is a
    /// complete, self-describing tree root. Identical across a tree's nodes,
    /// so structural sharing stores it once in practice.
    pub header: Manifest,
    /// Number of entries in the segment.
    pub count: u32,
    /// The layout id shared by every key in this leaf (see
    /// [`Key::layout`](crate::Key::layout)); selects the schema the columns
    /// were encoded under.
    pub layout: u8,
    /// One encoded column per key-schema component, in schema order.
    pub columns: Vec<ColumnData>,
    /// Entry values, index-aligned with the entries.
    pub values: Vec<Value>,
}

impl<Value> PersistentSegment<Value>
where
    Value: self::Value,
{
    /// Encodes sorted entries into the columnar segment form, splitting each
    /// key into its schema components.
    ///
    /// A leaf is normally single-layout (keys are partitioned by their
    /// leading component, so leaves rarely straddle a layout boundary). When
    /// every entry shares a layout, the leaf is encoded under that layout's
    /// schema. When a leaf *does* straddle a boundary and holds more than one
    /// layout, it is encoded under the opaque whole-key schema and marked
    /// with [`MIXED_LAYOUT`], so decode stays correct without a tree-shape
    /// change; such leaves are rare (one per layout boundary in the tree).
    pub fn from_entries<Key: self::Key>(
        entries: Vec<Entry<Key, Value>>,
        header: Manifest,
    ) -> Result<Self, DialogSearchTreeError> {
        let count = entries.len() as u32;
        let first_layout = entries
            .first()
            .map(|entry| entry.key.layout())
            .ok_or_else(|| {
                DialogSearchTreeError::Node("Attempted to encode an empty segment".into())
            })?;
        let uniform = entries
            .iter()
            .all(|entry| entry.key.layout() == first_layout);

        let (layout, schema) = if uniform {
            (first_layout, Key::schema(first_layout))
        } else {
            (MIXED_LAYOUT, crate::Schema::opaque())
        };

        // Split every key into its component slices, borrowing from the keys.
        // Under the mixed-layout opaque schema, `components` for a structured
        // key would push its own (varying) components, so use the whole key
        // as the single opaque component directly.
        let mut rows: Vec<Vec<&[u8]>> = Vec::with_capacity(entries.len());
        for entry in &entries {
            if layout == MIXED_LAYOUT {
                rows.push(vec![entry.key.as_ref()]);
            } else {
                let mut row = Vec::with_capacity(schema.len());
                entry.key.components(&mut row);
                // Enforce the `Key` contract before anything is encoded: the
                // slice count must match the schema (a surplus slice would
                // otherwise be silently dropped by the column encoder — data
                // loss in a content-addressed node) and the slices must cover
                // the key's comparison bytes exactly.
                if row.len() != schema.len() {
                    return Err(DialogSearchTreeError::Node(format!(
                        "Key split into {} components for a schema of {}",
                        row.len(),
                        schema.len()
                    )));
                }
                if row.iter().map(|slice| slice.len()).sum::<usize>() != entry.key.as_ref().len() {
                    return Err(DialogSearchTreeError::Node(
                        "Key components do not cover the key's bytes".into(),
                    ));
                }
                rows.push(row);
            }
        }
        let columns = encode_columns(&schema, &rows)?;

        let values = entries.into_iter().map(|entry| entry.value).collect();
        Ok(Self {
            header,
            count,
            layout,
            columns,
            values,
        })
    }
}

/// The body of a tree node, either an index or a leaf segment.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedNodeBody)]
pub enum PersistentNodeBody<Value> {
    /// An index node containing links to child nodes.
    Index(PersistentIndex<Value>),
    /// A leaf segment containing key-value entries.
    Segment(PersistentSegment<Value>),
}

impl<Value> PersistentNodeBody<Value>
where
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    /// Serializes this node body to bytes.
    ///
    /// Returns the serializer's [`AlignedVec`] directly so the alignment
    /// that in-place archive access depends on is preserved all the way
    /// into the node [`Buffer`](crate::Buffer).
    pub fn as_bytes(&self) -> Result<AlignedVec, DialogSearchTreeError> {
        rkyv::to_bytes(self).map_err(|error| DialogSearchTreeError::Encoding(format!("{error}")))
    }
}

impl<Value> PersistentNodeBody<Value>
where
    Value: self::Value,
{
    /// Builds an index node body from child links, stamping the tree's format
    /// header.
    ///
    /// `novelty` is the buffer of ops pending against this subtree, sorted by
    /// key; it is grouped per child link here (by the same rule routing and a
    /// flush use) and each link's buffer is encoded with the segment codec.
    /// An empty `novelty` yields a canonical (fully flushed) index — THE
    /// canonical byte form, byte-identical to a node built with no buffers.
    pub fn index_from_links<Key>(
        links: Vec<Link>,
        novelty: Vec<NoveltyEntry<Value>>,
        header: Manifest,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        if links.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        let buffers = group_novelty::<Key, Value>(&links, novelty)?;
        let mut index = PersistentIndex::from_links(links, header);
        index.novelty = buffers;
        Ok(PersistentNodeBody::Index(index))
    }

    /// Builds a leaf segment node body from entries, stamping the tree's
    /// format header.
    pub fn segment_from_entries<Key>(
        entries: Vec<Entry<Key, Value>>,
        header: Manifest,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        if entries.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create a segment from zero entries".into(),
            ));
        }
        Ok(PersistentNodeBody::Segment(
            PersistentSegment::from_entries(entries, header)?,
        ))
    }
}

/// The winning buffered op for `key` in a decoded (transient) buffer, or
/// `None` when the key is not buffered here.
///
/// **The single definition of how a buffered op resolves on owned buffers**,
/// shared by the transient readers (point reads on lifted trees, the
/// differential's settled nodes) so they cannot drift apart; the archived
/// per-link readers resolve by the same last-op-wins rule through
/// [`ArchivedNoveltyBuffer`]'s decode. A buffer is sorted by key and stable
/// within a key, so the run of equal-key entries is contiguous and its last
/// element is the most recent op, matching how a flush replays them.
pub fn resolve_pending<'a, Value>(
    novelty: &'a [NoveltyEntry<Value>],
    key: &[u8],
) -> Option<&'a NoveltyOp<Value>> {
    let at = novelty.partition_point(|entry| entry.key.as_slice() < key);
    if at < novelty.len() && novelty[at].key.as_slice() == key {
        let mut last = at;
        while last + 1 < novelty.len() && novelty[last + 1].key.as_slice() == key {
            last += 1;
        }
        Some(&novelty[last].op)
    } else {
        None
    }
}
