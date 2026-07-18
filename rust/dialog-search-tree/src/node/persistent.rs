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
    node::codec::common_prefix,
    node::columnar::{ColumnData, encode_columns},
};
use std::marker::PhantomData;

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
    pub fn as_index(&self) -> Result<&ArchivedIndex, DialogSearchTreeError> {
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

/// An index node holding its children as a front-coded separator table.
///
/// Each child contributes its lower-bound separator (see [`Link`]); the table
/// stores the longest common prefix of all separators once and each
/// separator's remaining suffix contiguously. Routing compares a probe
/// against the prefix once, then against suffix slices, reconstructing
/// nothing.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedIndex)]
pub struct PersistentIndex {
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
}

impl PersistentIndex {
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
            suffixes.extend_from_slice(&link.separator[prefix_length..]);
            ends.push(suffixes.len() as u32);
            hashes.push(link.node);
        }

        Self {
            header,
            prefix,
            suffixes,
            ends,
            hashes,
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
    Index(PersistentIndex),
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
    pub fn index_from_links(
        links: Vec<Link>,
        header: Manifest,
    ) -> Result<Self, DialogSearchTreeError> {
        if links.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        Ok(PersistentNodeBody::Index(PersistentIndex::from_links(
            links, header,
        )))
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
