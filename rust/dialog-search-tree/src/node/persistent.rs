use dialog_common::Blake3Hash;
use rkyv::{
    Archive, Deserialize, Serialize,
    bytecheck::CheckBytes,
    rancor::Strategy,
    ser::{Serializer, allocator::ArenaHandle, sharing::Share},
    util::AlignedVec,
    validation::{Validator, archive::ArchiveValidator, shared::SharedValidator},
};

use bijoux::{Decode as _, Encode as _};

use crate::{
    Buffer, DialogSearchTreeError, Entry, Key, LegacyManifest, Link, Manifest, Scale, Schema,
    Separator, Value,
    distribution::summary::{Knobs, PieceSummary},
    hashed::HashColumn,
    node::codec::{common_prefix, encode_keys},
    node::columnar::{ColumnData, StreamingLeaf, column_slices, encode_column_values},
};
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

/// What a node derives from its own bytes and keeps with them: each part is
/// a pure function of the bytes (and, for the summary, of the manifest knobs
/// recorded beside it), computed on first use and dropped with the node.
/// Whoever holds the node, a node cache above all, decides how long that is.
#[derive(Default)]
struct Derived {
    /// A leaf's keys, decoded for repeated lookups.
    keys: OnceLock<Arc<DecodedKeys>>,
    /// The hashes of a leaf's keys or an index's separators, as the
    /// boundary rules ask for them.
    hashes: OnceLock<HashColumn>,
    /// A leaf's summary as a piece of a forced run.
    summary: OnceLock<(Knobs, Arc<PieceSummary>)>,
}

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

    /// Position of the first key at or above `probe` (the partition
    /// point): where a range starting at `probe` enters this leaf. O(log n)
    /// key comparisons against the linear walk a front-coded stream needs.
    pub fn lower_bound(&self, probe: &[u8]) -> usize {
        let mut low = 0usize;
        let mut high = self.len();
        while low < high {
            let middle = low + (high - low) / 2;
            if self.get(middle).expect("index in range") < probe {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        low
    }

    /// Position of the key equal to `probe`, or `None`. Keys are stored in
    /// entry (sorted) order, so this is a binary search — O(log n) key
    /// comparisons against the linear front-coded stream a fresh decode
    /// requires.
    #[inline]
    pub fn binary_search(&self, probe: &[u8]) -> Option<usize> {
        let mut low = 0usize;
        let mut high = self.len();
        while low < high {
            let middle = low + (high - low) / 2;
            match self.get(middle).expect("index in range").cmp(probe) {
                Ordering::Less => low = middle + 1,
                Ordering::Greater => high = middle,
                Ordering::Equal => return Some(middle),
            }
        }
        None
    }
}

/// The node layout version of a tagged node (see `notes/tree-node-format.md`
/// and `format/table.csv`): `version, kind, header length, manifest fields,
/// zero padding to 16 bytes, rkyv body`.
pub const TAGGED_LAYOUT: u64 = 0x02;

/// The layout version the legacy (untagged) layout is known by. Never
/// written as a tag: a legacy node is bare rkyv bytes.
pub const LEGACY_LAYOUT: u64 = 0x01;

/// The node kind code of a leaf segment.
const SEGMENT_KIND: u64 = 0x00;

/// The node kind code of an index node.
const INDEX_KIND: u64 = 0x01;

/// The alignment the body of a tagged node starts at. Node bytes live in a
/// 16-byte-aligned [`AlignedVec`], and rkyv reads the body in place.
const BODY_ALIGNMENT: usize = 16;

/// What a tagged node's prelude says beyond its kind: where its manifest
/// fields sit, and how long it is.
#[derive(Clone, Copy, Debug)]
struct Prelude {
    /// Byte range of the manifest fields.
    fields: (u32, u32),
    /// The prelude's length: the byte offset of the body, a nonzero multiple
    /// of 16.
    len: u32,
}

/// A tree node in its serialized, content-addressed form.
///
/// A [`PersistentNode`] holds the node's bytes in a [`Buffer`] and is
/// identified by its [`Blake3Hash`]. The structured contents are recovered as
/// a zero-copy [`NodeBody`] view via [`body`](PersistentNode::body).
///
/// Two layouts are read. A **tagged** node (every node written now) starts
/// with its layout version and kind, then the tree's manifest as encoded
/// fields, zero padding to 16 bytes, and the rkyv body of its kind. A
/// **legacy** node (written before the tagged layout) is bare rkyv bytes with
/// the manifest inlined as a fixed struct; it stays readable, and nothing
/// writes it any more.
///
/// Validity is a type invariant: a `PersistentNode` can only be constructed
/// through one of two [`TryFrom`] conversions. [`TryFrom<Buffer>`] runs full
/// validation on untrusted bytes (storage, the network).
/// [`TryFrom<&PersistentNodeBody<Value>>`] encodes a body this crate built,
/// which is valid by construction and needs no revalidation. No unsafe
/// constructor exists, and a [`PersistentNodeBody`] cannot itself be built from
/// raw bytes, only from typed data. Either way the body bytes are a valid
/// archive of the node's kind, so [`body`](Self::body) is infallible and costs
/// a pointer cast rather than a bytecheck pass per access.
///
/// The key and value types are markers only, so a node is `Send` and `Sync`
/// exactly when its buffer is, whatever the types it is read as. That lets
/// a [`NodeCache`](crate::NodeCache) share checked nodes across threads.
#[derive(Debug)]
pub struct PersistentNode<Key, Value> {
    key: PhantomData<fn() -> Key>,
    value: PhantomData<fn() -> Value>,

    buffer: Buffer,
    /// Where the node's archived index or segment sits in its bytes, found
    /// once when the node is built: its byte offset, with [`INDEX_BIT`] set
    /// for an index and [`LEGACY_BIT`] for a node in the legacy layout (both
    /// archived types align to 4, so the low two bits are free). This is all
    /// a node keeps beside its bytes, and it makes [`body`](Self::body) an
    /// addition and a bit test.
    root: usize,
}

/// [`PersistentNode::root`]'s bit for an index.
const INDEX_BIT: usize = 1;
/// [`PersistentNode::root`]'s bit for a node in the legacy layout.
const LEGACY_BIT: usize = 2;
/// The bits of [`PersistentNode::root`] that are not the offset.
const ROOT_BITS: usize = INDEX_BIT | LEGACY_BIT;

const _: () = {
    assert!(std::mem::align_of::<ArchivedIndex<Vec<u8>>>() > ROOT_BITS);
    assert!(std::mem::align_of::<ArchivedSegment<Vec<u8>>>() > ROOT_BITS);
};

/// The packed [`PersistentNode::root`] for `body`, a reference into `bytes`
/// that rkyv returned from validating them (or that points into bytes just
/// serialized), so it lies within `bytes`, at an offset aligned to 4.
#[inline(always)]
fn root_of<Value: self::Value>(bytes: &[u8], body: NodeBody<'_, Value>, legacy: bool) -> usize {
    let (at, index) = match body {
        NodeBody::Index(index) => (std::ptr::from_ref(index).addr(), INDEX_BIT),
        NodeBody::Segment(segment) => (std::ptr::from_ref(segment).addr(), 0),
    };
    let offset = at - bytes.as_ptr().addr();
    debug_assert_eq!(offset & ROOT_BITS, 0);
    offset | index | if legacy { LEGACY_BIT } else { 0 }
}

/// A node's body: the archived index or segment it holds, borrowed from its
/// bytes.
pub enum NodeBody<'a, Value>
where
    Value: self::Value,
{
    /// An index node containing links to child nodes.
    Index(&'a ArchivedIndex<Value>),
    /// A leaf segment containing key-value entries.
    Segment(&'a ArchivedSegment<Value>),
}

// Manual impl: a clone shares the buffer, whatever the marker types are.
impl<Key, Value> Clone for PersistentNode<Key, Value> {
    fn clone(&self) -> Self {
        Self {
            key: PhantomData,
            value: PhantomData,
            buffer: self.buffer.clone(),
            root: self.root,
        }
    }
}

impl<Key, Value> PersistentNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
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
    pub fn to_link(&self, separator: impl Into<Separator>) -> Link {
        Link {
            separator: separator.into(),
            node: self.buffer.blake3_hash().clone(),
            scale: self.scale(),
        }
    }

    /// Rough size of the subtree this node roots, for query planning.
    ///
    /// A leaf reports its own entry count. An index sums its children's
    /// estimates and encodes the total once, so error stays bounded by
    /// `sqrt(2)` at any height rather than compounding per level (see
    /// [`Scale::total`]). Reads only this node, never its children.
    ///
    /// Ops pending in an index's novelty are excluded: they have not yet
    /// reached the subtree they are destined for, so counting them here would
    /// double-count once they flush.
    pub fn scale(&self) -> Scale {
        match self.body() {
            NodeBody::Index(index) => Scale::total(index.scales.iter().map(Scale::from)),
            NodeBody::Segment(segment) => Scale::of(segment.count.to_native() as u64),
        }
    }

    /// Returns the upper bound (last) key of this segment node, decoded to
    /// its bytes, if it has one.
    ///
    /// Index nodes carry no full keys (their table holds separators), so this
    /// returns `None` for an index; full bounds exist only in leaves.
    pub fn upper_bound(&self) -> Result<Option<Vec<u8>>, DialogSearchTreeError> {
        match self.body() {
            NodeBody::Index(_) => Ok(None),
            NodeBody::Segment(segment) => segment.last_key::<Key>().map(Some),
        }
    }

    /// Accesses the deserialized body of this node.
    ///
    /// Infallible: validity is the type's construction invariant. The two
    /// [`TryFrom`] conversions are the only ways to build a node, and neither
    /// can admit an invalid archive, so no per-access validation runs.
    pub fn body(&self) -> NodeBody<'_, Value> {
        // SAFETY: `root` is the offset of an archived index (with `INDEX_BIT`)
        // or segment in this node's bytes, taken from a reference into them:
        // either one rkyv returned after its full validation of the node
        // (`TryFrom<Buffer>`), or one into bytes serialized from a typed body
        // (`TryFrom<&PersistentNodeBody<Value>>`), a valid archive by
        // construction. No unsafe constructor exists, and a
        // `PersistentNodeBody` cannot itself be built from raw bytes. Buffers
        // are immutable and every clone shares one allocation, so the
        // reference is as valid now as when the offset was taken.
        unsafe {
            let at = self.buffer.as_ref().as_ptr().add(self.root & !ROOT_BITS);
            if self.root & INDEX_BIT != 0 {
                NodeBody::Index(&*at.cast::<ArchivedIndex<Value>>())
            } else {
                NodeBody::Segment(&*at.cast::<ArchivedSegment<Value>>())
            }
        }
    }

    /// The body of a legacy node.
    ///
    /// # Safety
    ///
    /// The node must be a legacy node (`self.root & LEGACY_BIT != 0`),
    /// whose whole buffer `TryFrom<Buffer>` validated as this type.
    unsafe fn legacy(&self) -> &ArchivedLegacyNodeBody<Value> {
        // SAFETY: the caller guarantees a legacy node, whose buffer was
        // validated as exactly this type when the node was built.
        unsafe { rkyv::access_unchecked::<ArchivedLegacyNodeBody<Value>>(self.buffer.as_ref()) }
    }

    /// The node's layout version: [`TAGGED_LAYOUT`], or [`LEGACY_LAYOUT`] for
    /// a node written before the tagged layout.
    pub fn layout_version(&self) -> u64 {
        if self.root & LEGACY_BIT != 0 {
            LEGACY_LAYOUT
        } else {
            TAGGED_LAYOUT
        }
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
        let derived = self.derived();
        if let Some(keys) = derived.keys.get() {
            return Ok(keys.clone());
        }
        // Decode before taking the cell: the decode can fail, and a racing
        // reader computes the same keys, so whichever lands first stands.
        let keys = Arc::new(self.materialize_keys()?);
        Ok(derived.keys.get_or_init(|| keys).clone())
    }

    /// What this node has derived from its bytes so far, kept on its
    /// [`Buffer`] so every holder of the node shares it.
    fn derived(&self) -> Arc<Derived> {
        self.buffer
            .memoize_decode(|| Ok::<_, std::convert::Infallible>(Derived::default()))
            .ok()
            .flatten()
            // Only if something else took the buffer's one slot: derive
            // without keeping, which costs repeat work and nothing else.
            .unwrap_or_default()
    }

    /// The cells this node keeps hashes in: one per entry of a leaf, for
    /// its key, or per link of an index, for its separator, each filled the
    /// first time a boundary rule asks for that hash. What is opened from
    /// the node shares them ([`TransientNode::open`](crate::TransientNode::open)),
    /// so stored bytes are hashed once for as long as the node is held,
    /// however many times it is opened.
    pub(crate) fn hashes(&self) -> HashColumn {
        self.derived()
            .hashes
            .get_or_init(|| {
                let count = match self.body() {
                    NodeBody::Segment(segment) => segment.len(),
                    NodeBody::Index(index) => index.len(),
                };
                (0..count).map(|_| OnceLock::new()).collect()
            })
            .clone()
    }

    /// Hands this node the hashes already computed for what it was sealed
    /// from, in entry (or link) order: `None` where none was asked for. A
    /// node sealed from ranked entries then opens with their hashes in
    /// place instead of computing every one again.
    pub(crate) fn adopt_hashes(&self, hashes: Vec<Option<Blake3Hash>>) {
        if hashes.iter().all(Option::is_none) {
            return;
        }
        let column: HashColumn = hashes
            .into_iter()
            .map(|hash| hash.map(OnceLock::from).unwrap_or_default())
            .collect();
        let _ = self.derived().hashes.set(column);
    }

    /// This piece's summary for the forced-run quiet check, if one was kept
    /// under `manifest`'s knobs ([`summarize`](Self::summarize)).
    pub(crate) fn summary(&self, manifest: &Manifest) -> Option<Arc<PieceSummary>> {
        let derived = self.buffer.memoized::<Derived>()?;
        let (knobs, summary) = derived.summary.get()?;
        (*knobs == Knobs::from(manifest)).then(|| summary.clone())
    }

    /// Keeps `summary` with this node as its summary under `manifest`'s
    /// knobs, returning the shared handle. A summary is a pure function of
    /// the node's bytes and those knobs, so it never goes stale; a node
    /// already holding one under other knobs keeps that one.
    pub(crate) fn summarize(
        &self,
        manifest: &Manifest,
        summary: PieceSummary,
    ) -> Arc<PieceSummary> {
        let summary = Arc::new(summary);
        let _ = self
            .derived()
            .summary
            .set((Knobs::from(manifest), summary.clone()));
        summary
    }

    /// Decodes this segment's keys into the flat-arena form. Used both to
    /// populate the memo and, on a first (un-memoized) touch, transiently.
    fn materialize_keys(&self) -> Result<DecodedKeys, DialogSearchTreeError> {
        match self.body() {
            NodeBody::Segment(segment) => {
                let mut keys = segment.keys::<Key>()?;
                let mut arena = Vec::new();
                let mut ends = Vec::new();
                while let Some((_, key)) = keys.next_key()? {
                    arena.extend_from_slice(key);
                    ends.push(arena.len());
                }
                Ok(DecodedKeys { arena, ends })
            }
            NodeBody::Index(_) => Err(DialogSearchTreeError::Access(
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
        if self.root & LEGACY_BIT != 0 {
            // SAFETY: checked just above that this is a legacy node.
            let header = match unsafe { self.legacy() } {
                ArchivedLegacyNodeBody::Index(legacy) => &legacy.header,
                ArchivedLegacyNodeBody::Segment(legacy) => &legacy.header,
            };
            let header = rkyv::deserialize::<LegacyManifest, rkyv::rancor::Error>(header)
                .map_err(|error| DialogSearchTreeError::Access(format!("{error}")))?;
            return Ok(Manifest::from(header));
        }
        let bytes = self.buffer.as_ref();
        let (start, end) = tagged_prelude(bytes)
            .ok_or_else(|| {
                DialogSearchTreeError::Access("A tagged node lost its prelude".to_string())
            })?
            .fields;
        decode_manifest(&bytes[start as usize..end as usize])
    }

    /// Whether this node is the empty tree's node: an index with no
    /// children and no buffered ops, carrying the format manifest and
    /// nothing else (see [`persist_empty_root`](crate::persist_empty_root)),
    /// so every root, the empty one included, is an index. A zero-entry
    /// segment, the empty tree's node before, reads as empty too. Such a
    /// node is a pure format marker — the persisted root of an empty tree,
    /// never an interior node — and load paths treat it as the absence of a
    /// root.
    pub fn is_empty(&self) -> Result<bool, DialogSearchTreeError> {
        Ok(match self.body() {
            NodeBody::Index(index) => index.is_empty() && index.novelty_len() == 0,
            NodeBody::Segment(segment) => segment.len() == 0,
        })
    }

    /// Interprets this node as an index node, returning an error if it's a
    /// segment.
    pub fn as_index(&self) -> Result<&ArchivedIndex<Value>, DialogSearchTreeError> {
        match self.body() {
            NodeBody::Index(index) => Ok(index),
            NodeBody::Segment(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a segment node as an index node".to_string(),
            )),
        }
    }

    /// Interprets this node as a segment node, returning an error if it's an
    /// index.
    pub fn as_segment(&self) -> Result<&ArchivedSegment<Value>, DialogSearchTreeError> {
        match self.body() {
            NodeBody::Segment(segment) => Ok(segment),
            NodeBody::Index(_) => Err(DialogSearchTreeError::Access(
                "Attempted to interpret a index node as an segment node".to_string(),
            )),
        }
    }
}

/// Builds a node from a buffer of untrusted bytes (storage, the network),
/// validating that it is a tagged or legacy node of its kind. This
/// is the only validation the node ever runs; it establishes the invariant
/// that [`body`](PersistentNode::body) relies on for the node and all its
/// clones.
impl<Key, Value> TryFrom<Buffer> for PersistentNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    type Error = DialogSearchTreeError;

    fn try_from(buffer: Buffer) -> Result<Self, Self::Error> {
        // The node of a tree under the default manifest, which is nearly every
        // node read: its prelude is one fixed 16-byte word, so recognizing it
        // and validating the body is all a read does. Everything else (a
        // manifest with fields, a legacy node, bytes that are neither) goes
        // through `from_other`, kept out of line so this path stays small.
        let checked = check_tagged_body::<Value>(buffer.as_ref());
        if checked & OTHER_PRELUDE == 0 {
            return Ok(Self {
                key: PhantomData,
                value: PhantomData,
                root: checked,
                buffer,
            });
        }
        Self::from_other(buffer, checked & !OTHER_PRELUDE)
    }
}

impl<Key, Value> PersistentNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// [`TryFrom<Buffer>`] for any node but a tagged one under the default
    /// manifest.
    #[cold]
    #[inline(never)]
    fn from_other(buffer: Buffer, root: usize) -> Result<Self, DialogSearchTreeError> {
        // `root` is the body [`check_tagged_body`] already validated, as the
        // kind the prelude states, or 0 when the bytes are not a valid tagged
        // body. A validated body needs no second check: what is left is the
        // rest of the prelude, plain bytes read here. Bytes whose prelude is
        // not a well-formed tagged one are read as a legacy node: its
        // first bytes are arbitrary rkyv data and may happen to equal a
        // version, but then fail these checks within a few bytes.
        let bytes = buffer.as_ref();
        let mut refused = None;
        if root != 0
            && let Some(prelude) = tagged_prelude(bytes)
        {
            let (start, end) = prelude.fields;
            // The fields are decoded here only to check this build can read
            // them. Fields it cannot read (malformed, or an unknown critical
            // field) make the bytes not a tagged node of this build. They are
            // almost surely a tagged node all the same, so the manifest's
            // error is the one reported unless the bytes happen to read as a
            // legacy node.
            match decode_manifest(&bytes[start as usize..end as usize]) {
                Ok(_) => {
                    PreludeMemo::remember(bytes, &prelude);
                    return Ok(Self {
                        key: PhantomData,
                        value: PhantomData,
                        root,
                        buffer,
                    });
                }
                Err(error) => refused = Some(error),
            }
        }
        Self::from_legacy(buffer, refused)
    }

    /// Reads bytes that are not a tagged node as a legacy node, or reports
    /// why they are neither: `refused` when they were a tagged node whose
    /// fields this build cannot read.
    fn from_legacy(
        buffer: Buffer,
        refused: Option<DialogSearchTreeError>,
    ) -> Result<Self, DialogSearchTreeError> {
        // Validated with rkyv's shared-pointer validator, unlike the hot
        // tagged path, so every nested check is compiled separately from that
        // path's. Sharing them gave those checks a second caller, and the
        // compiler then stopped inlining them into the tagged path, costing
        // every read.
        let bytes = buffer.as_ref();
        let legacy =
            match rkyv::access::<ArchivedLegacyNodeBody<Value>, rkyv::rancor::Error>(bytes) {
                Ok(legacy) => legacy,
                Err(error) => {
                    return Err(refused
                        .unwrap_or_else(|| DialogSearchTreeError::Access(format!("{error}"))));
                }
            };
        let body = match legacy {
            ArchivedLegacyNodeBody::Index(legacy) => NodeBody::Index(legacy.index()),
            ArchivedLegacyNodeBody::Segment(legacy) => NodeBody::Segment(legacy.segment()),
        };
        let root = root_of(bytes, body, true);
        Ok(Self {
            key: PhantomData,
            value: PhantomData,
            root,
            buffer,
        })
    }
}

/// [`check_tagged_body`]'s flag for bytes that are not a finished tagged node
/// under the default manifest. Only in that function's result: a node's own
/// `root` uses this bit as [`LEGACY_BIT`].
const OTHER_PRELUDE: usize = LEGACY_BIT;

/// Validates `bytes` as a tagged node's body and checks for the default
/// manifest's prelude. Returns:
///
/// - the packed [`PersistentNode::root`] when they are a valid tagged node
///   under the default manifest (without [`OTHER_PRELUDE`]);
/// - that root with [`OTHER_PRELUDE`] set when the body is valid but the
///   prelude is some other one, left to the cold path to parse;
/// - [`OTHER_PRELUDE`] alone when they are not a valid tagged body.
///
/// A body at offset 0 would overlap its own prelude; no writer makes one,
/// and both paths refuse it: this one reports "not a valid body", and a
/// root of 0 with [`OTHER_PRELUDE`] is what the cold path refuses too.
///
/// Every tagged node is validated here and nowhere else. This is the whole
/// read path of nearly every node, so it is held to the
/// cost of reading an untagged node:
///
/// - One out-of-line function returning one word, so the caller keeps nothing
///   live across it but the buffer.
/// - Validation through rkyv's low-level entry point, with no shared-pointer
///   validator: tagged bodies hold no shared pointers, and the shared one
///   builds and drops an empty map for every node. No other path uses this
///   validator, so its checks have this single caller and are inlined here.
/// - The prelude's kind byte picks the type the body is validated as, so the
///   kind is stated once and cannot disagree with the body.
/// - It validates the whole buffer rather than the bytes after the prelude:
///   the archive's root is at the end either way. A body could then point
///   into its own prelude and read those bytes as data, which is memory-safe
///   and which no writer produces.
#[inline(never)]
fn check_tagged_body<Value>(bytes: &[u8]) -> usize
where
    Value: self::Value,
    Value::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    let Some(head) = bytes.first_chunk::<BODY_ALIGNMENT>() else {
        return OTHER_PRELUDE;
    };
    // The kind code is the root's `INDEX_BIT`, so it packs in as is.
    const _: () = assert!(SEGMENT_KIND == 0 && INDEX_KIND as usize == INDEX_BIT);
    // The version is checked with the rest of the prelude below: a node of
    // another version fails that and is left to the cold path.
    let kind = head[1];
    let at = match u64::from(kind) {
        SEGMENT_KIND => {
            rkyv::api::low::access::<ArchivedSegment<Value>, rkyv::rancor::Error>(bytes)
                .map(|segment| std::ptr::from_ref(segment).addr())
        }
        INDEX_KIND => rkyv::api::low::access::<ArchivedIndex<Value>, rkyv::rancor::Error>(bytes)
            .map(|index| std::ptr::from_ref(index).addr()),
        _ => return OTHER_PRELUDE,
    };
    let Ok(at) = at else {
        return OTHER_PRELUDE;
    };
    let offset = at - bytes.as_ptr().addr();
    // A body at offset 0 sits where its prelude is: the bytes then read
    // as two things at once, which no writer produces. Refused here as
    // the cold path refuses it, rather than accepted with a root of 0.
    if offset == 0 {
        return OTHER_PRELUDE;
    }
    let root = offset | usize::from(kind);
    // One comparison of the first 16 bytes against the default manifest's
    // prelude for this kind checks the empty header and the padding.
    if u128::from_le_bytes(*head) != TAGGED_LAYOUT as u128 | u128::from(kind) << 8
        && !PreludeMemo::accepts(bytes, u64::from(kind))
    {
        return root | OTHER_PRELUDE;
    }
    root
}

/// The tagged prelude of `bytes`, if they start with one. `None` when the
/// bytes do not start with a well-formed prelude of a known layout version:
/// an unknown version or kind, a header running past the end, or padding that
/// is not exactly zero bytes up to the body.
fn tagged_prelude(bytes: &[u8]) -> Option<Prelude> {
    // The version and kind are values below 248, so each is exactly one byte
    // in the only encoding bijou64 allows; any other byte is not a tagged
    // node of a known version.
    let [version, kind, ..] = *bytes else {
        return None;
    };
    if u64::from(version) != TAGGED_LAYOUT {
        return None;
    }
    if u64::from(kind) != SEGMENT_KIND && u64::from(kind) != INDEX_KIND {
        return None;
    }
    let (length, read) = u64::decode(bytes.get(2..)?).ok()?;
    let start = 2 + read;
    let end = start.checked_add(usize::try_from(length).ok()?)?;
    let body = end.checked_next_multiple_of(BODY_ALIGNMENT)?;
    if body >= bytes.len() {
        return None;
    }
    // The padding is the tail of the 16 bytes before the body (the body sits
    // at least 16 bytes in, past the 3-byte preamble), under 16 bytes long:
    // check it as one word rather than byte by byte.
    let padding = body - end;
    let window: [u8; BODY_ALIGNMENT] = bytes[body - BODY_ALIGNMENT..body].try_into().ok()?;
    if padding > 0 && u128::from_le_bytes(window) >> (8 * (BODY_ALIGNMENT - padding)) != 0 {
        return None;
    }
    Some(Prelude {
        fields: (u32::try_from(start).ok()?, u32::try_from(end).ok()?),
        len: u32::try_from(body).ok()?,
    })
}

/// The prelude of the last node under a non-default manifest this thread
/// accepted. Every node of a tree carries the same prelude but for its kind
/// byte, so the rest of that tree's nodes are accepted by comparing their
/// first bytes against it, without parsing the prelude or its fields again.
/// Sound because equal bytes parse equally: an identical prelude is as
/// well-formed, and its fields as readable, as the one accepted.
///
/// A prelude is a multiple of 16 bytes long; one of 16 or 32 (up to 29
/// bytes of fields) is remembered, as its two 16-byte words with the kind
/// byte cleared.
struct PreludeMemo;

impl PreludeMemo {
    /// Clears the kind byte of a prelude's first word.
    const WITHOUT_KIND: u128 = !(0xff << 8);

    thread_local! {
        /// Whether a prelude is remembered, its first word with the kind byte
        /// cleared, and its second word with the mask that selects it (all
        /// ones for a 32-byte prelude, zero for a 16-byte one).
        static LAST: std::cell::Cell<(bool, u128, u128, u128)> =
            const { std::cell::Cell::new((false, 0, 0, 0)) };
    }

    /// The first two 16-byte words of `bytes`, if it holds them.
    #[inline(always)]
    fn words(bytes: &[u8]) -> Option<(u128, u128)> {
        let head = bytes.first_chunk::<{ 2 * BODY_ALIGNMENT }>()?;
        let (low, high) = head.split_at(BODY_ALIGNMENT);
        Some((
            u128::from_le_bytes(low.try_into().ok()?),
            u128::from_le_bytes(high.try_into().ok()?),
        ))
    }

    /// Whether `bytes`, whose body is of `kind`, start with the prelude last
    /// accepted.
    #[inline(always)]
    fn accepts(bytes: &[u8], kind: u64) -> bool {
        let (known, low, high, mask) = Self::LAST.get();
        let Some((first, second)) = Self::words(bytes) else {
            return false;
        };
        known & (first == low | u128::from(kind) << 8) & (second & mask == high)
    }

    /// Remembers the prelude of `bytes`, just accepted.
    fn remember(bytes: &[u8], prelude: &Prelude) {
        let mask = match prelude.len as usize {
            BODY_ALIGNMENT => 0,
            len if len == 2 * BODY_ALIGNMENT => u128::MAX,
            _ => return,
        };
        if let Some((first, second)) = Self::words(bytes) {
            Self::LAST.set((true, first & Self::WITHOUT_KIND, second & mask, mask));
        }
    }
}

/// The manifest that encodes to no fields: every field at its table default.
/// Recognizing it lets a node's prelude be written and read as a fixed word.
fn fieldless() -> &'static Manifest {
    static FIELDLESS: std::sync::OnceLock<Manifest> = std::sync::OnceLock::new();
    FIELDLESS.get_or_init(|| Manifest::decode(&[]).expect("no fields decode"))
}

/// Writes a tagged node's prelude into the empty `bytes`: its layout version,
/// its kind, the manifest's fields and zero padding to the body.
///
/// Every node of a tree carries the same fields, so they are not encoded per
/// node. Under a manifest with no fields the prelude is a fixed 16-byte word;
/// otherwise the fields encoded for the previous node this thread wrote are
/// reused when its manifest is the same, which it is for every node an edit
/// writes after the first.
fn write_prelude(bytes: &mut AlignedVec, manifest: &Manifest, index: bool) {
    use std::cell::RefCell;
    thread_local! {
        static LAST: RefCell<Option<(Manifest, Vec<u8>)>> = const { RefCell::new(None) };
    }

    let kind = if index { INDEX_KIND } else { SEGMENT_KIND };
    if manifest.is_fieldless() {
        let mut word = [0u8; BODY_ALIGNMENT];
        word[0] = TAGGED_LAYOUT as u8;
        word[1] = kind as u8;
        bytes.extend_from_slice(&word);
        return;
    }
    LAST.with_borrow_mut(|last| {
        if last.as_ref().is_none_or(|(known, _)| known != manifest) {
            let mut fields = Vec::new();
            manifest.encode(&mut fields);
            *last = Some((manifest.clone(), fields));
        }
        let (_, fields) = last.as_ref().expect("just filled");
        let mut preamble = Vec::with_capacity(4);
        TAGGED_LAYOUT.encode(&mut preamble);
        kind.encode(&mut preamble);
        (fields.len() as u64).encode(&mut preamble);
        let body = (preamble.len() + fields.len()).next_multiple_of(BODY_ALIGNMENT);
        bytes.reserve(body);
        bytes.extend_from_slice(&preamble);
        bytes.extend_from_slice(fields);
        bytes.resize(body, 0);
    });
}

/// Decodes a node's manifest fields, remembering the result by their bytes.
///
/// Every node of a tree carries byte-identical fields, so after the first node
/// of a tree this is one lookup; the default manifest (no fields) is never
/// parsed at all. Bounded by clearing when full: every entry is cheap to
/// recover.
fn decode_manifest(fields: &[u8]) -> Result<Manifest, DialogSearchTreeError> {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    const CAPACITY: usize = 1024;
    static MEMO: OnceLock<Mutex<HashMap<Box<[u8]>, Manifest>>> = OnceLock::new();
    thread_local! {
        // The last fields this thread decoded: a run of reads nearly always
        // stays within one tree, so this answers without a lock or a hash.
        static LAST: RefCell<Option<(Box<[u8]>, Manifest)>> = const { RefCell::new(None) };
    }

    if fields.is_empty() {
        return Ok(fieldless().clone());
    }
    let last = LAST.with_borrow(|last| {
        last.as_ref()
            .filter(|(bytes, _)| **bytes == *fields)
            .map(|(_, manifest)| manifest.clone())
    });
    if let Some(manifest) = last {
        return Ok(manifest);
    }
    let memo = MEMO.get_or_init(|| Mutex::new(HashMap::new()));
    let manifest = match memo.lock().ok().and_then(|memo| memo.get(fields).cloned()) {
        Some(manifest) => manifest,
        None => {
            let manifest = Manifest::decode(fields)?;
            if let Ok(mut memo) = memo.lock() {
                if memo.len() >= CAPACITY {
                    memo.clear();
                }
                memo.insert(fields.into(), manifest.clone());
            }
            manifest
        }
    };
    LAST.set(Some((fields.into(), manifest.clone())));
    Ok(manifest)
}

/// Seals a node body this crate built into its persistent (tagged) form.
/// Validity is carried by the type: the rkyv body of a `PersistentNodeBody`'s
/// index or segment is by construction a valid archive of that type, exactly
/// what [`body`](PersistentNode::body) accesses unchecked, so no revalidation
/// and no caller assertion are needed.
impl<Key, Value> TryFrom<&PersistentNodeBody<Value>> for PersistentNode<Key, Value>
where
    Key: self::Key,
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    type Error = DialogSearchTreeError;

    fn try_from(body: &PersistentNodeBody<Value>) -> Result<Self, Self::Error> {
        let (bytes, at) = body.encode()?;
        // SAFETY: the bytes after `at` were just serialized from a typed
        // index or segment, so they are a valid archive of it.
        let archived = unsafe {
            match &body.node {
                NodeKind::Index(_) => {
                    NodeBody::Index(rkyv::access_unchecked::<ArchivedIndex<Value>>(&bytes[at..]))
                }
                NodeKind::Segment(_) => NodeBody::Segment(rkyv::access_unchecked::<
                    ArchivedSegment<Value>,
                >(&bytes[at..])),
            }
        };
        let root = root_of(&bytes, archived, false);
        Ok(Self {
            buffer: Buffer::from(bytes),
            key: PhantomData,
            value: PhantomData,
            root,
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
        let (count, layout, columns) = Self::key_columns::<Key>(&entries)?;

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

    /// [`from_entries`](Self::from_entries) without consuming the op list:
    /// the values are cloned into the buffer instead of moved. Exists for
    /// the non-consuming persist, which must keep the decoded ops live for
    /// later appends while embedding their encoding into the frame.
    pub fn from_entries_ref<Key: self::Key>(
        child: u32,
        entries: &[NoveltyEntry<Value>],
    ) -> Result<Self, DialogSearchTreeError> {
        let (count, layout, columns) = Self::key_columns::<Key>(entries)?;

        let mut polarity = Vec::with_capacity(entries.len());
        let mut values = Vec::new();
        for entry in entries {
            match &entry.op {
                NoveltyOp::Assert(value) => {
                    polarity.push(1);
                    values.push(value.clone());
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

    /// The key side of the buffer encoding, shared by the consuming and
    /// borrowing constructors: layout classification plus the columnar
    /// encode of every key.
    fn key_columns<Key: self::Key>(
        entries: &[NoveltyEntry<Value>],
    ) -> Result<(u32, u8, Vec<ColumnData>), DialogSearchTreeError> {
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
            // Buffered keys are raw bytes, and the split borrows from them
            // directly ([`Key::components_of`]): no typed key is
            // reconstructed, no per-op row list is materialized: the slices
            // land straight in the per-column vecs the encoder consumes.
            (
                first_layout,
                encode_split_keys::<Key>(
                    &schema,
                    first_layout,
                    entries.iter().map(|entry| entry.key.as_slice()),
                    entries.len(),
                )?,
            )
        } else {
            // The opaque schema is a single whole-key arena column; encode it
            // directly from the key slices rather than through the per-row
            // component table (which would allocate a one-slice row per op).
            let keys: Vec<&[u8]> = entries.iter().map(|entry| entry.key.as_slice()).collect();
            let (prefix, stream) = encode_keys(&keys);
            (MIXED_LAYOUT, vec![ColumnData::Arena { prefix, stream }])
        };

        Ok((count, layout, columns))
    }

    /// The raw buffered weight this sealed buffer carries (key bytes plus
    /// value payload weights, retracts charged at 16; the per-op overhead is
    /// added from the manifest by the caller), for the byte-capped flush
    /// trigger: computed by streaming the key columns, with no entry
    /// materialization.
    pub fn weight<Key: self::Key>(&self) -> Result<usize, DialogSearchTreeError> {
        let mut weight = 0usize;
        let mut keys = self.keys::<Key>()?;
        while let Some((_, key)) = keys.next_key()? {
            weight += key.len();
        }
        weight += self
            .values
            .iter()
            .map(|value| value.payload_weight())
            .sum::<usize>();
        weight += 16 * self.polarity.iter().filter(|&&p| p == 0).count();
        Ok(weight)
    }

    /// The op count claimed by `count`, validated against the polarity and
    /// value tables, mirroring [`ArchivedNoveltyBuffer::checked_count`] for a
    /// buffer held in its owned form (a sealed transient link buffer).
    pub fn checked_count(&self) -> Result<usize, DialogSearchTreeError> {
        let count = self.count as usize;
        if count != self.polarity.len() {
            return Err(DialogSearchTreeError::Encoding(
                "Novelty buffer count disagrees with its polarity column".into(),
            ));
        }
        let mut asserts = 0usize;
        for &polarity in self.polarity.iter() {
            match polarity {
                0 => {}
                1 => asserts += 1,
                _ => {
                    return Err(DialogSearchTreeError::Encoding(
                        "Novelty polarity is neither assert nor retract".into(),
                    ));
                }
            }
        }
        if asserts != self.values.len() {
            return Err(DialogSearchTreeError::Encoding(
                "Novelty buffer values disagree with its polarity column".into(),
            ));
        }
        Ok(count)
    }

    /// A streaming decoder over this buffer's full keys, in entry order: the
    /// owned-form counterpart of [`ArchivedNoveltyBuffer::keys`], reading the
    /// encoded columns without materializing them.
    pub fn keys<Key: self::Key>(&self) -> Result<StreamingLeaf<'_>, DialogSearchTreeError> {
        let count = self.checked_count()?;
        let schema = if self.layout == MIXED_LAYOUT {
            Schema::opaque()
        } else {
            Key::schema(self.layout)
        };
        let columns: Vec<_> = self.columns.iter().map(column_slices).collect();
        StreamingLeaf::new(&schema, &columns, count)
    }

    /// The op at entry `at`, cloned to its own value. A retract reads only the
    /// polarity column; an assert clones its value from the assert-aligned
    /// value table.
    pub fn op_at(&self, at: usize) -> Result<NoveltyOp<Value>, DialogSearchTreeError> {
        let slot = self.polarity[..at.min(self.polarity.len())]
            .iter()
            .filter(|&&p| p == 1)
            .count();
        self.op_with_slot(at, slot)
    }

    /// The op at entry `at` given its value-table `slot` (the number of
    /// asserts before it), for readers that track the slot while streaming
    /// the buffer instead of re-scanning the polarity column per op.
    pub(crate) fn op_with_slot(
        &self,
        at: usize,
        slot: usize,
    ) -> Result<NoveltyOp<Value>, DialogSearchTreeError> {
        match self.polarity.get(at) {
            None => Err(DialogSearchTreeError::Encoding(
                "Novelty entry out of range".into(),
            )),
            Some(0) => Ok(NoveltyOp::Retract),
            Some(1) => {
                let value = self.values.get(slot).ok_or_else(|| {
                    DialogSearchTreeError::Encoding("Novelty value out of range".into())
                })?;
                Ok(NoveltyOp::Assert(value.clone()))
            }
            Some(_) => Err(DialogSearchTreeError::Encoding(
                "Novelty polarity is neither assert nor retract".into(),
            )),
        }
    }

    /// The winning op for `key` in this buffer, or `None` when the key is not
    /// buffered here. The buffer is sorted by key with the newest op for a key
    /// last, so the scan keeps the last equal key and stops at the first
    /// greater one; the winner's value-table slot is tracked as the polarity
    /// column is walked, and only the winner's value is decoded.
    pub fn resolve<Key: self::Key>(
        &self,
        key: &[u8],
    ) -> Result<Option<NoveltyOp<Value>>, DialogSearchTreeError> {
        let mut keys = self.keys::<Key>()?;
        let mut winner: Option<(usize, usize)> = None;
        let mut asserts = 0usize;
        while let Some((at, entry_key)) = keys.next_key()? {
            let slot = asserts;
            // `keys()` validated every polarity byte as 0 or 1.
            if self.polarity.get(at).copied() == Some(1) {
                asserts += 1;
            }
            match entry_key.cmp(key) {
                Ordering::Less => {}
                Ordering::Equal => winner = Some((at, slot)),
                Ordering::Greater => break,
            }
        }
        winner
            .map(|(at, slot)| self.op_with_slot(at, slot))
            .transpose()
    }

    /// Decodes the whole buffer to owned entries, in entry order: the lift a
    /// write performs when it touches a sealed link buffer.
    pub fn entries<Key: self::Key>(
        &self,
    ) -> Result<Vec<NoveltyEntry<Value>>, DialogSearchTreeError> {
        let mut out = Vec::with_capacity(self.count as usize);
        let mut keys = self.keys::<Key>()?;
        let mut slot = 0usize;
        while let Some((at, key)) = keys.next_key()? {
            let op = match self.polarity.get(at) {
                Some(0) => NoveltyOp::Retract,
                Some(1) => {
                    let value = self.values.get(slot).ok_or_else(|| {
                        DialogSearchTreeError::Encoding("Novelty value out of range".into())
                    })?;
                    slot += 1;
                    NoveltyOp::Assert(value.clone())
                }
                _ => {
                    return Err(DialogSearchTreeError::Encoding(
                        "Novelty polarity is neither assert nor retract".into(),
                    ));
                }
            };
            out.push(NoveltyEntry {
                key: key.to_vec(),
                op,
            });
        }
        Ok(out)
    }
}

/// Splits each key's raw bytes into its schema components and encodes the
/// columns, in one pass: the slices land straight in the per-column vecs the
/// encoder consumes ([`encode_column_values`]), with one reused row buffer,
/// no typed key reconstruction, and no per-key allocation. Shared by the leaf
/// and novelty-buffer encoders, whose keys both live as plain bytes.
///
/// Enforces the [`Key`] contract before anything is encoded: the slice count
/// must match the schema (a surplus slice would otherwise be silently
/// dropped by the column encoder, data loss in a content-addressed node)
/// and the slices must cover the key's bytes exactly.
fn encode_split_keys<'a, Key>(
    schema: &Schema,
    layout: u8,
    keys: impl Iterator<Item = &'a [u8]>,
    count: usize,
) -> Result<Vec<ColumnData>, DialogSearchTreeError>
where
    Key: self::Key,
{
    let mut values: Vec<Vec<&[u8]>> = schema
        .components()
        .iter()
        .map(|_| Vec::with_capacity(count))
        .collect();
    let mut row: Vec<&[u8]> = Vec::with_capacity(schema.len());
    for key in keys {
        row.clear();
        Key::components_of(key, layout, &mut row)?;
        if row.len() != schema.len() {
            return Err(DialogSearchTreeError::Node(format!(
                "Key split into {} components for a schema of {}",
                row.len(),
                schema.len()
            )));
        }
        if row.iter().map(|slice| slice.len()).sum::<usize>() != key.len() {
            return Err(DialogSearchTreeError::Node(
                "Key components do not cover the key's bytes".into(),
            ));
        }
        for (column, slice) in values.iter_mut().zip(&row) {
            column.push(slice);
        }
    }
    encode_column_values(schema, &values)
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
    /// Rough size of each child's subtree, in child order, for query planning.
    ///
    /// Advisory only, and deliberately coarse: a [`Scale`] changes only when a
    /// subtree crosses a bucket boundary, so ordinary edits leave it (and
    /// therefore this node's hash) untouched. An exact count would move on
    /// every insert and dirty the whole root path. Excludes ops pending in
    /// `novelty`, which have not yet reached the subtrees they are destined
    /// for.
    pub scales: Vec<Scale>,
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
    pub fn from_links(links: Vec<Link>) -> Self {
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
        let mut scales = Vec::with_capacity(links.len());
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
            scales.push(link.scale);
        }

        Self {
            prefix,
            suffixes,
            ends,
            hashes,
            scales,
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
    ) -> Result<Self, DialogSearchTreeError> {
        let count = entries.len() as u32;
        let Some(first_layout) = entries.first().map(|entry| entry.key.layout()) else {
            // The empty tree's node: the manifest with no entries (and, being
            // a leaf, no children or novelty). This is the persisted form of
            // an empty tree under every manifest — the format must survive
            // emptiness, or a session reopening the tree would silently
            // continue under the defaults (the manifest-continuity bug the
            // adversarial soak caught). The encoding is fixed — opaque
            // layout, one empty arena column — so every replica's empty tree
            // under a given manifest is byte-identical.
            return Ok(Self::empty());
        };
        let uniform = entries
            .iter()
            .all(|entry| entry.key.layout() == first_layout);

        let (layout, schema) = if uniform {
            (first_layout, Key::schema(first_layout))
        } else {
            (MIXED_LAYOUT, crate::Schema::opaque())
        };

        // Split every key into its component slices, borrowing from the keys.
        // Under the mixed-layout opaque schema, a structured key's own split
        // would push its (varying) components, so the whole key is encoded as
        // the single opaque component directly.
        let columns = if layout == MIXED_LAYOUT {
            let keys: Vec<&[u8]> = entries.iter().map(|entry| entry.key.as_ref()).collect();
            let (prefix, stream) = encode_keys(&keys);
            vec![ColumnData::Arena { prefix, stream }]
        } else {
            encode_split_keys::<Key>(
                &schema,
                layout,
                entries.iter().map(|entry| entry.key.as_ref()),
                entries.len(),
            )?
        };

        let values = entries.into_iter().map(|entry| entry.value).collect();
        Ok(Self {
            count,
            layout,
            columns,
            values,
        })
    }

    /// The zero-entry segment: a tree node that carries the format manifest
    /// and nothing else. It was the empty tree's persisted representation
    /// before that became an index with no children (see
    /// `persist_empty_root`), and still reads as an empty tree. The column set mirrors the [`MIXED_LAYOUT`] opaque schema
    /// (one whole-key arena column, here empty) so decode paths see the
    /// arity they expect.
    pub fn empty() -> Self {
        Self {
            count: 0,
            layout: MIXED_LAYOUT,
            columns: vec![ColumnData::Arena {
                prefix: Vec::new(),
                stream: Vec::new(),
            }],
            values: Vec::new(),
        }
    }
}

/// The body of a tree node, either an index or a leaf segment, together with
/// the tree manifest it is stamped with.
///
/// Load-bearing invariant: a `PersistentNodeBody` is only ever built from typed
/// data (its constructors take entries, links, and buffers), never decoded from
/// raw bytes. [`PersistentNode`]'s [`body`](PersistentNode::body) relies on this
/// for the soundness of its unchecked archive access: because every body is a
/// genuine typed value, the rkyv bytes of its index or segment are by
/// construction a valid archive of that type, so the node sealed from it
/// ([`TryFrom<&PersistentNodeBody<Value>>`](PersistentNode)) needs no
/// revalidation. Do not add a constructor that builds a body from untrusted
/// bytes; that would let a node be sealed around an unvalidated archive and make
/// `body`'s `access_unchecked` unsound. Untrusted bytes must instead go through
/// [`TryFrom<Buffer>`](PersistentNode), which validates.
#[derive(Debug, Clone)]
pub struct PersistentNodeBody<Value> {
    manifest: Manifest,
    node: NodeKind<Value>,
}

impl<Value: self::Value> NodeKind<Value> {
    /// About how many bytes this node encodes to, prelude included: enough
    /// that encoding it rarely reallocates, not so much that a cached node
    /// holds much spare capacity.
    fn size_hint(&self) -> usize {
        const FIXED: usize = 96;
        let columns = |columns: &[ColumnData]| -> usize {
            columns
                .iter()
                .map(|column| match column {
                    ColumnData::Arena { prefix, stream } => 16 + prefix.len() + stream.len(),
                    ColumnData::Dictionary {
                        table,
                        table_ends,
                        indices,
                    } => 24 + table.len() + table_ends.len() + indices.len(),
                })
                .sum()
        };
        let values = |values: &[Value]| -> usize {
            values.iter().map(|value| 8 + value.payload_weight()).sum()
        };
        // Content bytes under-count the archive (lengths, relative pointers,
        // alignment, and value payloads beyond their weight estimate) by about
        // a quarter; an estimate short of the real size costs a reallocation
        // to the next power of two, one over it only the difference.
        let content = match self {
            NodeKind::Index(index) => {
                index.prefix.len()
                    + index.suffixes.len()
                    + 4 * index.ends.len()
                    + 32 * index.hashes.len()
                    + std::mem::size_of::<Scale>() * index.scales.len()
                    + index
                        .novelty
                        .iter()
                        .map(|buffer| {
                            32 + columns(&buffer.columns)
                                + buffer.polarity.len()
                                + values(&buffer.values)
                        })
                        .sum::<usize>()
            }
            NodeKind::Segment(segment) => columns(&segment.columns) + values(&segment.values),
        };
        FIXED + content + content / 3
    }
}

/// The node a [`PersistentNodeBody`] holds. Only the index or segment itself
/// is archived: a tagged node states its kind once, in the prelude.
#[derive(Debug, Clone)]
enum NodeKind<Value> {
    Segment(PersistentSegment<Value>),
    Index(PersistentIndex<Value>),
}

impl<Value> PersistentNodeBody<Value>
where
    Value: self::Value
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    /// Encodes this body as a tagged node: its layout version, kind, header
    /// length and manifest fields, zero padding to a 16-byte boundary, then
    /// the rkyv body. Returns the bytes and the body's offset in them.
    ///
    /// The bytes are the serializer's [`AlignedVec`], so the alignment that
    /// in-place archive access depends on is preserved all the way into the
    /// node [`Buffer`](crate::Buffer); the body is serialized straight after
    /// the padding, at an aligned offset, with no copy.
    pub fn encode(&self) -> Result<(AlignedVec, usize), DialogSearchTreeError> {
        // Sized for the whole node up front: the prelude alone would be a
        // 16-byte allocation that serialization immediately outgrows.
        let mut bytes = AlignedVec::with_capacity(self.node.size_hint());
        write_prelude(
            &mut bytes,
            &self.manifest,
            matches!(self.node, NodeKind::Index(_)),
        );
        let body = bytes.len();
        let bytes = match &self.node {
            NodeKind::Index(index) => rkyv::api::high::to_bytes_in(index, bytes),
            NodeKind::Segment(segment) => rkyv::api::high::to_bytes_in(segment, bytes),
        }
        .map_err(|error: rkyv::rancor::Error| {
            DialogSearchTreeError::Encoding(format!("{error}"))
        })?;
        Ok((bytes, body))
    }

    /// Serializes this node body to its node bytes (see [`encode`](Self::encode)).
    pub fn as_bytes(&self) -> Result<AlignedVec, DialogSearchTreeError> {
        Ok(self.encode()?.0)
    }
}

impl<Value> PersistentNodeBody<Value>
where
    Value: self::Value,
{
    /// Builds an index node body from child links, stamping the tree's format
    /// manifest.
    ///
    /// `novelty` is the buffer of ops pending against this subtree, sorted by
    /// key; it is grouped per child link here (by the same rule routing and a
    /// flush use) and each link's buffer is encoded with the segment codec.
    /// An empty `novelty` yields a canonical (fully flushed) index — THE
    /// canonical byte form, byte-identical to a node built with no buffers.
    pub fn index_from_links<Key>(
        links: Vec<Link>,
        novelty: Vec<NoveltyEntry<Value>>,
        manifest: Manifest,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        let buffers = group_novelty::<Key, Value>(&links, novelty)?;
        Self::index_from_buffers(links, buffers, manifest)
    }

    /// Builds an index node body from child links and per-link novelty buffers
    /// already in stored form, stamping the tree's format manifest.
    ///
    /// This is the persist path for a transient index whose grouping happened
    /// at enqueue time: each buffer is either a sealed stored encoding reused
    /// verbatim or a fresh encode of a touched link, in strictly ascending
    /// child order, and it is embedded without re-encoding anything.
    pub fn index_from_buffers(
        links: Vec<Link>,
        buffers: Vec<NoveltyBuffer<Value>>,
        manifest: Manifest,
    ) -> Result<Self, DialogSearchTreeError> {
        if links.is_empty() {
            return Err(DialogSearchTreeError::Node(
                "Attempted to create an index from zero links".into(),
            ));
        }
        debug_assert!(
            buffers.windows(2).all(|pair| pair[0].child < pair[1].child)
                && buffers
                    .last()
                    .is_none_or(|buffer| (buffer.child as usize) < links.len()),
            "novelty buffers must be in strictly ascending child order within the links"
        );
        let mut index = PersistentIndex::from_links(links);
        index.novelty = buffers;
        Ok(Self {
            manifest,
            node: NodeKind::Index(index),
        })
    }

    /// Wraps a built index under the tree's format manifest.
    pub fn from_index(index: PersistentIndex<Value>, manifest: Manifest) -> Self {
        Self {
            manifest,
            node: NodeKind::Index(index),
        }
    }

    /// Wraps a built segment under the tree's format manifest.
    pub fn from_segment(segment: PersistentSegment<Value>, manifest: Manifest) -> Self {
        Self {
            manifest,
            node: NodeKind::Segment(segment),
        }
    }

    /// The segment this body holds, or `None` for an index.
    #[cfg(test)]
    pub(crate) fn into_segment(self) -> Option<PersistentSegment<Value>> {
        match self.node {
            NodeKind::Segment(segment) => Some(segment),
            NodeKind::Index(_) => None,
        }
    }

    /// Builds a leaf segment node body from entries, stamping the tree's
    /// format manifest. Zero entries build the empty tree's node — the
    /// manifest-carrying format marker (see [`PersistentSegment::empty`]),
    /// legitimate only as the root of an empty tree, never interior.
    pub fn segment_from_entries<Key>(
        entries: Vec<Entry<Key, Value>>,
        manifest: Manifest,
    ) -> Result<Self, DialogSearchTreeError>
    where
        Key: self::Key,
    {
        Ok(Self {
            manifest,
            node: NodeKind::Segment(PersistentSegment::from_entries(entries)?),
        })
    }
}

/// An index node as the legacy (untagged) layout archives it: the manifest as
/// a fixed struct, then the index's fields inline. Read only; nothing writes
/// it.
///
/// The fields repeat [`PersistentIndex`]'s, in order, rather than nesting it:
/// a nested index would share its validation code with tagged nodes, and code
/// with two callers is not inlined, which cost every tagged read. Archived
/// structs are `repr(C)` and every field here aligns to 4, so the fields after
/// the header are laid out exactly as an [`ArchivedIndex`], which is how a
/// legacy node hands one out ([`ArchivedLegacyIndex::index`]). The layout is
/// asserted at compile time below, and the legacy fixtures in
/// `tests/legacy_nodes.rs` pin the bytes.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedLegacyIndex)]
pub struct LegacyIndex<Value> {
    /// The tree's manifest in its legacy form.
    pub header: LegacyManifest,
    /// See [`PersistentIndex::prefix`].
    pub prefix: Vec<u8>,
    /// See [`PersistentIndex::suffixes`].
    pub suffixes: Vec<u8>,
    /// See [`PersistentIndex::ends`].
    pub ends: Vec<u32>,
    /// See [`PersistentIndex::hashes`].
    pub hashes: Vec<Blake3Hash>,
    /// See [`PersistentIndex::scales`].
    pub scales: Vec<Scale>,
    /// See [`PersistentIndex::novelty`].
    pub novelty: Vec<NoveltyBuffer<Value>>,
}

/// A segment as the legacy (untagged) layout archives it: the manifest, then
/// [`PersistentSegment`]'s fields inline. See [`LegacyIndex`] for why the
/// fields repeat rather than nest.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedLegacySegment)]
pub struct LegacySegment<Value> {
    /// The tree's manifest in its legacy form.
    pub header: LegacyManifest,
    /// See [`PersistentSegment::count`].
    pub count: u32,
    /// See [`PersistentSegment::layout`].
    pub layout: u8,
    /// See [`PersistentSegment::columns`].
    pub columns: Vec<ColumnData>,
    /// See [`PersistentSegment::values`].
    pub values: Vec<Value>,
}

impl<Value: self::Value> ArchivedLegacyIndex<Value> {
    /// The index this legacy node holds, read in place.
    pub fn index(&self) -> &ArchivedIndex<Value> {
        // SAFETY: from `prefix` on, this struct's fields are exactly
        // `ArchivedIndex<Value>`'s, in order, at the same relative offsets
        // (both `repr(C)`; asserted below), and `prefix` sits at an offset
        // aligned for it. Relative pointers resolve from their own address,
        // which is unchanged, and the whole struct was validated.
        unsafe { &*std::ptr::from_ref(&self.prefix).cast::<ArchivedIndex<Value>>() }
    }
}

impl<Value: self::Value> ArchivedLegacySegment<Value> {
    /// The segment this legacy node holds, read in place.
    pub fn segment(&self) -> &ArchivedSegment<Value> {
        // SAFETY: as for `ArchivedLegacyIndex::index`, from `count` on.
        unsafe { &*std::ptr::from_ref(&self.count).cast::<ArchivedSegment<Value>>() }
    }
}

// The in-place views above rely on these layouts. Field types archive to the
// same size and alignment whatever `Value` is (relative pointers and lengths),
// so checking one instantiation checks them all.
const _: () = {
    use std::mem::{align_of, offset_of, size_of};
    type Index = ArchivedIndex<Vec<u8>>;
    type Legacy = ArchivedLegacyIndex<Vec<u8>>;
    let base = offset_of!(Legacy, prefix);
    assert!(base % align_of::<Index>() == 0);
    assert!(offset_of!(Legacy, suffixes) == base + offset_of!(Index, suffixes));
    assert!(offset_of!(Legacy, ends) == base + offset_of!(Index, ends));
    assert!(offset_of!(Legacy, hashes) == base + offset_of!(Index, hashes));
    assert!(offset_of!(Legacy, scales) == base + offset_of!(Index, scales));
    assert!(offset_of!(Legacy, novelty) == base + offset_of!(Index, novelty));
    assert!(offset_of!(Index, prefix) == 0);
    assert!(size_of::<Legacy>() >= base + size_of::<Index>());

    type Segment = ArchivedSegment<Vec<u8>>;
    type LegacySeg = ArchivedLegacySegment<Vec<u8>>;
    let base = offset_of!(LegacySeg, count);
    assert!(base % align_of::<Segment>() == 0);
    assert!(offset_of!(LegacySeg, layout) == base + offset_of!(Segment, layout));
    assert!(offset_of!(LegacySeg, columns) == base + offset_of!(Segment, columns));
    assert!(offset_of!(LegacySeg, values) == base + offset_of!(Segment, values));
    assert!(offset_of!(Segment, count) == 0);
    assert!(size_of::<LegacySeg>() >= base + size_of::<Segment>());
};

/// The root of a node in the legacy (untagged) layout: bare rkyv bytes with
/// the manifest inlined. Read only; nothing writes it.
#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
#[rkyv(archived = ArchivedLegacyNodeBody)]
pub enum LegacyNodeBody<Value> {
    /// An index node.
    Index(LegacyIndex<Value>),
    /// A leaf segment.
    Segment(LegacySegment<Value>),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The prelude's kind byte decides which type the body is read as: a
    /// segment whose kind byte is changed to an index's does not read as the
    /// segment it holds.
    #[test]
    fn it_reads_the_body_as_the_kind_the_prelude_states() {
        let good = segment_under(Manifest::default());
        let node = PersistentNode::<[u8; 4], Vec<u8>>::try_from(good.clone()).expect("reads");
        assert!(matches!(node.body(), NodeBody::Segment(_)));
        assert_eq!(u64::from(good.as_ref()[1]), SEGMENT_KIND);
        let mut bytes = rkyv::util::AlignedVec::<16>::new();
        bytes.extend_from_slice(good.as_ref());
        bytes[1] = INDEX_KIND as u8;
        assert!(PersistentNode::<[u8; 4], Vec<u8>>::try_from(Buffer::from(bytes)).is_err());
    }

    fn segment_under(manifest: Manifest) -> Buffer {
        let entries = (0u8..4).map(|i| Entry::new([i; 4], vec![i; 8])).collect();
        let body: PersistentNodeBody<Vec<u8>> =
            PersistentNodeBody::segment_from_entries::<[u8; 4]>(entries, manifest)
                .expect("a segment encodes");
        Buffer::from(body.as_bytes().expect("encodes"))
    }

    /// Nodes of trees under different non-default manifests, read alternately,
    /// each report their own manifest: the remembered prelude of one tree
    /// never stands in for another's.
    #[test]
    fn it_reads_alternating_non_default_manifests() {
        let four = Manifest {
            fanout_n: 4,
            ..Manifest::default()
        };
        let five = Manifest {
            fanout_n: 5,
            ..Manifest::default()
        };
        let (a, b) = (segment_under(four.clone()), segment_under(five.clone()));
        for _ in 0..3 {
            for (buffer, manifest) in [(&a, &four), (&b, &five), (&b, &five), (&a, &four)] {
                let node =
                    PersistentNode::<[u8; 4], Vec<u8>>::try_from(buffer.clone()).expect("reads");
                assert_eq!(&node.manifest().expect("has a manifest"), manifest);
            }
        }
    }

    /// A node whose prelude differs from the remembered one only in a padding
    /// byte is refused, not accepted by the memo.
    #[test]
    fn it_refuses_a_remembered_prelude_with_nonzero_padding() {
        let four = Manifest {
            fanout_n: 4,
            ..Manifest::default()
        };
        let good = segment_under(four);
        PersistentNode::<[u8; 4], Vec<u8>>::try_from(good.clone()).expect("reads");
        let mut bytes = rkyv::util::AlignedVec::<16>::new();
        bytes.extend_from_slice(good.as_ref());
        assert_eq!(
            bytes[BODY_ALIGNMENT - 1],
            0,
            "the last prelude byte is padding"
        );
        bytes[BODY_ALIGNMENT - 1] = 1;
        assert!(PersistentNode::<[u8; 4], Vec<u8>>::try_from(Buffer::from(bytes)).is_err());
    }
}
