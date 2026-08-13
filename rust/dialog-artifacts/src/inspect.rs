//! Tree-node inspection: the [`Load`] effect and pure node decoders.
//!
//! Together these back the query engine's tree procedures (`tree/node`,
//! `tree/span`, `tree/key`, `tree/manifest`): [`Load`] fetches a raw
//! node block by content hash through the evaluation environment, and
//! the `inspect_*` functions project the node's *logical model* out of
//! the fetched bytes without touching storage:
//!
//! - a node is an index (a table of spans) or a segment (a run of
//!   entries), with a byte size, a scale estimate, and pending novelty;
//! - an index's spans each delegate a key range `[separator, until)` to
//!   a child, carry the ops buffered against that range, and exist as
//!   boundaries because the seam coin ranked their separator;
//! - a segment's entries are keys, each ranked by the leaf coin;
//! - every node embeds the format [`Manifest`] it was written under,
//!   making a bare hash a self-describing root.
//!
//! # Why this is sound under differential subscriptions
//!
//! [`Load`] is *idempotent*: a node block is content-addressed, so the
//! bytes behind a hash — and therefore every row projected from them —
//! can never change. A different tree is a different hash. This includes
//! buffered novelty: a node's hash covers the ops riding on it. Rows
//! derived through `Load` are permanent; they can become unnecessary,
//! never wrong, so no invalidation machinery is required for them. The
//! only mutable fact in the domain is "what is the current root?", which
//! reaches queries as an ordinary tracked fact (`dialog.branch/tree`),
//! never through this effect. Contrast a locality probe ("is this block
//! cached here?"): that answer changes without a commit, is *not*
//! idempotent, and must not be served through this module.

use dialog_capability::Command;
use dialog_search_tree::{Buffer, Distribution, Geometric, Manifest, PersistentNode, Rank};
use dialog_storage::Blake3Hash;
use rkyv::deserialize;
use rkyv::rancor::Error as RkyvError;

use crate::key::varkey::{self, ValuePayload};
use crate::{
    ATTRIBUTE_KEY_TAG, AttributeKey, BLOB_KEY_TAG, BlobRecord, COVERAGE_KEY_TAG, Datum,
    DialogArtifactsError, ENTITY_KEY_TAG, EntityKey, HISTORY_KEY_TAG, Key, KeyView, State,
    VALUE_KEY_TAG, Value, ValueKey, decode_value,
};

/// The raw content hash a [`Load`] resolves: the same 32 bytes a
/// revision's tree reference and an index span's child hash carry.
pub type NodeReference = Blake3Hash;

/// Command for loading a raw tree node block by its content hash.
///
/// The counterpart of [`Select`](crate::Select) for the tree procedures:
/// where `Select` scans key ranges and yields artifacts, `Load` fetches
/// one content-addressed block for structural inspection. `Ok(None)`
/// means the block is not available anywhere the provider can reach —
/// the unreplicated-contributes-nothing convention.
pub struct Load;

impl Command for Load {
    type Input = Blake3Hash;
    type Output = Result<Option<Vec<u8>>, DialogArtifactsError>;
}

/// The node type the artifact tree persists, instantiated for inspection.
type ArtifactNode = PersistentNode<Key, State<Datum>>;

/// Structural description of one persisted node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSummary {
    /// `"index"` for a table of spans, `"segment"` for a run of
    /// entries.
    pub kind: &'static str,
    /// Serialized block size in bytes (what a fetch pays for).
    pub size: u64,
    /// Span count for an index, entry count for a segment.
    pub count: u64,
    /// The node's [`Scale`](dialog_search_tree::Scale) code: a one-byte
    /// log-scale estimate of the subtree's entry count. Advisory — an
    /// upper bound that excludes ops still pending in novelty buffers.
    pub scale: u64,
    /// Buffered hitchhiker ops riding this node, summed over its spans
    /// (always 0 for a segment): the window into buffered-vs-canonical
    /// cost.
    pub novelty: u64,
}

/// One span of an index node: the key range the node delegates to one
/// child, with everything pending against it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpanSummary {
    /// Position among the node's spans.
    pub at: u64,
    /// Content hash of the child node rooting the span's subtree.
    pub node: Blake3Hash,
    /// The span's lower bound: a front-coded prefix of its subtree's
    /// minimum leaf key. Empty for the leftmost span (reads as −∞).
    pub separator: Vec<u8>,
    /// The span's upper bound: the next span's separator. Empty for the
    /// last span (reads as +∞). Together with `separator` this makes
    /// each row a self-contained range `[separator, until)`.
    pub until: Vec<u8>,
    /// The subtree's advisory [`Scale`](dialog_search_tree::Scale)
    /// code.
    pub scale: u64,
    /// Seam rank of the separator under the node's embedded manifest:
    /// the level coin that made this boundary exist. 0 for the leftmost
    /// span (no boundary) and for separators past the manifest's
    /// `max_separator` (forced backstop seams).
    pub rank: Rank,
    /// Buffered hitchhiker ops pending against this span (0 when it
    /// carries no buffer). Covered by the node's hash, so as immutable
    /// as every other field.
    pub novelty: u64,
}

/// One entry of a segment node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySummary {
    /// The entry's variable-length index key bytes.
    pub key: Vec<u8>,
    /// The key's leaf-coin rank under the node's embedded manifest:
    /// what decides whether a leaf boundary forms after this entry.
    pub rank: Rank,
}

/// One entry of a segment node with its stored claim metadata — the
/// value side of the entry, where the fact content itself lives in the
/// key (see [`inspect_keys`] / `dialog/key-part`). This is what makes
/// the history and coverage regions legible: their entries' versions,
/// causes, and coverage all ride here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntrySummary {
    /// Position within the segment.
    pub at: u64,
    /// The entry's variable-length index key bytes.
    pub key: Vec<u8>,
    /// `"asserted"` for a stored datum, `"removed"` for a tombstone.
    pub state: &'static str,
    /// Whether the datum marks a retraction (a covering record in the
    /// history/coverage regions).
    pub retraction: bool,
    /// Origin half of the claim's version (32 bytes; empty when the
    /// write was unversioned).
    pub origin: Vec<u8>,
    /// Edition half of the claim's version (0 when unversioned).
    pub edition: u64,
    /// Number of prior claim versions in the entry's cause.
    pub cause: u64,
    /// Extra claim versions collapsed into this entry (identical-value
    /// claims from other writers standing at the same key).
    pub collapsed: u64,
    /// Versions a covering record supersedes.
    pub supersedes: u64,
    /// The spilled value's 32-byte block reference, when the key's
    /// value spilled past the manifest's inline threshold.
    pub spill: Option<Blake3Hash>,
}

/// One blob-index entry of a segment node: the content-derived
/// metadata the tree stores about a referenced blob. The blob's bytes
/// themselves live in the blob store, not the tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobEntrySummary {
    /// Position within the segment.
    pub at: u64,
    /// The referenced blob's 32-byte content hash, from the key.
    pub blob: Vec<u8>,
    /// The record's encoding version.
    pub version: u64,
    /// Total size of the blob in bytes.
    pub size: u64,
}

/// The format manifest a node embeds, field for field (see
/// [`Manifest`]). Every node carries one, so a bare node hash is a
/// self-describing root and mixed-format trees are visible per node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestSummary {
    /// Format version; pins how the rest of the node is interpreted.
    pub version: u64,
    /// Branching parameter `n`; expected fanout is `2^n`.
    pub fanout_n: u64,
    /// Keys longer than this never become boundaries.
    pub max_separator: u64,
    /// Values longer than this spill to the block store.
    pub inline_n: u64,
    /// Leading raw value bytes a spilled value's key keeps as prefix.
    pub spill_prefix: u64,
    /// Leaf-run weight cap; 0 disables it.
    pub max_segment: u64,
    /// Hard frame-weight ceiling as a multiple of `max_segment`; 0
    /// disables it.
    pub frame_ceiling_factor: u64,
    /// Which candidate seam a forced cut anchors at (0 = rendezvous,
    /// 1 = hybrid).
    pub anchor_selector: u64,
}

/// Decode the node behind `bytes` into its [`NodeSummary`].
pub fn inspect_node(bytes: Vec<u8>) -> Result<NodeSummary, DialogArtifactsError> {
    let size = bytes.len() as u64;
    let node = ArtifactNode::new(Buffer::from(bytes));
    let scale = node.scale()?.as_u8() as u64;

    Ok(match node.as_index() {
        Ok(index) => NodeSummary {
            kind: "index",
            size,
            count: index.len() as u64,
            scale,
            novelty: index.novelty_len() as u64,
        },
        Err(_) => {
            let segment = node.as_segment()?;
            NodeSummary {
                kind: "segment",
                size,
                count: segment.len() as u64,
                scale,
                novelty: 0,
            }
        }
    })
}

/// Decode the index node behind `bytes` into one [`SpanSummary`] per
/// span, in key order. A segment has no spans and yields an empty
/// vector (not an error — queries union over mixed levels).
pub fn inspect_spans(bytes: Vec<u8>) -> Result<Vec<SpanSummary>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    let Ok(index) = node.as_index() else {
        return Ok(Vec::new());
    };
    let manifest = node.manifest()?;

    let mut spans = Vec::with_capacity(index.len());
    for at in 0..index.len() {
        let separator = index.separator(at)?;
        // The span's upper bound is the NEXT span's separator; the last
        // span is unbounded above (empty = +∞).
        let until = if at + 1 < index.len() {
            index.separator(at + 1)?
        } else {
            Vec::new()
        };
        let rank = if separator.is_empty() {
            0
        } else {
            Geometric::seam_rank(&separator, &manifest)
        };
        spans.push(SpanSummary {
            at: at as u64,
            node: *index.hash_at(at)?.as_bytes(),
            separator,
            until,
            scale: index.scale_at(at)?.as_u8() as u64,
            rank,
            novelty: index
                .buffer_for(at)
                .map(|buffer| buffer.count.to_native() as u64)
                .unwrap_or(0),
        });
    }
    Ok(spans)
}

/// Decode the segment node behind `bytes` into one [`KeySummary`] per
/// entry, in entry order. An index node has no entries and yields an
/// empty vector.
pub fn inspect_keys(bytes: Vec<u8>) -> Result<Vec<KeySummary>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    if node.as_index().is_ok() {
        return Ok(Vec::new());
    }
    let manifest = node.manifest()?;
    let segment = node.as_segment()?;
    let mut keys = Vec::with_capacity(segment.len());
    let mut cursor = segment.keys::<Key>()?;
    while let Some((_, key)) = cursor.next_key()? {
        let key = key.to_vec();
        let rank = Geometric::rank(&key, &manifest);
        keys.push(KeySummary { key, rank });
    }
    Ok(keys)
}

/// Decode the segment node behind `bytes` into one [`EntrySummary`]
/// per entry, in entry order — the claim-metadata (value) side of each
/// entry. Blob-index entries carry no claim metadata and surface
/// through [`inspect_blob_records`] instead; they still appear here
/// (with empty metadata) so counts line up. An index node yields an
/// empty vector.
pub fn inspect_entries(bytes: Vec<u8>) -> Result<Vec<EntrySummary>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    if node.as_index().is_ok() {
        return Ok(Vec::new());
    }
    let segment = node.as_segment()?;
    let mut entries = Vec::with_capacity(segment.len());
    let mut cursor = segment.keys::<Key>()?;
    while let Some((at, key)) = cursor.next_key()? {
        let key = key.to_vec();
        // Lenient spill detection: only fact-shaped keys carry a value
        // payload; anything that does not parse is simply not spilled.
        let spill = varkey::parse_key(&key).and_then(|parts| match parts.value {
            ValuePayload::Spilled { hash, .. } => Blake3Hash::try_from(hash.as_slice()).ok(),
            ValuePayload::Inline(_) => None,
        });
        let state: State<Datum> = deserialize::<State<Datum>, RkyvError>(segment.value_at(at)?)
            .map_err(|error| DialogArtifactsError::Tree(format!("entry decode: {error}")))?;
        entries.push(match state {
            State::Added(datum) => EntrySummary {
                at: at as u64,
                key,
                state: "asserted",
                retraction: datum.retraction,
                origin: datum
                    .version
                    .as_ref()
                    .map(|version| version.origin.0.to_vec())
                    .unwrap_or_default(),
                edition: datum
                    .version
                    .as_ref()
                    .map(|version| version.edition.value())
                    .unwrap_or_default(),
                cause: datum
                    .cause
                    .as_ref()
                    .map(|cause| cause.len() as u64)
                    .unwrap_or_default(),
                collapsed: datum.collapsed.len() as u64,
                supersedes: datum.supersedes.len() as u64,
                spill,
            },
            State::Removed => EntrySummary {
                at: at as u64,
                key,
                state: "removed",
                retraction: false,
                origin: Vec::new(),
                edition: 0,
                cause: 0,
                collapsed: 0,
                supersedes: 0,
                spill,
            },
        });
    }
    Ok(entries)
}

/// Decode the segment node behind `bytes` into one [`BlobEntrySummary`]
/// per blob-index entry. Non-blob entries (and index nodes) yield no
/// rows.
pub fn inspect_blob_records(bytes: Vec<u8>) -> Result<Vec<BlobEntrySummary>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    if node.as_index().is_ok() {
        return Ok(Vec::new());
    }
    let segment = node.as_segment()?;
    let mut records = Vec::new();
    let mut cursor = segment.keys::<Key>()?;
    while let Some((at, key)) = cursor.next_key()? {
        if key.first() != Some(&BLOB_KEY_TAG) {
            continue;
        }
        let state: State<Datum> = deserialize::<State<Datum>, RkyvError>(segment.value_at(at)?)
            .map_err(|error| DialogArtifactsError::Tree(format!("entry decode: {error}")))?;
        if let Some(record) = BlobRecord::from_state(&state)? {
            records.push(BlobEntrySummary {
                at: at as u64,
                blob: key[1..].to_vec(),
                version: record.version as u64,
                size: record.size,
            });
        }
    }
    Ok(records)
}

/// Decode the manifest embedded in the node behind `bytes`.
pub fn inspect_manifest(bytes: Vec<u8>) -> Result<ManifestSummary, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    let manifest: Manifest = node.manifest()?;
    Ok(ManifestSummary {
        version: manifest.version as u64,
        fanout_n: manifest.fanout_n as u64,
        max_separator: manifest.max_separator as u64,
        inline_n: manifest.inline_n as u64,
        spill_prefix: manifest.spill_prefix as u64,
        max_segment: manifest.max_segment as u64,
        frame_ceiling_factor: manifest.frame_ceiling_factor as u64,
        anchor_selector: manifest.anchor_selector as u64,
    })
}

/// One decoded component of an index key, in sort order. `kind` names
/// what the component is (and selects an inspector's color/glyph),
/// `text` is the human rendering, `bytes` the raw component bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyComponent {
    /// What this component is: `index`, `entity`, `attribute`,
    /// `vtype`, `value`, `spill`, `origin`, `edition`, `blob`,
    /// `prefix`, `min`, or `opaque`.
    pub kind: &'static str,
    /// Human rendering of the component.
    pub text: String,
    /// The raw component bytes.
    pub bytes: Vec<u8>,
}

impl KeyComponent {
    fn new(kind: &'static str, text: String, bytes: Vec<u8>) -> Self {
        Self { kind, text, bytes }
    }
}

/// Lowercase hex of `bytes`.
fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

/// The human name of a key's index ordering, from its leading tag byte.
fn tag_name(tag: u8) -> &'static str {
    match tag {
        ENTITY_KEY_TAG => "entity",
        ATTRIBUTE_KEY_TAG => "attribute",
        VALUE_KEY_TAG => "value",
        HISTORY_KEY_TAG => "history",
        BLOB_KEY_TAG => "blob",
        COVERAGE_KEY_TAG => "coverage",
        _ => "unknown",
    }
}

/// Compact human rendering of a decoded value, typed by formatting:
/// strings quoted, entities as their URI, bytes and records as hex.
fn render_value(value: &Value) -> String {
    match value {
        Value::String(text) => format!("{text:?}"),
        Value::Entity(entity) => entity.to_string(),
        Value::Symbol(symbol) => symbol.to_string(),
        Value::Boolean(boolean) => boolean.to_string(),
        Value::UnsignedInt(number) => number.to_string(),
        Value::SignedInt(number) => format!("{number:+}"),
        Value::Float(number) => format!("{number:?}"),
        Value::Bytes(bytes) => hex(bytes),
        Value::Record(bytes) => format!("record:{}", hex(bytes)),
    }
}

/// The four logical fact fields a `KeyView` reads, in EAV logical
/// order; per-tag arms reorder them into that ordering's sort order.
fn fact_fields<V: KeyView>(view: &V) -> [KeyComponent; 4] {
    let entity = view.entity();
    let attribute = view.attribute();
    let vtype = view.value_type();
    let value = if view.value_is_spilled() {
        let hash = view.value_spill_hash().unwrap_or_default();
        KeyComponent::new("spill", format!("spill:{}", hex(hash)), hash.to_vec())
    } else {
        let payload = view.value_payload();
        let text = match decode_value(vtype, payload) {
            Some((value, _)) => render_value(&value),
            None => hex(payload),
        };
        KeyComponent::new("value", text, payload.to_vec())
    };
    [
        KeyComponent::new(
            "entity",
            String::from_utf8_lossy(entity.raw()).into_owned(),
            entity.raw().to_vec(),
        ),
        KeyComponent::new(
            "attribute",
            String::from_utf8_lossy(attribute.raw()).into_owned(),
            attribute.raw().to_vec(),
        ),
        KeyComponent::new("vtype", vtype.to_string(), vec![vtype as u8]),
        value,
    ]
}

/// Push the fact fields in `order` (indices into EAV logical order).
fn push_fields(out: &mut Vec<KeyComponent>, order: [usize; 4], fields: [KeyComponent; 4]) {
    let mut fields: Vec<Option<KeyComponent>> = fields.into_iter().map(Some).collect();
    for at in order {
        if let Some(field) = fields[at].take() {
            out.push(field);
        }
    }
}

/// Byte length of the version prefix (origin ‖ edition) that follows
/// the tag in history/coverage keys.
const VERSION_PREFIX: usize = 32 + 8;

/// Decompose a full, variable-length index key into components, in the
/// key's own sort order. A key that does not parse under its tag's
/// schema (unknown tag, truncated history prefix) yields a single
/// `opaque` component — never zero components for non-empty input, so
/// an inspector always has something to show. Empty input yields the
/// `min` marker (the −∞ boundary).
pub fn key_components(bytes: &[u8]) -> Vec<KeyComponent> {
    if bytes.is_empty() {
        return vec![KeyComponent::new("min", "⊥ start".into(), Vec::new())];
    }
    let tag = bytes[0];
    let mut out = vec![KeyComponent::new("index", tag_name(tag).into(), vec![tag])];
    let key = Key::from(bytes.to_vec());

    match tag {
        ENTITY_KEY_TAG => push_fields(&mut out, [0, 1, 2, 3], fact_fields(&EntityKey(&key))),
        ATTRIBUTE_KEY_TAG => push_fields(&mut out, [1, 0, 2, 3], fact_fields(&AttributeKey(&key))),
        VALUE_KEY_TAG => push_fields(&mut out, [2, 3, 1, 0], fact_fields(&ValueKey(&key))),
        // History / coverage: tag ‖ origin(32) ‖ edition(8, BE) ‖ the
        // fact fields under the entity (EAV) ordering.
        HISTORY_KEY_TAG | COVERAGE_KEY_TAG => {
            if bytes.len() < 1 + VERSION_PREFIX {
                return vec![KeyComponent::new("opaque", hex(bytes), bytes.to_vec())];
            }
            let origin = &bytes[1..33];
            let edition = u64::from_be_bytes(bytes[33..41].try_into().unwrap_or_default());
            out.push(KeyComponent::new(
                "origin",
                format!("origin:{}", hex(origin)),
                origin.to_vec(),
            ));
            out.push(KeyComponent::new(
                "edition",
                format!("@{edition}"),
                bytes[33..41].to_vec(),
            ));
            let tail = &bytes[1 + VERSION_PREFIX..];
            if !tail.is_empty() {
                let mut synthetic = Vec::with_capacity(tail.len() + 1);
                synthetic.push(ENTITY_KEY_TAG);
                synthetic.extend_from_slice(tail);
                let tail_key = Key::from(synthetic);
                push_fields(&mut out, [0, 1, 2, 3], fact_fields(&EntityKey(&tail_key)));
            }
        }
        // Blob: tag ‖ blob hash, one content-addressed reference.
        BLOB_KEY_TAG => {
            let hash = &bytes[1..];
            out.push(KeyComponent::new(
                "blob",
                format!("blob:{}", hex(hash)),
                hash.to_vec(),
            ));
        }
        _ => return vec![KeyComponent::new("opaque", hex(bytes), bytes.to_vec())],
    }
    out
}

/// Decompose a span separator. A separator is a front-coded *prefix*
/// of a full key: the column framing a full-key parse relies on lies
/// past the truncation, so this is deliberately lenient — the tag
/// component plus the post-tag prefix bytes as one `prefix` component
/// (utf8-lossy text, since the leading columns are textual for every
/// fact ordering). The empty separator is the leftmost span's boundary
/// and yields the `min` marker.
pub fn separator_components(bytes: &[u8]) -> Vec<KeyComponent> {
    if bytes.is_empty() {
        return vec![KeyComponent::new("min", "⊥ start".into(), Vec::new())];
    }
    let tag = bytes[0];
    let mut out = vec![KeyComponent::new("index", tag_name(tag).into(), vec![tag])];
    let prefix = &bytes[1..];
    if !prefix.is_empty() {
        out.push(KeyComponent::new(
            "prefix",
            String::from_utf8_lossy(prefix).into_owned(),
            prefix.to_vec(),
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_common::Blake3Hash as NodeHash;
    use dialog_search_tree::{Link, Manifest, NoveltyEntry, NoveltyOp, PersistentNodeBody, Scale};

    use super::*;
    use crate::{Artifact, Entity, EntityKey, KeyType as _, Value as ArtifactValue};

    /// An entity-ordered key for a `test/name` fact on a fresh entity.
    fn artifact_key(name: &str) -> Vec<u8> {
        let fact = Artifact {
            the: "test/name".parse().expect("attribute parses"),
            of: Entity::new().expect("entity mints"),
            is: ArtifactValue::String(name.into()),
            cause: None,
        };
        EntityKey::from_artifact(&fact, &Manifest::default())
            .into_key()
            .bytes()
            .to_vec()
    }

    /// Spans surface the logical range model: each row carries its
    /// `[separator, until)` bounds, its per-span novelty, and the node
    /// summary carries the total.
    #[dialog_common::test]
    fn it_reports_spans_with_ranges_and_novelty() -> anyhow::Result<()> {
        // Two keys in sort order; the second span's separator is the
        // larger key itself (a key is a prefix of itself), so `low`
        // routes left and `high` routes right.
        let mut keys = [artifact_key("a"), artifact_key("b")];
        keys.sort();
        let [low, high] = keys;

        let links = vec![
            Link {
                separator: Vec::new(),
                node: NodeHash::hash(b"left"),
                scale: Scale::EMPTY,
            },
            Link {
                separator: high.clone(),
                node: NodeHash::hash(b"right"),
                scale: Scale::EMPTY,
            },
        ];
        // Tombstone ops need no value, keeping the fixture free of
        // Datum construction. One op left, one right.
        let novelty = vec![
            NoveltyEntry {
                key: low,
                op: NoveltyOp::Retract,
            },
            NoveltyEntry {
                key: high.clone(),
                op: NoveltyOp::Retract,
            },
        ];
        let body = PersistentNodeBody::<State<Datum>>::index_from_links::<Key>(
            links,
            novelty,
            Manifest::default(),
        )?;
        let bytes: Vec<u8> = body.as_bytes()?.as_ref().to_vec();

        let node = inspect_node(bytes.clone())?;
        assert_eq!(node.kind, "index");
        assert_eq!(node.novelty, 2, "total buffered ops: {node:?}");

        let spans = inspect_spans(bytes.clone())?;
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].novelty, 1, "one op pends on the left span");
        assert_eq!(spans[1].novelty, 1, "one op pends on the right span");
        // Each span is a self-contained range: the left span ends where
        // the right begins; the outer bounds are open (−∞, +∞).
        assert!(spans[0].separator.is_empty(), "leftmost is −∞");
        assert_eq!(spans[0].until, high, "left span ends at the boundary");
        assert_eq!(spans[1].separator, high);
        assert!(spans[1].until.is_empty(), "last span is +∞");

        let manifest = inspect_manifest(bytes)?;
        assert_eq!(manifest.version, Manifest::default().version as u64);
        assert_eq!(manifest.fanout_n, Manifest::default().fanout_n as u64);
        Ok(())
    }

    /// Key decomposition helpers: a full entity key parses into its
    /// components; separators stay lenient.
    #[dialog_common::test]
    fn it_decomposes_key_components() {
        let key = artifact_key("value");
        let components = key_components(&key);
        let kinds: Vec<&str> = components.iter().map(|part| part.kind).collect();
        assert_eq!(
            kinds,
            vec!["index", "entity", "attribute", "vtype", "value"]
        );
        assert_eq!(components[2].text, "test/name");

        let separator = separator_components(&key[..7]);
        assert_eq!(separator[0].kind, "index");
        assert_eq!(separator[1].kind, "prefix");

        assert_eq!(separator_components(&[])[0].kind, "min");
    }

    /// Blob-index entries decode into their content-derived records:
    /// hash from the key, `{version, size}` from the stored record.
    #[dialog_common::test]
    fn it_reports_blob_records() -> anyhow::Result<()> {
        use dialog_search_tree::Entry;

        let hash = NodeHash::hash(b"blob-bytes");
        let key = crate::BlobKey::new(hash.as_bytes()).0;
        let mut record = vec![1u8];
        record.extend_from_slice(&5u64.to_be_bytes());
        let datum = Datum {
            cause: None,
            blob: Some(record),
            version: None,
            collapsed: Vec::new(),
            supersedes: Vec::new(),
            retraction: false,
        };
        let entries = vec![Entry {
            key,
            value: State::Added(datum),
        }];
        let body = PersistentNodeBody::<State<Datum>>::segment_from_entries::<Key>(
            entries,
            Manifest::default(),
        )?;
        let bytes: Vec<u8> = body.as_bytes()?.as_ref().to_vec();

        let records = inspect_blob_records(bytes.clone())?;
        assert_eq!(records.len(), 1, "one blob record: {records:?}");
        assert_eq!(records[0].size, 5);
        assert_eq!(records[0].version, 1);
        assert_eq!(records[0].blob, hash.as_bytes().to_vec());

        // The generic entry view counts it too (with empty claim
        // metadata), so per-node counts line up.
        let generic = inspect_entries(bytes)?;
        assert_eq!(generic.len(), 1);
        assert_eq!(generic[0].origin, Vec::<u8>::new());
        Ok(())
    }
}
