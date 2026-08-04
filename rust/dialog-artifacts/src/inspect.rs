//! Tree-node inspection: the [`Load`] effect and pure node decoders.
//!
//! Together these back the query engine's tree procedures (`tree/node`,
//! `tree/link`, `tree/key`): [`Load`] fetches a raw node block by content
//! hash through the evaluation environment, and the `inspect_*` functions
//! project structure out of the fetched bytes without touching storage.
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
use dialog_search_tree::{Buffer, Distribution, Geometric, PersistentNode, Rank};
use dialog_storage::Blake3Hash;

use crate::{
    ATTRIBUTE_KEY_TAG, AttributeKey, BLOB_KEY_TAG, COVERAGE_KEY_TAG, Datum, DialogArtifactsError,
    ENTITY_KEY_TAG, EntityKey, HISTORY_KEY_TAG, Key, KeyView, State, VALUE_KEY_TAG, Value,
    ValueKey, decode_value,
};

/// The raw content hash a [`Load`] resolves: the same 32 bytes a
/// revision's tree reference and an index link's child hash carry.
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
    /// `"index"` for a node of child links, `"segment"` for a leaf of
    /// entries.
    pub kind: &'static str,
    /// Serialized block size in bytes (what a fetch pays for).
    pub size: u64,
    /// Child-link count for an index, entry count for a segment.
    pub count: u64,
    /// The node's upper-bound key: the last entry key of a segment.
    /// Empty for an index node (its table holds separators, not whole
    /// keys) — the per-link separators are exposed by [`inspect_links`].
    pub bound: Vec<u8>,
    /// Rank of the upper bound under the node's own embedded manifest
    /// (0 when there is no bound). Higher rank ⇒ higher boundary in the
    /// tree.
    pub rank: Rank,
    /// The node's [`Scale`](dialog_search_tree::Scale) code: a one-byte
    /// log-scale estimate of the subtree's entry count.
    pub scale: u64,
    /// Buffered hitchhiker ops riding this node (always 0 for a
    /// segment): the window into buffered-vs-canonical cost.
    pub novelty: u64,
}

/// Structural description of one child link of an index node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkSummary {
    /// Position among siblings.
    pub at: u64,
    /// Content hash of the referenced child node.
    pub node: Blake3Hash,
    /// The link's separator: the left-edge boundary of the referenced
    /// subtree, a front-coded prefix of its minimum leaf key. Empty for
    /// the level's global leftmost link (reads as negative infinity).
    pub separator: Vec<u8>,
    /// The child subtree's advisory [`Scale`](dialog_search_tree::Scale)
    /// code.
    pub scale: u64,
    /// Rank of the separator under the node's embedded manifest: the
    /// seam level this boundary falls on.
    pub rank: Rank,
    /// Buffered hitchhiker ops pending against this subtree (0 when the
    /// link carries no buffer). Covered by the node's hash, so as
    /// immutable as every other field.
    pub novelty: u64,
}

/// Decode the node behind `bytes` into its [`NodeSummary`].
pub fn inspect_node(bytes: Vec<u8>) -> Result<NodeSummary, DialogArtifactsError> {
    let size = bytes.len() as u64;
    let node = ArtifactNode::new(Buffer::from(bytes));
    let manifest = node.manifest()?;
    let scale = node.scale()?.as_u8() as u64;
    let bound = node.upper_bound()?.unwrap_or_default();
    let rank = if bound.is_empty() {
        0
    } else {
        Geometric::rank(&bound, &manifest)
    };

    Ok(match node.as_index() {
        Ok(index) => NodeSummary {
            kind: "index",
            size,
            count: index.len() as u64,
            bound,
            rank,
            scale,
            novelty: index.novelty_len() as u64,
        },
        Err(_) => {
            let segment = node.as_segment()?;
            NodeSummary {
                kind: "segment",
                size,
                count: segment.len() as u64,
                bound,
                rank,
                scale,
                novelty: 0,
            }
        }
    })
}

/// Decode the index node behind `bytes` into one [`LinkSummary`] per
/// child link, in position order. A segment has no links and yields an
/// empty vector (not an error — queries union over mixed levels).
pub fn inspect_links(bytes: Vec<u8>) -> Result<Vec<LinkSummary>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    let Ok(index) = node.as_index() else {
        return Ok(Vec::new());
    };
    let manifest = node.manifest()?;

    let mut links = Vec::with_capacity(index.len());
    for at in 0..index.len() {
        let separator = index.separator(at)?;
        let rank = if separator.is_empty() {
            0
        } else {
            Geometric::rank(&separator, &manifest)
        };
        links.push(LinkSummary {
            at: at as u64,
            node: *index.hash_at(at)?.as_bytes(),
            separator,
            scale: index.scale_at(at)?.as_u8() as u64,
            rank,
            novelty: index
                .buffer_for(at)
                .map(|buffer| buffer.count.to_native() as u64)
                .unwrap_or(0),
        });
    }
    Ok(links)
}

/// Decode the segment node behind `bytes` into its entry keys, in entry
/// order. An index node has no entries and yields an empty vector.
pub fn inspect_keys(bytes: Vec<u8>) -> Result<Vec<Vec<u8>>, DialogArtifactsError> {
    let node = ArtifactNode::new(Buffer::from(bytes));
    if node.as_index().is_ok() {
        return Ok(Vec::new());
    }
    let segment = node.as_segment()?;
    let mut keys = Vec::with_capacity(segment.len());
    let mut cursor = segment.keys::<Key>()?;
    while let Some((_, key)) = cursor.next_key()? {
        keys.push(key.to_vec());
    }
    Ok(keys)
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

/// Decompose a link separator. A separator is a front-coded *prefix*
/// of a full key: the column framing a full-key parse relies on lies
/// past the truncation, so this is deliberately lenient — the tag
/// component plus the post-tag prefix bytes as one `prefix` component
/// (utf8-lossy text, since the leading columns are textual for every
/// fact ordering). The empty separator is the level's global leftmost
/// boundary and yields the `min` marker.
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

    /// Per-link novelty surfaces on link rows: ops route to the link
    /// whose range holds their key, the counts land per link, and the
    /// node summary carries the total.
    #[test]
    fn it_reports_per_link_novelty() -> anyhow::Result<()> {
        // Two keys in sort order; the second link's separator is the
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
                key: high,
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

        let links = inspect_links(bytes)?;
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].novelty, 1, "one op rides the left link");
        assert_eq!(links[1].novelty, 1, "one op rides the right link");
        Ok(())
    }

    /// Key decomposition helpers: a full entity key parses into its
    /// components; separators stay lenient.
    #[test]
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
}
