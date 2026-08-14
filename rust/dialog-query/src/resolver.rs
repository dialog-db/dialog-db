//! Resolver premises: moded, multi-row premises resolved by selecting
//! from content-addressed storage through the evaluation environment.
//!
//! Scans and resolvers are the same kind of thing — selects the
//! environment answers — distinguished by the *address space* they
//! select from, and every difference between them falls out of that
//! one split:
//!
//! - A **scan** selects by key range over the mutable head of the
//!   indexes ([`Select`](dialog_artifacts::Select)). The head moves,
//!   so its result set can be invalidated — hence demand covers,
//!   recorded per scanned range. A range can also be enumerated, so
//!   an unconstrained scan is expensive but defined.
//! - A **resolver** selects by content address over the immutable block
//!   universe ([`Load`](dialog_artifacts::inspect::Load)). A hash
//!   resolves to the same bytes forever, so a resolver's rows are a pure
//!   function of its bound inputs — no fact demand exists to record
//!   (see `dialog_artifacts::inspect` for the full soundness
//!   argument); what changes over time is *reachability* — which
//!   root the branch head names — and the revision-anchored
//!   subscription (`Demand::head`) tracks exactly that. A content
//!   address cannot be enumerated, so an unbound resolver is non-viable
//!   (`estimate() → None`) until a join binds it: the parameter
//!   machinery it shares with formulas (named [`Cells`] slots,
//!   [`Requirement::Required`](crate::Requirement) inputs) enforces
//!   the mode.
//!
//! The admission rule for new resolvers is the address-space rule, not a
//! case-by-case idempotency argument: a resolver may select only from
//! content-addressed (immutable) storage. Anything addressed by
//! mutable state is a scan and must record demand.
//!
//! The first resolvers expose the search tree's *logical model* (see
//! `notes/tree-relations.md`): a node is an index (a table of spans)
//! or a segment (a run of entries); an index's spans each delegate a
//! key range `[separator, until)` to a child and carry the ops
//! buffered against it; a segment's entries are ranked keys; and every
//! node embeds the format manifest it was written under. Nodes are
//! keyed by the same base58 reference `dialog.branch/tree` carries, so
//! a query reaches the tree by joining through `BranchRevision`:
//!
//! ```text
//! BranchRevision(branch, tree: ?root)
//!   ⋈ tree/node(of: ?root, kind: ?kind, size: ?size)
//!   ⋈ tree/span(of: ?root, node: ?child, separator: ?from, until: ?to)
//!   ⋈ tree/node(of: ?child, …)
//! ```

use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::sync::LazyLock;

use base58::{FromBase58, ToBase58};
use dialog_artifacts::inspect::{self, Load};
use dialog_capability::Provider;
use serde::{Deserialize, Serialize};

use crate::artifact::Type as ValueType;
use crate::error::EvaluationError;
use crate::formula::cell::Cells;
use crate::query::Application;
use crate::selection::{Match, Selection};
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{Environment, Parameters, Schema, Scope, Value, try_stream};

/// Base cost of a resolver step: one content-addressed block fetch
/// plus decode, scheduled after cheap in-memory premises but ahead of
/// broad scans.
pub const RESOLVER_COST: usize = 200;

/// Serde default for omitted parameter slots.
fn blank() -> Term<Any> {
    Term::blank()
}

/// The kind of a block-reference input cell: base58 `String` or raw
/// 32-byte `Bytes` — the union the evaluator's `node_reference`
/// actually accepts, so rule type inference admits chaining a
/// decomposition formula's bytes output straight into a resolver.
fn reference_kind() -> Kind {
    Kind::from(ValueType::String).union(&Kind::from(ValueType::Bytes))
}

/// The `tree/node` resolver: describe the node behind a reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeNodeQuery {
    /// Node reference (base58 of the node's content hash) — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// `"index"` (a table of spans) or `"segment"` (a run of entries).
    #[serde(default = "blank")]
    pub kind: Term<Any>,
    /// Serialized block size in bytes.
    #[serde(default = "blank")]
    pub size: Term<Any>,
    /// Span count (index) or entry count (segment).
    #[serde(default = "blank")]
    pub count: Term<Any>,
    /// The node's scale code (advisory log-scale subtree size).
    #[serde(default = "blank")]
    pub scale: Term<Any>,
    /// Buffered hitchhiker ops riding this node (0 for a segment).
    #[serde(default = "blank")]
    pub novelty: Term<Any>,
}

/// The `tree/span` resolver: one row per span of an index node — the
/// key range `[separator, until)` the node delegates to one child,
/// with everything pending against it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeSpanQuery {
    /// Node reference of the index node — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// Position among the node's spans.
    #[serde(default = "blank")]
    pub at: Term<Any>,
    /// The child node reference (base58) rooting the span's subtree,
    /// feeding the next `tree/node`/`tree/span` input.
    #[serde(default = "blank")]
    pub node: Term<Any>,
    /// The span's lower bound: a front-coded prefix of the subtree's
    /// minimum leaf key. Empty for the leftmost span (−∞).
    #[serde(default = "blank")]
    pub separator: Term<Any>,
    /// The span's upper bound: the next span's separator. Empty for
    /// the last span (+∞).
    #[serde(default = "blank")]
    pub until: Term<Any>,
    /// The subtree's advisory scale code.
    #[serde(default = "blank")]
    pub scale: Term<Any>,
    /// Seam rank of the separator: the level coin that made this
    /// boundary exist (0 for the leftmost span and forced seams).
    #[serde(default = "blank")]
    pub rank: Term<Any>,
    /// Buffered hitchhiker ops pending against this span.
    #[serde(default = "blank")]
    pub novelty: Term<Any>,
}

/// The `tree/key` resolver: one row per entry of a segment node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeKeyQuery {
    /// Node reference of the segment node — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// Position within the segment.
    #[serde(default = "blank")]
    pub at: Term<Any>,
    /// The entry's variable-length index key bytes.
    #[serde(default = "blank")]
    pub key: Term<Any>,
    /// The key's leaf-coin rank: what decides whether a leaf boundary
    /// forms after this entry.
    #[serde(default = "blank")]
    pub rank: Term<Any>,
}

/// The `tree/entry` resolver: one row per entry of a segment node,
/// surfacing the entry's stored claim metadata — the *value* side; the
/// fact content itself lives in the key (`tree/key` +
/// `dialog/key-part`). This is what makes the history and coverage
/// regions legible: versions, causes, and coverage ride here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeEntryQuery {
    /// Node reference of the segment node — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// Position within the segment.
    #[serde(default = "blank")]
    pub at: Term<Any>,
    /// The entry's variable-length index key bytes.
    #[serde(default = "blank")]
    pub key: Term<Any>,
    /// `"asserted"` for a stored datum, `"removed"` for a tombstone.
    #[serde(default = "blank")]
    pub state: Term<Any>,
    /// Whether the datum marks a retraction (a covering record).
    #[serde(default = "blank")]
    pub retraction: Term<Any>,
    /// Origin half of the claim's version (32 bytes; empty when the
    /// write was unversioned).
    #[serde(default = "blank")]
    pub origin: Term<Any>,
    /// Edition half of the claim's version (0 when unversioned).
    #[serde(default = "blank")]
    pub edition: Term<Any>,
    /// Number of prior claim versions in the entry's cause.
    #[serde(default = "blank")]
    pub cause: Term<Any>,
    /// Extra claim versions collapsed into this entry.
    #[serde(default = "blank")]
    pub collapsed: Term<Any>,
    /// Versions a covering record supersedes.
    #[serde(default = "blank")]
    pub supersedes: Term<Any>,
    /// The spilled value's block reference (base58), empty when the
    /// value is inline in the key. Feeds `tree/value`.
    #[serde(default = "blank")]
    pub spill: Term<Any>,
}

/// The `tree/value` resolver: read a spilled value's raw bytes by its
/// content-addressed block reference (a `tree/entry` row's `spill`).
/// The block holds the value's raw bytes; the value's type is in the
/// key (`dialog/key-part`'s `vtype` component).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeValueQuery {
    /// The spilled value's 32-byte block reference — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// Size of the value in bytes.
    #[serde(default = "blank")]
    pub size: Term<Any>,
    /// The value's raw bytes.
    #[serde(default = "blank")]
    pub bytes: Term<Any>,
}

/// The `tree/blob` resolver: one row per blob-index entry of a
/// segment node — the content-derived metadata the tree stores about a
/// referenced blob. The blob's bytes live in the blob store, outside
/// the tree; sizes and hashes here are what replication and the
/// inspector reason with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeBlobQuery {
    /// Node reference of the segment node — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// Position within the segment.
    #[serde(default = "blank")]
    pub at: Term<Any>,
    /// The referenced blob's 32-byte content hash.
    #[serde(default = "blank")]
    pub blob: Term<Any>,
    /// The blob record's encoding version.
    #[serde(default = "blank")]
    pub version: Term<Any>,
    /// Total size of the blob in bytes.
    #[serde(default = "blank")]
    pub size: Term<Any>,
}

/// The `tree/manifest` resolver: the format manifest the node embeds,
/// field for field. Every node carries one, so a bare reference is
/// self-describing and mixed-format trees are visible per node.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TreeManifestQuery {
    /// Node reference — required.
    #[serde(default = "blank")]
    pub of: Term<Any>,
    /// Format version; pins how the rest of the node is interpreted.
    #[serde(default = "blank")]
    pub version: Term<Any>,
    /// Branching parameter `n`; expected fanout is `2^n`.
    #[serde(default = "blank")]
    pub fanout_n: Term<Any>,
    /// Keys longer than this never become boundaries.
    #[serde(default = "blank")]
    pub max_separator: Term<Any>,
    /// Values longer than this spill to the block store.
    #[serde(default = "blank")]
    pub inline_n: Term<Any>,
    /// Leading raw value bytes a spilled value's key keeps as prefix.
    #[serde(default = "blank")]
    pub spill_prefix: Term<Any>,
    /// Leaf-run weight cap; 0 disables it.
    #[serde(default = "blank")]
    pub max_segment: Term<Any>,
    /// Hard frame-weight ceiling as a multiple of `max_segment`.
    #[serde(default = "blank")]
    pub frame_ceiling_factor: Term<Any>,
    /// Which candidate seam a forced cut anchors at.
    #[serde(default = "blank")]
    pub anchor_selector: Term<Any>,
}

/// A resolver premise bound to specific term arguments.
///
/// Serializes as `{"assert": "<name>", "where": <params>}`, the same
/// tagged form formulas use — a bare-string `assert` whose name lives
/// in the resolver registry rather than the formula registry.
// Variant sizes track each resolver's term count, exactly as
// `FormulaQuery`'s do; instances are transient planning values, never
// bulk-stored, so boxing would cost more indirection than the size
// skew costs memory.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "assert", content = "where")]
pub enum ResolverQuery {
    /// Resolver `tree/node`.
    #[serde(rename = "tree/node")]
    TreeNode(TreeNodeQuery),
    /// Resolver `tree/span`.
    #[serde(rename = "tree/span")]
    TreeSpan(TreeSpanQuery),
    /// Resolver `tree/key`.
    #[serde(rename = "tree/key")]
    TreeKey(TreeKeyQuery),
    /// Resolver `tree/entry`.
    #[serde(rename = "tree/entry")]
    TreeEntry(TreeEntryQuery),
    /// Resolver `tree/value`.
    #[serde(rename = "tree/value")]
    TreeValue(TreeValueQuery),
    /// Resolver `tree/blob`.
    #[serde(rename = "tree/blob")]
    TreeBlob(TreeBlobQuery),
    /// Resolver `tree/manifest`.
    #[serde(rename = "tree/manifest")]
    TreeManifest(TreeManifestQuery),
}

static TREE_NODE_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("Node reference: base58 of the node's content hash.")
            .required();
        builder
            .cell("kind", Some(Kind::from(ValueType::String)))
            .the("\"index\" (a table of spans) or \"segment\" (a run of entries).");
        builder
            .cell("size", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Serialized block size in bytes.");
        builder
            .cell("count", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Span count (index) or entry count (segment).");
        builder
            .cell("scale", Some(Kind::from(ValueType::UnsignedInt)))
            .the("The node's advisory scale code.");
        builder
            .cell("novelty", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Buffered ops riding this node (0 for a segment).");
    })
});

static TREE_SPAN_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("Node reference of the index node.")
            .required();
        builder
            .cell("at", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Position among the node's spans.");
        builder
            .cell("node", Some(Kind::from(ValueType::String)))
            .the("The child node reference rooting the span's subtree.");
        builder
            .cell("separator", Some(Kind::from(ValueType::Bytes)))
            .the("The span's lower bound (empty = −∞).");
        builder
            .cell("until", Some(Kind::from(ValueType::Bytes)))
            .the("The span's upper bound: the next separator (empty = +∞).");
        builder
            .cell("scale", Some(Kind::from(ValueType::UnsignedInt)))
            .the("The subtree's advisory scale code.");
        builder
            .cell("rank", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Seam rank of the separator (the boundary's level coin).");
        builder
            .cell("novelty", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Buffered ops pending against this span.");
    })
});

static TREE_KEY_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("Node reference of the segment node.")
            .required();
        builder
            .cell("at", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Position within the segment.");
        builder
            .cell("key", Some(Kind::from(ValueType::Bytes)))
            .the("The entry's variable-length index key bytes.");
        builder
            .cell("rank", Some(Kind::from(ValueType::UnsignedInt)))
            .the("The key's leaf-coin rank.");
    })
});

static TREE_ENTRY_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("Node reference of the segment node.")
            .required();
        builder
            .cell("at", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Position within the segment.");
        builder
            .cell("key", Some(Kind::from(ValueType::Bytes)))
            .the("The entry's variable-length index key bytes.");
        builder
            .cell("state", Some(Kind::from(ValueType::String)))
            .the("\"asserted\" or \"removed\".");
        builder
            .cell("retraction", Some(Kind::from(ValueType::Boolean)))
            .the("Whether the datum marks a retraction.");
        builder
            .cell("origin", Some(Kind::from(ValueType::Bytes)))
            .the("Origin half of the claim's version (empty = unversioned).");
        builder
            .cell("edition", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Edition half of the claim's version.");
        builder
            .cell("cause", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Prior claim versions in the entry's cause.");
        builder
            .cell("collapsed", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Extra claim versions collapsed into this entry.");
        builder
            .cell("supersedes", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Versions a covering record supersedes.");
        builder
            .cell("spill", Some(Kind::from(ValueType::String)))
            .the("Spilled value's block reference (empty = inline).");
    })
});

static TREE_VALUE_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("The spilled value's 32-byte block reference.")
            .required();
        builder
            .cell("size", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Size of the value in bytes.");
        builder
            .cell("bytes", Some(Kind::from(ValueType::Bytes)))
            .the("The value's raw bytes.");
    })
});

static TREE_BLOB_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("Node reference of the segment node.")
            .required();
        builder
            .cell("at", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Position within the segment.");
        builder
            .cell("blob", Some(Kind::from(ValueType::Bytes)))
            .the("The referenced blob's 32-byte content hash.");
        builder
            .cell("version", Some(Kind::from(ValueType::UnsignedInt)))
            .the("The blob record's encoding version.");
        builder
            .cell("size", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Total size of the blob in bytes.");
    })
});

static TREE_MANIFEST_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(reference_kind()))
            .the("Node reference.")
            .required();
        builder
            .cell("version", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Format version.");
        builder
            .cell("fanout_n", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Branching parameter n; expected fanout is 2^n.");
        builder
            .cell("max_separator", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Keys longer than this never become boundaries.");
        builder
            .cell("inline_n", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Values longer than this spill to the block store.");
        builder
            .cell("spill_prefix", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Order-preserving prefix bytes a spilled value's key keeps.");
        builder
            .cell("max_segment", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Leaf-run weight cap; 0 disables it.");
        builder
            .cell(
                "frame_ceiling_factor",
                Some(Kind::from(ValueType::UnsignedInt)),
            )
            .the("Hard frame-weight ceiling as a multiple of max_segment.");
        builder
            .cell("anchor_selector", Some(Kind::from(ValueType::UnsignedInt)))
            .the("Which candidate seam a forced cut anchors at.");
    })
});

impl ResolverQuery {
    /// Returns the formal notation name (e.g. `"tree/node"`).
    pub fn name(&self) -> &'static str {
        match self {
            Self::TreeNode(_) => "tree/node",
            Self::TreeSpan(_) => "tree/span",
            Self::TreeKey(_) => "tree/key",
            Self::TreeEntry(_) => "tree/entry",
            Self::TreeValue(_) => "tree/value",
            Self::TreeBlob(_) => "tree/blob",
            Self::TreeManifest(_) => "tree/manifest",
        }
    }

    /// Returns the static cell definitions for this resolver.
    pub(crate) fn cells(&self) -> &'static Cells {
        match self {
            Self::TreeNode(_) => &TREE_NODE_CELLS,
            Self::TreeSpan(_) => &TREE_SPAN_CELLS,
            Self::TreeKey(_) => &TREE_KEY_CELLS,
            Self::TreeEntry(_) => &TREE_ENTRY_CELLS,
            Self::TreeValue(_) => &TREE_VALUE_CELLS,
            Self::TreeBlob(_) => &TREE_BLOB_CELLS,
            Self::TreeManifest(_) => &TREE_MANIFEST_CELLS,
        }
    }

    /// Returns the schema for this resolver.
    pub fn schema(&self) -> Schema {
        self.cells().into()
    }

    /// The required node-reference input term.
    fn of(&self) -> &Term<Any> {
        match self {
            Self::TreeNode(query) => &query.of,
            Self::TreeSpan(query) => &query.of,
            Self::TreeKey(query) => &query.of,
            Self::TreeEntry(query) => &query.of,
            Self::TreeValue(query) => &query.of,
            Self::TreeBlob(query) => &query.of,
            Self::TreeManifest(query) => &query.of,
        }
    }

    /// Estimate the cost of this resolver given the environment.
    ///
    /// `None` while the node-reference input is an unbound variable:
    /// node hashes are not enumerable, and an unbound scan is the one
    /// shape that would degrade subscriptions — the planner refuses to
    /// schedule it until a join binds the input. A *blank* `of` (a
    /// query that never names the input at all) is scheduled but can
    /// match nothing: every row fails the reference lookup, so such a
    /// query returns empty rather than erroring.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        self.of().is_bound(env).then_some(RESOLVER_COST)
    }

    /// Returns the parameters for this resolver application.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        match self {
            Self::TreeNode(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("kind".into(), query.kind.clone());
                params.insert("size".into(), query.size.clone());
                params.insert("count".into(), query.count.clone());
                params.insert("scale".into(), query.scale.clone());
                params.insert("novelty".into(), query.novelty.clone());
            }
            Self::TreeSpan(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("at".into(), query.at.clone());
                params.insert("node".into(), query.node.clone());
                params.insert("separator".into(), query.separator.clone());
                params.insert("until".into(), query.until.clone());
                params.insert("scale".into(), query.scale.clone());
                params.insert("rank".into(), query.rank.clone());
                params.insert("novelty".into(), query.novelty.clone());
            }
            Self::TreeKey(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("at".into(), query.at.clone());
                params.insert("key".into(), query.key.clone());
                params.insert("rank".into(), query.rank.clone());
            }
            Self::TreeEntry(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("at".into(), query.at.clone());
                params.insert("key".into(), query.key.clone());
                params.insert("state".into(), query.state.clone());
                params.insert("retraction".into(), query.retraction.clone());
                params.insert("origin".into(), query.origin.clone());
                params.insert("edition".into(), query.edition.clone());
                params.insert("cause".into(), query.cause.clone());
                params.insert("collapsed".into(), query.collapsed.clone());
                params.insert("supersedes".into(), query.supersedes.clone());
                params.insert("spill".into(), query.spill.clone());
            }
            Self::TreeValue(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("size".into(), query.size.clone());
                params.insert("bytes".into(), query.bytes.clone());
            }
            Self::TreeBlob(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("at".into(), query.at.clone());
                params.insert("blob".into(), query.blob.clone());
                params.insert("version".into(), query.version.clone());
                params.insert("size".into(), query.size.clone());
            }
            Self::TreeManifest(query) => {
                params.insert("of".into(), query.of.clone());
                params.insert("version".into(), query.version.clone());
                params.insert("fanout_n".into(), query.fanout_n.clone());
                params.insert("max_separator".into(), query.max_separator.clone());
                params.insert("inline_n".into(), query.inline_n.clone());
                params.insert("spill_prefix".into(), query.spill_prefix.clone());
                params.insert("max_segment".into(), query.max_segment.clone());
                params.insert(
                    "frame_ceiling_factor".into(),
                    query.frame_ceiling_factor.clone(),
                );
                params.insert("anchor_selector".into(), query.anchor_selector.clone());
            }
        }
        params
    }

    /// Resolve the node-reference input against the row: a base58
    /// string in a constant or a bound variable. `None` filters the
    /// row (unresolvable or malformed references contribute nothing,
    /// mirroring the forged-record-projects-nothing convention).
    fn node_reference(&self, base: &Match) -> Option<inspect::NodeReference> {
        let value = match self.of() {
            Term::Constant(value) => value.clone(),
            term => match base.lookup(term) {
                Ok(crate::Binding::Present(value)) => value,
                _ => return None,
            },
        };
        match value {
            Value::String(reference) => {
                let bytes = reference.from_base58().ok()?;
                <[u8; 32]>::try_from(bytes).ok()
            }
            // Raw 32-byte references chain too (e.g. straight from a
            // decomposition formula's bytes output).
            Value::Bytes(bytes) => <[u8; 32]>::try_from(bytes).ok(),
            _ => None,
        }
    }

    /// Evaluate this resolver over the incoming selection.
    ///
    /// Per input row: resolve the node reference, perform the
    /// idempotent [`Load`] effect through the environment, decode the
    /// block, and project one output row per result — a segment asked
    /// for spans, an index asked for keys, an absent block, or a
    /// resolvable reference whose block is not a tree node at all
    /// (e.g. a spilled value block joined into `tree/node`) all yield
    /// zero rows, mirroring the malformed-reference convention above.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Scope<'a>,
    {
        let resolver = self;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                let Some(reference) = resolver.node_reference(&base) else {
                    continue;
                };
                let Some(bytes) = Provider::<Load>::execute(env, reference).await? else {
                    continue;
                };
                match &resolver {
                    ResolverQuery::TreeNode(query) => {
                        let Ok(node) = inspect::inspect_node(bytes) else {
                            continue;
                        };
                        let row = project(&base, &[
                            (&query.kind, Value::String(node.kind.into())),
                            (&query.size, Value::UnsignedInt(node.size.into())),
                            (&query.count, Value::UnsignedInt(node.count.into())),
                            (&query.scale, Value::UnsignedInt(node.scale.into())),
                            (&query.novelty, Value::UnsignedInt(node.novelty.into())),
                        ])?;
                        if let Some(row) = row {
                            yield row;
                        }
                    }
                    ResolverQuery::TreeSpan(query) => {
                        let Ok(spans) = inspect::inspect_spans(bytes) else {
                            continue;
                        };
                        for span in spans {
                            let row = project(&base, &[
                                (&query.at, Value::UnsignedInt(span.at.into())),
                                (&query.node, Value::String(span.node.to_base58())),
                                (&query.separator, Value::Bytes(span.separator)),
                                (&query.until, Value::Bytes(span.until)),
                                (&query.scale, Value::UnsignedInt(span.scale.into())),
                                (&query.rank, Value::UnsignedInt(span.rank.into())),
                                (&query.novelty, Value::UnsignedInt(span.novelty.into())),
                            ])?;
                            if let Some(row) = row {
                                yield row;
                            }
                        }
                    }
                    ResolverQuery::TreeKey(query) => {
                        let Ok(keys) = inspect::inspect_keys(bytes) else {
                            continue;
                        };
                        for (at, entry) in keys.into_iter().enumerate() {
                            let row = project(&base, &[
                                (&query.at, Value::UnsignedInt(at as u128)),
                                (&query.key, Value::Bytes(entry.key)),
                                (&query.rank, Value::UnsignedInt(entry.rank.into())),
                            ])?;
                            if let Some(row) = row {
                                yield row;
                            }
                        }
                    }
                    ResolverQuery::TreeEntry(query) => {
                        let Ok(entries) = inspect::inspect_entries(bytes) else {
                            continue;
                        };
                        for entry in entries {
                            let spill = entry
                                .spill
                                .map(|reference| reference.to_base58())
                                .unwrap_or_default();
                            let row = project(&base, &[
                                (&query.at, Value::UnsignedInt(entry.at.into())),
                                (&query.key, Value::Bytes(entry.key)),
                                (&query.state, Value::String(entry.state.into())),
                                (&query.retraction, Value::Boolean(entry.retraction)),
                                (&query.origin, Value::Bytes(entry.origin)),
                                (&query.edition, Value::UnsignedInt(entry.edition.into())),
                                (&query.cause, Value::UnsignedInt(entry.cause.into())),
                                (&query.collapsed, Value::UnsignedInt(entry.collapsed.into())),
                                (
                                    &query.supersedes,
                                    Value::UnsignedInt(entry.supersedes.into()),
                                ),
                                (&query.spill, Value::String(spill)),
                            ])?;
                            if let Some(row) = row {
                                yield row;
                            }
                        }
                    }
                    ResolverQuery::TreeValue(query) => {
                        // The loaded block IS the value: raw bytes, no
                        // node decode. Type information lives in the
                        // key the reference came from.
                        let row = project(&base, &[
                            (&query.size, Value::UnsignedInt(bytes.len() as u128)),
                            (&query.bytes, Value::Bytes(bytes)),
                        ])?;
                        if let Some(row) = row {
                            yield row;
                        }
                    }
                    ResolverQuery::TreeBlob(query) => {
                        let Ok(records) = inspect::inspect_blob_records(bytes) else {
                            continue;
                        };
                        for record in records {
                            let row = project(&base, &[
                                (&query.at, Value::UnsignedInt(record.at.into())),
                                (&query.blob, Value::Bytes(record.blob)),
                                (&query.version, Value::UnsignedInt(record.version.into())),
                                (&query.size, Value::UnsignedInt(record.size.into())),
                            ])?;
                            if let Some(row) = row {
                                yield row;
                            }
                        }
                    }
                    ResolverQuery::TreeManifest(query) => {
                        let Ok(manifest) = inspect::inspect_manifest(bytes) else {
                            continue;
                        };
                        let row = project(&base, &[
                            (&query.version, Value::UnsignedInt(manifest.version.into())),
                            (&query.fanout_n, Value::UnsignedInt(manifest.fanout_n.into())),
                            (
                                &query.max_separator,
                                Value::UnsignedInt(manifest.max_separator.into()),
                            ),
                            (&query.inline_n, Value::UnsignedInt(manifest.inline_n.into())),
                            (
                                &query.spill_prefix,
                                Value::UnsignedInt(manifest.spill_prefix.into()),
                            ),
                            (
                                &query.max_segment,
                                Value::UnsignedInt(manifest.max_segment.into()),
                            ),
                            (
                                &query.frame_ceiling_factor,
                                Value::UnsignedInt(manifest.frame_ceiling_factor.into()),
                            ),
                            (
                                &query.anchor_selector,
                                Value::UnsignedInt(manifest.anchor_selector.into()),
                            ),
                        ])?;
                        if let Some(row) = row {
                            yield row;
                        }
                    }
                }
            }
        }
    }
}

/// Extend `base` with the projected output values. A constant slot
/// whose value disagrees, or a pre-bound variable that conflicts,
/// filters the row (`None`) — the membership-test semantics shared
/// with formulas; any other bind failure is a genuine error.
fn project(base: &Match, fields: &[(&Term<Any>, Value)]) -> Result<Option<Match>, EvaluationError> {
    let mut row = base.clone();
    for (term, value) in fields {
        match term {
            Term::Constant(expected) => {
                if expected != value {
                    return Ok(None);
                }
            }
            _ => match row.bind(term, value.clone()) {
                Ok(()) => {}
                Err(EvaluationError::Assignment { .. })
                | Err(EvaluationError::KindMismatch { .. }) => return Ok(None),
                Err(error) => return Err(error),
            },
        }
    }
    Ok(Some(row))
}

/// A realized resolver row: the value each named slot bound.
/// Constant slots are echoed; slots the row did not bind are absent.
pub type ResolverConclusion = BTreeMap<String, Value>;

impl Application for ResolverQuery {
    type Conclusion = ResolverConclusion;

    fn evaluate<'a, Env, M: Selection + 'a>(self, selection: M, env: &'a Env) -> impl Selection + 'a
    where
        Env: Scope<'a>,
    {
        self.evaluate(env, selection)
    }

    fn realize(&self, input: Match) -> Result<Self::Conclusion, EvaluationError> {
        let mut row = BTreeMap::new();
        for (slot, term) in self.parameters().iter() {
            match term {
                Term::Constant(value) => {
                    row.insert(slot.clone(), value.clone());
                }
                term => {
                    if let Ok(crate::Binding::Present(value)) = input.lookup(term) {
                        row.insert(slot.clone(), value);
                    }
                }
            }
        }
        Ok(row)
    }
}

impl Display for ResolverQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(of: {})", self.name(), self.of())
    }
}
