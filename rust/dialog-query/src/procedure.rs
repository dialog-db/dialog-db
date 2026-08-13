//! Procedure premises: moded, multi-row premises resolved by performing
//! an idempotent effect through the evaluation environment.
//!
//! A procedure is a premise kind of its own — the resemblance to
//! formulas begins and ends at the parameter machinery (named slots
//! with [`Requirement::Required`](crate::Requirement) inputs, and the
//! `estimate() → None` protocol that keeps an unbound-input premise
//! unschedulable until a join binds it). Everything past the cells
//! differs: where a formula computes in-process and a scan streams a
//! demand-recorded selector, a procedure performs an *idempotent*
//! effect — one whose result is a pure function of its bound inputs
//! and the immutable, content-addressed universe — and projects rows
//! from the result. Scans are procedures' closest relative: both are
//! premises the environment answers; the difference is the effect
//! ([`Select`](dialog_artifacts::Select) vs
//! [`Load`](dialog_artifacts::inspect::Load)) and that an idempotent
//! effect needs no demand recording, because nothing can ever
//! invalidate its rows (see `dialog_artifacts::inspect` for the full
//! soundness argument).
//!
//! The first procedures expose the search tree's *logical model* (see
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

/// Base cost of a procedure step: one content-addressed block fetch
/// plus decode, scheduled after cheap in-memory premises but ahead of
/// broad scans.
pub const PROCEDURE_COST: usize = 200;

/// Serde default for omitted parameter slots.
fn blank() -> Term<Any> {
    Term::blank()
}

/// The `tree/node` procedure: describe the node behind a reference.
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

/// The `tree/span` procedure: one row per span of an index node — the
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

/// The `tree/key` procedure: one row per entry of a segment node.
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

/// The `tree/manifest` procedure: the format manifest the node embeds,
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

/// A procedure premise bound to specific term arguments.
///
/// Serializes as `{"assert": "<name>", "where": <params>}`, the same
/// tagged form formulas use — a bare-string `assert` whose name lives
/// in the procedure registry rather than the formula registry.
// Variant sizes track each procedure's term count, exactly as
// `FormulaQuery`'s do; instances are transient planning values, never
// bulk-stored, so boxing would cost more indirection than the size
// skew costs memory.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "assert", content = "where")]
pub enum ProcedureQuery {
    /// Procedure `tree/node`.
    #[serde(rename = "tree/node")]
    TreeNode(TreeNodeQuery),
    /// Procedure `tree/span`.
    #[serde(rename = "tree/span")]
    TreeSpan(TreeSpanQuery),
    /// Procedure `tree/key`.
    #[serde(rename = "tree/key")]
    TreeKey(TreeKeyQuery),
    /// Procedure `tree/manifest`.
    #[serde(rename = "tree/manifest")]
    TreeManifest(TreeManifestQuery),
}

static TREE_NODE_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(Kind::from(ValueType::String)))
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
            .cell("of", Some(Kind::from(ValueType::String)))
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
            .cell("of", Some(Kind::from(ValueType::String)))
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

static TREE_MANIFEST_CELLS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(Kind::from(ValueType::String)))
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

impl ProcedureQuery {
    /// Returns the formal notation name (e.g. `"tree/node"`).
    pub fn name(&self) -> &'static str {
        match self {
            Self::TreeNode(_) => "tree/node",
            Self::TreeSpan(_) => "tree/span",
            Self::TreeKey(_) => "tree/key",
            Self::TreeManifest(_) => "tree/manifest",
        }
    }

    /// Returns the static cell definitions for this procedure.
    pub(crate) fn cells(&self) -> &'static Cells {
        match self {
            Self::TreeNode(_) => &TREE_NODE_CELLS,
            Self::TreeSpan(_) => &TREE_SPAN_CELLS,
            Self::TreeKey(_) => &TREE_KEY_CELLS,
            Self::TreeManifest(_) => &TREE_MANIFEST_CELLS,
        }
    }

    /// Returns the schema for this procedure.
    pub fn schema(&self) -> Schema {
        self.cells().into()
    }

    /// The required node-reference input term.
    fn of(&self) -> &Term<Any> {
        match self {
            Self::TreeNode(query) => &query.of,
            Self::TreeSpan(query) => &query.of,
            Self::TreeKey(query) => &query.of,
            Self::TreeManifest(query) => &query.of,
        }
    }

    /// Estimate the cost of this procedure given the environment.
    ///
    /// `None` while the node-reference input is unbound: node hashes
    /// are not enumerable, and an unbound scan is the one shape that
    /// would degrade subscriptions — the planner refuses to schedule
    /// it until a join binds the input.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        self.of().is_bound(env).then_some(PROCEDURE_COST)
    }

    /// Returns the parameters for this procedure application.
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
        let Value::String(reference) = value else {
            return None;
        };
        let bytes = reference.from_base58().ok()?;
        <[u8; 32]>::try_from(bytes).ok()
    }

    /// Evaluate this procedure over the incoming selection.
    ///
    /// Per input row: resolve the node reference, perform the
    /// idempotent [`Load`] effect through the environment, decode the
    /// block, and project one output row per result — a segment asked
    /// for spans, an index asked for keys, or an absent block all
    /// yield zero rows.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        env: &'a Env,
        selection: M,
    ) -> impl Selection + 'a
    where
        Env: Scope<'a>,
    {
        let procedure = self;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                let Some(reference) = procedure.node_reference(&base) else {
                    continue;
                };
                let Some(bytes) = Provider::<Load>::execute(env, reference).await? else {
                    continue;
                };
                match &procedure {
                    ProcedureQuery::TreeNode(query) => {
                        let node = inspect::inspect_node(bytes)?;
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
                    ProcedureQuery::TreeSpan(query) => {
                        for span in inspect::inspect_spans(bytes)? {
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
                    ProcedureQuery::TreeKey(query) => {
                        for (at, entry) in inspect::inspect_keys(bytes)?.into_iter().enumerate() {
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
                    ProcedureQuery::TreeManifest(query) => {
                        let manifest = inspect::inspect_manifest(bytes)?;
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

/// A realized procedure row: the value each named slot bound.
/// Constant slots are echoed; slots the row did not bind are absent.
pub type ProcedureConclusion = BTreeMap<String, Value>;

impl Application for ProcedureQuery {
    type Conclusion = ProcedureConclusion;

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

impl Display for ProcedureQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(of: {})", self.name(), self.of())
    }
}
