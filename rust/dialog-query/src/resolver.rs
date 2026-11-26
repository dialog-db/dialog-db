//! Resolver premises: accessors over a search-tree node.
//!
//! Scans and resolvers are the same kind of thing — selects the
//! environment answers — distinguished by the *address space* they
//! select from, and every difference between them falls out of that
//! one split:
//!
//! - A **scan** selects by key range over the mutable head of the
//!   indexes ([`Select`](dialog_corpus::Select)). The head moves,
//!   so its result set can be invalidated — hence demand covers,
//!   recorded per scanned range. A range can also be enumerated, so
//!   an unconstrained scan is expensive but defined.
//! - A **resolver** selects by content address over the immutable block
//!   universe. A hash resolves to the same bytes forever, so a
//!   resolver's rows are a pure function of the handle it is given — no
//!   fact demand exists to record (see `dialog_corpus::inspect` for the
//!   full soundness argument); what changes over time is
//!   *reachability* — which root the branch head names — and the
//!   revision-anchored subscription (`Demand::head`) tracks exactly
//!   that. A content address cannot be enumerated, so a resolver whose
//!   handle is unbound is non-viable (`estimate() → None`) until a join
//!   binds it. That is also what keeps a query from reading, and
//!   fetching, a tree it was not pointed at.
//!
//! The admission rule for new resolvers is the address-space rule, not a
//! case-by-case idempotency argument: a resolver may select only from
//! content-addressed (immutable) storage. Anything addressed by
//! mutable state is a scan and must record demand. Whether a replica
//! holds a block is such state, and is not asked here.
//!
//! # A node is a handle, and a resolver reads one part of it
//!
//! A node is opaque to a query. What a query holds of it is its handle,
//! the `node:<base58(hash)>` entity ([`Entity::from_node`]):
//! `dialog.branch/tree` carries the root's, and `tree/span` yields each
//! child's. A resolver takes the handle as `of`, asks the environment
//! for the node ([`nodes::Read`]), and yields one part of it as rows.
//! The environment reads, checks and decodes a node once and keeps it,
//! as it does for the tree's own reads; a query that asks several
//! resolvers about one node decodes it once.
//!
//! - `tree/node`: what the node is, and the format it was written under.
//! - `tree/span`: an index node's children, with what the node holds
//!   about each.
//! - `tree/entry`: a segment node's entries, each whole, or the
//!   operations an index node buffers, each naming its span.
//! - `tree/block`: the node's block as stored.
//! - `tree/value`: a spilled value's bytes. Its handle is an `asset:`
//!   entity, since a spilled value lives in the blob store, as every
//!   `asset:` does; a `tree/entry` row yields it as `spill`.
//!
//! Nothing a node holds is left out. A slot that does not apply to a
//! row (the spill of an inline value, the claim of a blob record) is
//! bound absent, not to a zero or an empty value.
//!
//! ```text
//! BranchRevision(branch, tree: ?root)
//!   ⋈ tree/node(of: ?root, kind: ?kind, size: ?size)
//!   ⋈ tree/span(of: ?root, node: ?child, separator: ?from, until: ?to)
//!   ⋈ tree/node(of: ?child, …)
//! ```

use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use std::sync::LazyLock;

use base58::ToBase58;
use dialog_capability::Subject;
use dialog_common::{Blake3Hash, Buffer};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{DialogCorpusError, inspect};
use dialog_effects::archive::prelude::ArchiveScope;
use dialog_effects::blob;
use dialog_search_tree::nodes;
use serde::{Deserialize, Serialize};

use crate::corpus::Type as ValueType;
use crate::error::EvaluationError;
use crate::formula::cell::Cells;
use crate::query::Application;
use crate::selection::{Match, Selection};
use crate::supplied::{ANALYZE, ANALYZE_CELLS, NotationAnalyzeQuery};
use crate::term::Term;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{Entity, Environment, Parameters, Schema, Scope, Value, try_stream};

/// The archive a resolver reads a content address from: the wildcard
/// subject's, meaning any archive the scope reads.
///
/// A content address names the same bytes in every archive, so a
/// resolver asks for the block itself and leaves where it lives to the
/// environment: a query over several lines answers from each line's
/// catalog in turn (the first that holds the block wins), and an
/// environment over one archive answers from that one.
///
/// ```no_run
/// # use dialog_effects::archive::Load;
/// # use dialog_query::resolver::anywhere;
/// # let digest = dialog_common::Blake3Hash::hash(b"a node");
/// let load = Load::new(anywhere().index(), digest);
/// # use dialog_capability::Subject;
/// assert!(Subject::from(load.catalog.subject().clone()).is_any());
/// ```
pub fn anywhere() -> ArchiveScope {
    ArchiveScope::new(Subject::any())
}

/// `block` if it hashes to `reference`. A resolver's rows are a pure
/// function of the reference (see the module doc), which holds only of
/// the bytes the reference names: an environment handing back other bytes
/// for it (a corrupt store, a line answering for the wrong block) is a
/// failed read, refused rather than inspected as if it were the block.
///
/// The node layer checks a node's block this way itself; this is for the
/// one read that does not go through it, a spilled value's.
fn verified(reference: &[u8; 32], block: Buffer) -> Result<Buffer, DialogCorpusError> {
    if block.blake3_hash() != &Blake3Hash::from(*reference) {
        return Err(DialogCorpusError::InvalidValue(format!(
            "block {} does not hash to its reference",
            reference.to_base58()
        )));
    }
    Ok(block)
}

/// Base cost of a resolver step: one node read, which may fetch.
///
/// Above any read of facts the planner knows, so that in a body where a
/// fact decides whether a node is to be read at all (a rule that reads
/// only the nodes a space has recorded it holds), the fact is asked
/// first and a node it rules out is never read.
pub const RESOLVER_COST: usize = 50_000;

/// Serde default for omitted parameter slots.
fn blank() -> Term<Any> {
    Term::blank()
}

/// Declares one resolver's query struct, its cells, and how its slots
/// are read and replaced, from one list of slots: a slot's name in a
/// query, its kind and its description are stated once.
macro_rules! accessor {
    (
        $(#[$meta:meta])*
        $query:ident, $cells:ident, of: $handle:literal;
        $( $field:ident => $name:literal : $kind:ident, $doc:literal; )*
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        pub struct $query {
            #[doc = $handle]
            #[doc = " Required."]
            #[serde(default = "blank")]
            pub of: Term<Any>,
            $(
                #[doc = $doc]
                #[serde(default = "blank", rename = $name)]
                pub $field: Term<Any>,
            )*
        }

        static $cells: LazyLock<Cells> = LazyLock::new(|| {
            Cells::define(|builder| {
                builder
                    .cell("of", Some(Kind::from(ValueType::Entity)))
                    .the($handle)
                    .required();
                $(
                    builder
                        .cell($name, Some(Kind::from(ValueType::$kind)))
                        .the($doc);
                )*
            })
        });

        impl $query {
            /// Every slot of this resolver, by its name in a query.
            fn parameters(&self) -> Parameters {
                let mut parameters = Parameters::new();
                parameters.insert("of".into(), self.of.clone());
                $( parameters.insert($name.into(), self.$field.clone()); )*
                parameters
            }

            /// Replace each slot `terms` names; the others keep theirs.
            fn replace(&mut self, terms: &Parameters) {
                if let Some(term) = terms.get("of") {
                    self.of = term.clone();
                }
                $(
                    if let Some(term) = terms.get($name) {
                        self.$field = term.clone();
                    }
                )*
            }
        }
    };
}

accessor! {
    /// The `tree/node` resolver: what a node is, and the format it was
    /// written under. One row.
    TreeNodeQuery, TREE_NODE_CELLS, of: "The node, as its `node:` entity.";
    kind => "kind": String, "`\"index\"` (a table of spans) or `\"segment\"` (a run of entries).";
    size => "size": UnsignedInt, "Size of the node's block in bytes.";
    count => "count": UnsignedInt, "Span count (index) or entry count (segment).";
    scale => "scale": UnsignedInt, "The node's scale code: an advisory log-scale size of its subtree.";
    novelty => "novelty": UnsignedInt, "Buffered ops riding this node (0 for a segment).";
    version => "version": UnsignedInt, "Layout version; pins how the rest of the node is read.";
    fanout_exponent => "fanout-exponent": UnsignedInt, "Branching parameter `n`: how many bits of a key's hash decide a boundary; expected fanout is `2^n`.";
    max_separator => "max-separator": UnsignedInt, "Keys longer than this never become boundaries.";
    inline_n => "inline-n": UnsignedInt, "Values longer than this spill to the blob store.";
    spill_prefix => "spill-prefix": UnsignedInt, "Leading raw value bytes a spilled value's key keeps.";
    max_segment => "max-segment": UnsignedInt, "Leaf-run weight cap; 0 disables it.";
    frame_ceiling_factor => "frame-ceiling-factor": UnsignedInt, "Hard frame-weight ceiling as a multiple of `max-segment`.";
    anchor_selector => "anchor-selector": UnsignedInt, "Which candidate seam a forced cut anchors at.";
    entry_overhead => "entry-overhead": UnsignedInt, "Weight charged per leaf entry beyond its key and value bytes.";
    key_overhead => "key-overhead": UnsignedInt, "Weight the per-key cut floor charges beyond a key's bytes.";
    link_overhead => "link-overhead": UnsignedInt, "Weight charged per index link beyond its separator and child hash.";
}

accessor! {
    /// The `tree/span` resolver: one row per span of an index node — the
    /// key range `[separator, until)` the node delegates to one child,
    /// with everything it holds against it. No rows for a segment.
    TreeSpanQuery, TREE_SPAN_CELLS, of: "The index node, as its `node:` entity.";
    span => "span": Entity, "The span's own entity: this node and the span's position in it.";
    at => "at": UnsignedInt, "Position among the node's spans.";
    node => "node": Entity, "The child node rooting the span's subtree, as its `node:` entity.";
    separator => "separator": Bytes, "The span's lower bound: a prefix of the subtree's least key. Empty for the first span.";
    until => "until": Bytes, "The span's upper bound: the next span's separator. Empty for the last span.";
    scale => "scale": UnsignedInt, "The subtree's advisory scale code.";
    rank => "rank": UnsignedInt, "Seam rank of the separator: the level coin that made this boundary.";
    novelty => "novelty": UnsignedInt, "Buffered ops the node holds against this span.";
}

accessor! {
    /// The `tree/entry` resolver: one row per entry of a segment node,
    /// whole — its key, its state and claim, the value it spilled, and
    /// the blob record it is when it is one. For an index node, one row
    /// per operation it buffers, naming the span it rides: writes headed
    /// for a subtree that are not written down in it yet.
    TreeEntryQuery, TREE_ENTRY_CELLS, of: "The node, as its `node:` entity.";
    entry => "entry": Entity, "The entry's own entity: this node and the entry's position in it.";
    at => "at": UnsignedInt, "Position within the segment, or among the index node's buffered operations.";
    span => "span": Entity, "The span a buffered operation rides, as `tree/span` names it. Absent on a segment's entry.";
    key => "key": Bytes, "The entry's index key.";
    rank => "rank": UnsignedInt, "The key's leaf-coin rank: what decides whether a boundary forms after it.";
    state => "state": String, "`\"asserted\"` for a stored datum, `\"removed\"` for a tombstone.";
    retraction => "retraction": Boolean, "Whether the datum marks a retraction. Absent on a blob record.";
    origin => "origin": Bytes, "Origin half of the claim's version. Absent when the write was unversioned.";
    edition => "edition": UnsignedInt, "Edition half of the claim's version. Absent when the write was unversioned.";
    cause => "cause": UnsignedInt, "Prior claim versions in the entry's cause. Absent on a blob record.";
    collapsed => "collapsed": UnsignedInt, "Claim versions collapsed into this entry. Absent on a blob record.";
    supersedes => "supersedes": UnsignedInt, "Versions a covering record supersedes. Absent on a blob record.";
    spill => "spill": Entity, "The spilled value, as its `asset:` entity. Absent when the value is inline.";
    blob => "blob": Entity, "The blob a blob record is about, as its `asset:` entity. Absent on any other entry.";
    blob_size => "blob-size": UnsignedInt, "Size of that blob in bytes. Absent on any other entry.";
    blob_version => "blob-version": UnsignedInt, "The blob record's encoding version. Absent on any other entry.";
}

accessor! {
    /// The `tree/block` resolver: a node's block as stored. One row.
    TreeBlockQuery, TREE_BLOCK_CELLS, of: "The node, as its `node:` entity.";
    bytes => "bytes": Bytes, "The node's block.";
}

accessor! {
    /// The `tree/value` resolver: a spilled value's bytes, by the
    /// `asset:` entity a `tree/entry` row yields as `spill`. The value's
    /// type is in the entry's key (`dialog/key-part`). One row.
    TreeValueQuery, TREE_VALUE_CELLS, of: "The spilled value, as its `asset:` entity.";
    size => "size": UnsignedInt, "Size of the value in bytes.";
    bytes => "bytes": Bytes, "The value's raw bytes.";
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
    /// Resolver `tree/entry`.
    #[serde(rename = "tree/entry")]
    TreeEntry(TreeEntryQuery),
    /// Resolver `tree/block`.
    #[serde(rename = "tree/block")]
    TreeBlock(TreeBlockQuery),
    /// Resolver `tree/value`.
    #[serde(rename = "tree/value")]
    TreeValue(TreeValueQuery),
    /// Resolver `notation/analyze`: declared here, its implementation
    /// supplied by the notation through the environment
    /// ([`crate::supplied`]). No rule may hold it.
    #[serde(rename = "notation/analyze")]
    NotationAnalyze(NotationAnalyzeQuery),
}

/// One slot of a row a resolver yields: the term it binds, and its
/// value, or `None` when the slot does not apply to the row.
pub(crate) type Slot<'a> = (&'a Term<Any>, Option<Value>);

impl ResolverQuery {
    /// Returns the formal notation name (e.g. `"tree/node"`).
    pub fn name(&self) -> &'static str {
        match self {
            Self::TreeNode(_) => "tree/node",
            Self::TreeSpan(_) => "tree/span",
            Self::TreeEntry(_) => "tree/entry",
            Self::TreeBlock(_) => "tree/block",
            Self::TreeValue(_) => "tree/value",
            Self::NotationAnalyze(_) => ANALYZE,
        }
    }

    /// Returns the static cell definitions for this resolver.
    pub(crate) fn cells(&self) -> &'static Cells {
        match self {
            Self::TreeNode(_) => &TREE_NODE_CELLS,
            Self::TreeSpan(_) => &TREE_SPAN_CELLS,
            Self::TreeEntry(_) => &TREE_ENTRY_CELLS,
            Self::TreeBlock(_) => &TREE_BLOCK_CELLS,
            Self::TreeValue(_) => &TREE_VALUE_CELLS,
            Self::NotationAnalyze(_) => &ANALYZE_CELLS,
        }
    }

    /// Returns the schema for this resolver.
    pub fn schema(&self) -> Schema {
        self.cells().into()
    }

    /// The required handle term.
    fn of(&self) -> &Term<Any> {
        match self {
            Self::TreeNode(query) => &query.of,
            Self::TreeSpan(query) => &query.of,
            Self::TreeEntry(query) => &query.of,
            Self::TreeBlock(query) => &query.of,
            Self::TreeValue(query) => &query.of,
            Self::NotationAnalyze(query) => query.forms(),
        }
    }

    /// Estimate the cost of this resolver given the environment.
    ///
    /// `None` while the handle is an unbound variable: nodes are not
    /// enumerable, and a resolver that went looking for them would read,
    /// and fetch, a whole tree — the planner refuses to schedule it until
    /// a join binds the handle. A *blank* `of` (a query that never names
    /// the handle at all) is scheduled but can match nothing: every row
    /// fails the handle lookup, so such a query returns empty rather
    /// than erroring.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        self.of().is_bound(env).then_some(RESOLVER_COST)
    }

    /// Returns the parameters for this resolver application.
    pub fn parameters(&self) -> Parameters {
        match self {
            Self::TreeNode(query) => query.parameters(),
            Self::TreeSpan(query) => query.parameters(),
            Self::TreeEntry(query) => query.parameters(),
            Self::TreeBlock(query) => query.parameters(),
            Self::TreeValue(query) => query.parameters(),
            Self::NotationAnalyze(query) => query.terms().clone(),
        }
    }

    /// This resolver over `terms` instead of its own: a slot `terms`
    /// does not name keeps its current term.
    pub fn with_parameters(&self, terms: &Parameters) -> Self {
        let mut renamed = self.clone();
        match &mut renamed {
            Self::TreeNode(query) => query.replace(terms),
            Self::TreeSpan(query) => query.replace(terms),
            Self::TreeEntry(query) => query.replace(terms),
            Self::TreeBlock(query) => query.replace(terms),
            Self::TreeValue(query) => query.replace(terms),
            Self::NotationAnalyze(query) => query.replace(terms),
        }
        renamed
    }

    /// The hash the handle names in this row: a node's for a `node:`
    /// entity, a spilled value's for an `asset:` given to `tree/value`.
    /// `None` filters the row: a handle that is unbound, of another
    /// scheme, or malformed contributes nothing, mirroring the
    /// forged-record-projects-nothing convention.
    fn reference(&self, base: &Match) -> Option<[u8; 32]> {
        let value = match self.of() {
            Term::Constant(value) => value.clone(),
            term => match base.lookup(term) {
                Ok(crate::Binding::Present(value)) => value,
                _ => return None,
            },
        };
        let Value::Entity(entity) = value else {
            return None;
        };
        match self {
            Self::TreeValue(_) => entity.blob_hash(),
            _ => entity.node_hash(),
        }
    }

    /// Evaluate this resolver over the incoming selection.
    ///
    /// Per input row: take the hash its handle names, ask the
    /// environment for the node ([`nodes::Read`] of the wildcard subject,
    /// as [`anywhere`] names its archive), and project one output row per result. A node the
    /// environment cannot reach, a segment asked for spans, an index
    /// with nothing buffered asked for entries, and a handle that names
    /// no node all yield zero rows.
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
            // A declared resolver whose implementation another crate
            // supplies reads no node: it is asked of that implementation.
            if let ResolverQuery::NotationAnalyze(query) = &resolver {
                for await row in query.clone().evaluate(env, selection) {
                    yield row?;
                }
                return;
            }
            for await candidate in selection {
                let mut base = candidate?;
                // Every row this one extends into shares its bindings.
                base.share();
                let Some(reference) = resolver.reference(&base) else {
                    continue;
                };
                let rows = match &resolver {
                    // A spilled value lives in the blob store, apart from
                    // the tree's nodes, and is read as the bytes it is.
                    ResolverQuery::TreeValue(query) => {
                        let loaded = blob::Load::new(anywhere(), reference)
                            .perform(env)
                            .await
                            .map_err(DialogCorpusError::from)?;
                        let Some(block) = loaded else {
                            continue;
                        };
                        let bytes = verified(&reference, block)?.into_vec();
                        rows(&base, vec![vec![
                            (&query.size, Some(Value::UnsignedInt(bytes.len() as u128))),
                            (&query.bytes, Some(Value::Bytes(bytes))),
                        ]])?
                    }
                    _ => {
                        let read = nodes::Read::<CorpusNode>::new(
                            Subject::any(),
                            Blake3Hash::from(reference),
                        );
                        let Some(node) = read
                            .perform(env)
                            .await
                            .map_err(DialogCorpusError::from)?
                        else {
                            continue;
                        };
                        resolver.project(&base, &reference, &node)?
                    }
                };
                for row in rows {
                    yield row;
                }
            }
        }
    }

    /// The rows `node` projects to under this resolver, each extending
    /// `base`. `reference` is the node's hash.
    fn project(
        &self,
        base: &Match,
        reference: &[u8; 32],
        node: &CorpusNode,
    ) -> Result<Vec<Match>, EvaluationError> {
        let unsigned = |value: u64| Some(Value::UnsignedInt(value.into()));
        let projected = match self {
            Self::TreeNode(query) => {
                let summary = inspect::summary(node)?;
                let manifest = inspect::manifest(node)?;
                vec![vec![
                    (&query.kind, Some(Value::String(summary.kind.into()))),
                    (&query.size, unsigned(summary.size)),
                    (&query.count, unsigned(summary.count)),
                    (&query.scale, unsigned(summary.scale)),
                    (&query.novelty, unsigned(summary.novelty)),
                    (&query.version, unsigned(manifest.version)),
                    (&query.fanout_exponent, unsigned(manifest.fanout_n)),
                    (&query.max_separator, unsigned(manifest.max_separator)),
                    (&query.inline_n, unsigned(manifest.inline_n)),
                    (&query.spill_prefix, unsigned(manifest.spill_prefix)),
                    (&query.max_segment, unsigned(manifest.max_segment)),
                    (
                        &query.frame_ceiling_factor,
                        unsigned(manifest.frame_ceiling_factor),
                    ),
                    (&query.anchor_selector, unsigned(manifest.anchor_selector)),
                    (&query.entry_overhead, unsigned(manifest.entry_overhead)),
                    (&query.key_overhead, unsigned(manifest.key_overhead)),
                    (&query.link_overhead, unsigned(manifest.link_overhead)),
                ]]
            }
            Self::TreeSpan(query) => {
                let mut projected = Vec::new();
                for span in inspect::spans(node)? {
                    projected.push(vec![
                        (
                            &query.span,
                            Some(Value::Entity(placed("span", reference, span.at)?)),
                        ),
                        (&query.at, unsigned(span.at)),
                        (&query.node, Some(Value::Entity(node_entity(&span.node)?))),
                        (&query.separator, Some(Value::Bytes(span.separator))),
                        (&query.until, Some(Value::Bytes(span.until))),
                        (&query.scale, unsigned(span.scale)),
                        (&query.rank, unsigned(span.rank)),
                        (&query.novelty, unsigned(span.novelty)),
                    ]);
                }
                projected
            }
            Self::TreeEntry(query) => {
                let ranks = inspect::keys(node)?;
                let blobs: HashMap<u64, inspect::BlobEntrySummary> = inspect::blob_records(node)
                    // A record this build cannot decode keeps its entry's
                    // row, without the blob slots.
                    .unwrap_or_default()
                    .into_iter()
                    .map(|record| (record.at, record))
                    .collect();
                let mut projected = Vec::new();
                for entry in inspect::entries(node)? {
                    let blob = blobs.get(&entry.at);
                    // A blob record carries no claim: it says what a blob
                    // is, not who wrote a fact.
                    let claim = |value: Option<Value>| if blob.is_some() { None } else { value };
                    let versioned = !entry.origin.is_empty();
                    let spill = match &entry.spill {
                        Some(hash) => Some(Value::Entity(asset_entity(hash)?)),
                        None => None,
                    };
                    let about = match blob {
                        Some(record) => <[u8; 32]>::try_from(record.blob.as_slice())
                            .ok()
                            .map(|hash| asset_entity(&hash))
                            .transpose()?
                            .map(Value::Entity),
                        None => None,
                    };
                    projected.push(vec![
                        (
                            &query.entry,
                            Some(Value::Entity(placed("entry", reference, entry.at)?)),
                        ),
                        (&query.at, unsigned(entry.at)),
                        (
                            &query.span,
                            match entry.span {
                                Some(at) => Some(Value::Entity(placed("span", reference, at)?)),
                                None => None,
                            },
                        ),
                        (
                            &query.rank,
                            ranks
                                .get(entry.at as usize)
                                .and_then(|key| unsigned(key.rank)),
                        ),
                        (&query.key, Some(Value::Bytes(entry.key))),
                        (&query.state, Some(Value::String(entry.state.into()))),
                        (
                            &query.retraction,
                            claim(Some(Value::Boolean(entry.retraction))),
                        ),
                        (
                            &query.origin,
                            claim(versioned.then(|| Value::Bytes(entry.origin.clone()))),
                        ),
                        (
                            &query.edition,
                            claim(versioned.then(|| Value::UnsignedInt(entry.edition.into()))),
                        ),
                        (&query.cause, claim(unsigned(entry.cause))),
                        (&query.collapsed, claim(unsigned(entry.collapsed))),
                        (&query.supersedes, claim(unsigned(entry.supersedes))),
                        (&query.spill, spill),
                        (&query.blob, about),
                        (
                            &query.blob_size,
                            blob.and_then(|record| unsigned(record.size)),
                        ),
                        (
                            &query.blob_version,
                            blob.and_then(|record| unsigned(record.version)),
                        ),
                    ]);
                }
                projected
            }
            // A node that cannot give its block is one that cannot be
            // read: no rows, as for any node that does not decode.
            Self::TreeBlock(query) => node
                .buffer()
                .map(|block| {
                    vec![vec![(
                        &query.bytes,
                        Some(Value::Bytes(block.as_ref().to_vec())),
                    )]]
                })
                .unwrap_or_default(),
            // Read through the blob store in `evaluate`, not as a node.
            Self::TreeValue(_) => Vec::new(),
            // Asked of its implementation in `evaluate`, never a node.
            Self::NotationAnalyze(_) => Vec::new(),
        };
        rows(base, projected)
    }
}

/// The entity naming the tree node `reference` hashes to.
fn node_entity(reference: &[u8; 32]) -> Result<Entity, DialogCorpusError> {
    Entity::from_node(reference)
        .map_err(|error| DialogCorpusError::InvalidValue(format!("node entity: {error:?}")))
}

/// The entity naming the content `hash` names in the blob store.
fn asset_entity(hash: &[u8; 32]) -> Result<Entity, DialogCorpusError> {
    Entity::from_blob(hash)
        .map_err(|error| DialogCorpusError::InvalidValue(format!("asset entity: {error:?}")))
}

/// The entity of a row that is a place in a node: `scheme` (`span` or
/// `entry`), the node's hash, and the row's position in it, as
/// `<scheme>:<base58(hash)>/<position>`.
///
/// A span and an entry are not content of their own: the same key range
/// can be delegated by two nodes, and what is asked of one is what its
/// node holds about it. So a row is named by where it sits, which is
/// also what lets a reader holding the entity find the node.
fn placed(scheme: &str, node: &[u8; 32], at: u64) -> Result<Entity, DialogCorpusError> {
    format!("{scheme}:{}/{at}", node.to_base58())
        .parse()
        .map_err(|error| DialogCorpusError::InvalidValue(format!("{scheme} entity: {error:?}")))
}

/// One row extending `base` per entry of `projected`. A row is dropped
/// when a constant slot disagrees with its value or a variable it would
/// bind is already bound otherwise — the membership-test semantics
/// shared with formulas; any other bind failure is a genuine error.
pub(crate) fn rows(
    base: &Match,
    projected: Vec<Vec<Slot<'_>>>,
) -> Result<Vec<Match>, EvaluationError> {
    let mut rows = Vec::with_capacity(projected.len());
    'row: for slots in projected {
        let mut row = base.clone();
        for (term, value) in slots {
            let bound = match (term, value) {
                (Term::Constant(expected), Some(value)) => {
                    if *expected != value
                        && (expected.data_type() == value.data_type()
                            || !expected.same_number(&value))
                    {
                        continue 'row;
                    }
                    Ok(())
                }
                // A constant asks for a value; a slot that does not
                // apply has none to agree with.
                (Term::Constant(_), None) => continue 'row,
                (term, Some(value)) => row.bind(term, value),
                (term, None) => row.bind_absent(term),
            };
            match bound {
                Ok(()) => {}
                Err(EvaluationError::Assignment { .. })
                | Err(EvaluationError::KindMismatch { .. }) => continue 'row,
                Err(error) => return Err(error),
            }
        }
        rows.push(row);
    }
    Ok(rows)
}

/// A realized resolver row: the value each named slot bound.
/// Constant slots are echoed; slots the row did not bind, or bound
/// absent, are absent.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A row whose constant slot is a number keeps the row when the value
    /// is that number in another type, and drops it when it is another
    /// number.
    #[dialog_common::test]
    fn it_keeps_a_row_whose_value_is_its_constant_by_number_value() -> anyhow::Result<()> {
        let three = Term::<Any>::Constant(Value::UnsignedInt(3));
        let kept = rows(&Match::new(), vec![vec![(&three, Some(Value::Float(3.0)))]])?;
        assert_eq!(kept.len(), 1, "3.0 is 3");
        let dropped = rows(&Match::new(), vec![vec![(&three, Some(Value::Float(3.5)))]])?;
        assert_eq!(dropped.len(), 0, "3.5 is not 3");
        Ok(())
    }

    /// A spilled value's block is checked against the hash its handle
    /// names: a store answering with another block fails the premise
    /// rather than yielding rows attributed to a block it never read.
    #[dialog_common::test]
    fn it_refuses_a_block_that_does_not_hash_to_its_reference() {
        let asked = Buffer::from(b"the block asked for".to_vec());
        let other = Buffer::from(b"some other block".to_vec());
        let reference: [u8; 32] = *asked.blake3_hash().as_bytes();

        assert_eq!(verified(&reference, asked.clone()).unwrap(), asked);
        assert!(matches!(
            verified(&reference, other),
            Err(DialogCorpusError::InvalidValue(_))
        ));
    }

    /// A span and an entry are named by where they sit, and the entity
    /// says so readably.
    #[dialog_common::test]
    fn it_names_a_row_by_its_node_and_position() {
        let node = [7u8; 32];
        let span = placed("span", &node, 3).expect("a span entity");
        assert_eq!(span.as_str(), format!("span:{}/3", node.to_base58()));
        assert_ne!(span, placed("span", &node, 4).unwrap());
        assert_ne!(span, placed("entry", &node, 3).unwrap());
    }

    /// Every resolver deserializes from its name with no slots given,
    /// and its handle is the one slot it requires.
    #[dialog_common::test]
    fn it_requires_only_the_handle() {
        for name in [
            "tree/node",
            "tree/span",
            "tree/entry",
            "tree/block",
            "tree/value",
        ] {
            let query: ResolverQuery =
                serde_json::from_value(serde_json::json!({ "assert": name, "where": {} }))
                    .unwrap_or_else(|error| panic!("{name} deserializes: {error}"));
            assert_eq!(query.name(), name);
            let schema = query.schema();
            let required: Vec<&String> = schema
                .iter()
                .filter(|(_, field)| field.requirement().is_required())
                .map(|(slot, _)| slot)
                .collect();
            assert_eq!(required, vec!["of"], "{name} requires its handle alone");
            assert_eq!(
                query.estimate(&Environment::new()),
                None,
                "{name} is not scheduled without its handle"
            );
        }
    }

    /// In a rule where a fact decides whether a node is read, the fact is
    /// asked first, whether or not the caller gave the node: a node the
    /// fact rules out is then never read, and so never fetched.
    #[dialog_common::test]
    fn it_is_planned_after_the_fact_that_names_its_node() {
        use crate::planner::Plan;
        use crate::rule::DeductiveRuleDescriptor;

        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "kind": { "the": "probe/node-kind", "as": "text:" }
            }},
            "when": [
                {
                    "assert": "tree/node",
                    "where": {
                        "of": { "?": { "name": "this" } },
                        "kind": { "?": { "name": "kind" } }
                    }
                },
                {
                    "assert": { "with": {
                        "held": { "the": "probe/held", "as": "text:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "held": { "?": { "name": "held" } }
                    }
                }
            ]
        }))
        .expect("a rule descriptor");
        let rule = descriptor.compile().expect("the rule compiles");

        let mut given = Environment::new();
        given.add("this");
        for scope in [Environment::new(), given] {
            let plan = rule.plan(&scope);
            assert_eq!(plan.steps.len(), 2, "two premises, two steps");
            assert!(
                matches!(plan.steps.last(), Some(Plan::Resolver(..))),
                "the node is read last: {:?}",
                plan.steps
            );
        }
    }

    /// A slot is named in a query as the resolver's cells name it,
    /// kebab-case, whatever the field is called in Rust.
    #[dialog_common::test]
    fn it_reads_slots_by_their_query_names() {
        let query: ResolverQuery = serde_json::from_value(serde_json::json!({
            "assert": "tree/node",
            "where": { "fanout-exponent": { "?": { "name": "exponent" } } }
        }))
        .expect("deserializes");
        let ResolverQuery::TreeNode(node) = &query else {
            panic!("a tree/node query");
        };
        assert_eq!(node.fanout_exponent.name(), Some("exponent"));
        assert!(query.parameters().get("fanout-exponent").is_some());
        assert!(
            query
                .schema()
                .iter()
                .any(|(slot, _)| slot == "fanout-exponent")
        );
    }
}
