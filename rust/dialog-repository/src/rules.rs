//! Layered deductive-rule resolution.
//!
//! A query reads from a stack of layers — each branch in scope, its
//! session overlay, and the per-query overlay. Just as facts are unioned across
//! layers (see [`layer`](crate::layer)), *rules* are too: each layer
//! reports the deductive rules it holds concluding a queried concept,
//! and the union of those — plus the implicit per-descriptor rule built
//! once — is what the query engine plans.
//!
//! # Storage shape (`dialog.rule/*`)
//!
//! A deductive rule is stored as facts:
//! - `dialog.rule/conclusion` `of` rule-entity `is` the concept entity it
//!   concludes — the index a layer looks rules up by.
//! - `dialog.rule/source` `of` rule-entity `is` the canonical dag-cbor
//!   `DeductiveRuleDescriptor` (a `Value::Bytes`) — the body, hydrated
//!   via `DeductiveRule::decode`. (Bytes, not Record: `Value::Record`
//!   isn't yet supported end-to-end through the index; the bytes are
//!   opaque to the query layer either way.)
//!
//! These names are a dialog-repository convention (like
//! `dialog.session/*`).
//!
//! # Two layers, two caches
//!
//! - A **durable** layer reads a branch's committed tree. Its rule
//!   discovery (the `derives` lookup) is cacheable by branch head —
//!   the committed rule set for a concept only changes when the head
//!   moves. Hydrated bodies are cached by content-addressed rule entity.
//! - A **transient** layer reads an overlay: a line's session overlay,
//!   the per-query overlay, or a transaction's staged writes. Overlay
//!   rules (`tx.assert(rule)` / `.with(rule)`, uncommitted — the head
//!   has NOT moved) are never head-cached: a rule set assembled over
//!   them is keyed by the rule facts each layer holds (see [`RuleKey`]),
//!   which is what makes the "overlay rule masked by a head-keyed cache"
//!   bug structurally impossible.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::RangeInclusive;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use dialog_artifacts::history::REVISION_ATTRIBUTE;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::tree::selector_range;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Entity, Key, Relation, Statement, Update, Value,
};
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::{ConceptRules, Exact, Installed, PlanCache};
use dialog_query::error::EvaluationError;
use dialog_query::formula::revision::{RevisionParentQuery, RevisionQuery};
use dialog_query::rule::statement::{Reach, derives_entities};
use dialog_query::session::Quarantine;
use dialog_query::type_system::Type as Kind;
use dialog_query::types::Any;
use dialog_query::{
    AttributeQuery, Cardinality, ConceptQuery, DeductiveRule, Descriptor, FormulaQuery,
    InductiveRule, Parameters, Premise, Proposition, Term, the,
};
use dialog_search_tree::Manifest;
use parking_lot::RwLock;

use crate::{Revision, schema};
use dialog_common::Blake3Hash;

// The `dialog.rule/*` vocabulary and the Statement lowerings that
// install/uninstall a rule by plain assertion/retraction live with the
// rule types themselves; this module re-uses them for its selectors,
// caches, and dispatch probing.
pub(crate) use dialog_query::rule::statement::{
    conclusion_attr, derives_attr, head_entities, on_attr, quarantined_attr, reads_attr,
    source_attr,
};

/// Whether facts of `the` are rule facts: every `dialog.rule/*` attribute
/// and the `dialog.concept/transient` marker. Rule resolution reads
/// nothing else, so a layer's rule facts name every rule set assembled
/// over it (see [`RuleKey`]), and a write of no rule fact leaves them as
/// they were.
pub(crate) fn is_rule_attribute(the: &Relation) -> bool {
    let name = the.as_str();
    name.starts_with("dialog.rule/") || name == "dialog.concept/transient"
}

/// The `dialog.concept/transient` marker attribute. A concept carrying it
/// is a *command*: facts of it dispatched into a transaction (and heads
/// of rules concluding it) live for one induction round and are never
/// committed.
pub(crate) fn transient_attr() -> Relation {
    the!("dialog.concept/transient").into()
}

/// Hydrate a compiled [`InductiveRule`] from a `dialog.rule/source` claim
/// value (the canonical dag-cbor
/// [`InductiveRuleDescriptor`](dialog_query::rule::inductive::descriptor::InductiveRuleDescriptor)).
pub(crate) fn hydrate_inductive(source: &[u8]) -> Result<InductiveRule, EvaluationError> {
    InductiveRule::decode(source)
        .map_err(|reason| EvaluationError::Store(format!("inductive rule hydrate: {reason}")))
}

/// [`Statement`] wrapper declaring a concept transient: facts of it are
/// commands, dispatched rather than asserted, living for one induction
/// round and never committed. The marker is a branch-level fact — it is
/// deliberately not part of the concept's content address, so the same
/// descriptor is durable on one branch and transient on another.
pub struct Transient(pub Entity);

impl Statement for Transient {
    fn assert(self, update: &mut impl Update) {
        update.associate(
            transient_attr(),
            self.0,
            Value::Boolean(true),
            dialog_artifacts::Pick::All,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(transient_attr(), self.0, Value::Boolean(true));
    }
}

/// Selector for `dialog.rule/derives is = <on:attribute>` — finds the rule
/// entities deriving an attribute, whatever concept they were written
/// against.
pub(crate) fn derives_selector(on: &Entity) -> ArtifactSelector<Constrained> {
    ArtifactSelector::new()
        .the(derives_attr())
        .is(Value::Entity(on.clone()))
}

/// The head-index entities of an attribute concept: one per relation
/// it reads, several for a ranked chain. Empty for a concept that is
/// not an attribute concept.
pub(crate) fn derives_keys(attribute: &ConceptDescriptor) -> Vec<Entity> {
    if attribute.attribute_field().is_none() {
        return Vec::new();
    }
    head_entities(attribute).into_iter().collect()
}

/// Selector for `dialog.rule/source of = <rule>` — fetches a rule's body.
pub(crate) fn source_selector(rule: &Entity) -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(source_attr()).of(rule.clone())
}

/// Hydrate a compiled [`DeductiveRule`] from a `dialog.rule/source` claim
/// value (the canonical dag-cbor [`DeductiveRuleDescriptor`]).
#[tracing::instrument(skip_all, name = "hydrate_rule")]
pub(crate) fn hydrate(source: &[u8]) -> Result<DeductiveRule, EvaluationError> {
    DeductiveRule::decode(source)
        .map_err(|reason| EvaluationError::Store(format!("rule hydrate: {reason}")))
}

/// Extract the rule entities from a batch of `dialog.rule/conclusion`
/// artifacts — each artifact's `of` is a rule entity.
pub(crate) fn rule_entities(conclusion_claims: Vec<Artifact>) -> Vec<Entity> {
    conclusion_claims.into_iter().map(|a| a.of).collect()
}

/// Extract the source bytes from a `dialog.rule/source` artifact batch.
pub(crate) fn source_bytes(source_claims: Vec<Artifact>) -> Option<Vec<u8>> {
    source_claims.into_iter().find_map(|a| match a.is {
        Value::Bytes(bytes) => Some(bytes),
        _ => None,
    })
}

/// The built-in rules concluding `concept`, if it is one of the
/// derived version-control concepts (empty otherwise).
///
/// [`schema::Revision`] and [`schema::RevisionParent`] have no stored
/// facts: a revision describes itself with one signed
/// `dialog.db/revision` record, and these rules project its fields at
/// query time through the `dialog/revision` formulas — which refuse
/// records that don't verify, so forged attribution never surfaces in
/// a query result.
///
/// [`schema::RevisionAncestor`] is the transitive closure of
/// [`schema::RevisionParent`] — the classic recursive pair (a parent
/// is an ancestor; a parent's ancestor is an ancestor), evaluated by
/// the engine's semi-naive fixpoint. Reaching through the parent
/// *concept* rather than re-scanning records keeps the trust boundary
/// in one place: every edge the closure walks was signature-verified
/// by the projection rule.
pub(crate) fn builtin(concept: &Entity) -> Vec<DeductiveRule> {
    static REVISION: OnceLock<DeductiveRule> = OnceLock::new();
    static PARENT: OnceLock<DeductiveRule> = OnceLock::new();
    static ANCESTOR: OnceLock<Vec<DeductiveRule>> = OnceLock::new();

    let revision = <schema::Revision as Descriptor<ConceptDescriptor>>::descriptor();
    if *concept == revision.this() {
        return vec![
            REVISION
                .get_or_init(|| {
                    projection_rule(
                        revision.clone(),
                        RevisionQuery {
                            of: Term::var("record"),
                            this: Term::var("this"),
                            branch: Term::var("branch"),
                            issuer: Term::var("issuer"),
                            authority: Term::var("authority"),
                            edition: Term::var("edition"),
                        }
                        .into(),
                    )
                })
                .clone(),
        ];
    }

    let parent = <schema::RevisionParent as Descriptor<ConceptDescriptor>>::descriptor();
    if *concept == parent.this() {
        return vec![
            PARENT
                .get_or_init(|| {
                    projection_rule(
                        parent.clone(),
                        RevisionParentQuery {
                            of: Term::var("record"),
                            this: Term::var("this"),
                            parent: Term::var("parent"),
                        }
                        .into(),
                    )
                })
                .clone(),
        ];
    }

    let ancestor = <schema::RevisionAncestor as Descriptor<ConceptDescriptor>>::descriptor();
    if *concept == ancestor.this() {
        return ANCESTOR
            .get_or_init(|| ancestor_rules(ancestor.clone(), parent.clone()))
            .clone();
    }

    let pull = <schema::PullUpstream as Descriptor<ConceptDescriptor>>::descriptor();
    if *concept == pull.this() {
        static PULL: OnceLock<DeductiveRule> = OnceLock::new();
        return vec![
            PULL.get_or_init(|| {
                upstream_rule(
                    pull.clone(),
                    <schema::BranchPull as Descriptor<ConceptDescriptor>>::descriptor().clone(),
                    "pull",
                )
            })
            .clone(),
        ];
    }

    let push = <schema::PushUpstream as Descriptor<ConceptDescriptor>>::descriptor();
    if *concept == push.this() {
        static PUSH: OnceLock<DeductiveRule> = OnceLock::new();
        return vec![
            PUSH.get_or_init(|| {
                upstream_rule(
                    push.clone(),
                    <schema::BranchPush as Descriptor<ConceptDescriptor>>::descriptor().clone(),
                    "push",
                )
            })
            .clone(),
        ];
    }

    Vec::new()
}

/// Every built-in rule, for indexing by head attribute.
pub(crate) fn builtin_rules() -> &'static [DeductiveRule] {
    static ALL: OnceLock<Vec<DeductiveRule>> = OnceLock::new();
    ALL.get_or_init(|| {
        [
            <schema::Revision as Descriptor<ConceptDescriptor>>::descriptor().this(),
            <schema::RevisionParent as Descriptor<ConceptDescriptor>>::descriptor().this(),
            <schema::RevisionAncestor as Descriptor<ConceptDescriptor>>::descriptor().this(),
            <schema::PullUpstream as Descriptor<ConceptDescriptor>>::descriptor().this(),
            <schema::PushUpstream as Descriptor<ConceptDescriptor>>::descriptor().this(),
        ]
        .iter()
        .flat_map(builtin)
        .collect()
    })
}

/// The built-in rules deriving the attribute concept `attribute`, each
/// re-headed onto it: how a query over one attribute of a built-in
/// concept sees the built-in derivation.
/// Whether a built-in rule derives the relation whose trigger key is
/// `on`: the key alone decides, so a caller formed it from the
/// attribute without describing or hashing a concept.
pub(crate) fn builtin_derives(on: &Entity) -> bool {
    static KEYS: OnceLock<HashSet<Entity>> = OnceLock::new();
    KEYS.get_or_init(|| builtin_rules().iter().flat_map(derives_entities).collect())
        .contains(on)
}

pub(crate) fn builtin_deriving(attribute: &Entity) -> Vec<DeductiveRule> {
    static HEADS: OnceLock<HashMap<Entity, Vec<DeductiveRule>>> = OnceLock::new();
    HEADS
        .get_or_init(|| {
            let mut heads: HashMap<Entity, Vec<DeductiveRule>> = HashMap::new();
            for rule in builtin_rules() {
                for head in rule.heads().expect("built-in rules split per attribute") {
                    heads
                        .entry(ConceptDescriptor::of_attribute(&head.field).this())
                        .or_default()
                        .push(head.rule);
                }
            }
            heads
        })
        .get(attribute)
        .cloned()
        .unwrap_or_default()
}

/// The rule resolving a branch's pull or push relation to where the
/// tracked branch lives:
///
/// ```text
/// upstream(this, upstream, name, subject, peer) :-
///     relation(this, upstream),
///     branch(upstream, name, replica),
///     replica(replica, subject, peer).
/// ```
///
/// `relation` is [`schema::BranchPull`] or [`schema::BranchPush`], and
/// `field` the name of its tracked-branch field.
fn upstream_rule(
    conclusion: ConceptDescriptor,
    relation: ConceptDescriptor,
    field: &str,
) -> DeductiveRule {
    fn premise(predicate: ConceptDescriptor, terms: &[(&str, &str)]) -> Premise {
        let mut parameters = Parameters::new();
        for (field, var) in terms {
            parameters.insert((*field).to_string(), Term::<Any>::var(*var));
        }
        Premise::Assert(Proposition::Concept(ConceptQuery {
            terms: parameters,
            predicate,
        }))
    }

    DeductiveRule::new(
        conclusion,
        vec![
            premise(relation, &[("this", "this"), (field, "upstream")]),
            premise(
                <schema::Branch as Descriptor<ConceptDescriptor>>::descriptor().clone(),
                &[
                    ("this", "upstream"),
                    ("name", "name"),
                    ("replica", "replica"),
                ],
            ),
            premise(
                <schema::Replica as Descriptor<ConceptDescriptor>>::descriptor().clone(),
                &[
                    ("this", "replica"),
                    ("subject", "subject"),
                    ("peer", "peer"),
                ],
            ),
        ],
    )
    .expect("the upstream rule compiles")
}

/// The recursive pair concluding [`schema::RevisionAncestor`]:
///
/// ```text
/// ancestor(this, a) :- parent(this, a).
/// ancestor(this, a) :- parent(this, p), ancestor(p, a).
/// ```
fn ancestor_rules(conclusion: ConceptDescriptor, parent: ConceptDescriptor) -> Vec<DeductiveRule> {
    fn edge(parent: &ConceptDescriptor, this: &str, parent_var: &str) -> Premise {
        let mut terms = Parameters::new();
        terms.insert("this".to_string(), Term::<Any>::var(this));
        terms.insert("parent".to_string(), Term::<Any>::var(parent_var));
        Premise::Assert(Proposition::Concept(ConceptQuery {
            terms,
            predicate: parent.clone(),
        }))
    }

    let base = DeductiveRule::new(conclusion.clone(), vec![edge(&parent, "this", "ancestor")])
        .expect("the ancestor base rule compiles");

    let mut step_terms = Parameters::new();
    step_terms.insert("this".to_string(), Term::<Any>::var("p"));
    step_terms.insert("ancestor".to_string(), Term::<Any>::var("ancestor"));
    let step = DeductiveRule::new(
        conclusion.clone(),
        vec![
            edge(&parent, "this", "p"),
            Premise::Assert(Proposition::Concept(ConceptQuery {
                terms: step_terms,
                predicate: conclusion,
            })),
        ],
    )
    .expect("the ancestor step rule compiles");

    vec![base, step]
}

/// Assemble a record-projection rule: scan the revision entity's
/// `dialog.db/revision` fact into `?record`, then apply `formula` to
/// project its fields. The formula derives `?this` from the record's
/// own contents, so sharing the variable with the scan's entity makes
/// the join reject a record replayed at another revision entity.
fn projection_rule(conclusion: ConceptDescriptor, formula: FormulaQuery) -> DeductiveRule {
    let scan: Premise = AttributeQuery::new(
        Term::Constant(Value::Symbol(
            REVISION_ATTRIBUTE
                .parse()
                .expect("the revision attribute is valid"),
        )),
        Term::var("this"),
        Term::<Any>::typed_var("record", Kind::from(dialog_query::Type::Record)),
        Term::blank(),
        Some(Cardinality::One),
    )
    .into();

    DeductiveRule::new(conclusion, vec![scan, formula.into()])
        .expect("the revision projection rule compiles")
}

/// Per-branch caches for durable rule discovery + hydration.
///
/// Held on a [`Branch`](crate::Branch), shared (`Arc`) so the work one
/// query does benefits the next.
#[derive(Debug, Default)]
pub struct RuleCache {
    inner: RwLock<RuleCacheInner>,
    /// How many assembled rule sets were served, so a test can tell a
    /// reuse from a rebuild.
    #[cfg(test)]
    reused: AtomicUsize,
}

/// The committed trigger footprint at a branch head: every `on:`
/// entity present in `dialog.rule/on` (inductive triggers) and
/// `dialog.rule/reads` (deductive support edges). The O(1) gate commit-time
/// dispatch intersects touched attributes against before any probe.
#[derive(Debug, Default, Clone)]
pub(crate) struct TriggerFootprint {
    /// `on:` entities some inductive rule watches.
    pub(crate) on: BTreeSet<Entity>,
    /// `on:` entities some deductive rule's body reads.
    pub(crate) reads: BTreeSet<Entity>,
}

/// A tree root, or `None` for a line with no tree yet.
type Root = Option<[u8; 32]>;

/// The rule facts of the layers a rule set was assembled over: what the
/// set is a function of, so a later read whose layers hold the same rule
/// facts is served it.
///
/// A line is named by its rules root: the root of the newest tree whose
/// commit changed a rule fact, carried across the commits since (see
/// [`RuleCache::rules_root`]). An overlay or staged layer is named by
/// the hash of the rule facts it holds and hides, and is left out when
/// it holds none, so a query with a session overlay of plain facts, or a
/// transaction that staged none, shares the branch query's entries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuleKey {
    /// Each line's rules root.
    pub(crate) lines: Vec<Root>,
    /// Each overlay (the lines' session overlays, then the query's own)
    /// that holds rule facts, by its position and their hash.
    pub(crate) overlays: Vec<(usize, Blake3Hash)>,
    /// Each staged layer that holds rule facts, likewise.
    pub(crate) staged: Vec<(usize, Blake3Hash)>,
}

#[derive(Debug, Default)]
struct RuleCacheInner {
    /// Which rule entities derive an attribute, each with the standing
    /// of the commit installing it, as of a branch head. Keyed by the
    /// attribute's `on:` entity and tagged with the head it was scanned
    /// at, so a head advance (commit/pull) re-scans it.
    derived: HashMap<Entity, (Revision, Vec<(Entity, Installed)>)>,
    /// A rule's head re-spelled onto an attribute concept, keyed by
    /// (rule entity, attribute concept entity). Both halves are
    /// content-addressed, so an entry is never stale.
    heads: HashMap<(Entity, Entity), DeductiveRule>,
    /// Hydrated rule bodies, keyed by the rule entity and the hash of the
    /// bytes they were decoded from. A body is looked up only with bytes
    /// the reader read itself, so the cache decodes and never answers
    /// whether a rule exists, and forged bytes under a known entity miss
    /// it and fail their content-address check as they would cold.
    bodies: HashMap<(Entity, Blake3Hash), DeductiveRule>,
    /// The committed trigger footprint, as of a branch head.
    footprint: Option<(Revision, TriggerFootprint)>,
    /// Committed inductive-rule entities watching an `on:` entity, as
    /// of a branch head.
    triggers: HashMap<Entity, (Revision, Vec<Entity>)>,
    /// Committed deductive-rule entities whose bodies read an `on:`
    /// entity, as of a branch head.
    reads: HashMap<Entity, (Revision, Vec<Entity>)>,
    /// Hydrated inductive bodies, keyed like `bodies`.
    inductive: HashMap<(Entity, Blake3Hash), InductiveRule>,
    /// Whether a concept carries the committed `dialog.concept/transient`
    /// marker, as of a branch head.
    transient: HashMap<Entity, (Revision, bool)>,
    /// A concept's assembled rule set -- built-in, committed, overlay,
    /// and its program analysis attached -- as of the [`RuleKey`] it
    /// was resolved from. Assembling it is most of what planning a warm
    /// query costs, and it changes only when a layer does.
    ///
    /// Kept with the descriptor it was assembled for: the set carries
    /// that descriptor's implicit rule, which binds its field names, and
    /// descriptors differing only in field names share an identity. One
    /// set per spelling, at most [`SPELLINGS`] per concept, so queries
    /// spelling one concept differently (a view's fields and a rule
    /// premise's) do not evict each other's.
    bundles: HashMap<Entity, Vec<Bundle>>,
    /// Every rule the program analysis sets aside over layers holding
    /// these rule facts, with the rule-discovery reads finding them.
    quarantined: Option<(RuleKey, Vec<Quarantine>, RuleDemand)>,
    /// A tree root and the rules root it carries: the root of the newest
    /// tree before it whose commit changed a rule fact. Recorded by a
    /// commit that changes none, as the head it minted; any other head
    /// is its own rules root.
    rules_root: Option<(Root, Root)>,
    /// A selecting concept's rule and covering rule, keyed by the
    /// concept, the attributes it reads as derived and the one source
    /// rule deriving them (if one). Pure functions of their key, so
    /// never stale; kept with the descriptor, whose field names the
    /// rule binds.
    selecting: HashMap<SelectingKey, Selecting>,
}

/// How many spellings of one concept keep an assembled rule set each.
const SPELLINGS: usize = 4;

/// A rule set assembled for one descriptor, as of the rule facts of the
/// layers it was resolved from.
#[derive(Debug, Clone)]
struct Bundle {
    key: RuleKey,
    descriptor: ConceptDescriptor,
    rules: ConceptRules,
    /// What the rule-discovery reads assembling it demanded, recorded
    /// as rule demand for a subscription that reuses it.
    demand: RuleDemand,
}

/// One rule-discovery read: the selector and the manifest it was keyed
/// under.
pub(crate) type RuleRead = (ArtifactSelector<Constrained>, Manifest);

/// The key ranges an entry's rule-discovery reads demanded, by the format
/// they were keyed under, each read's range built once when the entry is
/// recorded and merged with the ranges it overlaps. A subscription served
/// the entry records these as they are: an entry serves many queries, and
/// building a range per read for each was most of what serving it cost.
#[derive(Clone, Debug, Default)]
pub(crate) struct RuleDemand(Arc<[(Manifest, Vec<RangeInclusive<Key>>)]>);

impl RuleDemand {
    /// The ranges `reads` demand.
    pub(crate) fn new(reads: Vec<RuleRead>) -> Self {
        let mut keyed: Vec<(Manifest, Vec<RangeInclusive<Key>>)> = Vec::new();
        for (selector, manifest) in reads {
            let range = selector_range(&selector, &manifest);
            match keyed.iter_mut().find(|(keyed, _)| *keyed == manifest) {
                Some((_, ranges)) => ranges.push(range),
                None => keyed.push((manifest, vec![range])),
            }
        }
        for (_, ranges) in &mut keyed {
            ranges.sort_by(|a, b| a.start().cmp(b.start()));
            let mut merged: Vec<RangeInclusive<Key>> = Vec::with_capacity(ranges.len());
            for range in ranges.drain(..) {
                match merged.last_mut() {
                    Some(last) if range.start() <= last.end() => {
                        if range.end() > last.end() {
                            *last = last.start().clone()..=range.end().clone();
                        }
                    }
                    _ => merged.push(range),
                }
            }
            *ranges = merged;
        }
        Self(keyed.into())
    }

    /// Record these ranges as `demand`'s rule demand.
    pub(crate) fn record(&self, demand: &crate::Demand) {
        for (manifest, ranges) in self.0.iter() {
            demand.record_rule_ranges(manifest, ranges);
        }
    }
}

/// What a selecting rule is a function of: the concept, the attribute
/// concepts it reads as derived (sorted) and the sole source rule
/// deriving them, when there is one.
type SelectingKey = (Entity, Vec<Entity>, Option<Entity>);

/// A selecting concept's rules, with the descriptor they were built
/// for.
#[derive(Clone, Debug)]
pub(crate) struct Selecting {
    pub(crate) descriptor: ConceptDescriptor,
    pub(crate) rule: DeductiveRule,
    pub(crate) exact: Option<Exact>,
}

impl RuleCache {
    /// A fresh, empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cached committed rule entities deriving the attribute `on`, each
    /// with when it was installed, if scanned at `head`; `None` if
    /// absent or stale.
    pub(crate) fn derived(&self, on: &Entity, head: &Revision) -> Option<Vec<(Entity, Installed)>> {
        let inner = self.inner.read();
        match inner.derived.get(on) {
            Some((scanned_at, entities)) if scanned_at == head => Some(entities.clone()),
            _ => None,
        }
    }

    /// Record the committed rule entities deriving `on` at `head`.
    pub(crate) fn record_derived(
        &self,
        on: Entity,
        head: Revision,
        entities: Vec<(Entity, Installed)>,
    ) {
        self.inner.write().derived.insert(on, (head, entities));
    }

    /// The head of `rule` deriving the relation indexed by `attribute`, if recorded.
    pub(crate) fn head(&self, rule: &Entity, attribute: &Entity) -> Option<DeductiveRule> {
        self.inner
            .read()
            .heads
            .get(&(rule.clone(), attribute.clone()))
            .cloned()
    }

    /// Record the head of `rule` re-spelled onto `attribute`.
    pub(crate) fn record_head(&self, rule: Entity, attribute: Entity, head: DeductiveRule) {
        self.inner.write().heads.insert((rule, attribute), head);
    }

    /// The rules root of the tree at `root`: the root a commit that
    /// changed no rule fact carried to it, or `root` itself. A head moved
    /// any other way (a pull, a merge, a commit before this cache) is
    /// its own rules root, so its rule sets are assembled once again,
    /// never served stale.
    pub(crate) fn rules_root(&self, root: Root) -> Root {
        match &self.inner.read().rules_root {
            Some((head, rules)) if *head == root => *rules,
            _ => root,
        }
    }

    /// Note that the commit from the tree at `previous` to the tree at
    /// `root` changed no rule fact, so `root` carries the rules root of
    /// `previous`.
    pub(crate) fn carry_rules_root(&self, previous: Root, root: Root) {
        let rules = self.rules_root(previous);
        self.inner.write().rules_root = Some((root, rules));
    }

    /// Every rule set aside over layers holding the rule facts `key`
    /// names, if recorded for exactly those, with what the reads that
    /// found them demanded.
    pub(crate) fn quarantined(&self, key: &RuleKey) -> Option<(Vec<Quarantine>, RuleDemand)> {
        match &self.inner.read().quarantined {
            Some((at, quarantined, demand)) if at == key => {
                Some((quarantined.clone(), demand.clone()))
            }
            _ => None,
        }
    }

    /// Record every rule set aside over layers holding the rule facts
    /// `key` names.
    pub(crate) fn record_quarantined(
        &self,
        key: RuleKey,
        quarantined: Vec<Quarantine>,
        reads: Vec<RuleRead>,
    ) {
        self.inner.write().quarantined = Some((key, quarantined, RuleDemand::new(reads)));
    }

    /// The rule set assembled for `descriptor` over layers holding the
    /// rule facts `key` names, if one was recorded for exactly that
    /// descriptor and those rule facts.
    pub(crate) fn bundle(
        &self,
        descriptor: &ConceptDescriptor,
        key: &RuleKey,
    ) -> Option<(ConceptRules, RuleDemand)> {
        let inner = self.inner.read();
        let bundle = inner
            .bundles
            .get(&descriptor.this())?
            .iter()
            .find(|bundle| bundle.descriptor == *descriptor && bundle.key == *key)?;
        #[cfg(test)]
        self.reused.fetch_add(1, Ordering::Relaxed);
        Some((bundle.rules.clone(), bundle.demand.clone()))
    }

    /// How many spellings of `concept` hold an assembled rule set.
    #[cfg(test)]
    pub(crate) fn spellings(&self, concept: &Entity) -> usize {
        self.inner.read().bundles.get(concept).map_or(0, Vec::len)
    }

    /// How many assembled rule sets [`bundle`](Self::bundle) served.
    #[cfg(test)]
    pub(crate) fn reused(&self) -> usize {
        self.reused.load(Ordering::Relaxed)
    }

    /// Record the rule set assembled for `descriptor` over layers holding
    /// the rule facts `key` names, with the rule-discovery reads
    /// assembling it made, replacing one recorded for that spelling
    /// before, and the oldest spelling of its concept past
    /// [`SPELLINGS`].
    pub(crate) fn record_bundle(
        &self,
        descriptor: ConceptDescriptor,
        key: RuleKey,
        rules: ConceptRules,
        reads: Vec<RuleRead>,
    ) {
        let mut inner = self.inner.write();
        let spellings = inner.bundles.entry(descriptor.this()).or_default();
        spellings.retain(|bundle| bundle.descriptor != descriptor);
        if spellings.len() == SPELLINGS {
            spellings.remove(0);
        }
        spellings.push(Bundle {
            key,
            descriptor,
            rules,
            demand: RuleDemand::new(reads),
        });
    }

    /// The selecting rules built for `descriptor` reading `derived`
    /// through attribute concepts with `sole` as their one source, if
    /// built before for a descriptor spelled the same.
    pub(crate) fn selecting(
        &self,
        descriptor: &ConceptDescriptor,
        derived: &[Entity],
        sole: &Option<Entity>,
    ) -> Option<Selecting> {
        let inner = self.inner.read();
        let key = (descriptor.this(), derived.to_vec(), sole.clone());
        inner
            .selecting
            .get(&key)
            .filter(|found| found.descriptor == *descriptor)
            .cloned()
    }

    /// Record the selecting rules built for `descriptor`.
    pub(crate) fn record_selecting(
        &self,
        derived: Vec<Entity>,
        sole: Option<Entity>,
        selecting: Selecting,
    ) {
        let key = (selecting.descriptor.this(), derived, sole);
        self.inner.write().selecting.insert(key, selecting);
    }

    /// The body `rule` decoded to from `bytes`, if decoded before.
    pub(crate) fn body(&self, rule: &Entity, bytes: &[u8]) -> Option<DeductiveRule> {
        self.inner
            .read()
            .bodies
            .get(&(rule.clone(), Blake3Hash::hash(bytes)))
            .cloned()
    }

    /// Cache the body `rule` decoded to from `bytes`, once it was checked
    /// to be stored under its content address.
    pub(crate) fn record_body(&self, rule: Entity, bytes: &[u8], body: DeductiveRule) {
        self.inner
            .write()
            .bodies
            .insert((rule, Blake3Hash::hash(bytes)), body);
    }

    /// The committed trigger footprint if scanned at `head`; `None` if
    /// absent or stale.
    pub(crate) fn footprint(&self, head: &Revision) -> Option<TriggerFootprint> {
        match &self.inner.read().footprint {
            Some((scanned_at, footprint)) if scanned_at == head => Some(footprint.clone()),
            _ => None,
        }
    }

    /// Record the committed trigger footprint at `head`.
    pub(crate) fn record_footprint(&self, head: Revision, footprint: TriggerFootprint) {
        self.inner.write().footprint = Some((head, footprint));
    }

    /// Cached committed inductive-rule entities watching `on` if
    /// scanned at `head`.
    pub(crate) fn triggers(&self, on: &Entity, head: &Revision) -> Option<Vec<Entity>> {
        match self.inner.read().triggers.get(on) {
            Some((scanned_at, entities)) if scanned_at == head => Some(entities.clone()),
            _ => None,
        }
    }

    /// Record the committed inductive-rule entities watching `on` at
    /// `head`.
    pub(crate) fn record_triggers(&self, on: Entity, head: Revision, entities: Vec<Entity>) {
        self.inner.write().triggers.insert(on, (head, entities));
    }

    /// Cached committed deductive-rule entities reading `on` if
    /// scanned at `head`.
    pub(crate) fn reads(&self, on: &Entity, head: &Revision) -> Option<Vec<Entity>> {
        match self.inner.read().reads.get(on) {
            Some((scanned_at, entities)) if scanned_at == head => Some(entities.clone()),
            _ => None,
        }
    }

    /// Record the committed deductive-rule entities reading `on` at
    /// `head`.
    pub(crate) fn record_reads(&self, on: Entity, head: Revision, entities: Vec<Entity>) {
        self.inner.write().reads.insert(on, (head, entities));
    }

    /// The inductive body `rule` decoded to from `bytes`, if decoded
    /// before.
    pub(crate) fn inductive(&self, rule: &Entity, bytes: &[u8]) -> Option<InductiveRule> {
        self.inner
            .read()
            .inductive
            .get(&(rule.clone(), Blake3Hash::hash(bytes)))
            .cloned()
    }

    /// Cache the inductive body `rule` decoded to from `bytes`, once it
    /// was checked to be stored under its content address.
    pub(crate) fn record_inductive(&self, rule: Entity, bytes: &[u8], body: InductiveRule) {
        self.inner
            .write()
            .inductive
            .insert((rule, Blake3Hash::hash(bytes)), body);
    }

    /// The cached committed transience verdict for `concept` if
    /// scanned at `head`.
    pub(crate) fn transient(&self, concept: &Entity, head: &Revision) -> Option<bool> {
        match self.inner.read().transient.get(concept) {
            Some((scanned_at, verdict)) if scanned_at == head => Some(*verdict),
            _ => None,
        }
    }

    /// Record the committed transience verdict for `concept` at `head`.
    pub(crate) fn record_transient(&self, concept: Entity, head: Revision, verdict: bool) {
        self.inner
            .write()
            .transient
            .insert(concept, (head, verdict));
    }
}

/// Assemble a [`ConceptRules`] for `concept` from the implicit rule plus
/// the installed rules found across the layers' rule sets.
///
/// `durable` are the rules read (and cached) from each branch's
/// committed tree; `transient` are read fresh from the overlay. Both are
/// already hydrated; this just installs them onto the implicit rule.
///
/// `plan_cache` is the owning branch's shared plan cache, so the
/// per-query re-assembly reuses plans earlier queries computed.
pub(crate) fn assemble(
    concept: &ConceptDescriptor,
    rules: impl IntoIterator<Item = DeductiveRule>,
    plan_cache: PlanCache,
) -> ConceptRules {
    let mut concept_rules = ConceptRules::with_plan_cache(concept, plan_cache);
    for rule in rules {
        concept_rules.install(rule);
    }
    concept_rules
}

/// The head of `rule` deriving the relation indexed by `on`, if it has
/// one: the head concluding that relation's attribute concept, whatever
/// type or pick the reader declares over it.
pub(crate) fn head_onto(
    rule: &DeductiveRule,
    on: &Entity,
) -> Result<Option<DeductiveRule>, EvaluationError> {
    let heads = rule
        .heads()
        .map_err(|error| EvaluationError::Store(format!("rule head: {error}")))?;
    Ok(heads
        .into_iter()
        .find(|head| Reach::of(head.field.the()).on_entity() == Some(on.clone()))
        .map(|head| head.rule))
}

// Re-export a shared cache handle type alias for the branch to hold.
pub(crate) type SharedRuleCache = Arc<RuleCache>;

#[cfg(test)]
mod tests {

    use super::*;
    use dialog_query::session::ProgramAnalysis;

    /// The ancestor closure only works if the engine notices the
    /// rule's self-reference and routes evaluation through the
    /// fixpoint — a rules-shape regression here would surface as
    /// unbounded top-down recursion at query time.
    #[test]
    fn it_builds_recursive_ancestor_rules() {
        let ancestor = <schema::RevisionAncestor as Descriptor<ConceptDescriptor>>::descriptor();
        let entity = ancestor.this();
        let rules = builtin(&entity);
        assert_eq!(rules.len(), 2, "the base rule and the inductive step");
        // The analysis keys an attribute concept by its relation, where
        // every read of it meets.
        let node = ProgramAnalysis::node(ancestor);
        let bundle = assemble(ancestor, rules, PlanCache::default());
        let analysis = ProgramAnalysis::analyze([(&node, &bundle)]);
        assert!(
            analysis.is_recursive(&node),
            "the step rule's self-reference makes the concept recursive"
        );
    }
}
