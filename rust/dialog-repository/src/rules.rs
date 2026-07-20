//! Layered deductive-rule resolution.
//!
//! A query reads from a stack of layers — each branch in scope, plus
//! the per-query [`Changes`] overlay. Just as facts are unioned across
//! layers (see [`layer`](crate::layer)), *rules* are too: each layer
//! reports the deductive rules it holds concluding a queried concept,
//! and the union of those — plus the implicit per-descriptor rule built
//! once — is what the query engine plans.
//!
//! # Storage shape (`db.rule/*`)
//!
//! A deductive rule is stored as facts:
//! - `db.rule/conclusion` `of` rule-entity `is` the concept entity it
//!   concludes — the index a layer looks rules up by.
//! - `db.rule/source` `of` rule-entity `is` the canonical dag-cbor
//!   `DeductiveRuleDescriptor` (a `Value::Bytes`) — the body, hydrated
//!   via `DeductiveRule::decode`. (Bytes, not Record: `Value::Record`
//!   isn't yet supported end-to-end through the index; the bytes are
//!   opaque to the query layer either way.)
//!
//! These names are a dialog-repository convention (like
//! `dialog.session/*` / `dialog.meta/*`).
//!
//! # Two layers, two caches
//!
//! - A **durable** layer reads a branch's committed tree. Its rule
//!   discovery (the `conclusion` lookup) is cacheable by branch head —
//!   the committed rule set for a concept only changes when the head
//!   moves. Hydrated bodies are cached by content-addressed rule entity.
//! - A **transient** layer reads the per-query overlay. Overlay rules
//!   (`tx.assert(rule)` / `.with(rule)`, uncommitted — the head has NOT
//!   moved) are read fresh every query and never head-cached. Keeping
//!   the overlay in its own layer is what makes the "overlay rule masked
//!   by a head-keyed cache" bug structurally impossible.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use dialog_artifacts::history::REVISION_ATTRIBUTE;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{Artifact, ArtifactSelector, Attribute, Changes, Entity, Value};
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::{ConceptRules, PlanCache};
use dialog_query::error::EvaluationError;
use dialog_query::formula::revision::{RevisionParentQuery, RevisionQuery};
use dialog_query::type_system::Type as Kind;
use dialog_query::types::Any;
use dialog_query::{
    AttributeQuery, Cardinality, ConceptQuery, DeductiveRule, Descriptor, FormulaQuery, Parameters,
    Premise, Proposition, Term, the,
};
use parking_lot::RwLock;

use crate::{Revision, schema};

/// The `db.rule/conclusion` index attribute, validated at compile time.
fn conclusion_attr() -> Attribute {
    the!("db.rule/conclusion").into()
}

/// The `db.rule/source` body attribute, validated at compile time.
fn source_attr() -> Attribute {
    the!("db.rule/source").into()
}

/// Selector for `db.rule/conclusion is = <concept>` — finds the rule
/// entities concluding a concept.
pub(crate) fn conclusion_selector(concept: &Entity) -> ArtifactSelector<Constrained> {
    ArtifactSelector::new()
        .the(conclusion_attr())
        .is(Value::Entity(concept.clone()))
}

/// Selector for `db.rule/source of = <rule>` — fetches a rule's body.
pub(crate) fn source_selector(rule: &Entity) -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(source_attr()).of(rule.clone())
}

/// Hydrate a compiled [`DeductiveRule`] from a `db.rule/source` claim
/// value (the canonical dag-cbor [`DeductiveRuleDescriptor`]).
pub(crate) fn hydrate(source: &[u8]) -> Result<DeductiveRule, EvaluationError> {
    DeductiveRule::decode(source)
        .map_err(|reason| EvaluationError::Store(format!("rule hydrate: {reason}")))
}

/// Extract the rule entities from a batch of `db.rule/conclusion`
/// artifacts — each artifact's `of` is a rule entity.
pub(crate) fn rule_entities(conclusion_claims: Vec<Artifact>) -> Vec<Entity> {
    conclusion_claims.into_iter().map(|a| a.of).collect()
}

/// Extract the source bytes from a `db.rule/source` artifact batch.
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
                            lineage: Term::var("lineage"),
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

    Vec::new()
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
}

#[derive(Debug, Default)]
struct RuleCacheInner {
    /// Which rule entities conclude a concept, as of a branch head.
    /// Keyed by concept; tagged with the head it was scanned at so a
    /// head advance (commit/pull) triggers a re-scan of that concept.
    discovery: HashMap<Entity, (Revision, Vec<Entity>)>,
    /// Hydrated rule bodies, keyed by content-addressed rule entity.
    /// Never stale (the key is a content hash), so this survives head
    /// changes and is shared across concepts.
    bodies: HashMap<Entity, DeductiveRule>,
}

impl RuleCache {
    /// A fresh, empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cached committed rule entities concluding `concept` if scanned at
    /// `head`; `None` if absent or stale (caller must re-scan the tree).
    pub(crate) fn discovered(&self, concept: &Entity, head: &Revision) -> Option<Vec<Entity>> {
        let inner = self.inner.read();
        match inner.discovery.get(concept) {
            Some((scanned_at, entities)) if scanned_at == head => Some(entities.clone()),
            _ => None,
        }
    }

    /// Record the committed rule entities concluding `concept` at `head`.
    pub(crate) fn record_discovery(&self, concept: Entity, head: Revision, entities: Vec<Entity>) {
        self.inner
            .write()
            .discovery
            .insert(concept, (head, entities));
    }

    /// A cached hydrated body by rule entity, if present.
    pub(crate) fn body(&self, rule: &Entity) -> Option<DeductiveRule> {
        self.inner.read().bodies.get(rule).cloned()
    }

    /// Cache a hydrated body under its content-addressed entity.
    pub(crate) fn record_body(&self, rule: Entity, body: DeductiveRule) {
        self.inner.write().bodies.insert(rule, body);
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

/// Read rules from an overlay [`Changes`] batch concluding `concept`.
///
/// The overlay is in-memory, so this is cheap and done fresh every
/// query (never cached). Walks the batch for `db.rule/conclusion`
/// pointing at `concept`, then their `db.rule/source` bodies.
pub(crate) fn overlay_rules(changes: &Changes, concept: &Entity) -> Vec<DeductiveRule> {
    use dialog_artifacts::Change;

    let conclusion = conclusion_attr();
    let source = source_attr();

    // rule entities whose conclusion is `concept`, asserted in the overlay.
    let mut rule_entities: Vec<Entity> = Vec::new();
    for (entity, attribute, change) in changes.iter() {
        if *attribute == conclusion
            && let Change::Assert(Value::Entity(c)) | Change::Replace(Value::Entity(c)) = change
            && c == concept
        {
            rule_entities.push(entity.clone());
        }
    }

    // each rule entity's source body, hydrated.
    let mut out = Vec::new();
    for rule_entity in rule_entities {
        for (entity, attribute, change) in changes.iter() {
            if *entity == rule_entity
                && *attribute == source
                && let Change::Assert(Value::Bytes(bytes)) | Change::Replace(Value::Bytes(bytes)) =
                    change
                && let Ok(rule) = hydrate(bytes)
            {
                out.push(rule);
                break;
            }
        }
    }
    out
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
        let bundle = assemble(ancestor, rules, PlanCache::default());
        let analysis = ProgramAnalysis::analyze([(&entity, &bundle)]);
        assert!(
            analysis.is_recursive(&entity),
            "the step rule's self-reference makes the concept recursive"
        );
    }
}
