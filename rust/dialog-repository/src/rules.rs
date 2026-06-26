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
//! - `db.rule/source` `of` rule-entity `is` the JSON
//!   [`DeductiveRuleDescriptor`] — the body, hydrated via
//!   [`DeductiveRuleDescriptor::compile`].
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
use std::sync::Arc;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{Artifact, ArtifactSelector, Attribute, Changes, Entity, Value};
use dialog_query::DeductiveRule;
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptRules;
use dialog_query::error::EvaluationError;
use dialog_query::rule::DeductiveRuleDescriptor;
use parking_lot::RwLock;

use crate::Revision;

/// Build the `db.rule/<name>` attribute used in rule-fact selectors.
fn rule_attr(name: &str) -> Attribute {
    format!("db.rule/{name}")
        .parse()
        .expect("db.rule/<name> is a valid attribute URI")
}

/// Selector for `db.rule/conclusion is = <concept>` — finds the rule
/// entities concluding a concept.
pub(crate) fn conclusion_selector(concept: &Entity) -> ArtifactSelector<Constrained> {
    ArtifactSelector::new()
        .the(rule_attr("conclusion"))
        .is(Value::Entity(concept.clone()))
}

/// Selector for `db.rule/source of = <rule>` — fetches a rule's body.
pub(crate) fn source_selector(rule: &Entity) -> ArtifactSelector<Constrained> {
    ArtifactSelector::new()
        .the(rule_attr("source"))
        .of(rule.clone())
}

/// Hydrate a compiled [`DeductiveRule`] from a `db.rule/source` claim
/// value (the JSON [`DeductiveRuleDescriptor`]).
pub(crate) fn hydrate(source: &str) -> Result<DeductiveRule, EvaluationError> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_str(source)
        .map_err(|e| EvaluationError::Store(format!("rule source parse: {e}")))?;
    descriptor
        .compile()
        .map_err(|e| EvaluationError::Store(format!("rule compile: {e}")))
}

/// Extract the rule entities from a batch of `db.rule/conclusion`
/// artifacts — each artifact's `of` is a rule entity.
pub(crate) fn rule_entities(conclusion_claims: Vec<Artifact>) -> Vec<Entity> {
    conclusion_claims.into_iter().map(|a| a.of).collect()
}

/// Extract the source string from a `db.rule/source` artifact batch.
pub(crate) fn source_string(source_claims: Vec<Artifact>) -> Option<String> {
    source_claims.into_iter().find_map(|a| match a.is {
        Value::String(s) => Some(s),
        _ => None,
    })
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
pub(crate) fn assemble(
    concept: &ConceptDescriptor,
    rules: impl IntoIterator<Item = DeductiveRule>,
) -> ConceptRules {
    let mut concept_rules = ConceptRules::new(concept);
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

    let conclusion_attr = rule_attr("conclusion");
    let source_attr = rule_attr("source");

    // rule entities whose conclusion is `concept`, asserted in the overlay.
    let mut rule_entities: Vec<Entity> = Vec::new();
    for (entity, attribute, change) in changes.iter() {
        if *attribute == conclusion_attr
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
                && *attribute == source_attr
                && let Change::Assert(Value::String(s)) | Change::Replace(Value::String(s)) = change
                && let Ok(rule) = hydrate(s)
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
