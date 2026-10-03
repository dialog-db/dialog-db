//! Affected-entity discovery: the delta-join step of incremental
//! subscription maintenance.
//!
//! Given a set of changed base facts and a subscribed concept, which
//! head entities can the changes affect? For an *entity-local* rule
//! (every premise reads `of ?this`) the answer is just the changed
//! facts' subjects. For a non-local rule — one whose body reads
//! other entities' facts through a concept premise (a concept-typed
//! field's conformance check, a variant's negation) — the answer is
//! discovered by a *delta-join*: bind the changed fact into the
//! premise it can match, evaluate the rule's remaining premises
//! sideways with those bindings, and project the head variable.
//! Each affected head is then re-derived goal-directedly by the
//! caller (DRed's delete/re-derive, per entity).
//!
//! The discovery over-approximates but never misses: a fact bound
//! into a premise it merely *could* match yields candidate heads,
//! and re-derivation settles the truth per head. `None` means the
//! discovery could not bound the affected set (recursion, a rule
//! shape it does not handle, an unplannable sideways join) and the
//! caller must fall back to full re-evaluation, which is always
//! sound.

use std::collections::{BTreeSet, HashMap};

use dialog_capability::Provider;
use futures_util::TryStreamExt as _;

use crate::artifact::Artifact;
use crate::attribute::The;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptQuery;
use crate::error::EvaluationError;
use crate::negation::Negation;
use crate::planner::Planner;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::rule::deductive::DeductiveRule;
use crate::selection::{Binding, Match};
use crate::source::SelectRules;
use crate::term::Term;
use crate::types::Any;
use crate::{Entity, Environment, Value};

/// The head entities of `concept` that the changed facts can
/// affect, or `None` when the set cannot be bounded and the caller
/// must re-evaluate in full.
///
/// Nested concept premises are followed to arbitrary depth: the
/// reachable concepts form an acyclic graph (recursion anywhere in
/// it returns `None`), each is resolved once, and affected sets are
/// computed bottom-up — a target's affected heads feed the premises
/// that apply it, so a change can propagate through any number of
/// derivation layers.
pub async fn affected_entities<'a, Env>(
    concept: &ConceptDescriptor,
    facts: &[Artifact],
    env: &'a Env,
) -> Result<Option<BTreeSet<Entity>>, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    let root = concept.this();

    // Collect the reachable concepts and their resolved rules.
    let mut rules_of: HashMap<Entity, Vec<DeductiveRule>> = HashMap::new();
    let mut targets_of: HashMap<Entity, BTreeSet<Entity>> = HashMap::new();
    let mut queue = vec![concept.clone()];
    while let Some(descriptor) = queue.pop() {
        let entity = descriptor.this();
        if rules_of.contains_key(&entity) {
            continue;
        }
        let bundle = Provider::<SelectRules>::execute(env, descriptor).await?;
        if bundle.recursion().is_some() {
            return Ok(None);
        }
        // A reducing rule's fold spans entities: one changed fact
        // moves its whole group's row, and a retraction can make a
        // group's row disappear without any change to the head
        // entity's own facts. Per-entity re-derivation cannot bound
        // that, so the caller recomputes (A3's recompute-per-poll
        // model; incremental aggregate maintenance is A5).
        if bundle.rules().any(|rule| !rule.reduce().is_empty()) {
            return Ok(None);
        }
        let rules: Vec<DeductiveRule> = bundle.rules().cloned().collect();
        let mut targets = BTreeSet::new();
        for rule in &rules {
            for premise in rule.analysis().premises() {
                if let Some(query) = concept_premise(premise) {
                    targets.insert(query.predicate.this());
                    queue.push(query.predicate.clone());
                }
            }
        }
        targets_of.insert(entity.clone(), targets);
        rules_of.insert(entity, rules);
    }

    // Bottom-up: a concept computes once every target it applies
    // has. The graph is acyclic (recursion returned above), so this
    // always makes progress.
    let mut affected: HashMap<Entity, BTreeSet<Entity>> = HashMap::new();
    let mut pending: Vec<Entity> = rules_of.keys().cloned().collect();
    while !pending.is_empty() {
        let ready: Vec<Entity> = pending
            .iter()
            .filter(|entity| {
                targets_of[*entity]
                    .iter()
                    .all(|target| affected.contains_key(target))
            })
            .cloned()
            .collect();
        if ready.is_empty() {
            // Unreachable for an acyclic graph; bail rather than spin.
            return Ok(None);
        }
        for entity in &ready {
            let mut heads = BTreeSet::new();
            for rule in &rules_of[entity] {
                if rule.analysis().is_entity_local() {
                    // A change to E's facts only affects E's rows, and
                    // only under an attribute the rule reads: a change
                    // under one it never reads leaves its rows for E as
                    // they were. Every target an entity-local rule has
                    // is a concept selecting stored attributes, so this
                    // is what keeps a change to one concept's facts
                    // from being joined through every other.
                    heads.extend(
                        facts
                            .iter()
                            .filter(|fact| reads_attribute(rule, fact))
                            .map(|fact| fact.of.clone()),
                    );
                    continue;
                }
                match rule_heads(rule, facts, &affected, env).await? {
                    Some(found) => heads.extend(found),
                    None => return Ok(None),
                }
            }
            affected.insert(entity.clone(), heads);
        }
        pending.retain(|entity| !ready.contains(entity));
    }

    Ok(affected.remove(&root))
}

/// Whether some attribute premise of `rule` reads the attribute of
/// `fact`: one naming it, or one over a variable attribute, which
/// reads any.
fn reads_attribute(rule: &DeductiveRule, fact: &Artifact) -> bool {
    let attribute = Value::from(The::from(fact.the.clone()));
    rule.analysis().premises().any(|premise| {
        let the = match premise {
            Premise::Assert(Proposition::Attribute(query)) => query.the(),
            Premise::Assert(Proposition::OptionalAttribute(query)) => query.query().the(),
            Premise::Unless(Negation(Proposition::Attribute(query))) => query.the(),
            _ => return false,
        };
        the.as_constant().is_none_or(|value| *value == attribute)
    })
}

/// The concept application of a premise, positive or negated.
fn concept_premise(premise: &Premise) -> Option<&ConceptQuery> {
    match premise {
        Premise::Assert(Proposition::Concept(query)) => Some(query),
        Premise::Unless(Negation(Proposition::Concept(query))) => Some(query),
        _ => None,
    }
}

/// The candidate head entities one non-local rule can produce or
/// retract given the changed facts: per premise, bind what the
/// change can match and join the remaining premises sideways.
/// Concept premises bind their `this` slot from the target's
/// already-computed affected heads.
async fn rule_heads<'a, Env>(
    rule: &DeductiveRule,
    facts: &[Artifact],
    affected: &HashMap<Entity, BTreeSet<Entity>>,
    env: &'a Env,
) -> Result<Option<BTreeSet<Entity>>, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    let premises = &rule.analysis().premises;
    let mut heads = BTreeSet::new();

    for (index, premise) in premises.iter().enumerate() {
        // Attribute premises (positive, optional, or negated) are
        // matched directly by the changed facts.
        let attribute_terms = match premise {
            Premise::Assert(Proposition::Attribute(query)) => {
                Some((query.the().clone(), query.of().clone(), query.is().clone()))
            }
            Premise::Assert(Proposition::OptionalAttribute(query)) => Some((
                query.query().the().clone(),
                query.of().clone(),
                query.is().clone(),
            )),
            Premise::Unless(Negation(Proposition::Attribute(query))) => {
                Some((query.the().clone(), query.of().clone(), query.is().clone()))
            }
            _ => None,
        };
        if let Some((the, of, is)) = attribute_terms {
            for fact in facts {
                let mut matched = Match::new();
                let mut scope = Environment::new();
                if !bind_slot(
                    &mut matched,
                    &mut scope,
                    the.as_constant(),
                    the.name(),
                    Value::from(The::from(fact.the.clone())),
                ) {
                    continue;
                }
                if !bind_slot(
                    &mut matched,
                    &mut scope,
                    of.as_constant(),
                    of.name(),
                    Value::Entity(fact.of.clone()),
                ) {
                    continue;
                }
                if !bind_slot(
                    &mut matched,
                    &mut scope,
                    is.as_constant(),
                    is.name(),
                    fact.is.clone(),
                ) {
                    continue;
                }
                match sideways_heads(premises, index, matched, &scope, rule, env).await? {
                    Some(found) => heads.extend(found),
                    None => return Ok(None),
                }
            }
            continue;
        }

        // Concept premises (positive or negated): the change's
        // effect on the target is its already-computed affected
        // set; each affected target head, bound into the premise's
        // `this` slot, joins sideways to candidate heads.
        if let Some(query) = concept_premise(premise) {
            let Some(target_heads) = affected.get(&query.predicate.this()) else {
                return Ok(None);
            };
            let Some(this) = query.terms.get("this") else {
                return Ok(None);
            };
            for entity in target_heads {
                let mut matched = Match::new();
                let mut scope = Environment::new();
                if !bind_slot(
                    &mut matched,
                    &mut scope,
                    this.as_constant(),
                    this.name(),
                    Value::Entity(entity.clone()),
                ) {
                    continue;
                }
                match sideways_heads(premises, index, matched, &scope, rule, env).await? {
                    Some(found) => heads.extend(found),
                    None => return Ok(None),
                }
            }
        }
    }

    Ok(Some(heads))
}

/// Bind one premise slot from a changed value. A constant slot
/// filters (the fact must match it); a named variable binds; a
/// blank slot matches anything. Returns `false` when the fact
/// cannot match the slot.
fn bind_slot(
    matched: &mut Match,
    scope: &mut Environment,
    constant: Option<&Value>,
    name: Option<&str>,
    value: Value,
) -> bool {
    if let Some(expected) = constant {
        return *expected == value;
    }
    match name {
        Some(name) => {
            if matched.bind(&Term::<Any>::var(name), value).is_err() {
                return false;
            }
            scope.add(name);
            true
        }
        None => true,
    }
}

/// Join the rule's remaining premises against the bound match and
/// project the head (`?this`) of every result. `None` when the
/// sideways join cannot be planned from these bindings or a result
/// leaves the head unbound — the caller falls back.
async fn sideways_heads<'a, Env>(
    premises: &[Premise],
    source: usize,
    matched: Match,
    scope: &Environment,
    rule: &DeductiveRule,
    env: &'a Env,
) -> Result<Option<BTreeSet<Entity>>, EvaluationError>
where
    Env: crate::Scope<'a>,
{
    // The head may already be bound by the source premise itself.
    if let Ok(Binding::Present(Value::Entity(entity))) = matched.lookup(&Term::<Any>::var("this")) {
        return Ok(Some(BTreeSet::from([entity])));
    }

    let rest: Vec<Premise> = premises
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != source)
        .map(|(_, premise)| premise.clone())
        .collect();
    if rest.is_empty() {
        // Only the source premise, and it does not bind the head:
        // nothing to join through.
        return Ok(None);
    }

    let Ok(plan) = Planner::with_types(rest, rule.analysis().types.clone()).plan(scope) else {
        return Ok(None);
    };

    let mut heads = BTreeSet::new();
    let results: Vec<Match> = plan.evaluate(matched.seed(), env).try_collect().await?;
    for result in results {
        match result.lookup(&Term::<Any>::var("this")) {
            Ok(Binding::Present(Value::Entity(entity))) => {
                heads.insert(entity);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(heads))
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::parameters::Parameters;
    use crate::session::RuleRegistry;
    use crate::source::test::TestEnv;
    use crate::the;
    use dialog_peer::helpers::{test_repo, test_session_with_peer};

    fn concept(field: &str, the: &str, kind: Type) -> ConceptDescriptor {
        ConceptDescriptor::try_from(vec![(
            field,
            AttributeDescriptor::new(the.parse().expect("an attribute"), "", Cardinality::One, Some(kind)),
        )])
        .expect("a well-formed concept")
    }

    fn premise(concept: &ConceptDescriptor, terms: &[(&str, &str)]) -> Premise {
        let mut parameters = Parameters::new();
        for (field, variable) in terms {
            parameters.insert(field.to_string(), Term::<Any>::var(*variable));
        }
        Premise::Assert(Proposition::Concept(ConceptQuery {
            terms: parameters,
            predicate: concept.clone(),
        }))
    }

    /// A change under an attribute no rule reads affects nothing: the
    /// tag's colour is not what the label is made of, so the document
    /// linking to the tag is not re-derived. A change to the tag
    /// itself is.
    #[dialog_common::test]
    async fn it_ignores_a_change_under_an_attribute_the_target_never_reads() -> anyhow::Result<()> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let doc = Entity::new()?;
        let tag = Entity::new()?;
        branch
            .transaction()
            .assert(the!("doc/link").of(doc.clone()).is(tag.clone()))
            .assert(the!("doc/tag").of(tag.clone()).is("x".to_string()))
            .assert(the!("doc/color").of(tag.clone()).is("red".to_string()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let link = concept("to", "doc/link", Type::Entity);
        let tagged = concept("tag", "doc/tag", Type::String);
        let labelled = concept("label", "doc/label", Type::String);
        let rule = DeductiveRule::new(
            labelled.clone(),
            vec![
                premise(&link, &[("this", "this"), ("to", "tag")]),
                premise(&tagged, &[("this", "tag"), ("tag", "label")]),
            ],
        )?;
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let env = TestEnv::new(&branch, &operator, registry);

        let recoloured = Artifact {
            the: "doc/color".parse()?,
            of: tag.clone(),
            is: Value::String("blue".to_string()),
            cause: None,
        };
        assert_eq!(
            affected_entities(&labelled, &[recoloured], &env).await?,
            Some(BTreeSet::new()),
            "a colour change reaches no label"
        );

        let retagged = Artifact {
            the: "doc/tag".parse()?,
            of: tag.clone(),
            is: Value::String("y".to_string()),
            cause: None,
        };
        assert_eq!(
            affected_entities(&labelled, &[retagged], &env).await?,
            Some(BTreeSet::from([doc])),
            "a tag change reaches the document linking to it"
        );
        Ok(())
    }
}
