use super::dependencies::{ProgramAnalysis, Violation};
use crate::Entity;
use crate::EvaluationError;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::{ConceptRules, PlanCache};
use crate::rule::deductive::DeductiveRule;
use crate::rule::statement::{Reach, head_entities};
use crate::source::SelectRules;
use dialog_capability::Provider;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::{Arc, RwLock};

/// An assembled bundle and the analysis of its dependency closure.
type Assembled = (ConceptRules, Arc<ProgramAnalysis>);

/// Thread-safe registry of *deductive* rules, keyed by the attributes
/// their heads derive. Inductive rules
/// ([`InductiveRule`](crate::rule::InductiveRule)) have a
/// different lifecycle: they participate in transactions rather
/// than queries, and will be installed via a separate path in the
/// future.
///
/// A rule derives one relation per head attribute, so it is indexed
/// once per attribute (by the attribute's `on:` entity, the same key
/// the stored `dialog.rule/derives` index uses). Acquiring the rules
/// of a concept gathers every rule whose head shares an attribute with
/// it and projects each onto the concept — see
/// [`ConceptRules::assemble`] and `notes/attribute-level-deduction.md`
/// — so a query over any set of attributes sees every derivation of
/// those attributes, whatever head the rule was written against.
///
/// Both [`Session`](super::Session) and [`QuerySession`](super::QuerySession)
/// hold a `RuleRegistry`. When a concept query needs rules, the registry
/// returns a [`ConceptRules`](crate::concept::application::ConceptRules)
/// bundle containing the default rule (derived from the concept's
/// attributes) plus the projected rules, together with a
/// per-adornment plan cache.
///
/// Cloning a registry is cheap: the underlying maps are wrapped in
/// `Arc<RwLock<…>>` so all clones share the same rule set and caches.
#[derive(Debug, Clone, Default)]
pub struct RuleRegistry {
    /// Every registered rule, under the `on:` entity of each attribute
    /// its head derives.
    by_head: Arc<RwLock<HashMap<Entity, Vec<DeductiveRule>>>>,
    /// Assembled bundles and the analysis of their dependency
    /// closure, keyed by the queried descriptor's canonical bytes
    /// (identity *and* field spelling, since a projection is named
    /// after the querying descriptor). Cleared on every install.
    bundles: Arc<RwLock<HashMap<Vec<u8>, Assembled>>>,
    /// Lazily computed program-level dependency analysis over every
    /// registered head (recursion and stratification), shared across
    /// clones and invalidated by [`register`](Self::register) /
    /// [`extend`](Self::extend).
    analysis: Arc<RwLock<Option<Arc<ProgramAnalysis>>>>,
}

fn poisoned<E: Display>(error: E) -> EvaluationError {
    EvaluationError::Store(error.to_string())
}

impl RuleRegistry {
    /// Creates an empty rule registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a deductive rule, deduplicating by identity.
    /// Invalidates every assembled bundle and the cached analysis.
    ///
    /// Registration is *unconditional* with respect to
    /// stratification: rules can be installed concurrently on
    /// multiple replicas and the merged set must converge, so
    /// whole-set properties (recursion, negation or aggregation
    /// through recursion) are checked by
    /// [`validate`](Self::validate) and at query time, never here.
    /// Only lock poisoning errors.
    pub fn register(&mut self, rule: DeductiveRule) -> Result<(), EvaluationError> {
        {
            let mut by_head = self.by_head.write().map_err(poisoned)?;
            for on in head_entities(rule.conclusion()) {
                let rules = by_head.entry(on).or_default();
                if !rules.iter().any(|known| known.same(&rule)) {
                    rules.push(rule.clone());
                }
            }
        }
        self.invalidate()
    }

    /// Every registered rule whose head shares an attribute with
    /// `descriptor`, each once, plus the names of the descriptor's
    /// fields some rule derives.
    fn candidates(
        &self,
        descriptor: &ConceptDescriptor,
    ) -> Result<(Vec<DeductiveRule>, HashSet<String>), EvaluationError> {
        let by_head = self.by_head.read().map_err(poisoned)?;
        let mut candidates: Vec<DeductiveRule> = Vec::new();
        let mut derived = HashSet::new();
        for (name, field) in descriptor.with().iter() {
            let Some(on) = Reach::of(field.descriptor().the()).on_entity() else {
                continue;
            };
            let Some(rules) = by_head.get(&on) else {
                continue;
            };
            if !rules.is_empty() {
                derived.insert(name.to_string());
            }
            for rule in rules {
                if !candidates.iter().any(|known| known.same(rule)) {
                    candidates.push(rule.clone());
                }
            }
        }
        Ok((candidates, derived))
    }

    /// The bundle for `descriptor`: the implicit rule plus every
    /// registered rule projected onto it.
    fn bundle(&self, descriptor: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let (candidates, derived) = self.candidates(descriptor)?;
        ConceptRules::assemble(
            descriptor,
            candidates,
            &|name, _| derived.contains(name),
            PlanCache::default(),
        )
        .map_err(|error| EvaluationError::Store(error.to_string()))
    }

    /// The dependency closure from `root`: the root's bundle plus the
    /// bundle of every concept reachable through concept premises.
    fn closure(
        &self,
        root: &ConceptDescriptor,
        root_bundle: &ConceptRules,
    ) -> Result<Vec<(Entity, ConceptRules)>, EvaluationError> {
        let mut entries: Vec<(Entity, ConceptRules)> = Vec::new();
        let mut seen: HashSet<Entity> = HashSet::new();
        let mut queue: Vec<ConceptDescriptor> = Vec::new();

        seen.insert(root.this());
        queue.extend(root_bundle.referenced().cloned());
        entries.push((root.this(), root_bundle.clone()));

        while let Some(descriptor) = queue.pop() {
            let entity = descriptor.this();
            if !seen.insert(entity.clone()) {
                continue;
            }
            // A concept no rule derives contributes only its structural
            // (`conforms`) edges, which the analysis adds for any
            // referenced descriptor without an entry; assembling its
            // bundle would compile an implicit rule for nothing.
            if self.candidates(&descriptor)?.0.is_empty() {
                continue;
            }
            let bundle = self.bundle(&descriptor)?;
            queue.extend(bundle.referenced().cloned());
            entries.push((entity, bundle));
        }
        Ok(entries)
    }

    /// Acquire rules for the given concept: the implicit rule plus every
    /// registered rule projected onto it, so this always returns a
    /// ConceptRules regardless of whether any rules were explicitly
    /// installed.
    ///
    /// Runs the query-time dependency check over the concept's
    /// closure first: an ill-stratified closure fails with
    /// [`EvaluationError::NegationThroughRecursion`] or
    /// [`EvaluationError::AggregationThroughRecursion`], so
    /// ill-stratified regions of the program fail exactly the
    /// queries that touch them. When the concept itself sits on a
    /// (stratified) dependency cycle, the returned rules carry the
    /// program analysis so evaluation switches to the semi-naive
    /// fixpoint.
    pub fn acquire(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let key = serde_ipld_dagcbor::to_vec(predicate)
            .map_err(|error| EvaluationError::Store(error.to_string()))?;
        let cached = self.bundles.read().map_err(poisoned)?.get(&key).cloned();
        let (rules, analysis) = match cached {
            Some(entry) => entry,
            None => {
                let rules = self.bundle(predicate)?;
                let entries = self.closure(predicate, &rules)?;
                let analysis = Arc::new(ProgramAnalysis::analyze(
                    entries.iter().map(|(entity, bundle)| (entity, bundle)),
                ));
                self.bundles
                    .write()
                    .map_err(poisoned)?
                    .insert(key, (rules.clone(), analysis.clone()));
                (rules, analysis)
            }
        };
        analysis.check(predicate)?;
        let entity = predicate.this();
        Ok(if analysis.is_recursive(&entity) {
            rules.with_recursion(analysis)
        } else {
            rules
        })
    }

    /// Merge every registered rule from `other` into this registry.
    ///
    /// Like [`register`](Self::register), merging is unconditional:
    /// the merged set may be ill-stratified, which
    /// [`validate`](Self::validate) reports and queries surface.
    pub fn extend(&mut self, other: &RuleRegistry) -> Result<(), EvaluationError> {
        let rules: Vec<DeductiveRule> = other
            .by_head
            .read()
            .map_err(poisoned)?
            .values()
            .flatten()
            .cloned()
            .collect();
        for rule in rules {
            self.register(rule)?;
        }
        Ok(())
    }

    /// The current program analysis snapshot, computing it if the
    /// rule set changed since the last one: the dependency closure
    /// from every registered rule's head.
    pub fn analysis(&self) -> Result<Arc<ProgramAnalysis>, EvaluationError> {
        if let Some(analysis) = self.analysis.read().map_err(poisoned)?.as_ref() {
            return Ok(analysis.clone());
        }
        let heads: Vec<ConceptDescriptor> = {
            let by_head = self.by_head.read().map_err(poisoned)?;
            let mut heads: Vec<ConceptDescriptor> = Vec::new();
            for rule in by_head.values().flatten() {
                if !heads.contains(rule.conclusion()) {
                    heads.push(rule.conclusion().clone());
                }
            }
            heads
        };
        let mut entries: Vec<(Entity, ConceptRules)> = Vec::new();
        let mut seen: HashSet<Entity> = HashSet::new();
        for head in heads {
            let bundle = self.bundle(&head)?;
            for (entity, bundle) in self.closure(&head, &bundle)? {
                if seen.insert(entity.clone()) {
                    entries.push((entity, bundle));
                }
            }
        }
        let analysis = Arc::new(ProgramAnalysis::analyze(
            entries.iter().map(|(entity, bundle)| (entity, bundle)),
        ));
        *self.analysis.write().map_err(poisoned)? = Some(analysis.clone());
        Ok(analysis)
    }

    /// Every stratification violation in the current rule set.
    /// Callers decide what to do: surface as a warning after an
    /// install, refuse to proceed after a merge, or ignore and let
    /// queries fail individually.
    pub fn validate(&self) -> Result<Vec<Violation>, EvaluationError> {
        Ok(self.analysis()?.violations().to_vec())
    }

    /// Whether the concept participates in a dependency cycle in
    /// the current rule set.
    pub fn is_recursive(&self, concept: &Entity) -> Result<bool, EvaluationError> {
        Ok(self.analysis()?.is_recursive(concept))
    }

    fn invalidate(&self) -> Result<(), EvaluationError> {
        *self.analysis.write().map_err(poisoned)? = None;
        self.bundles.write().map_err(poisoned)?.clear();
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<SelectRules> for RuleRegistry {
    async fn execute(&self, input: ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        self.acquire(&input)
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Term;
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::the;

    fn person_concept() -> ConceptDescriptor {
        ConceptDescriptor::try_from([(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
        .unwrap()
    }

    #[dialog_common::test]
    async fn it_returns_implicit_rules_for_an_unseen_concept() {
        let registry = RuleRegistry::new();
        let descriptor = person_concept();
        let rules = Provider::<SelectRules>::execute(&registry, descriptor)
            .await
            .expect("acquire should succeed");
        assert!(
            rules.installed().is_empty(),
            "no rules installed, only implicit"
        );
    }

    #[dialog_common::test]
    async fn it_surfaces_a_registered_rule_through_the_provider() {
        let mut registry = RuleRegistry::new();
        let descriptor = person_concept();
        let rule = DeductiveRule::from(&descriptor);
        registry.register(rule.clone()).unwrap();

        let rules = Provider::<SelectRules>::execute(&registry, descriptor)
            .await
            .expect("acquire");
        assert_eq!(rules.installed().len(), 1);
        assert_eq!(rules.installed()[0], rule);
    }

    #[dialog_common::test]
    async fn it_copies_entries_for_unseen_concepts_on_extend() {
        let descriptor = person_concept();
        let rule = DeductiveRule::from(&descriptor);
        let mut src = RuleRegistry::new();
        src.register(rule.clone()).unwrap();

        let mut dst = RuleRegistry::new();
        dst.extend(&src).unwrap();
        assert_eq!(dst.acquire(&descriptor).unwrap().installed()[0], rule);
    }

    #[dialog_common::test]
    async fn it_merges_installed_rules_for_a_shared_concept_on_extend() {
        // Two registries with different rules for the same concept; extend
        // should produce a registry where both rules are installed.
        let descriptor = person_concept();
        let rule_a = DeductiveRule::from(&descriptor);
        // Same conclusion, body uses `None` cardinality (`All` variant)
        // instead of the implicit `One`, produces a distinct rule.
        let rule_b = DeductiveRule::new(
            descriptor.clone(),
            vec![
                AttributeQuery::new(
                    Term::from(the!("person/name")),
                    Term::var("this"),
                    Term::var("name"),
                    Term::blank(),
                    None,
                )
                .into(),
            ],
        )
        .expect("rule_b is valid");
        assert_ne!(rule_a, rule_b);

        let mut a = RuleRegistry::new();
        a.register(rule_a.clone()).unwrap();
        let mut b = RuleRegistry::new();
        b.register(rule_b.clone()).unwrap();

        a.extend(&b).unwrap();
        let merged = a.acquire(&descriptor).unwrap();
        assert_eq!(merged.installed().len(), 2);
        assert!(merged.installed().contains(&rule_a));
        assert!(merged.installed().contains(&rule_b));
    }
}
