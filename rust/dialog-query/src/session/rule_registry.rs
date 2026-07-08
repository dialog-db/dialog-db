use super::dependencies::{NegationViolation, ProgramAnalysis};
use crate::Entity;
use crate::EvaluationError;
use crate::concept::descriptor::ConceptDescriptor;
use crate::concept::query::ConceptRules;
use crate::rule::deductive::DeductiveRule;
use crate::source::SelectRules;
use dialog_capability::Provider;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Thread-safe registry of *deductive* rules, keyed by the
/// conclusion entity. Inductive rules
/// ([`InductiveRule`](crate::rule::InductiveRule)) have a
/// different lifecycle: they participate in transactions rather
/// than queries, and will be installed via a separate path in the
/// future.
///
/// Both [`Session`](super::Session) and [`QuerySession`](super::QuerySession)
/// hold a `RuleRegistry`. When a concept query needs rules, the registry
/// returns a [`ConceptRules`](crate::concept::application::ConceptRules)
/// bundle containing the default rule (derived from the concept's
/// attributes) plus any explicitly installed rules, together with a
/// per-adornment plan cache.
///
/// Cloning a registry is cheap: the underlying `HashMap` is wrapped in
/// `Arc<RwLock<…>>` so all clones share the same rule set and caches.
#[derive(Debug, Clone, Default)]
pub struct RuleRegistry {
    rules: Arc<RwLock<HashMap<Entity, ConceptRules>>>,
    /// Lazily computed program-level dependency analysis (recursion
    /// and stratification), shared across clones and invalidated by
    /// [`register`](Self::register) / [`extend`](Self::extend).
    analysis: Arc<RwLock<Option<Arc<ProgramAnalysis>>>>,
}

impl RuleRegistry {
    /// Creates an empty rule registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a deductive rule, deduplicating by equality.
    /// Invalidates cached plans for the affected concept entity.
    ///
    /// Registration is *unconditional* with respect to
    /// stratification: rules can be installed concurrently on
    /// multiple replicas and the merged set must converge, so
    /// whole-set properties (recursion, negation through recursion)
    /// are checked by [`validate`](Self::validate) and at query
    /// time, never here. Only lock poisoning errors.
    pub fn register(&mut self, rule: DeductiveRule) -> Result<(), EvaluationError> {
        let entity = rule.conclusion().this();
        self.rules
            .write()
            .map_err(|e| EvaluationError::Store(e.to_string()))?
            .entry(entity)
            .or_insert_with(|| ConceptRules::new(rule.conclusion()))
            .install(rule);
        self.invalidate_analysis()?;
        Ok(())
    }

    /// Acquire rules for the given concept. Creates the default rule from
    /// the predicate's attributes on first access, so this always returns
    /// a ConceptRules regardless of whether any rules were explicitly installed.
    ///
    /// Runs the query-time dependency check over the concept's
    /// closure first: an ill-stratified closure fails with
    /// [`EvaluationError::NegationThroughRecursion`], so
    /// ill-stratified regions of the program fail exactly the
    /// queries that touch them. When the concept itself sits on a
    /// (stratified) dependency cycle, the returned rules carry the
    /// program analysis so evaluation switches to the semi-naive
    /// fixpoint.
    pub fn acquire(&self, predicate: &ConceptDescriptor) -> Result<ConceptRules, EvaluationError> {
        let analysis = self.analysis()?;
        analysis.check(predicate)?;
        let entity = predicate.this();
        let rules = self
            .rules
            .write()
            .map_err(|e| EvaluationError::Store(e.to_string()))?
            .entry(entity.clone())
            .or_insert_with(|| ConceptRules::new(predicate))
            .clone();
        Ok(if analysis.is_recursive(&entity) {
            rules.with_recursion(analysis)
        } else {
            rules
        })
    }

    /// Merge every per-concept rule set from `other` into this registry.
    ///
    /// Entries that exist on both sides are folded together via
    /// [`ConceptRules::extend`] so installed rules from both contribute.
    /// Like [`register`](Self::register), merging is unconditional:
    /// the merged set may be ill-stratified, which
    /// [`validate`](Self::validate) reports and queries surface.
    pub fn extend(&mut self, other: &RuleRegistry) -> Result<(), EvaluationError> {
        let other_rules = other
            .rules
            .read()
            .map_err(|e| EvaluationError::Store(e.to_string()))?;
        let mut self_rules = self
            .rules
            .write()
            .map_err(|e| EvaluationError::Store(e.to_string()))?;
        for (entity, rules) in other_rules.iter() {
            self_rules
                .entry(entity.clone())
                .and_modify(|existing| existing.extend(rules))
                .or_insert_with(|| rules.clone());
        }
        drop(self_rules);
        self.invalidate_analysis()?;
        Ok(())
    }

    /// The current program analysis snapshot, computing it if the
    /// rule set changed since the last one.
    pub fn analysis(&self) -> Result<Arc<ProgramAnalysis>, EvaluationError> {
        if let Some(analysis) = self
            .analysis
            .read()
            .map_err(|e| EvaluationError::Store(e.to_string()))?
            .as_ref()
        {
            return Ok(analysis.clone());
        }
        let rules = self
            .rules
            .read()
            .map_err(|e| EvaluationError::Store(e.to_string()))?;
        let analysis = Arc::new(ProgramAnalysis::analyze(rules.iter()));
        drop(rules);
        *self
            .analysis
            .write()
            .map_err(|e| EvaluationError::Store(e.to_string()))? = Some(analysis.clone());
        Ok(analysis)
    }

    /// Every stratification violation in the current rule set.
    /// Callers decide what to do: surface as a warning after an
    /// install, refuse to proceed after a merge, or ignore and let
    /// queries fail individually.
    pub fn validate(&self) -> Result<Vec<NegationViolation>, EvaluationError> {
        Ok(self.analysis()?.violations().to_vec())
    }

    /// Whether the concept participates in a dependency cycle in
    /// the current rule set.
    pub fn is_recursive(&self, concept: &Entity) -> Result<bool, EvaluationError> {
        Ok(self.analysis()?.is_recursive(concept))
    }

    fn invalidate_analysis(&self) -> Result<(), EvaluationError> {
        *self
            .analysis
            .write()
            .map_err(|e| EvaluationError::Store(e.to_string()))? = None;
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
