//! Per-concept rule management with adornment-keyed plan caching.
//!
//! Each `ConceptRules` owns the *deductive* rules for a single
//! concept entity and caches execution plans keyed by adornment
//! (binding pattern). This is the per-concept counterpart to the
//! registry-level indexing in `RuleRegistry`. Inductive rules
//! ([`InductiveRule`](crate::rule::InductiveRule)) participate in
//! transactions rather than queries and will be installed via a
//! separate path in the future.

use std::iter;

use super::adornment::Adornment;
use crate::DeductiveRule;
use crate::concept::descriptor::ConceptDescriptor;
use crate::parameters::Parameters;
use crate::planner::Disjunction;
use crate::selection::Match;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// All rules for a single concept, with adornment-keyed plan caching.
///
/// Inspired by magic set optimization: each distinct binding pattern
/// (adornment) produces a specialized execution plan that exploits
/// known bindings for cheaper execution.
#[derive(Debug, Clone)]
pub struct ConceptRules {
    implicit: DeductiveRule,
    installed: Vec<DeductiveRule>,
    plans: Arc<RwLock<HashMap<Adornment, Arc<Disjunction>>>>,
}

impl ConceptRules {
    /// Create a new `ConceptRules` from a concept predicate.
    /// The predicate is used to derive the default rule.
    pub fn new(descriptor: &ConceptDescriptor) -> Self {
        Self {
            implicit: DeductiveRule::from(descriptor),
            installed: Vec::new(),
            plans: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Install a deductive rule, deduplicating by equality.
    /// Clears the plan cache when a genuinely new rule is added.
    pub fn install(&mut self, rule: DeductiveRule) {
        if !self.installed.contains(&rule) {
            self.installed.push(rule);
            self.plans.write().unwrap().clear();
        }
    }

    /// The explicitly installed rules (does not include the implicit rule).
    pub fn installed(&self) -> &[DeductiveRule] {
        &self.installed
    }

    /// Install every rule from `other` into this `ConceptRules`.
    ///
    /// Used when combining two rule sources (e.g. a primary registry and an
    /// overlay) so all installed rules contribute to planning. The implicit
    /// rule is the same in both — it is derived from the concept descriptor —
    /// so only the `installed` set is merged.
    pub fn extend(&mut self, other: &ConceptRules) {
        for rule in &other.installed {
            self.install(rule.clone());
        }
    }

    /// Get or compute a cached plan for the given binding pattern.
    pub fn plan(&self, terms: &Parameters, matched: &Match) -> Arc<Disjunction> {
        let adornment = Adornment::derive(terms, matched);

        // Fast path: read lock
        if let Some(plan) = self.plans.read().unwrap().get(&adornment) {
            return plan.clone();
        }

        // Slow path: replan all rules with inferred scope
        let scope = adornment.into_environment(terms);
        let all_rules = iter::once(&self.implicit).chain(&self.installed);
        let plan: Disjunction = all_rules.map(|rule| rule.plan(&scope)).collect();

        let fork = Arc::new(plan);
        self.plans.write().unwrap().insert(adornment, fork.clone());
        fork
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
        ConceptDescriptor::from([(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "person name",
                Cardinality::One,
                Some(Type::String),
            ),
        )])
    }

    fn alt_rule(descriptor: &ConceptDescriptor) -> DeductiveRule {
        // Distinct rule body so install does not dedup against the implicit.
        DeductiveRule::new(
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
        .expect("alt rule compiles")
    }

    #[dialog_common::test]
    fn installed_returns_empty_for_fresh_concept_rules() {
        let rules = ConceptRules::new(&person_concept());
        assert!(rules.installed().is_empty());
    }

    #[dialog_common::test]
    fn installed_lists_registered_rules_in_order() {
        let descriptor = person_concept();
        let mut rules = ConceptRules::new(&descriptor);
        let a = alt_rule(&descriptor);
        rules.install(a.clone());
        assert_eq!(rules.installed(), &[a]);
    }

    #[dialog_common::test]
    fn extend_merges_installed_rules_from_other() {
        let descriptor = person_concept();
        let mut a = ConceptRules::new(&descriptor);
        let mut b = ConceptRules::new(&descriptor);

        let rule = alt_rule(&descriptor);
        b.install(rule.clone());
        a.extend(&b);

        assert_eq!(a.installed().len(), 1);
        assert_eq!(a.installed()[0], rule);
    }

    #[dialog_common::test]
    fn extend_dedups_against_existing_installed() {
        let descriptor = person_concept();
        let rule = alt_rule(&descriptor);
        let mut a = ConceptRules::new(&descriptor);
        a.install(rule.clone());
        let mut b = ConceptRules::new(&descriptor);
        b.install(rule.clone());
        a.extend(&b);
        assert_eq!(
            a.installed().len(),
            1,
            "duplicate rule must not be added twice"
        );
    }

    #[dialog_common::test]
    fn extend_invalidates_plan_cache_when_rules_change() {
        let descriptor = person_concept();
        let mut rules = ConceptRules::new(&descriptor);

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("e"));
        terms.insert("name".into(), Term::var("n"));

        let candidate = Match::new();
        let plan_before = rules.plan(&terms, &candidate);

        let mut other = ConceptRules::new(&descriptor);
        other.install(alt_rule(&descriptor));
        rules.extend(&other);

        let plan_after = rules.plan(&terms, &candidate);
        assert!(
            !Arc::ptr_eq(&plan_before, &plan_after),
            "extend that adds a new rule must invalidate the plan cache"
        );
    }

    #[dialog_common::test]
    fn extend_keeps_plan_cache_when_nothing_new() {
        let descriptor = person_concept();
        let rules = ConceptRules::new(&descriptor);

        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("e"));
        terms.insert("name".into(), Term::var("n"));

        let candidate = Match::new();
        let plan_before = rules.plan(&terms, &candidate);

        // Extend with an empty other: no new installed rules, cache preserved.
        let empty = ConceptRules::new(&descriptor);
        let mut rules_clone = rules.clone();
        rules_clone.extend(&empty);

        // The clone shares the plan cache Arc with the original; planning the
        // same adornment hits the cache.
        let plan_after = rules_clone.plan(&terms, &candidate);
        assert!(
            Arc::ptr_eq(&plan_before, &plan_after),
            "no-op extend must not clear cached plans"
        );
    }
}
