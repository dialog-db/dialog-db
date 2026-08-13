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
use super::fixpoint::Continuation;
use super::plan_cache::PlanCache;
use crate::DeductiveRule;
use crate::concept::descriptor::ConceptDescriptor;
use crate::parameters::Parameters;
use crate::planner::Disjunction;
use crate::selection::Match;
use crate::session::ProgramAnalysis;
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
    /// Cross-query cache of planned per-rule [`Conjunction`]s, keyed by
    /// content-addressed `(rule, adornment)`. Shared from the owning
    /// branch so a re-assembled `ConceptRules` (the layered case) reuses
    /// plans the previous query computed. A standalone instance gets a
    /// private one via [`PlanCache::default`].
    plan_cache: PlanCache,
    /// Present when this concept participates in a dependency
    /// cycle: the program analysis the fixpoint evaluator needs to
    /// tell in-component premises from base ones. Attached by
    /// [`RuleRegistry::acquire`](crate::session::RuleRegistry) and
    /// read by `ConceptQuery::evaluate`.
    recursion: Option<Arc<ProgramAnalysis>>,
    /// A standing subscription's retained fixpoint, when one is
    /// polling this concept: evaluation continues the retained
    /// answer table (or rebuilds into it) instead of computing a
    /// throwaway fixpoint.
    continuation: Option<Continuation>,
}

impl ConceptRules {
    /// Create a new `ConceptRules` from a concept predicate, with a
    /// private (unshared) plan cache. The predicate is used to derive the
    /// default rule.
    pub fn new(descriptor: &ConceptDescriptor) -> Self {
        Self::with_plan_cache(descriptor, PlanCache::default())
    }

    /// Create a new `ConceptRules` sharing `plan_cache` with its owner.
    ///
    /// The repository assembles a fresh `ConceptRules` per query from its
    /// layers; passing the branch's cache here lets each assembly reuse
    /// plans the previous one computed (see [`PlanCache`]).
    pub fn with_plan_cache(descriptor: &ConceptDescriptor, plan_cache: PlanCache) -> Self {
        Self {
            implicit: DeductiveRule::from(descriptor),
            installed: Vec::new(),
            plans: Arc::new(RwLock::new(HashMap::new())),
            plan_cache,
            recursion: None,
            continuation: None,
        }
    }

    /// Attach the program analysis marking this concept recursive.
    /// Evaluation switches to the semi-naive fixpoint when present.
    pub fn with_recursion(mut self, analysis: Arc<ProgramAnalysis>) -> Self {
        self.recursion = Some(analysis);
        self
    }

    /// The program analysis attached when this concept participates
    /// in a dependency cycle; `None` for ordinary concepts.
    pub fn recursion(&self) -> Option<&Arc<ProgramAnalysis>> {
        self.recursion.as_ref()
    }

    /// Attach a standing subscription's retained fixpoint. Only
    /// consulted when the concept is recursive.
    pub fn with_continuation(mut self, continuation: Continuation) -> Self {
        self.continuation = Some(continuation);
        self
    }

    /// The retained fixpoint attached by a polling subscription, if
    /// any.
    pub fn continuation(&self) -> Option<&Continuation> {
        self.continuation.as_ref()
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

    /// Every rule for this concept: the implicit rule (derived from
    /// the descriptor) followed by the installed ones. This is the
    /// rule set the program-level dependency analysis walks.
    pub fn rules(&self) -> impl Iterator<Item = &DeductiveRule> {
        iter::once(&self.implicit).chain(self.installed.iter())
    }

    /// Install every rule from `other` into this `ConceptRules`.
    ///
    /// Used when combining two rule sources (e.g. a primary registry and an
    /// overlay) so all installed rules contribute to planning. The implicit
    /// rule is the same in both (it is derived from the concept descriptor)
    /// so only the `installed` set is merged.
    pub fn extend(&mut self, other: &ConceptRules) {
        for rule in &other.installed {
            self.install(rule.clone());
        }
    }

    /// Get or compute a cached plan for the given binding pattern.
    ///
    /// Each rule is planned through the shared, branch-owned
    /// [`PlanCache`], keyed by `(rule identity,
    /// adornment)`. Planning a rule is a pure function of its body and
    /// the adornment, so this memoizes across *every* query that uses
    /// the rule — including ones that re-assembled this concept's rule
    /// set from scratch (the layered-resolution case, where a
    /// per-instance cache would never be reused). The per-instance
    /// `plans` map below keys the assembled [`Disjunction`] by
    /// adornment, so a repeated identical call on the *same*
    /// `ConceptRules` skips even the (cheap) re-assembly.
    pub fn plan(&self, terms: &Parameters, matched: &Match) -> Arc<Disjunction> {
        let adornment = Adornment::derive(terms, matched);

        if let Some(plan) = self.plans.read().unwrap().get(&adornment) {
            return plan.clone();
        }

        let scope = adornment.into_environment(terms);
        // The implicit rule is planned directly: its body is raw
        // attribute queries that have no serializable (content-addressed)
        // identity, so it can't key the global cache. It's also a pure
        // function of this concept's descriptor and cheap to plan. Only
        // *installed* rules — which have concept/formula bodies and thus
        // a `this()` — are memoized globally by `(rule, adornment)`.
        let plan: Disjunction = iter::once(self.implicit.plan(&scope))
            .chain(self.installed.iter().map(|rule| {
                self.plan_cache
                    .get_or_plan(rule, adornment, || rule.plan(&scope))
            }))
            .collect();

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
    use crate::attribute::query::AttributeQuery;
    use crate::attribute::{AttributeDescriptor, Cardinality, Type};
    use crate::the;
    use crate::{Term, Value};

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
    fn it_returns_empty_installed_list_for_fresh_concept_rules() {
        let rules = ConceptRules::new(&person_concept());
        assert!(rules.installed().is_empty());
    }

    #[dialog_common::test]
    fn it_lists_registered_rules_in_install_order() {
        let descriptor = person_concept();
        let mut rules = ConceptRules::new(&descriptor);
        let a = alt_rule(&descriptor);
        rules.install(a.clone());
        assert_eq!(rules.installed(), &[a]);
    }

    #[dialog_common::test]
    fn it_merges_installed_rules_from_another_registry() {
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
    fn it_dedups_extended_rules_against_existing_installed() {
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
    fn it_invalidates_plan_cache_when_extend_adds_rules() {
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
    fn it_keeps_plan_cache_when_extend_adds_nothing() {
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

    /// Soundness probe for a GLOBAL per-(rule, adornment) plan cache:
    /// does `DeductiveRule::plan` depend only on the binding *pattern*
    /// (the adornment), or also on the caller's variable *names*?
    ///
    /// `Adornment::into_environment` binds the caller's term names into
    /// the scope, so this is not obvious. If the two `Conjunction`s
    /// below are equal, `(rule, adornment)` is a sound cache key. If
    /// not, the key must also capture the name mapping (or the scope
    /// must be normalized to slot indices first).
    #[dialog_common::test]
    fn it_plans_independently_of_caller_variable_names() {
        let descriptor = person_concept();
        let rule = alt_rule(&descriptor);

        // Two term maps: same slots bound (this, name both variables →
        // both "free" in an empty match → same adornment), but different
        // caller variable names.
        let mut terms_a = Parameters::new();
        terms_a.insert("this".into(), Term::var("e1"));
        terms_a.insert("name".into(), Term::var("n1"));

        let mut terms_b = Parameters::new();
        terms_b.insert("this".into(), Term::var("e2"));
        terms_b.insert("name".into(), Term::var("n2"));

        let matched = Match::new();
        let adorn_a = Adornment::derive(&terms_a, &matched);
        let adorn_b = Adornment::derive(&terms_b, &matched);
        assert_eq!(adorn_a, adorn_b, "same binding pattern ⇒ same adornment");

        let plan_a = rule.plan(&adorn_a.into_environment(&terms_a));
        let plan_b = rule.plan(&adorn_b.into_environment(&terms_b));

        assert_eq!(
            plan_a, plan_b,
            "rule plan must depend only on the adornment, not caller var names — \
             else (rule, adornment) is an unsound global cache key"
        );

        // Now bind `name` (cardinality-one) in one and not the other:
        // different adornment ⇒ plans may legitimately differ. This just
        // confirms the adornment actually distinguishes binding patterns.
        let mut bound = Match::new();
        bound
            .bind(&Term::var("n1"), Value::from("x".to_string()))
            .unwrap();
        let adorn_bound = Adornment::derive(&terms_a, &bound);
        assert_ne!(
            adorn_a, adorn_bound,
            "binding a slot must change the adornment"
        );
    }
}
