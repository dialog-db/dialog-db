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
