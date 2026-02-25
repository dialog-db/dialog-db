//! Per-concept rule management with adornment-keyed plan caching.
//!
//! Each `ConceptRules` owns the rules for a single concept entity and caches
//! execution plans keyed by adornment (binding pattern). This is the per-concept
//! counterpart to the registry-level indexing in `RuleRegistry`.

use crate::DeductiveRule;
use crate::parameters::Parameters;
use crate::planner::{Fork, Join};
use crate::predicate::ConceptPredicate;
use crate::proposition::concept::adornment::Adornment;
use crate::selection::Answer;
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
    plans: Arc<RwLock<HashMap<Adornment, Arc<Fork>>>>,
}

impl ConceptRules {
    /// Create a new `ConceptRules` from a concept predicate.
    /// The predicate is used to derive the default rule.
    pub fn new(predicate: &ConceptPredicate) -> Self {
        Self {
            implicit: DeductiveRule::from(predicate),
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

    /// Get or compute a cached plan for the given binding pattern.
    pub fn plan(&self, terms: &Parameters, answer: &Answer) -> Arc<Fork> {
        let adornment = Adornment::derive(terms, answer);

        // Fast path: read lock
        if let Some(plan) = self.plans.read().unwrap().get(&adornment) {
            return plan.clone();
        }

        // Slow path: replan all rules with inferred scope
        let scope = adornment.into_environment(terms);
        let all_rules = std::iter::once(&self.implicit).chain(&self.installed);
        let fork = all_rules
            .map(|rule| Join::from(&rule.premises))
            .map(|join| join.plan(&scope).unwrap_or(join))
            .fold(Fork::new(), |fork, join| fork.or(join));

        let fork = Arc::new(fork);
        self.plans.write().unwrap().insert(adornment, fork.clone());
        fork
    }
}
