//! Join operations for combining multiple selections
//!
//! This module implements join operations needed for rule evaluation,
//! particularly for joining multiple fact selector results on shared variables.

use crate::artifact::{ArtifactStore, Value};
use crate::plan::{EvaluationContext, EvaluationPlan, Plan};
use crate::selection::{Match, Selection};
use crate::QueryError;
use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A join operation that combines two selections on shared variables
///
/// This implements the basic join operation needed for rule evaluation.
/// It takes two selections and produces results where the shared variables
/// have consistent bindings.
pub struct Join<L: Selection, R: Selection> {
    left: L,
    right: R,
    join_variables: Vec<String>,
    left_results: Vec<Match>,
    right_results: Vec<Match>,
    current_idx: usize,
    left_exhausted: bool,
    right_exhausted: bool,
}

impl<L: Selection, R: Selection> Join<L, R> {
    /// Create a new join operation
    pub fn new(left: L, right: R, join_variables: Vec<String>) -> Self {
        Self {
            left,
            right,
            join_variables,
            left_results: Vec::new(),
            right_results: Vec::new(),
            current_idx: 0,
            left_exhausted: false,
            right_exhausted: false,
        }
    }

    /// Check if two match frames are compatible on the join variables
    fn matches_compatible(left: &Match, right: &Match, join_vars: &[String]) -> bool {
        for var in join_vars {
            // For now, we'll use a simple approach - check if both have the variable
            // and if they do, they must be equal. In a full implementation, we'd
            // need proper unification logic.
            if left.has(&crate::term::Term::<Value>::var(var))
                && right.has(&crate::term::Term::<Value>::var(var))
            {
                // Both have the variable - they must match
                let left_val = left.resolve_value(&crate::term::Term::<Value>::var(var));
                let right_val = right.resolve_value(&crate::term::Term::<Value>::var(var));

                match (left_val, right_val) {
                    (Ok(l), Ok(r)) => {
                        if l != r {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
        }
        true
    }

    /// Combine two compatible match frames
    fn combine_matches(left: &Match, _right: &Match) -> Result<Match, QueryError> {
        // Start with the left match
        let result = left.clone();

        // For each variable in the right match, try to unify it with the result
        // This is a simplified implementation - a full one would need proper unification
        // For now, we just return the left match since we've already checked compatibility
        Ok(result)
    }
}

impl<L: Selection, R: Selection> Stream for Join<L, R> {
    type Item = Result<Match, QueryError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // This is a very simplified join implementation
        // A full implementation would need proper streaming join algorithms

        // For now, just return empty to avoid infinite loops
        Poll::Ready(None)
    }
}

/// A plan that joins multiple fact selector plans on shared variables
///
/// This is particularly useful for rule matches where multiple attributes
/// must exist on the same entity.
#[derive(Debug, Clone)]
pub struct FactSelectorJoinPlan {
    /// The fact selector plans to join
    pub plans: Vec<crate::fact_selector::FactSelectorPlan<Value>>,
    /// Variables to join on (typically includes "this" for entity joins)
    pub join_variables: Vec<String>,
}

impl FactSelectorJoinPlan {
    /// Create a new join plan
    pub fn new(
        plans: Vec<crate::fact_selector::FactSelectorPlan<Value>>,
        join_variables: Vec<String>,
    ) -> Self {
        Self {
            plans,
            join_variables,
        }
    }

    /// Create a join plan that joins on the entity variable
    pub fn join_on_entity(plans: Vec<crate::fact_selector::FactSelectorPlan<Value>>) -> Self {
        Self::new(plans, vec!["this".to_string()])
    }
}

impl Plan for FactSelectorJoinPlan {}

impl EvaluationPlan for FactSelectorJoinPlan {
    fn cost(&self) -> f64 {
        // Cost is sum of all plan costs plus join overhead
        let base_cost: f64 = self.plans.iter().map(|p| p.cost()).sum();
        base_cost * (self.plans.len() as f64) // Multiply by number of joins
    }

    fn evaluate<S, M>(&self, _context: EvaluationContext<S, M>) -> impl Selection + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: Selection + 'static,
    {
        // For now, return empty selection
        // Full implementation would:
        // 1. Evaluate each plan
        // 2. Join results on shared variables
        // 3. Return combined matches
        crate::selection::EmptySelection::new()
    }
}

/// Helper to create attribute join plans for rule matches
pub fn create_attribute_join(
    entity: crate::term::Term<crate::artifact::Entity>,
    attributes: Vec<(String, crate::term::Term<Value>)>,
) -> FactSelectorJoinPlan {
    let mut plans = Vec::new();

    for (attr_name, value_term) in attributes {
        let attr = attr_name.parse::<crate::artifact::Attribute>().unwrap();

        let selector = crate::fact_selector::FactSelector::<Value> {
            the: Some(crate::term::Term::from(attr)),
            of: Some(entity.clone()),
            is: Some(value_term),
            fact: None,
        };

        if let Ok(plan) = selector.plan(&crate::syntax::VariableScope::new()) {
            plans.push(plan);
        }
    }

    FactSelectorJoinPlan::join_on_entity(plans)
}
