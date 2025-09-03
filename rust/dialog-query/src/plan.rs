//! Query execution plans - traits and context for evaluation

use crate::artifact::ArtifactStore;
use crate::query::Store;
use crate::syntax::VariableScope;
use crate::{Match, Selection};
use dialog_common::ConditionalSend;
use futures_util::stream::once;
use std::collections::BTreeMap;
use thiserror::Error;

pub fn fresh<S: ArtifactStore>(store: S) -> EvaluationContext<S, impl Selection> {
    let selection = once(async move { Ok(Match::new()) });
    EvaluationContext { store, selection }
}

/// A single result frame with variable bindings
/// Equivalent to MatchFrame in TypeScript: Map<Variable, Scalar>
pub type MatchFrame = BTreeMap<String, crate::artifact::Value>;

/// Evaluation context passed to plans during execution
/// Based on TypeScript EvaluationContext in @query/src/api.ts
pub struct EvaluationContext<S, M>
where
    S: ArtifactStore,
    M: Selection,
{
    /// Current selection of frames being processed (equivalent to frames in familiar-query)
    pub selection: M,
    /// Artifact store for querying facts (equivalent to source/Querier in TypeScript)
    pub store: S,
}

impl<S, M> EvaluationContext<S, M>
where
    S: ArtifactStore,
    M: Selection,
{
    /// Create a new evaluation context
    pub fn single(store: S, selection: M) -> Self {
        Self { store, selection }
    }

    pub fn new(store: S) -> EvaluationContext<S, impl Selection> {
        let selection = once(async move { Ok(Match::new()) });

        EvaluationContext { store, selection }
    }
}

/// Describes cost of the plan execution. Infinity, implies plan is not
/// executable because some of the input variables are not bound.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cost {
    Infinity,
    Estimate(usize),
}

impl Cost {
    pub fn add(&mut self, cost: usize) -> &mut Self {
        match self {
            Cost::Infinity => {
                *self = Cost::Estimate(cost);
            }
            Cost::Estimate(total) => {
                *total += cost;
            }
        };
        self
    }
    pub fn subtract(&mut self, cost: usize) -> &mut Self {
        match self {
            Cost::Infinity => {}
            Cost::Estimate(total) => {
                *total -= cost;
            }
        };
        self
    }

    /// Add cost to this cost
    pub fn join(&mut self, cost: &Cost) -> &mut Self {
        match self {
            // If current cost is infinity, replace it with the the given
            // cost.
            Cost::Infinity => {
                *self = cost.clone();
            }
            // If current cost is estimate, add the given cost to it unless it
            // is infinity.
            Cost::Estimate(total) => match cost {
                Cost::Infinity => {}
                Cost::Estimate(cost) => {
                    *total += cost;
                }
            },
        };
        self
    }
}

/// Trait implemented by execution plans
/// Following the familiar-query pattern: process selection of frames and return new frames
pub trait EvaluationPlan: Clone + std::fmt::Debug + ConditionalSend {
    /// Get the estimated cost of executing this plan
    fn cost(&self) -> usize;
    /// Set of variables that this plan will bind
    fn provides(&self) -> VariableScope;
    /// Execute this plan with the given context and return result frames
    /// This follows the familiar-query pattern where frames flow through the evaluation
    fn evaluate<S: Store, M: Selection>(&self, context: EvaluationContext<S, M>) -> impl Selection;
}

/// Local ordering trait for EvaluationPlan types
/// This provides the same functionality as Ord but avoids orphan rule issues
pub trait PlanOrdering {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
}

/// Blanket implementation of PlanOrdering for EvaluationPlan based on cost and variable provision
impl<T: EvaluationPlan> PlanOrdering for T {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary comparison: cost (lower is better)
        match self.cost().cmp(&other.cost()) {
            std::cmp::Ordering::Equal => {
                // Tie-breaker: number of provided variables (more is better)
                other.provides().size().cmp(&self.provides().size())
            }
            other_ordering => other_ordering,
        }
    }
}

/// Possible solutution to planning error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Solution {
    /// Set of variables that need to be bound before plan can be completed.
    pub requires: VariableScope,
}

pub type PlanResult<P> = Result<P, PlanError>;

/// Errors that can occur during query planning and execution
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("Can not plan query due to unsatisfied dependency")]
pub struct PlanError {
    pub description: String,
    pub solutions: Vec<Solution>,
}
