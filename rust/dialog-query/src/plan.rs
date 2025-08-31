//! Query execution plans - traits and context for evaluation

use crate::artifact::ArtifactStore;
use crate::Selection;
use dialog_common::ConditionalSend;
use std::collections::BTreeMap;

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
    pub fn new(store: S, selection: M) -> Self {
        Self { store, selection }
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
    /// Add cost to this cost
    pub fn add(&mut self, cost: &Cost) {
        match self {
            // If current cost is infinity, replace it with the the given
            // cost.
            Cost::Infinity => {
                std::mem::replace(self, cost.clone());
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
    }
}

/// Trait implemented by execution plans
/// Following the familiar-query pattern: process selection of frames and return new frames
pub trait EvaluationPlan: Clone + std::fmt::Debug + ConditionalSend {
    /// Get the estimated cost of executing this plan
    fn cost(&self) -> &Cost;
    /// Execute this plan with the given context and return result frames
    /// This follows the familiar-query pattern where frames flow through the evaluation
    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl Selection + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: Selection + 'static;
}
