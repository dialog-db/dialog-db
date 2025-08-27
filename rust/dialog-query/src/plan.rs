//! Query execution plans - traits and context for evaluation

use crate::Selection;
use crate::artifact::ArtifactStore;
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

pub trait Plan: Clone + std::fmt::Debug + ConditionalSend {}

/// Trait implemented by execution plans
/// Following the familiar-query pattern: process selection of frames and return new frames
pub trait EvaluationPlan: Plan {
    /// Get the estimated cost of executing this plan
    fn cost(&self) -> f64;
    /// Execute this plan with the given context and return result frames
    /// This follows the familiar-query pattern where frames flow through the evaluation
    fn evaluate<S, M>(&self, context: EvaluationContext<S, M>) -> impl Selection + '_
    where
        S: ArtifactStore + Clone + Send + 'static,
        M: Selection + 'static;
}
