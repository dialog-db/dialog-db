//! Query execution plans - traits and context for evaluation

pub use crate::artifact::ArtifactStore;
pub use crate::query::Source;
pub use crate::{try_stream, Match, Selection, Value};
pub use dialog_common::ConditionalSend;
pub use futures_util::stream::once;
use std::collections::BTreeMap;

pub mod application;
pub mod concept;
pub mod fact;
pub mod formula;

pub mod join;
pub mod negation;
pub mod rule;
pub use futures_util::{stream, TryStreamExt};

pub use super::parameters::Parameters;
pub use super::syntax::VariableScope;
pub use application::ApplicationPlan;
pub use concept::ConceptPlan;
pub use core::cmp::Ordering;
pub use fact::FactApplicationPlan;
pub use formula::FormulaApplicationPlan;
pub use join::Join;
pub use negation::NegationPlan;
pub use rule::RuleApplicationPlan;

/// Top-level execution plan that can be either a positive application or a negation.
/// Used by the query planner to organize premise execution.
#[derive(Debug, Clone, PartialEq)]
pub enum Plan {
    /// Positive application that produces matches
    Application(ApplicationPlan),
    /// Negative application that filters out matches
    Negation(NegationPlan),
}

impl EvaluationPlan for Plan {
    fn cost(&self) -> usize {
        match self {
            Plan::Application(plan) => plan.cost(),
            Plan::Negation(plan) => plan.cost(),
        }
    }

    fn provides(&self) -> &VariableScope {
        match self {
            Plan::Application(plan) => plan.provides(),
            Plan::Negation(plan) => plan.provides(),
        }
    }

    fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let source = self.clone();
        try_stream! {
            match source {
                Plan::Application(plan) => {
                    for await output in plan.evaluate(context) {
                        yield output?
                    }
                },
                Plan::Negation(plan) => {
                    for await output in plan.evaluate(context) {
                        yield output?
                    }
                }
            }
        }
    }
}

impl PartialOrd for Plan {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Plan::Application(_), Plan::Negation(_)) => Some(core::cmp::Ordering::Less),
            (Plan::Negation(_), Plan::Application(_)) => Some(core::cmp::Ordering::Greater),
            (Plan::Application(left), Plan::Application(right)) => left.partial_cmp(right),
            (Plan::Negation(left), Plan::Negation(right)) => left.partial_cmp(right),
        }
    }
}

pub fn fresh<S: ArtifactStore>(store: S) -> EvaluationContext<S, impl Selection> {
    let selection = once(async move { Ok(Match::new()) });
    EvaluationContext { store, selection }
}

/// A single result frame with variable bindings
/// Equivalent to MatchFrame in TypeScript: Map<Variable, Scalar>
pub type MatchFrame = BTreeMap<String, Value>;

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

/// Trait implemented by execution plans
/// Following the familiar-query pattern: process selection of frames and return new frames
pub trait EvaluationPlan: Clone + std::fmt::Debug + ConditionalSend {
    /// Get the estimated cost of executing this plan
    fn cost(&self) -> usize;
    /// Set of variables that this plan will bind
    fn provides(&self) -> &VariableScope;
    /// Execute this plan with the given context and return result frames
    /// This follows the familiar-query pattern where frames flow through the evaluation
    fn evaluate<S: Source, M: Selection>(&self, context: EvaluationContext<S, M>)
        -> impl Selection;
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
