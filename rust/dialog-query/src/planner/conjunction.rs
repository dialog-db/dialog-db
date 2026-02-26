use super::{Plan, Planner};
use crate::error::CompileError;
use crate::selection::Answers;
use crate::{Environment, Source};
use core::pin::Pin;

/// An ordered sequence of [`Plan`] steps produced by the query planner.
///
/// A `Conjunction` is the main execution plan for a conjunction of premises.
/// The planner orders the steps so that each step's prerequisites are
/// satisfied by the bindings produced by earlier steps. At evaluation time,
/// the join feeds an initial [`Answer`](crate::selection::Answer) stream
/// through each step in order, progressively binding more variables.
///
/// The `cost` field is the sum of all step costs and is used when comparing
/// alternative plans (e.g. across different rule bodies in a [`Disjunction`](super::Disjunction)).
///
/// Create a `Conjunction` via [`Planner::plan`](super::Planner::plan) or
/// [`Conjunction::plan`] to re-optimize with different bindings.
#[derive(Debug, Clone, PartialEq)]
pub struct Conjunction {
    /// The ordered steps to execute
    pub steps: Vec<Plan>,
    /// Total execution cost
    pub cost: usize,
    /// Variables provided/bound by this join
    pub binds: Environment,
    /// Variables required in the environment to execute this join
    pub env: Environment,
}

impl Conjunction {
    /// Re-plan this join against a new scope.
    ///
    /// Converts the steps back into planner candidates and re-orders them
    /// for optimal execution given the new bindings. This is used when a
    /// rule's premises need to be re-evaluated with different known bindings
    /// (e.g. adornment-based optimization in concepts).
    pub fn plan(&self, scope: &Environment) -> Result<Self, CompileError> {
        let premises: Vec<_> = self.steps.iter().map(|step| step.premise.clone()).collect();
        Planner::from(premises).plan(scope)
    }

    /// Evaluate this conjunction by executing all steps in order.
    /// Each step feeds its output as input to the next, building up bindings.
    ///
    /// Returns `Pin<Box<...>>` because each step's output type depends on the
    /// previous step. Boxing erases the nesting from the type and keeps each
    /// step at pointer size on the stack.
    pub fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> Pin<Box<dyn Answers>> {
        self.steps.into_iter().fold(
            Box::pin(answers) as Pin<Box<dyn Answers>>,
            |answers, step| Box::pin(step.evaluate(answers, source)),
        )
    }
}
