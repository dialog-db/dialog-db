use super::{Plan, Planner};
use crate::Environment;
use crate::error::TypeError;
use crate::selection::Selection;
use crate::source::SelectRules;
use core::pin::Pin;
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;

/// An ordered sequence of [`Plan`] steps produced by the query planner.
///
/// A `Conjunction` is the main execution plan for a conjunction of premises.
/// The planner orders the steps so that each step's prerequisites are
/// satisfied by the bindings produced by earlier steps. At evaluation time,
/// the join feeds an initial [`Match`](crate::selection::Match) stream
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
    pub fn plan(&self, scope: &Environment) -> Result<Self, TypeError> {
        let premises: Vec<_> = self.steps.iter().map(|step| step.premise.clone()).collect();
        Planner::from(premises).plan(scope)
    }

    /// Evaluate this conjunction by executing all steps in order.
    /// Each step feeds its output as input to the next, building up bindings.
    ///
    /// Returns `Pin<Box<...>>` because each step's output type depends on the
    /// previous step. Boxing erases the nesting from the type and keeps each
    /// step at pointer size on the stack.
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> Pin<Box<dyn Selection + 'a>>
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        self.steps.into_iter().fold(
            Box::pin(selection) as Pin<Box<dyn Selection + 'a>>,
            |selection, plan| Box::pin(plan.evaluate(selection, env)),
        )
    }
}
