use crate::selection::Selection;
use crate::{Environment, Premise, Source};

/// A finalized, ready-to-execute premise produced by the query planner.
///
/// A `Plan` is the lightweight output of a successful [`Candidate`]. It carries
/// only the information needed at execution time: the premise itself, its
/// estimated cost, the variables it will bind, and the variables already
/// bound in the environment. The cached schema and parameter data used during
/// planning are dropped at this point.
///
/// Plans are assembled into a [`Conjunction`](crate::Conjunction) — an ordered sequence of
/// steps that the query engine evaluates to produce results.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// The premise this plan will execute.
    pub premise: Premise,
    /// Estimated execution cost.
    pub cost: usize,
    /// Variables that this plan will bind upon execution.
    pub binds: Environment,
    /// Variables already bound in the environment when this plan runs.
    pub env: Environment,
}

impl Plan {
    /// Returns the estimated execution cost.
    pub fn cost(&self) -> usize {
        self.cost
    }

    /// Returns the set of variables this plan will bind.
    pub fn binds(&self) -> &Environment {
        &self.binds
    }

    /// Returns the environment of already-bound variables for this plan.
    pub fn env(&self) -> &Environment {
        &self.env
    }

    /// Evaluate this plan with the given selection and source
    pub fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        self.premise.evaluate(selection, source)
    }
}
