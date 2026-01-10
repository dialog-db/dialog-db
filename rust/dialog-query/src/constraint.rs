//! Constraint system for query variables
//!
//! Constraints express relationships between terms that must be satisfied during
//! query evaluation. Unlike applications which query the knowledge base, constraints
//! operate on variable bindings to filter, infer, or validate values.

pub mod equality;

pub use equality::Equality;

use crate::selection::Answers;
use crate::{Environment, EvaluationContext, Parameters, Schema, Source};
use std::fmt::Display;

/// Constraint enum representing different types of constraints between terms.
///
/// Constraints express relationships that variables must satisfy. They support
/// bidirectional inference when possible, meaning if one term is bound, the other
/// can often be inferred from the constraint.
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    /// Equality constraint between two terms.
    ///
    /// Enforces that both terms must have equal values. Supports bidirectional
    /// inference: if one term is bound, the other will be inferred to have the
    /// same value.
    Equality(Equality),
}

impl Constraint {
    /// Returns the schema for this constraint.
    ///
    /// The schema describes what parameters the constraint requires to be evaluable.
    pub fn schema(&self) -> Schema {
        match self {
            Constraint::Equality(constraint) => constraint.schema(),
        }
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    ///
    /// Returns `Some(cost)` if the constraint can be evaluated (at least one term is bound).
    /// Returns `None` if the constraint cannot be evaluated yet (neither term is bound).
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Constraint::Equality(constraint) => constraint.estimate(env),
        }
    }

    /// Returns the parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        match self {
            Constraint::Equality(constraint) => constraint.parameters(),
        }
    }

    /// Evaluates the constraint against the current selection of answers.
    ///
    /// This method processes each answer in the input selection and:
    /// - **Filters** answers where constraints are violated
    /// - **Infers** missing bindings when possible
    /// - **Errors** when constraints cannot be evaluated
    ///
    /// # Returns
    /// A stream of answers that satisfy the constraint, with any necessary
    /// variable bindings added through inference.
    pub fn evaluate<S: Source, M: Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Answers {
        match self {
            Constraint::Equality(constraint) => constraint.evaluate(context),
        }
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Constraint::Equality(constraint) => Display::fmt(constraint, f),
        }
    }
}

impl From<Equality> for Constraint {
    fn from(constraint: Equality) -> Self {
        Constraint::Equality(constraint)
    }
}
