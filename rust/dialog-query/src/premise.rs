//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

pub use super::negation::Negation;
use crate::Source;
use crate::constraint::Constraint;
use crate::environment::Environment;
pub use crate::error::{AnalyzerError, QueryResult};
use crate::formula::query::FormulaQuery;
use crate::proposition::Proposition;
use crate::selection::{Match, Selection};
use crate::{Parameters, Schema};
use futures_util::future::Either;
use std::fmt::Display;

/// A single condition in a deductive rule's body.
///
/// Rules are built from an ordered sequence of premises. During query
/// planning each premise is wrapped in an [`Candidate`](crate::Candidate)
/// to determine whether it can execute given the current variable bindings.
/// At execution time, premises are evaluated in the order chosen by the
/// planner: each premise receives the stream of [`Match`](crate::selection::Match)s
/// produced so far and extends it with new bindings.
///
/// There are two kinds of premise:
/// - `When` — queries the knowledge base or applies a constraint via a
///   [`Proposition`] (fact, concept, formula, or constraint).
/// - `Unless` — a [`Negation`] that *excludes* matches matching a pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that queries the knowledge base or applies a constraint.
    Assert(Proposition),
    /// A negated premise that excludes matches from the selection.
    Unless(Negation),
}

impl Premise {
    /// Estimate the cost of this premise given the current environment.
    /// Returns None if the premise cannot be executed without more constraints.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Premise::Assert(application) => application.estimate(env),
            Premise::Unless(negation) => negation.estimate(env),
        }
    }

    /// Returns the parameter bindings for this premise
    pub fn parameters(&self) -> Parameters {
        match self {
            Premise::Assert(application) => application.parameters(),
            Premise::Unless(negation) => negation.parameters(),
        }
    }

    /// Returns the schema describing this premise's parameters
    pub fn schema(&self) -> Schema {
        match self {
            Premise::Assert(application) => application.schema(),
            Premise::Unless(negation) => negation.schema(),
        }
    }

    /// Evaluate this premise with the given selection and source
    pub fn evaluate<S: Source, M: Selection>(self, selection: M, source: &S) -> impl Selection {
        match self {
            Premise::Assert(application) => Either::Left(application.evaluate(selection, source)),
            Premise::Unless(negation) => Either::Right(negation.evaluate(selection, source)),
        }
    }

    /// Execute this premise against the given store
    pub fn perform<S: Source>(self, store: &S) -> impl Selection {
        self.evaluate(Match::new().seed(), store)
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Premise::Assert(application) => Display::fmt(&application, f),
            Premise::Unless(negation) => Display::fmt(&negation, f),
        }
    }
}

impl std::ops::Not for Premise {
    type Output = Premise;

    fn not(self) -> Self::Output {
        match self {
            Premise::Assert(proposition) => Premise::Unless(Negation::not(proposition)),
            Premise::Unless(Negation(proposition)) => Premise::Assert(proposition),
        }
    }
}

impl From<Constraint> for Premise {
    fn from(constraint: Constraint) -> Self {
        Premise::Assert(Proposition::Constraint(constraint))
    }
}

impl From<FormulaQuery> for Premise {
    fn from(application: FormulaQuery) -> Self {
        Premise::Assert(Proposition::Formula(application))
    }
}
