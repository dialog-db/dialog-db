//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

pub use super::constraint::Constraint;
pub use super::negation::Negation;
use crate::Source;
use crate::environment::Environment;
pub use crate::error::{AnalyzerError, PlanError, QueryResult};
use crate::formula::application::FormulaApplication;
use crate::proposition::Proposition;
use crate::selection::{Answer, Answers};
use crate::{Parameters, Schema};
use futures_util::future::Either;
use std::fmt::Display;

/// A single condition in a deductive rule's body.
///
/// Rules are built from an ordered sequence of premises. During query
/// planning each premise is wrapped in an [`Candidate`](crate::Candidate)
/// to determine whether it can execute given the current variable bindings.
/// At execution time, premises are evaluated in the order chosen by the
/// planner: each premise receives the stream of [`Answer`](crate::selection::Answer)s
/// produced so far and extends it with new bindings.
///
/// There are three kinds of premise:
/// - `When` — queries the knowledge base via a [`Proposition`] (fact, concept,
///   or formula lookup).
/// - `Where` — applies a [`Constraint`] between already-bound variables
///   (equality, comparison, etc.).
/// - `Unless` — a [`Negation`] that *excludes* answers matching a pattern.
///
/// TODO: Large enum variant - Constrain (320 bytes) is much larger than other variants.
/// Consider boxing Constraint to reduce memory usage when storing Apply/Exclude variants.
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that queries the knowledge base.
    When(Proposition),
    /// A constraint that relates variables (equality, comparison, etc.).
    Where(Constraint),
    /// A negated premise that excludes matches from the selection.
    Unless(Negation),
}

impl Premise {
    /// Estimate the cost of this premise given the current environment.
    /// Returns None if the premise cannot be executed without more constraints.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Premise::When(application) => application.estimate(env),
            Premise::Where(constraint) => constraint.estimate(env),
            Premise::Unless(negation) => negation.estimate(env),
        }
    }

    /// Returns the parameter bindings for this premise
    pub fn parameters(&self) -> Parameters {
        match self {
            Premise::When(application) => application.parameters(),
            Premise::Where(constraint) => constraint.parameters(),
            Premise::Unless(negation) => negation.parameters(),
        }
    }

    /// Returns the schema describing this premise's parameters
    pub fn schema(&self) -> Schema {
        match self {
            Premise::When(application) => application.schema(),
            Premise::Where(constraint) => constraint.schema(),
            Premise::Unless(negation) => negation.schema(),
        }
    }

    /// Evaluate this premise with the given answers and source
    pub fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        match self {
            Premise::When(application) => {
                Either::Left(Either::Left(application.evaluate(answers, source)))
            }
            Premise::Where(constraint) => Either::Left(Either::Right(constraint.evaluate(answers))),
            Premise::Unless(negation) => Either::Right(negation.evaluate(answers, source)),
        }
    }

    /// Execute this premise against the given store
    pub fn perform<S: Source>(self, store: &S) -> impl Answers {
        self.evaluate(Answer::new().seed(), store)
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Premise::When(application) => Display::fmt(&application, f),
            Premise::Where(constraint) => Display::fmt(&constraint, f),
            Premise::Unless(negation) => Display::fmt(&negation, f),
        }
    }
}

impl From<Constraint> for Premise {
    fn from(constraint: Constraint) -> Self {
        Premise::Where(constraint)
    }
}

impl From<FormulaApplication> for Premise {
    fn from(application: FormulaApplication) -> Self {
        Premise::When(Proposition::Formula(application))
    }
}
