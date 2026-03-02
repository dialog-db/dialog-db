pub use crate::concept::application::ConceptQuery;
use crate::constraint::Constraint;
pub use crate::error::AnalyzerError;
pub use crate::error::QueryResult;
pub use crate::formula::query::FormulaQuery;
pub use crate::premise::{Negation, Premise};
pub use crate::relation::query::RelationQuery;
use crate::selection::Answers;
pub use crate::{Environment, Parameters, Schema, Source};
use futures_util::future::Either;
pub use std::fmt::Display;

/// A knowledge-base query embedded inside a [`Premise::When`](crate::Premise::When).
///
/// Each variant binds a different kind of application:
/// - `Relation` — low-level EAV triple lookup against the fact store.
/// - `Concept` — entity-level query using a concept predicate and its
///   associated deductive rules.
/// - `Formula` — pure computation that derives new bindings from existing
///   ones without touching the fact store.
/// - `Constraint` — pure variable constraint (equality, comparison) that
///   filters or infers bindings without querying stored data.
#[derive(Debug, Clone, PartialEq)]
pub enum Proposition {
    /// Relation query with separate domain and name.
    /// Boxed to reduce enum size (RelationQuery is ~432 bytes vs ~96 for other variants).
    Relation(Box<RelationQuery>),
    /// Concept realization - matching entities against concept patterns
    Concept(ConceptQuery),
    /// Application of a formula for computation
    Formula(FormulaQuery),
    /// Constraint between variables (equality, comparison, etc.)
    Constraint(Constraint),
}

impl Proposition {
    /// Estimate the cost of this application given the current environment.
    /// Each application type knows how to calculate its cost based on what's bound.
    /// Returns None if the application cannot be executed without more constraints.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Proposition::Relation(application) => application.estimate(env),
            Proposition::Concept(application) => application.estimate(env),
            Proposition::Formula(application) => application.estimate(env),
            Proposition::Constraint(constraint) => constraint.estimate(env),
        }
    }

    /// Evaluate this application against the given context, producing answers
    pub fn evaluate<S: Source, M: Answers>(self, answers: M, source: &S) -> impl Answers {
        match self {
            Proposition::Relation(application) => Either::Left(Either::Left(Either::Left(
                application.evaluate_with_provenance(source.clone(), answers),
            ))),
            Proposition::Concept(application) => Either::Left(Either::Left(Either::Right(
                application.evaluate(answers, source),
            ))),
            Proposition::Formula(application) => {
                Either::Left(Either::Right(application.evaluate(answers)))
            }
            Proposition::Constraint(constraint) => Either::Right(constraint.evaluate(answers)),
        }
    }

    /// Returns the parameter bindings for this application
    pub fn parameters(&self) -> Parameters {
        match self {
            Proposition::Relation(application) => application.parameters(),
            Proposition::Concept(application) => application.parameters(),
            Proposition::Formula(application) => application.parameters(),
            Proposition::Constraint(constraint) => constraint.parameters(),
        }
    }

    /// Returns the schema describing this application's parameters
    pub fn schema(&self) -> Schema {
        match self {
            Proposition::Relation(application) => application.schema(),
            Proposition::Concept(application) => application.schema(),
            Proposition::Formula(application) => application.schema(),
            Proposition::Constraint(constraint) => constraint.schema(),
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Unless(Negation::not(self.clone()))
    }
}

impl From<ConceptQuery> for Proposition {
    fn from(selector: ConceptQuery) -> Self {
        Proposition::Concept(selector)
    }
}

impl From<FormulaQuery> for Proposition {
    fn from(application: FormulaQuery) -> Self {
        Proposition::Formula(application)
    }
}

impl Display for Proposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Proposition::Relation(application) => Display::fmt(application, f),
            Proposition::Concept(application) => Display::fmt(application, f),
            Proposition::Formula(application) => Display::fmt(application, f),
            Proposition::Constraint(constraint) => Display::fmt(constraint, f),
        }
    }
}

impl From<Constraint> for Proposition {
    fn from(constraint: Constraint) -> Self {
        Proposition::Constraint(constraint)
    }
}
