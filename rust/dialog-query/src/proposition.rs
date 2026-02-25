/// Concept application for querying entities that match a concept pattern
pub mod concept;
/// Formula application for computed values
pub mod formula;
/// Relation application for queries with separate namespace and name
pub mod relation;

pub use crate::analyzer::AnalyzerError;
pub use crate::error::{PlanError, QueryResult};
pub use crate::premise::{Negation, Premise};
pub use crate::{Environment, EvaluationContext, Source};
pub use concept::ConceptApplication;
pub use formula::FormulaApplication;
use futures_util::future::Either;
pub use relation::RelationApplication;
pub use std::fmt::Display;

/// Different types of applications that can query the knowledge base.
/// Constraints are separate `Premise` variants since they express relationships
/// between variables rather than querying the knowledge base.
#[derive(Debug, Clone, PartialEq)]
pub enum Proposition {
    /// Relation query with separate namespace and name.
    /// Boxed to reduce enum size (RelationApplication is ~432 bytes vs ~96 for other variants).
    Relation(Box<RelationApplication>),
    /// Concept realization - matching entities against concept patterns
    Concept(ConceptApplication),
    /// Application of a formula for computation
    Formula(FormulaApplication),
}

impl Proposition {
    /// Estimate the cost of this application given the current environment.
    /// Each application type knows how to calculate its cost based on what's bound.
    /// Returns None if the application cannot be executed without more constraints.
    pub fn estimate(&self, env: &crate::Environment) -> Option<usize> {
        match self {
            Proposition::Relation(application) => application.estimate(env),
            Proposition::Concept(application) => application.estimate(env),
            Proposition::Formula(application) => application.estimate(env),
        }
    }

    /// Evaluate this application against the given context, producing answers
    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::selection::Answers {
        match self {
            Proposition::Relation(application) => Either::Left(Either::Left(
                application.evaluate_with_provenance(context.source, context.selection),
            )),
            Proposition::Concept(application) => {
                Either::Left(Either::Right(application.evaluate(context)))
            }
            Proposition::Formula(application) => Either::Right(application.evaluate(context)),
        }
    }

    /// Returns the parameter bindings for this application
    pub fn parameters(&self) -> crate::Parameters {
        match self {
            Proposition::Relation(application) => application.parameters(),
            Proposition::Concept(application) => application.parameters(),
            Proposition::Formula(application) => application.parameters(),
        }
    }

    /// Returns the schema describing this application's parameters
    pub fn schema(&self) -> crate::Schema {
        match self {
            Proposition::Relation(application) => application.schema(),
            Proposition::Concept(application) => application.schema(),
            Proposition::Formula(application) => application.schema(),
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Unless(Negation::not(self.clone()))
    }
}

impl From<ConceptApplication> for Proposition {
    fn from(selector: ConceptApplication) -> Self {
        Proposition::Concept(selector)
    }
}

impl From<FormulaApplication> for Proposition {
    fn from(application: FormulaApplication) -> Self {
        Proposition::Formula(application)
    }
}

impl Display for Proposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Proposition::Relation(application) => Display::fmt(application, f),
            Proposition::Concept(application) => Display::fmt(application, f),
            Proposition::Formula(application) => Display::fmt(application, f),
        }
    }
}
