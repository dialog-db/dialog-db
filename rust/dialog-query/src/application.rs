/// Concept application for querying entities that match a concept pattern
pub mod concept;
/// Formula application for computed values
pub mod formula;
/// Relation application for queries with separate namespace and name
pub mod relation;

pub use crate::analyzer::AnalyzerError;
pub use crate::context::new_context;
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
pub enum Application {
    /// Relation query with separate namespace and name.
    /// Boxed to reduce enum size (RelationApplication is ~432 bytes vs ~96 for other variants).
    Relation(Box<RelationApplication>),
    /// Concept realization - matching entities against concept patterns
    Concept(ConceptApplication),
    /// Application of a formula for computation
    Formula(FormulaApplication),
}

impl Application {
    /// Estimate the cost of this application given the current environment.
    /// Each application type knows how to calculate its cost based on what's bound.
    /// Returns None if the application cannot be executed without more constraints.
    pub fn estimate(&self, env: &crate::Environment) -> Option<usize> {
        match self {
            Application::Relation(application) => application.estimate(env),
            Application::Concept(application) => application.estimate(env),
            Application::Formula(application) => application.estimate(env),
        }
    }

    /// Evaluate this application against the given context, producing answers
    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::selection::Answers {
        match self {
            Application::Relation(application) => Either::Left(Either::Left(
                application.evaluate_with_provenance(context.source, context.selection),
            )),
            Application::Concept(application) => {
                Either::Left(Either::Right(application.clone().evaluate(context)))
            }
            Application::Formula(application) => Either::Right(application.evaluate(context)),
        }
    }

    /// Returns the parameter bindings for this application
    pub fn parameters(&self) -> crate::Parameters {
        match self {
            Application::Relation(application) => application.parameters(),
            Application::Concept(application) => application.parameters(),
            Application::Formula(application) => application.parameters(),
        }
    }

    /// Returns the schema describing this application's parameters
    pub fn schema(&self) -> crate::Schema {
        match self {
            Application::Relation(application) => application.schema(),
            Application::Concept(application) => application.schema(),
            Application::Formula(application) => application.schema(),
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Exclude(Negation::not(self.clone()))
    }

    /// Execute this application as a query against the given store
    pub fn query<S: Source>(&self, store: &S) -> impl crate::selection::Answers {
        let store = store.clone();
        let context = new_context(store);
        self.evaluate(context)
    }
}

impl From<ConceptApplication> for Application {
    fn from(selector: ConceptApplication) -> Self {
        Application::Concept(selector)
    }
}

impl From<FormulaApplication> for Application {
    fn from(application: FormulaApplication) -> Self {
        Application::Formula(application)
    }
}

impl Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Application::Relation(application) => Display::fmt(application, f),
            Application::Concept(application) => Display::fmt(application, f),
            Application::Formula(application) => Display::fmt(application, f),
        }
    }
}
