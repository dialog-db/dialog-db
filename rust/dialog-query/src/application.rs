pub mod concept;
pub mod constraint;
pub mod fact;
pub mod formula;

pub use crate::analyzer::AnalyzerError;
use crate::application;
pub use crate::context::new_context;
pub use crate::error::{PlanError, QueryResult};
pub use crate::premise::{Negation, Premise};
pub use crate::query::Circuit;
pub use crate::{Environment, EvaluationContext, Source};
use async_stream::try_stream;
pub use concept::ConceptApplication;
pub use constraint::ConstraintApplication;
pub use fact::FactApplication;
pub use formula::FormulaApplication;
pub use std::fmt::Display;

/// Represents different types of applications that can be used as premises in rules.
/// Each variant corresponds to a different kind of query operation.
#[derive(Debug, Clone, PartialEq)]
pub enum Application {
    /// Direct fact selection from the knowledge base
    Fact(FactApplication),
    /// Concept realization - matching entities against concept patterns
    Concept(ConceptApplication),
    /// Application of a formula for computation
    Formula(FormulaApplication),
    /// Equality constraint between two terms
    Constraint(ConstraintApplication),
}

impl Application {
    /// Estimate the cost of this application given the current environment.
    /// Each application type knows how to calculate its cost based on what's bound.
    /// Returns None if the application cannot be executed without more constraints.
    pub fn estimate(&self, env: &crate::Environment) -> Option<usize> {
        match self {
            Application::Fact(application) => application.estimate(env),
            Application::Constraint(application) => application.estimate(env),
            Application::Concept(application) => application.estimate(env),
            Application::Formula(application) => application.estimate(env),
        }
    }

    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl crate::selection::Answers {
        let source = self.clone();
        try_stream! {
            match source {
                Application::Fact(application) => {
                    for await item in application.evaluate(context) {
                        yield item?;
                    }
                },
                Application::Constraint(application) => {
                    for await item in application.evaluate(context) {
                        yield item?;
                    }
                },
                Application::Concept(application) => {
                    for await item in application.evaluate(context) {
                        yield item?;
                    }
                },
                Application::Formula(application) => {
                    for await item in application.evaluate(context) {
                        yield item?;
                    }
                },
            }
        }
    }

    pub fn parameters(&self) -> crate::Parameters {
        match self {
            Application::Fact(application) => application.parameters(),
            Application::Constraint(application) => application.parameters(),
            Application::Concept(application) => application.parameters(),
            Application::Formula(application) => application.parameters(),
        }
    }

    pub fn schema(&self) -> crate::Schema {
        match self {
            Application::Fact(application) => application.schema(),
            Application::Constraint(application) => application.schema(),
            Application::Concept(application) => application.schema(),
            Application::Formula(application) => application.schema(),
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Exclude(Negation::not(self.clone()))
    }

    pub fn query<S: Source>(&self, store: &S) -> impl crate::selection::Answers {
        let store = store.clone();
        let context = new_context(store);
        self.evaluate(context)
    }
}

impl From<FactApplication> for Application {
    fn from(selector: FactApplication) -> Self {
        Application::Fact(selector)
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

impl From<ConstraintApplication> for Application {
    fn from(constraint: ConstraintApplication) -> Self {
        Application::Constraint(constraint)
    }
}

impl Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Application::Fact(application) => Display::fmt(application, f),
            Application::Constraint(constraint) => Display::fmt(constraint, f),
            Application::Concept(application) => Display::fmt(application, f),
            Application::Formula(application) => Display::fmt(application, f),
        }
    }
}
