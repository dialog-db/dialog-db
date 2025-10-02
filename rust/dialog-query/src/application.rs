pub mod concept;
pub mod fact;
pub mod formula;
pub mod join;
pub mod rule;

pub use crate::analyzer::{AnalyzerError, LegacyAnalysis};
pub use crate::error::PlanError;
pub use crate::plan::ApplicationPlan;
pub use crate::premise::{Negation, Premise};
pub use crate::Dependencies;
pub use crate::VariableScope;
pub use concept::ConceptApplication;
pub use fact::FactApplication;
pub use formula::FormulaApplication;
pub use join::{Join, PlanCandidate};
pub use rule::RuleApplication;
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
}

impl Application {
    pub fn cost(&self) -> usize {
        match self {
            Application::Fact(application) => application.cost(),
            Application::Concept(application) => application.cost(),
            Application::Formula(application) => application.cost(),
        }
    }

    /// Estimate the cost of this application given the current environment.
    /// Each application type knows how to calculate its cost based on what's bound.
    /// Returns None if the application cannot be executed without more constraints.
    pub fn estimate(&self, env: &crate::VariableScope) -> Option<usize> {
        match self {
            Application::Fact(application) => application.estimate(env),
            Application::Concept(application) => application.estimate(env),
            Application::Formula(application) => application.estimate(env),
        }
    }

    pub fn parameters(&self) -> crate::Parameters {
        match self {
            Application::Fact(application) => application.parameters(),
            Application::Concept(application) => application.parameters(),
            Application::Formula(application) => application.parameters(),
        }
    }

    pub fn schema(&self) -> crate::Schema {
        match self {
            Application::Fact(application) => application.schema(),
            Application::Concept(application) => application.schema(),
            Application::Formula(application) => application.schema(),
        }
    }

    pub fn dependencies(&self) -> Dependencies {
        match self {
            Application::Fact(application) => application.dependencies(),
            Application::Concept(application) => application.dependencies(),
            Application::Formula(application) => application.dependencies(),
        }
    }
    /// Analyzes this application to determine its dependencies and base cost.
    pub fn analyze(&self) -> LegacyAnalysis {
        match self {
            Application::Fact(selector) => selector.analyze(),
            Application::Concept(concept) => concept.analyze(),
            Application::Formula(application) => application.analyze(),
        }
    }

    /// Creates an execution plan for this application within the given variable scope.
    pub fn plan(&self, scope: &VariableScope) -> Result<ApplicationPlan, PlanError> {
        match self {
            Application::Fact(select) => Ok(ApplicationPlan::Fact(select.plan(&scope)?)),
            Application::Concept(concept) => Ok(ApplicationPlan::Concept(concept.plan(&scope)?)),
            Application::Formula(application) => {
                Ok(ApplicationPlan::Formula(application.plan(scope)?))
            }
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Exclude(Negation::not(self.clone()))
    }
}

impl Display for Application {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Application::Fact(application) => Display::fmt(application, f),
            Application::Concept(application) => Display::fmt(application, f),
            Application::Formula(application) => Display::fmt(application, f),
        }
    }
}

// impl Syntax for Application {
//     fn analyze<'a>(&'a self, env: &crate::analyzer::Environment) -> Stats<'a, Self> {
//         match self {
//             Application::Fact(application) => {
//                 let Stats {
//                     cost,
//                     required,
//                     desired,
//                     ..
//                 } = Syntax::analyze(application, env);

//                 Stats {
//                     syntax: self,
//                     cost,
//                     required,
//                     desired,
//                 }
//             }
//             Application::Concept(application) => {
//                 let Stats {
//                     cost,
//                     required,
//                     desired,
//                     ..
//                 } = Syntax::analyze(application, env);

//                 Stats {
//                     syntax: self,
//                     cost,
//                     required,
//                     desired,
//                 }
//             }
//             Application::Formula(application) => {
//                 let Stats {
//                     cost,
//                     required,
//                     desired,
//                     ..
//                 } = Syntax::analyze(application, env);

//                 Stats {
//                     syntax: self,
//                     cost,
//                     required,
//                     desired,
//                 }
//             }
//         }
//     }
// }
