pub mod concept;
pub mod fact;
pub mod formula;
pub mod join;
pub mod rule;

use crate::analyzer::Planner;
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

impl Planner for Application {
    fn init(&self, plan: &mut crate::analyzer::Analysis, env: &VariableScope) {
        match self {
            Application::Fact(application) => Planner::init(application, plan, env),
            Application::Concept(application) => Planner::init(application, plan, env),
            Application::Formula(application) => Planner::init(application, plan, env),
        }
    }
    fn update(&self, plan: &mut crate::analyzer::Analysis, env: &VariableScope) {
        match self {
            Application::Fact(application) => Planner::update(application, plan, env),
            Application::Concept(application) => Planner::update(application, plan, env),
            Application::Formula(application) => Planner::update(application, plan, env),
        }
    }
}
