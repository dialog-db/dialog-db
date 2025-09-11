pub mod concept;
pub mod fact;
pub mod formula;
pub mod join;
pub mod rule;

pub use crate::analyzer::{Analysis, AnalyzerError};
pub use crate::error::PlanError;
pub use crate::plan::ApplicationPlan;
pub use crate::premise::{Negation, Premise};
pub use crate::VariableScope;
pub use concept::ConcetApplication;
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
    Select(FactApplication),
    /// Concept realization - matching entities against concept patterns
    Realize(ConcetApplication),
    /// Application of another deductive rule
    ApplyRule(RuleApplication),
    /// Application of a formula for computation
    ApplyFormula(FormulaApplication),
}

impl Application {
    /// Analyzes this application to determine its dependencies and base cost.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        match self {
            Application::Select(selector) => selector.analyze(),
            Application::Realize(concept) => concept.analyze(),
            Application::ApplyRule(application) => application.analyze(),
            Application::ApplyFormula(application) => application.analyze(),
        }
    }

    /// Creates an execution plan for this application within the given variable scope.
    pub fn plan(&self, scope: &VariableScope) -> Result<ApplicationPlan, PlanError> {
        match self {
            Application::Select(select) => select.plan(&scope).map(ApplicationPlan::Select),
            Application::Realize(concept) => concept.plan(&scope).map(ApplicationPlan::Concept),
            Application::ApplyRule(application) => {
                application.plan(scope).map(ApplicationPlan::Rule)
            }

            Application::ApplyFormula(application) => {
                application.plan(scope).map(ApplicationPlan::Formula)
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
            Application::Select(application) => Display::fmt(application, f),
            Application::Realize(application) => Display::fmt(application, f),
            Application::ApplyFormula(application) => Display::fmt(application, f),
            Application::ApplyRule(application) => Display::fmt(application, f),
        }
    }
}
