//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

pub use super::application::Application;
use super::application::{FactApplication, FormulaApplication};
pub use super::negation::Negation;
pub use super::plan::{EvaluationPlan, Plan};
pub use crate::analyzer::{Analysis, LegacyAnalysis, Planner};
pub use crate::error::{AnalyzerError, PlanError};
pub use crate::syntax::VariableScope;
pub use crate::Dependencies;
use std::fmt::Display;

/// Represents a premise in a rule - a condition that must be satisfied.
/// Can be either a positive application or a negated exclusion.
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that produces matches.
    Apply(Application),
    /// A negated premise that excludes matches from the selection.
    Exclude(Negation),
}

impl Premise {
    pub fn dependencies(&self) -> Dependencies {
        match self {
            Premise::Apply(application) => application.dependencies(),
            Premise::Exclude(negation) => negation.dependencies(),
        }
    }
    pub fn cost(&self) -> usize {
        match self {
            Premise::Apply(application) => application.cost(),
            Premise::Exclude(negation) => negation.cost(),
        }
    }
    /// Creates an execution plan for this premise within the given variable scope.
    pub fn plan(&self, scope: &VariableScope) -> Result<Plan, PlanError> {
        match self {
            Premise::Apply(application) => application.plan(scope).map(Plan::Application),
            Premise::Exclude(negation) => negation.plan(scope).map(Plan::Negation),
        }
    }

    /// Analyzes this premise to determine its dependencies and cost.
    pub fn analyze(&self) -> LegacyAnalysis {
        match self {
            Premise::Apply(application) => application.analyze(),
            // Negation requires that all of the underlying dependencies to be
            // derived before the execution. That is why we mark all of the
            // underlying dependencies as required.
            Premise::Exclude(negation) => negation.analyze(),
        }
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Premise::Apply(application) => Display::fmt(&application, f),
            Premise::Exclude(negation) => Display::fmt(&negation, f),
        }
    }
}

// impl Syntax for Premise {
//     fn analyze<'a>(&'a self, env: &crate::analyzer::Environment) -> Stats<'a, Self> {
//         match self {
//             Premise::Apply(application) => {
//                 let Stats {
//                     cost,
//                     required,
//                     desired,
//                     ..
//                 } = Syntax::analyze(application, env);
//                 Stats {
//                     cost,
//                     required,
//                     desired,
//                     syntax: self,
//                 }
//             },
//             Premise::Exclude(negation) => {
//                 let Stats {
//                     cost,
//                     required,
//                     desired,
//                     ..
//                 } = Syntax::analyze(negation, env);
//                 Stats {
//                     cost,
//                     required,
//                     desired,
//                     syntax: self,
//                 }
//         }
//     }
// }

impl Planner for Premise {
    fn init(&self, analysis: &mut Analysis, env: &VariableScope) {
        match self {
            Self::Apply(application) => Planner::init(application, analysis, env),
            Self::Exclude(negation) => Planner::init(negation, analysis, env),
        }
    }
    fn update(&self, analysis: &mut Analysis, env: &VariableScope) {
        match self {
            Self::Apply(application) => Planner::update(application, analysis, env),
            Self::Exclude(negation) => Planner::update(negation, analysis, env),
        }
    }
}

impl From<FormulaApplication> for Premise {
    fn from(application: FormulaApplication) -> Self {
        Premise::Apply(Application::Formula(application))
    }
}

impl From<FactApplication> for Premise {
    fn from(selector: FactApplication) -> Self {
        Premise::Apply(Application::Fact(selector))
    }
}

impl From<&FactApplication> for Premise {
    fn from(selector: &FactApplication) -> Self {
        Premise::Apply(Application::Fact(selector.clone()))
    }
}
