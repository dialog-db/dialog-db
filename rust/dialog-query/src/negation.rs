use super::analyzer::Analysis;
use super::application::Application;
use super::error::{AnalyzerError, PlanError};
use super::plan::NegationPlan;
use super::{Dependencies, VariableScope};
use std::fmt::Display;

// FactSelectorPlan's EvaluationPlan implementation is in fact_selector.rs

/// Represents a negated application that excludes matching results.
/// Used in rules to specify conditions that must NOT hold.
#[derive(Debug, Clone, PartialEq)]
pub struct Negation(pub Application);

impl Negation {
    pub fn not(application: Application) -> Self {
        Negation(application)
    }
    /// Analyzes this negation to determine dependencies and cost.
    /// All dependencies become required since negation must fully evaluate its condition.
    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        let Negation(application) = self;
        let mut dependencies = Dependencies::new();
        let analysis = application.analyze()?;
        for (name, _) in analysis.dependencies.iter() {
            dependencies.require(name.into());
        }

        Ok(Analysis {
            dependencies,
            cost: analysis.cost,
        })
    }
    /// Creates an execution plan for this negation within the given variable scope.
    pub fn plan(&self, scope: &VariableScope) -> Result<NegationPlan, PlanError> {
        let Negation(application) = self;
        let plan = application.plan(&scope)?;

        Ok(plan.not())
    }
}

impl Display for Negation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}
