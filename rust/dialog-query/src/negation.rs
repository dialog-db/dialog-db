
use super::analyzer::LegacyAnalysis;
use super::application::Application;
use super::error::PlanError;
use super::plan::NegationPlan;
use super::{Dependencies, VariableScope};
use std::fmt::Display;

// FactSelectorPlan's EvaluationPlan implementation is in fact_selector.rs

/// Cost overhead added for negation operations (checking non-existence)
pub const NEGATION_OVERHEAD: usize = 100;

/// Represents a negated application that excludes matching results.
/// Used in rules to specify conditions that must NOT hold.
#[derive(Debug, Clone, PartialEq)]
pub struct Negation(pub Application);

impl Negation {
    pub fn not(application: Application) -> Self {
        Negation(application)
    }

    pub fn cost(&self) -> usize {
        let Negation(application) = self;
        application.cost()
    }

    /// Estimate the cost of this negation given the current environment.
    /// Negation adds overhead to the underlying application's cost.
    /// Returns None if the underlying application cannot be executed.
    pub fn estimate(&self, env: &VariableScope) -> Option<usize> {
        let Negation(application) = self;
        application.estimate(env).map(|cost| cost + NEGATION_OVERHEAD)
    }

    pub fn parameters(&self) -> crate::Parameters {
        let Negation(application) = self;
        application.parameters()
    }

    /// Returns schema for negation - all parameters become required
    /// because negation can't run until all terms are bound
    pub fn schema(&self) -> crate::Schema {
        let Negation(application) = self;
        let mut schema = application.schema();

        // Convert all desired parameters to required
        for (_, constraint) in schema.iter_mut() {
            constraint.requirement = crate::Requirement::Required(None);
        }

        schema
    }

    pub fn dependencies(&self) -> Dependencies {
        let Negation(application) = self;
        let mut dependencies = Dependencies::new();
        for (name, _) in application.dependencies().iter() {
            dependencies.require(name.into());
        }
        dependencies
    }
    /// Analyzes this negation to determine dependencies and cost.
    /// All dependencies become required since negation must fully evaluate its condition.
    pub fn analyze(&self) -> LegacyAnalysis {
        let Negation(application) = self;
        let LegacyAnalysis { cost, dependencies } = application.analyze();
        let mut analysis = LegacyAnalysis::new(cost + 100);
        let required = &mut analysis.dependencies;
        for (name, _) in dependencies.iter() {
            required.require(name.into());
        }

        analysis
    }
    /// Creates an execution plan for this negation within the given variable scope.
    pub fn plan(&self, scope: &VariableScope) -> Result<NegationPlan, PlanError> {
        let Negation(application) = self;
        let plan = application.plan(&scope)?;

        Ok(plan.not())
    }
}

// impl Syntax for Negation {
//     fn analyze<'a>(&'a self, env: &analyzer::Environment) -> Stats<'a, Self> {
//         let mut analysis = Syntax::analyze(&self.0, env);
//         analysis.require_all();

//         Stats {
//             syntax: self,
//             cost: analysis.cost,
//             required: analysis.required,
//             desired: analysis.desired,
//         }
//     }
// }

impl Display for Negation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}
