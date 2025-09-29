use crate::analyzer::Planner;
use crate::application::ApplicationAnalysis;
use crate::plan::Plan;
use crate::{analyzer, dependencies};

use super::analyzer::{Analysis, Stats, Syntax};
use super::application::Application;
use super::error::{AnalyzerError, PlanError};
use super::plan::NegationPlan;
use super::{Dependencies, VariableScope};
use std::fmt::Display;
use std::os::macos::raw::stat;

// FactSelectorPlan's EvaluationPlan implementation is in fact_selector.rs

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
    pub fn analyze(&self) -> Analysis {
        let Negation(application) = self;
        let Analysis { cost, dependencies } = application.analyze();
        let mut analysis = Analysis::new(cost + 100);
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

    pub fn compile(self) -> Result<NegationAnalysis, AnalyzerError> {
        let Negation(source) = self;
        let mut dependencies = Dependencies::new();
        let application = source.compile()?;
        for (name, _) in application.dependencies().iter() {
            dependencies.require(name.into());
        }

        Ok(NegationAnalysis {
            analysis: Analysis {
                cost: application.cost(),
                dependencies,
            },
            application,
        })
    }
}

impl Planner for Negation {
    fn init(&self, plan: &mut analyzer::SyntaxAnalysis, env: &VariableScope) {
        let Negation(application) = self;
        Planner::init(application, plan, env);
        plan.require_all();
    }
    fn update(&self, plan: &mut analyzer::SyntaxAnalysis, env: &VariableScope) {
        let Negation(application) = self;
        Planner::update(application, plan, env);
        plan.require_all();
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

#[derive(Debug, Clone, PartialEq)]
pub struct NegationAnalysis {
    pub application: ApplicationAnalysis,
    pub analysis: Analysis,
}

impl NegationAnalysis {
    pub fn cost(&self) -> usize {
        self.analysis.cost
    }

    pub fn dependencies(&self) -> &'_ Dependencies {
        &self.analysis.dependencies
    }
}

impl Display for Negation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Negation(application) = self;
        write!(f, "! {}", application)
    }
}
