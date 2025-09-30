pub use super::Application;
pub use crate::analyzer::LegacyAnalysis;
use crate::analyzer::Planner;
pub use crate::cursor::Cursor;
pub use crate::error::{AnalyzerError, FormulaEvaluationError, PlanError};
pub use crate::plan::FormulaApplicationPlan;
use crate::predicate::formula::Cells;
use crate::Term;
pub use crate::{Dependencies, Match, Parameters, Requirement, VariableScope};
use std::fmt::Display;

pub const PARAM_COST: usize = 10;

/// Non-generic formula application that can be evaluated over a stream of matches
///
/// This struct represents a formula that has been bound to specific term mappings.
/// Unlike the previous generic version, this can be stored alongside other applications
/// in the deductive rule system, allowing formulas to be used as premises in rules.
#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplication {
    /// Formula identifier being applied
    pub name: &'static str,
    /// Farmula cells for planning and analysis
    pub cells: &'static Cells,

    /// Parameter of the application keyed by names
    pub parameters: Parameters,

    /// Base cost of evalutaion not accounting for the dependencies
    pub cost: usize,

    /// Function pointer to the formula's computation logic
    pub compute: fn(&mut Cursor) -> Result<Vec<Match>, FormulaEvaluationError>,
}

impl FormulaApplication {
    /// Computes a single match using this formula
    pub fn derive(&self, input: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        let mut cursor = Cursor::new(input, self.parameters.clone());
        (self.compute)(&mut cursor)
    }

    pub fn analyze(&self) -> LegacyAnalysis {
        let mut analysis = LegacyAnalysis::new(self.cost);
        for (name, cell) in self.cells.iter() {
            match cell.requirement() {
                Requirement::Derived(cost) => {
                    analysis.desire(self.parameters.get(name), *cost);
                }
                Requirement::Choice { cost, .. } => {
                    // Formulas don't use choice groups, treat as derived
                    analysis.desire(self.parameters.get(name), *cost);
                }
                // We should be checking this at the application time not
                // during analysis
                Requirement::Required => {
                    // analysis.require(self.parameters.get(name));
                }
            }
        }

        analysis
    }

    pub fn cost(&self) -> usize {
        self.cost
    }

    pub fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::new();
        for (name, cell) in self.cells.iter() {
            match cell.requirement() {
                Requirement::Derived(cost) => {
                    dependencies.desire(name.to_string(), *cost);
                }
                Requirement::Choice { cost, .. } => {
                    // Formulas don't use choice groups, treat as derived
                    dependencies.desire(name.to_string(), *cost);
                }
                Requirement::Required => {
                    dependencies.require(name.to_string());
                }
            }
        }
        dependencies
    }

    pub fn plan(&self, scope: &VariableScope) -> Result<FormulaApplicationPlan, PlanError> {
        let mut cost = self.cost;
        let mut derives = VariableScope::new();
        // We ensure that all terms for all required formula parametrs are
        // applied, otherwise we fail. We also identify all the dependencies
        // that formula will derive.
        for (name, cell) in self.cells.iter() {
            let requirement = cell.requirement();
            let term = self.parameters.get(name);
            match requirement {
                Requirement::Required => {
                    if let Some(parameter) = term {
                        if scope.contains(&parameter) {
                            Ok(())
                        } else {
                            Err(PlanError::UnboundFormulaParameter {
                                formula: self.name,
                                cell: name.into(),
                                parameter: parameter.clone(),
                            })
                        }
                    } else {
                        Err(PlanError::OmitsRequiredCell {
                            formula: self.name,
                            cell: name.into(),
                        })
                    }?;
                }
                Requirement::Derived(estimate) | Requirement::Choice { cost: estimate, .. } => {
                    match term {
                        Some(term) => {
                            derives.add(term);
                        }
                        None => {
                            cost += estimate;
                        }
                    }
                }
            }
        }

        Ok(FormulaApplicationPlan {
            cost: self.cost,
            derives,
            application: self.clone(),
        })
    }

    pub fn compile(self) -> Result<FormulaApplicationAnalysis, AnalyzerError> {
        Ok(FormulaApplicationAnalysis {
            analysis: LegacyAnalysis {
                cost: self.cost,
                dependencies: self.dependencies(),
            },
            application: self,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplicationAnalysis {
    pub application: FormulaApplication,
    pub analysis: LegacyAnalysis,
}

impl FormulaApplicationAnalysis {
    pub fn dependencies(&self) -> &Dependencies {
        &self.analysis.dependencies
    }
    pub fn cost(&self) -> usize {
        self.analysis.cost
    }
}

impl Display for FormulaApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.name)?;
        for (name, term) in self.parameters.iter() {
            write!(f, "{}: {},", name, term)?;
        }
        write!(f, "}}")
    }
}

impl From<FormulaApplication> for Application {
    fn from(application: FormulaApplication) -> Self {
        Application::Formula(application)
    }
}

impl Planner for FormulaApplication {
    fn init(&self, plan: &mut crate::analyzer::Analysis, env: &VariableScope) {
        let blank = Term::blank();
        for (name, cell) in self.cells.iter() {
            let term = self.parameters.get(name).unwrap_or(&blank);
            if env.contains(term) {
                plan.desire(term, 0);
            } else {
                match cell.requirement() {
                    Requirement::Derived(cost) | Requirement::Choice { cost, .. } => {
                        plan.desire(term, *cost);
                    }
                    Requirement::Required => {
                        plan.require(term);
                    }
                }
            }
        }
    }
    fn update(&self, plan: &mut crate::analyzer::Analysis, env: &VariableScope) {
        for (name, _) in self.cells.iter() {
            if let Some(parameter) = self.parameters.get(name) {
                if env.contains(parameter) {
                    plan.desire(parameter, 0);
                }
            }
        }
    }
}
