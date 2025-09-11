pub use super::Application;
pub use crate::analyzer::Analysis;
pub use crate::cursor::Cursor;
pub use crate::error::{AnalyzerError, FormulaEvaluationError, PlanError};
pub use crate::plan::FormulaApplicationPlan;
pub use crate::{Dependencies, Match, Parameters, Requirement, VariableScope};
use std::fmt::Display;

/// Non-generic formula application that can be evaluated over a stream of matches
///
/// This struct represents a formula that has been bound to specific term mappings.
/// Unlike the previous generic version, this can be stored alongside other applications
/// in the deductive rule system, allowing formulas to be used as premises in rules.
#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplication {
    /// Formula identifier being applied
    pub name: &'static str,
    /// Parameter of the application keyed by names
    pub parameters: Parameters,

    /// Parameter dependencies for planning and analysis
    pub dependencies: Dependencies,

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

    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        Ok(Analysis {
            cost: 5,
            dependencies: self.dependencies.clone(),
        })
    }

    pub fn plan(&self, scope: &VariableScope) -> Result<FormulaApplicationPlan, PlanError> {
        let mut cost = self.cost;
        let mut derives = VariableScope::new();
        // We ensure that all terms for all required formula parametrs are
        // applied, otherwise we fail. We also identify all the dependencies
        // that formula will derive.
        for (name, requirement) in self.dependencies.iter() {
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
                Requirement::Derived(estimate) => match term {
                    Some(term) => {
                        derives.add(term);
                    }
                    None => {
                        cost += estimate;
                    }
                },
            }
        }

        Ok(FormulaApplicationPlan {
            application: self.clone(),
            cost,
            derives,
        })
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
        Application::ApplyFormula(application)
    }
}
