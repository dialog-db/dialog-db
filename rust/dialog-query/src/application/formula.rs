pub use super::Application;
pub use crate::analyzer::LegacyAnalysis;
pub use crate::cursor::Cursor;
pub use crate::error::{AnalyzerError, FormulaEvaluationError, PlanError, QueryError};
pub use crate::plan::FormulaApplicationPlan;
use crate::predicate::formula::Cells;
use crate::selection::TryExpand;
use crate::SelectionExt;
pub use crate::{try_stream, EvaluationContext, Selection, Source};

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
                Requirement::Optional => {
                    analysis.desire(self.parameters.get(name), 0);
                }
                Requirement::Required(Some(_)) => {
                    // Formulas don't use choice groups, treat as derived
                    analysis.desire(self.parameters.get(name), 0);
                }
                // We should be checking this at the application time not
                // during analysis
                Requirement::Required(None) => {
                    // analysis.require(self.parameters.get(name));
                }
            }
        }

        analysis
    }

    pub fn cost(&self) -> usize {
        self.cost
    }

    /// Estimate the cost of this formula given the current environment.
    /// For formulas, cost is constant - it's the computational cost of the formula itself.
    /// Formulas are always bound (they compute rather than query), so always returns Some.
    pub fn estimate(&self, _env: &VariableScope) -> Option<usize> {
        Some(self.cost)
    }

    /// Returns the schema for this formula
    pub fn schema(&self) -> crate::Schema {
        self.cells.into()
    }

    /// Returns the parameters for this formula application
    pub fn parameters(&self) -> Parameters {
        self.parameters.clone()
    }

    pub fn dependencies(&self) -> Dependencies {
        let mut dependencies = Dependencies::new();
        for (name, cell) in self.cells.iter() {
            match cell.requirement() {
                Requirement::Optional => {
                    dependencies.desire(name.to_string(), 0);
                }
                Requirement::Required(Some(_)) => {
                    // Formulas don't use choice groups, treat as derived
                    dependencies.desire(name.to_string(), 0);
                }
                Requirement::Required(None) => {
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
                Requirement::Required(_) => {
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
                Requirement::Optional => match term {
                    Some(term) => {
                        derives.add(term);
                    }
                    None => {
                        // Derived parameters don't add cost - cost is calculated via estimate()
                    }
                },
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

    pub fn expand(&self, frame: Match) -> Result<Vec<Match>, QueryError> {
        let compute = self.compute;
        let mut cursor = Cursor::new(frame, self.parameters.clone());
        let expansion = compute(&mut cursor);
        // Map results and omit inconsistent matches
        match expansion {
            Ok(output) => Ok(output),
            Err(e) => match e {
                FormulaEvaluationError::VariableInconsistency { .. } => Ok(vec![]),
                FormulaEvaluationError::RequiredParameter { parameter } => {
                    Err(QueryError::RequiredFormulaParamater { parameter })
                }
                FormulaEvaluationError::UnboundVariable { parameter, .. } => {
                    Err(QueryError::UnboundVariable {
                        variable_name: parameter,
                    })
                }
                FormulaEvaluationError::TypeMismatch { expected, actual } => {
                    Err(QueryError::InvalidTerm {
                        message: format!("Type mismatch: expected {}, got {}", expected, actual),
                    })
                }
            },
        }
    }

    pub fn evaluate<S: Source, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        context.selection.try_expand(self.clone())
        // let parameters = self.parameters.clone();
        // let compute = self.compute;
        // context.selection.try_expand_with_fn(|frame| {
        //     let mut cursor = Cursor::new(frame, parameters.clone());
        //     let expansion = compute(&mut cursor);
        //     // let expansion = self.expand(frame);
        //     // Map results and omit inconsistent matches
        //     match expansion {
        //         Ok(output) => Ok(output),
        //         Err(e) => match e {
        //             FormulaEvaluationError::VariableInconsistency { .. } => Ok(vec![]),
        //             FormulaEvaluationError::RequiredParameter { parameter } => {
        //                 Err(QueryError::RequiredFormulaParamater { parameter })
        //             }
        //             FormulaEvaluationError::UnboundVariable { parameter, .. } => {
        //                 Err(QueryError::UnboundVariable {
        //                     variable_name: parameter,
        //                 })
        //             }
        //             FormulaEvaluationError::TypeMismatch { expected, actual } => {
        //                 Err(QueryError::InvalidTerm {
        //                     message: format!(
        //                         "Type mismatch: expected {}, got {}",
        //                         expected, actual
        //                     ),
        //                 })
        //             }
        //         },
        //     }
        // })

        //     try_stream! {

        //         for await source in context.selection {
        //             let frame = source?;
        //             let mut cursor = Cursor::new(frame, parameters.clone());
        //             let expansion = compute(&mut cursor);
        //             // let expansion = self.expand(frame);
        //             // Map results and omit inconsistent matches
        //             let results = match expansion {
        //                 Ok(output) => Ok(output),
        //                 Err(e) => {
        //                     match e {
        //                         FormulaEvaluationError::VariableInconsistency { .. } => Ok(vec![]),
        //                         FormulaEvaluationError::RequiredParameter { parameter } => {
        //                             Err(QueryError::RequiredFormulaParamater { parameter })
        //                         },
        //                         FormulaEvaluationError::UnboundVariable { parameter, .. } => {
        //                             Err(QueryError::UnboundVariable { variable_name: parameter })
        //                         },
        //                         FormulaEvaluationError::TypeMismatch { expected, actual } => {
        //                             Err(QueryError::InvalidTerm {
        //                                 message: format!("Type mismatch: expected {}, got {}", expected, actual)
        //                             })
        //                         },
        //                     }
        //                 }
        //             }?;

        //             for output in results {
        //                 yield output;
        //             }
        //         }
        //     }
    }
}

impl TryExpand for FormulaApplication {
    fn try_expand(&self, frame: Match) -> Result<Vec<Match>, QueryError> {
        self.expand(frame)
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
