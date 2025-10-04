pub use super::Application;
pub use crate::cursor::Cursor;
pub use crate::error::{AnalyzerError, FormulaEvaluationError, PlanError, QueryError};
use crate::predicate::formula::Cells;
use crate::selection::TryExpand;
use crate::SelectionExt;
pub use crate::{try_stream, EvaluationContext, Selection, Source};

pub use crate::{Environment, Match, Parameters, Requirement};
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

    /// Estimate the cost of this formula given the current environment.
    /// For formulas, cost is constant - it's the computational cost of the formula itself.
    /// Formulas are always bound (they compute rather than query), so always returns Some.
    pub fn estimate(&self, _env: &Environment) -> Option<usize> {
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
