pub use super::Application;
pub use crate::cursor::Cursor;
pub use crate::error::{AnalyzerError, FormulaEvaluationError, PlanError, QueryError};
use crate::predicate::formula::Cells;
pub use crate::{try_stream, Answer, Answers, EvaluationContext, Source};

pub use crate::{Environment, Parameters, Requirement};
use std::fmt::Display;
use std::sync::Arc;

pub const PARAM_COST: usize = 10;

/// Non-generic formula application that can be evaluated over a stream of matches
///
/// This struct represents a formula that has been bound to specific term mappings.
/// Unlike the previous generic version, this can be stored alongside other applications
/// in the deductive rule system, allowing formulas to be used as premises in rules.
#[derive(Debug, Clone)]
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
    pub compute: fn(&mut Cursor) -> Result<Vec<Answer>, FormulaEvaluationError>,
}

impl PartialEq for FormulaApplication {
    fn eq(&self, other: &Self) -> bool {
        // Compare all fields except the function pointer, which doesn't have meaningful equality
        self.name == other.name
            && self.cells == other.cells
            && self.parameters == other.parameters
            && self.cost == other.cost
            && std::ptr::addr_eq(self.compute as *const (), other.compute as *const ())
    }
}

impl FormulaApplication {
    /// Computes answers using this formula
    pub fn derive(&self, input: Answer) -> Result<Vec<Answer>, FormulaEvaluationError> {
        // Create Arc from self for cursor - this is a cheap pointer clone, not cloning the whole struct
        let formula = Arc::new(self.clone());
        let mut cursor = Cursor::new(formula, input, self.parameters.clone());
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

    pub fn expand(&self, frame: Answer) -> Result<Vec<Answer>, QueryError> {
        let compute = self.compute;
        let formula = Arc::new(self.clone());
        let mut cursor = Cursor::new(formula, frame, self.parameters.clone());
        let expansion = compute(&mut cursor);
        // Map results and omit inconsistent answers
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

    pub fn evaluate<S: Source, M: crate::selection::Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Answers {
        // Formulas now work natively with Answer and track provenance via Factor::Derived
        let formula = self.clone();
        try_stream! {
            for await each in context.selection {

                // Expand directly with Answer - no conversions needed
                for answer in formula.expand(each?)? {
                    yield answer;
                }
            }
        }
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

impl Display for FormulaApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.name)?;
        for (name, term) in self.parameters.iter() {
            write!(f, "{}: {},", name, term)?;
        }
        write!(f, "}}")
    }
}
