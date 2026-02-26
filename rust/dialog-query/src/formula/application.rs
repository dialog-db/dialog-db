use crate::error::{FormulaEvaluationError, QueryError};
use crate::formula::bindings::Bindings;
use crate::formula::cell::Cells;
use crate::selection::{Answer, Answers};
use crate::{Environment, Parameters, Schema, try_stream};
use std::fmt::Display;
use std::sync::Arc;

/// Cost per parameter for formula evaluation
pub const PARAM_COST: usize = 10;

/// A formula premise bound to specific term arguments.
///
/// Unlike a [`RelationApplication`](crate::relation::application::RelationApplication)
/// which reads from the fact store, a `FormulaApplication` performs pure
/// computation: it reads already-bound variables via [`Bindings`], runs a
/// user-defined `compute` function, and writes the results back as new
/// variable bindings.
///
/// The `cells` field provides the formula's [`Cells`] schema (parameter
/// names, types, and requirement levels) so the planner can determine
/// prerequisites and estimate cost without invoking the formula.
#[derive(Debug, Clone)]
pub struct FormulaApplication {
    /// Formula identifier being applied
    pub name: &'static str,
    /// Formula cells for planning and analysis
    pub cells: &'static Cells,

    /// Parameter of the application keyed by names
    pub parameters: Parameters,

    /// Base cost of evalutaion not accounting for the dependencies
    pub cost: usize,

    /// Function pointer to the formula's computation logic
    pub compute: fn(&mut Bindings) -> Result<Vec<Answer>, FormulaEvaluationError>,
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
        // Create Arc from self for bindings - this is a cheap pointer clone, not cloning the whole struct
        let formula = Arc::new(self.clone());
        let mut bindings = Bindings::new(formula, input, self.parameters.clone());
        (self.compute)(&mut bindings)
    }

    /// Estimate the cost of this formula given the current environment.
    /// For formulas, cost is constant - it's the computational cost of the formula itself.
    /// Formulas are always bound (they compute rather than query), so always returns Some.
    pub fn estimate(&self, _env: &Environment) -> Option<usize> {
        Some(self.cost)
    }

    /// Returns the schema for this formula
    pub fn schema(&self) -> Schema {
        self.cells.into()
    }

    /// Returns the parameters for this formula application
    pub fn parameters(&self) -> Parameters {
        self.parameters.clone()
    }

    /// Expand this formula with the given answer, mapping errors to QueryError
    pub fn expand(&self, answer: Answer) -> Result<Vec<Answer>, QueryError> {
        let compute = self.compute;
        let formula = Arc::new(self.clone());
        let mut bindings = Bindings::new(formula, answer, self.parameters.clone());
        let expansion = compute(&mut bindings);
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

    /// Evaluate this formula against the given answers stream, expanding each input answer
    pub fn evaluate<M: Answers>(self, answers: M) -> impl Answers {
        // Formulas now work natively with Answer and track provenance via Factor::Derived
        let formula = self;
        try_stream! {
            for await each in answers {

                // Expand directly with Answer - no conversions needed
                for answer in formula.expand(each?)? {
                    yield answer;
                }
            }
        }
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
