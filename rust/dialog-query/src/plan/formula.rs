pub use super::{try_stream, EvaluationContext, Selection, Store, VariableScope};
pub use crate::application::FormulaApplication;
pub use crate::cursor::Cursor;
pub use crate::error::{FormulaEvaluationError, QueryError};

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplicationPlan {
    /// Planned formula application
    pub application: FormulaApplication,

    /// Cost of evalutaion in a scope where all of the non `dervider`
    /// `dependencies` are bound.
    pub cost: usize,

    /// Set of terms that will be derived by this application during
    /// after evaluation
    pub derives: VariableScope,
}

impl FormulaApplicationPlan {
    pub fn cost(&self) -> usize {
        self.cost
    }
    pub fn provides(&self) -> &VariableScope {
        &self.derives
    }
    /// Evaluate the formula over a stream of matches
    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let terms = self.application.parameters.clone();
        let compute = self.application.compute;
        try_stream! {

            for await source in context.selection {
                let frame = source?;
                let mut cursor = Cursor::new(frame, terms.clone());
                let expansion = compute(&mut cursor);
                // let expansion = self.expand(frame);
                // Map results and omit inconsistent matches
                let results = match expansion {
                    Ok(output) => Ok(output),
                    Err(e) => {
                        match e {
                            FormulaEvaluationError::VariableInconsistency { .. } => Ok(vec![]),
                            FormulaEvaluationError::RequiredParameter { parameter } => {
                                Err(QueryError::RequiredFormulaParamater { parameter })
                            },
                            FormulaEvaluationError::UnboundVariable { parameter, .. } => {
                                Err(QueryError::UnboundVariable { variable_name: parameter })
                            },
                            FormulaEvaluationError::TypeMismatch { expected, actual } => {
                                Err(QueryError::InvalidTerm {
                                    message: format!("Type mismatch: expected {}, got {}", expected, actual)
                                })
                            },
                        }
                    }
                }?;

                for output in results {
                    yield output;
                }
            }
        }
    }
}
