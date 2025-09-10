//! Error types for the query engine

use crate::artifact::{DialogArtifactsError, Value, ValueDataType};
use crate::term::Term;
use thiserror::Error;

/// Errors that can occur during query planning and execution
#[derive(Error, Debug, Clone, PartialEq)]
pub enum QueryError {
    /// A variable was referenced but not bound in the current scope
    #[error("Unbound variable {variable_name:?} referenced")]
    UnboundVariable { variable_name: String },

    /// A rule application is missing required parameters
    #[error("Rule application omits required parameter \"{parameter}\"")]
    MissingRuleParameter { parameter: String },

    /// A formula evaluation error
    #[error("Formula application omits required parameter: \"{parameter}\"")]
    RequiredFormulaParamater { parameter: String },

    /// A variable was used inconsistently in a formula
    #[error("Variable inconsistency: {parameter:?} has actual value {actual:?} but expected {expected:?}")]
    VariableInconsistency {
        parameter: String,
        actual: Term<Value>,
        expected: Term<Value>,
    },

    /// A variable appears in both input and output of a formula
    #[error("Variable {variable_name:?} cannot appear in both input and output")]
    VariableInputOutputConflict { variable_name: String },

    /// Planning failed due to circular dependencies
    #[error("Cannot plan query due to circular dependencies")]
    CircularDependency,

    /// Invalid rule structure
    #[error("Invalid rule: {reason}")]
    InvalidRule { reason: String },

    /// Serialization/deserialization errors
    #[error("Serialization error: {message}")]
    Serialization { message: String },

    /// Variable not supported in this context
    #[error("Variable not supported: {message}")]
    VariableNotSupported { message: String },

    /// Invalid attribute format
    #[error("Invalid attribute: {attribute}")]
    InvalidAttribute { attribute: String },

    /// Invalid term type
    #[error("Invalid term: {message}")]
    InvalidTerm { message: String },

    /// Empty selector error
    #[error("Empty selector: {message}")]
    EmptySelector { message: String },

    #[error("Fact store: {0}")]
    FactStore(String),

    /// Query planning errors
    #[error("Planning error: {message}")]
    PlanningError { message: String },
}

/// Result type for query operations
pub type QueryResult<T> = Result<T, QueryError>;

impl From<serde_json::Error> for QueryError {
    fn from(err: serde_json::Error) -> Self {
        Self::Serialization {
            message: err.to_string(),
        }
    }
}

impl From<DialogArtifactsError> for QueryError {
    fn from(value: DialogArtifactsError) -> Self {
        QueryError::FactStore(format!("{value}"))
    }
}


impl From<InconsistencyError> for QueryError {
    fn from(err: InconsistencyError) -> Self {
        match err {
            InconsistencyError::UnboundVariableError(var) => {
                QueryError::UnboundVariable { variable_name: var }
            }
            _ => QueryError::FactStore(err.to_string()),
        }
    }
}

#[derive(Error, Debug)]
pub enum InconsistencyError {
    #[error("Variable type is inconsistent with value: {0}")]
    TypeError(String),
    #[error("Different variable cannot be assigned: {0}")]
    AssignmentError(String),

    #[error("Type mismatch: expected {expected:?}, got {actual:?}")]
    TypeMismatch { expected: Value, actual: Value },

    #[error("Unbound variable: {0}")]
    UnboundVariableError(String),

    #[error("Type mismatch: expected value of type {expected}, got {actual}")]
    UnexpectedType {
        expected: ValueDataType,
        actual: ValueDataType,
    },

    #[error("Invalid fact selector")]
    UnconstrainedSelector,

    #[error("Type conversion error: {0}")]
    TypeConversion(#[from] crate::artifact::TypeError),
}
