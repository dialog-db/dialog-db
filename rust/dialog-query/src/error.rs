//! Error types for the query engine

use dialog_artifacts::{DialogArtifactsError, Value, ValueDataType};
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
    TypeConversion(#[from] dialog_artifacts::TypeError),
}
