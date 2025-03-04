use thiserror::Error;

#[derive(Error, Debug)]
pub enum XQueryError {
    #[error("Cannot parse value as attribute: {0}")]
    InvalidAttribute(String),

    #[error("Proposed matching pattern is not allowed: {0}")]
    InvalidPattern(String),

    #[error("Variable could not be assigned: {0}")]
    InvalidAssignment(String),
}
