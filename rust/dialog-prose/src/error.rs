//! Error types for the dialog-prose parser.

use std::fmt;

/// Errors that can occur during parsing.
#[derive(Debug, Clone, PartialEq)]
pub enum ParseError {
    /// Expected a particular token or construct but found something else.
    Expected { expected: String, found: String },
    /// A clause had no name parts or arguments.
    EmptyClause,
    /// A list literal `[...]` was not properly terminated.
    UnterminatedList,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::Expected { expected, found } => {
                write!(f, "expected {expected}, found {found}")
            }
            ParseError::EmptyClause => write!(f, "empty clause"),
            ParseError::UnterminatedList => write!(f, "unterminated list"),
        }
    }
}

impl std::error::Error for ParseError {}
