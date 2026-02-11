//! Selector errors.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Error type for parsing selector.
#[derive(Debug, Error, PartialEq, Serialize, Deserialize)]
pub enum ParseError {
    /// Contains trailing input.
    #[error("unmatched trailing input")]
    TrailingInput(String),

    /// Unknown pattern in selector.
    #[error("unknown pattern: {0}")]
    UnknownPattern(String),

    /// Missing starting dot.
    #[error("missing starting dot: {0}")]
    MissingStartingDot(String),

    /// Starts with double dot.
    #[error("starts with double dot: {0}")]
    StartsWithDoubleDot(String),
}

/// Selector error when selecting into a concrete value.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum SelectorErrorReason {
    /// Index out of bounds.
    #[error("Index out of bounds")]
    IndexOutOfBounds,

    /// Selector key not found.
    #[error("Key not found")]
    KeyNotFound,

    /// Value is not a list, but selector expects a list.
    #[error("Not a list")]
    NotAList,

    /// Value is not a map, but selector expects a map.
    #[error("Not a map")]
    NotAMap,

    /// Value is not a collection, but selector expects a collection.
    #[error("Not a collection")]
    NotACollection,

    /// Value is not a number, but selector expects a number.
    #[error("Not a number")]
    NotANumber,

    /// Value is not a string, but selector expects a string.
    #[error("Not a string")]
    NotAString,
}
