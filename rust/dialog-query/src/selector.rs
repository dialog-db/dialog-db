//! Selector patterns for matching against the database

use serde::{Deserialize, Serialize};
use crate::term::Term;

/// Selector pattern for matching against the database (with variables/constants)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Selector {
    /// The entity (subject)
    pub entity: Term,
    /// The attribute (predicate) 
    pub attribute: Term,
    /// The value (object)
    pub value: Term,
}

impl Selector {
    /// Create a new selector with the given terms
    pub fn new(entity: Term, attribute: Term, value: Term) -> Self {
        Self {
            entity,
            attribute,
            value,
        }
    }
}