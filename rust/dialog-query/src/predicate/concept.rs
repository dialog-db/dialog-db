use crate::{Attribute, Value};
use std::collections::HashMap;

/// Represents a concept which is a set of attributes that define an entity type.
/// Concepts are similar to tables in relational databases but are more flexible
/// as they can be derived from rules rather than just stored directly.
#[derive(Debug, Clone, PartialEq)]
pub struct Concept {
    /// Concept identifier used to look concepts up by.
    pub operator: String,
    /// Map of attribute names to their definitions for this concept.
    pub attributes: HashMap<String, Attribute<Value>>,
}
