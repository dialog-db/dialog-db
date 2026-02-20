//! Schema system for describing parameter signatures
//!
//! This module provides a schema system that describes the structure,
//! types, and requirements of parameters for different premise types.

use crate::Type;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schema defines set of named fields
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    fields: HashMap<String, Field>,
}

impl Schema {
    /// Creates a new empty schema.
    pub fn new() -> Self {
        Self {
            fields: HashMap::new(),
        }
    }

    /// Inserts a named field into the schema.
    pub fn insert(&mut self, name: String, constraint: Field) {
        self.fields.insert(name, constraint);
    }

    /// Returns `true` if the schema contains a field with the given name.
    pub fn contains(&self, name: &str) -> bool {
        self.fields.contains_key(name)
    }

    /// Returns a reference to the field with the given name, if present.
    pub fn get(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }

    /// Returns an iterator over all `(name, field)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &Field)> {
        self.fields.iter()
    }

    /// Returns a mutable iterator over all `(name, field)` pairs.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut Field)> {
        self.fields.iter_mut()
    }

    /// Returns the number of fields in the schema.
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Returns `true` if the schema has no fields.
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

/// Cardinality indicates whether an attribute can have one or many values
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Cardinality {
    /// The attribute holds a single value per entity.
    One,
    /// The attribute holds multiple values per entity.
    Many,
}

impl Cardinality {
    /// Estimates the cost of a fact query given what's known about the triple (the, of, is).
    ///
    /// # Parameters
    /// - `the`: Is the attribute known?
    /// - `of`: Is the entity known?
    /// - `is`: Is the value known?
    ///
    /// # Cost Model
    /// The cost depends on how many components are known and the cardinality:
    /// - 3 known (lookup): Precise lookup, low cost
    /// - 2 known (select): Index-based selection
    /// - 1 known (scan): Table/index scan
    /// - 0 known: Unbound (should be rejected)
    pub fn estimate(&self, the: bool, of: bool, is: bool) -> Option<usize> {
        use crate::application::fact::*;

        let count = (the as usize) + (of as usize) + (is as usize);

        match (count, self) {
            // Three constraints - fully bound lookup
            (3, Cardinality::One) => Some(SEGMENT_READ_COST),
            (3, Cardinality::Many) => Some(RANGE_READ_COST),

            // Two constraints - index-based select
            (2, Cardinality::One) => Some(SEGMENT_READ_COST),
            (2, Cardinality::Many) => Some(RANGE_SCAN_COST),

            // One constraint - table/index scan
            (1, Cardinality::One) => Some(RANGE_SCAN_COST),
            (1, Cardinality::Many) => Some(INDEX_SCAN),

            // No constraints - unbound query
            _ => None,
        }
    }
}

/// Field descriptor describes a type cardinality and fields requirement type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Field {
    /// Human-readable description of the field.
    pub description: String,
    /// Expected value type, or `None` if unconstrained.
    pub content_type: Option<Type>,
    /// Whether the field is required or optional.
    pub requirement: Requirement,
    /// Whether the field holds one or many values.
    pub cardinality: Cardinality,
}

impl Field {
    /// Creates a new field with the given description, type, and requirement.
    /// Cardinality defaults to [`Cardinality::One`].
    pub fn new(description: String, content_type: Option<Type>, requirement: Requirement) -> Self {
        Self {
            description,
            content_type,
            requirement,
            cardinality: Cardinality::One,
        }
    }

    /// Returns the field description.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Returns the expected content type, if any.
    pub fn content_type(&self) -> Option<Type> {
        self.content_type
    }

    /// Returns the field's requirement level.
    pub fn requirement(&self) -> &Requirement {
        &self.requirement
    }

    /// Returns the field's cardinality.
    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
    }
}

/// Represents the requirement level for a dependency in a rule or formula.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Requirement {
    /// Dependency that must be provided externally or via choice group.
    /// If Some(group), this is part of a choice group.
    /// If None, must be provided externally (no derivation possible).
    Required(Option<Group>),
    /// Dependency that can be derived if not provided.
    Optional,
}

impl Requirement {
    /// Creates a new unique choice group.
    pub fn new_group() -> Group {
        Group::new()
    }
    /// Checks if this is a required (non-derivable) dependency.
    pub fn is_required(&self) -> bool {
        matches!(self, Requirement::Required(_))
    }

    /// Check if this requirement is part of a choice group
    pub fn group(&self) -> Option<Group> {
        match self {
            Requirement::Required(Some(group)) => Some(*group),
            Requirement::Required(None) => None,
            Requirement::Optional => None,
        }
    }

    /// Creates a required requirement with no choice group.
    pub fn required() -> Self {
        Requirement::Required(None)
    }

    /// Creates an optional requirement.
    pub fn optional() -> Self {
        Requirement::Optional
    }
}

/// Identifier for a choice group
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Group(usize);

impl Default for Group {
    fn default() -> Self {
        Self::new()
    }
}

impl Group {
    /// Creates a new group with a unique auto-incremented identifier.
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Group(id)
    }

    /// Creates required requirement that is part of this group.
    pub fn required(&self) -> Requirement {
        Requirement::Required(Some(*self))
    }

    /// Creates optional requirement.
    pub fn optional(&self) -> Requirement {
        Requirement::Optional
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn test_requirement_properties() {
        let required = Requirement::Required(None);
        let derived = Requirement::Optional;

        assert!(required.is_required());
        assert!(!derived.is_required());
    }
}
