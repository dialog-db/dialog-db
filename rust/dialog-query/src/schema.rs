//! Schema system for describing parameter signatures
//!
//! This module provides a schema system that describes the structure,
//! types, and requirements of parameters for different premise types.

use crate::Type;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Describes the parameter signature of a premise.
///
/// Every premise type (relation, concept, formula) advertises a `Schema`
/// that lists its named fields together with their types, cardinalities, and
/// requirement levels. The query planner inspects the schema to determine
/// which variables must already be bound (required fields) and which will
/// be produced (optional fields), and to estimate the cost of executing the
/// premise under a given [`Environment`](crate::Environment).
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

/// Cost of a segment read for Cardinality::One with 3/3 or 2/3 constraints.
/// This is a direct lookup that reads from a single segment.
pub const SEGMENT_READ_COST: usize = 100;

/// Cost of a range read for Cardinality::Many with 3/3 constraints.
/// This read could potentially span multiple segments but is bounded.
pub const RANGE_READ_COST: usize = 200;

/// Cost of a range scan for Cardinality::Many with 2/3 constraints,
/// or Cardinality::One with 1/3 constraints.
/// This scan is likely to span multiple segments.
pub const RANGE_SCAN_COST: usize = 1_000;

/// Cost of an index scan for Cardinality::Many with 1/3 constraints.
/// This is the most expensive query pattern - scanning with minimal constraints.
pub const INDEX_SCAN: usize = 5_000;

/// Overhead cost for concept queries due to potential rule evaluation.
/// Concepts may have associated deductive rules that need to be checked and evaluated.
pub const CONCEPT_OVERHEAD: usize = 1_000;

/// Whether an attribute holds a single value or multiple values per entity.
///
/// Cardinality directly affects query cost estimation: a `Many` attribute
/// may return multiple rows for the same `(attribute, entity)` pair, so
/// scans over it are more expensive. The cost model in
/// [`Cardinality::estimate`] uses this to assign costs that the planner
/// uses when ordering premises.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Cardinality {
    /// The attribute holds a single value per entity.
    #[default]
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

/// Metadata for a single named parameter in a [`Schema`].
///
/// Captures the parameter's expected value type, whether it accepts one or
/// many values ([`Cardinality`]), and whether it must be bound before the
/// premise can execute ([`Requirement`]). The planner reads these fields
/// to classify each parameter as a prerequisite or a produced binding.
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

/// Whether a parameter must be externally bound before a premise can execute.
///
/// The planner uses this to partition a premise's parameters into
/// [`Prerequisites`](crate::planner::Prerequisites) (must be bound) and
/// bindings the premise will produce. Parameters in the same [`Group`] form
/// a *choice group* — if any member of the group is bound, the entire group
/// is satisfied.
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

/// Identifier for a choice group within a [`Requirement`].
///
/// A choice group ties together parameters that are interchangeable inputs.
/// When *any* parameter in the group is bound, the entire group is considered
/// satisfied, and the remaining unbound members become optional bindings
/// rather than prerequisites. Each `Group` carries a globally unique id
/// assigned via an atomic counter.
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
