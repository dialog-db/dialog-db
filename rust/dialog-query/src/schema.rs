//! Schema system for describing parameter signatures
//!
//! This module provides a generic schema system that describes the structure,
//! types, and requirements of parameters across different premise types.

use crate::{Cardinality, Requirement, Type};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Generic schema that maps parameter names to their descriptors
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema<T: Descriptor> {
    descriptors: HashMap<String, T>,
}

impl<T: Descriptor> Schema<T> {
    pub fn new() -> Self {
        Self {
            descriptors: HashMap::new(),
        }
    }

    pub fn insert(&mut self, name: String, descriptor: T) {
        self.descriptors.insert(name, descriptor);
    }

    pub fn get(&self, name: &str) -> Option<&T> {
        self.descriptors.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &T)> {
        self.descriptors.iter()
    }

    pub fn len(&self) -> usize {
        self.descriptors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.descriptors.is_empty()
    }
}

impl<T: Descriptor> Default for Schema<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for parameter descriptors - describes a parameter's type, requirement, etc.
pub trait Descriptor: Clone {
    /// Human-readable description of this parameter
    fn description(&self) -> &str;

    /// The data type of this parameter (None means any type)
    fn content_type(&self) -> Option<Type>;

    /// How this parameter is required/derived
    fn requirement(&self) -> &Requirement;

    /// Cardinality constraint (1, ?, +, *)
    fn cardinality(&self) -> Cardinality {
        Cardinality::One // Default
    }
}

/// Constraint descriptor - for fact selectors
/// Describes a parameter's type and requirement without additional metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constraint {
    pub description: String,
    pub content_type: Option<Type>,
    pub requirement: Requirement,
}

impl Descriptor for Constraint {
    fn description(&self) -> &str {
        &self.description
    }

    fn content_type(&self) -> Option<Type> {
        self.content_type
    }

    fn requirement(&self) -> &Requirement {
        &self.requirement
    }
}
