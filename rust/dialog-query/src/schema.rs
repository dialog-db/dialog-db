//! Schema system for describing parameter signatures
//!
//! This module provides a schema system that describes the structure,
//! types, and requirements of parameters for different premise types.

use crate::{Cardinality, Requirement, Type};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schema that maps parameter names to their constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Schema {
    constraints: HashMap<String, Constraint>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            constraints: HashMap::new(),
        }
    }

    pub fn insert(&mut self, name: String, constraint: Constraint) {
        self.constraints.insert(name, constraint);
    }

    pub fn contains(&self, name: &str) -> bool {
        self.constraints.contains_key(name)
    }

    pub fn get(&self, name: &str) -> Option<&Constraint> {
        self.constraints.get(name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Constraint)> {
        self.constraints.iter()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&String, &mut Constraint)> {
        self.constraints.iter_mut()
    }

    pub fn len(&self) -> usize {
        self.constraints.len()
    }

    pub fn is_empty(&self) -> bool {
        self.constraints.is_empty()
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

/// Constraint descriptor - describes a parameter's type and requirement
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Constraint {
    pub description: String,
    pub content_type: Option<Type>,
    pub requirement: Requirement,
    pub cardinality: Cardinality,
}

impl Constraint {
    pub fn new(description: String, content_type: Option<Type>, requirement: Requirement) -> Self {
        Self {
            description,
            content_type,
            requirement,
            cardinality: Cardinality::One,
        }
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn content_type(&self) -> Option<Type> {
        self.content_type
    }

    pub fn requirement(&self) -> &Requirement {
        &self.requirement
    }

    pub fn cardinality(&self) -> Cardinality {
        self.cardinality
    }
}
