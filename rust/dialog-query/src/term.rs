//! Term types for pattern matching and query construction

use serde::{Deserialize, Serialize};
use dialog_artifacts::Value;
use crate::variable::Variable;

/// Term is either a constant value or a variable placeholder
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Term {
    /// A concrete value
    Constant(Value),
    /// A variable placeholder
    Variable(Variable),
}

impl Term {
    /// Check if this term is a variable
    pub fn is_variable(&self) -> bool {
        matches!(self, Term::Variable(_))
    }

    /// Check if this term is a constant
    pub fn is_constant(&self) -> bool {
        matches!(self, Term::Constant(_))
    }

    /// Get the variable if this term is one
    pub fn as_variable(&self) -> Option<&Variable> {
        match self {
            Term::Variable(var) => Some(var),
            Term::Constant(_) => None,
        }
    }

    /// Get the constant value if this term is one
    pub fn as_constant(&self) -> Option<&Value> {
        match self {
            Term::Constant(value) => Some(value),
            Term::Variable(_) => None,
        }
    }
}

impl From<Value> for Term {
    fn from(value: Value) -> Self {
        Term::Constant(value)
    }
}

impl From<Variable> for Term {
    fn from(variable: Variable) -> Self {
        Term::Variable(variable)
    }
}

impl From<String> for Term {
    fn from(s: String) -> Self {
        Term::Constant(Value::String(s))
    }
}

impl From<&str> for Term {
    fn from(s: &str) -> Self {
        Term::Constant(Value::String(s.to_string()))
    }
}

impl From<dialog_artifacts::Attribute> for Term {
    fn from(attr: dialog_artifacts::Attribute) -> Self {
        Term::Constant(Value::String(attr.to_string()))
    }
}

impl From<dialog_artifacts::Entity> for Term {
    fn from(entity: dialog_artifacts::Entity) -> Self {
        Term::Constant(Value::Entity(entity))
    }
}