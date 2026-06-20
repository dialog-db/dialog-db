//! Constraint system for query variables
//!
//! Constraints express relationships between terms that must be satisfied during
//! query evaluation. Unlike applications which query the knowledge base, constraints
//! operate on variable bindings to filter, infer, or validate values.

pub mod coalesce;
pub mod equality;

use std::fmt;

pub use coalesce::Coalesce;
pub use equality::Equality;

use crate::selection::Selection;
use crate::{Environment, Parameters, Schema};
use auto_enums::auto_enum;
use std::fmt::Display;

/// Constraint enum representing different types of constraints between terms.
///
/// Constraints express relationships that variables must satisfy. They support
/// bidirectional inference when possible, meaning if one term is bound, the other
/// can often be inferred from the constraint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "assert", content = "where")]
pub enum Constraint {
    /// Equality constraint between two terms.
    #[serde(rename = "==")]
    Equality(Equality),
    /// Coalesce constraint: bind `is` from `source` when Present,
    /// else from `fallback`. Set-widening unwrap.
    #[serde(rename = "coalesce")]
    Coalesce(Coalesce),
}

impl Constraint {
    /// Returns the schema for this constraint.
    pub fn schema(&self) -> Schema {
        match self {
            Constraint::Equality(c) => c.schema(),
            Constraint::Coalesce(c) => c.schema(),
        }
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Constraint::Equality(c) => c.estimate(env),
            Constraint::Coalesce(c) => c.estimate(env),
        }
    }

    /// Returns the parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        match self {
            Constraint::Equality(c) => c.parameters(),
            Constraint::Coalesce(c) => c.parameters(),
        }
    }

    /// Evaluates the constraint against the current selection of matches.
    #[auto_enum(futures03::Stream)]
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        match self {
            Constraint::Equality(c) => c.evaluate(selection),
            Constraint::Coalesce(c) => c.evaluate(selection),
        }
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::Equality(c) => Display::fmt(c, f),
            Constraint::Coalesce(c) => Display::fmt(c, f),
        }
    }
}

impl From<Equality> for Constraint {
    fn from(constraint: Equality) -> Self {
        Constraint::Equality(constraint)
    }
}

impl From<Coalesce> for Constraint {
    fn from(constraint: Coalesce) -> Self {
        Constraint::Coalesce(constraint)
    }
}
