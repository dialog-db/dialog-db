//! Constraint system for query variables
//!
//! Constraints express relationships between terms that must be satisfied during
//! query evaluation. Unlike applications which query the knowledge base, constraints
//! operate on variable bindings to filter, infer, or validate values.

pub mod coalesce;
pub mod equality;
pub mod type_of;

use std::fmt;

pub use coalesce::Coalesce;
pub use equality::Equality;
pub use type_of::TypeOf;

use crate::selection::Selection;
use crate::{Environment, Parameters, Schema};
use futures_util::future::Either;
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
    /// Coalesce constraint — bind `is` from `source` when Present,
    /// else from `fallback`. Set-widening unwrap.
    #[serde(rename = "coalesce")]
    Coalesce(Coalesce),
    /// Type predicate — the subject's value inhabits a kind
    /// (`?x.text()`, `?x.number()`). Occurrence typing as a premise.
    #[serde(rename = "type")]
    TypeOf(TypeOf),
}

impl Constraint {
    /// Returns the schema for this constraint.
    pub fn schema(&self) -> Schema {
        match self {
            Constraint::Equality(c) => c.schema(),
            Constraint::Coalesce(c) => c.schema(),
            Constraint::TypeOf(c) => c.schema(),
        }
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Constraint::Equality(c) => c.estimate(env),
            Constraint::Coalesce(c) => c.estimate(env),
            Constraint::TypeOf(c) => c.estimate(env),
        }
    }

    /// Returns the parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        match self {
            Constraint::Equality(c) => c.parameters(),
            Constraint::Coalesce(c) => c.parameters(),
            Constraint::TypeOf(c) => c.parameters(),
        }
    }

    /// Evaluates the constraint against the current selection of matches.
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        match self {
            Constraint::Equality(c) => Either::Left(Either::Left(c.evaluate(selection))),
            Constraint::Coalesce(c) => Either::Left(Either::Right(c.evaluate(selection))),
            Constraint::TypeOf(c) => Either::Right(c.evaluate(selection)),
        }
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::Equality(c) => Display::fmt(c, f),
            Constraint::Coalesce(c) => Display::fmt(c, f),
            Constraint::TypeOf(c) => Display::fmt(c, f),
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
