//! Constraint system for query variables
//!
//! Constraints express relationships between terms that must be satisfied during
//! query evaluation. Unlike applications which query the knowledge base, constraints
//! operate on variable bindings to filter, infer, or validate values.

pub mod coalesce;
pub mod compare;
pub mod equality;
pub mod starts_with;
pub mod type_of;

use std::fmt;

pub use coalesce::Coalesce;
pub use compare::{AtLeast, AtMost, GreaterThan, LessThan};
pub use equality::Equality;
pub use starts_with::StartsWith;
pub use type_of::TypeOf;

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
    /// Type predicate — the subject's value inhabits a kind
    /// (`?x.text()`, `?x.number()`). Occurrence typing as a premise.
    #[serde(rename = "type")]
    TypeOf(TypeOf),
    /// Prefix predicate — the subject's lexical form begins with a
    /// string prefix, over the TEXTUAL kinds.
    #[serde(rename = "starts-with")]
    StartsWith(StartsWith),
    /// Range predicate — strictly less than, over the COMPARABLE
    /// kinds.
    #[serde(rename = "<")]
    LessThan(LessThan),
    /// Range predicate — less than or equal, over the COMPARABLE
    /// kinds.
    #[serde(rename = "<=")]
    AtMost(AtMost),
    /// Range predicate — strictly greater than, over the COMPARABLE
    /// kinds.
    #[serde(rename = ">")]
    GreaterThan(GreaterThan),
    /// Range predicate — greater than or equal, over the COMPARABLE
    /// kinds.
    #[serde(rename = ">=")]
    AtLeast(AtLeast),
}

impl Constraint {
    /// Returns the schema for this constraint.
    pub fn schema(&self) -> Schema {
        match self {
            Constraint::Equality(c) => c.schema(),
            Constraint::Coalesce(c) => c.schema(),
            Constraint::TypeOf(c) => c.schema(),
            Constraint::StartsWith(c) => c.schema(),
            Constraint::LessThan(c) => c.schema(),
            Constraint::AtMost(c) => c.schema(),
            Constraint::GreaterThan(c) => c.schema(),
            Constraint::AtLeast(c) => c.schema(),
        }
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Constraint::Equality(c) => c.estimate(env),
            Constraint::Coalesce(c) => c.estimate(env),
            Constraint::TypeOf(c) => c.estimate(env),
            Constraint::StartsWith(c) => c.estimate(env),
            Constraint::LessThan(c) => c.estimate(env),
            Constraint::AtMost(c) => c.estimate(env),
            Constraint::GreaterThan(c) => c.estimate(env),
            Constraint::AtLeast(c) => c.estimate(env),
        }
    }

    /// Returns the parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        match self {
            Constraint::Equality(c) => c.parameters(),
            Constraint::Coalesce(c) => c.parameters(),
            Constraint::TypeOf(c) => c.parameters(),
            Constraint::StartsWith(c) => c.parameters(),
            Constraint::LessThan(c) => c.parameters(),
            Constraint::AtMost(c) => c.parameters(),
            Constraint::GreaterThan(c) => c.parameters(),
            Constraint::AtLeast(c) => c.parameters(),
        }
    }

    /// Evaluates the constraint against the current selection of matches.
    #[auto_enum(futures03::Stream)]
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        match self {
            Constraint::Equality(c) => c.evaluate(selection),
            Constraint::Coalesce(c) => c.evaluate(selection),
            Constraint::TypeOf(c) => c.evaluate(selection),
            Constraint::StartsWith(c) => c.evaluate(selection),
            Constraint::LessThan(c) => c.evaluate(selection),
            Constraint::AtMost(c) => c.evaluate(selection),
            Constraint::GreaterThan(c) => c.evaluate(selection),
            Constraint::AtLeast(c) => c.evaluate(selection),
        }
    }
}

impl Display for Constraint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constraint::Equality(c) => Display::fmt(c, f),
            Constraint::Coalesce(c) => Display::fmt(c, f),
            Constraint::TypeOf(c) => Display::fmt(c, f),
            Constraint::StartsWith(c) => Display::fmt(c, f),
            Constraint::LessThan(c) => Display::fmt(c, f),
            Constraint::AtMost(c) => Display::fmt(c, f),
            Constraint::GreaterThan(c) => Display::fmt(c, f),
            Constraint::AtLeast(c) => Display::fmt(c, f),
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
