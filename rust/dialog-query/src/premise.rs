//! Premise trait for rule conditions
//!
//! This module defines the premise system used in rule conditions. Premises represent
//! patterns that can be matched against facts in the knowledge base during rule evaluation.
//!
//! Note: Premises are only used in rule conditions (the "when" part), not in conclusions.

pub use super::negation::Negation;
use crate::constraint::Constraint;
use crate::environment::Environment;
pub use crate::error::{AnalyzerError, QueryResult};
use crate::formula::query::FormulaQuery;
use crate::proposition::Proposition;
use crate::{Parameters, Schema};
use std::fmt::{self, Display};
use std::ops;

/// A single condition in a deductive rule's body.
///
/// Rules are built from an ordered sequence of premises. During query
/// planning each premise is wrapped in an [`Candidate`](crate::Candidate)
/// to determine whether it can execute given the current variable bindings.
/// At execution time, premises are evaluated in the order chosen by the
/// planner: each premise receives the stream of [`Match`](crate::selection::Match)s
/// produced so far and extends it with new bindings.
///
/// There are two kinds of premise:
/// - `When`: queries the knowledge base or applies a constraint via a
///   [`Proposition`] (fact, concept, formula, or constraint).
/// - `Unless`: a [`Negation`] that *excludes* matches matching a pattern.
#[derive(Debug, Clone, PartialEq)]
pub enum Premise {
    /// A positive premise that queries the knowledge base or applies a constraint.
    Assert(Proposition),
    /// A negated premise that excludes matches from the selection.
    Unless(Negation),
}

impl Premise {
    /// Estimate the cost of this premise given the current environment.
    /// Returns None if the premise cannot be executed without more constraints.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Premise::Assert(application) => application.estimate(env),
            Premise::Unless(negation) => negation.estimate(env),
        }
    }

    /// Returns the parameter bindings for this premise
    pub fn parameters(&self) -> Parameters {
        match self {
            Premise::Assert(application) => application.parameters(),
            Premise::Unless(negation) => negation.parameters(),
        }
    }

    /// Returns the schema describing this premise's parameters
    pub fn schema(&self) -> Schema {
        match self {
            Premise::Assert(application) => application.schema(),
            Premise::Unless(negation) => negation.schema(),
        }
    }
}

impl Display for Premise {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Premise::Assert(application) => Display::fmt(&application, f),
            Premise::Unless(negation) => Display::fmt(&negation, f),
        }
    }
}

impl ops::Not for Premise {
    type Output = Premise;

    fn not(self) -> Self::Output {
        match self {
            Premise::Assert(proposition) => Premise::Unless(Negation::not(proposition)),
            Premise::Unless(Negation(proposition)) => Premise::Assert(proposition),
        }
    }
}

impl From<Constraint> for Premise {
    fn from(constraint: Constraint) -> Self {
        Premise::Assert(Proposition::Constraint(constraint))
    }
}

impl From<FormulaQuery> for Premise {
    fn from(application: FormulaQuery) -> Self {
        Premise::Assert(Proposition::Formula(application))
    }
}

/// A premise reading `the` of `this` into `is` as the formal notation
/// spells it: a concept premise over the attribute concept, read under
/// `cardinality`'s policy (`all` when none is given). Tests build rules
/// with it, since every installed rule is written in the formal
/// notation, where a raw attribute scan has no spelling.
#[cfg(test)]
pub(crate) fn reading(
    the: crate::attribute::The,
    this: crate::Term<crate::Entity>,
    is: crate::Term<crate::types::Any>,
    cardinality: Option<crate::attribute::Cardinality>,
) -> Premise {
    let the = the
        .as_constant()
        .expect("a premise reads a named attribute")
        .clone();
    let field =
        crate::ConceptFieldDescriptor::required(crate::attribute::AttributeDescriptor::new(
            the,
            "",
            cardinality.unwrap_or(crate::attribute::Cardinality::Many),
            None,
        ));
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), this.into());
    terms.insert(crate::ConceptDescriptor::VALUE.to_string(), is);
    Premise::Assert(Proposition::Concept(crate::concept::query::ConceptQuery {
        predicate: crate::ConceptDescriptor::of_attribute(&field),
        terms,
    }))
}
