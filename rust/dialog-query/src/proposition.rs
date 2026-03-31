use std::fmt;

use crate::attribute::query::AttributeQuery;
use crate::concept::descriptor::ConceptDescriptor;
pub use crate::concept::query::ConceptQuery;
use crate::constraint::Constraint;
pub use crate::error::AnalyzerError;
pub use crate::error::QueryResult;
pub use crate::formula::query::FormulaQuery;
pub use crate::premise::{Negation, Premise};
use crate::query::Application;
use crate::selection::Selection;
use crate::source::SelectRules;
pub use crate::{Environment, Parameters, Schema};
use dialog_artifacts::Select;
use dialog_capability::Provider;
use dialog_common::ConditionalSync;
use futures_util::future::Either;
use serde::de;
use serde::ser;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
pub use std::fmt::Display;

/// A knowledge-base query embedded inside a [`Premise::When`](crate::Premise::When).
///
/// Each variant binds a different kind of application:
/// - `Attribute` — EAV triple lookup against the fact store, with
///   cardinality-aware winner selection.
/// - `Concept` — entity-level query using a concept predicate and its
///   associated deductive rules.
/// - `Formula` — pure computation that derives new bindings from existing
///   ones without touching the fact store.
/// - `Constraint` — pure variable constraint (equality, comparison) that
///   filters or infers bindings without querying stored data.
#[derive(Debug, Clone, PartialEq)]
pub enum Proposition {
    /// Concept realization - matching entities against concept patterns
    Concept(ConceptQuery),
    /// Application of a formula for computation
    Formula(FormulaQuery),
    /// Attribute query — cardinality-aware EAV lookup.
    /// Boxed to reduce enum size.
    Attribute(Box<AttributeQuery>),
    /// Constraint between variables (equality, comparison, etc.)
    Constraint(Constraint),
}

impl Proposition {
    /// Estimate the cost of this application given the current environment.
    /// Each application type knows how to calculate its cost based on what's bound.
    /// Returns None if the application cannot be executed without more constraints.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        match self {
            Proposition::Attribute(query) => query.estimate(env),
            Proposition::Concept(application) => application.estimate(env),
            Proposition::Formula(application) => application.estimate(env),
            Proposition::Constraint(constraint) => constraint.estimate(env),
        }
    }

    /// Evaluate this application against the given context, producing a selection stream
    pub fn evaluate<'a, Env, M: Selection + 'a>(
        self,
        selection: M,
        env: &'a Env,
    ) -> impl Selection + 'a
    where
        Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
    {
        match self {
            Proposition::Attribute(query) => Either::Left(Either::Left(Either::Left(
                Application::evaluate(*query, selection, env),
            ))),
            Proposition::Concept(application) => Either::Left(Either::Left(Either::Right(
                application.evaluate(selection, env),
            ))),
            Proposition::Formula(application) => {
                Either::Left(Either::Right(application.evaluate(selection)))
            }
            Proposition::Constraint(constraint) => Either::Right(constraint.evaluate(selection)),
        }
    }

    /// Returns the parameter bindings for this application
    pub fn parameters(&self) -> Parameters {
        match self {
            Proposition::Attribute(query) => query.parameters(),
            Proposition::Concept(application) => application.parameters(),
            Proposition::Formula(application) => application.parameters(),
            Proposition::Constraint(constraint) => constraint.parameters(),
        }
    }

    /// Returns the schema describing this application's parameters
    pub fn schema(&self) -> Schema {
        match self {
            Proposition::Attribute(query) => query.schema(),
            Proposition::Concept(application) => application.schema(),
            Proposition::Formula(application) => application.schema(),
            Proposition::Constraint(constraint) => constraint.schema(),
        }
    }

    /// Creates a negated premise from this application.
    pub fn not(&self) -> Premise {
        Premise::Unless(Negation::not(self.clone()))
    }
}

impl From<ConceptQuery> for Proposition {
    fn from(selector: ConceptQuery) -> Self {
        Proposition::Concept(selector)
    }
}

impl From<FormulaQuery> for Proposition {
    fn from(application: FormulaQuery) -> Self {
        Proposition::Formula(application)
    }
}

impl Display for Proposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Proposition::Attribute(query) => Display::fmt(query, f),
            Proposition::Concept(application) => Display::fmt(application, f),
            Proposition::Formula(application) => Display::fmt(application, f),
            Proposition::Constraint(constraint) => Display::fmt(constraint, f),
        }
    }
}

impl From<Constraint> for Proposition {
    fn from(constraint: Constraint) -> Self {
        Proposition::Constraint(constraint)
    }
}

impl Serialize for Proposition {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Proposition::Concept(cq) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("assert", &cq.predicate)?;
                map.serialize_entry("where", &cq.terms)?;
                map.end()
            }
            Proposition::Formula(fq) => fq.serialize(serializer),
            Proposition::Constraint(c) => c.serialize(serializer),
            Proposition::Attribute(_) => Err(ser::Error::custom(
                "Attribute propositions cannot be serialized in formal notation",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Proposition {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Deserialize into a raw JSON value first so we can peek at the "assert" field
        let raw: serde_json::Value = serde_json::Value::deserialize(deserializer)?;

        let assert_val = raw
            .get("assert")
            .ok_or_else(|| de::Error::missing_field("assert"))?;

        match assert_val {
            // Object → concept descriptor
            serde_json::Value::Object(_) => {
                let predicate: ConceptDescriptor =
                    serde_json::from_value(assert_val.clone()).map_err(de::Error::custom)?;
                let terms: Parameters = raw
                    .get("where")
                    .ok_or_else(|| de::Error::missing_field("where"))
                    .and_then(|v| serde_json::from_value(v.clone()).map_err(de::Error::custom))?;
                Ok(Proposition::Concept(ConceptQuery { predicate, terms }))
            }
            // String "==" → Constraint
            serde_json::Value::String(name) if name == "==" => {
                let constraint: Constraint =
                    serde_json::from_value(raw).map_err(de::Error::custom)?;
                Ok(Proposition::Constraint(constraint))
            }
            // Other string → FormulaQuery
            serde_json::Value::String(_) => {
                let fq: FormulaQuery = serde_json::from_value(raw).map_err(de::Error::custom)?;
                Ok(Proposition::Formula(fq))
            }
            _ => Err(de::Error::custom(
                "\"assert\" must be a concept object or a formula/constraint name string",
            )),
        }
    }
}

// Serde tests for Proposition are in formula::query::tests
