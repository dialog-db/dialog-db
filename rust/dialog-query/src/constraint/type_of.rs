//! Type predicate — occurrence typing as a premise.
//!
//! `TypeOf` asserts that a term's value inhabits a kind:
//! `?x.text()`, `?x.number()`, `?x.entity()`. Two effects, one
//! declaration:
//!
//! - **Inference**: the predicate's schema carries the kind as its
//!   slot content type, so rule-level inference narrows the
//!   variable — every premise reading `?x` afterwards (including
//!   the scans feeding it) sees the narrowed kind, and a conflict
//!   with another slot's kind is a compile-time error.
//! - **Evaluation**: a row whose value does not inhabit the kind is
//!   a non-match — filtered, never an error, like every scalar
//!   slot.
//!
//! The planned range predicates (`starts-with` over TEXTUAL,
//! numeric comparisons) extend this same shape with a refinement
//! payload.

use std::fmt;
use std::fmt::Display;
use std::ops::Not;

use crate::artifact::Type as ValueType;
use crate::error::EvaluationError;
use crate::selection::Selection;
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{Binding, Cardinality, Environment, Field, Parameters, Requirement, Schema, Term};
use crate::{Constraint, Negation, Premise, Proposition, try_stream};

/// Cost for evaluating a type predicate (single row lookup + bitmask
/// test).
const TYPE_OF_COST: usize = 1;

/// Type predicate: the value of `of` inhabits the kind `is`.
///
/// Constructed via the [`Term`] sugar — [`Term::text`],
/// [`Term::number`], [`Term::textual`], … — or directly for an
/// arbitrary kind.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TypeOf {
    /// The subject term.
    pub of: Term<Any>,
    /// The kind the subject's value must inhabit.
    pub is: Kind,
}

impl TypeOf {
    /// Create a type predicate over the given term and kind.
    pub fn new(of: Term<Any>, is: Kind) -> Self {
        Self { of, is }
    }

    /// Schema: the subject is a *hard* requirement (the predicate
    /// consumes a bound value; the planner orders it after the
    /// premise binding it), and the slot's content type is the
    /// demanded kind — which is how the narrowing reaches
    /// inference.
    pub fn schema(&self) -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            "of".to_string(),
            Field {
                description: "Term whose value must inhabit the kind".to_string(),
                content_type: Some(self.is.clone()),
                requirement: Requirement::required(),
                cardinality: Cardinality::One,
            },
        );
        schema
    }

    /// Estimate cost. Constant — a row-local bitmask test.
    pub fn estimate(&self, _env: &Environment) -> Option<usize> {
        Some(TYPE_OF_COST)
    }

    /// Returns the named parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        params.insert("of".to_string(), self.of.clone());
        params
    }

    /// Evaluate: filter rows whose value does not inhabit the kind.
    ///
    /// - `Present(v)`: yield iff the kind admits `v`.
    /// - `Absent`: yield iff the kind explicitly admits `Nothing`
    ///   (a scalar kind matches nothing against a claimed absence).
    /// - Unbound: a planner-contract violation — the schema
    ///   hard-requires the subject — surfaced as an error.
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        let of = self.of;
        let kind = self.is;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                match base.lookup(&of) {
                    Ok(Binding::Present(value)) => {
                        if kind.admits(&value) {
                            yield base;
                        }
                    }
                    Ok(Binding::Absent) => {
                        if kind.is_optional() {
                            yield base;
                        }
                    }
                    Err(_) => {
                        Err(EvaluationError::UnboundVariable {
                            variable_name: of.name().unwrap_or("of").to_string(),
                        })?;
                    }
                }
            }
        }
    }
}

impl Display for TypeOf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "type({}) == {}", self.of, self.is)
    }
}

impl Term<Any> {
    /// Type predicate: this term's value inhabits `kind`.
    pub fn typed_as(self, kind: Kind) -> Premise {
        Premise::Assert(Proposition::Constraint(Constraint::TypeOf(TypeOf::new(
            self, kind,
        ))))
    }

    /// `?x` is a string.
    pub fn text(self) -> Premise {
        self.typed_as(Kind::from(ValueType::String))
    }

    /// `?x` is textual: a string, a symbol, or an entity — the kinds
    /// a lexical prefix can range over.
    pub fn textual(self) -> Premise {
        self.typed_as(Kind::from(Primitive::TEXTUAL))
    }

    /// `?x` is numeric: an unsigned integer, a signed integer, or a
    /// float.
    pub fn number(self) -> Premise {
        self.typed_as(Kind::from(Primitive::NUMERIC))
    }

    /// `?x` is an entity.
    pub fn entity(self) -> Premise {
        self.typed_as(Kind::from(ValueType::Entity))
    }

    /// `?x` is a symbol (an attribute name).
    pub fn symbol(self) -> Premise {
        self.typed_as(Kind::from(ValueType::Symbol))
    }

    /// `?x` is a boolean.
    pub fn boolean(self) -> Premise {
        self.typed_as(Kind::from(ValueType::Boolean))
    }

    /// `?x` is bytes.
    pub fn bytes(self) -> Premise {
        self.typed_as(Kind::from(ValueType::Bytes))
    }
}

impl From<TypeOf> for Constraint {
    fn from(predicate: TypeOf) -> Self {
        Constraint::TypeOf(predicate)
    }
}

impl Not for TypeOf {
    type Output = Premise;

    fn not(self) -> Self::Output {
        Premise::Unless(Negation::not(Proposition::Constraint(Constraint::TypeOf(
            self,
        ))))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Value;
    use crate::selection::Match;
    use futures_util::TryStreamExt;

    /// Present values filter by kind membership.
    #[dialog_common::test]
    async fn it_filters_by_kind() -> Result<(), EvaluationError> {
        let predicate = TypeOf::new(Term::var("x"), Kind::from(ValueType::String));

        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::String("hi".into()))?;
        let results: Vec<Match> = predicate.clone().evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 1, "a String inhabits text");

        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::UnsignedInt(7))?;
        let results: Vec<Match> = predicate.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "a number does not inhabit text");
        Ok(())
    }

    /// An Absent binding matches nothing in a scalar kind.
    #[dialog_common::test]
    async fn it_filters_absent_for_scalar_kinds() -> Result<(), EvaluationError> {
        let predicate = TypeOf::new(Term::var("x"), Kind::from(ValueType::String));
        let mut row = Match::new();
        row.bind_absent(&Term::var("x"))?;
        let results: Vec<Match> = predicate.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "Absent matches nothing scalar");
        Ok(())
    }

    /// An unbound subject is a planner-contract violation.
    #[dialog_common::test]
    async fn it_errors_on_unbound_subject() {
        let predicate = TypeOf::new(Term::var("x"), Kind::from(ValueType::String));
        let results: Result<Vec<Match>, _> =
            predicate.evaluate(Match::new().seed()).try_collect().await;
        assert!(results.is_err(), "unbound subject must error");
    }
}
