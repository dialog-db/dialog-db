//! Numeric range predicates: `<`, `<=`, `>`, `>=` as premises.
//!
//! Four constraints over the NUMERIC kinds, sharing one comparison
//! core. Like every scalar premise they *filter*: a row whose sides
//! cannot be ordered — a non-numeric value, a mixed-type pair, a
//! NaN — is a non-match, never an error and never a coercion. The
//! same strict no-promotion semantics as formula arithmetic
//! (`notes/formula-schemes.md`), with the same release valve: a
//! *constant* side is a polymorphic literal that adapts losslessly
//! to the data's type per row (`1` compares against floats as
//! `1.0`; `1.5` against integer data is a non-match because no
//! integer is `1.5`). Data is never adapted.
//!
//! Inference: both slots carry the NUMERIC bound, narrowing the
//! variables on use. The slots are not yet *linked* (a comparison
//! does not force its sides to one instantiation the way a formula
//! scheme does); the planned per-atom interval refinements will
//! ride these same predicates into scan-range pushdown.

use std::cmp::Ordering;
use std::fmt;
use std::fmt::Display;
use std::ops::Not;

use crate::artifact::Value;
use crate::error::EvaluationError;
use crate::formula::number::Numeric;
use crate::selection::Selection;
use crate::type_system::Primitive;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{Binding, Cardinality, Environment, Field, Parameters, Requirement, Schema, Term};
use crate::{Constraint, Negation, Premise, Proposition, try_stream};

/// Cost for evaluating a comparison (single row lookup + ordering).
const COMPARE_COST: usize = 1;

/// Order two values for comparison, adapting literal sides.
///
/// `None` is a non-match: a non-numeric value, a NaN, or a
/// mixed-type pair neither side of which is a literal that adapts
/// losslessly to the other's type.
fn ordering(
    of: &Value,
    of_is_literal: bool,
    with: &Value,
    with_is_literal: bool,
) -> Option<Ordering> {
    let of = Numeric::try_from(of.clone()).ok()?;
    let with = Numeric::try_from(with.clone()).ok()?;
    if of.value_type() == with.value_type() {
        return of.compare(with);
    }
    if with_is_literal && let Some(adapted) = with.instantiate(of.value_type()) {
        return of.compare(adapted);
    }
    if of_is_literal && let Some(adapted) = of.instantiate(with.value_type()) {
        return adapted.compare(with);
    }
    None
}

macro_rules! define_comparison {
    (
        $(#[$doc:meta])*
        $name:ident, $symbol:literal, [$($accepts:pat),+], $sugar:ident,
        lower: $lower:literal, inclusive: $inclusive:literal
    ) => {
        $(#[$doc])*
        ///
        /// Constructed via the [`Term`] sugar of the same name.
        #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
        pub struct $name {
            /// The left side of the comparison.
            pub of: Term<Any>,
            /// The right side of the comparison.
            pub with: Term<Any>,
        }

        impl $name {
            /// Create a comparison over the given sides.
            pub fn new(of: Term<Any>, with: Term<Any>) -> Self {
                Self { of, with }
            }

            /// Schema: both sides are *hard* requirements (the
            /// predicate consumes bound values; the planner orders
            /// it after the premises binding them), bounded NUMERIC.
            ///
            /// A constant side additionally proves an interval bound
            /// on the OTHER side, carried as a
            /// [`Refinement`](crate::type_system::Refinement) — how
            /// the bound travels through inference to the scan-range
            /// pushdown. The interval records the literal's own type;
            /// consumers must honor the per-row literal adaptation
            /// (see the pushdown's single-numeric-type gate).
            pub fn schema(&self) -> Schema {
                let numeric = Kind::from(Primitive::NUMERIC);
                let (of_content, with_content) =
                    match (self.of.as_constant(), self.with.as_constant()) {
                        // `of REL bound`: the bound constrains `of` on
                        // this relation's own side.
                        (None, Some(bound)) => (
                            numeric
                                .clone()
                                .with_interval(bound, $inclusive, $lower)
                                .unwrap_or_else(|| numeric.clone()),
                            numeric.clone(),
                        ),
                        // `bound REL with`: mirrored — the bound sits on
                        // the opposite side of `with`.
                        (Some(bound), None) => (
                            numeric.clone(),
                            numeric
                                .clone()
                                .with_interval(bound, $inclusive, !$lower)
                                .unwrap_or_else(|| numeric.clone()),
                        ),
                        _ => (numeric.clone(), numeric.clone()),
                    };
                let mut schema = Schema::new();
                schema.insert(
                    "of".to_string(),
                    Field {
                        description: "Left side of the comparison".to_string(),
                        content_type: Some(of_content),
                        requirement: Requirement::required(),
                        cardinality: Cardinality::One,
                    },
                );
                schema.insert(
                    "with".to_string(),
                    Field {
                        description: "Right side of the comparison".to_string(),
                        content_type: Some(with_content),
                        requirement: Requirement::required(),
                        cardinality: Cardinality::One,
                    },
                );
                schema
            }

            /// Estimate cost. Constant — a row-local ordering.
            pub fn estimate(&self, _env: &Environment) -> Option<usize> {
                Some(COMPARE_COST)
            }

            /// Returns the named parameters for this constraint.
            pub fn parameters(&self) -> Parameters {
                let mut params = Parameters::new();
                params.insert("of".to_string(), self.of.clone());
                params.insert("with".to_string(), self.with.clone());
                params
            }

            /// Evaluate: filter rows whose sides do not stand in the
            /// relation — or cannot be ordered at all (non-numeric,
            /// mixed types with no adaptable literal, NaN). An
            /// `Absent` side matches nothing; an unbound side is a
            /// planner-contract violation, surfaced as an error.
            pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
                let of = self.of;
                let with = self.with;
                let of_is_literal = of.is_constant();
                let with_is_literal = with.is_constant();
                try_stream! {
                    for await candidate in selection {
                        let base = candidate?;
                        match (base.lookup(&of), base.lookup(&with)) {
                            (Ok(Binding::Present(left)), Ok(Binding::Present(right))) => {
                                if let Some(order) =
                                    ordering(&left, of_is_literal, &right, with_is_literal)
                                    && matches!(order, $($accepts)|+)
                                {
                                    yield base;
                                }
                            }
                            (Ok(_), Ok(_)) => {}
                            (Err(_), _) => {
                                Err(EvaluationError::UnboundVariable {
                                    variable_name: of.name().unwrap_or("of").to_string(),
                                })?;
                            }
                            (_, Err(_)) => {
                                Err(EvaluationError::UnboundVariable {
                                    variable_name: with.name().unwrap_or("with").to_string(),
                                })?;
                            }
                        }
                    }
                }
            }
        }

        impl Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{} {} {}", self.of, $symbol, self.with)
            }
        }

        impl From<$name> for Constraint {
            fn from(predicate: $name) -> Self {
                Constraint::$name(predicate)
            }
        }

        impl Not for $name {
            type Output = Premise;

            fn not(self) -> Self::Output {
                Premise::Unless(Negation::not(Proposition::Constraint(
                    Constraint::$name(self),
                )))
            }
        }

        impl Term<Any> {
            $(#[$doc])*
            pub fn $sugar(self, with: impl Into<Term<Any>>) -> Premise {
                Premise::Assert(Proposition::Constraint(Constraint::$name(
                    $name::new(self, with.into()),
                )))
            }
        }
    };
}

define_comparison!(
    /// Range predicate: `of` is strictly less than `with`.
    LessThan, "<", [Ordering::Less], less_than,
    lower: false, inclusive: false
);

define_comparison!(
    /// Range predicate: `of` is less than or equal to `with`.
    AtMost, "<=", [Ordering::Less, Ordering::Equal], at_most,
    lower: false, inclusive: true
);

define_comparison!(
    /// Range predicate: `of` is strictly greater than `with`.
    GreaterThan, ">", [Ordering::Greater], greater_than,
    lower: true, inclusive: false
);

define_comparison!(
    /// Range predicate: `of` is greater than or equal to `with`.
    AtLeast, ">=", [Ordering::Greater, Ordering::Equal], at_least,
    lower: true, inclusive: true
);

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::artifact::{Type as ValueType, decode_value};
    use crate::rule::TypeEnv;
    use crate::selection::Match;
    use crate::types::Scalar;
    use futures_util::TryStreamExt;

    async fn count(premise: LessThan, x: Value) -> Result<usize, EvaluationError> {
        let mut row = Match::new();
        row.bind(&Term::var("x"), x)?;
        let results: Vec<Match> = premise.evaluate(row.seed()).try_collect().await?;
        Ok(results.len())
    }

    fn below(limit: impl Scalar) -> LessThan {
        LessThan::new(Term::var("x"), Term::constant(limit))
    }

    /// Same-type sides order by value; each relation accepts its
    /// orderings.
    #[dialog_common::test]
    async fn it_orders_within_one_type() -> Result<(), EvaluationError> {
        assert_eq!(count(below(5u64), Value::UnsignedInt(3)).await?, 1);
        assert_eq!(count(below(5u64), Value::UnsignedInt(5)).await?, 0);
        assert_eq!(count(below(5u64), Value::UnsignedInt(7)).await?, 0);

        let at_most = AtMost::new(Term::var("x"), Term::constant(5u64));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::UnsignedInt(5))?;
        let results: Vec<Match> = at_most.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 1, "<= accepts equality");

        let at_least = AtLeast::new(Term::var("x"), Term::constant(-2i64));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::SignedInt(-1))?;
        let results: Vec<Match> = at_least.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 1, "signed comparison orders negatives");

        let greater = GreaterThan::new(Term::var("x"), Term::constant(0.5f64));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::Float(1.5))?;
        let results: Vec<Match> = greater.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 1, "float comparison orders");
        Ok(())
    }

    /// A literal side adapts losslessly to the data's type; data is
    /// never adapted, so an unadaptable literal is a non-match even
    /// when the mathematical ordering is defined.
    #[dialog_common::test]
    async fn it_adapts_literals_losslessly() -> Result<(), EvaluationError> {
        assert_eq!(
            count(below(1u64), Value::Float(0.5)).await?,
            1,
            "integer literal compares against float data as 1.0"
        );
        assert_eq!(
            count(below(1.5f64), Value::UnsignedInt(1)).await?,
            0,
            "no integer is 1.5: the literal cannot adapt, non-match"
        );
        assert_eq!(
            count(below(2u64), Value::SignedInt(-3)).await?,
            1,
            "unsigned literal adapts to signed data"
        );
        Ok(())
    }

    /// Two data sides of different numeric types are a non-match —
    /// strict no-promotion, like formula arithmetic.
    #[dialog_common::test]
    async fn it_filters_mixed_data() -> Result<(), EvaluationError> {
        let premise = LessThan::new(Term::var("x"), Term::var("y"));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::UnsignedInt(1))?;
        row.bind(&Term::var("y"), Value::Float(2.0))?;
        let results: Vec<Match> = premise.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "neither side is a literal: no adaptation");
        Ok(())
    }

    /// Values outside NUMERIC, NaN, and Absent are non-matches.
    #[dialog_common::test]
    async fn it_filters_unorderable_rows() -> Result<(), EvaluationError> {
        assert_eq!(count(below(5u64), Value::String("3".into())).await?, 0);
        assert_eq!(count(below(f64::MAX), Value::Float(f64::NAN)).await?, 0);

        let premise = AtLeast::new(Term::var("x"), Term::constant(f64::NAN));
        let mut row = Match::new();
        row.bind(&Term::var("x"), Value::Float(1.0))?;
        let results: Vec<Match> = premise.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "NaN stands in no relation");

        let premise = below(5u64);
        let mut row = Match::new();
        row.bind_absent(&Term::var("x"))?;
        let results: Vec<Match> = premise.evaluate(row.seed()).try_collect().await?;
        assert_eq!(results.len(), 0, "Absent matches nothing scalar");
        Ok(())
    }

    /// An unbound side is a planner-contract violation.
    #[dialog_common::test]
    async fn it_errors_on_unbound_sides() {
        let premise = below(5u64);
        let results: Result<Vec<Match>, _> =
            premise.evaluate(Match::new().seed()).try_collect().await;
        assert!(results.is_err(), "unbound left side must error");
    }

    /// Comparisons narrow both sides to NUMERIC rule-wide.
    #[dialog_common::test]
    fn it_bounds_both_sides_numeric() -> anyhow::Result<()> {
        let premises = vec![Term::<Any>::var("x").less_than(Term::<Any>::var("y"))];
        let env = TypeEnv::infer(&premises)?;
        for var in ["x", "y"] {
            assert_eq!(
                env.get(var).expect("inferred").primitive_part(),
                Primitive::NUMERIC,
                "{var} is bounded by the comparison"
            );
        }
        Ok(())
    }

    /// A constant side proves an interval on the variable side,
    /// carried through inference as a refinement — the vehicle for
    /// scan-range pushdown. The mirrored operand order flips the
    /// bound onto the opposite side of the relation.
    #[dialog_common::test]
    fn it_stamps_interval_refinements_via_inference() -> anyhow::Result<()> {
        // `x >= 5`: an inclusive lower bound on `x`.
        let premises = vec![Term::<Any>::var("x").at_least(Term::constant(5u64))];
        let env = TypeEnv::infer(&premises)?;
        let interval = env
            .get("x")
            .expect("inferred")
            .refinement()
            .expect("refined")
            .interval
            .clone()
            .expect("interval recorded");
        assert_eq!(interval.value_type, ValueType::UnsignedInt);
        let lower = interval.lower.expect("lower bound");
        assert!(lower.inclusive, ">= is inclusive");
        assert!(interval.upper.is_none());
        assert_eq!(
            decode_value(ValueType::UnsignedInt, &lower.encoded).map(|(value, _)| value),
            Some(Value::UnsignedInt(5)),
            "the bound round-trips through the order-preserving encoding"
        );

        // `5 < x` mirrors to an exclusive lower bound on `x`.
        let premises = vec![Term::<Any>::constant(5u64).less_than(Term::<Any>::var("x"))];
        let env = TypeEnv::infer(&premises)?;
        let interval = env
            .get("x")
            .expect("inferred")
            .refinement()
            .expect("refined")
            .interval
            .clone()
            .expect("interval recorded");
        let lower = interval
            .lower
            .expect("the mirrored bound lands on the lower side");
        assert!(!lower.inclusive, "a strict relation stays strict");
        assert!(interval.upper.is_none());

        // `x <= 9.5`: an inclusive upper bound recording the
        // literal's own type.
        let premises = vec![Term::<Any>::var("x").at_most(Term::constant(9.5f64))];
        let env = TypeEnv::infer(&premises)?;
        let interval = env
            .get("x")
            .expect("inferred")
            .refinement()
            .expect("refined")
            .interval
            .clone()
            .expect("interval recorded");
        assert_eq!(interval.value_type, ValueType::Float);
        assert!(interval.lower.is_none());
        assert!(interval.upper.expect("upper bound").inclusive);
        Ok(())
    }

    /// Two variable sides prove no interval: the bound must be a
    /// constant the schema can encode.
    #[dialog_common::test]
    fn it_stamps_no_interval_without_a_constant_side() -> anyhow::Result<()> {
        let premises = vec![Term::<Any>::var("x").less_than(Term::<Any>::var("y"))];
        let env = TypeEnv::infer(&premises)?;
        for var in ["x", "y"] {
            assert!(
                env.get(var).expect("inferred").refinement().is_none(),
                "{var} carries no interval from a variable-variable comparison"
            );
        }
        Ok(())
    }

    /// A side already known non-numeric is a compile-time conflict.
    #[dialog_common::test]
    fn it_rejects_known_non_numeric_sides() {
        let premises = vec![
            Term::<Any>::var("x").text(),
            Term::<Any>::var("x").less_than(Term::constant(5u64)),
        ];
        assert!(
            TypeEnv::infer(&premises).is_err(),
            "String and NUMERIC have an empty meet"
        );
    }
}
