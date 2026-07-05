use std::fmt;

use crate::artifact::Type as ValueType;
use crate::error::EvaluationError;
use crate::formula::bindings::Bindings;
use crate::formula::cell::Cells;
use crate::formula::conversions::{self, ParseFloat, ParseSignedInteger, ParseUnsignedInteger};
use crate::formula::logic::{And, Not, Or};
use crate::formula::math::{Difference, Modulo, Product, Quotient, Sum};
use crate::formula::number::Numeric;
use crate::formula::string::{Concatenate, Length, Like, Lowercase, Uppercase};
use crate::selection::{Match, Selection};
use crate::term::Term;
use crate::types::Any;
use crate::{Binding, Environment, Formula, Parameters, Schema, Value, try_stream};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

/// Cost per parameter for formula evaluation
pub const PARAM_COST: usize = 10;

/// Defines the [`FormulaQuery`] enum from a list of
/// `"formal/name" => Variant(FormulaType, QueryType)` entries.
///
/// Generates:
/// - The enum with `#[serde(tag = "assert", content = "where")]` and per-variant renames
/// - Per-variant dispatch: `name()`, `cells()`, `resolve()`, `cost()`, `parameters()`
/// - `From<QueryType> for FormulaQuery` for each variant
///
/// The variant payload names the query struct *directly* rather than
/// through the `Query<T>` projection: the projection requires
/// `Predicate`, which (for generic formulas) is conditional on the
/// very `From` impls this macro generates — naming the struct breaks
/// that cycle.
macro_rules! define_formulas {
    ( $( $name:literal => $variant:ident($ty:ty, $q:ty) ),* $(,)? ) => {
        /// A formula premise bound to specific term arguments.
        ///
        /// Each variant wraps the typed `Query<T>` struct (e.g. `SumQuery`) generated
        /// by the `Formula` derive macro. Serializes as `{"assert": "<name>", "where": <params>}`.
        #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
        #[serde(tag = "assert", content = "where")]
        pub enum FormulaQuery {
            $(
                #[doc = concat!("Formula `", $name, "`")]
                #[serde(rename = $name)]
                $variant($q),
            )*
        }

        impl FormulaQuery {
            /// Returns the formal notation name (e.g. `"math/sum"`).
            pub fn name(&self) -> &'static str {
                match self { $( Self::$variant(_) => $name, )* }
            }

            /// Returns the static cell definitions for this formula.
            pub(crate) fn cells(&self) -> &'static Cells {
                match self { $( Self::$variant(_) => <$ty>::cells(), )* }
            }

            /// Runs the formula's resolve logic against the given bindings.
            fn resolve(&self, bindings: &mut Bindings) -> Result<Vec<Match>, EvaluationError> {
                match self { $( Self::$variant(_) => <$ty>::resolve(bindings), )* }
            }

            /// Returns the base cost of evaluating this formula.
            pub fn cost(&self) -> usize {
                match self { $( Self::$variant(_) => <$ty>::cost(), )* }
            }

            /// Returns the parameters for this formula application.
            pub fn parameters(&self) -> Parameters {
                match self { $( Self::$variant(q) => q.clone().into(), )* }
            }
        }

        $(
            impl From<$q> for FormulaQuery {
                fn from(q: $q) -> Self { Self::$variant(q) }
            }
        )*
    };
}

define_formulas! {
    "math/sum"                => Sum(Sum, super::math::SumQuery),
    "math/difference"         => Difference(Difference, super::math::DifferenceQuery),
    "math/product"            => Product(Product, super::math::ProductQuery),
    "math/quotient"           => Quotient(Quotient, super::math::QuotientQuery),
    "math/modulo"             => Modulo(Modulo, super::math::ModuloQuery),
    "text/concatenate"        => Concatenate(Concatenate, super::string::ConcatenateQuery),
    "text/length"             => Length(Length, super::string::LengthQuery),
    "text/upper-case"         => Uppercase(Uppercase, super::string::UppercaseQuery),
    "text/lower-case"         => Lowercase(Lowercase, super::string::LowercaseQuery),
    "text/like"               => Like(Like, super::string::LikeQuery),

    "boolean/and"             => And(And, super::logic::AndQuery),
    "boolean/or"              => Or(Or, super::logic::OrQuery),
    "boolean/not"             => Not(Not, super::logic::NotQuery),

    "text/from"               => ToString(conversions::ToString, conversions::ToStringQuery),
    "unsigned-integer/parse"  => ParseUnsignedInteger(ParseUnsignedInteger, conversions::ParseUnsignedIntegerQuery),
    "signed-integer/parse"    => ParseSignedInteger(ParseSignedInteger, conversions::ParseSignedIntegerQuery),
    "float/parse"             => ParseFloat(ParseFloat, conversions::ParseFloatQuery),
}

impl FormulaQuery {
    /// Returns the schema for this formula.
    pub fn schema(&self) -> Schema {
        self.cells().into()
    }

    /// Estimate the cost of this formula given the current environment.
    pub fn estimate(&self, _env: &Environment) -> Option<usize> {
        Some(self.cost())
    }

    /// Adapt polymorphic literals to the row's scheme instantiation.
    ///
    /// For each scheme group, the row's *variable* members determine
    /// the instantiation (the data's type); *constant* numeric
    /// members — literals — are then converted into it losslessly.
    /// `None` means some literal has no lossless form in the row's
    /// type: the row is a non-match. Data values are never touched —
    /// only literals adapt, which is the strict-data /
    /// polymorphic-literal split (see notes/formula-schemes.md).
    fn adapt_literals(&self, row: &Match) -> Option<Parameters> {
        let mut parameters = self.parameters();
        let mut instantiation: HashMap<&str, ValueType> = HashMap::new();

        // First pass: the row's instantiation per scheme group, from
        // variable members bound to numeric values.
        for (slot, cell) in self.cells().iter() {
            let Some(label) = cell.scheme_label() else {
                continue;
            };
            let Some(term) = parameters.get(slot) else {
                continue;
            };
            if term.name().is_some()
                && let Ok(Binding::Present(value)) = row.lookup(term)
                && Numeric::try_from(value.clone()).is_ok()
            {
                instantiation.entry(label).or_insert(value.data_type());
            }
        }

        if instantiation.is_empty() {
            return Some(parameters);
        }

        // Second pass: adapt constant members into their group's
        // instantiation.
        let mut adapted: Vec<(String, Term<Any>)> = Vec::new();
        for (slot, cell) in self.cells().iter() {
            let Some(label) = cell.scheme_label() else {
                continue;
            };
            let Some(target) = instantiation.get(label) else {
                continue;
            };
            let Some(Term::Constant(value)) = parameters.get(slot) else {
                continue;
            };
            let Ok(literal) = Numeric::try_from(value.clone()) else {
                continue;
            };
            let instantiated = literal.instantiate(*target)?;
            adapted.push((slot.to_string(), Term::Constant(Value::from(instantiated))));
        }
        for (slot, term) in adapted {
            parameters.insert(slot, term);
        }
        Some(parameters)
    }

    /// Computes matches using this formula
    pub fn compute(&self, input: Match) -> Result<Vec<Match>, EvaluationError> {
        let formula = Arc::new(self.clone());
        let Some(parameters) = self.adapt_literals(&input) else {
            // A literal has no lossless form in the row's type: the
            // row is a non-match.
            return Ok(vec![]);
        };
        let mut bindings = Bindings::new(formula, input, parameters);
        self.resolve(&mut bindings)
    }

    /// Expand this formula with the given match, swallowing the
    /// row-local non-matches:
    ///
    /// - a `Conflict` (an output slot is already bound to a different
    ///   value) filters the row: the formula's result disagrees with
    ///   what the row already holds;
    /// - an `Absent` input filters the row: a formula input is a
    ///   scalar slot, and a row claiming "known to have no value"
    ///   matches nothing through it. This is how the
    ///   filter-by-default semantics of optional values holds across
    ///   a concept boundary: the boundary delivers `Absent`, and the
    ///   premise demanding a present value excludes the row instead
    ///   of aborting the stream.
    ///
    /// Anything else (an unbound required input, a type mismatch) is
    /// a genuine evaluation failure and propagates.
    pub fn expand(&self, matched: Match) -> Result<Vec<Match>, EvaluationError> {
        let formula = Arc::new(self.clone());
        let Some(parameters) = self.adapt_literals(&matched) else {
            // A literal has no lossless form in the row's type: the
            // row is a non-match.
            return Ok(vec![]);
        };
        let mut bindings = Bindings::new(formula, matched, parameters);
        match self.resolve(&mut bindings) {
            Ok(output) => Ok(output),
            Err(EvaluationError::Conflict { .. })
            | Err(EvaluationError::Absent { .. })
            // A row value outside an input's type is a row-local
            // non-match (a scalar slot filters), same as at a scan.
            | Err(EvaluationError::TypeMismatch { .. }) => Ok(vec![]),
            Err(e) => Err(e),
        }
    }

    /// Evaluate this formula against the given selection stream
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        let formula = self;
        try_stream! {
            for await candidate in selection {
                for extension in formula.expand(candidate?)? {
                    yield extension;
                }
            }
        }
    }
}

impl Display for FormulaQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let params = self.parameters();
        write!(f, "{} {{", self.name())?;
        for (name, term) in params.iter() {
            write!(f, "{}: {},", name, term)?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Proposition;
    use crate::Query;
    use crate::constraint::Constraint;
    use crate::constraint::coalesce::Coalesce;
    use crate::constraint::equality::Equality;
    use crate::term::Term;
    use crate::types::Any;
    use serde_json::json;

    fn assert_round_trip(fq: FormulaQuery, expected_name: &str, expected_fields: &[&str]) {
        let json = serde_json::to_value(&fq).unwrap();

        assert_eq!(
            json["assert"], expected_name,
            "assert field should match formal name"
        );
        assert!(json["where"].is_object(), "where field should be an object");

        for field in expected_fields {
            assert!(
                json["where"].get(*field).is_some(),
                "where should contain field \"{}\"",
                field
            );
        }

        let deserialized: FormulaQuery = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(fq.name(), deserialized.name());

        let orig_params = fq.parameters();
        let deser_params = deserialized.parameters();
        for field in expected_fields {
            assert_eq!(
                orig_params.get(field).and_then(|t| t.name()),
                deser_params.get(field).and_then(|t| t.name()),
                "variable names should match for field \"{}\"",
                field
            );
        }
    }

    #[test]
    fn it_serializes_sum_formula() {
        let fq = FormulaQuery::Sum(Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::var("result"),
        });
        assert_round_trip(fq, "math/sum", &["of", "with", "is"]);
    }

    #[test]
    fn it_serializes_difference_formula() {
        let fq = FormulaQuery::Difference(Query::<Difference> {
            of: Term::var("a"),
            subtract: Term::var("b"),
            is: Term::var("diff"),
        });
        assert_round_trip(fq, "math/difference", &["of", "subtract", "is"]);
    }

    #[test]
    fn it_serializes_product_formula() {
        let fq = FormulaQuery::Product(Query::<Product> {
            of: Term::var("a"),
            times: Term::var("b"),
            is: Term::var("prod"),
        });
        assert_round_trip(fq, "math/product", &["of", "times", "is"]);
    }

    #[test]
    fn it_serializes_quotient_formula() {
        let fq = FormulaQuery::Quotient(Query::<Quotient> {
            of: Term::var("a"),
            by: Term::var("b"),
            is: Term::var("quot"),
        });
        assert_round_trip(fq, "math/quotient", &["of", "by", "is"]);
    }

    #[test]
    fn it_serializes_modulo_formula() {
        let fq = FormulaQuery::Modulo(Query::<Modulo> {
            of: Term::var("a"),
            by: Term::var("b"),
            is: Term::var("rem"),
        });
        assert_round_trip(fq, "math/modulo", &["of", "by", "is"]);
    }

    #[test]
    fn it_serializes_concatenate_formula() {
        let fq = FormulaQuery::Concatenate(Query::<Concatenate> {
            first: Term::var("left"),
            second: Term::var("right"),
            is: Term::var("joined"),
        });
        assert_round_trip(fq, "text/concatenate", &["first", "second", "is"]);
    }

    #[test]
    fn it_serializes_length_formula() {
        let fq = FormulaQuery::Length(Query::<Length> {
            of: Term::var("text"),
            is: Term::var("len"),
        });
        assert_round_trip(fq, "text/length", &["of", "is"]);
    }

    #[test]
    fn it_serializes_uppercase_formula() {
        let fq = FormulaQuery::Uppercase(Query::<Uppercase> {
            of: Term::var("text"),
            is: Term::var("upper"),
        });
        assert_round_trip(fq, "text/upper-case", &["of", "is"]);
    }

    #[test]
    fn it_serializes_lowercase_formula() {
        let fq = FormulaQuery::Lowercase(Query::<Lowercase> {
            of: Term::var("text"),
            is: Term::var("lower"),
        });
        assert_round_trip(fq, "text/lower-case", &["of", "is"]);
    }

    #[test]
    fn it_serializes_like_formula() {
        let fq = FormulaQuery::Like(Query::<Like> {
            text: Term::var("text"),
            pattern: Term::var("pat"),
            is: Term::var("matches"),
        });
        assert_round_trip(fq, "text/like", &["text", "pattern", "is"]);
    }

    #[test]
    fn it_serializes_to_string_formula() {
        let fq = FormulaQuery::ToString(Query::<conversions::ToString> {
            value: Term::var("value"),
            is: Term::var("text"),
        });
        assert_round_trip(fq, "text/from", &["value", "is"]);
    }

    #[test]
    fn it_serializes_parse_unsigned_integer_formula() {
        let fq = FormulaQuery::ParseUnsignedInteger(Query::<ParseUnsignedInteger> {
            text: Term::var("text"),
            is: Term::var("num"),
        });
        assert_round_trip(fq, "unsigned-integer/parse", &["text", "is"]);
    }

    #[test]
    fn it_serializes_parse_signed_integer_formula() {
        let fq = FormulaQuery::ParseSignedInteger(Query::<ParseSignedInteger> {
            text: Term::var("text"),
            is: Term::var("num"),
        });
        assert_round_trip(fq, "signed-integer/parse", &["text", "is"]);
    }

    #[test]
    fn it_serializes_parse_float_formula() {
        let fq = FormulaQuery::ParseFloat(Query::<ParseFloat> {
            text: Term::var("text"),
            is: Term::var("num"),
        });
        assert_round_trip(fq, "float/parse", &["text", "is"]);
    }

    #[test]
    fn it_serializes_and_formula() {
        let fq = FormulaQuery::And(Query::<And> {
            left: Term::var("a"),
            right: Term::var("b"),
            is: Term::var("result"),
        });
        assert_round_trip(fq, "boolean/and", &["left", "right", "is"]);
    }

    #[test]
    fn it_serializes_or_formula() {
        let fq = FormulaQuery::Or(Query::<Or> {
            left: Term::var("a"),
            right: Term::var("b"),
            is: Term::var("result"),
        });
        assert_round_trip(fq, "boolean/or", &["left", "right", "is"]);
    }

    #[test]
    fn it_serializes_not_formula() {
        let fq = FormulaQuery::Not(Query::<Not> {
            value: Term::var("flag"),
            is: Term::var("negated"),
        });
        assert_round_trip(fq, "boolean/not", &["value", "is"]);
    }

    #[test]
    fn it_parses_formula_from_json() {
        let json = json!({
            "assert": "math/sum",
            "where": {
                "of": { "?": { "name": "x" } },
                "with": { "?": { "name": "y" } },
                "is": { "?": { "name": "result" } }
            }
        });

        let fq: FormulaQuery = serde_json::from_value(json).unwrap();
        assert_eq!(fq.name(), "math/sum");
        let params = fq.parameters();
        assert_eq!(params.get("of").and_then(|t| t.name()), Some("x"));
        assert_eq!(params.get("with").and_then(|t| t.name()), Some("y"));
        assert_eq!(params.get("is").and_then(|t| t.name()), Some("result"));
    }

    #[test]
    fn it_parses_formula_with_constants() {
        let json = json!({
            "assert": "math/sum",
            "where": {
                "of": 5,
                "with": 3,
                "is": { "?": { "name": "result" } }
            }
        });

        let fq: FormulaQuery = serde_json::from_value(json).unwrap();
        assert_eq!(fq.name(), "math/sum");
    }

    #[test]
    fn it_rejects_unknown_formula() {
        let json = json!({
            "assert": "unknown/formula",
            "where": {}
        });

        let result: Result<FormulaQuery, _> = serde_json::from_value(json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("unknown variant"),
            "error should mention unknown variant: {err}"
        );
    }

    #[test]
    fn it_rejects_missing_assert() {
        let json = json!({ "where": {} });
        let result: Result<FormulaQuery, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn it_rejects_missing_where() {
        let json = json!({ "assert": "math/sum" });
        let result: Result<FormulaQuery, _> = serde_json::from_value(json);
        assert!(result.is_err());
    }

    #[test]
    fn it_round_trips_formula_as_proposition() {
        let fq = FormulaQuery::Sum(Query::<Sum> {
            of: Term::var("x"),
            with: Term::var("y"),
            is: Term::var("result"),
        });
        let prop = Proposition::Formula(fq);

        let json = serde_json::to_value(&prop).unwrap();
        assert_eq!(json["assert"], "math/sum");
        assert!(json["where"].is_object());

        let deserialized: Proposition = serde_json::from_value(json).unwrap();
        match &deserialized {
            Proposition::Formula(fq) => {
                assert_eq!(fq.name(), "math/sum");
            }
            other => panic!("Expected Formula, got {:?}", other),
        }
    }

    #[test]
    fn it_round_trips_equality_as_proposition() {
        let eq = Equality::new(Term::<Any>::var("x"), Term::<Any>::var("y"));
        let prop = Proposition::Constraint(Constraint::Equality(eq));

        let json = serde_json::to_value(&prop).unwrap();
        assert_eq!(json["assert"], "==");
        assert!(json["where"]["this"].is_object());
        assert!(json["where"]["is"].is_object());

        let deserialized: Proposition = serde_json::from_value(json).unwrap();
        match &deserialized {
            Proposition::Constraint(Constraint::Equality(eq)) => {
                assert_eq!(eq.this, Term::<Any>::var("x"));
                assert_eq!(eq.is, Term::<Any>::var("y"));
            }
            other => panic!("Expected Constraint(Equality), got {:?}", other),
        }
    }

    /// Coalesce round-trips through Proposition. Regression for
    /// the bug where Proposition::deserialize matched only "==" as
    /// a constraint and routed every other string to FormulaQuery.
    #[dialog_common::test]
    fn it_round_trips_coalesce_as_proposition() {
        let coalesce = Coalesce::new(
            Term::<Any>::var("source"),
            Term::<Any>::var("fallback"),
            Term::<Any>::var("is"),
        );
        let prop = Proposition::Constraint(Constraint::Coalesce(coalesce));

        let json = serde_json::to_value(&prop).unwrap();
        assert_eq!(json["assert"], "coalesce");

        let deserialized: Proposition = serde_json::from_value(json).unwrap();
        match &deserialized {
            Proposition::Constraint(Constraint::Coalesce(c)) => {
                assert_eq!(c.source.name(), Some("source"));
                assert_eq!(c.fallback.name(), Some("fallback"));
                assert_eq!(c.is.name(), Some("is"));
            }
            other => panic!("Expected Constraint(Coalesce), got {:?}", other),
        }
    }

    #[test]
    fn it_round_trips_concept_as_proposition() {
        let json_str = r#"{
            "assert": {
                "with": {
                    "name": { "the": "person/name" },
                    "age": { "the": "person/age" }
                }
            },
            "where": {
                "name": { "?": { "name": "n" } },
                "age": { "?": { "name": "a" } }
            }
        }"#;

        let prop: Proposition = serde_json::from_str(json_str).unwrap();
        match &prop {
            Proposition::Concept(cq) => {
                assert_eq!(cq.terms.get("name"), Some(&Term::<Any>::var("n")));
                assert_eq!(cq.terms.get("age"), Some(&Term::<Any>::var("a")));
            }
            other => panic!("Expected Concept, got {:?}", other),
        }

        let json = serde_json::to_value(&prop).unwrap();
        assert!(json["assert"]["with"].is_object());
        assert!(json["where"].is_object());
    }

    #[test]
    fn it_distinguishes_formula_from_constraint() {
        let formula_json = json!({
            "assert": "math/sum",
            "where": {
                "of": { "?": { "name": "x" } },
                "with": { "?": { "name": "y" } },
                "is": { "?": { "name": "r" } }
            }
        });

        let constraint_json = json!({
            "assert": "==",
            "where": {
                "this": { "?": { "name": "x" } },
                "is": { "?": { "name": "y" } }
            }
        });

        let formula: Proposition = serde_json::from_value(formula_json).unwrap();
        let constraint: Proposition = serde_json::from_value(constraint_json).unwrap();

        assert!(matches!(formula, Proposition::Formula(_)));
        assert!(matches!(constraint, Proposition::Constraint(_)));
    }

    #[test]
    fn it_preserves_variant_type_through_round_trip() {
        let fq = FormulaQuery::Length(Query::<Length> {
            of: Term::var("input"),
            is: Term::var("len"),
        });

        let json = serde_json::to_value(&fq).unwrap();
        let deserialized: FormulaQuery = serde_json::from_value(json).unwrap();

        assert!(matches!(deserialized, FormulaQuery::Length(_)));
    }
}
