//! Equality constraint between two terms
//!
//! Enforces that two terms must have equal values during query evaluation.
//! Supports bidirectional inference: if one term is bound, the other will be
//! inferred to have the same value.
//!
//! Either side may be a [`Pattern`]. A map, a list or a tuple of terms is
//! bound when every variable inside it is, and is then the value it
//! builds. Where the other side is bound instead, the pattern takes that
//! value apart and binds the variables inside it.

use std::fmt;

use crate::pattern::Pattern;
use crate::selection::Match;
use crate::type_system::Type as Kind;
use crate::types::Any;
pub use crate::{
    Binding, Cardinality, Environment, EvaluationError, Field, Parameters, Requirement, Schema,
    Selection, Term, Value, try_stream,
};
use dialog_corpus::ValueDataType;
use std::fmt::Display;

/// Cost for evaluating an equality constraint (simple comparison operation)
const EQUALITY_COST: usize = 1;

/// Equality constraint between two terms.
///
/// This constraint ensures that two terms must have equal values during query evaluation.
/// It implements three key behaviors:
///
/// 1. **Filtering**: When both terms are already bound, the constraint filters out selection
///    where the values don't match.
///
/// 2. **Bidirectional Inference**: When only one term is bound, the constraint infers the
///    value of the unbound term from the bound one. This works in both directions.
///
/// 3. **Error on Unbound**: When neither term is bound, the constraint cannot be evaluated
///    and raises a `ConstraintViolation` error.
///
/// A side that is a map, a list or a tuple of terms ([`Equality::between`])
/// is bound when every variable inside it is. Inferring it from the other
/// side takes the other side's value apart, and a value that is not of the
/// pattern's shape filters the match.
///
/// # Example
/// ```no_run
/// # use dialog_query::constraint::equality::Equality;
/// # use dialog_query::{Term, types::Any};
/// // x must equal y
/// let eq = Equality::new(Term::<Any>::var("x"), Term::<Any>::var("y"));
/// ```
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Equality {
    /// The left-hand parameter of the equality constraint
    pub this: Pattern,
    /// The right-hand parameter of the equality constraint
    pub is: Pattern,
}

/// The kind a side of an equality has: a leaf's own, and a record for a
/// map, a list or a tuple, which is what a value of that shape is kept
/// as.
fn kind_of(side: &Pattern) -> Option<Kind> {
    match side {
        Pattern::Term(term) => term.kind(),
        _ => Some(Kind::from(ValueDataType::Record)),
    }
}

impl Equality {
    /// Creates a new equality constraint between two parameters.
    pub fn new(this: Term<Any>, is: Term<Any>) -> Self {
        Self {
            this: Pattern::Term(this),
            is: Pattern::Term(is),
        }
    }

    /// Creates an equality constraint between two patterns: a term, or a
    /// map, a list or a tuple of terms.
    pub fn between(this: impl Into<Pattern>, is: impl Into<Pattern>) -> Self {
        Self {
            this: this.into(),
            is: is.into(),
        }
    }

    /// Returns the schema for this constraint.
    ///
    /// The schema describes what parameters the constraint requires to be evaluable.
    pub fn schema(&self) -> Schema {
        let mut schema = Schema::new();
        let requirement = Requirement::new_group();
        schema.insert(
            "this".into(),
            Field {
                description: "Term that must be equal to the \"is\" term.".into(),
                content_type: kind_of(&self.this),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema.insert(
            "is".into(),
            Field {
                description: "Term that must be equal to the \"this\" term.".into(),
                content_type: kind_of(&self.is),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    ///
    /// Returns `Some(cost)` if the constraint can be evaluated (at least one term is bound).
    /// Returns `None` if the constraint cannot be evaluated yet (neither term is bound).
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        if self.this.is_bound(env) | self.is.is_bound(env) {
            Some(EQUALITY_COST)
        } else {
            None
        }
    }

    /// Returns the parameters for this constraint.
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        params.set("this".to_string(), self.this.clone());
        params.set("is".to_string(), self.is.clone());
        params
    }

    /// Evaluates the constraint against the current selection of matches.
    ///
    /// This method processes each match in the input selection and:
    /// - **Filters** matches where both terms are bound but have different values
    /// - **Infers** missing bindings when one term is bound and the other isn't
    /// - **Errors** when neither term is bound (ConstraintViolation)
    ///
    /// # Returns
    /// A stream of matches that satisfy the constraint, with any necessary
    /// variable bindings added through inference.
    pub fn evaluate<M: Selection>(self, selection: M) -> impl Selection {
        try_stream! {
            for await candidate in selection {
                let base = candidate?;
                match (&self.this, &self.is) {
                    (Pattern::Term(this), Pattern::Term(is)) => {
                        if let Some(extension) = Self::equate(base, this, is)? {
                            yield extension;
                        }
                    }
                    (this, is) => {
                        if let Some(extension) = Self::unify(base, this, is)? {
                            yield extension;
                        }
                    }
                }
            }
        }
    }

    /// One row of an equality between two terms: the row when it
    /// satisfies the equality, extended with what the equality infers,
    /// or nothing when it does not.
    fn equate(
        base: Match,
        this: &Term<Any>,
        is: &Term<Any>,
    ) -> Result<Option<Match>, EvaluationError> {
        Ok(match (base.lookup(this), base.lookup(is)) {
            // Both Present: yield iff values match.
            (Ok(Binding::Present(this_val)), Ok(Binding::Present(is_val))) => {
                (this_val == is_val).then_some(base)
            }
            // Both Absent: Nothing == Nothing, yield.
            (Ok(Binding::Absent), Ok(Binding::Absent)) => Some(base),
            // One side Absent, other Present: disjoint kinds
            // (Nothing vs primitive), can never be equal: filter.
            (Ok(Binding::Absent), Ok(Binding::Present(_)))
            | (Ok(Binding::Present(_)), Ok(Binding::Absent)) => None,
            // One side Present, other unbound: propagate the value.
            (Err(_), Ok(Binding::Present(is_val))) => {
                let mut extension = base.clone();
                extension.bind(this, is_val)?;
                Some(extension)
            }
            (Ok(Binding::Present(this_val)), Err(_)) => {
                let mut extension = base.clone();
                extension.bind(is, this_val)?;
                Some(extension)
            }
            // One side Absent, other unbound: propagate Absent
            // iff the unbound term's kind explicitly admits
            // Nothing. Types are checked, not assumed:
            // carrying a claimed absence into a term requires
            // the term to have opted into absence, so an
            // untyped term filters.
            (Ok(Binding::Absent), Err(_)) => {
                if is.is_optional() {
                    let mut extension = base.clone();
                    extension.bind_absent(is)?;
                    Some(extension)
                } else {
                    None
                }
            }
            (Err(_), Ok(Binding::Absent)) => {
                if this.is_optional() {
                    let mut extension = base.clone();
                    extension.bind_absent(this)?;
                    Some(extension)
                } else {
                    None
                }
            }
            // Neither bound: equality has nothing to work with.
            (Err(_), Err(_)) => {
                return Err(EvaluationError::ConstraintViolation {
                    constraint: format!("{} == {}", this, is),
                });
            }
        })
    }

    /// One row of an equality a side of which is a map, a list or a
    /// tuple of terms. A side every variable of which has a value is
    /// that value: with both, the row passes when they are one value;
    /// with one, the other side takes it apart, binding its variables,
    /// and a value it does not match filters the row. A side that is
    /// bound to nothing has no value of any shape, so the row is
    /// filtered. With neither, the equality has nothing to work with.
    fn unify(base: Match, this: &Pattern, is: &Pattern) -> Result<Option<Match>, EvaluationError> {
        let absent = |side: &Pattern| {
            side.terms()
                .into_iter()
                .any(|term| matches!(base.lookup(term), Ok(Binding::Absent)))
        };
        if absent(this) || absent(is) {
            return Ok(None);
        }
        let (value, other) = match (this.build(&base), is.build(&base)) {
            (Some(this_val), Some(is_val)) => return Ok((this_val == is_val).then_some(base)),
            (Some(value), None) => (value, is),
            (None, Some(value)) => (value, this),
            (None, None) => {
                // A side whose variables all have values and which
                // still is no value (a rest that is not a map) equals
                // nothing.
                let environment = base.environment();
                if this.is_bound(&environment) || is.is_bound(&environment) {
                    return Ok(None);
                }
                return Err(EvaluationError::ConstraintViolation {
                    constraint: format!("{} == {}", this, is),
                });
            }
        };
        let mut extension = base.clone();
        Ok(other.unify(&value, &mut extension)?.then_some(extension))
    }
}

impl Display for Equality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.this, &self.is) {
            (Pattern::Term(this), Pattern::Term(is)) => write!(f, "{} == {}", this, is),
            (this, is) => write!(f, "{} == {}", this, is),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::corpus::Type as ValueType;
    use futures_util::TryStreamExt;

    #[dialog_common::test]
    async fn it_passes_when_both_terms_equal() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut candidate = Match::new();
        candidate.bind(&Term::var("x"), Value::from(42))?;
        candidate.bind(&Term::var("y"), Value::from(42))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1, "Should have one result");
        assert_eq!(
            results[0].lookup(&Term::var("x"))?.content()?,
            Value::from(42),
            "x should still be 42"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_when_terms_differ() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut candidate = Match::new();
        candidate.bind(&Term::var("x"), Value::from(42))?;
        candidate.bind(&Term::var("y"), Value::from(99))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "Should have no results when values don't match"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_infers_this_from_is() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut candidate = Match::new();
        candidate.bind(&Term::var("y"), Value::from(42))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1, "Should have one result");
        assert_eq!(
            results[0].lookup(&Term::var("x"))?.content()?,
            Value::from(42),
            "x should be inferred as 42"
        );

        Ok(())
    }

    #[dialog_common::test]
    fn it_estimates_zero_cost_when_bound() {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut env = Environment::new();
        env.add("x");

        assert_eq!(
            constraint.estimate(&env),
            Some(EQUALITY_COST),
            "Should return cost when at least one term is bound"
        );
    }

    #[dialog_common::test]
    fn it_estimates_none_when_unbound() {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));
        let env = Environment::new();

        assert_eq!(
            constraint.estimate(&env),
            None,
            "Should return None when neither term is bound"
        );
    }

    /// Untyped term sides produce `None` content_type: no
    /// static info is available without rule-compile time
    /// unification.
    #[dialog_common::test]
    fn schema_unknown_term_yields_none() {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));
        let schema = constraint.schema();
        let this_field = schema.get("this").expect("this field present");
        let is_field = schema.get("is").expect("is field present");
        assert!(this_field.content_type().is_none());
        assert!(is_field.content_type().is_none());
    }

    /// Absent on both sides: Nothing == Nothing, yield.
    #[dialog_common::test]
    async fn it_yields_when_both_sides_absent() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::<Any>::var("x"))?;
        candidate.bind_absent(&Term::<Any>::var("y"))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1, "Absent == Absent should yield");
        Ok(())
    }

    /// Absent on one side, Present on the other: disjoint kinds,
    /// filter.
    #[dialog_common::test]
    async fn it_filters_when_absent_meets_present() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::<Any>::var("x"))?;
        candidate.bind(&Term::var("y"), Value::from(42))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 0, "Absent == Present should filter");
        Ok(())
    }

    /// Absent on one side, unbound on the other where the unbound
    /// term's kind admits Nothing: propagate Absent.
    #[dialog_common::test]
    async fn it_infers_absent_into_optional_term() -> Result<(), EvaluationError> {
        let constraint = Equality::new(
            Term::var("x"),
            Term::<Any>::from(Term::<Option<String>>::var("y")),
        );

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::<Any>::var("x"))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].lookup(&Term::<Any>::var("y"))?, Binding::Absent);
        Ok(())
    }

    /// Absent on one side, unbound on the other where the unbound
    /// term's kind does NOT admit Nothing: filter (would propagate
    /// Absent into a non-optional slot).
    #[dialog_common::test]
    async fn it_filters_absent_into_required_term() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::<Any>::from(Term::<String>::var("y")));

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::<Any>::var("x"))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "Absent cannot bind a non-optional term: filter"
        );
        Ok(())
    }

    /// An *untyped* term does not accept a propagated Absent either:
    /// carrying a claimed absence into a term requires the term to
    /// have explicitly opted into absence (an `Option`-typed kind).
    #[dialog_common::test]
    async fn it_filters_absent_into_untyped_term() -> Result<(), EvaluationError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::<Any>::var("x"))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "an untyped term has not opted into absence: filter"
        );
        Ok(())
    }

    /// A typed constant on one side produces a concrete primitive
    /// for that field's content_type.
    #[dialog_common::test]
    fn schema_lifts_typed_constant_to_primitive() {
        let constraint = Equality::new(Term::var("x"), Term::constant(42u32));
        let schema = constraint.schema();
        let is_field = schema.get("is").expect("is field present");
        let content = is_field.content_type().expect("content_type present");
        assert_eq!(content.as_value_type(), Some(ValueType::UnsignedInt));
    }
}
