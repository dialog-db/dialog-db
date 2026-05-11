//! Equality constraint between two terms
//!
//! Enforces that two terms must have equal values during query evaluation.
//! Supports bidirectional inference: if one term is bound, the other will be
//! inferred to have the same value.

use std::fmt;

use crate::types::Any;
pub use crate::{
    Binding, Cardinality, Environment, EvaluationError, Field, Parameters, Requirement, Schema,
    Selection, Term, Value, try_stream,
};
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
    pub this: Term<Any>,
    /// The right-hand parameter of the equality constraint
    pub is: Term<Any>,
}

impl Equality {
    /// Creates a new equality constraint between two parameters.
    pub fn new(this: Term<Any>, is: Term<Any>) -> Self {
        Self { this, is }
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
                content_type: self.this.kind(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema.insert(
            "is".into(),
            Field {
                description: "Term that must be equal to the \"this\" term.".into(),
                content_type: self.is.kind(),
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
        params.insert("this".to_string(), self.this.clone());
        params.insert("is".to_string(), self.is.clone());
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
        let this = self.this;
        let is = self.is;
        try_stream! {
            for await candidate in selection {
                let base = candidate?;

                match (base.lookup(&this), base.lookup(&is)) {
                    // Both Present: yield iff values match.
                    (Ok(Binding::Present(this_val)), Ok(Binding::Present(is_val))) => {
                        if this_val == is_val {
                            yield base;
                        }
                    }
                    // Both Absent: Nothing == Nothing, yield.
                    (Ok(Binding::Absent), Ok(Binding::Absent)) => {
                        yield base;
                    }
                    // One side Absent, other Present: disjoint kinds
                    // (Nothing vs primitive), can never be equal — filter.
                    (Ok(Binding::Absent), Ok(Binding::Present(_)))
                    | (Ok(Binding::Present(_)), Ok(Binding::Absent)) => {}
                    // One side Present, other unbound: propagate the value.
                    (Err(_), Ok(Binding::Present(is_val))) => {
                        let mut extension = base.clone();
                        extension.bind(&this, is_val)?;
                        yield extension;
                    }
                    (Ok(Binding::Present(this_val)), Err(_)) => {
                        let mut extension = base.clone();
                        extension.bind(&is, this_val)?;
                        yield extension;
                    }
                    // One side Absent, other unbound: propagate Absent
                    // iff the unbound term's kind admits Nothing.
                    // Otherwise the row violates the unbound term's
                    // type contract — filter.
                    (Ok(Binding::Absent), Err(_)) => {
                        if is.is_optional() || is.kind().is_none() {
                            let mut extension = base.clone();
                            extension.bind_absent(&is)?;
                            yield extension;
                        }
                    }
                    (Err(_), Ok(Binding::Absent)) => {
                        if this.is_optional() || this.kind().is_none() {
                            let mut extension = base.clone();
                            extension.bind_absent(&this)?;
                            yield extension;
                        }
                    }
                    // Neither bound — equality has nothing to work with.
                    (Err(_), Err(_)) => {
                        Err(EvaluationError::ConstraintViolation {
                            constraint: format!("{} == {}", this, is)
                        })?;
                    }
                };
            }
        }
    }
}

impl Display for Equality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} == {}", self.this, self.is)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::Match;
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

    /// Untyped term sides produce `None` content_type — no
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

    /// Absent on both sides — Nothing == Nothing, yield.
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

    /// Absent on one side, Present on the other — disjoint kinds,
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
    /// term's kind admits Nothing — propagate Absent.
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
    /// term's kind does NOT admit Nothing — filter (would propagate
    /// Absent into a non-optional slot).
    #[dialog_common::test]
    async fn it_filters_absent_into_required_term() -> Result<(), EvaluationError> {
        let constraint = Equality::new(
            Term::var("x"),
            Term::<Any>::from(Term::<String>::var("y")),
        );

        let mut candidate = Match::new();
        candidate.bind_absent(&Term::<Any>::var("x"))?;

        let results: Vec<Match> = constraint.evaluate(candidate.seed()).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "Absent cannot bind a non-optional term — filter"
        );
        Ok(())
    }

    /// A typed constant on one side produces a concrete primitive
    /// for that field's content_type.
    #[dialog_common::test]
    fn schema_lifts_typed_constant_to_primitive() {
        use crate::artifact::Type as ValueType;
        let constraint = Equality::new(Term::var("x"), Term::constant(42u32));
        let schema = constraint.schema();
        let is_field = schema.get("is").expect("is field present");
        let content = is_field.content_type().expect("content_type present");
        assert_eq!(content.as_value_type(), Some(ValueType::UnsignedInt));
    }
}
