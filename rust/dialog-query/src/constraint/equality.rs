//! Equality constraint between two terms
//!
//! Enforces that two terms must have equal values during query evaluation.
//! Supports bidirectional inference: if one term is bound, the other will be
//! inferred to have the same value.

pub use crate::selection::Evidence;
pub use crate::{
    Answers, Cardinality, Environment, Field, Parameter, Parameters, QueryError, Requirement,
    Schema, Term, Value, try_stream,
};
use std::fmt::Display;

/// Cost for evaluating an equality constraint (simple comparison operation)
const EQUALITY_COST: usize = 1;

/// Equality constraint between two terms.
///
/// This constraint ensures that two terms must have equal values during query evaluation.
/// It implements three key behaviors:
///
/// 1. **Filtering**: When both terms are already bound, the constraint filters out answers
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
/// # use dialog_query::Parameter;
/// // x must equal y
/// let eq = Equality::new(Parameter::var("x"), Parameter::var("y"));
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Equality {
    /// The left-hand parameter of the equality constraint
    pub this: Parameter,
    /// The right-hand parameter of the equality constraint
    pub is: Parameter,
}

impl Equality {
    /// Creates a new equality constraint between two parameters.
    pub fn new(this: Parameter, is: Parameter) -> Self {
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
                content_type: self.this.content_type(),
                requirement: requirement.required(),
                cardinality: Cardinality::One,
            },
        );
        schema.insert(
            "is".into(),
            Field {
                description: "Term that must be equal to the \"this\" term.".into(),
                content_type: self.is.content_type(),
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

    /// Evaluates the constraint against the current selection of answers.
    ///
    /// This method processes each answer in the input selection and:
    /// - **Filters** answers where both terms are bound but have different values
    /// - **Infers** missing bindings when one term is bound and the other isn't
    /// - **Errors** when neither term is bound (ConstraintViolation)
    ///
    /// # Returns
    /// A stream of answers that satisfy the constraint, with any necessary
    /// variable bindings added through inference.
    pub fn evaluate<M: Answers>(self, answers: M) -> impl Answers {
        let this = self.this;
        let is = self.is;
        try_stream! {
            for await each in answers {
                let input = each?;

                match (input.resolve(&this), input.resolve(&is)) {
                    // Case 1: Both terms are bound - verify they are equal
                    // Only pass through the answer if the values match
                    (Ok(this_val), Ok(is_val)) => {
                        if this_val == is_val {
                            yield input;
                        }
                        // Otherwise filter out this answer (no yield)
                    }
                    // Case 2: Only "is" is bound - infer "this" from "is"
                    // Add the inferred binding to the answer
                    (Err(_), Ok(is_val)) => {
                        let mut answer = input.clone();
                        answer.merge(Evidence::Parameter {
                            term: &this,
                            value: &is_val,
                        })?;

                        yield answer;
                    }
                    // Case 3: Only "this" is bound - infer "is" from "this"
                    // Add the inferred binding to the answer
                    (Ok(this_val), Err(_)) => {
                        let mut answer = input.clone();
                        answer.merge(Evidence::Parameter {
                            term: &is,
                            value: &this_val,
                        })?;

                        yield answer;
                    }
                    // Case 4: Neither term is bound - cannot evaluate
                    // Raise a constraint violation error
                    (Err(_), Err(_)) => {
                        Err(QueryError::ConstraintViolation {
                            constraint: format!("{} == {}", this, is)
                        })?;
                    }
                };
            }
        }
    }
}

impl Display for Equality {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} == {}", self.this, self.is)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::Answer;
    use futures_util::TryStreamExt;

    #[dialog_common::test]
    async fn it_passes_when_both_terms_equal() -> Result<(), QueryError> {
        let constraint = Equality::new(Parameter::var("x"), Parameter::var("y"));

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Parameter::var("x"),
            value: &Value::from(42),
        })?;
        answer.merge(Evidence::Parameter {
            term: &Parameter::var("y"),
            value: &Value::from(42),
        })?;

        let answers = futures_util::stream::iter(vec![Ok(answer.clone())]);
        let results: Vec<Answer> = constraint.evaluate(answers).try_collect().await?;

        assert_eq!(results.len(), 1, "Should have one result");
        assert_eq!(
            results[0].resolve(&Parameter::var("x"))?,
            Value::from(42),
            "x should still be 42"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_when_terms_differ() -> Result<(), QueryError> {
        let constraint = Equality::new(Parameter::var("x"), Parameter::var("y"));

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Parameter::var("x"),
            value: &Value::from(42),
        })?;
        answer.merge(Evidence::Parameter {
            term: &Parameter::var("y"),
            value: &Value::from(99),
        })?;

        let answers = futures_util::stream::iter(vec![Ok(answer.clone())]);
        let results: Vec<Answer> = constraint.evaluate(answers).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "Should have no results when values don't match"
        );

        Ok(())
    }

    #[dialog_common::test]
    async fn it_infers_this_from_is() -> Result<(), QueryError> {
        let constraint = Equality::new(Parameter::var("x"), Parameter::var("y"));

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Parameter::var("y"),
            value: &Value::from(42),
        })?;

        let answers = futures_util::stream::iter(vec![Ok(answer.clone())]);
        let results: Vec<Answer> = constraint.evaluate(answers).try_collect().await?;

        assert_eq!(results.len(), 1, "Should have one result");
        assert_eq!(
            results[0].resolve(&Parameter::var("x"))?,
            Value::from(42),
            "x should be inferred as 42"
        );

        Ok(())
    }

    #[dialog_common::test]
    fn it_estimates_zero_cost_when_bound() {
        let constraint = Equality::new(Parameter::var("x"), Parameter::var("y"));

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
        let constraint = Equality::new(Parameter::var("x"), Parameter::var("y"));
        let env = Environment::new();

        assert_eq!(
            constraint.estimate(&env),
            None,
            "Should return None when neither term is bound"
        );
    }
}
