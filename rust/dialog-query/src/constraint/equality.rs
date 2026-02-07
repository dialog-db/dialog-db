//! Equality constraint between two terms
//!
//! Enforces that two terms must have equal values during query evaluation.
//! Supports bidirectional inference: if one term is bound, the other will be
//! inferred to have the same value.

pub use crate::selection::Evidence;
pub use crate::{
    Answers, Environment, EvaluationContext, Field, Parameters, QueryError, Requirement, Schema,
    Source, Term, Value, try_stream,
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
/// ```ignore
/// // x must equal y
/// Constraint::Equality(Term::var("x"), Term::var("y"))
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct Equality {
    /// The left-hand term of the equality constraint
    pub this: Term<Value>,
    /// The right-hand term of the equality constraint
    pub is: Term<Value>,
}

impl Equality {
    /// Creates a new equality constraint between two terms.
    pub fn new(this: Term<Value>, is: Term<Value>) -> Self {
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
                cardinality: crate::Cardinality::One,
            },
        );
        schema.insert(
            "is".into(),
            Field {
                description: "Term that must be equal to the \"this\" term.".into(),
                content_type: self.is.content_type(),
                requirement: requirement.required(),
                cardinality: crate::Cardinality::One,
            },
        );
        schema
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    ///
    /// Returns `Some(cost)` if the constraint can be evaluated (at least one term is bound).
    /// Returns `None` if the constraint cannot be evaluated yet (neither term is bound).
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        if env.contains(&self.this) | env.contains(&self.is) {
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
    pub fn evaluate<S: Source, M: Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Answers {
        let this = self.this.clone();
        let is = self.is.clone();
        try_stream! {
            for await each in context.selection {
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
    use crate::{Session, artifact::Artifacts};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::TryStreamExt;

    #[dialog_macros::test]
    async fn test_equality_both_terms_bound_and_equal() -> Result<(), QueryError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Term::var("x"),
            value: &Value::from(42),
        })?;
        answer.merge(Evidence::Parameter {
            term: &Term::var("y"),
            value: &Value::from(42),
        })?;

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let context = EvaluationContext {
            selection: futures_util::stream::iter(vec![Ok(answer.clone())]),
            source: session,
            scope: Environment::new(),
        };

        let results: Vec<Answer> = constraint.evaluate(context).try_collect().await?;

        assert_eq!(results.len(), 1, "Should have one result");
        assert_eq!(
            results[0].resolve(&Term::<Value>::var("x"))?,
            Value::from(42),
            "x should still be 42"
        );

        Ok(())
    }

    #[dialog_macros::test]
    async fn test_equality_both_terms_bound_but_not_equal() -> Result<(), QueryError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Term::var("x"),
            value: &Value::from(42),
        })?;
        answer.merge(Evidence::Parameter {
            term: &Term::var("y"),
            value: &Value::from(99),
        })?;

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let context = EvaluationContext {
            selection: futures_util::stream::iter(vec![Ok(answer.clone())]),
            source: session,
            scope: Environment::new(),
        };

        let results: Vec<Answer> = constraint.evaluate(context).try_collect().await?;

        assert_eq!(
            results.len(),
            0,
            "Should have no results when values don't match"
        );

        Ok(())
    }

    #[dialog_macros::test]
    async fn test_equality_infers_this_from_is() -> Result<(), QueryError> {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Term::var("y"),
            value: &Value::from(42),
        })?;

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let context = EvaluationContext {
            selection: futures_util::stream::iter(vec![Ok(answer.clone())]),
            source: session,
            scope: Environment::new(),
        };

        let results: Vec<Answer> = constraint.evaluate(context).try_collect().await?;

        assert_eq!(results.len(), 1, "Should have one result");
        assert_eq!(
            results[0].resolve(&Term::<Value>::var("x"))?,
            Value::from(42),
            "x should be inferred as 42"
        );

        Ok(())
    }

    #[test]
    fn test_equality_estimate_when_bound() {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("x"));

        assert_eq!(
            constraint.estimate(&env),
            Some(EQUALITY_COST),
            "Should return cost when at least one term is bound"
        );
    }

    #[test]
    fn test_equality_estimate_when_unbound() {
        let constraint = Equality::new(Term::var("x"), Term::var("y"));
        let env = Environment::new();

        assert_eq!(
            constraint.estimate(&env),
            None,
            "Should return None when neither term is bound"
        );
    }
}
