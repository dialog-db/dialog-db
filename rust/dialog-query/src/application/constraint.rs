pub use super::Application;
pub use crate::selection::Evidence;
pub use crate::{
    try_stream, Answers, Environment, EvaluationContext, Field, Parameters, QueryError,
    Requirement, Schema, Source, Term, Value,
};
use std::fmt::Display;

/// Cost for evaluating an equality constraint (simple comparison operation)
const EQUALITY_COST: usize = 1;

/// Equality constraint between two terms that supports bidirectional inference.
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
/// // Create constraint that x must equal y
/// let constraint = Term::var("x").eq(Term::var("y"));
///
/// // If x=5 is already bound, then y will be inferred as 5
/// // If y=5 is already bound, then x will be inferred as 5
/// // If both are bound, only answers where x==y pass through
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct ConstraintApplication {
    /// The left-hand term of the equality constraint
    pub this: Term<Value>,
    /// The right-hand term of the equality constraint
    pub is: Term<Value>,
}

impl ConstraintApplication {
    /// Returns the schema for this constraint application.
    ///
    /// The schema requires either "this" or "is" to be bound in the environment,
    /// allowing the constraint to infer the other term's value.
    pub fn schema(&self) -> Schema {
        let mut schema = Schema::new();
        let requirement = Requirement::new();
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
                content_type: self.this.content_type(),
                requirement: requirement.required(),
                cardinality: crate::Cardinality::One,
            },
        );

        schema
    }

    /// Estimates the cost of evaluating this constraint given the current environment.
    ///
    /// Returns `Some(EQUALITY_COST)` if at least one of the terms ("this" or "is") is
    /// bound in the environment, allowing the constraint to be evaluated.
    ///
    /// Returns `None` if neither term is bound, meaning the constraint cannot be
    /// evaluated yet and should be deferred until more bindings are available.
    pub fn estimate(&self, env: &Environment) -> Option<usize> {
        if env.contains(&self.this) | env.contains(&self.is) {
            Some(EQUALITY_COST)
        } else {
            None
        }
    }

    /// Returns the parameters for this constraint application
    pub fn parameters(&self) -> Parameters {
        let mut params = Parameters::new();
        params.insert("this".to_string(), self.this.clone());
        params.insert("is".to_string(), self.is.clone());
        params
    }

    /// Evaluates the equality constraint against the current selection of answers.
    ///
    /// This method processes each answer in the input selection and:
    /// - **Filters** answers where both terms are bound but have different values
    /// - **Infers** missing bindings when one term is bound and the other isn't
    /// - **Errors** when neither term is bound (ConstraintViolation)
    ///
    /// The evaluation supports bidirectional inference, meaning if "this" is bound,
    /// "is" will be inferred (and vice versa).
    ///
    /// # Returns
    /// A stream of answers that satisfy the equality constraint, with any necessary
    /// variable bindings added through inference.
    pub fn evaluate<S: Source, M: Answers>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Answers {
        let constraint = self.clone();
        try_stream! {
            for await each in context.selection {
                let input = each?;

                match (input.resolve(&constraint.this), input.resolve(&constraint.is)) {
                    // Case 1: Both terms are bound - verify they are equal
                    // Only pass through the answer if the values match
                    (Ok(this), Ok(is)) => {
                        if this == is {
                            yield input;
                        }
                        // Otherwise filter out this answer (no yield)
                    }
                    // Case 2: Only "is" is bound - infer "this" from "is"
                    // Add the inferred binding to the answer
                    (Err(_), Ok(is)) => {
                        let mut answer = input.clone();
                        answer.merge(Evidence::Parameter {
                            term: &constraint.this,
                            value: &is,
                        })?;

                        yield answer;
                    }
                    // Case 3: Only "this" is bound - infer "is" from "this"
                    // Add the inferred binding to the answer
                    (Ok(this), Err(_)) => {
                        let mut answer = input.clone();
                        answer.merge(Evidence::Parameter {
                            term: &constraint.is,
                            value: &this,
                        })?;

                        yield answer;
                    }
                    // Case 4: Neither term is bound - cannot evaluate
                    // Raise a constraint violation error
                    (Err(_), Err(_)) => {
                        Err(QueryError::ConstraintViolation {
                            constraint: constraint.clone()
                        })?;
                    }
                };
            }
        }
    }
}

impl Display for ConstraintApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} == {}", self.this, self.is)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::Answer;
    use crate::{artifact::Artifacts, Session};
    use dialog_storage::MemoryStorageBackend;
    use futures_util::TryStreamExt;

    #[tokio::test]
    async fn test_both_terms_bound_and_equal() -> Result<(), QueryError> {
        // When both terms are bound to the same value, the answer should pass through
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

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

    #[tokio::test]
    async fn test_both_terms_bound_but_not_equal() -> Result<(), QueryError> {
        // When both terms are bound to different values, the answer should be filtered out
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

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

    #[tokio::test]
    async fn test_only_is_bound_infers_this() -> Result<(), QueryError> {
        // When only "is" is bound, "this" should be inferred
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

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
        assert_eq!(
            results[0].resolve(&Term::<Value>::var("y"))?,
            Value::from(42),
            "y should still be 42"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_only_this_bound_infers_is() -> Result<(), QueryError> {
        // When only "this" is bound, "is" should be inferred
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

        let mut answer = Answer::new();
        answer.merge(Evidence::Parameter {
            term: &Term::var("x"),
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
        assert_eq!(
            results[0].resolve(&Term::<Value>::var("y"))?,
            Value::from(42),
            "y should be inferred as 42"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_neither_term_bound_errors() {
        // When neither term is bound, it should error with ConstraintViolation
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

        let answer = Answer::new();

        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();
        let session = Session::open(artifacts);

        let context = EvaluationContext {
            selection: futures_util::stream::iter(vec![Ok(answer.clone())]),
            source: session,
            scope: Environment::new(),
        };

        let result: Result<Vec<Answer>, QueryError> =
            constraint.evaluate(context).try_collect().await;

        assert!(result.is_err(), "Should error when neither term is bound");
        match result {
            Err(QueryError::ConstraintViolation { .. }) => {
                // Expected error type
            }
            _ => panic!("Expected ConstraintViolation error"),
        }
    }

    #[test]
    fn test_estimate_returns_some_when_this_bound() {
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("x"));

        assert_eq!(
            constraint.estimate(&env),
            Some(EQUALITY_COST),
            "Should return cost when 'this' is bound"
        );
    }

    #[test]
    fn test_estimate_returns_some_when_is_bound() {
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

        let mut env = Environment::new();
        env.add(&Term::<Value>::var("y"));

        assert_eq!(
            constraint.estimate(&env),
            Some(EQUALITY_COST),
            "Should return cost when 'is' is bound"
        );
    }

    #[test]
    fn test_estimate_returns_none_when_neither_bound() {
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

        let env = Environment::new();

        assert_eq!(
            constraint.estimate(&env),
            None,
            "Should return None when neither term is bound"
        );
    }

    #[test]
    fn test_schema_requires_either_this_or_is() {
        let constraint = ConstraintApplication {
            this: Term::var("x"),
            is: Term::var("y"),
        };

        let schema = constraint.schema();

        assert!(schema.contains("this"), "Schema should have 'this' field");
        assert!(schema.contains("is"), "Schema should have 'is' field");

        let this_field = schema.get("this").unwrap();
        let is_field = schema.get("is").unwrap();

        assert!(
            this_field.requirement.is_required(),
            "'this' should be required"
        );
        assert!(
            is_field.requirement.is_required(),
            "'is' should be required"
        );
    }
}
