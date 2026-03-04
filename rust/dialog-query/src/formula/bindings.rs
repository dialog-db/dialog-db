//! Bindings for reading and writing values during formula evaluation
//!
//! The `Bindings` type provides a controlled interface for formulas
//! to read input values during evaluation. It maintains a mapping between
//! formula parameter names and their corresponding terms in the evaluation context.

use crate::artifact::TypeError;
use crate::error::EvaluationError;
use crate::formula::query::FormulaQuery;
use crate::selection::Answer;
use crate::term::Term;
use crate::{Parameters, Value};
use std::sync::Arc;

/// Parameter-to-value bindings for formula evaluation.
///
/// Provides a mapping layer between formula parameter names and
/// the actual terms used in the evaluation context.
#[derive(Debug, Clone)]
pub struct Bindings {
    /// The current answer containing variable bindings
    ///
    /// NOTE: Public for compatibility with existing formula implementations.
    /// Use `source()` accessor method instead where possible.
    pub source: Answer,

    /// Mapping from parameter names to query terms
    ///
    /// NOTE: Public for compatibility with existing formula implementations.
    /// Use `terms()` accessor method instead where possible.
    pub terms: Parameters,

    /// The formula application these bindings belong to (kept for identity only)
    #[allow(dead_code)]
    formula: Arc<FormulaQuery>,
}

impl Bindings {
    /// Create new bindings for formula evaluation
    pub fn new(formula: Arc<FormulaQuery>, source: impl Into<Answer>, terms: Parameters) -> Self {
        Self {
            source: source.into(),
            terms,
            formula,
        }
    }

    /// Read a typed value from the bindings using a parameter name
    pub fn read<T: TryFrom<Value, Error = TypeError>>(
        &mut self,
        key: &str,
    ) -> Result<T, EvaluationError> {
        Ok(T::try_from(self.resolve(key)?)?)
    }

    /// Resolve a parameter to its Value
    pub fn resolve(&mut self, key: &str) -> Result<Value, EvaluationError> {
        let param = self
            .terms
            .get(key)
            .ok_or_else(|| EvaluationError::MissingParameter {
                parameter: key.into(),
            })?;

        self.source
            .resolve(param)
            .map_err(|_| EvaluationError::UnboundFormulaVariable {
                term: Box::new(param.clone()),
                parameter: key.into(),
            })
    }

    /// Get an immutable reference to the source answer
    pub fn source(&self) -> &Answer {
        &self.source
    }

    /// Get an immutable reference to the terms mapping
    pub fn terms(&self) -> &Parameters {
        &self.terms
    }

    /// Write a value to the bindings
    ///
    /// Binds the value to the parameter's term in the answer.
    /// Fails if the parameter key is not in the terms mapping
    /// or if the assignment conflicts with an existing value.
    pub fn write(&mut self, key: &str, value: &Value) -> Result<(), EvaluationError> {
        let param = self
            .terms
            .get(key)
            .ok_or_else(|| EvaluationError::MissingParameter {
                parameter: key.into(),
            })?;

        // For constant parameters, verify the computed value matches the constant.
        // Answer::bind treats constants as no-ops, so we must check here.
        if let Term::Constant(expected) = param {
            if expected != value {
                return Err(EvaluationError::Conflict {
                    parameter: key.into(),
                    actual: Box::new(Term::Constant(value.clone())),
                    expected: Box::new(param.clone()),
                });
            }
            return Ok(());
        }

        self.source
            .bind(param, value.clone())
            .map_err(|_| EvaluationError::Conflict {
                parameter: key.into(),
                actual: Box::new(param.clone()),
                expected: Box::new(Term::Constant(value.clone())),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;

    fn test_formula() -> crate::formula::query::FormulaQuery {
        use crate::formula::math;
        crate::formula::query::FormulaQuery::Sum(crate::Query::<math::Sum> {
            of: crate::Term::var("_unused_of"),
            with: crate::Term::var("_unused_with"),
            is: crate::Term::var("_unused_is"),
        })
    }

    #[dialog_common::test]
    fn it_reads_bound_values() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("test"));

        let mut source = Answer::new();
        source
            .bind(&Term::var("test"), 42u32.into())
            .expect("Failed to create test match");

        let formula = test_formula();
        let mut bindings = Bindings::new(Arc::new(formula), source, terms);

        let value = bindings.read::<u32>("value").expect("Failed to read value");
        assert_eq!(value, 42);
    }

    #[dialog_common::test]
    fn it_errors_on_missing_parameter() {
        use crate::selection::Answer;

        let terms = Parameters::new();
        let source = Answer::new();
        let formula = test_formula();
        let mut bindings = Bindings::new(Arc::new(formula), source, terms);

        let result = bindings.read::<u32>("missing");
        assert!(matches!(
            result,
            Err(EvaluationError::MissingParameter { .. })
        ));
    }

    #[dialog_common::test]
    fn it_errors_on_unbound_variable() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("unbound"));

        let source = Answer::new();
        let formula = test_formula();
        let mut bindings = Bindings::new(Arc::new(formula), source, terms);

        let result = bindings.read::<u32>("value");
        assert!(matches!(
            result,
            Err(EvaluationError::UnboundFormulaVariable { .. })
        ));
    }

    #[dialog_common::test]
    fn it_rejects_conflicting_assignment() {
        use crate::selection::Answer;

        let mut answer = Answer::new();
        answer
            .bind(&Term::var("test"), 42u32.into())
            .expect("bind should succeed");

        let result = answer.bind(&Term::var("test"), Value::UnsignedInt(100));
        assert!(
            result.is_err(),
            "Answer.bind() should reject conflicting value assignment"
        );
    }

    #[dialog_common::test]
    fn it_rejects_conflicting_write_value() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("test"));

        let mut source = Answer::new();
        source
            .bind(&Term::var("test"), 42u32.into())
            .expect("Failed to create test match");

        let formula = test_formula();
        let mut bindings = Bindings::new(Arc::new(formula), source, terms);

        let value = bindings.read::<u32>("value").expect("Failed to read value");
        assert_eq!(value, 42);

        let conflicting_value = Value::UnsignedInt(100);
        let result = bindings.write("value", &conflicting_value);

        assert!(
            result.is_err(),
            "Bindings.write() should reject conflicting value"
        );

        let unchanged_value = bindings
            .read::<u32>("value")
            .expect("Failed to read value after conflict");
        assert_eq!(unchanged_value, 42);
    }

    #[dialog_common::test]
    fn it_accepts_matching_constant_write() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::from(42u32).into());

        let source = Answer::new();
        let formula = test_formula();
        let mut bindings = Bindings::new(Arc::new(formula), source, terms);

        let result = bindings.write("value", &Value::UnsignedInt(42));
        assert!(
            result.is_ok(),
            "Writing a value that matches the constant should succeed"
        );
    }

    #[dialog_common::test]
    fn it_rejects_mismatched_constant_write() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::from(99u32).into());

        let source = Answer::new();
        let formula = test_formula();
        let mut bindings = Bindings::new(Arc::new(formula), source, terms);

        let result = bindings.write("value", &Value::UnsignedInt(8));
        assert!(
            result.is_err(),
            "Writing a value that conflicts with the constant should fail"
        );
        assert!(
            matches!(result.unwrap_err(), EvaluationError::Conflict { .. }),
            "Should be a Conflict error"
        );
    }
}
