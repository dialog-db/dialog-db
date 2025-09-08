//! Formula2 system - A refined approach to formula evaluation
//!
//! This module implements the Formula2 trait system as outlined in the notes/formula.md
//! specification, providing a type-safe and efficient way to define and execute formulas.

use crate::deductive_rule::Terms;
use crate::{try_stream, EvaluationContext, Match, QueryError, Selection, Store, Term, Value};
use std::marker::PhantomData;
use thiserror::Error;

/// Errors that can occur during formula evaluation
#[derive(Error, Debug, Clone, PartialEq)]
pub enum FormulaEvaluationError {
    // TODO: Capture formula somehow here.
    #[error("Formula application omits required parameter \"{parameter}\"")]
    RequiredParameter { parameter: String },
    /// A required variable was not found in the input
    #[error("Variable {term} for '{parameter}' required parameter is not bound")]
    UnboundVariable {
        parameter: String,
        term: Term<Value>,
    },

    #[error(
        "Variable for the '{parameter}' is bound to {actual} which is inconsistent with value being set: {expected}"
    )]
    VariableInconsistency {
        parameter: String,
        actual: Term<Value>,
        expected: Term<Value>,
    },

    /// Type mismatch when reading from Match
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

/// A cursor for reading from and writing to matches with term mappings
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    pub source: Match,
    pub terms: Terms,
}

impl Cursor {
    pub fn new(source: Match, terms: Terms) -> Self {
        Self { source, terms }
    }

    /// Read a typed value from the cursor using field name
    pub fn read<T: Cast>(&self, key: &str) -> Result<T, FormulaEvaluationError> {
        let term =
            self.terms
                .get(key)
                .ok_or_else(|| FormulaEvaluationError::RequiredParameter {
                    parameter: key.into(),
                })?;

        let value = self.source.resolve_value(term).map_err(|_| {
            FormulaEvaluationError::UnboundVariable {
                term: term.clone(),
                parameter: key.into(),
            }
        })?;

        T::try_cast(&value)
    }

    /// Write a value to the cursor using field name
    pub fn write(&mut self, key: &str, value: &Value) -> Result<(), FormulaEvaluationError> {
        if let Some(term) = self.terms.get(key) {
            self.source = self
                .source
                // TODO: Better align error types
                .unify_value(term.clone(), value.clone())
                .map_err(|_| FormulaEvaluationError::VariableInconsistency {
                    parameter: key.into(),
                    expected: term.clone(),
                    actual: self.source.resolve(term),
                })?;
        }

        Ok(())
    }
}

/// Trait for casting values from the generic Value type to specific types
pub trait Cast: Sized {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError>;
}

impl Cast for u32 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::UnsignedInt(n) => Ok(*n as u32),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "u32".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

impl Cast for i32 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::SignedInt(n) => Ok(*n as i32),
            Value::UnsignedInt(n) => Ok(*n as i32),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "i32".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

impl Cast for String {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::String(s) => Ok(s.clone()),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "String".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

impl Cast for bool {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            // The Value enum doesn't have a Bool variant, so we handle it as a string or int
            Value::String(s) => match s.as_str() {
                "true" => Ok(true),
                "false" => Ok(false),
                _ => Err(FormulaEvaluationError::TypeMismatch {
                    expected: "bool".into(),
                    actual: format!("String({})", s),
                }),
            },
            Value::UnsignedInt(n) => Ok(*n != 0),
            Value::SignedInt(n) => Ok(*n != 0),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "bool".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

impl Cast for f64 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::Float(f) => Ok(*f),
            Value::SignedInt(i) => Ok(*i as f64),
            Value::UnsignedInt(u) => Ok(*u as f64),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "f64".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

/// The main Formula2 trait
pub trait Formula: Sized {
    type Input: TryFrom<Cursor, Error = FormulaEvaluationError>;
    type Match;

    /// Derive output instances from cursor input
    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError>;

    /// Write this formula's output to the cursor
    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError>;

    /// Convert derived outputs to Match instances
    fn derive_match(cursor: &Cursor) -> Result<Vec<Match>, FormulaEvaluationError> {
        let outputs = Self::derive(cursor)?;
        let mut results = Vec::new();

        for output in outputs {
            let mut out_cursor = cursor.clone();
            output.write(&mut out_cursor)?;
            results.push(out_cursor.source);
        }

        Ok(results)
    }

    /// Create a formula application with term bindings
    fn apply(terms: Terms) -> FormulaApplication<Self> {
        FormulaApplication {
            terms,
            _phantom: PhantomData,
        }
    }
}

/// Trait for formulas that can compute their outputs
pub trait Compute: Formula + Sized {
    fn compute(input: Self::Input) -> Vec<Self>;
}

/// Formula application that can be evaluated over a stream of matches
pub struct FormulaApplication<F: Formula> {
    pub terms: Terms,
    pub _phantom: PhantomData<F>,
}

impl<F: Formula> FormulaApplication<F> {
    /// Expand a single match using this formula
    pub fn expand(&self, frame: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        let cursor = Cursor::new(frame, self.terms.clone());
        F::derive_match(&cursor)
    }

    /// Evaluate the formula over a stream of matches
    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let terms = self.terms.clone();
        try_stream! {
            for await source in context.selection {
                let frame = source?;
                let cursor = Cursor::new(frame, terms.clone());
                // Map results and omit inconsistent matches
                let results = match F::derive_match(&cursor) {
                    Ok(output) => Ok(output),
                    Err(e) => {
                        match e {
                            FormulaEvaluationError::VariableInconsistency { .. } => Ok(vec![]),
                            FormulaEvaluationError::RequiredParameter { parameter } => {
                                Err(QueryError::RequiredFormulaParamater { parameter })
                            },
                            FormulaEvaluationError::UnboundVariable { parameter, .. } => {
                                Err(QueryError::UnboundVariable { variable_name: parameter })
                            },
                            FormulaEvaluationError::TypeMismatch { expected, actual } => {
                                Err(QueryError::InvalidTerm {
                                    message: format!("Type mismatch: expected {}, got {}", expected, actual)
                                })
                            },
                        }
                    }
                }?;

                for output in results {
                    yield output;
                }
            }
        }
    }
}

// ============================================================================
// Example: Sum Formula Implementation
// ============================================================================

/// Example Sum formula that adds two numbers
#[derive(Debug, Clone)]
pub struct Sum {
    pub of: u32,
    pub with: u32,
    pub is: u32,
}

/// Input structure for Sum formula
pub struct SumInput {
    pub of: u32,
    pub with: u32,
}

impl TryFrom<Cursor> for SumInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let with = cursor.read::<u32>("with")?;
        Ok(SumInput { of, with })
    }
}

/// Match structure for Sum formula (for future macro generation)
pub struct SumMatch {
    pub of: Term<u32>,
    pub with: Term<u32>,
    pub is: Term<u32>,
}

impl Compute for Sum {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Sum {
            of: input.of,
            with: input.with,
            is: input.of + input.with,
        }]
    }
}

impl Formula for Sum {
    type Input = SumInput;
    type Match = SumMatch;

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::UnsignedInt(self.is.into());
        cursor.write("is", &value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;

    #[test]
    fn test_sum_formula_basic() {
        // Create Terms mapping
        let mut terms = Terms::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("with".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        // Create input match with x=5, y=3
        let input = Match::new()
            .set(Term::var("x"), 5u32)
            .expect("Failed to set x")
            .set(Term::var("y"), 3u32)
            .expect("Failed to set y");

        // Create formula application
        let app = Sum::apply(terms);

        // Expand the formula
        let results = app.expand(input).expect("Formula expansion failed");

        // Verify results
        assert_eq!(results.len(), 1);
        let output = &results[0];

        // Check that x and y are preserved
        assert_eq!(output.get::<u32>(&Term::var("x")).ok(), Some(5));
        assert_eq!(output.get::<u32>(&Term::var("y")).ok(), Some(3));

        // Check that result is computed correctly
        assert_eq!(output.get::<u32>(&Term::var("result")).ok(), Some(8));
    }

    #[test]
    fn test_cursor_read_write() {
        let mut terms = Terms::new();
        terms.insert("value".to_string(), Term::var("test").into());

        let source = Match::new()
            .set(Term::var("test"), 42u32)
            .expect("Failed to create test match");

        let cursor = Cursor::new(source, terms);

        // Test reading
        let value = cursor.read::<u32>("value").expect("Failed to read value");
        assert_eq!(value, 42);

        // Test writing
        let mut write_cursor = cursor.clone();
        let new_value = Value::UnsignedInt(100);
        write_cursor
            .write("value", &new_value)
            .expect("Failed to write value");

        let written_value = write_cursor
            .read::<u32>("value")
            .expect("Failed to read written value");
        assert_eq!(written_value, 100);
    }

    #[test]
    fn test_cast_implementations() {
        let u32_val = Value::UnsignedInt(42);
        assert_eq!(u32::try_cast(&u32_val).unwrap(), 42);

        let i32_val = Value::SignedInt(-10);
        assert_eq!(i32::try_cast(&i32_val).unwrap(), -10);

        let string_val = Value::String("hello".to_string());
        assert_eq!(String::try_cast(&string_val).unwrap(), "hello");

        // Test type mismatch
        let result = u32::try_cast(&string_val);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FormulaEvaluationError::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_sum_formula_missing_input() {
        let mut terms = Terms::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("with".to_string(), Term::var("missing").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("x"), 5u32)
            .expect("Failed to set x");

        let app = Sum::apply(terms);
        let result = app.expand(input);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FormulaEvaluationError::UnboundVariable { .. }
        ));
    }

    #[test]
    fn test_sum_formula_multiple_expand() {
        // Test multiple expansions without the stream complexity
        let mut terms = Terms::new();
        terms.insert("of".to_string(), Term::var("a").into());
        terms.insert("with".to_string(), Term::var("b").into());
        terms.insert("is".to_string(), Term::var("sum").into());

        let app = Sum::apply(terms);

        // Test first input: 2 + 3 = 5
        let input1 = Match::new()
            .set(Term::var("a"), 2u32)
            .unwrap()
            .set(Term::var("b"), 3u32)
            .unwrap();

        let results1 = app.expand(input1).expect("First expansion failed");
        assert_eq!(results1.len(), 1);
        let result1 = &results1[0];
        assert_eq!(result1.get::<u32>(&Term::var("a")).ok(), Some(2));
        assert_eq!(result1.get::<u32>(&Term::var("b")).ok(), Some(3));
        assert_eq!(result1.get::<u32>(&Term::var("sum")).ok(), Some(5));

        // Test second input: 10 + 15 = 25
        let input2 = Match::new()
            .set(Term::var("a"), 10u32)
            .unwrap()
            .set(Term::var("b"), 15u32)
            .unwrap();

        let results2 = app.expand(input2).expect("Second expansion failed");
        assert_eq!(results2.len(), 1);
        let result2 = &results2[0];
        assert_eq!(result2.get::<u32>(&Term::var("a")).ok(), Some(10));
        assert_eq!(result2.get::<u32>(&Term::var("b")).ok(), Some(15));
        assert_eq!(result2.get::<u32>(&Term::var("sum")).ok(), Some(25));
    }

    #[test]
    fn test_multiple_cast_types() {
        // Test various data types
        let bool_val = Value::String("true".to_string());
        assert_eq!(bool::try_cast(&bool_val).unwrap(), true);

        let f64_val = Value::Float(3.14);
        assert_eq!(f64::try_cast(&f64_val).unwrap(), 3.14);

        // Test integer to bool conversion
        let int_true = Value::UnsignedInt(1);
        assert_eq!(bool::try_cast(&int_true).unwrap(), true);

        let int_false = Value::UnsignedInt(0);
        assert_eq!(bool::try_cast(&int_false).unwrap(), false);
    }
}
