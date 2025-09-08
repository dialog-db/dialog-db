//! Formula system for type-safe data transformations in queries
//!
//! This module provides a powerful and extensible system for defining formulas that
//! transform data during query evaluation. Formulas enable computed fields, data
//! transformations, and complex calculations while maintaining type safety.
//!
//! # Overview
//!
//! The formula system consists of several key components:
//!
//! - **[`Formula`] trait** - The core trait that all formulas must implement
//! - **[`Compute`] trait** - Optional trait for formulas that compute outputs from inputs
//! - **[`FormulaApplication`]** - Represents a formula bound to specific term mappings
//! - **[`Cursor`](crate::cursor::Cursor)** - Provides read/write access during evaluation
//! - **[`Cast`](crate::value::Cast)** - Type conversion between Value and Rust types
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐
//! │   User Query    │
//! └────────┬────────┘
//!          │ Terms mapping: {of: ?x, with: ?y, is: ?result}
//!          ▼
//! ┌─────────────────┐
//! │FormulaApplication│
//! └────────┬────────┘
//!          │ For each input Match
//!          ▼
//! ┌─────────────────┐
//! │     Cursor      │ Reads: ?x → 5, ?y → 3
//! └────────┬────────┘
//!          │
//!          ▼
//! ┌─────────────────┐
//! │  Formula Logic  │ Computes: 5 + 3 = 8
//! └────────┬────────┘
//!          │
//!          ▼
//! ┌─────────────────┐
//! │  Write Results  │ Writes: ?result → 8
//! └─────────────────┘
//! ```
//!
//! # Example: Sum Formula
//!
//! Here's a complete example of implementing a Sum formula:
//!
//! ```ignore
//! use dialog_query::formula::{Formula, Compute, FormulaApplication};
//! use dialog_query::{Term, Terms, Match, Value, cursor::Cursor};
//!
//! // 1. Define the formula struct with input and output fields
//! #[derive(Debug, Clone)]
//! struct Sum {
//!     of: u32,      // Input field
//!     with: u32,    // Input field
//!     is: u32,      // Output field (computed)
//! }
//!
//! // 2. Define the input type
//! struct SumInput {
//!     of: u32,
//!     with: u32,
//! }
//!
//! // 3. Implement conversion from Cursor to Input
//! impl TryFrom<Cursor> for SumInput {
//!     type Error = FormulaEvaluationError;
//!
//!     fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
//!         Ok(SumInput {
//!             of: cursor.read("of")?,
//!             with: cursor.read("with")?,
//!         })
//!     }
//! }
//!
//! // 4. Implement the Compute trait for the logic
//! impl Compute for Sum {
//!     fn compute(input: Self::Input) -> Vec<Self> {
//!         vec![Sum {
//!             of: input.of,
//!             with: input.with,
//!             is: input.of + input.with,  // The actual computation
//!         }]
//!     }
//! }
//!
//! // 5. Implement the Formula trait
//! impl Formula for Sum {
//!     type Input = SumInput;
//!     type Match = ();  // Not used yet, for future macro generation
//!
//!     fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
//!         let input = Self::Input::try_from(cursor.clone())?;
//!         Ok(Self::compute(input))
//!     }
//!
//!     fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
//!         cursor.write("is", &Value::UnsignedInt(self.is as u64))
//!     }
//! }
//!
//! // 6. Use the formula in a query
//! let mut terms = Terms::new();
//! terms.insert("of".to_string(), Term::var("x"));
//! terms.insert("with".to_string(), Term::var("y"));
//! terms.insert("is".to_string(), Term::var("result"));
//!
//! let formula_app = Sum::apply(terms);
//!
//! // Apply to a match with x=5, y=3
//! let input_match = Match::new()
//!     .set(Term::var("x"), 5u32)?
//!     .set(Term::var("y"), 3u32)?;
//!
//! let results = formula_app.expand(input_match)?;
//! assert_eq!(results[0].get::<u32>(&Term::var("result"))?, 8);
//! ```
//!
//! # Design Principles
//!
//! 1. **Type Safety** - Formulas work with strongly typed inputs and outputs
//! 2. **Composability** - Formulas can be chained and combined in queries
//! 3. **Separation of Concerns** - Logic (Compute) is separate from I/O (Cursor)
//! 4. **Error Handling** - Clear error types for all failure modes
//! 5. **Performance** - Zero-cost abstractions where possible
//!
//! # Future Enhancements
//!
//! The formula system is designed to support future macro generation that will
//! automatically derive the boilerplate code, making formula definition as simple as:
//!
//! ```ignore
//! #[derive(Formula)]
//! struct Sum {
//!     of: u32,
//!     with: u32,
//!     #[computed]
//!     is: u32,
//! }
//!
//! impl Compute for Sum {
//!     fn compute(input: Self::Input) -> Vec<Self> {
//!         vec![Sum {
//!             of: input.of,
//!             with: input.with,
//!             is: input.of + input.with,
//!         }]
//!     }
//! }
//! ```

use crate::cursor::Cursor;
use crate::deductive_rule::Terms;
use crate::value::Cast;
use crate::{try_stream, EvaluationContext, Match, QueryError, Selection, Store, Term, Value};
use std::marker::PhantomData;
use thiserror::Error;

/// Errors that can occur during formula evaluation
///
/// These errors cover all failure modes in the formula system, from missing
/// parameters to type mismatches. Each error provides detailed context to
/// help diagnose issues during development and debugging.
#[derive(Error, Debug, Clone, PartialEq)]
pub enum FormulaEvaluationError {
    /// A required parameter is not present in the term mapping
    ///
    /// This occurs when a formula tries to read a parameter that wasn't
    /// provided in the Terms mapping when the formula was applied.
    ///
    /// # Example
    /// ```ignore
    /// let mut terms = Terms::new();
    /// // Missing "with" parameter!
    /// terms.insert("of".to_string(), Term::var("x"));
    ///
    /// let app = Sum::apply(terms);
    /// // Will fail with RequiredParameter { parameter: "with" }
    /// ```
    #[error("Formula application omits required parameter \"{parameter}\"")]
    RequiredParameter { parameter: String },

    /// A variable required by the formula is not bound in the input match
    ///
    /// This occurs when the formula's parameter is mapped to a variable,
    /// but that variable has no value in the current match frame.
    ///
    /// # Example
    /// ```ignore
    /// let input = Match::new();
    /// // Variable ?x is not bound!
    /// let result = formula.expand(input);
    /// // Fails with UnboundVariable for parameter "of" → Term::var("x")
    /// ```
    #[error("Variable {term} for '{parameter}' required parameter is not bound")]
    UnboundVariable {
        parameter: String,
        term: Term<Value>,
    },

    /// Attempt to write a value that conflicts with an existing binding
    ///
    /// This occurs when a formula tries to write a value to a variable
    /// that already has a different value bound to it. This maintains
    /// logical consistency in the query evaluation.
    ///
    /// # Example
    /// ```ignore
    /// let input = Match::new()
    ///     .set(Term::var("result"), 10)?; // Already bound to 10
    ///
    /// // Formula tries to write 8 to ?result
    /// let result = sum_formula.expand(input);
    /// // Fails with VariableInconsistency
    /// ```
    #[error(
        "Variable for the '{parameter}' is bound to {actual} which is inconsistent with value being set: {expected}"
    )]
    VariableInconsistency {
        parameter: String,
        actual: Term<Value>,
        expected: Term<Value>,
    },

    /// Type conversion failed when casting a Value to the requested type
    ///
    /// This occurs when using the Cast trait to convert a Value to a
    /// specific Rust type, but the Value's actual type is incompatible.
    ///
    /// # Example
    /// ```ignore
    /// let value = Value::String("hello".to_string());
    /// let number: u32 = u32::try_cast(&value)?;
    /// // Fails with TypeMismatch { expected: "u32", actual: "String" }
    /// ```
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },
}

/// Core trait for implementing formulas in the query system
///
/// The `Formula` trait defines the interface that all formulas must implement.
/// It provides a type-safe way to transform data during query evaluation.
///
/// # Type Parameters
///
/// - `Input`: The input type that can be constructed from a [`Cursor`].
///   This type should contain all the fields the formula needs to read.
/// - `Match`: Currently unused, reserved for future macro generation that
///   will create match patterns for formula applications.
///
/// # Implementation Guide
///
/// To implement a formula:
///
/// 1. Define an input type that implements `TryFrom<Cursor>`
/// 2. Implement `derive` to create output instances from input
/// 3. Implement `write` to write computed values back to the cursor
///
/// Most formulas should also implement the [`Compute`] trait to separate
/// the computation logic from the I/O operations.
///
/// # Example
///
/// See the module-level documentation for a complete example.
pub trait Formula: Sized {
    /// The input type for this formula
    ///
    /// This type must be constructible from a Cursor and should contain
    /// all the fields that the formula needs to read from the input.
    type Input: TryFrom<Cursor, Error = FormulaEvaluationError>;

    /// Match type for future pattern matching support
    ///
    /// Currently unused. In future versions, this will be used by macros
    /// to generate pattern matching code for formula applications in queries.
    type Match;

    /// Derive output instances from the input cursor
    ///
    /// This method is responsible for:
    /// 1. Reading input values from the cursor
    /// 2. Performing the formula's computation
    /// 3. Returning the computed output instances
    ///
    /// Most implementations will delegate to `Compute::compute` after
    /// extracting the input from the cursor.
    ///
    /// # Arguments
    /// * `cursor` - The cursor providing access to input values
    ///
    /// # Returns
    /// * `Ok(Vec<Self>)` - One or more output instances
    /// * `Err(_)` - If reading inputs fails or computation cannot proceed
    ///
    /// # Note
    /// Returning a `Vec` allows formulas to produce multiple outputs for
    /// a single input, enabling one-to-many transformations.
    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError>;

    /// Write this formula instance's output values to the cursor
    ///
    /// This method is called for each output instance produced by `derive`
    /// to write the computed values back to the cursor.
    ///
    /// # Arguments
    /// * `cursor` - The cursor to write output values to
    ///
    /// # Returns
    /// * `Ok(())` - If all writes succeeded
    /// * `Err(_)` - If writing fails (e.g., due to inconsistency)
    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError>;

    /// Convert derived outputs to Match instances
    ///
    /// This method orchestrates the full formula evaluation:
    /// 1. Calls `derive` to compute outputs
    /// 2. For each output, clones the cursor and calls `write`
    /// 3. Collects the resulting matches
    ///
    /// This default implementation should work for most formulas.
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
    ///
    /// This method binds the formula to specific term mappings, creating
    /// a [`FormulaApplication`] that can be evaluated over streams of matches.
    ///
    /// # Arguments
    /// * `terms` - Mapping from formula parameter names to query terms
    ///
    /// # Example
    /// ```ignore
    /// let mut terms = Terms::new();
    /// terms.insert("x".to_string(), Term::var("input"));
    /// terms.insert("y".to_string(), Term::var("output"));
    ///
    /// let app = MyFormula::apply(terms);
    /// ```
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
