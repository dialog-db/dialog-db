//! Cursor for reading values during formula evaluation
//!
//! The `Cursor` type provides a controlled interface for formulas
//! to read input values during evaluation. It maintains a mapping between
//! formula parameter names and their corresponding terms in the evaluation context,
//! and tracks which values were read for provenance tracking.
//!
//! # Overview
//!
//! A cursor consists of:
//! - An `Answer` containing the current variable bindings and provenance
//! - A `Parameters` mapping from parameter names to their term representations
//! - Read tracking to record dependencies for Factor::Derived creation
//!
//! # Example
//!
//! ```ignore
//! use dialog_query::cursor::Cursor;
//! use dialog_query::{Term, Answer, Value, Parameters};
//!
//! let mut parameters = Parameters::new();
//! parameters.insert("x".to_string(), Term::var("input_x"));
//!
//! let source = Answer::new();
//! // ... populate answer with factors
//!
//! let mut cursor = Cursor::new(formula, source, parameters);
//! let x: u32 = cursor.read("x").unwrap();  // Reads from variable "input_x"
//! ```

use crate::artifact::TypeError;
use crate::error::FormulaEvaluationError;
use crate::selection::{Answer, Factors};
use crate::{Parameters, Value};
use std::sync::Arc;

/// A cursor for reading and writing values during formula evaluation
///
/// The cursor provides a mapping layer between formula parameter names and
/// the actual terms used in the evaluation context. It tracks all reads
/// to enable proper provenance tracking for derived values.
///
/// Cursors are specific to formula evaluation and should not be used for other purposes.
#[derive(Debug, Clone)]
pub struct Cursor {
    /// The current answer containing variable bindings and provenance
    ///
    /// NOTE: Public for compatibility with existing formula implementations.
    /// Use `source()` accessor method instead where possible.
    pub source: Answer,

    /// Mapping from parameter names to query terms
    ///
    /// NOTE: Public for compatibility with existing formula implementations.
    /// Use `terms()` accessor method instead where possible.
    pub terms: Parameters,

    /// Tracks which parameters were read (for Factor::Derived provenance)
    reads: std::collections::HashMap<String, Factors>,

    /// The formula application that is evaluating this cursor
    /// Used to create Factor::Derived with proper provenance
    /// Stored as Arc to avoid cloning the entire FormulaApplication
    formula: Arc<crate::application::formula::FormulaApplication>,
}

impl Cursor {
    /// Create a new cursor for formula evaluation
    ///
    /// # Arguments
    /// * `formula` - The formula application being evaluated (for provenance tracking)
    /// * `source` - The answer containing current variable bindings and provenance
    /// * `terms` - Mapping from formula parameter names to query terms
    pub fn new(
        formula: Arc<crate::application::formula::FormulaApplication>,
        source: impl Into<Answer>,
        terms: Parameters,
    ) -> Self {
        Self {
            source: source.into(),
            terms,
            reads: std::collections::HashMap::new(),
            formula,
        }
    }

    /// Read a typed value from the cursor using a parameter name
    ///
    /// This method:
    /// 1. Looks up the parameter name in the terms mapping
    /// 2. Resolves the corresponding term to get its value
    /// 3. Converts the value to the requested type
    ///
    /// # Type Parameters
    /// * `T` - The type to convert the value to (must implement `TryFrom<Value>`)
    ///
    /// # Arguments
    /// * `key` - The formula parameter name
    ///
    /// # Returns
    /// * `Ok(T)` - The value cast to the requested type
    /// * `Err(RequiredParameter)` - If the parameter is not in the terms mapping
    /// * `Err(UnboundVariable)` - If the term's variable is not bound
    /// * `Err(TypeMismatch)` - If the value cannot be converted to type T
    ///
    pub fn read<T: TryFrom<Value, Error = TypeError>>(
        &mut self,
        key: &str,
    ) -> Result<T, FormulaEvaluationError> {
        Ok(T::try_from(self.resolve(key)?)?)
    }

    pub fn resolve(&mut self, key: &str) -> Result<Value, FormulaEvaluationError> {
        let term =
            self.terms
                .get(key)
                .ok_or_else(|| FormulaEvaluationError::RequiredParameter {
                    parameter: key.into(),
                })?;

        // Track what we read for provenance
        if let Some(factors) = self.source.resolve_factors(term) {
            self.reads.insert(key.to_string(), factors.clone());
        }

        // Get the value from the answer
        let value =
            self.source
                .resolve(term)
                .map_err(|_| FormulaEvaluationError::UnboundVariable {
                    term: term.clone(),
                    parameter: key.into(),
                })?;

        Ok(value)
    }

    /// Get an immutable reference to the source answer
    ///
    /// This can be useful for accessing the answer's conclusions directly
    /// without going through the parameter mapping.
    pub fn source(&self) -> &Answer {
        &self.source
    }

    /// Get an immutable reference to the terms mapping
    ///
    /// This exposes the mapping between parameter names and query terms.
    pub fn terms(&self) -> &Parameters {
        &self.terms
    }

    /// Get the tracked reads (which parameters were read during evaluation)
    ///
    /// Returns a mapping from parameter names to the Factors that were read.
    /// This is used to create Factor::Derived with proper provenance.
    pub fn reads(&self) -> &std::collections::HashMap<String, Factors> {
        &self.reads
    }

    /// Consume the cursor and return the source answer and reads
    ///
    /// This is typically used after formula evaluation to access the
    /// provenance information for creating derived factors.
    pub fn into_parts(self) -> (Answer, std::collections::HashMap<String, Factors>) {
        (self.source, self.reads)
    }

    // ===== Compatibility methods for Match-based formulas =====
    // These will be removed once formulas are updated to work with Answer

    /// Write a value to the cursor
    ///
    /// Creates a Factor::Derived with proper provenance tracking from the formula
    /// and tracked reads. Fails if the parameter key is not in the terms mapping.
    ///
    /// # Arguments
    /// * `key` - The parameter name to write to (must exist in terms)
    /// * `value` - The value to write
    ///
    /// # Returns
    /// * `Ok(())` - Value written successfully
    /// * `Err(RequiredParameter)` - If key is not in terms mapping
    /// * `Err(VariableInconsistency)` - If assignment conflicts with existing value
    pub fn write(&mut self, key: &str, value: &Value) -> Result<(), FormulaEvaluationError> {
        use crate::selection::Factor;

        // Fail if parameter not in terms (don't silently ignore)
        let term =
            self.terms
                .get(key)
                .ok_or_else(|| FormulaEvaluationError::RequiredParameter {
                    parameter: key.into(),
                })?;

        // Create a Derived factor with proper provenance
        let factor = Factor::Derived {
            value: value.clone(),
            from: self.reads.clone(), // Use the tracked reads as dependencies
            formula: Arc::clone(&self.formula), // Clone the Arc, not the FormulaApplication
        };

        // Assign to the answer - this will fail if there's a conflicting value
        self.source.assign(term, &factor).map_err(|_| {
            // Convert assignment errors to VariableInconsistency
            FormulaEvaluationError::VariableInconsistency {
                parameter: key.into(),
                actual: term.clone(),
                expected: crate::Term::Constant(value.clone()),
            }
        })?;

        Ok(())
    }
}

impl From<TypeError> for FormulaEvaluationError {
    fn from(error: TypeError) -> Self {
        let TypeError::TypeMismatch(expected, actual) = error;
        FormulaEvaluationError::TypeMismatch { expected, actual }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;

    // Helper to create a test formula for cursor tests
    fn test_formula() -> crate::application::formula::FormulaApplication {
        use std::sync::OnceLock;
        static EMPTY_CELLS: OnceLock<crate::predicate::formula::Cells> = OnceLock::new();
        let cells = EMPTY_CELLS.get_or_init(crate::predicate::formula::Cells::new);

        crate::application::formula::FormulaApplication {
            name: "test",
            compute: |_| Ok(vec![]),
            cost: 0,
            parameters: crate::Parameters::new(),
            cells,
        }
    }

    #[dialog_common::test]
    fn test_cursor_read() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("test"));

        let match_data = Answer::new()
            .set(Term::var("test"), 42u32)
            .expect("Failed to create test match");

        let source = match_data;
        let formula = test_formula();
        let mut cursor = Cursor::new(Arc::new(formula.clone()), source, terms);

        // Test reading
        let value = cursor.read::<u32>("value").expect("Failed to read value");
        assert_eq!(value, 42);

        // Verify read was tracked
        assert_eq!(cursor.reads().len(), 1);
        assert!(cursor.reads().contains_key("value"));
    }

    #[dialog_common::test]
    fn test_cursor_missing_parameter() {
        use crate::selection::Answer;

        let terms = Parameters::new(); // Empty terms
        let source = Answer::new();
        let formula = test_formula();
        let mut cursor = Cursor::new(Arc::new(formula.clone()), source, terms);

        let result = cursor.read::<u32>("missing");
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::RequiredParameter { .. })
        ));
    }

    #[dialog_common::test]
    fn test_cursor_unbound_variable() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("unbound"));

        let source = Answer::new(); // No bindings
        let formula = test_formula();
        let mut cursor = Cursor::new(Arc::new(formula.clone()), source, terms);

        let result = cursor.read::<u32>("value");
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::UnboundVariable { .. })
        ));
    }

    #[dialog_common::test]
    fn test_cursor_read_tracks_provenance() {
        use crate::selection::Answer;

        let mut params = Parameters::new();
        params.insert("x".to_string(), Term::var("input_x"));
        params.insert("y".to_string(), Term::var("input_y"));

        let match_data = Answer::new()
            .set(Term::var("input_x"), 10u32)
            .expect("set should succeed")
            .set(Term::var("input_y"), 20u32)
            .expect("set should succeed");

        let source = match_data;
        let formula = test_formula();
        let mut cursor = Cursor::new(Arc::new(formula.clone()), source, params);

        // Initially no reads tracked
        assert_eq!(cursor.reads().len(), 0);

        // Read x
        let _x = cursor.read::<u32>("x").expect("read should succeed");
        assert_eq!(cursor.reads().len(), 1);
        assert!(cursor.reads().contains_key("x"));

        // Read y
        let _y = cursor.read::<u32>("y").expect("read should succeed");
        assert_eq!(cursor.reads().len(), 2);
        assert!(cursor.reads().contains_key("y"));
    }

    #[dialog_common::test]
    fn test_answer_rejects_conflicting_assignment() {
        use crate::selection::{Answer, Factor};
        use std::collections::HashMap;
        use std::sync::Arc;

        // Create an Answer with a value already bound
        let match_data = Answer::new()
            .set(Term::var("test"), 42u32)
            .expect("set should succeed");

        let mut answer = match_data;

        // Try to assign a conflicting value
        use std::sync::OnceLock;
        static EMPTY_CELLS: OnceLock<crate::predicate::formula::Cells> = OnceLock::new();
        let cells = EMPTY_CELLS.get_or_init(crate::predicate::formula::Cells::new);

        let conflicting_factor = Factor::Derived {
            value: Value::UnsignedInt(100),
            from: HashMap::new(),
            formula: Arc::new(crate::application::formula::FormulaApplication {
                name: "test",
                compute: |_| Ok(vec![]),
                cost: 0,
                parameters: crate::Parameters::new(),
                cells,
            }),
        };

        // This should fail because "test" is already bound to 42
        let result = answer.assign(&Term::var("test"), &conflicting_factor);
        assert!(
            result.is_err(),
            "Answer.assign() should reject conflicting value assignment"
        );
    }

    #[dialog_common::test]
    #[allow(deprecated)]
    fn test_cursor_write_rejects_conflicting_value() {
        use crate::selection::Answer;

        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("test"));

        // Create cursor with initial value
        let source = Answer::new()
            .set(Term::var("test"), 42u32)
            .expect("Failed to create test match");

        let formula = test_formula();
        let mut cursor = Cursor::new(Arc::new(formula.clone()), source, terms);

        // Read the initial value to verify it's there
        let value = cursor.read::<u32>("value").expect("Failed to read value");
        assert_eq!(value, 42);

        // Try to write a conflicting value - this should fail
        let conflicting_value = Value::UnsignedInt(100);
        let result = cursor.write("value", &conflicting_value);

        assert!(
            result.is_err(),
            "Cursor.write() should reject conflicting value. \
             Got Ok() but expected Err() when writing {} to variable already bound to {}",
            100,
            42
        );

        // Verify the original value is unchanged
        let unchanged_value = cursor
            .read::<u32>("value")
            .expect("Failed to read value after conflict");
        assert_eq!(
            unchanged_value, 42,
            "Original value should remain unchanged after failed write"
        );
    }

    #[dialog_common::test]
    fn test_cursor_into_parts() {
        use crate::selection::Answer;

        let mut params = Parameters::new();
        params.insert("x".to_string(), Term::var("input_x"));
        params.insert("y".to_string(), Term::var("input_y"));

        let match_data = Answer::new()
            .set(Term::var("input_x"), 10u32)
            .expect("set should succeed")
            .set(Term::var("input_y"), 20u32)
            .expect("set should succeed");

        let source = match_data;
        let formula = test_formula();
        let mut cursor = Cursor::new(Arc::new(formula.clone()), source, params);

        // Read some values to track reads
        let _x = cursor.read::<u32>("x").expect("read should succeed");
        let _y = cursor.read::<u32>("y").expect("read should succeed");

        // Consume cursor and check parts
        let (answer, reads) = cursor.into_parts();

        // Verify we got the answer back
        assert_eq!(
            answer
                .resolve(&Term::<u32>::var("input_x"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(10u32)
        );

        // Verify reads were tracked
        assert_eq!(reads.len(), 2);
        assert!(reads.contains_key("x"));
        assert!(reads.contains_key("y"));
    }
}
