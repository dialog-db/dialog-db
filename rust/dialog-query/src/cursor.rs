//! Cursor for reading and writing values in predicate application
//!
//! The `Cursor` type provides a controlled interface for predicate application
//! to read input values and write output values during evaluation. It maintains
//! a mapping between application parameter names and their corresponding terms
//! in the evaluation context.
//!
//! # Overview
//!
//! A cursor consists of:
//! - A `Match` containing the current variable bindings
//! - A `Terms` mapping from parameter names to their term representations
//!
//! This design allows predicate applications to work with named parameters
//! independently of how those parameters are bound in the evaluation context.
//!
//! # Example
//!
//! ```ignore
//! let mut terms = Terms::new();
//! terms.insert("x".to_string(), Term::var("input_x"));
//! terms.insert("result".to_string(), Term::var("output_y"));
//!
//! let match_frame = Match::new()
//!     .set(Term::var("input_x"), 42u32)?;
//!
//! let cursor = Cursor::new(match_frame, terms);
//! let x_value: u32 = cursor.read("x")?;  // Reads from variable "input_x"
//! ```

use crate::deductive_rule::Terms;
use crate::formula::FormulaEvaluationError;
use crate::value::Cast;
use crate::{Match, Value};

/// A cursor for reading from and writing to matches during formula evaluation
///
/// The cursor provides a mapping layer between application parameter names and
/// the actual terms used in the evaluation context. This abstraction allows
/// predicate applications to be defined independently of their usage context.
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    /// The current match containing variable bindings
    pub source: Match,
    /// Mapping from parameter names to query terms
    pub terms: Terms,
}

impl Cursor {
    /// Create a new cursor from a match and term mappings
    ///
    /// # Arguments
    /// * `source` - The match containing current variable bindings
    /// * `terms` - Mapping from formula parameter names to query terms
    pub fn new(source: Match, terms: Terms) -> Self {
        Self { source, terms }
    }

    /// Read a typed value from the cursor using a parameter name
    ///
    /// This method:
    /// 1. Looks up the parameter name in the terms mapping
    /// 2. Resolves the corresponding term to get its value
    /// 3. Casts the value to the requested type
    ///
    /// # Type Parameters
    /// * `T` - The type to cast the value to (must implement `Cast`)
    ///
    /// # Arguments
    /// * `key` - The formula parameter name
    ///
    /// # Returns
    /// * `Ok(T)` - The value cast to the requested type
    /// * `Err(RequiredParameter)` - If the parameter is not in the terms mapping
    /// * `Err(UnboundVariable)` - If the term's variable is not bound
    /// * `Err(TypeMismatch)` - If the value cannot be cast to type T
    ///
    /// # Example
    /// ```ignore
    /// let x: u32 = cursor.read("x")?;
    /// let name: String = cursor.read("name")?;
    /// ```
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

    /// Write a value to the cursor using a parameter name
    ///
    /// This method:
    /// 1. Looks up the parameter name in the terms mapping
    /// 2. Unifies the value with the corresponding term
    /// 3. Updates the cursor's match with the new binding
    ///
    /// If the term is already bound to a different value, this will fail
    /// with a `VariableInconsistency` error.
    ///
    /// # Arguments
    /// * `key` - The formula parameter name
    /// * `value` - The value to write
    ///
    /// # Returns
    /// * `Ok(())` - If the write succeeded
    /// * `Err(VariableInconsistency)` - If the term is already bound to a different value
    ///
    /// # Example
    /// ```ignore
    /// cursor.write("result", &Value::UnsignedInt(42))?;
    /// ```
    pub fn write(&mut self, key: &str, value: &Value) -> Result<(), FormulaEvaluationError> {
        if let Some(term) = self.terms.get(key) {
            self.source = self
                .source
                .unify_value(term.clone(), value.clone())
                .map_err(|_| FormulaEvaluationError::VariableInconsistency {
                    parameter: key.into(),
                    expected: term.clone(),
                    actual: self.source.resolve(term),
                })?;
        }

        Ok(())
    }

    /// Get an immutable reference to the underlying match
    ///
    /// This can be useful for accessing the match's variable bindings directly
    /// without going through the parameter mapping.
    pub fn source(&self) -> &Match {
        &self.source
    }

    /// Get an immutable reference to the terms mapping
    ///
    /// This exposes the mapping between parameter names and query terms.
    pub fn terms(&self) -> &Terms {
        &self.terms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;

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
    fn test_cursor_missing_parameter() {
        let terms = Terms::new(); // Empty terms
        let source = Match::new();
        let cursor = Cursor::new(source, terms);

        let result = cursor.read::<u32>("missing");
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::RequiredParameter { .. })
        ));
    }

    #[test]
    fn test_cursor_unbound_variable() {
        let mut terms = Terms::new();
        terms.insert("value".to_string(), Term::var("unbound").into());

        let source = Match::new(); // No bindings
        let cursor = Cursor::new(source, terms);

        let result = cursor.read::<u32>("value");
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::UnboundVariable { .. })
        ));
    }
}
