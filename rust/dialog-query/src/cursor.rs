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
//! ```
//! use dialog_query::cursor::Cursor;
//! use dialog_query::{Term, Match, Value, Parameters};
//!
//! let mut parameters = Parameters::new();
//! parameters.insert("x".to_string(), Term::var("input_x"));
//! parameters.insert("result".to_string(), Term::var("output_y"));
//!
//! let source = Match::new()
//!     .set(Term::var("input_x"), 42u32).unwrap();
//!
//! let cursor = Cursor::new(source, parameters);
//! let x: u32 = cursor.read("x").unwrap();  // Reads from variable "input_x"
//! assert_eq!(x, 42);
//! ```

use crate::artifact::TypeError;
use crate::error::FormulaEvaluationError;
use crate::{Match, Parameters, Value};

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
    pub terms: Parameters,
}

// TODO: Rename cursor

impl Cursor {
    /// Create a new cursor from a match and term mappings
    ///
    /// # Arguments
    /// * `source` - The match containing current variable bindings
    /// * `terms` - Mapping from formula parameter names to query terms
    pub fn new(source: Match, terms: Parameters) -> Self {
        Self { source, terms }
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
    /// # Example
    /// ```
    /// # use dialog_query::cursor::Cursor;
    /// # use dialog_query::{Term, Match, Value, Parameters};
    /// # let mut parameters = Parameters::new();
    /// # parameters.insert("x".to_string(), Term::var("test_x"));
    /// # parameters.insert("name".to_string(), Term::var("test_name"));
    /// # let input = Match::new()
    /// #     .set(Term::var("test_x"), 42u32).unwrap()
    /// #     .set(Term::var("test_name"), "hello".to_string()).unwrap();
    /// # let cursor = Cursor::new(input, parameters);
    /// let x: u32 = cursor.read("x").unwrap();
    /// let name: String = cursor.read("name").unwrap();
    /// assert_eq!(x, 42);
    /// assert_eq!(name, "hello");
    /// ```
    pub fn read<T: TryFrom<Value, Error = TypeError>>(
        &self,
        key: &str,
    ) -> Result<T, FormulaEvaluationError> {
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

        T::try_from(value).map_err(|e| {
            let TypeError::TypeMismatch(expected, actual) = e;
            FormulaEvaluationError::TypeMismatch { expected, actual }
        })
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
    /// ```
    /// # use dialog_query::cursor::Cursor;
    /// # use dialog_query::{Term, Match, Value, Parameters};
    /// # let mut parameters = Parameters::new();
    /// # parameters.insert("result".to_string(), Term::var("output"));
    /// # let source = Match::new();
    /// # let mut cursor = Cursor::new(source, parameters);
    /// cursor.write("result", &Value::UnsignedInt(42)).unwrap();
    /// let result: u32 = cursor.read("result").unwrap();
    /// assert_eq!(result, 42);
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
    pub fn terms(&self) -> &Parameters {
        &self.terms
    }

    /// Merge values from a frame into the source match using the parameter mapping.
    ///
    /// For each parameter in the cursor's terms:
    /// - If it's a variable in the terms, look up the corresponding value in the frame
    /// - Unify that value with the variable in the source match
    ///
    /// This is the reverse operation of creating an initial match - it takes results
    /// from evaluation and merges them back into the original context.
    ///
    /// # Arguments
    /// * `frame` - The match containing evaluation results to merge
    ///
    /// # Returns
    /// * `Ok(Match)` - New match with source data plus merged frame values
    /// * `Err(InconsistencyError)` - If unification fails
    pub fn merge(&self, frame: &Match) -> Result<Match, crate::InconsistencyError> {
        let mut output = self.source.clone();

        for (param_name, term) in self.terms.iter() {
            // If this parameter is a variable with a name, map it from frame to output
            if let crate::Term::Variable {
                name: Some(var_name),
                ..
            } = term
            {
                // Look up the value in the frame using the parameter name
                if let Some(value) = frame.variables.get(param_name.as_str()) {
                    // Bind the value to our variable name in the output
                    output = output.unify(crate::Term::<Value>::var(var_name), value.clone())?;
                }
            }
        }

        Ok(output)
    }
}

/// Convert a Cursor into a Match by extracting all resolved constants.
///
/// This creates an initial match for evaluation by:
/// 1. Resolving all terms in the cursor using the source match
/// 2. Unifying any resolved constants into a new match
///
/// This is useful for creating the starting point for query evaluation.
impl TryFrom<&Cursor> for Match {
    type Error = crate::InconsistencyError;

    fn try_from(cursor: &Cursor) -> Result<Self, Self::Error> {
        let mut result = Match::new();

        for (name, term) in cursor.terms.iter() {
            let resolved = cursor.source.resolve(term);

            // If resolved to a constant, bind it in the result match
            if let crate::Term::Constant(value) = resolved {
                result = result.unify(crate::Term::<Value>::var(name), value)?;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Term;

    #[test]
    fn test_cursor_read_write() {
        let mut terms = Parameters::new();
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
        let terms = Parameters::new(); // Empty terms
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
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("unbound").into());

        let source = Match::new(); // No bindings
        let cursor = Cursor::new(source, terms);

        let result = cursor.read::<u32>("value");
        assert!(matches!(
            result,
            Err(FormulaEvaluationError::UnboundVariable { .. })
        ));
    }

    #[test]
    fn test_cursor_try_from_extracts_constants() {
        use crate::artifact::Entity;

        // Create parameters with mixed terms
        let mut params = Parameters::new();
        params.insert("this".to_string(), Term::var("entity"));
        params.insert(
            "name".to_string(),
            Term::Constant(Value::String("Alice".to_string())),
        );
        params.insert("age".to_string(), Term::var("person_age"));

        // Create a match with some variable bindings
        let entity = Entity::new().expect("entity creation should succeed");
        let mut input = Match::new();
        input = input
            .set(Term::var("entity"), entity.clone())
            .expect("set should succeed");
        input = input
            .set(Term::var("person_age"), 25u32)
            .expect("set should succeed");

        // Create cursor and convert to Match
        let cursor = Cursor::new(input, params);
        let result_match = Match::try_from(&cursor).expect("conversion should succeed");

        // Check that the result match has all resolved constants bound
        assert_eq!(
            result_match.get(&Term::<Value>::var("this")).ok(),
            Some(Value::Entity(entity)),
            "Variable 'entity' should be bound to entity constant"
        );
        assert_eq!(
            result_match.get(&Term::<Value>::var("name")).ok(),
            Some(Value::String("Alice".to_string())),
            "Constant 'Alice' should be bound to name"
        );
        assert_eq!(
            result_match.get(&Term::<Value>::var("age")).ok(),
            Some(Value::UnsignedInt(25)),
            "Variable 'person_age' should be bound to 25"
        );
    }

    #[test]
    fn test_cursor_merge_applies_frame() {
        use crate::artifact::Entity;

        // Setup: cursor with parameters mapping implicit names to user variables
        let mut params = Parameters::new();
        params.insert("this".to_string(), Term::var("my_entity"));
        params.insert("name".to_string(), Term::var("my_name"));

        let entity = Entity::new().expect("entity creation should succeed");
        let source = Match::new()
            .set(Term::var("original"), "original_value".to_string())
            .expect("set should succeed");

        let cursor = Cursor::new(source, params);

        // Frame has values for the implicit variable names
        let frame = Match::new()
            .set(Term::var("this"), entity.clone())
            .expect("set should succeed")
            .set(Term::var("name"), "Alice".to_string())
            .expect("set should succeed");

        // Merge should map frame values to user variable names
        let output = cursor.merge(&frame).expect("merge should succeed");

        // Check that:
        // 1. Original source data is preserved
        assert_eq!(
            output.get(&Term::<String>::var("original")).ok(),
            Some("original_value".to_string()),
            "Original source value should be preserved"
        );

        // 2. Frame values are mapped to user variable names
        assert_eq!(
            output.get(&Term::<Value>::var("my_entity")).ok(),
            Some(Value::Entity(entity)),
            "Frame 'this' should be mapped to 'my_entity'"
        );
        assert_eq!(
            output.get(&Term::<String>::var("my_name")).ok(),
            Some("Alice".to_string()),
            "Frame 'name' should be mapped to 'my_name'"
        );
    }
}
