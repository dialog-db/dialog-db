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
//! - **[`FormulaApplication`]** - Non-generic formula bound to term mappings, integrable with rules
//! - **[`Cursor`](crate::cursor::Cursor)** - Provides read/write access during evaluation
//! - **[`Dependencies`](crate::deductive_rule::Dependencies)** - Declares parameter requirements
//! - **Standard `TryFrom<Value>`** - Type conversion between Value and Rust types
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
//! ```
//! use dialog_query::formula::{Formula, Compute, FormulaApplication, FormulaEvaluationError};
//! use dialog_query::deductive_rule::{Terms, Dependencies};
//! use dialog_query::cursor::Cursor;
//! use dialog_query::{Term, Match, Value};
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
//!     fn name() -> &'static str {
//!         "sum"
//!     }
//!
//!     fn dependencies() -> Dependencies {
//!         let mut deps = Dependencies::new();
//!         deps.require("of".to_string());
//!         deps.require("with".to_string());
//!         deps.provide("is".to_string());
//!         deps
//!     }
//!
//!     fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
//!         let input = Self::Input::try_from(cursor.clone())?;
//!         Ok(Self::compute(input))
//!     }
//!
//!     fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
//!         cursor.write("is", &Value::UnsignedInt(self.is.into()))
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
//!     .set(Term::var("x"), 5u32).unwrap()
//!     .set(Term::var("y"), 3u32).unwrap();
//!
//! let results = formula_app.expand(input_match).unwrap();
//! assert_eq!(results[0].get::<u32>(&Term::var("result")).unwrap(), 8);
//! ```
//!
//! # Design Principles
//!
//! 1. **Type Safety** - Formulas work with strongly typed inputs and outputs
//! 2. **Integration** - Non-generic applications integrate seamlessly with rule system
//! 3. **Composability** - Formulas can be chained and combined in queries and rules
//! 4. **Separation of Concerns** - Logic (Compute) is separate from I/O (Cursor)
//! 5. **Dependency Declaration** - Clear parameter requirements for planning
//! 6. **Error Handling** - Clear error types for all failure modes
//! 7. **Performance** - Zero-cost abstractions where possible
//!
//! # Integration with Deductive Rules
//!
//! The non-generic `FormulaApplication` design allows formulas to be seamlessly integrated
//! with the deductive rule system. Formulas can now be used as premises in rules,
//! participate in query planning, and be stored alongside other rule applications.
//!
//! # Future Enhancements
//!
//! The formula system is designed to support future macro generation that will
//! automatically derive the boilerplate code, making formula definition as simple as:
//!
//! ```rust,ignore
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
use crate::deductive_rule::{Analysis, AnalyzerError, Dependencies, PlanError, Requirement, Terms};
use crate::{try_stream, EvaluationContext, Match, QueryError, Selection, Store, Term, Value};
use crate::{ValueDataType, VariableScope};
use std::fmt::Display;
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
    /// ```should_panic
    /// # use dialog_query::formula::{Sum, Formula};
    /// # use dialog_query::deductive_rule::Terms;
    /// # use dialog_query::{Term, Match, Value};
    /// let mut terms = Terms::new();
    /// // Missing "with" parameter!
    /// terms.insert("of".to_string(), Term::var("x"));
    ///
    /// let app = Sum::apply(terms);
    /// let input = Match::new().set(Term::var("x"), 5u32).unwrap();
    /// let result = app.expand(input).unwrap(); // Will panic with RequiredParameter
    /// ```
    #[error("Formula application omits required parameter \"{parameter}\"")]
    RequiredParameter { parameter: String },

    /// A variable required by the formula is not bound in the input match
    ///
    /// This occurs when the formula's parameter is mapped to a variable,
    /// but that variable has no value in the current match frame.
    ///
    /// # Example
    /// ```should_panic
    /// # use dialog_query::formula::{Sum, Formula};
    /// # use dialog_query::deductive_rule::Terms;
    /// # use dialog_query::{Term, Match, Value};
    /// # let mut terms = Terms::new();
    /// # terms.insert("of".to_string(), Term::var("x"));
    /// # terms.insert("with".to_string(), Term::var("y"));
    /// # terms.insert("is".to_string(), Term::var("result"));
    /// # let app = Sum::apply(terms);
    /// let input = Match::new();
    /// // Variable ?x is not bound!
    /// let result = app.expand(input).unwrap(); // Will panic with UnboundVariable
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
    /// # use dialog_query::formula::{Sum, Formula};
    /// # use dialog_query::deductive_rule::Terms;
    /// # use dialog_query::{Term, Match, Value};
    /// # let mut terms = Terms::new();
    /// # terms.insert("of".to_string(), Term::var("x"));
    /// # terms.insert("with".to_string(), Term::var("y"));
    /// # terms.insert("is".to_string(), Term::var("result"));
    /// # let app = Sum::apply(terms);
    /// let input = Match::new()
    ///     .set(Term::var("x"), 5u32).unwrap()
    ///     .set(Term::var("y"), 3u32).unwrap()
    ///     .set(Term::var("result"), 10u32).unwrap(); // Already bound to 10
    ///
    /// // Behavior when trying to write to already bound variable is TBD
    /// let result = app.expand(input);
    /// // Implementation details for handling inconsistencies are still being refined
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
    /// This occurs when using `TryFrom<Value>` to convert a Value to a
    /// specific Rust type, but the Value's actual type is incompatible.
    ///
    /// # Example
    /// ```ignore
    /// let value = Value::String("hello".to_string());
    /// let number: u32 = u32::try_cast(&value)?;
    /// // Fails with TypeMismatch { expected: "u32", actual: "String" }
    /// ```
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch {
        expected: ValueDataType,
        actual: ValueDataType,
    },
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
/// 2. Implement `name()` to return the formula's identifier
/// 3. Implement `dependencies()` to declare parameter requirements
/// 4. Implement `derive` to create output instances from input
/// 5. Implement `write` to write computed values back to the cursor
///
/// Most formulas should also implement the [`Compute`] trait to separate
/// the computation logic from the I/O operations.
///
/// # Example
///
/// See the module-level documentation for a complete example.
pub trait Formula: Sized + Clone {
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

    fn dependencies() -> Dependencies;

    fn name() -> &'static str;

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
    fn derive_match(cursor: &mut Cursor) -> Result<Vec<Match>, FormulaEvaluationError> {
        let outputs = Self::derive(cursor)?;
        let mut results = Vec::new();

        for output in outputs {
            output.write(cursor)?;
            results.push(cursor.source.clone());
        }

        Ok(results)
    }

    /// Create a formula application with term bindings
    ///
    /// This method binds the formula to specific term mappings, creating
    /// a non-generic [`FormulaApplication`] that can be evaluated over streams of matches
    /// and integrated with the deductive rule system.
    ///
    /// # Arguments
    /// * `terms` - Mapping from formula parameter names to query terms
    ///
    /// # Example
    /// ```ignore
    /// let mut terms = Terms::new();
    /// terms.insert("of".to_string(), Term::var("input1"));
    /// terms.insert("with".to_string(), Term::var("input2"));
    /// terms.insert("is".to_string(), Term::var("output"));
    ///
    /// let app = Sum::apply(terms);
    /// ```
    fn apply(terms: Terms) -> FormulaApplication {
        FormulaApplication {
            cost: 5,
            terms,
            name: Self::name(),
            dependencies: Self::dependencies(),
            compute: |cursor| Self::derive_match(cursor),
        }
    }
}

/// Trait for formulas that can compute their outputs
pub trait Compute: Formula + Sized {
    fn compute(input: Self::Input) -> Vec<Self>;
}

/// Non-generic formula application that can be evaluated over a stream of matches
///
/// This struct represents a formula that has been bound to specific term mappings.
/// Unlike the previous generic version, this can be stored alongside other applications
/// in the deductive rule system, allowing formulas to be used as premises in rules.
#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplication {
    pub cost: usize,
    /// Parameter name to term mappings
    pub terms: Terms,
    /// Formula identifier for error reporting and debugging
    pub name: &'static str,
    /// Parameter dependencies for planning and analysis
    pub dependencies: Dependencies,
    /// Function pointer to the formula's computation logic
    pub compute: fn(&mut Cursor) -> Result<Vec<Match>, FormulaEvaluationError>,
}

impl FormulaApplication {
    /// Expand a single match using this formula
    pub fn expand(&self, frame: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        let mut cursor = Cursor::new(frame, self.terms.clone());
        (self.compute)(&mut cursor)
    }

    pub fn analyze(&self) -> Result<Analysis, AnalyzerError> {
        Ok(Analysis {
            cost: 5,
            dependencies: self.dependencies.clone(),
        })
    }

    pub fn plan(&self, scope: &VariableScope) -> Result<FormulaApplicationPlan, PlanError> {
        let mut cost = self.cost;
        let mut provides = VariableScope::new();
        // We ensure that all terms for all required formula parametrs are
        // applied, otherwise we fail.
        for (name, requirement) in self.dependencies.iter() {
            let term = self.terms.get(name);
            match requirement {
                Requirement::Required => {
                    if let Some(parameter) = term {
                        if scope.contains(&parameter) {
                            Ok(())
                        } else {
                            Err(PlanError::UnboundFormulaParameter {
                                formula: self.name,
                                cell: name.into(),
                                parameter: parameter.clone(),
                            })
                        }
                    } else {
                        Err(PlanError::OmitsRequiredCell {
                            formula: self.name,
                            cell: name.into(),
                        })
                    }?;
                }
                Requirement::Derived(estimate) => match term {
                    Some(term) => {
                        provides.add(term);
                    }
                    None => {
                        cost += estimate;
                    }
                },
            }
        }

        Ok(FormulaApplicationPlan {
            cost,
            provides,
            terms: self.terms.clone(),
            name: self.name,
            dependencies: self.dependencies.clone(),
            compute: self.compute,
        })
    }
}

impl Display for FormulaApplication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.name)?;
        for (name, term) in self.terms.iter() {
            write!(f, "{}: {},", name, term)?;
        }
        write!(f, "}}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct FormulaApplicationPlan {
    pub cost: usize,
    /// Number of bindings it provides on evaluation
    pub provides: VariableScope,
    /// Parameter name to term mappings
    pub terms: Terms,
    /// Formula identifier for error reporting and debugging
    pub name: &'static str,
    /// Parameter dependencies for planning and analysis
    pub dependencies: Dependencies,
    /// Function pointer to the formula's computation logic
    pub compute: fn(&mut Cursor) -> Result<Vec<Match>, FormulaEvaluationError>,
}

impl FormulaApplicationPlan {
    pub fn cost(&self) -> usize {
        self.cost
    }
    pub fn provides(&self) -> &VariableScope {
        &self.provides
    }
    /// Evaluate the formula over a stream of matches
    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let terms = self.terms.clone();
        let compute = self.compute;
        try_stream! {

            for await source in context.selection {
                let frame = source?;
                let mut cursor = Cursor::new(frame, terms.clone());
                let expansion = compute(&mut cursor);
                // let expansion = self.expand(frame);
                // Map results and omit inconsistent matches
                let results = match expansion {
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

    fn name() -> &'static str {
        "sum"
    }
    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.require("with".into());
        dependencies.provide("is".into());

        dependencies
    }

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
    fn test_multiple_try_from_types() {
        // Test various data types with standard TryFrom<Value>
        let bool_val = Value::Boolean(true);
        assert_eq!(bool::try_from(bool_val).unwrap(), true);

        let f64_val = Value::Float(3.14);
        assert_eq!(f64::try_from(f64_val).unwrap(), 3.14);

        let string_val = Value::String("hello".to_string());
        assert_eq!(String::try_from(string_val).unwrap(), "hello");

        let u32_val = Value::UnsignedInt(42);
        assert_eq!(u32::try_from(u32_val).unwrap(), 42);

        let i32_val = Value::SignedInt(-10);
        assert_eq!(i32::try_from(i32_val).unwrap(), -10);
    }
}
