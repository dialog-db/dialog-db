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
//! use dialog_query::{Formula, Compute, Parameters, Term, Match, Value, Dependencies};
//! use dialog_query::application::{FormulaApplication};
//! use dialog_query::error::FormulaEvaluationError;
//! use dialog_query::cursor::Cursor;
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
//! let mut parameters = Parameters::new();
//! parameters.insert("of".to_string(), Term::var("x"));
//! parameters.insert("with".to_string(), Term::var("y"));
//! parameters.insert("is".to_string(), Term::var("result"));
//!
//! let sum = Sum::apply(parameters);
//!
//! // Apply to a match with x=5, y=3
//! let source = Match::new()
//!     .set(Term::var("x"), 5u32).unwrap()
//!     .set(Term::var("y"), 3u32).unwrap();
//!
//! let results = sum.derive(source).unwrap();
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

use crate::application::FormulaApplication;
use crate::cursor::Cursor;
use crate::error::FormulaEvaluationError;
use crate::Match;
use crate::{Dependencies, Parameters};

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
    fn apply(terms: Parameters) -> FormulaApplication {
        FormulaApplication {
            cost: 5,
            parameters: terms,
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
