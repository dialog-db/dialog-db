//! Built-in formulas for common data transformations
//!
//! - Mathematical operations (sum, difference, product, quotient, modulo)
//! - String operations (concatenate, length, uppercase, lowercase)
//! - Type conversions (to_string, parse_number)
//! - Boolean logic (and, or, not)

/// Bindings for reading/writing values during formula evaluation.
pub mod bindings;
/// Formula cell types for parameter slot definitions.
pub mod cell;
/// Input type alias for formula input cells.
pub mod input;
/// Formula query for computed values.
pub mod query;

pub use bindings::*;
pub use cell::*;
pub use input::*;
pub use query::*;

/// Type conversion formulas (to_string, parse_number)
pub mod conversions;
/// Boolean logic formulas (and, or, not)
pub mod logic;
/// Mathematical operation formulas (sum, difference, product, quotient, modulo)
pub mod math;
/// String manipulation formulas (concatenate, length, uppercase, lowercase, like)
pub mod string;

pub use conversions::{ParseNumber, ToString};
pub use logic::{And, Not, Or};
pub use math::{Difference, Modulo, Product, Quotient, Sum};
pub use string::{Concatenate, Length, Like, Lowercase, Uppercase};

use crate::Predicate;
use crate::error::{FormulaEvaluationError, SchemaError};
use crate::selection::Answer;
use crate::{Parameters, Schema};

/// Core trait for implementing formulas in the query system
///
/// The `Formula` trait defines the interface that all formulas must implement.
/// It provides a type-safe way to transform data during query evaluation.
///
/// # Type Parameters
///
/// - `Input`: The input type that can be constructed from a [`Bindings`].
///   This type should contain all the fields the formula needs to read.
///
/// # Implementation Guide
///
/// To implement a formula:
///
/// 1. Define an input type that implements `TryFrom<Bindings>`
/// 2. Implement `name()` to return the formula's identifier
/// 3. Implement `dependencies()` to declare parameter requirements
/// 4. Implement `derive` to create output instances from input
/// 5. Implement `write` to write computed values back to the bindings
///
/// # Example
///
/// See the module-level documentation for a complete example.
pub trait Formula: Predicate + Sized + Clone {
    /// The input type for this formula
    ///
    /// This type must be constructible from a Bindings and should contain
    /// all the fields that the formula needs to read from the input.
    type Input: In;

    /// Returns the estimated cost of evaluating this formula.
    fn cost() -> usize;
    /// Returns the static cell definitions for this formula's parameters.
    fn cells() -> &'static Cells;
    /// Returns the operator name identifying this formula.
    fn operator() -> &'static str;

    /// Returns the schema derived from this formula's cell definitions.
    fn schema() -> Schema {
        Self::cells().into()
    }

    /// Returns an iterator over the operand names of this formula.
    fn operands(&self) -> impl Iterator<Item = &str> {
        Self::cells().keys()
    }

    /// Convert derived outputs to Answer instances with proper provenance
    ///
    /// This method orchestrates the full formula evaluation:
    /// 1. Calls `derive` to compute outputs
    /// 2. For each output, calls `write` to add values to bindings
    /// 3. Returns the Answer with Factor::Derived provenance
    ///
    /// This default implementation should work for most formulas.
    fn compute(bindings: &mut Bindings) -> Result<Vec<Answer>, FormulaEvaluationError> {
        let mut answers = Vec::new();
        let input: Self::Input = bindings.try_into()?;
        for output in Self::derive(input) {
            let mut bindings = bindings.clone();
            output.write(&mut bindings)?;
            answers.push(bindings.source);
        }

        Ok(answers)
    }

    /// This method contains actual logic for deriving an output from provided
    /// inputs.
    fn derive(input: Self::Input) -> Vec<Self>;

    /// Write this formula instance's output values to the bindings.
    ///
    /// This method is called for each output instance produced by `derive`
    /// to write the computed values back to the bindings.
    fn write(&self, bindings: &mut Bindings) -> Result<(), FormulaEvaluationError>;

    /// Create a formula application with term bindings
    ///
    /// This method binds the formula to specific term mappings, creating
    /// a non-generic [`FormulaQuery`] that can be evaluated over streams of matches
    /// and integrated with the deductive rule system.
    ///
    /// # Arguments
    /// * `terms` - Mapping from formula parameter names to query terms
    ///
    /// # Example
    /// ```no_run
    /// # use dialog_query::{Parameter, Parameters, Formula};
    /// # use dialog_query::formula::math::Sum;
    /// let mut terms = Parameters::new();
    /// terms.insert("of".to_string(), Parameter::var("input1"));
    /// terms.insert("with".to_string(), Parameter::var("input2"));
    /// terms.insert("is".to_string(), Parameter::var("output"));
    ///
    /// let app = Sum::apply(terms)?;
    /// # Ok::<(), dialog_query::error::SchemaError>(())
    /// ```
    fn apply(terms: Parameters) -> Result<FormulaQuery, SchemaError> {
        let cells = Self::cells();

        Ok(FormulaQuery {
            name: Self::operator(),
            cells,
            cost: Self::cost(),
            parameters: cells.conform(terms)?,
            compute: |bindings| Self::compute(bindings),
        })
    }
}

/// Trait alias for types that can be constructed from a [`Bindings`] as formula input.
pub trait In: for<'a> TryFrom<&'a mut Bindings, Error = FormulaEvaluationError> {}
impl<T: for<'a> TryFrom<&'a mut Bindings, Error = FormulaEvaluationError>> In for T {}
