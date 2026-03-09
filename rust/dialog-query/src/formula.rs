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

pub use conversions::{ParseFloat, ParseSignedInteger, ParseUnsignedInteger, ToString};
pub use logic::{And, Not, Or};
pub use math::{Difference, Modulo, Product, Quotient, Sum};
pub use string::{Concatenate, Length, Like, Lowercase, Uppercase};

use crate::Parameters;
use crate::Predicate;
use crate::Schema;
use crate::error::{EvaluationError, TypeError};
use crate::selection::Match;

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
/// 4. Implement `compute` to create output instances from input
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

    /// Returns the schema derived from this formula's cell definitions.
    fn schema() -> Schema {
        Self::cells().into()
    }

    /// Returns an iterator over the operand names of this formula.
    fn operands(&self) -> impl Iterator<Item = &str> {
        Self::cells().keys()
    }

    /// Resolve bindings by orchestrating the full read-compute-write cycle.
    ///
    /// 1. Calls `compute` to produce outputs from input
    /// 2. For each output, calls `write` to add values to bindings
    /// 3. Returns the Match with the output values bound
    ///
    /// This default implementation should work for most formulas.
    fn resolve(bindings: &mut Bindings) -> Result<Vec<Match>, EvaluationError> {
        let mut results = Vec::new();
        let input: Self::Input = bindings.try_into()?;
        for output in Self::compute(input) {
            let mut bindings = bindings.clone();
            output.write(&mut bindings)?;
            results.push(bindings.source);
        }

        Ok(results)
    }

    /// Create a formula application from raw parameters.
    ///
    /// Validates parameters against the formula's cell definitions,
    /// then constructs the typed application struct.
    fn apply(terms: Parameters) -> Result<<Self as Predicate>::Application, TypeError>;

    /// Pure computation: given bound inputs, produce output instances.
    fn compute(input: Self::Input) -> Vec<Self>;

    /// Write this formula instance's output values to the bindings.
    ///
    /// This method is called for each output instance produced by `derive`
    /// to write the computed values back to the bindings.
    fn write(&self, bindings: &mut Bindings) -> Result<(), EvaluationError>;
}

/// Trait alias for types that can be constructed from a [`Bindings`] as formula input.
pub trait In: for<'a> TryFrom<&'a mut Bindings, Error = EvaluationError> {}
impl<T: for<'a> TryFrom<&'a mut Bindings, Error = EvaluationError>> In for T {}
