//! Formula system for query evaluation
//!
//! This module provides a type-safe formula expansion system that allows
//! for transforming data through user-defined formulas while maintaining
//! type safety at compile time and enabling dynamic dispatch at runtime.

use crate::{try_stream, EvaluationContext, QueryError, Selection, Store};
use crate::{Match, Term};

use std::fmt::{self, Debug, Display};
use thiserror::Error;

/// Core trait for defining formulas with strongly-typed inputs and outputs.
///
/// # Design Decisions
///
/// - **Sized + Clone + Debug**: These bounds ensure that formulas can be:
///   - Stored by value (Sized)
///   - Cloned for use in async contexts (Clone)
///   - Inspected for debugging (Debug)
///
/// - **Associated types**: Input/Output types provide compile-time type safety
///   while allowing each formula to define its own data contract
///
/// # Example
/// ```ignore
/// struct Increment;
/// impl Formula for Increment {
///     type Input = IncInput;
///     type Output = IncOutput;
///
///     fn expand(&self, input: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError> {
///         Ok(vec![IncOutput { value: input.value + 1 }])
///     }
/// }
/// ```
pub trait Formula: Sized + Clone + Debug {
    /// The input type this formula expects
    type Input;

    /// The output type this formula produces
    type Output;

    /// Expands the input into zero or more output values.
    ///
    /// Formulas can:
    /// - Filter (return empty vec)
    /// - Transform (return single item)
    /// - Expand (return multiple items)
    fn expand(&self, terms: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError>;

    /// Converts this formula into a type-erased FormulaApplication.
    ///
    /// This enables dynamic dispatch while preserving the formula's behavior.
    /// The FormulaWrapper bound ensures the formula can be converted to/from Match types.
    fn new(self) -> FormulaApplication
    where
        Self: FormulaWrapper,
    {
        FormulaApplication {
            terms: Match::new(),
            formula: Box::new(self) as Box<dyn FormulaWrapper>,
        }
    }
}

/// Errors that can occur during formula evaluation
#[derive(Error, Debug, Clone, PartialEq)]
pub enum FormulaEvaluationError {
    /// A required variable was not found in the input Match
    #[error("Required cell '{name}' has no value")]
    ReadError { name: String },

    /// Failed to convert input Match to formula's Input type
    #[error("Failed to convert input: {message}")]
    InputConversionError { message: String },

    /// Failed to convert formula's Output type to Match
    #[error("Failed to convert output: {message}")]
    OutputConversionError { message: String },

    /// Type mismatch when reading from Match
    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    /// Generic conversion error with details
    #[error("Conversion error: {0}")]
    ConversionError(String),
}

/// Implementation allowing Box<T> to be used as a Formula where T is a Formula.
///
/// This enables heap allocation of formulas when needed, particularly useful
/// for recursive formula structures or when the formula type isn't known at compile time.
impl<T: ?Sized> Formula for Box<T>
where
    T: Formula,
{
    type Input = T::Input;
    type Output = T::Output;

    fn expand(&self, args: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError> {
        (**self).expand(args)
    }
}

/// Type-erased wrapper trait for formulas that enables dynamic dispatch.
///
/// # Design Decisions
///
/// - **Send + Sync**: Required for use in async contexts and thread safety
/// - **Debug**: Enables inspection of boxed formulas for debugging
/// - **'static**: Required for async streams - ensures no borrowed data
/// - **No Clone supertrait**: Clone cannot be object-safe due to returning Self,
///   so we provide clone_box() instead for cloning trait objects
///
/// This trait acts as a bridge between the strongly-typed Formula trait
/// and the dynamically-typed query evaluation system.
pub trait FormulaWrapper: Send + Sync + Debug + 'static {
    /// Apply this formula to a Match, producing zero or more result Matches
    fn apply(&self, terms: Match) -> Result<Vec<Match>, FormulaEvaluationError>;

    /// Clone this formula into a new boxed trait object.
    ///
    /// This method works around the object-safety limitation of Clone::clone(),
    /// which returns Self and thus cannot be used with trait objects.
    fn clone_box(&self) -> Box<dyn FormulaWrapper>;
}

/// Blanket implementation that makes any Formula automatically implement FormulaWrapper.
///
/// # Type Constraints
///
/// - `I: TryFrom<Match>`: Input must be constructible from Match with FormulaEvaluationError
/// - `O: TryInto<Match>`: Output must be convertible to Match with FormulaEvaluationError
///
/// This implementation handles the type conversions between the strongly-typed
/// formula domain and the dynamically-typed Match domain. Both conversions must
/// use FormulaEvaluationError as their error type to provide consistent error handling.
impl<T, I, O> FormulaWrapper for T
where
    T: Formula<Input = I, Output = O> + Send + Sync + Clone + 'static,
    I: TryFrom<Match, Error = FormulaEvaluationError>,
    O: TryInto<Match, Error = FormulaEvaluationError>,
{
    fn apply(&self, source: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        // Convert Match to strongly-typed input, propagating any conversion errors
        let input = I::try_from(source)?;

        // Apply the formula to get strongly-typed outputs
        let outputs = self.expand(input)?;

        // Convert each output back to Match, collecting any errors
        let mut results = Vec::with_capacity(outputs.len());
        for output in outputs {
            let match_ = output.try_into()?;
            results.push(match_);
        }

        Ok(results)
    }

    fn clone_box(&self) -> Box<dyn FormulaWrapper> {
        Box::new(self.clone())
    }
}

/// Implementation of Clone for boxed FormulaWrapper trait objects.
///
/// This allows Box<dyn FormulaWrapper> to be cloned even though the trait
/// itself doesn't have Clone as a supertrait (which would make it non-object-safe).
impl Clone for Box<dyn FormulaWrapper> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// A formula application represents a formula ready to be evaluated in a query context.
///
/// This type combines a formula with its initial terms and provides methods
/// for both single application and stream-based evaluation.
#[derive(Clone, Debug)]
pub struct FormulaApplication {
    /// Initial terms/context for the formula (currently unused but reserved for future use)
    terms: Match,

    /// The type-erased formula that will be applied
    formula: Box<dyn FormulaWrapper>,
}

impl FormulaApplication {
    /// Apply the formula to a single Match input
    pub fn apply(&self, terms: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        self.formula.apply(terms)
    }

    /// Evaluate the formula over a stream of Matches.
    ///
    /// This method:
    /// 1. Iterates through each Match in the input selection
    /// 2. Applies the formula to each Match
    /// 3. Yields all output Matches
    /// 4. Converts formula errors to query errors
    ///
    /// The formula is cloned before entering the async block to avoid
    /// lifetime issues with the borrow checker.
    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let formula = self.formula.clone();

        try_stream! {
            for await source in context.selection {
                let frame = source?;

                // Apply formula and convert any errors to QueryError
                let outputs = formula.apply(frame).map_err(|e| match e {
                    FormulaEvaluationError::ReadError { name } => {
                        QueryError::UnboundVariable { variable_name: name }
                    },
                    FormulaEvaluationError::InputConversionError { message } => {
                        QueryError::InvalidTerm { message }
                    },
                    FormulaEvaluationError::OutputConversionError { message } => {
                        QueryError::Serialization { message }
                    },
                    FormulaEvaluationError::TypeMismatch { expected, actual } => {
                        QueryError::InvalidTerm {
                            message: format!("Type mismatch: expected {}, got {}", expected, actual)
                        }
                    },
                    FormulaEvaluationError::ConversionError(msg) => {
                        QueryError::InvalidTerm { message: msg }
                    },
                })?;

                // Yield each output frame
                for frame in outputs {
                    yield frame;
                }
            }
        }
    }
}

// ============================================================================
// Example Implementation: Increment Formula
// ============================================================================

/// Example formula that increments a numeric value
#[derive(Debug, Clone, PartialEq)]
struct Inc;

/// Input structure for the Inc formula
#[derive(Debug)]
struct IncInput {
    pub of: i32,
}

impl TryFrom<Match> for IncInput {
    type Error = FormulaEvaluationError;

    fn try_from(match_: Match) -> Result<Self, Self::Error> {
        let of =
            match_
                .get::<i32>(&Term::var("of"))
                .map_err(|_| FormulaEvaluationError::ReadError {
                    name: "of".to_string(),
                })?;
        Ok(IncInput { of })
    }
}

/// Output structure for the Inc formula
#[derive(Debug)]
struct IncOutput {
    pub is: i32,
}

impl TryInto<Match> for IncOutput {
    type Error = FormulaEvaluationError;

    fn try_into(self) -> Result<Match, Self::Error> {
        Match::new()
            .set::<i32>(Term::var("is"), self.is)
            .map_err(|e| FormulaEvaluationError::OutputConversionError {
                message: format!("Failed to set 'is' field: {:?}", e),
            })
    }
}

impl Formula for Inc {
    type Input = IncInput;
    type Output = IncOutput;

    fn expand(&self, input: Self::Input) -> Result<Vec<Self::Output>, FormulaEvaluationError> {
        // Simple increment operation
        Ok(vec![IncOutput { is: input.of + 1 }])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_increment_formula() {
        // Create an instance of the increment formula
        let inc = Inc.new();

        // Create input with 'of' = 0
        let input = Match::new()
            .set::<i32>(Term::var("of"), 0)
            .expect("Failed to create input Match");

        // Apply the formula
        let result = inc.apply(input).expect("Formula application failed");

        // Create expected output with 'is' = 1
        let expected = Match::new()
            .set::<i32>(Term::var("is"), 1)
            .expect("Failed to create expected Match");

        // Verify the result
        assert_eq!(result, vec![expected]);
    }

    #[test]
    fn test_missing_input_field() {
        let inc = Inc.new();

        // Create input without the required 'of' field
        let input = Match::new();

        // Apply the formula - should fail with ReadError
        let result = inc.apply(input);

        assert!(matches!(
            result,
            Err(FormulaEvaluationError::ReadError { name }) if name == "of"
        ));
    }

    #[test]
    fn test_type_mismatch_handling() {
        // This test demonstrates how type mismatches would be handled
        // In a real scenario, if Match.get::<i32> fails due to type mismatch,
        // it would be converted to a ReadError
        let inc = Inc.new();

        // Create input with wrong type (if the API supported it)
        // For now, we just verify the error types exist and are properly defined
        let error = FormulaEvaluationError::TypeMismatch {
            expected: "i32".to_string(),
            actual: "string".to_string(),
        };

        assert_eq!(error.to_string(), "Type mismatch: expected i32, got string");
    }
}
