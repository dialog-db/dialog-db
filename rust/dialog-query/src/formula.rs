//! Formula system for query evaluation
//!
//! This module provides a type-safe formula expansion system that allows
//! for transforming data through user-defined formulas while maintaining
//! type safety at compile time and enabling dynamic dispatch at runtime.

use crate::deductive_rule::{
    AnalyzerError, FormulaApplication as DeductiveFormulaApplication, Terms,
};
use crate::{try_stream, EvaluationContext, QueryError, Selection, Store};
use crate::{Match, Term, Value};
use std::collections::BTreeMap;
use std::fmt::{self, Debug, Display};
use std::marker::PhantomData;
use std::sync::Arc;
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

    /// Expands the input into zero or more output values.
    ///
    /// Formulas can:
    /// - Filter (return empty vec)
    /// - Transform (return single item)
    /// - Expand (return multiple items)
    fn expand(terms: Self::Input) -> Result<Vec<Self>, FormulaEvaluationError>;

    /// Converts this formula into a type-erased FormulaApplication.
    ///
    /// Creates a FormulaApplication with the given terms for integration
    /// with the deductive rule system.
    fn apply(terms: Terms) -> Result<DeductiveFormulaApplication, AnalyzerError>
    where
        Self: TryInto<Match, Error = FormulaEvaluationError> + 'static,
        Self::Input: TryFrom<Match, Error = FormulaEvaluationError>,
    {
        // Import necessary types
        use crate::deductive_rule::{Cells, Formula as DeductiveFormula};

        // Create a basic deductive formula structure
        let formula = DeductiveFormula::new(std::any::type_name::<Self>());

        // Create a FormulaApplication with the expansion function
        let mut app = DeductiveFormulaApplication::new(formula, terms);
        app.expand_fn = Some(create_expand_fn::<Self>());
        Ok(app)
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

    fn expand(terms: Self::Input) -> Result<Vec<Self>, FormulaEvaluationError> {
        T::expand(terms).map(|vec| vec.into_iter().map(Box::new).collect())
    }
}

// ============================================================================
// Example Implementation: Increment Formula
// ============================================================================

/// Example formula that increments a numeric value
#[derive(Debug, Clone, PartialEq)]
pub struct Inc {
    pub of: i32,
    pub is: i32,
}

/// Input structure for the Inc formula
#[derive(Debug)]
pub struct IncInput {
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

impl TryInto<Match> for Inc {
    type Error = FormulaEvaluationError;

    fn try_into(self) -> Result<Match, Self::Error> {
        let out = Match::new();

        let out = out.set::<i32>(Term::var("is"), self.is).map_err(|e| {
            FormulaEvaluationError::OutputConversionError {
                message: format!("Failed to set 'is' field: {:?}", e),
            }
        })?;

        let out = out.set::<i32>(Term::var("of"), self.of).map_err(|e| {
            FormulaEvaluationError::OutputConversionError {
                message: format!("Failed to set 'of' field: {:?}", e),
            }
        })?;

        Ok(out)
    }
}

impl Formula for Inc {
    type Input = IncInput;

    fn expand(input: Self::Input) -> Result<Vec<Self>, FormulaEvaluationError> {
        // Simple increment operation
        Ok(vec![Inc {
            of: input.of.clone(),
            is: input.of + 1,
        }])
    }
}

/// Extension methods for DeductiveFormulaApplication
impl DeductiveFormulaApplication {
    /// Evaluate the formula over a stream of Matches
    /// This is the main method that should be used for formula evaluation
    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let terms = self.terms.clone();
        let expand_fn = self.expand_fn.clone();

        try_stream! {
            for await source in context.selection {
                let frame = source?;

                // If we have an expansion function, use it
                if let Some(expand) = expand_fn {
                    let outputs = expand(&frame, &terms).map_err(|e| match e {
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
                    for output in outputs {
                        yield output;
                    }
                } else {
                    // No expansion function, just pass through
                    yield frame;
                }
            }
        }
    }

    /// Expand a single Match through a formula with a given expansion function
    pub fn expand_with(
        &self,
        source: &Match,
        expand_fn: impl Fn(&Match, &Terms) -> Result<Vec<Match>, FormulaEvaluationError>,
    ) -> Result<Vec<Match>, FormulaEvaluationError> {
        expand_fn(source, &self.terms)
    }

    /// Evaluate the formula over a stream of Matches
    pub fn evaluate_with<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
        expand_fn: impl Fn(&Match, &Terms) -> Result<Vec<Match>, FormulaEvaluationError>
            + Send
            + Sync
            + 'static,
    ) -> impl Selection {
        let terms = self.terms.clone();

        try_stream! {
            for await source in context.selection {
                let frame = source?;

                // Apply formula with terms mapping
                let outputs = expand_fn(&frame, &terms).map_err(|e| match e {
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

/// Helper function to create an expansion function for a specific Formula type
pub fn create_expand_fn<F>() -> fn(&Match, &Terms) -> Result<Vec<Match>, FormulaEvaluationError>
where
    F: Formula,
    F::Input: TryFrom<Match, Error = FormulaEvaluationError>,
    F: TryInto<Match, Error = FormulaEvaluationError>,
{
    fn expand<F>(source: &Match, terms: &Terms) -> Result<Vec<Match>, FormulaEvaluationError>
    where
        F: Formula,
        F::Input: TryFrom<Match, Error = FormulaEvaluationError>,
        F: TryInto<Match, Error = FormulaEvaluationError>,
    {
        // Read values from source Match using Terms mapping
        let mut variables = BTreeMap::new();

        // For each term in the mapping, copy the value from source to input
        for (field_name, term) in terms.iter() {
            // Try to copy the value from source to input_match
            // We need to handle this carefully as we don't know the exact type
            // For now, we'll just try to copy as-is using the internal representation
            if let Some(var_name) = term.as_variable_name() {
                if let Some(value) = source.variables.get(var_name) {
                    variables.insert(field_name.clone(), value.clone());
                }
            }
        }

        let input_match = Match {
            variables: Arc::new(variables),
        };
        // Convert to typed input
        let input = F::Input::try_from(input_match)?;

        // Apply the formula
        let outputs = F::expand(input)?;

        // Convert outputs back to Matches
        let mut results = Vec::with_capacity(outputs.len());
        for output in outputs {
            // Start with the source Match to preserve other fields
            let mut result_vars = (*source.variables).clone();

            // Convert output to Match to get the computed fields
            let output_match: Match = output.try_into()?;

            eprintln!(
                "DEBUG: Output match variables: {:?}",
                output_match.variables
            );

            // Copy computed fields to result using Terms mapping
            for (field_name, term) in terms.iter() {
                if let Some(var_name) = term.as_variable_name() {
                    if let Some(value) = output_match.variables.get(field_name) {
                        result_vars.insert(var_name.to_string(), value.clone());
                    }
                }
            }

            let result = Match {
                variables: Arc::new(result_vars),
            };
            results.push(result);
        }

        Ok(results)
    }

    expand::<F>
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_formula_with_evaluate() {
        // Create Terms mapping
        let mut terms = Terms::new();
        terms.insert("of".to_string(), Term::var("x").into());
        terms.insert("is".to_string(), Term::var("y").into());

        // Create the formula application
        let app = Inc::apply(terms.clone()).expect("Failed to create formula application");

        // Create input with 'x' = 5
        let input = Match::new()
            .set::<i32>(Term::var("x"), 5)
            .expect("Failed to create input Match");

        // Expand the formula using expand_with
        let expand_fn = create_expand_fn::<Inc>();
        let result = app
            .expand_with(&input, expand_fn)
            .expect("Formula expansion failed");

        // Verify the result
        assert_eq!(result.len(), 1);
        let output = &result[0];
        assert_eq!(output.get::<i32>(&Term::var("x")).ok(), Some(5));
        assert_eq!(output.get::<i32>(&Term::var("y")).ok(), Some(6));
    }
}

#[derive(Debug, Clone)]
pub struct Sum {
    of: u32,
    with: u32,
    // This is derived from the sum of `of` and `with`.
    is: u32,
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
trait Compute: Formula2 + Sized {
    fn compute(input: Self::Input) -> Vec<Self>;
}

pub struct SumInput {
    of: u32,
    with: u32,
}

impl TryFrom<Cursor> for SumInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<u32>("of")?;
        let with = cursor.read::<u32>("with")?;
        Ok(SumInput { of, with })
    }
}

pub struct SumMatch {
    of: Term<u32>,
    with: Term<u32>,
    is: Term<u32>,
}

impl Formula2 for Sum {
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

    // fn apply(terms: Match) -> FormulaApplication2<Self> {
    //     FormulaApplication2::new(terms, || {})
    // }
}

trait Formula2: Sized {
    type Input: TryFrom<Cursor, Error = FormulaEvaluationError>;

    type Match;

    fn derive_match(cursor: &Cursor) -> Result<Vec<Match>, FormulaEvaluationError> {
        let output = Self::derive(cursor)?;
        let mut out = cursor.clone();
        let mut results = Vec::new();
        for match_item in output {
            match_item.write(&mut out)?;
            results.push(out.source.clone());
        }
        Ok(results)
    }

    // fn compute(cursor: Cursor) -> Result<Self::Match, FormulaEvaluationError> {
    //     let input = Self::Input::try_from(cursor)?;
    //     let output = Self::derive(input);
    //     output.write(cursor);
    //     Ok(cursor.source)
    // }
    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError>;

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError>;

    fn apply(terms: Terms) -> FormulaApplication2 {
        FormulaApplication2 {
            terms,
            formula: |cursor| Self::derive_match(&cursor),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    pub source: Match,
    pub terms: Terms,
}

impl Cursor {
    pub fn new(source: Match, terms: Terms) -> Self {
        Cursor { source, terms }
    }

    pub fn read<T: Cast>(&self, key: &str) -> Result<T, FormulaEvaluationError> {
        let term = self
            .terms
            .get(key)
            .ok_or(FormulaEvaluationError::ReadError {
                name: key.to_string(),
            })?;

        let out = self
            .source
            .get(&term)
            .or(Err(FormulaEvaluationError::ReadError {
                name: key.to_string(),
            }))?;

        T::try_cast(&out)
    }

    pub fn write(&mut self, key: &str, value: &Value) -> Result<(), FormulaEvaluationError> {
        let term = self
            .terms
            .get(key)
            .ok_or(FormulaEvaluationError::ReadError {
                name: key.to_string(),
            })?;

        let frame = self.source.clone();

        self.source = frame.unify(term.clone(), value.clone()).or(Err(
            FormulaEvaluationError::OutputConversionError {
                message: "Failed to unify term and value".into(),
            },
        ))?;

        Ok(())
    }
}

trait Cast: Sized {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError>;
}

impl Cast for u32 {
    fn try_cast(value: &Value) -> Result<Self, FormulaEvaluationError> {
        match value {
            Value::UnsignedInt(n) => Ok(n.clone() as u32),
            _ => Err(FormulaEvaluationError::TypeMismatch {
                expected: "i32".into(),
                actual: value.data_type().to_string(),
            }),
        }
    }
}

pub struct FormulaApplication2 {
    pub terms: Terms,
    pub formula: fn(&Cursor) -> Result<Vec<Match>, FormulaEvaluationError>,
}
impl FormulaApplication2 {
    pub fn expand(&self, frame: Match) -> Result<Vec<Match>, FormulaEvaluationError> {
        let mut matches = Vec::new();
        let cursor = Cursor::new(frame, self.terms.clone());
        for each in (self.formula)(&cursor)? {
            matches.push(each);
        }

        Ok(matches)
    }

    pub fn evaluate<S: Store, M: Selection>(
        &self,
        context: EvaluationContext<S, M>,
    ) -> impl Selection {
        let formula = self.formula;
        let terms = self.terms.clone();
        try_stream! {
            for await source in context.selection {
                let frame = source?;
                let cursor = Cursor::new(frame, terms.clone());
                let outputs = formula(&cursor).or(Err(QueryError::PlanningError {
                    message: "Failed to evaluate formula".into(),
                }))?;


                // Yield each output frame
                for output in outputs {
                    yield output;
                }
            }
        }
    }
}
