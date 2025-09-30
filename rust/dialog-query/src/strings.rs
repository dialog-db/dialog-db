//! String manipulation formulas for the query system
//!
//! This module provides formulas for common string operations including
//! concatenation, length calculation, case conversion, and basic string processing.

use crate::{
    cursor::Cursor, error::FormulaEvaluationError, predicate::formula::Cells, Compute,
    Dependencies, Formula, Type, Value,
};
use std::sync::OnceLock;

// ============================================================================
// String Operations: Concatenate, Length, Uppercase, Lowercase
// ============================================================================

/// Concatenate formula that joins two strings
#[derive(Debug, Clone)]
pub struct Concatenate {
    pub first: String,
    pub second: String,
    pub is: String,
}

pub struct ConcatenateInput {
    pub first: String,
    pub second: String,
}

impl TryFrom<Cursor> for ConcatenateInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let first = cursor.read::<String>("first")?;
        let second = cursor.read::<String>("second")?;
        Ok(ConcatenateInput { first, second })
    }
}

impl Compute for Concatenate {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Concatenate {
            first: input.first.clone(),
            second: input.second.clone(),
            is: format!("{}{}", input.first, input.second),
        }]
    }
}

static CONCATENATE_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Concatenate {
    type Input = ConcatenateInput;
    type Match = ();

    fn operator() -> &'static str {
        "concatenate"
    }

    fn cells() -> &'static Cells {
        CONCATENATE_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("first", Type::String).the("First string").required();

                cell("second", Type::String).the("Second string").required();

                cell("is", Type::String)
                    .the("Concatenated string")
                    .derived(2);
            })
        })
    }

    fn cost() -> usize {
        2
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("first".into());
        dependencies.require("second".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::String(self.is.clone());
        cursor.write("is", &value)
    }
}

/// Length formula that computes the length of a string
#[derive(Debug, Clone)]
pub struct Length {
    pub of: String,
    pub is: u32,
}

pub struct LengthInput {
    pub of: String,
}

impl TryFrom<Cursor> for LengthInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<String>("of")?;
        Ok(LengthInput { of })
    }
}

impl Compute for Length {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Length {
            of: input.of.clone(),
            is: input.of.len() as u32,
        }]
    }
}

static LENGTH_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Length {
    type Input = LengthInput;
    type Match = ();

    fn operator() -> &'static str {
        "length"
    }

    fn cells() -> &'static Cells {
        LENGTH_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("of", Type::String).the("String to measure").required();

                cell("is", Type::UnsignedInt)
                    .the("Length of string")
                    .derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
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

/// Uppercase formula that converts a string to uppercase
#[derive(Debug, Clone)]
pub struct Uppercase {
    pub of: String,
    pub is: String,
}

pub struct UppercaseInput {
    pub of: String,
}

impl TryFrom<Cursor> for UppercaseInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<String>("of")?;
        Ok(UppercaseInput { of })
    }
}

impl Compute for Uppercase {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Uppercase {
            of: input.of.clone(),
            is: input.of.to_uppercase(),
        }]
    }
}

static UPPERCASE_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Uppercase {
    type Input = UppercaseInput;
    type Match = ();

    fn operator() -> &'static str {
        "uppercase"
    }

    fn cells() -> &'static Cells {
        UPPERCASE_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("of", Type::String).the("String to convert").required();

                cell("is", Type::String).the("Uppercase string").derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::String(self.is.clone());
        cursor.write("is", &value)
    }
}

/// Lowercase formula that converts a string to lowercase
#[derive(Debug, Clone)]
pub struct Lowercase {
    pub of: String,
    pub is: String,
}

pub struct LowercaseInput {
    pub of: String,
}

impl TryFrom<Cursor> for LowercaseInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let of = cursor.read::<String>("of")?;
        Ok(LowercaseInput { of })
    }
}

impl Compute for Lowercase {
    fn compute(input: Self::Input) -> Vec<Self> {
        vec![Lowercase {
            of: input.of.clone(),
            is: input.of.to_lowercase(),
        }]
    }
}

static LOWERCASE_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for Lowercase {
    type Input = LowercaseInput;
    type Match = ();

    fn operator() -> &'static str {
        "lowercase"
    }

    fn cells() -> &'static Cells {
        LOWERCASE_CELLS.get_or_init(|| {
            Cells::define(|cell| {
                cell("of", Type::String).the("String to convert").required();

                cell("is", Type::String).the("Lowercase string").derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("of".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor.clone())?;
        Ok(Self::compute(input))
    }

    fn write(&self, cursor: &mut Cursor) -> Result<(), FormulaEvaluationError> {
        let value = Value::String(self.is.clone());
        cursor.write("is", &value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Match, Parameters, Term};

    #[test]
    fn test_concatenate_formula() {
        let mut terms = Parameters::new();
        terms.insert("first".to_string(), Term::var("x").into());
        terms.insert("second".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("x"), "Hello".to_string())
            .unwrap()
            .set(Term::var("y"), " World".to_string())
            .unwrap();

        let app = Concatenate::apply(terms);
        let results = app.derive(input).expect("Concatenate failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("result")).ok(),
            Some("Hello World".to_string())
        );
    }

    #[test]
    fn test_length_formula() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text").into());
        terms.insert("is".to_string(), Term::var("len").into());

        let input = Match::new()
            .set(Term::var("text"), "Hello".to_string())
            .unwrap();

        let app = Length::apply(terms);
        let results = app.derive(input).expect("Length failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<u32>(&Term::var("len")).ok(), Some(5));
    }

    #[test]
    fn test_uppercase_formula() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text").into());
        terms.insert("is".to_string(), Term::var("upper").into());

        let input = Match::new()
            .set(Term::var("text"), "hello world".to_string())
            .unwrap();

        let app = Uppercase::apply(terms);
        let results = app.derive(input).expect("Uppercase failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("upper")).ok(),
            Some("HELLO WORLD".to_string())
        );
    }

    #[test]
    fn test_lowercase_formula() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text").into());
        terms.insert("is".to_string(), Term::var("lower").into());

        let input = Match::new()
            .set(Term::var("text"), "HELLO WORLD".to_string())
            .unwrap();

        let app = Lowercase::apply(terms);
        let results = app.derive(input).expect("Lowercase failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("lower")).ok(),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn test_empty_string_length() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text").into());
        terms.insert("is".to_string(), Term::var("len").into());

        let input = Match::new().set(Term::var("text"), "".to_string()).unwrap();

        let app = Length::apply(terms);
        let results = app.derive(input).expect("Length of empty string failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<u32>(&Term::var("len")).ok(), Some(0));
    }

    #[test]
    fn test_concatenate_empty_strings() {
        let mut terms = Parameters::new();
        terms.insert("first".to_string(), Term::var("x").into());
        terms.insert("second".to_string(), Term::var("y").into());
        terms.insert("is".to_string(), Term::var("result").into());

        let input = Match::new()
            .set(Term::var("x"), "".to_string())
            .unwrap()
            .set(Term::var("y"), "World".to_string())
            .unwrap();

        let app = Concatenate::apply(terms);
        let results = app
            .derive(input)
            .expect("Concatenate with empty string failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("result")).ok(),
            Some("World".to_string())
        );
    }
}
