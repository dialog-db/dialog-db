//! Type conversion formulas for the query system
//!
//! This module provides formulas for converting between different types,
//! including string conversion and number parsing operations.

use crate::{cursor::Cursor, error::FormulaEvaluationError, Compute, Dependencies, Formula, Value};

// ============================================================================
// Type Conversion Operations: ToString, ParseNumber
// ============================================================================

/// ToString formula that converts any supported type to string
#[derive(Debug, Clone)]
pub struct ToString {
    pub value: Value,
    pub is: String,
}

pub struct ToStringInput {
    pub value: Value,
}

impl TryFrom<Cursor> for ToStringInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        // Read the raw Value without type conversion since we accept any type
        let term =
            cursor
                .terms
                .get("value")
                .ok_or_else(|| FormulaEvaluationError::RequiredParameter {
                    parameter: "value".into(),
                })?;

        let value = cursor.source.resolve_value(term).map_err(|_| {
            FormulaEvaluationError::UnboundVariable {
                term: term.clone(),
                parameter: "value".into(),
            }
        })?;

        Ok(ToStringInput { value })
    }
}

impl Compute for ToString {
    fn compute(input: Self::Input) -> Vec<Self> {
        let string_repr = match &input.value {
            Value::String(s) => s.clone(),
            Value::UnsignedInt(n) => n.to_string(),
            Value::SignedInt(n) => n.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::Entity(e) => e.to_string(),
            Value::Symbol(s) => s.to_string(),
            Value::Bytes(bytes) => format!("Bytes({} bytes)", bytes.len()),
            Value::Record(record) => format!("Record({} bytes)", record.len()),
        };

        vec![ToString {
            value: input.value,
            is: string_repr,
        }]
    }
}

impl Formula for ToString {
    type Input = ToStringInput;
    type Match = ();

    fn name() -> &'static str {
        "to_string"
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("value".into());
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

/// ParseNumber formula that converts a string to a number (u32)
#[derive(Debug, Clone)]
pub struct ParseNumber {
    pub text: String,
    pub is: u32,
}

pub struct ParseNumberInput {
    pub text: String,
}

impl TryFrom<Cursor> for ParseNumberInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: Cursor) -> Result<Self, Self::Error> {
        let text = cursor.read::<String>("text")?;
        Ok(ParseNumberInput { text })
    }
}

impl Compute for ParseNumber {
    fn compute(input: Self::Input) -> Vec<Self> {
        // Try to parse the string as a u32
        match input.text.trim().parse::<u32>() {
            Ok(number) => vec![ParseNumber {
                text: input.text,
                is: number,
            }],
            Err(_) => {
                // Return empty Vec if parsing fails - this will be filtered out
                vec![]
            }
        }
    }
}

impl Formula for ParseNumber {
    type Input = ParseNumberInput;
    type Match = ();

    fn name() -> &'static str {
        "parse_number"
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("text".into());
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
    use crate::{Entity, Match, Parameters, Term};

    #[test]
    fn test_to_string_number() {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("num").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let input = Match::new().set(Term::var("num"), 42u32).unwrap();

        let app = ToString::apply(terms);
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("str")).ok(),
            Some("42".to_string())
        );
    }

    #[test]
    fn test_to_string_boolean() {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let input = Match::new().set(Term::var("bool"), true).unwrap();

        let app = ToString::apply(terms);
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("str")).ok(),
            Some("true".to_string())
        );
    }

    #[test]
    fn test_to_string_string() {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("text").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let input = Match::new()
            .set(Term::var("text"), "hello".to_string())
            .unwrap();

        let app = ToString::apply(terms);
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("str")).ok(),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_to_string_entity() {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("entity").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let entity = Entity::new().unwrap();
        let input = Match::new()
            .set(Term::var("entity"), entity.clone())
            .unwrap();

        let app = ToString::apply(terms);
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result.get::<String>(&Term::var("str")).ok(),
            Some(entity.to_string())
        );
    }

    #[test]
    fn test_parse_number_valid() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Match::new()
            .set(Term::var("str"), "123".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms);
        let results = app.derive(input).expect("ParseNumber failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<u32>(&Term::var("num")).ok(), Some(123));
    }

    #[test]
    fn test_parse_number_with_whitespace() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Match::new()
            .set(Term::var("str"), "  456  ".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms);
        let results = app.derive(input).expect("ParseNumber failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(result.get::<u32>(&Term::var("num")).ok(), Some(456));
    }

    #[test]
    fn test_parse_number_invalid() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Match::new()
            .set(Term::var("str"), "not a number".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms);
        let results = app
            .derive(input)
            .expect("ParseNumber should handle invalid input");

        // Should return empty Vec for invalid input
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parse_number_empty_string() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Match::new().set(Term::var("str"), "".to_string()).unwrap();

        let app = ParseNumber::apply(terms);
        let results = app
            .derive(input)
            .expect("ParseNumber should handle empty string");

        // Should return empty Vec for empty string
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_parse_number_negative() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Match::new()
            .set(Term::var("str"), "-123".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms);
        let results = app
            .derive(input)
            .expect("ParseNumber should handle negative input");

        // Should return empty Vec for negative numbers since we parse as u32
        assert_eq!(results.len(), 0);
    }
}
