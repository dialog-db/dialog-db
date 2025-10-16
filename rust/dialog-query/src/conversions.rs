//! Type conversion formulas for the query system
//!
//! This module provides formulas for converting between different types,
//! including string conversion and number parsing operations.

use crate::{
    cursor::Cursor, error::FormulaEvaluationError, predicate::formula::Cells, Compute,
    Dependencies, Formula, Type, Value,
};

use std::sync::OnceLock;

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

impl TryFrom<&mut Cursor> for ToStringInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
        // Read the raw Value without type conversion since we accept any type
        let term =
            cursor
                .terms
                .get("value")
                .ok_or_else(|| FormulaEvaluationError::RequiredParameter {
                    parameter: "value".into(),
                })?;

        let value =
            cursor
                .source
                .resolve(term)
                .map_err(|_| FormulaEvaluationError::UnboundVariable {
                    term: term.clone(),
                    parameter: "value".into(),
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

static TO_STRING_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for ToString {
    type Input = ToStringInput;
    type Match = ();

    fn operator() -> &'static str {
        "to_string"
    }

    fn cells() -> &'static Cells {
        TO_STRING_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("value", Type::String) // Note: accepts any type
                    .the("Value to convert")
                    .required();

                builder
                    .cell("is", Type::String)
                    .the("String representation")
                    .derived(1);
            })
        })
    }

    fn cost() -> usize {
        1
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("value".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
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

impl TryFrom<&mut Cursor> for ParseNumberInput {
    type Error = FormulaEvaluationError;

    fn try_from(cursor: &mut Cursor) -> Result<Self, Self::Error> {
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

static PARSE_NUMBER_CELLS: OnceLock<Cells> = OnceLock::new();

impl Formula for ParseNumber {
    type Input = ParseNumberInput;
    type Match = ();

    fn operator() -> &'static str {
        "parse_number"
    }

    fn cells() -> &'static Cells {
        PARSE_NUMBER_CELLS.get_or_init(|| {
            Cells::define(|builder| {
                builder
                    .cell("text", Type::String)
                    .the("String to parse")
                    .required();

                builder
                    .cell("is", Type::UnsignedInt)
                    .the("Parsed number")
                    .derived(2);
            })
        })
    }

    fn cost() -> usize {
        2
    }

    fn dependencies() -> Dependencies {
        let mut dependencies = Dependencies::new();
        dependencies.require("text".into());
        dependencies.provide("is".into());
        dependencies
    }

    fn derive(cursor: &mut Cursor) -> Result<Vec<Self>, FormulaEvaluationError> {
        let input = Self::Input::try_from(cursor)?;
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
    use crate::{selection::Answer, Entity, Parameters, Term};

    #[test]
    fn test_to_string_number() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("num").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let input = Answer::new().set(Term::var("num"), 42u32).unwrap();

        let app = ToString::apply(terms)?;
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("str"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("42".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_to_string_boolean() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("bool").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let input = Answer::new().set(Term::var("bool"), true).unwrap();

        let app = ToString::apply(terms)?;
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("str"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("true".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_to_string_string() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("text").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let input = Answer::new()
            .set(Term::var("text"), "hello".to_string())
            .unwrap();

        let app = ToString::apply(terms)?;
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("str"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("hello".to_string())
        );
        Ok(())
    }

    #[test]
    fn test_to_string_entity() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("entity").into());
        terms.insert("is".to_string(), Term::var("str").into());

        let entity = Entity::new().unwrap();
        let input = Answer::new()
            .set(Term::var("entity"), entity.clone())
            .unwrap();

        let app = ToString::apply(terms)?;
        let results = app.derive(input).expect("ToString failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            String::try_from(result.resolve(&Term::<String>::var("str"))?)?,
            entity.to_string()
        );
        Ok(())
    }

    #[test]
    fn test_parse_number_valid() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Answer::new()
            .set(Term::var("str"), "123".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms)?;
        let results = app.derive(input).expect("ParseNumber failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("num"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(123)
        );
        Ok(())
    }

    #[test]
    fn test_parse_number_with_whitespace() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Answer::new()
            .set(Term::var("str"), "  456  ".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms)?;
        let results = app.derive(input).expect("ParseNumber failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("num"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(456)
        );
        Ok(())
    }

    #[test]
    fn test_parse_number_invalid() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Answer::new()
            .set(Term::var("str"), "not a number".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms)?;
        let results = app
            .derive(input)
            .expect("ParseNumber should handle invalid input");

        // Should return empty Vec for invalid input
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[test]
    fn test_parse_number_empty_string() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Answer::new().set(Term::var("str"), "".to_string()).unwrap();

        let app = ParseNumber::apply(terms)?;
        let results = app
            .derive(input)
            .expect("ParseNumber should handle empty string");

        // Should return empty Vec for empty string
        assert_eq!(results.len(), 0);
        Ok(())
    }

    #[test]
    fn test_parse_number_negative() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("str").into());
        terms.insert("is".to_string(), Term::var("num").into());

        let input = Answer::new()
            .set(Term::var("str"), "-123".to_string())
            .unwrap();

        let app = ParseNumber::apply(terms)?;
        let results = app
            .derive(input)
            .expect("ParseNumber should handle negative input");

        // Should return empty Vec for negative numbers since we parse as u32
        assert_eq!(results.len(), 0);
        Ok(())
    }
}
