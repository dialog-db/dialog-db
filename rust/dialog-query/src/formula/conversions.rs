//! Type conversion formulas for the query system
//!
//! This module provides formulas for converting between different types,
//! including string conversion and number parsing operations.

use crate::{Formula, Value, dsl::Input};

/// ToString formula that converts any supported type to string
#[derive(Debug, Clone, Formula)]
pub struct ToString {
    pub value: Value,
    #[derived]
    pub is: String,
}

impl ToString {
    pub fn derive(input: ToStringInput) -> Vec<Self> {
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

/// ParseNumber formula that converts a string to a number (u32)
#[derive(Debug, Clone, Formula)]
pub struct ParseNumber {
    /// String to parse
    pub text: String,
    /// Parsed number
    #[derived(cost = 2)]
    pub is: u32,
}

impl ParseNumber {
    pub fn derive(input: Input<Self>) -> Vec<Self> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Entity, Parameters, Term, selection::Answer};

    #[test]
    fn test_to_string_number() -> anyhow::Result<()> {
        let mut terms = Parameters::new();
        terms.insert("value".to_string(), Term::var("num"));
        terms.insert("is".to_string(), Term::var("str"));

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
        terms.insert("value".to_string(), Term::var("bool"));
        terms.insert("is".to_string(), Term::var("str"));

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
        terms.insert("value".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("str"));

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
        terms.insert("value".to_string(), Term::var("entity"));
        terms.insert("is".to_string(), Term::var("str"));

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
        terms.insert("text".to_string(), Term::var("str"));
        terms.insert("is".to_string(), Term::var("num"));

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
        terms.insert("text".to_string(), Term::var("str"));
        terms.insert("is".to_string(), Term::var("num"));

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
        terms.insert("text".to_string(), Term::var("str"));
        terms.insert("is".to_string(), Term::var("num"));

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
        terms.insert("text".to_string(), Term::var("str"));
        terms.insert("is".to_string(), Term::var("num"));

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
        terms.insert("text".to_string(), Term::var("str"));
        terms.insert("is".to_string(), Term::var("num"));

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
