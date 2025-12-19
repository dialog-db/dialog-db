//! String manipulation formulas for the query system
//!
//! This module provides formulas for common string operations including
//! concatenation, length calculation, case conversion, and basic string processing.

pub use crate::Formula;

// ============================================================================
// String Operations: Concatenate, Length, Uppercase, Lowercase
// ============================================================================

/// Concatenate formula that joins two strings
#[derive(Debug, Clone, Formula)]
pub struct Concatenate {
    /// First string
    pub first: String,
    /// Second string
    pub second: String,
    /// Concatenated string
    #[derived(cost = 2)]
    pub is: String,
}

impl Concatenate {
    pub fn derive(input: dialog_query::dsl::Input<Self>) -> Vec<Self> {
        vec![Concatenate {
            first: input.first.clone(),
            second: input.second.clone(),
            is: format!("{}{}", input.first, input.second),
        }]
    }
}

/// Length formula that computes the length of a string
#[derive(Debug, Clone, dialog_query_macros::Formula)]
pub struct Length {
    /// String to measure
    pub of: String,
    /// Length of string
    #[derived]
    pub is: u32,
}

impl Length {
    pub fn derive(input: dialog_query::dsl::Input<Self>) -> Vec<Self> {
        vec![Length {
            of: input.of.clone(),
            is: input.of.len() as u32,
        }]
    }
}

/// Uppercase formula that converts a string to uppercase
#[derive(Debug, Clone, dialog_query_macros::Formula)]
pub struct Uppercase {
    /// String to convert
    pub of: String,
    /// Uppercase string
    #[derived]
    pub is: String,
}

impl Uppercase {
    pub fn derive(input: dialog_query::dsl::Input<Self>) -> Vec<Self> {
        vec![Uppercase {
            of: input.of.clone(),
            is: input.of.to_uppercase(),
        }]
    }
}

/// Lowercase formula that converts a string to lowercase
#[derive(Debug, Clone, dialog_query_macros::Formula)]
pub struct Lowercase {
    /// String to convert
    pub of: String,
    /// Lowercase string
    #[derived]
    pub is: String,
}

impl Lowercase {
    pub fn derive(input: dialog_query::dsl::Input<Self>) -> Vec<Self> {
        vec![Lowercase {
            of: input.of.clone(),
            is: input.of.to_lowercase(),
        }]
    }
}

#[derive(Debug, Clone, dialog_query_macros::Formula)]
pub struct Is {
    /// String to convert
    pub of: String,
    /// Lowercase string
    #[derived]
    pub is: String,
}
impl Is {
    pub fn derive(input: dialog_query::dsl::Input<Self>) -> Vec<Self> {
        vec![Self {
            of: input.of.clone(),
            is: input.of.clone(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{selection::Answer, Parameters, Term};

    #[test]
    fn test_concatenate_formula() {
        let mut terms = Parameters::new();
        terms.insert("first".to_string(), Term::var("x"));
        terms.insert("second".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), "Hello".to_string())
            .unwrap()
            .set(Term::var("y"), " World".to_string())
            .unwrap();

        let app = Concatenate::apply(terms).expect("apply should work");
        let results = app.derive(input).expect("Concatenate failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("result"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("Hello World".to_string())
        );
    }

    #[test]
    fn test_length_formula() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("len"));

        let input = Answer::new()
            .set(Term::var("text"), "Hello".to_string())
            .unwrap();

        let app = Length::apply(terms).expect("apply should work");
        let results = app.derive(input).expect("Length failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("len"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
    }

    #[test]
    fn test_uppercase_formula() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("upper"));

        let input = Answer::new()
            .set(Term::var("text"), "hello world".to_string())
            .unwrap();

        let app = Uppercase::apply(terms).expect("apply should work");
        let results = app.derive(input).expect("Uppercase failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("upper"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("HELLO WORLD".to_string())
        );
    }

    #[test]
    fn test_lowercase_formula() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("lower"));

        let input = Answer::new()
            .set(Term::var("text"), "HELLO WORLD".to_string())
            .unwrap();

        let app = Lowercase::apply(terms).expect("apply should work");
        let results = app.derive(input).expect("Lowercase failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("lower"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn test_empty_string_length() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("len"));

        let input = Answer::new()
            .set(Term::var("text"), "".to_string())
            .unwrap();

        let app = Length::apply(terms).expect("apply should work");
        let results = app.derive(input).expect("Length of empty string failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<u32>::var("len"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(0)
        );
    }

    #[test]
    fn test_concatenate_empty_strings() {
        let mut terms = Parameters::new();
        terms.insert("first".to_string(), Term::var("x"));
        terms.insert("second".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("x"), "".to_string())
            .unwrap()
            .set(Term::var("y"), "World".to_string())
            .unwrap();

        let app = Concatenate::apply(terms).expect("apply should work");
        let results = app
            .derive(input)
            .expect("Concatenate with empty string failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .resolve(&Term::<String>::var("result"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("World".to_string())
        );
    }
}
