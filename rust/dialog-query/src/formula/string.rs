//! String manipulation formulas for the query system
//!
//! This module provides formulas for common string operations including
//! concatenation, length calculation, case conversion, and basic string processing.

pub use crate::Formula;

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
    /// Concatenate `first` and `second` into `is`
    pub fn derive(input: dialog_query::formula::Input<Self>) -> Vec<Self> {
        vec![Concatenate {
            first: input.first.clone(),
            second: input.second.clone(),
            is: format!("{}{}", input.first, input.second),
        }]
    }
}

/// Length formula that computes the length of a string
#[derive(Debug, Clone, dialog_macros::Formula)]
pub struct Length {
    /// String to measure
    pub of: String,
    /// Length of string
    #[derived]
    pub is: u32,
}

impl Length {
    /// Compute the length of the input string
    pub fn derive(input: dialog_query::formula::Input<Self>) -> Vec<Self> {
        vec![Length {
            of: input.of.clone(),
            is: input.of.len() as u32,
        }]
    }
}

/// Uppercase formula that converts a string to uppercase
#[derive(Debug, Clone, dialog_macros::Formula)]
pub struct Uppercase {
    /// String to convert
    pub of: String,
    /// Uppercase string
    #[derived]
    pub is: String,
}

impl Uppercase {
    /// Convert the input string to uppercase
    pub fn derive(input: dialog_query::formula::Input<Self>) -> Vec<Self> {
        vec![Uppercase {
            of: input.of.clone(),
            is: input.of.to_uppercase(),
        }]
    }
}

/// Lowercase formula that converts a string to lowercase
#[derive(Debug, Clone, dialog_macros::Formula)]
pub struct Lowercase {
    /// String to convert
    pub of: String,
    /// Lowercase string
    #[derived]
    pub is: String,
}

impl Lowercase {
    /// Convert the input string to lowercase
    pub fn derive(input: dialog_query::formula::Input<Self>) -> Vec<Self> {
        vec![Lowercase {
            of: input.of.clone(),
            is: input.of.to_lowercase(),
        }]
    }
}

/// Like formula that matches a string against a glob pattern.
///
/// Uses `*` for matching any number of characters and `?` for matching
/// a single character. Use `\` to escape `*`, `?`, or `\` itself.
///
/// Returns the matched text if the pattern matches, or no results if it
/// doesn't.
#[derive(Debug, Clone, Formula)]
pub struct Like {
    /// Text to match against
    pub text: String,
    /// Glob pattern (`*` = any chars, `?` = single char)
    pub pattern: String,
    /// The matched text (same as input text when pattern matches)
    #[derived(cost = 3)]
    pub is: String,
}

impl Like {
    /// Match text against pattern, returning the matched text or empty on mismatch
    pub fn derive(input: dialog_query::formula::Input<Self>) -> Vec<Self> {
        if glob_match(&input.pattern, &input.text) {
            vec![Like {
                text: input.text.clone(),
                pattern: input.pattern.clone(),
                is: input.text.clone(),
            }]
        } else {
            vec![]
        }
    }
}

/// Matches a string against a simple glob pattern.
///
/// `*` matches zero or more characters, `?` matches exactly one character.
/// `\` escapes the next character (so `\*` matches a literal `*`).
///
/// Uses an iterative backtracking algorithm. Runs in O(n*m) worst case
/// with no recursion.
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = text.chars().collect();

    let mut pi = 0; // pattern index
    let mut ti = 0; // text index

    // Saved positions for backtracking on `*`
    let mut star_pi = usize::MAX;
    let mut star_ti = 0;

    while ti < text.len() {
        let (pat_char, pat_advance) = if pi < pattern.len() {
            if pattern[pi] == '\\' && pi + 1 < pattern.len() {
                // Escaped character — treat next char as literal
                (Some(pattern[pi + 1]), 2)
            } else {
                (Some(pattern[pi]), 1)
            }
        } else {
            (None, 0)
        };

        match pat_char {
            Some('*') if pat_advance == 1 => {
                // Record backtrack point
                star_pi = pi;
                star_ti = ti;
                // Try matching zero characters — advance pattern only
                pi += 1;
            }
            Some('?') if pat_advance == 1 => {
                // Single-char wildcard, consume one from each
                pi += 1;
                ti += 1;
            }
            Some(c) if c == text[ti] => {
                // Literal match (including escaped chars)
                pi += pat_advance;
                ti += 1;
            }
            _ => {
                // Mismatch — backtrack to last `*` if available
                if star_pi == usize::MAX {
                    return false;
                }
                // The `*` consumes one more character than before
                star_ti += 1;
                ti = star_ti;
                pi = star_pi + 1;
            }
        }
    }

    // Consume any trailing `*`s in pattern
    while pi < pattern.len() && pattern[pi] == '*' {
        pi += 1;
    }

    pi == pattern.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::formula::query::FormulaQuery;
    use crate::{Parameters, Query, Term, selection::Answer};

    #[dialog_common::test]
    fn it_concatenates_strings() {
        let mut terms = Parameters::new();
        terms.insert("first".to_string(), Term::var("x"));
        terms.insert("second".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Answer::new();
        input
            .bind(&Term::var("x"), "Hello".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("y"), " World".to_string().into())
            .unwrap();

        let app: FormulaQuery = Concatenate::apply(terms).expect("apply should work").into();
        let results = app.derive(input).expect("Concatenate failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("Hello World".to_string())
        );
    }

    #[dialog_common::test]
    fn it_computes_string_length() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("len"));

        let mut input = Answer::new();
        input
            .bind(&Term::var("text"), "Hello".to_string().into())
            .unwrap();

        let app: FormulaQuery = Length::apply(terms).expect("apply should work").into();
        let results = app.derive(input).expect("Length failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("len"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(5)
        );
    }

    #[dialog_common::test]
    fn it_converts_to_uppercase() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("upper"));

        let mut input = Answer::new();
        input
            .bind(&Term::var("text"), "hello world".to_string().into())
            .unwrap();

        let app: FormulaQuery = Uppercase::apply(terms).expect("apply should work").into();
        let results = app.derive(input).expect("Uppercase failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("upper"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("HELLO WORLD".to_string())
        );
    }

    #[dialog_common::test]
    fn it_converts_to_lowercase() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("lower"));

        let mut input = Answer::new();
        input
            .bind(&Term::var("text"), "HELLO WORLD".to_string().into())
            .unwrap();

        let app: FormulaQuery = Lowercase::apply(terms).expect("apply should work").into();
        let results = app.derive(input).expect("Lowercase failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("lower"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("hello world".to_string())
        );
    }

    #[dialog_common::test]
    fn it_returns_zero_for_empty_string() {
        let mut terms = Parameters::new();
        terms.insert("of".to_string(), Term::var("text"));
        terms.insert("is".to_string(), Term::var("len"));

        let mut input = Answer::new();
        input
            .bind(&Term::var("text"), "".to_string().into())
            .unwrap();

        let app: FormulaQuery = Length::apply(terms).expect("apply should work").into();
        let results = app.derive(input).expect("Length of empty string failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("len"))
                .ok()
                .and_then(|v| u32::try_from(v).ok()),
            Some(0)
        );
    }

    #[dialog_common::test]
    fn it_concatenates_empty_strings() {
        let mut terms = Parameters::new();
        terms.insert("first".to_string(), Term::var("x"));
        terms.insert("second".to_string(), Term::var("y"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Answer::new();
        input.bind(&Term::var("x"), "".to_string().into()).unwrap();
        input
            .bind(&Term::var("y"), "World".to_string().into())
            .unwrap();

        let app: FormulaQuery = Concatenate::apply(terms).expect("apply should work").into();
        let results = app
            .derive(input)
            .expect("Concatenate with empty string failed");

        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert_eq!(
            result
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("World".to_string())
        );
    }

    #[dialog_common::test]
    fn it_chains_string_operations() -> anyhow::Result<()> {
        // Test Concatenate formula
        let mut concat_terms = Parameters::new();
        concat_terms.insert("first".to_string(), Term::var("fname"));
        concat_terms.insert("second".to_string(), Term::var("lname"));
        concat_terms.insert("is".to_string(), Term::var("full_name"));

        let concat_formula: FormulaQuery = Concatenate::apply(concat_terms)?.into();

        let mut concat_input = Answer::new();
        concat_input
            .bind(&Term::var("fname"), "John".to_string().into())
            .unwrap();
        concat_input
            .bind(&Term::var("lname"), " Doe".to_string().into())
            .unwrap();

        let concat_results = concat_formula.derive(concat_input)?;
        assert_eq!(concat_results.len(), 1);
        assert_eq!(
            String::try_from(concat_results[0].lookup(&Term::var("full_name")).unwrap()).ok(),
            Some("John Doe".to_string())
        );

        // Test Length formula
        let mut length_terms = Parameters::new();
        length_terms.insert("of".to_string(), Term::var("text"));
        length_terms.insert("is".to_string(), Term::var("length"));

        let length_formula: FormulaQuery = Length::apply(length_terms)?.into();

        let mut length_input = Answer::new();
        length_input
            .bind(&Term::var("text"), "Hello World".to_string().into())
            .unwrap();

        let length_results = length_formula.derive(length_input)?;
        assert_eq!(length_results.len(), 1);
        assert_eq!(
            u32::try_from(length_results[0].lookup(&Term::var("length")).unwrap()).ok(),
            Some(11)
        );

        // Test Uppercase formula
        let mut upper_terms = Parameters::new();
        upper_terms.insert("of".to_string(), Term::var("input"));
        upper_terms.insert("is".to_string(), Term::var("output"));

        let upper_formula: FormulaQuery = Uppercase::apply(upper_terms)?.into();

        let mut upper_input = Answer::new();
        upper_input
            .bind(&Term::var("input"), "hello world".to_string().into())
            .unwrap();

        let upper_results = upper_formula.derive(upper_input)?;
        assert_eq!(upper_results.len(), 1);
        assert_eq!(
            String::try_from(upper_results[0].lookup(&Term::var("output")).unwrap()).ok(),
            Some("HELLO WORLD".to_string())
        );

        Ok(())
    }

    #[dialog_common::test]
    fn it_matches_like_exact() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "hello".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "hello".to_string().into())
            .unwrap();

        let app: FormulaQuery = Like::apply(terms).expect("apply should work").into();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .lookup(&Term::var("result"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("hello".to_string())
        );
    }

    #[dialog_common::test]
    fn it_matches_like_star_wildcard() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app: FormulaQuery = Like::apply(terms).expect("apply should work").into();

        // Prefix match
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "hello world".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "hello*".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Suffix match
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "hello world".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "*world".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Contains match
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "hello world".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "*lo wo*".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // No match
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "hello world".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "goodbye*".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 0);
    }

    #[dialog_common::test]
    fn it_matches_like_question_wildcard() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app: FormulaQuery = Like::apply(terms).expect("apply should work").into();

        // Single char match
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "cat".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "c?t".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Too few chars
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "ct".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "c?t".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 0);
    }

    #[dialog_common::test]
    fn it_matches_like_with_escape() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app: FormulaQuery = Like::apply(terms).expect("apply should work").into();

        // Escaped star matches literal *
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "a*b".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "a\\*b".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Without escape, * is a wildcard
        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "aXYZb".to_string().into())
            .unwrap();
        input
            .bind(&Term::var("p"), "a*b".to_string().into())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);
    }

    #[dialog_common::test]
    fn it_matches_like_star_all() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app: FormulaQuery = Like::apply(terms).expect("apply should work").into();

        let mut input = Answer::new();
        input
            .bind(&Term::var("t"), "anything".to_string().into())
            .unwrap();
        input.bind(&Term::var("p"), "*".to_string().into()).unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);
    }

    #[dialog_common::test]
    fn it_matches_like_with_backtracking() {
        assert!(glob_match("*121", "12121"));
        assert!(glob_match("*113", "1113"));
        assert!(glob_match("*113", "11113"));
        assert!(glob_match("*ooo?ar", "foooobar"));
        assert!(glob_match("da*da*da*", "daaadabadmanda"));
        assert!(glob_match("*?2", "332"));
        assert!(glob_match("*?2", "3332"));
        assert!(!glob_match("*1?", "123"));
        assert!(!glob_match("*12", "122"));
    }

    #[dialog_common::test]
    fn it_handles_like_empty_inputs() {
        assert!(glob_match("*", ""));
        assert!(glob_match("", ""));
        assert!(!glob_match("?", ""));
        assert!(!glob_match("a", ""));
        assert!(!glob_match("", "a"));
    }

    #[dialog_common::test]
    fn it_matches_like_escaped_question() {
        assert!(glob_match("a\\?b", "a?b"));
        assert!(!glob_match("a\\?b", "axb"));
    }

    #[dialog_common::test]
    fn it_constructs_like_match_struct() {
        let pattern = Query::<Like> {
            text: Term::var("title"),
            pattern: Term::from("Hello*".to_string()),
            is: Term::var("matched"),
        };

        assert!(matches!(pattern.text, Term::Variable { .. }));
        assert!(matches!(pattern.is, Term::Variable { .. }));
        assert!(matches!(pattern.pattern, Term::Constant { .. }));
    }

    #[dialog_common::test]
    fn it_matches_like_escaped_backslash() {
        assert!(glob_match("a\\\\b", "a\\b"));
        assert!(!glob_match("a\\\\b", "axb"));
    }
}
