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

/// Like formula that matches a string against a glob pattern.
///
/// Uses `*` for matching any number of characters and `?` for matching
/// a single character. Use `\` to escape `*`, `?`, or `\` itself.
///
/// Returns the matched text if the pattern matches, or no results if it
/// doesn't.
#[derive(Debug, Clone, dialog_query_macros::Formula)]
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
    pub fn derive(input: dialog_query::dsl::Input<Self>) -> Vec<Self> {
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
/// Uses an iterative backtracking algorithm adapted from
/// [wildmatch](https://github.com/becheran/wildmatch) /
/// [Matching wildcards](https://en.wikipedia.org/wiki/Matching_wildcards).
/// Runs in O(n*m) worst case with no recursion.
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
    use crate::{dsl::Match, selection::Answer, Parameters, Term};

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
    fn test_like_exact_match() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let input = Answer::new()
            .set(Term::var("t"), "hello".to_string())
            .unwrap()
            .set(Term::var("p"), "hello".to_string())
            .unwrap();

        let app = Like::apply(terms).expect("apply should work");
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0]
                .resolve(&Term::<String>::var("result"))
                .ok()
                .and_then(|v| String::try_from(v).ok()),
            Some("hello".to_string())
        );
    }

    #[test]
    fn test_like_star_wildcard() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app = Like::apply(terms).expect("apply should work");

        // Prefix match
        let input = Answer::new()
            .set(Term::var("t"), "hello world".to_string())
            .unwrap()
            .set(Term::var("p"), "hello*".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Suffix match
        let input = Answer::new()
            .set(Term::var("t"), "hello world".to_string())
            .unwrap()
            .set(Term::var("p"), "*world".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Contains match
        let input = Answer::new()
            .set(Term::var("t"), "hello world".to_string())
            .unwrap()
            .set(Term::var("p"), "*lo wo*".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // No match
        let input = Answer::new()
            .set(Term::var("t"), "hello world".to_string())
            .unwrap()
            .set(Term::var("p"), "goodbye*".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_like_question_wildcard() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app = Like::apply(terms).expect("apply should work");

        // Single char match
        let input = Answer::new()
            .set(Term::var("t"), "cat".to_string())
            .unwrap()
            .set(Term::var("p"), "c?t".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Too few chars
        let input = Answer::new()
            .set(Term::var("t"), "ct".to_string())
            .unwrap()
            .set(Term::var("p"), "c?t".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn test_like_escape() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app = Like::apply(terms).expect("apply should work");

        // Escaped star matches literal *
        let input = Answer::new()
            .set(Term::var("t"), "a*b".to_string())
            .unwrap()
            .set(Term::var("p"), "a\\*b".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);

        // Without escape, * is a wildcard
        let input = Answer::new()
            .set(Term::var("t"), "aXYZb".to_string())
            .unwrap()
            .set(Term::var("p"), "a*b".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_like_match_all() {
        let mut terms = Parameters::new();
        terms.insert("text".to_string(), Term::var("t"));
        terms.insert("pattern".to_string(), Term::var("p"));
        terms.insert("is".to_string(), Term::var("result"));

        let app = Like::apply(terms).expect("apply should work");

        let input = Answer::new()
            .set(Term::var("t"), "anything".to_string())
            .unwrap()
            .set(Term::var("p"), "*".to_string())
            .unwrap();
        let results = app.derive(input).expect("Like failed");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_like_backtracking() {
        // Cases that require the `*` to backtrack multiple times
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

    #[test]
    fn test_like_empty_inputs() {
        assert!(glob_match("*", ""));
        assert!(glob_match("", ""));
        assert!(!glob_match("?", ""));
        assert!(!glob_match("a", ""));
        assert!(!glob_match("", "a"));
    }

    #[test]
    fn test_like_escaped_question() {
        // Escaped ? matches literal ?
        assert!(glob_match("a\\?b", "a?b"));
        assert!(!glob_match("a\\?b", "axb"));
    }

    #[test]
    fn test_like_match_struct() {
        let pattern = Match::<Like> {
            text: Term::var("title"),
            pattern: Term::from("Hello*".to_string()),
            is: Term::var("matched"),
        };

        assert!(matches!(pattern.text, Term::Variable { .. }));
        assert!(matches!(pattern.is, Term::Variable { .. }));
        assert!(matches!(pattern.pattern, Term::Constant { .. }));
    }

    #[test]
    fn test_like_escaped_backslash() {
        // Escaped backslash matches literal backslash
        assert!(glob_match("a\\\\b", "a\\b"));
        assert!(!glob_match("a\\\\b", "axb"));
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
