//! Tokenizer for natural language Datalog prose.
//!
//! Converts raw text into a stream of classified tokens, following the nl-datalog
//! design of splitting tokenization from parsing.
//!
//! Token types:
//! - **Keyword**: `if`, `and`
//! - **Name**: lowercase words forming clause name parts (e.g. `is`, `father`)
//! - **Var**: single uppercase letter or `_` (variable / wildcard)
//! - **Value**: capitalized identifier (e.g. `Homer`) or integer literal
//! - **Aggregate**: dot-prefixed name (e.g. `.count`)
//! - **BeginList** / **EndList**: `[` and `]`
//! - **Not**: `~`
//! - **Query**: `?`

use std::fmt;

/// A classified token produced by the tokenizer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// A keyword: `if` or `and`.
    Keyword(String),
    /// A clause name part: one or more lowercase words.
    Name(String),
    /// A variable: single uppercase letter.
    Var(char),
    /// A wildcard: `_`.
    Wildcard,
    /// A concrete value: capitalized identifier or integer.
    Value(ValueToken),
    /// An aggregate: `.count`, `.sum`, etc.
    Aggregate(String),
    /// `[`
    BeginList,
    /// `]`
    EndList,
    /// `~` (negation)
    Not,
    /// `?` (query marker)
    Query,
    /// A newline boundary.
    Newline,
}

/// A value token — either an identifier or an integer literal.
#[derive(Debug, Clone, PartialEq)]
pub enum ValueToken {
    Identifier(String),
    Integer(i64),
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Keyword(w) => write!(f, "{w}"),
            Token::Name(n) => write!(f, "{n}"),
            Token::Var(c) => write!(f, "{c}"),
            Token::Wildcard => write!(f, "_"),
            Token::Value(ValueToken::Identifier(s)) => write!(f, "{s}"),
            Token::Value(ValueToken::Integer(n)) => write!(f, "{n}"),
            Token::Aggregate(a) => write!(f, ".{a}"),
            Token::BeginList => write!(f, "["),
            Token::EndList => write!(f, "]"),
            Token::Not => write!(f, "~"),
            Token::Query => write!(f, "?"),
            Token::Newline => writeln!(f),
        }
    }
}

fn is_keyword(word: &str) -> bool {
    matches!(word, "if" | "and")
}

/// Tokenize a single line of input into a sequence of tokens.
///
/// Whitespace is consumed but not emitted. Tokens are classified according to
/// the nl-datalog convention:
/// - A single uppercase letter (not followed by alphanumerics) is a `Var`
/// - An uppercase letter followed by more letters/digits is a `Value` identifier
/// - A sequence of digits is a `Value` integer
/// - `_` alone is a `Wildcard`
/// - A lowercase word that is `if` or `and` is a `Keyword`
/// - Other lowercase words (may contain `'`) are `Name` parts; consecutive
///   name words are merged into a single `Name` token
fn tokenize_line(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    // Accumulator for consecutive name parts.
    let mut name_buf = String::new();

    let flush_name = |name_buf: &mut String, tokens: &mut Vec<Token>| {
        if !name_buf.is_empty() {
            tokens.push(Token::Name(std::mem::take(name_buf)));
        }
    };

    while i < len {
        let ch = chars[i];

        // Skip whitespace (but flush name buffer with space separator)
        if ch.is_ascii_whitespace() {
            // If we're accumulating a name, add a space separator so that
            // consecutive name words like "is", "'s", "father" become "is 's father"
            // But only if there's more name content coming — we'll handle that
            // when we see the next lowercase word.
            i += 1;
            continue;
        }

        // `[`
        if ch == '[' {
            flush_name(&mut name_buf, &mut tokens);
            tokens.push(Token::BeginList);
            i += 1;
            continue;
        }

        // `]`
        if ch == ']' {
            flush_name(&mut name_buf, &mut tokens);
            tokens.push(Token::EndList);
            i += 1;
            continue;
        }

        // `~`
        if ch == '~' {
            flush_name(&mut name_buf, &mut tokens);
            tokens.push(Token::Not);
            i += 1;
            continue;
        }

        // `?`
        if ch == '?' {
            flush_name(&mut name_buf, &mut tokens);
            tokens.push(Token::Query);
            i += 1;
            continue;
        }

        // `.` followed by lowercase → aggregate
        if ch == '.' && i + 1 < len && chars[i + 1].is_ascii_lowercase() {
            flush_name(&mut name_buf, &mut tokens);
            i += 1; // skip the dot
            let start = i;
            while i < len && (chars[i].is_ascii_lowercase() || chars[i].is_ascii_digit()) {
                i += 1;
            }
            let agg: String = chars[start..i].iter().collect();
            tokens.push(Token::Aggregate(agg));
            continue;
        }

        // `_` alone → wildcard (not followed by alphanumeric)
        if ch == '_'
            && (i + 1 >= len
                || !chars[i + 1].is_ascii_alphanumeric() && chars[i + 1] != '_')
        {
            flush_name(&mut name_buf, &mut tokens);
            tokens.push(Token::Wildcard);
            i += 1;
            continue;
        }

        // Uppercase letter → either Var (single letter) or Value (identifier)
        if ch.is_ascii_uppercase() {
            flush_name(&mut name_buf, &mut tokens);
            let start = i;
            i += 1;
            // Check if followed by more letters/digits/underscores
            while i < len && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            if word.len() == 1 {
                // Single uppercase letter → variable
                tokens.push(Token::Var(ch));
            } else {
                // Multi-char capitalized → value identifier
                tokens.push(Token::Value(ValueToken::Identifier(word)));
            }
            continue;
        }

        // Digit sequence → integer value
        if ch.is_ascii_digit() {
            flush_name(&mut name_buf, &mut tokens);
            let start = i;
            while i < len && chars[i].is_ascii_digit() {
                i += 1;
            }
            let num_str: String = chars[start..i].iter().collect();
            let n: i64 = num_str.parse().unwrap_or(0);
            tokens.push(Token::Value(ValueToken::Integer(n)));
            continue;
        }

        // Lowercase letter or apostrophe → name part (may be keyword)
        if ch.is_ascii_lowercase() || ch == '\'' {
            let start = i;
            while i < len
                && (chars[i].is_ascii_lowercase()
                    || chars[i].is_ascii_digit()
                    || chars[i] == '\'')
            {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();

            if is_keyword(&word) {
                flush_name(&mut name_buf, &mut tokens);
                tokens.push(Token::Keyword(word));
            } else {
                // Accumulate consecutive name parts
                if !name_buf.is_empty() {
                    name_buf.push(' ');
                }
                name_buf.push_str(&word);
            }
            continue;
        }

        // Comma (separator in lists) — just skip
        if ch == ',' {
            flush_name(&mut name_buf, &mut tokens);
            i += 1;
            continue;
        }

        // Skip any other character
        i += 1;
    }

    flush_name(&mut name_buf, &mut tokens);
    tokens
}

/// Tokenize a multi-line input, producing a flat token stream with `Newline` separators.
pub fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    for (idx, line) in input.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if idx > 0 && !tokens.is_empty() {
            tokens.push(Token::Newline);
        }
        tokens.extend(tokenize_line(trimmed));
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_fact() {
        let tokens = tokenize("Homer is Bart's father");
        assert_eq!(
            tokens,
            vec![
                Token::Value(ValueToken::Identifier("Homer".into())),
                Token::Name("is".into()),
                Token::Value(ValueToken::Identifier("Bart".into())),
                Token::Name("'s father".into()),
            ]
        );
    }

    #[test]
    fn test_rule_with_if_and() {
        let tokens = tokenize("X is Y's parent if X is Y's father");
        assert_eq!(
            tokens,
            vec![
                Token::Var('X'),
                Token::Name("is".into()),
                Token::Var('Y'),
                Token::Name("'s parent".into()),
                Token::Keyword("if".into()),
                Token::Var('X'),
                Token::Name("is".into()),
                Token::Var('Y'),
                Token::Name("'s father".into()),
            ]
        );
    }

    #[test]
    fn test_aggregate() {
        let tokens = tokenize("X has Y.count grandchildren");
        assert_eq!(
            tokens,
            vec![
                Token::Var('X'),
                Token::Name("has".into()),
                Token::Var('Y'),
                Token::Aggregate("count".into()),
                Token::Name("grandchildren".into()),
            ]
        );
    }

    #[test]
    fn test_negation() {
        let tokens = tokenize("~X has a friend");
        assert_eq!(
            tokens,
            vec![
                Token::Not,
                Token::Var('X'),
                Token::Name("has a friend".into()),
            ]
        );
    }

    #[test]
    fn test_wildcard() {
        let tokens = tokenize("_ is Bart's father");
        assert_eq!(
            tokens,
            vec![
                Token::Wildcard,
                Token::Name("is".into()),
                Token::Value(ValueToken::Identifier("Bart".into())),
                Token::Name("'s father".into()),
            ]
        );
    }

    #[test]
    fn test_query() {
        let tokens = tokenize("X is Bart's father?");
        assert_eq!(
            tokens,
            vec![
                Token::Var('X'),
                Token::Name("is".into()),
                Token::Value(ValueToken::Identifier("Bart".into())),
                Token::Name("'s father".into()),
                Token::Query,
            ]
        );
    }

    #[test]
    fn test_integer_value() {
        let tokens = tokenize("X has 42 points");
        assert_eq!(
            tokens,
            vec![
                Token::Var('X'),
                Token::Name("has".into()),
                Token::Value(ValueToken::Integer(42)),
                Token::Name("points".into()),
            ]
        );
    }

    #[test]
    fn test_list() {
        let tokens = tokenize("[1, 2, 3]");
        assert_eq!(
            tokens,
            vec![
                Token::BeginList,
                Token::Value(ValueToken::Integer(1)),
                Token::Value(ValueToken::Integer(2)),
                Token::Value(ValueToken::Integer(3)),
                Token::EndList,
            ]
        );
    }

    #[test]
    fn test_multiline() {
        let tokens = tokenize("Homer is Bart's father\nHomer is Lisa's father");
        assert_eq!(
            tokens,
            vec![
                Token::Value(ValueToken::Identifier("Homer".into())),
                Token::Name("is".into()),
                Token::Value(ValueToken::Identifier("Bart".into())),
                Token::Name("'s father".into()),
                Token::Newline,
                Token::Value(ValueToken::Identifier("Homer".into())),
                Token::Name("is".into()),
                Token::Value(ValueToken::Identifier("Lisa".into())),
                Token::Name("'s father".into()),
            ]
        );
    }
}
