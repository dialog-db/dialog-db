//! Tokenization layer — splitting input text into token facts.
//!
//! In dialog-query terms, tokenization is a Formula: a pure computation
//! that takes input text and produces a sequence of tokens. Each token
//! becomes a fact that downstream rules can query.
//!
//! ```rust,ignore
//! // As a dialog-query Formula:
//! #[derive(Formula)]
//! pub struct Tokenize {
//!     pub text: String,
//!     #[derived]
//!     pub tokens: Vec<Token>,
//! }
//!
//! // Or as individual token facts:
//! mod token {
//!     #[derive(Attribute, Clone)]
//!     pub struct Value(pub String);
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Position(pub u32);
//!
//!     #[derive(Attribute, Clone)]
//!     pub struct Kind(pub String);  // "word", "number", "punctuation"
//! }
//!
//! #[derive(Concept)]
//! pub struct Token {
//!     this: Entity,
//!     value: token::Value,
//!     position: token::Position,
//!     kind: token::Kind,
//! }
//! ```

/// A single token extracted from input text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// The token's text value, lowercased for matching.
    pub value: String,
    /// The original text before normalization.
    pub original: String,
    /// Zero-based position in the token sequence.
    pub position: usize,
    /// What kind of token this is.
    pub kind: TokenKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenKind {
    /// A word token (alphabetic).
    Word,
    /// A numeric token.
    Number,
    /// Punctuation.
    Punctuation,
    /// Whitespace (usually filtered out).
    Whitespace,
}

/// Tokenize input text into a sequence of tokens.
///
/// This is deliberately simple — it splits on whitespace and categorizes
/// each token. In the dialog-query integration, this would be a Formula
/// that derives Token facts from an input::Text fact.
pub fn tokenize(text: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut position = 0;

    for word in text.split_whitespace() {
        let kind = if word.chars().all(|c| c.is_alphabetic()) {
            TokenKind::Word
        } else if word.chars().all(|c| c.is_numeric() || c == '.' || c == ',') {
            TokenKind::Number
        } else {
            // Mixed or punctuation — treat as word for now
            TokenKind::Word
        };

        tokens.push(Token {
            value: word.to_lowercase(),
            original: word.to_string(),
            position,
            kind,
        });
        position += 1;
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokenize_simple_command() {
        let tokens = tokenize("translate hello to spanish");
        assert_eq!(tokens.len(), 4);
        assert_eq!(tokens[0].value, "translate");
        assert_eq!(tokens[1].value, "hello");
        assert_eq!(tokens[2].value, "to");
        assert_eq!(tokens[3].value, "spanish");
    }

    #[test]
    fn tokenize_preserves_original_case() {
        let tokens = tokenize("Email Bob");
        assert_eq!(tokens[0].value, "email");
        assert_eq!(tokens[0].original, "Email");
        assert_eq!(tokens[1].value, "bob");
        assert_eq!(tokens[1].original, "Bob");
    }
}
