//! # dialog-prose
//!
//! A natural language prose parser for Datalog, ported from
//! [nl-datalog](https://github.com/harc/nl-datalog).
//!
//! This crate provides a way to parse English-like sentences into Datalog
//! expressions (assertions, retractions, rules, and queries) and to render
//! those expressions back into prose.
//!
//! ## Syntax
//!
//! **Facts** (assertions):
//! ```text
//! Homer is Bart's father
//! ```
//!
//! **Rules** (deductive):
//! ```text
//! X is Y's parent if X is Y's father
//! X is Y's grandfather if X is Z's father and Z is Y's parent
//! ```
//!
//! **Queries**:
//! ```text
//! X is Bart's father?
//! ```
//!
//! **Retractions**:
//! ```text
//! ~Homer is Bart's father
//! ```
//!
//! **Negation** (in rule bodies):
//! ```text
//! X is lonely if ~X has a friend
//! ```
//!
//! **Aggregation** (in rule heads):
//! ```text
//! X has Y.count grandchildren if X is Y's grandfather
//! ```
//!
//! ## How it works
//!
//! Clauses use a name-template system where argument positions are marked with
//! `@`. The sentence `"Homer is Bart's father"` produces:
//! - template: `"@ is @'s father"`
//! - args: `[Value("Homer"), Value("Bart")]`
//!
//! Variables are single uppercase letters (`X`, `Y`, `Z`). Values are
//! capitalized identifiers (`Homer`, `Bart`) or integers. `_` is a wildcard.
//!
//! ## Example
//!
//! ```
//! use dialog_prose::{parse, ast::*};
//!
//! let doc = parse("Homer is Bart's father\nX is Y's parent if X is Y's father").unwrap();
//!
//! // Two statements parsed
//! assert_eq!(doc.statements.len(), 2);
//!
//! // Roundtrip back to prose
//! assert_eq!(
//!     doc.to_string(),
//!     "Homer is Bart's father\nX is Y's parent if X is Y's father"
//! );
//! ```

pub mod ast;
pub mod error;
pub mod parser;
pub mod tokenizer;

pub use error::ParseError;
pub use parser::{parse, parse_statement};
