#![warn(missing_docs)]

//! A notation for Dialog-DB: a small kernel, and the core forms defined
//! in it as rules.
//!
//! This crate is a proof of concept. It works in three phases, and what
//! passes between them is the syntax tree ([`syntax`]), which is kept
//! as data ([`data`]):
//!
//! | phase | takes | gives |
//! |---|---|---|
//! | [`parse`](parse()) | text | syntax trees, names unresolved |
//! | [`Analyzer::analyze`] | syntax trees | analyzed trees, every one a rule of the kernel, and what the definitions said about the document |
//! | [`compile`](compile()) | an analyzed tree | what Dialog-DB already runs: concept queries, deductive and inductive rules, facts and dispatched commands |
//!
//! The compiler reads the tree ([`kernel`]). Inside analysis the walker
//! ([`expand`]) still goes through the notation as parsed ([`form`]),
//! and is to read the tree.
//!
//! ```no_run
//! # fn main() -> Result<(), dialog_notation::Error> {
//! use dialog_notation::{Analyzer, compile, parse};
//!
//! let trees = parse(
//!     "(concept (task ?t title: ?x)
//!        (xyz.todo ?t text: (:last (:text ?x))))
//!      (query (task ?t title: ?x))",
//! )?;
//! let mut analyzer = Analyzer::new()?;
//! let analysis = analyzer.analyze(&trees)?;
//! assert!(analysis.diagnostics.is_empty());
//! let steps = compile(&analysis.analyzed[0])?;
//! assert_eq!(steps.len(), 1);
//! # Ok(())
//! # }
//! ```

pub mod analyze;
pub mod compile;
pub mod data;
pub mod expand;
pub mod form;
pub mod kernel;
pub mod parse;
pub mod pattern;
pub mod syntax;

pub use analyze::{Analysis, Analyzer};
pub use compile::{Query, Step, compile};
pub use data::Data;
pub use expand::{Diagnostic, Expander, Position};
pub use form::{Form, Part, read};
pub use parse::parse;
pub use syntax::{Parameters, Signature, Syntax};

/// Why a document was not read, expanded or compiled.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum Error {
    /// The text is not forms.
    #[error("cannot read: {0}")]
    Read(String),
    /// No rule says what a form is.
    #[error("nothing says what {0} is")]
    Unknown(String),
    /// Two rules say different things about a form.
    #[error("{0} is ambiguous: {1}")]
    Ambiguous(String, String),
    /// A form is understood and not allowed.
    #[error("{0}")]
    Refused(String),
    /// A kernel form has no counterpart in Dialog-DB yet.
    #[error("not supported yet: {0}")]
    Unsupported(String),
}
