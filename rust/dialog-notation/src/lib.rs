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
//! | [`Analyzer::analyze`] | syntax trees | analyzed trees, every one a rule of the kernel, which tree each came of, and diagnostics: every refusal and every warning, each with the way to the form it is about |
//! | [`compile`](compile()) | an analyzed tree | what Dialog-DB already runs: concept queries, deductive and inductive rules, facts and dispatched commands |
//!
//! Analysis is the walker ([`expand`]) and the patterns it matches
//! ([`pattern`]); the compiler reads the analyzed tree through
//! [`kernel`]. The notation as parsed ([`form`]) is the parser's own,
//! and what a tree is printed as: nothing after [`parse`](parse())
//! reads it.
//!
//! A tree says what a text means and not how it was written.
//! [`parse_written`] gives, beside the trees, the mapping that holds
//! what they left out ([`written`]): where each node is, the order its
//! parts were written in, a token spelled its own way, and the
//! comments. [`print`](print()) takes the two back to the text, and a
//! [`Diagnostic`] says the way to the form it is about, which the
//! mapping turns into a place.
//!
//! No phase stops at the first thing it refuses. [`parse_document`] and
//! [`Analyzer::analyze`] say every refusal, each a [`Diagnostic`], and
//! [`check`](check()) runs the three phases over a text and gives what
//! it compiles to and every diagnostic with where in the text it is,
//! which is what an editor or a language server asks for.
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
//! let analysis = analyzer.analyze(&trees);
//! assert!(analysis.diagnostics.is_empty());
//! let steps = compile(&analysis.analyzed[0])?;
//! assert_eq!(steps.len(), 1);
//! # Ok(())
//! # }
//! ```

pub mod analyze;
pub mod check;
pub mod compile;
pub mod data;
pub mod diagnostic;
pub mod expand;
pub mod form;
pub mod kernel;
pub mod parse;
pub mod pattern;
pub mod print;
pub mod syntax;
pub mod written;

pub use analyze::{Analysis, Analyzer};
pub use check::{Checked, check};
pub use compile::{Query, Step, compile};
pub use data::Data;
pub use diagnostic::{Diagnostic, Severity};
pub use expand::{Expander, Position};
pub use form::{Form, Part, read};
pub use parse::{Parsed, parse, parse_document, parse_written};
pub use print::print;
pub use syntax::{Parameters, Signature, Syntax};
pub use written::{Comment, Note, Path, Point, Segment, Slot, Span, Written};

/// Why a document was not read, expanded or compiled.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
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
