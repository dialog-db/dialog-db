#![warn(missing_docs)]

//! A notation for Dialog-DB: a small kernel, and the core forms defined
//! in it as rules.
//!
//! This crate is a proof of concept. It works in three phases, and what
//! passes between them is the syntax tree ([`Syntax`]), which is kept
//! as Dialog-DB's composite value:
//!
//! | phase | takes | gives |
//! |---|---|---|
//! | [`parse`](parse()) | text | syntax trees, names unresolved |
//! | [`Analyzer::analyze`] | syntax trees | analyzed trees, every one a rule of the kernel, which tree each came of, and diagnostics: every refusal and every warning, each with the way to the form it is about |
//! | [`compile`](compile()) | an analyzed tree | what Dialog-DB already runs: concept queries, deductive and inductive rules, facts and dispatched commands |
//!
//! Analysis is the walker and the patterns it matches; the compiler
//! reads the analyzed tree as the kernel's forms. The notation as parsed
//! ([`Form`], what [`read`](read()) gives) is the parser's own, and what
//! a tree is printed as: nothing after [`parse`](parse()) reads it.
//!
//! A tree says what a text means and not how it was written.
//! [`parse_written`] gives, beside the trees, the mapping that holds
//! what they left out ([`Written`]): where each node is, the order its
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
//!      (select (task ?t title: ?x))",
//! )?;
//! let mut analyzer = Analyzer::new()?;
//! let analysis = analyzer.analyze(&trees);
//! assert!(analysis.diagnostics.is_empty());
//! let steps = compile(&analysis.analyzed[0])?;
//! assert_eq!(steps.len(), 1);
//! # Ok(())
//! # }
//! ```

mod analyze;
mod check;
mod compile;
mod data;
mod declare;
mod diagnostic;
mod expand;
mod form;
mod kernel;
#[cfg(feature = "repository")]
pub mod live;
mod parse;
mod pattern;
mod print;
mod select;
mod syntax;
pub mod view;
mod written;

pub use analyze::{Analysis, Analyzer};
pub use check::{Checked, check, check_forms};
pub use compile::{Query, Step, Write, compile};
pub use declare::{Concept, Declaration, Key, Line};
pub use diagnostic::{Diagnostic, Severity};
pub use form::{Form, Part, read};
pub use parse::{Parsed, parse, parse_document, parse_written};
pub use print::print;
pub use select::{Folded, Selection};
pub use syntax::{Parameters, Signature, Syntax};
pub use view::plan::{Condition, Count, FoldedCount, Layer, Place, Placed, Plan, Source, Why};
pub use view::tree::{Address, Content, Patch, Posted, Rendered, Shown};
#[cfg(feature = "inspect")]
pub use view::tree::{Change, Counts, Given, Maintainer, Row, render};
pub use view::{Attribute, CALLED, Element, Event, Input, Node, Piece};
pub use written::{Comment, Note, Path, Point, Segment, Slot, Span, Written};

/// A form whose expansion does not end, as the expander refused it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endless {
    /// The form that was to be rewritten once more.
    pub form: String,
    /// The bound it went past: so many steps, so deep, or so many parts.
    pub past: String,
    /// The definitions it was rewritten through, each by its pattern, in
    /// the order first met.
    pub through: Vec<String>,
}

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
    /// A document writes a name the build writes itself, the one given.
    #[error("{0} is a name the notation writes itself, and a document cannot")]
    Reserved(String),
    /// Two sites of a document would derive into one relation, the one
    /// named, each said as its form.
    #[error("{0} would be the relation of two sites, {1} and {2}")]
    Clash(String, String, String),
    /// A form expands to itself again, through the definitions named,
    /// each by its pattern: its expansion would never end.
    #[error("{form} expands to itself again, through {through}", form = .0, through = .1.join(", then "))]
    Cycle(String, Vec<String>),
    /// A form is still expanding past a bound the expander keeps.
    #[error("{} is still expanding past {}, through {}", .0.form, .0.past, .0.through.join(", then "))]
    Endless(Box<Endless>),
}
