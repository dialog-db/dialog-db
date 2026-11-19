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

pub mod analyze;
pub mod check;
pub mod compile;
pub mod data;
pub mod declare;
pub mod definition;
pub mod diagnostic;
pub mod expand;
pub mod form;
pub mod kernel;
#[cfg(feature = "repository")]
pub mod live;
pub mod parse;
pub mod pattern;
pub mod print;
pub mod select;
pub mod syntax;
pub mod view;
pub mod written;

pub use analyze::{Analysis, Analyzer};
pub use check::{Checked, check, check_forms};
pub use compile::{Query, Step, Write, compile};
pub use declare::{Concept, Declaration, Key, Line};
pub use definition::{Binding, Defined, Definition, Names, Suggestion};
pub use diagnostic::{Diagnostic, Severity};
pub use expand::{Expander, Position};
pub use form::{Form, Part, read};
pub use parse::{Parsed, parse, parse_document, parse_written};
pub use print::print;
pub use select::{Folded, FoldedPart, Selection};
pub use syntax::{Parameters, Signature, Syntax};
pub use view::plan::{Condition, Count, FoldedCount, Layer, Place, Placed, Plan, Source, Why};
pub use view::tree::{
    Address, Change, Content, Counts, Given, Maintainer, Patch, Posted, Rendered, Row, Shown,
    render,
};
pub use view::{Attribute, CALLED, Element, Event, Input, Node, Piece};
pub use written::{Comment, Note, Path, Point, Segment, Slot, Span, Written};

/// A reference several definitions of one signature stand under, and
/// each of them, by its signature and its definition's entity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidates {
    /// The name as written.
    pub name: String,
    /// Each candidate: its signature, and its definition.
    pub candidates: Vec<(String, String)>,
}

/// Candidates of a name, each its definition and its namespace.
fn candidates(candidates: &[(String, String)]) -> String {
    candidates
        .iter()
        .map(|(signature, definition)| format!("{definition} ({signature})"))
        .collect::<Vec<_>>()
        .join(", ")
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
    /// A hash prefix, `todo#3f`, that several definitions of one
    /// signature begin with: a longer one says which.
    #[error("{} is defined more than once under one signature: {}", .0.name, candidates(&.0.candidates))]
    Several(Box<Candidates>),
    /// A name the space binds to nothing, where other names end as it
    /// does: `cdata/todo` for `todo`.
    #[error("{} is bound to nothing: {} is", .0.name, suggested(&.0.suggestions))]
    Unbound(Box<Suggested>),
    /// A name none of whose signatures the labels written fit: each it
    /// has, and another name ending as it does whose signature fits, if
    /// one does.
    #[error("{} takes {}, not {}{}", .0.name, .0.signatures.join(" or "), labelled(&.0.labels), fitting(&.0.suggestions))]
    Unfitting(Box<Suggested>),
}

fn fitting(suggestions: &[definition::Suggestion]) -> String {
    if suggestions.is_empty() {
        return String::new();
    }
    format!(": {} does", suggested(suggestions))
}

/// A name that was not resolved, the labels it was written with, and
/// what may have been meant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Suggested {
    /// The name as written.
    pub name: String,
    /// The labels written.
    pub labels: Vec<String>,
    /// The signatures the name has, where it has any.
    pub signatures: Vec<String>,
    /// What may have been meant, each where it is and under which
    /// signature.
    pub suggestions: Vec<definition::Suggestion>,
}

fn suggested(suggestions: &[definition::Suggestion]) -> String {
    suggestions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn labelled(labels: &[String]) -> String {
    if labels.is_empty() {
        return "application with no label".into();
    }
    labels
        .iter()
        .map(|label| format!("{label}:"))
        .collect::<Vec<_>>()
        .join(" ")
}
