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
//!
//! ## Applying a document
//!
//! [`apply`](apply()), with the `repository` feature, takes what
//! [`check`](check()) gave and runs it on a branch: every embedder runs
//! a document with it, so a document means one thing wherever it runs.
//! A document is one transaction, and its forms are a sequence for what
//! they read as for what they write: a write's `when` reads what the
//! forms before it wrote, through the transaction, and a form written
//! twice runs twice. A command a form issues, and what the rules it
//! fires write, land at commit, after every form: a later `when` does
//! not see what a rule makes of it. Queries and selects are answered
//! after the commit, from what the whole document left. A refusal or an
//! error on the way writes nothing. The [`apply`](mod@apply) module says each of
//! these in full.
//!
//! ## What a collection holds, and what is counted
//!
//! `(distinct ?x)` gathers the values a field takes across the rows into
//! one value, and that value is a set: equal values are one. `(count ?x)`
//! of it is the size of the set. It is the one function of a collection
//! there is: `count-distinct`, `sum`, `min`, `max` and `avg` are not
//! there yet, and a fold in a rule is refused, the notation's in a rule
//! that writes and Dialog-DB's in a rule that derives. So of three items
//! titled "same", "same" and "other", the titles are two, and the count
//! of them is 2. To count the items, gather something that holds each,
//! a record with the entity in it, and count that:
//!
//! ```
//! # use dialog_notation::{Analyzer, apply, check};
//! # use dialog_peer::helpers::{test_repo, test_session_with_peer};
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> anyhow::Result<()> {
//! # let (operator, profile) = test_session_with_peer().await;
//! # let repo = test_repo(&operator, &profile).await;
//! # let branch = repo.branch("main").open().perform(&operator).await?;
//! let document = r#"
//!   (concept (item ?t title: ?x)
//!     (xyz.item/title of: ?t is: ?x as: :text cardinality: :one))
//!   (assert (item item:1 title: "same"))
//!   (assert (item item:2 title: "same"))
//!   (assert (item item:3 title: "other"))
//!
//!   (query (item/titles is: ?all) (item _ title: (distinct ?all)))
//!   (query (item/size is: ?s)
//!     (item _ title: (distinct ?all))
//!     (is ?s {:titles (count ?all)}))
//!   (query (item/card ?t is: ?c) (item ?t title: ?x) (is ?c {:id ?t :title ?x}))
//!   (query (item/count is: ?s)
//!     (item/card _ is: (distinct ?cards))
//!     (is ?s {:items (count ?cards)}))
//!
//!   (select (item/titles is: ?all))
//!   (select (item/size is: ?s))
//!   (select (item/count is: ?s))"#;
//! # let checked = check(&mut Analyzer::new()?, document);
//! # assert!(checked.refusals().next().is_none(), "{:?}", checked.diagnostics);
//! let applied = apply(&branch, &checked, &operator).await?;
//! let answers: Vec<String> = applied.answers.iter().map(ToString::to_string).collect();
//! assert_eq!(
//!     answers,
//!     [
//!         r#"(item/titles is: {: "other" : "same"})"#,
//!         "(item/size is: {:titles 2})",
//!         "(item/count is: {:items 3})",
//!     ]
//! );
//! # Ok(())
//! # }
//! ```
//!
//! Against Datomic, whose choices these follow and part from: an
//! aggregate stands only where nothing is derived, in a query's answer
//! as in Datomic's `:find`, never in a rule; what a read gathers is
//! marked `(distinct ?x)`, Datomic's word, and is a set, as Datomic's
//! `distinct` gives one. There is no `count` over the rows of
//! a query, which in Datomic counts tuples and so counts a value once for
//! each row it stands in, and no `:with` to keep a variable that tells
//! rows apart: here a set merges equal answers, and entities are counted
//! by gathering what holds each of them, as above. `(collect ?x)`, the
//! word before `distinct`, is refused saying so.

mod analyze;
#[cfg(feature = "repository")]
pub mod apply;
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
#[cfg(feature = "repository")]
pub use apply::{Applied, ApplyError, apply};
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
pub use view::tree::{Address, Content, Given, Patch, Posted, Rendered, Shown};
#[cfg(feature = "inspect")]
pub use view::tree::{Change, Counts, Maintainer, Row, render};
pub use view::{Attribute, CALLED, Element, Event, Input, Node, Piece};
pub use written::{Comment, Note, Path, Point, Segment, Slot, Span, Written};

/// For tests only: how many characters of a site's reference name the
/// report relations compiled on this thread from now on, so a test can
/// make two sites meet. Twelve by default. It changes naming for every
/// compile on the thread until it is set again, and on wasm every test
/// runs on one thread, so a test that sets it sets it back, `12`, however
/// it ends; its worst outcome is a refusal of two sites named alike.
/// Only with the `inspect` feature.
#[cfg(feature = "inspect")]
pub fn name_sites_with(length: usize) {
    compile::SITE_LENGTH.with(|held| held.set(length));
}

/// For tests only: laying a rule on the overlay on this thread fails at
/// its write after `writes` more have been made, as a failing overlay
/// would; `None` lets every write through. Only with the `inspect`
/// feature.
#[cfg(feature = "inspect")]
pub fn fail_laying_after(writes: Option<usize>) {
    live::FAIL_LAYING.with(|held| held.set(writes));
}

/// Reading the values in the rows a query gives ([`Query::values`], a
/// view's rows, a fold's): a row reported as one record is a composite
/// map keyed by its names, and the whole of a fold, a set or a list, a
/// composite list. These read one back without the composite's own
/// encoding of keys.
///
/// ```no_run
/// # fn main() -> Result<(), dialog_notation::Error> {
/// # let answered: dialog_corpus::Value = unimplemented!();
/// use dialog_notation::value;
///
/// if let Some(card) = value::record(&answered)? {
///     let title = card.get("title");
///     let tags = card.get("tags").map(value::list).transpose()?.flatten();
/// #   let _ = (title, tags);
/// }
/// # Ok(())
/// # }
/// ```
pub mod value {
    /// A record's entries, by name, in order.
    pub use crate::data::Entries as Record;
    /// The record a value is, when it is a composite map keyed by text
    /// with no key twice; `None` for any other value.
    pub use crate::data::entries as record;
    /// The members of the list or set a value is; `None` for any other
    /// value.
    pub use crate::data::items as list;
}

impl Error {
    /// The same error with every text it says passed through `say`: what
    /// puts a form the compiler rewrote back as its author wrote it.
    pub(crate) fn said(self, say: &dyn Fn(&str) -> String) -> Error {
        match self {
            Error::Read(a) => Error::Read(say(&a)),
            Error::Unknown(a) => Error::Unknown(say(&a)),
            Error::Ambiguous(a, b) => Error::Ambiguous(say(&a), say(&b)),
            Error::Refused(a) => Error::Refused(say(&a)),
            Error::Unsupported(a) => Error::Unsupported(say(&a)),
            // A reserved name is what the document or the tree wrote,
            // and is shown as it wrote it.
            Error::Reserved(a) => Error::Reserved(a),
            Error::Clash(a, b, c) => Error::Clash(say(&a), say(&b), say(&c)),
            Error::Unsafe(a) => Error::Unsafe(say(&a)),
            Error::Twice(a, b, c) => Error::Twice(say(&a), say(&b), say(&c)),
            Error::Cycle(a, through) => {
                Error::Cycle(say(&a), through.iter().map(|part| say(part)).collect())
            }
            Error::Endless(endless) => Error::Endless(Box::new(Endless {
                form: say(&endless.form),
                past: say(&endless.past),
                through: endless.through.iter().map(|part| say(part)).collect(),
            })),
        }
    }
}

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
    /// Two sites would derive into one relation, the one named, each
    /// said as its form where it is known. What is shown names the sites,
    /// not the relation, which is the notation's own.
    #[error("{1} and {2} would derive into one relation and answer as one: write one apart")]
    Clash(String, String, String),
    /// A view's template lets data reach script, at the part given: an
    /// element that runs what it holds, an inline handler, or a URL a
    /// variable writes whole.
    #[error("{0}: a value of a row could reach script there")]
    Unsafe(String),
    /// One document defines one name twice, by the two forms given: what
    /// it means would depend on the order they are written in.
    #[error("{0} is defined twice in one document, by {1} and by {2}")]
    Twice(String, String, String),
    /// A form expands to itself again, through the definitions named,
    /// each by its pattern: its expansion would never end.
    #[error("{form} expands to itself again, through {through}", form = .0, through = .1.join(", then "))]
    Cycle(String, Vec<String>),
    /// A form is still expanding past a bound the expander keeps.
    #[error("{} is still expanding past {}, through {}", .0.form, .0.past, .0.through.join(", then "))]
    Endless(Box<Endless>),
}
