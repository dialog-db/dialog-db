#![warn(missing_docs)]

//! A notation for Dialog-DB: a small kernel, and the core forms defined
//! in it as rules.
//!
//! This crate is a proof of concept. It reads the notation
//! ([`form`]), expands every form until it is kernel ([`expand`]), and
//! compiles a kernel rule to what Dialog-DB already runs ([`compile`]):
//! concept queries, deductive and inductive rules, facts and dispatched
//! commands.
//!
//! ```no_run
//! # fn main() -> Result<(), dialog_notation::Error> {
//! use dialog_notation::{Expander, compile};
//!
//! let mut expander = Expander::new()?;
//! let (kernel, diagnostics) = expander.submit(
//!     "(concept (task ?t title: ?x)
//!        (xyz.todo ?t text: (:last (:text ?x))))
//!      (query (task ?t title: ?x))",
//! )?;
//! assert!(diagnostics.is_empty());
//! let steps = compile(&kernel[0])?;
//! assert_eq!(steps.len(), 1);
//! # Ok(())
//! # }
//! ```

pub mod compile;
pub mod expand;
pub mod form;
pub mod kernel;
pub mod pattern;

pub use compile::{Query, Step, compile};
pub use expand::{Diagnostic, Expander, Position};
pub use form::{Form, Part, read};

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
