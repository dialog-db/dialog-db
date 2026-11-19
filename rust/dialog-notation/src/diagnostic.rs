//! Diagnostics: what a phase has to say about a document, and where.
//!
//! Every phase can refuse a part of a document, and the analyzer can
//! also say something about a form without refusing it. Each is a
//! [`Diagnostic`]: a message, how much it matters, and where in the
//! document it is about, so an editor can show it there.
//!
//! The parser knows the text, so it says where as a [`Span`]. The
//! analyzer and the compiler know only trees, so they say the way to a
//! node, which the mapping the parser gave beside the trees
//! ([`Written`]) turns into a span.

use crate::written::{Path, Span, Written};
use crate::{Error, Syntax};

/// How much a diagnostic matters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Severity {
    /// The document is refused.
    Error,
    /// The document stands, and something about it is worth saying.
    Warning,
}

/// Something said about a part of a document: a refusal, or what a rule
/// said about a form without refusing it.
#[derive(Clone, Debug, PartialEq)]
pub struct Diagnostic {
    /// The form it is about, when there is a tree to be about: what the
    /// parser refuses never became one.
    pub form: Option<Syntax>,
    /// What was said.
    pub message: String,
    /// Where the form is in the document: the way to it, or to the
    /// tree it came of when it is not a part of what was written.
    pub path: Path,
    /// Where in the text it is, when whoever said it knew the text.
    pub span: Option<Span>,
    /// The refusal, when this is one.
    pub refusal: Option<Error>,
}

impl Diagnostic {
    /// A refusal of `form`, which is at `path`.
    pub fn refused(form: Option<Syntax>, path: Path, error: Error) -> Self {
        Diagnostic {
            form,
            message: error.to_string(),
            path,
            span: None,
            refusal: Some(error),
        }
    }

    /// A refusal of the text at `span`.
    pub fn misread(span: Span, error: Error) -> Self {
        Diagnostic {
            span: Some(span),
            ..Diagnostic::refused(None, Path::new(), error)
        }
    }

    /// How much it matters: a refusal is an error, anything else a
    /// warning.
    pub fn severity(&self) -> Severity {
        match self.refusal {
            Some(_) => Severity::Error,
            None => Severity::Warning,
        }
    }

    /// Where in the text it is: where it says, or where `written` says
    /// the node it is about is.
    pub fn place(&self, written: &Written) -> Option<Span> {
        self.span.or_else(|| written.span(&self.path))
    }
}
