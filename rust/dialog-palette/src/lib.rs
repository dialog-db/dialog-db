//! A Ubiquity-style command line.
//!
//! Commands say which role each of their fields plays and what may
//! fill it; a [`Grammar`] says which words introduce which roles in a
//! language. [`parse`] reads free text against both and returns scored
//! [`Parse`]s, best first, the way Ubiquity's Parser 2 did.
//!
//! The crate does no IO. A host loads the [`Registry`] (verbs, their
//! arguments, and the candidate rows of each noun concept) from its
//! store and keeps it current, then calls [`parse`] on every keystroke.

#![warn(missing_docs)]

mod grammar;
mod noun;
mod parser;
mod registry;

pub use grammar::{Branching, Grammar, Marker, OBJECT};
pub use noun::{ARBITRARY_TEXT, Suggestion, Value, match_score};
pub use parser::{Filled, Parse, Segment, SegmentKind, parse};
pub use registry::{Argument, Candidate, Context, Memory, Noun, Registry, Selection, Verb};
