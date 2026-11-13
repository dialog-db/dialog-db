//! What was written, beside what was said.
//!
//! A syntax tree says what a text means and nothing of how it was
//! written: the labelled parts of an application are kept by label, a
//! number is its value, and where anything stood is gone. [`Written`]
//! is the mapping that holds what the tree left out, a node at a time:
//!
//! | left out | held as |
//! |---|---|
//! | the order the parts were written in, and `of:` written or not | the [`Slot`]s of the node |
//! | a token spelled another way than a printer would spell it | the spelling |
//! | where the node is | its [`Span`] |
//! | a comment | its text, on the node below it |
//!
//! It does not hold the text. The space between two tokens is not kept
//! either: it is what lies between where one ends and the next starts.
//! [`print`](crate::print()) takes a tree and its mapping back to the
//! text.

use std::collections::BTreeMap;

/// A place in a text: a line and a column in it, both from zero, the
/// column counted in characters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Point {
    /// The line.
    pub line: u32,
    /// The column.
    pub column: u32,
}

impl Point {
    /// The place `columns` before this one, on the same line.
    pub fn back(self, columns: u32) -> Point {
        Point {
            line: self.line,
            column: self.column.saturating_sub(columns),
        }
    }
}

/// Where a node is: from its first character to just past its last.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Span {
    /// Its first character.
    pub start: Point,
    /// Just past its last.
    pub end: Point,
}

/// A comment: `; text`, to the end of its line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Comment {
    /// The comment as written, its `;` included.
    pub text: String,
    /// Where its `;` is.
    pub at: Point,
}

impl Comment {
    /// What the comment says: its text without the `;` and the space
    /// around it.
    pub fn said(&self) -> &str {
        self.text.trim_start_matches(';').trim()
    }
}

/// One segment of the way from a node to a node inside it.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Segment {
    /// A part with no label of an application, a member of a tuple, or
    /// a tree of a document, by its place.
    Item(usize),
    /// The operator of an application.
    Operator,
    /// A labelled part of an application.
    Term(String),
    /// The key of an entry of a struct.
    Key(usize),
    /// The value of an entry of a struct.
    Value(usize),
    /// What a repeat repeats.
    Repeated,
}

/// The way from a document to a node in it.
pub type Path = Vec<Segment>;

/// One part of an application as it was written, in its place among the
/// others.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Slot {
    /// A part with no label, or the first of them written `of:`.
    Item {
        /// Which of the parts with no label.
        at: usize,
        /// Where `of:` is, when it was written.
        of: Option<Point>,
    },
    /// A labelled part.
    Term {
        /// Its label.
        label: String,
        /// Where the label is.
        at: Point,
    },
    /// `...?rest`.
    Rest {
        /// Where it is.
        at: Point,
    },
}

/// What a tree left out of one node.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Note {
    /// Where the node is.
    pub span: Span,
    /// The token as written, where a printer would spell it otherwise:
    /// `007`, `1.50`, a text with `\n` in it.
    pub spelling: Option<String>,
    /// The parts of an application, in the order written.
    pub order: Vec<Slot>,
    /// The comments above the node, which are about it.
    pub comments: Vec<Comment>,
    /// The comments inside the node that no part is below: before its
    /// closing bracket, or before a `...`.
    pub closing: Vec<Comment>,
}

/// What the trees of a document left out of the text they were parsed
/// from.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Written {
    /// A note a node, by the way to it.
    pub notes: BTreeMap<Path, Note>,
    /// The comments after the last tree.
    pub trailing: Vec<Comment>,
    /// Where the text ends.
    pub end: Point,
}

impl Written {
    /// The note of the node at `path`.
    pub fn note(&self, path: &[Segment]) -> Option<&Note> {
        self.notes.get(path)
    }

    /// Where the node at `path` is.
    pub fn span(&self, path: &[Segment]) -> Option<Span> {
        self.note(path).map(|note| note.span)
    }

    /// The comments about the node at `path`: the ones above it.
    pub fn comments(&self, path: &[Segment]) -> &[Comment] {
        self.note(path).map_or(&[], |note| &note.comments)
    }

    /// Put the node at `path`, and every node inside it, one step
    /// further in: under what a repeat repeats.
    pub(crate) fn repeat(&mut self, path: &[Segment]) {
        let inside: Vec<Path> = self
            .notes
            .range(path.to_vec()..)
            .take_while(|(way, _)| way.starts_with(path))
            .map(|(way, _)| way.clone())
            .collect();
        for way in inside {
            if let Some(note) = self.notes.remove(&way) {
                let mut moved = path.to_vec();
                moved.push(Segment::Repeated);
                moved.extend_from_slice(&way[path.len()..]);
                self.notes.insert(moved, note);
            }
        }
    }
}
