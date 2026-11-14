//! Printing: from a tree and what it left out, back to the text.
//!
//! [`print()`] writes each token of a tree where the mapping says it was,
//! in the order the mapping says the parts were written. A node the
//! mapping does not know is written the one way a printer writes it:
//! the parts with no label, then the labelled parts by label, a space
//! between tokens.

use crate::Syntax;
use crate::written::{Comment, Path, Point, Segment, Slot, Written};

/// The text `trees` and `written` say: the text the trees were parsed
/// from, where `written` is what parsing it gave. Space at the end of a
/// line is not kept, and a tab is written as a space.
pub fn print(trees: &[Syntax], written: &Written) -> String {
    let mut printer = Printer {
        written,
        text: String::new(),
        here: Point::default(),
        tight: true,
        commented: false,
    };
    let mut path = Path::new();
    for (at, tree) in trees.iter().enumerate() {
        path.push(Segment::Item(at));
        printer.node(tree, &mut path, false);
        path.pop();
    }
    for comment in &written.trailing {
        printer.comment(comment);
    }
    printer.reach(written.end);
    printer.text
}

struct Printer<'a> {
    written: &'a Written,
    text: String,
    /// Where the next character goes.
    here: Point,
    /// Whether the next token stands against the one before it: at the
    /// start, and after an opening bracket.
    tight: bool,
    /// Whether the last token was a comment, which runs to the end of
    /// its line.
    commented: bool,
}

impl Printer<'_> {
    /// Move to `to` when it is ahead, and say whether it was.
    fn reach(&mut self, to: Point) -> bool {
        if to < self.here || (self.commented && to.line == self.here.line) {
            return false;
        }
        while self.here.line < to.line {
            self.text.push('\n');
            self.here.line += 1;
            self.here.column = 0;
        }
        while self.here.column < to.column {
            self.text.push(' ');
            self.here.column += 1;
        }
        true
    }

    /// Write `text` at `at`, or after what is written where no place is
    /// known for it or the place is behind.
    fn token(&mut self, at: Option<Point>, text: &str) {
        let closes = matches!(text, ")" | "]" | "}");
        if !at.is_some_and(|at| self.reach(at)) {
            if self.commented {
                self.text.push('\n');
                self.here.line += 1;
                self.here.column = 0;
            } else if !self.tight && !closes {
                self.text.push(' ');
                self.here.column += 1;
            }
        }
        self.text.push_str(text);
        for c in text.chars() {
            if c == '\n' {
                self.here.line += 1;
                self.here.column = 0;
            } else {
                self.here.column += 1;
            }
        }
        self.tight = matches!(text, "(" | "[" | "{");
        self.commented = false;
    }

    fn comment(&mut self, comment: &Comment) {
        self.token(Some(comment.at), &comment.text);
        self.tight = true;
        self.commented = true;
    }

    /// The comments of `closing` from `next` on that stand before `to`,
    /// or all that are left when there is no `to`.
    fn closing(&mut self, closing: &[Comment], next: &mut usize, to: Option<Point>) {
        while let Some(comment) = closing.get(*next)
            && to.is_none_or(|to| comment.at < to)
        {
            self.comment(comment);
            *next += 1;
        }
    }

    fn node(&mut self, tree: &Syntax, path: &mut Path, key: bool) {
        self.part(tree, path, key, None);
    }

    /// A node, with the comments above it that stand after `label`, the
    /// place of its label: the ones before it are written with the
    /// label.
    fn part(&mut self, tree: &Syntax, path: &mut Path, key: bool, label: Option<Point>) {
        let note = self.written.note(path);
        let span = note.map(|note| note.span);
        let closing = note.map_or(&[][..], |note| &note.closing);
        for comment in note.map_or(&[][..], |note| &note.comments) {
            if label.is_none_or(|label| comment.at > label) {
                self.comment(comment);
            }
        }
        let start = span.map(|span| span.start);
        let end = span.map(|span| span.end.back(1));
        match tree {
            // `'x`, where it was written so: the reader's sugar for
            // `(quote x)`.
            Syntax::Application(_, operand)
                if note.is_some_and(|note| note.spelling.as_deref() == Some("'"))
                    && operand.items.len() == 1
                    && operand.terms.is_empty() =>
            {
                self.token(start, "'");
                self.tight = true;
                path.push(Segment::Item(0));
                self.node(&operand.items[0], path, false);
                path.pop();
            }
            Syntax::Application(operator, operand) => {
                self.token(start, "(");
                path.push(Segment::Operator);
                self.node(operator, path, false);
                path.pop();
                // The parts in the order written, and then any the
                // mapping does not know, the way a printer orders them.
                let mut slots: Vec<Slot> = note
                    .map(|note| note.order.clone())
                    .unwrap_or_default()
                    .into_iter()
                    .filter(|slot| match slot {
                        Slot::Item { at, .. } => *at < operand.items.len(),
                        Slot::Term { label, .. } => operand.terms.contains_key(label),
                    })
                    .collect();
                let nowhere = Point {
                    line: u32::MAX,
                    column: u32::MAX,
                };
                for at in 0..operand.items.len() {
                    if !slots
                        .iter()
                        .any(|slot| matches!(slot, Slot::Item { at: known, .. } if *known == at))
                    {
                        slots.push(Slot::Item { at, of: None });
                    }
                }
                for label in operand.terms.keys() {
                    if !slots.iter().any(
                        |slot| matches!(slot, Slot::Term { label: known, .. } if known == label),
                    ) {
                        slots.push(Slot::Term {
                            label: label.clone(),
                            at: nowhere,
                        });
                    }
                }
                let known = |at: Point| (at != nowhere).then_some(at);
                let mut next = 0;
                for slot in &slots {
                    match slot {
                        Slot::Item { at, of } => {
                            path.push(Segment::Item(*at));
                            let starts = of.or(self.written.span(path).map(|span| span.start));
                            self.closing(closing, &mut next, starts);
                            if let Some(of) = of {
                                self.labelled("of", Some(*of), path);
                            }
                            self.part(&operand.items[*at], path, false, *of);
                            path.pop();
                        }
                        Slot::Term { label, at } => {
                            path.push(Segment::Term(label.clone()));
                            self.closing(closing, &mut next, known(*at));
                            self.labelled(label, known(*at), path);
                            self.part(&operand.terms[label], path, false, known(*at));
                            path.pop();
                        }
                    }
                }
                self.closing(closing, &mut next, None);
                self.token(end, ")");
            }
            Syntax::Tuple(items) => {
                self.token(start, "[");
                for (at, item) in items.iter().enumerate() {
                    path.push(Segment::Item(at));
                    self.node(item, path, false);
                    path.pop();
                }
                self.closing(closing, &mut 0, None);
                self.token(end, "]");
            }
            Syntax::Map(entries) => {
                self.token(start, "{");
                for (at, (name, value)) in entries.iter().enumerate() {
                    path.push(Segment::Key(at));
                    self.node(name, path, true);
                    path.pop();
                    path.push(Segment::Value(at));
                    self.node(value, path, false);
                    path.pop();
                }
                self.closing(closing, &mut 0, None);
                self.token(end, "}");
            }
            atom => {
                let spelled = note
                    .and_then(|note| note.spelling.clone())
                    .unwrap_or_else(|| atom.spelled(key));
                self.token(start, &spelled);
            }
        }
        if let Some(comment) = note.and_then(|note| note.after.as_ref()) {
            self.comment(comment);
        }
    }

    /// A label, after the comments of the part under it that stand
    /// above the label. The part writes the rest of them.
    fn labelled(&mut self, label: &str, at: Option<Point>, path: &Path) {
        if let (Some(at), Some(note)) = (at, self.written.note(path)) {
            for comment in note.comments.iter().filter(|comment| comment.at < at) {
                self.comment(comment);
            }
        }
        self.token(at, &format!("{label}:"));
    }
}
