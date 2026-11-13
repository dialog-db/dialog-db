//! Parsing: the first phase, from text to syntax trees.
//!
//! Nothing here knows what a form means. A name is a
//! [`Syntax::Reference`] whatever it names, and whether a form defines
//! one is for the [analyzer](crate::analyze) to say.

use crate::diagnostic::Diagnostic;
use crate::form::read_marked;
use crate::syntax::tree;
use crate::written::{Path, Segment, Written};
use crate::{Error, Syntax};

/// The syntax trees `text` says, one a top-level form, in the order
/// written.
pub fn parse(text: &str) -> Result<Vec<Syntax>, Error> {
    Ok(parse_written(text)?.0)
}

/// The syntax trees `text` says, and what they leave out of it: where
/// each node is, the order its parts were written in, a token spelled
/// its own way, and the comments. [`print`](crate::print()) takes the two
/// back to the text. The first thing refused is the error;
/// [`parse_document`] says every one, and where.
pub fn parse_written(text: &str) -> Result<(Vec<Syntax>, Written), Error> {
    let parsed = parse_document(text);
    match parsed
        .diagnostics
        .into_iter()
        .find_map(|diagnostic| diagnostic.refusal)
    {
        Some(error) => Err(error),
        None => Ok((parsed.trees, parsed.written)),
    }
}

/// What parsing made of a text.
#[derive(Clone, Debug, PartialEq)]
pub struct Parsed {
    /// The trees of the forms that were read, in the order written.
    pub trees: Vec<Syntax>,
    /// What the trees leave out of the text.
    pub written: Written,
    /// Every part of the text that was refused, each with where it is.
    pub diagnostics: Vec<Diagnostic>,
}

/// Parse `text` and say everything in it that is refused, each with
/// where in the text it is. Parsing does not stop at a refusal: a form
/// that is refused gives no tree, and the forms after it are parsed all
/// the same.
pub fn parse_document(text: &str) -> Parsed {
    let (forms, tail, misread) = read_marked(text);
    let mut written = Written {
        trailing: tail.comments,
        end: tail.end,
        ..Written::default()
    };
    let mut diagnostics = Vec::new();
    let mut trees = Vec::new();
    for (form, mark) in &forms {
        let mut path: Path = vec![Segment::Item(trees.len())];
        match tree(form, Some(mark), false, &mut path, &mut written) {
            Ok(node) => trees.push(node),
            Err(error) => {
                // Nothing of a refused form is kept: its place among
                // the trees is the next form's.
                let at = Segment::Item(trees.len());
                written.notes.retain(|way, _| way.first() != Some(&at));
                let span = written.refused.take().unwrap_or(mark.span);
                diagnostics.push(Diagnostic::misread(span, error));
            }
        }
    }
    diagnostics.extend(
        misread
            .into_iter()
            .map(|(error, span)| Diagnostic::misread(span, error)),
    );
    diagnostics.sort_by_key(|diagnostic| diagnostic.span.map(|span| span.start));
    Parsed {
        trees,
        written,
        diagnostics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::print;
    use crate::written::{Point, Slot, Span};

    /// The text a tree and its mapping print is the text they were
    /// parsed from.
    fn back(text: &str) -> Result<(), Error> {
        let (trees, written) = parse_written(text)?;
        assert_eq!(print(&trees, &written), text);
        Ok(())
    }

    #[dialog_common::test]
    fn it_prints_the_prelude_and_the_examples_as_they_were_written() -> Result<(), Error> {
        back(include_str!("prelude.clj"))?;
        back(include_str!("../examples/todo.clj"))?;
        back(include_str!("../examples/done.clj"))
    }

    #[dialog_common::test]
    fn it_prints_labels_in_the_order_written_and_of_where_it_was_written() -> Result<(), Error> {
        back("(todo text: ?x of: ?t state: :open)")?;
        back("(todo ?t state: :open text: ?x)")?;
        back("(expand (?name ?this ...?fields) in: :head)")?;
        back("(expand (?name ...?fields ?this))")?;
        back("(syntax (rule (report ?head ...) ?premise ... ?last))")
    }

    #[dialog_common::test]
    fn it_prints_a_token_as_it_was_spelled() -> Result<(), Error> {
        back(r#"(xyz.item item:1 n: 007 price: 1.50 text: "a\nb \"c\"" at: -0)"#)?;
        back("{name \"Ada\" age: 36 :open true [1 2] ?x}")?;
        let (_, written) = parse_written("(xyz.item item:1 n: 007 m: 7)")?;
        let spelling = |label: &str| {
            written
                .note(&[Segment::Item(0), Segment::Term(label.into())])
                .and_then(|note| note.spelling.clone())
        };
        assert_eq!(spelling("n").as_deref(), Some("007"));
        // What a printer spells the same way is not held.
        assert_eq!(spelling("m"), None);
        Ok(())
    }

    #[dialog_common::test]
    fn it_prints_space_and_lines_as_they_were() -> Result<(), Error> {
        back(
            "\n\n(rule (assert! (xyz.todo ?t   list: ?to))\n\n    (move (xyz.todo ?t\n                    list: ?to)))\n\n\n",
        )?;
        back("(a)(b) [ 1  2 ]{ k: 1 }")
    }

    /// A comment is about the form that ends before it on its line,
    /// and otherwise about the form below it.
    #[dialog_common::test]
    fn it_keeps_a_comment_on_the_form_it_is_about() -> Result<(), Error> {
        let text = "; A to-do, on a list.\n;; Two lines of it.\n(concept (todo ?t\n           ; what it says\n           text: ?x ; shown in the list\n           ; the list it is on\n           list: ?l\n           ; nothing below\n           ) ; the call\n  (xyz.todo ?t text: ?x ; again\n   ...?rest)) ; the whole of it\n; the end\n";
        back(text)?;
        let (_, written) = parse_written(text)?;
        let said = |path: &[Segment]| -> Vec<String> {
            written
                .comments(path)
                .iter()
                .map(|comment| comment.said().to_string())
                .collect()
        };
        assert_eq!(
            said(&[Segment::Item(0)]),
            vec!["A to-do, on a list.", "Two lines of it.", "the whole of it"]
        );
        let call = [Segment::Item(0), Segment::Item(0)];
        assert_eq!(said(&call), vec!["the call"]);
        let field = |label: &str| {
            [
                Segment::Item(0),
                Segment::Item(0),
                Segment::Term(label.into()),
            ]
        };
        // Above its label, and at the end of its line.
        assert_eq!(
            said(&field("text")),
            vec!["what it says", "shown in the list"]
        );
        assert_eq!(said(&field("list")), vec!["the list it is on"]);
        // A comment no form is below, and none ends before, is kept on
        // what it is inside.
        let closing = &written.note(&call).unwrap().closing;
        assert_eq!(closing[0].said(), "nothing below");
        assert_eq!(
            said(&[
                Segment::Item(0),
                Segment::Item(1),
                Segment::Term("text".into())
            ]),
            vec!["again"]
        );
        assert_eq!(written.trailing[0].said(), "the end");
        Ok(())
    }

    /// A comment with a blank line under it is about no form. It is
    /// still kept, and printed where it was.
    #[dialog_common::test]
    fn it_keeps_a_comment_with_a_blank_line_under_it_off_the_form_below() -> Result<(), Error> {
        let text = "; About the document.\n; Two lines of it.\n\n; About the form.\n(xyz.todo ?t\n  ; about nothing\n\n  ; about the text\n  text: ?x)\n";
        back(text)?;
        let (_, written) = parse_written(text)?;
        let said = |path: &[Segment]| -> Vec<String> {
            written
                .comments(path)
                .iter()
                .map(|comment| comment.said().to_string())
                .collect()
        };
        assert_eq!(said(&[Segment::Item(0)]), vec!["About the form."]);
        assert_eq!(
            said(&[Segment::Item(0), Segment::Term("text".into())]),
            vec!["about the text"]
        );
        Ok(())
    }

    /// A token that opens a form or labels a part ends no form, so a
    /// comment after it is about what is below.
    #[dialog_common::test]
    fn it_keeps_a_comment_after_a_bracket_or_a_label_on_the_form_below() -> Result<(), Error> {
        let text = "(xyz.todo ?t text: ; what it says\n  ?x\n  tags: [ ; the first\n    ?a ?rest ... ; any more\n  ])";
        back(text)?;
        let (_, written) = parse_written(text)?;
        let said = |path: &[Segment]| -> Vec<String> {
            written
                .comments(path)
                .iter()
                .map(|comment| comment.said().to_string())
                .collect()
        };
        let part = |label: &str| vec![Segment::Item(0), Segment::Term(label.into())];
        assert_eq!(said(&part("text")), vec!["what it says"]);
        let mut first = part("tags");
        first.push(Segment::Item(0));
        assert_eq!(said(&first), vec!["the first"]);
        let mut repeat = part("tags");
        repeat.push(Segment::Item(1));
        assert_eq!(said(&repeat), vec!["any more"]);
        Ok(())
    }

    #[dialog_common::test]
    fn it_says_where_each_node_is() -> Result<(), Error> {
        let (_, written) = parse_written("(todo ?t\n  state: :open)")?;
        let at = |line, column| Point { line, column };
        assert_eq!(
            written.span(&[Segment::Item(0)]),
            Some(Span {
                start: at(0, 0),
                end: at(1, 15)
            })
        );
        assert_eq!(
            written.span(&[Segment::Item(0), Segment::Term("state".into())]),
            Some(Span {
                start: at(1, 9),
                end: at(1, 14)
            })
        );
        assert_eq!(
            written.note(&[Segment::Item(0)]).unwrap().order,
            vec![
                Slot::Item { at: 0, of: None },
                Slot::Term {
                    label: "state".into(),
                    at: at(1, 2)
                }
            ]
        );
        Ok(())
    }

    /// A tree changed after it was parsed still prints: what the
    /// mapping knows stays where it was written, and what it does not
    /// know follows, the way a printer writes it.
    #[dialog_common::test]
    fn it_prints_a_part_the_mapping_does_not_know() -> Result<(), Error> {
        let (mut trees, written) = parse_written("(todo ?t state: :open)")?;
        let Syntax::Application(_, operand) = &mut trees[0] else {
            panic!("an application");
        };
        operand
            .terms
            .insert("list".into(), Syntax::Variable("l".into()));
        assert_eq!(print(&trees, &written), "(todo ?t state: :open list: ?l)");
        // With no mapping at all it is the one way a printer writes it.
        assert_eq!(
            print(&trees, &Written::default()),
            "(todo ?t list: ?l state: :open)"
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_parses_each_top_level_form_to_a_tree() -> Result<(), Error> {
        let trees = parse("(todo ?t state: :open) (xyz.todo todo:1 text: \"milk\")")?;
        assert_eq!(trees.len(), 2);
        assert!(matches!(&trees[0], Syntax::Application(operator, _)
            if **operator == Syntax::Reference("todo".into())));
        Ok(())
    }

    #[dialog_common::test]
    fn it_says_the_same_tree_however_the_labels_are_ordered() -> Result<(), Error> {
        assert_eq!(
            parse("(todo ?t state: :open text: ?x)")?,
            parse("(todo text: ?x of: ?t state: :open)")?
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_refuses_text_that_is_not_forms() {
        assert!(matches!(parse("(todo ?t"), Err(Error::Read(_))));
    }

    /// Parsing says every part of a text it refuses and where each is,
    /// and gives the trees of the forms around them.
    #[dialog_common::test]
    fn it_says_everything_it_refuses_and_where() {
        let text = "(todo ?t state: :open)
)
(xyz.todo ?t text: ?x
          text: ?y)
(f [... ?a])
(g {: 1})
(xyz.todo todo:1 n: 5)
(h \"never ends";
        let parsed = parse_document(text);
        let said: Vec<(String, u32, u32, u32, u32)> = parsed
            .diagnostics
            .iter()
            .map(|diagnostic| {
                let span = diagnostic.span.expect("the parser says where");
                (
                    diagnostic.message.clone(),
                    span.start.line,
                    span.start.column,
                    span.end.line,
                    span.end.column,
                )
            })
            .collect();
        assert_eq!(
            said,
            vec![
                (
                    "cannot read: a `)` that closes nothing".to_string(),
                    1,
                    0,
                    1,
                    1
                ),
                (
                    "(xyz.todo ?t text: ?x text: ?y) says `text:` twice".to_string(),
                    2,
                    0,
                    3,
                    19
                ),
                ("a `...` that repeats nothing".to_string(), 4, 4, 4, 7),
                (
                    "cannot read: a map key with no name".to_string(),
                    5,
                    4,
                    5,
                    5
                ),
                (
                    "cannot read: a form that never closes".to_string(),
                    7,
                    0,
                    7,
                    1
                ),
                (
                    "cannot read: a text that never ends".to_string(),
                    7,
                    3,
                    7,
                    14
                ),
            ]
        );
        assert!(
            parsed
                .diagnostics
                .iter()
                .all(|diagnostic| diagnostic.severity() == crate::Severity::Error)
        );
        // The forms around them are read, and the mapping is of them.
        assert_eq!(
            parsed.trees,
            parse("(todo ?t state: :open) (xyz.todo todo:1 n: 5)").unwrap()
        );
        assert_eq!(
            parsed
                .written
                .span(&[Segment::Item(1)])
                .map(|span| span.start),
            Some(Point { line: 6, column: 0 })
        );
    }
}
