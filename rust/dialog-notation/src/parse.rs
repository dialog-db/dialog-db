//! Parsing: the first phase, from text to syntax trees.
//!
//! Nothing here knows what a form means. A name is a
//! [`Syntax::Reference`] whatever it names, and whether a form defines
//! one is for the [analyzer](crate::analyze) to say.

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
/// back to the text.
pub fn parse_written(text: &str) -> Result<(Vec<Syntax>, Written), Error> {
    let (forms, tail) = read_marked(text)?;
    let mut written = Written {
        trailing: tail.comments,
        end: tail.end,
        ..Written::default()
    };
    let mut path = Path::new();
    let mut trees = Vec::new();
    for (at, (form, mark)) in forms.iter().enumerate() {
        path.push(Segment::Item(at));
        let node = tree(form, Some(mark), false, &mut path, &mut written);
        path.pop();
        trees.push(node?);
    }
    Ok((trees, written))
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

    #[dialog_common::test]
    fn it_keeps_a_comment_on_the_form_below_it() -> Result<(), Error> {
        let text = "; A to-do, on a list.\n;; Two lines of it.\n(concept (todo ?t\n           ; what it says\n           text: ?x ; ends the line\n           ; the list it is on\n           list: ?l\n           ; nothing below\n           )\n  (xyz.todo ?t text: ?x ; again\n   ...?rest))\n; the end\n";
        back(text)?;
        let (_, written) = parse_written(text)?;
        let said = |path: &[Segment]| -> Vec<String> {
            written
                .comments(path)
                .iter()
                .map(|comment| comment.said().to_string())
                .collect()
        };
        let concept = [Segment::Item(0)];
        assert_eq!(
            said(&concept),
            vec!["A to-do, on a list.", "Two lines of it."]
        );
        let field = |label: &str| {
            [
                Segment::Item(0),
                Segment::Item(0),
                Segment::Term(label.into()),
            ]
        };
        assert_eq!(said(&field("text")), vec!["what it says"]);
        // A comment that ends a line is, like any other, on the form
        // below it.
        assert_eq!(
            said(&field("list")),
            vec!["ends the line", "the list it is on"]
        );
        let closing = &written
            .note(&[Segment::Item(0), Segment::Item(0)])
            .unwrap()
            .closing;
        assert_eq!(closing[0].said(), "nothing below");
        assert_eq!(written.trailing[0].said(), "the end");
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
}
