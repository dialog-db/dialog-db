//! The three phases over one text, for whoever shows a document to its
//! author: an editor, a language server, a notebook.
//!
//! [`check`] parses a text, analyzes its trees and compiles the
//! analyzed trees. It gives what the document compiles to, and every
//! diagnostic of every phase with where in the text it is.

use crate::Syntax;
use crate::analyze::Analyzer;
use crate::compile::{Step, compile};
use crate::diagnostic::{Diagnostic, Severity};
use crate::parse::parse_document;
use crate::written::{Comment, Segment, Written};
use dialog_corpus::Value;

/// What the three phases made of a text.
#[derive(Debug, Clone)]
pub struct Checked {
    /// The analyzed trees the document came to, each with what the
    /// comments about the form it came of say as its `description:`.
    /// Nothing when any part of the document is refused.
    pub analyzed: Vec<Syntax>,
    /// What the analyzed trees compile to, in the order written.
    /// Nothing when any part of the document is refused.
    pub steps: Vec<Step>,
    /// What the trees of the document leave out of its text.
    pub written: Written,
    /// Everything there is to say about the document, each with where
    /// in the text it is.
    pub diagnostics: Vec<Diagnostic>,
}

impl Checked {
    /// The refusals: the diagnostics that are errors.
    pub fn refusals(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity() == Severity::Error)
    }
}

/// Parse, analyze and compile `text`, with the definitions `analyzer`
/// has been given.
///
/// A later phase runs when the one before it refused nothing, so a
/// refusal is not followed by the refusals it causes downstream. A
/// document with a refusal in it is refused whole: it compiles to
/// nothing, and `analyzer` keeps none of its definitions.
pub fn check(analyzer: &mut Analyzer, text: &str) -> Checked {
    let parsed = parse_document(text);
    let mut checked = Checked {
        analyzed: Vec::new(),
        steps: Vec::new(),
        written: parsed.written,
        diagnostics: parsed.diagnostics,
    };
    if checked.refusals().next().is_some() {
        return checked;
    }
    let mut trial = analyzer.clone();
    let analysis = trial.analyze(&parsed.trees);
    checked.diagnostics = analysis.diagnostics;
    let mut steps = Vec::new();
    let mut trees = Vec::new();
    for (analyzed, origin) in analysis.analyzed.iter().zip(&analysis.origin) {
        let analyzed = described(analyzed, &checked.written, *origin);
        let compiled = compile(&analyzed);
        trees.push(analyzed);
        match compiled {
            Ok(compiled) => steps.extend(compiled),
            // What the compiler refuses is said of the tree the rule
            // came of: nothing yet says which part of it.
            Err(error) => checked.diagnostics.push(Diagnostic::refused(
                parsed.trees.get(*origin).cloned(),
                vec![Segment::Item(*origin)],
                error,
            )),
        }
    }
    for diagnostic in &mut checked.diagnostics {
        diagnostic.span = diagnostic.place(&checked.written);
    }
    if checked.refusals().next().is_none() {
        checked.analyzed = trees;
        checked.steps = steps;
        *analyzer = trial;
    }
    checked
}

/// `analyzed` with what the comments about the tree it came of say as
/// its `description:`, when it has none.
fn described(analyzed: &Syntax, written: &Written, origin: usize) -> Syntax {
    let said: Vec<&str> = written
        .comments(&[Segment::Item(origin)])
        .into_iter()
        .map(Comment::said)
        .filter(|said| !said.is_empty())
        .collect();
    let mut analyzed = analyzed.clone();
    if let (false, Syntax::Application(_, operand)) = (said.is_empty(), &mut analyzed) {
        operand
            .terms
            .entry("description".into())
            .or_insert_with(|| Syntax::Constant(Value::String(said.join("\n"))));
    }
    analyzed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::written::Point;

    /// What a comment right above a rule says is the rule's
    /// description: the analyzed tree holds it, and the compiler is
    /// given it. A comment with a blank line under it is not about the
    /// rule.
    #[dialog_common::test]
    fn it_gives_a_rule_the_comment_above_it_as_its_description() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let checked = check(
            &mut analyzer,
            "; How a list is kept.

; Something to do, on a list.
(concept (todo ?t text: ?x done: ?d)
  (xyz.todo ?t text: (:last (:text ?x)) done: (:last ?d)))

; Adding a to-do
; opens it.
(rule (assert! (todo ?t text: ?x done: false))
  (add (todo ?t text: ?x))) ; and that is all

; What is left to do.
(rule (xyz.pending ?t text: ?x)
  (todo ?t text: ?x done: false))

(rule (xyz.done ?t text: ?x)
  (todo ?t text: ?x done: true))
",
        );
        assert_eq!(checked.diagnostics, Vec::new());
        let descriptions: Vec<Option<String>> = checked
            .analyzed
            .iter()
            .map(|rule| {
                crate::kernel::Rule::parse(rule)
                    .ok()
                    .and_then(|rule| rule.description)
            })
            .collect();
        assert_eq!(
            descriptions,
            vec![
                Some("Adding a to-do\nopens it.\nand that is all".to_string()),
                Some("What is left to do.".to_string()),
                None,
            ]
        );
        // The concept's is kept on its form.
        let said: Vec<&str> = checked
            .written
            .comments(&[Segment::Item(0)])
            .into_iter()
            .map(Comment::said)
            .collect();
        assert_eq!(said, vec!["Something to do, on a list."]);

        // Dialog-DB leaves a description out of the rule it builds and
        // out of what it stores of one, so it is kept as a fact about
        // the rule's entity, which a query can find.
        let mut rules = Vec::new();
        let mut described = Vec::new();
        for step in &checked.steps {
            match step {
                Step::Induce(rule) => {
                    assert_eq!(rule.descriptor().description, None);
                    rules.push(rule.this());
                }
                Step::Deduce(rule) => {
                    assert_eq!(rule.descriptor().description, None);
                    rules.push(rule.this());
                }
                Step::Assert(statement) => described.push(statement.this.clone()),
                _ => {}
            }
        }
        assert_eq!(rules.len(), 3);
        assert_eq!(described, rules[..2].to_vec());
        Ok(())
    }

    fn started(diagnostic: &Diagnostic) -> (u32, u32) {
        let start = diagnostic.span.expect("a place in the text").start;
        (start.line, start.column)
    }

    #[dialog_common::test]
    fn it_gives_what_a_document_compiles_to_and_what_was_said_of_it() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let checked = check(
            &mut analyzer,
            "(concept (todo ?t text: ?x)\n  (xyz.todo ?t text: (:last (:text ?x))))\n(assert! (todo todo:1 text: \"milk\"))\n(ad (todo todo:2 text: \"eggs\"))\n",
        );
        assert_eq!(checked.refusals().count(), 0);
        let [warning] = checked.diagnostics.as_slice() else {
            panic!("one warning: {:?}", checked.diagnostics);
        };
        assert_eq!(warning.severity(), Severity::Warning);
        assert_eq!(started(warning), (3, 0));
        assert!(matches!(checked.steps.first(), Some(Step::Assert(_))));
        assert!(checked.steps.len() > 1);
        Ok(())
    }

    /// Each phase says where what it refuses is, and a later phase does
    /// not run on what an earlier one refused.
    #[dialog_common::test]
    fn it_says_where_each_phase_refuses() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;

        let misread = check(&mut analyzer, "(xyz.a ?a n: ?n)\n  (frobnicate ?a))");
        let [refused] = misread.diagnostics.as_slice() else {
            panic!("one refusal: {:?}", misread.diagnostics);
        };
        assert!(matches!(&refused.refusal, Some(Error::Read(_))));
        assert_eq!(started(refused), (1, 17));

        let unknown = check(
            &mut analyzer,
            "(rule (xyz.seen ?a n: ?n)\n  (xyz.a ?a n: ?n)\n  (frobnicate ?a))",
        );
        let [refused] = unknown.diagnostics.as_slice() else {
            panic!("one refusal: {:?}", unknown.diagnostics);
        };
        assert!(matches!(&refused.refusal, Some(Error::Unknown(_))));
        assert_eq!(started(refused), (2, 2));
        assert_eq!(
            refused.span.map(|span| span.end),
            Some(Point {
                line: 2,
                column: 17
            })
        );

        let unsupported = check(
            &mut analyzer,
            "\n  (query (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))",
        );
        let [refused] = unsupported.diagnostics.as_slice() else {
            panic!("one refusal: {:?}", unsupported.diagnostics);
        };
        assert!(matches!(&refused.refusal, Some(Error::Unsupported(_))));
        assert_eq!(started(refused), (1, 2));
        assert!(unsupported.steps.is_empty());
        Ok(())
    }

    /// A document the compiler refuses is refused whole: the analyzer
    /// keeps none of its definitions.
    #[dialog_common::test]
    fn it_keeps_no_definition_of_a_document_the_compiler_refuses() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let refused = check(
            &mut analyzer,
            "(concept (item ?i n: ?n) (xyz.a ?i n: ?n))\n(query (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))",
        );
        assert!(matches!(
            refused
                .refusals()
                .next()
                .and_then(|refused| refused.refusal.as_ref()),
            Some(Error::Unsupported(_))
        ));
        assert!(refused.steps.is_empty());
        let after = check(&mut analyzer, "(query (item ?i n: ?n))");
        assert!(matches!(
            after
                .refusals()
                .next()
                .and_then(|refused| refused.refusal.as_ref()),
            Some(Error::Unknown(_))
        ));
        Ok(())
    }
}
