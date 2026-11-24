//! The three phases over one text, for whoever shows a document to its
//! author: an editor, a language server, a notebook.
//!
//! [`check`] parses a text, analyzes its trees and compiles the
//! analyzed trees. It gives what the document compiles to, and every
//! diagnostic of every phase with where in the text it is.

use crate::analyze::Analyzer;
use crate::{Error, Syntax};
use std::collections::{BTreeMap, BTreeSet};

use crate::compile::{Step, compile};
use crate::declare::{self, Declaration};
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
    /// For each of `steps`, the top-level form it came of, counted from
    /// 0 among the forms read.
    pub step_forms: Vec<usize>,
    /// What the document declares, in the order written: its concepts
    /// and commands, and its queries. Given whenever analysis refused
    /// nothing, whatever Dialog-DB's compiler refuses, so a back end
    /// that renders a view still reads it.
    pub declarations: Vec<Declaration>,
    /// For each of `declarations`, the top-level form it came of.
    pub declaration_forms: Vec<usize>,
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

    fn of(parsed: crate::parse::Parsed) -> (Checked, Vec<Syntax>) {
        (
            Checked {
                analyzed: Vec::new(),
                steps: Vec::new(),
                step_forms: Vec::new(),
                declarations: Vec::new(),
                declaration_forms: Vec::new(),
                written: parsed.written,
                diagnostics: parsed.diagnostics,
            },
            parsed.trees,
        )
    }

    /// The declarations of the forms `trees` analyzed to, a form in
    /// `refused` left out.
    fn declare(&mut self, trees: &[Syntax], analyzer: &Analyzer, refused: &BTreeSet<usize>) {
        let mut declared: Vec<(usize, Declaration)> = Vec::new();
        for (at, tree) in trees.iter().enumerate() {
            if tree.applies() == Some("concept")
                && !refused.contains(&at)
                && let Some(concept) = declare::concept(tree, at, &self.written, analyzer)
            {
                declared.push((at, Declaration::Concept(concept)));
            }
        }
        for (at, tree) in trees.iter().enumerate() {
            if tree.applies() == Some("query")
                && !refused.contains(&at)
                && let Some(query) = declare::query(tree, at, &self.written)
            {
                declared.push((at, query));
            }
        }
        declared.sort_by_key(|(at, _)| *at);
        for (at, declaration) in declared {
            self.declaration_forms.push(at);
            self.declarations.push(declaration);
        }
    }
}

/// Parse, analyze and compile `text`, with the definitions `analyzer`
/// has been given.
///
/// A later phase runs when the one before it refused nothing, so a
/// refusal is not followed by the refusals it causes downstream. A
/// document with a refusal in it is refused whole: it compiles to
/// nothing, and `analyzer` keeps none of its definitions. To check it
/// form by form, see [`check_forms`].
pub fn check(analyzer: &mut Analyzer, text: &str) -> Checked {
    let (mut checked, trees) = Checked::of(parse_document(text));
    if checked.refusals().next().is_some() {
        return checked;
    }
    let mut trial = analyzer.clone();
    let analysis = trial.analyze(&trees);
    checked.diagnostics = analysis.diagnostics.clone();
    if checked.refusals().next().is_none() {
        checked.declare(&trees, &trial, &BTreeSet::new());
    }
    let mut steps = Vec::new();
    let mut forms = Vec::new();
    let mut analyzed = Vec::new();
    for (tree, origin) in analysis.analyzed.iter().zip(&analysis.origin) {
        let tree = described(tree, &checked.written, *origin);
        let compiled = compile(&tree);
        analyzed.push(tree);
        match compiled {
            Ok(compiled) => {
                forms.extend(std::iter::repeat_n(*origin, compiled.len()));
                steps.extend(compiled);
            }
            // What the compiler refuses is said of the tree the rule
            // came of: nothing yet says which part of it.
            Err(error) => checked.diagnostics.push(Diagnostic::refused(
                trees.get(*origin).cloned(),
                vec![Segment::Item(*origin)],
                error,
            )),
        }
    }
    for (origin, error) in clashes(steps.iter().zip(forms.iter().copied()), &trees) {
        checked.diagnostics.push(Diagnostic::refused(
            trees.get(origin).cloned(),
            vec![Segment::Item(origin)],
            error,
        ));
    }
    for diagnostic in &mut checked.diagnostics {
        diagnostic.span = diagnostic.place(&checked.written);
    }
    if checked.refusals().next().is_none() {
        checked.analyzed = analyzed;
        checked.steps = steps;
        checked.step_forms = forms;
        *analyzer = trial;
    }
    checked
}

/// Parse, analyze and compile `text` form by form, with the definitions
/// `analyzer` has been given. A form that is refused, by whichever
/// phase, gives no tree, no step and no declaration, and every other
/// form gives its own; each refusal says which form it is about, by its
/// way and its place in the text. `analyzer` keeps the definitions of
/// the forms that were not refused.
///
/// A form the reader cannot read is no form: the forms after it are
/// counted from where it would have been.
pub fn check_forms(analyzer: &mut Analyzer, text: &str) -> Checked {
    let (mut checked, trees) = Checked::of(parse_document(text));
    let analysis = analyzer.analyze_forms(&trees);
    checked
        .diagnostics
        .extend(analysis.diagnostics.iter().cloned());
    let mut refused: BTreeSet<usize> = analysis
        .refusals()
        .filter_map(|refusal| match refusal.path.first() {
            Some(Segment::Item(at)) => Some(*at),
            _ => None,
        })
        .collect();
    checked.declare(&trees, analyzer, &refused);
    let mut compiled: Vec<(usize, Syntax, Vec<Step>)> = Vec::new();
    for (tree, origin) in analysis.analyzed.iter().zip(&analysis.origin) {
        if refused.contains(origin) {
            continue;
        }
        let tree = described(tree, &checked.written, *origin);
        match compile(&tree) {
            Ok(steps) => compiled.push((*origin, tree, steps)),
            Err(error) => {
                refused.insert(*origin);
                checked.diagnostics.push(Diagnostic::refused(
                    trees.get(*origin).cloned(),
                    vec![Segment::Item(*origin)],
                    error,
                ));
            }
        }
    }
    let kept = compiled
        .iter()
        .filter(|(origin, _, _)| !refused.contains(origin))
        .flat_map(|(origin, _, steps)| steps.iter().map(|step| (step, *origin)));
    for (origin, error) in clashes(kept, &trees) {
        refused.insert(origin);
        checked.diagnostics.push(Diagnostic::refused(
            trees.get(origin).cloned(),
            vec![Segment::Item(origin)],
            error,
        ));
    }
    for (origin, tree, steps) in compiled {
        if refused.contains(&origin) {
            continue;
        }
        checked.analyzed.push(tree);
        checked
            .step_forms
            .extend(std::iter::repeat_n(origin, steps.len()));
        checked.steps.extend(steps);
    }
    for diagnostic in &mut checked.diagnostics {
        diagnostic.span = diagnostic.place(&checked.written);
    }
    checked
}

/// Each form of `steps` whose report relation is named as one of an
/// earlier form's is, after another site: the two would derive into one
/// relation, and the later is refused, naming both.
fn clashes<'s>(
    steps: impl IntoIterator<Item = (&'s Step, usize)>,
    trees: &[Syntax],
) -> Vec<(usize, Error)> {
    let form = |at: usize| trees.get(at).map(ToString::to_string).unwrap_or_default();
    let mut named: BTreeMap<String, (String, usize)> = BTreeMap::new();
    let mut clashes = Vec::new();
    for (step, origin) in steps {
        for (relation, site) in step.derives() {
            match named.get(&relation) {
                Some((other, at)) if *other != site => {
                    clashes.push((origin, Error::Clash(relation, form(*at), form(origin))));
                }
                Some(_) => {}
                None => {
                    named.insert(relation, (site, origin));
                }
            }
        }
    }
    clashes
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

    /// A form nested past [`crate::form::DEPTH`] is refused where it
    /// goes too deep, and the forms beside it are read; one nested to the
    /// limit is read, analyzed, compiled and kept as a value, every stage
    /// on a thread with the default stack. Nesting a list and a formula
    /// both, since a quote, a bracket and an application each recurse.
    #[test]
    fn it_refuses_a_form_nested_too_deep_and_runs_one_at_the_limit() {
        use crate::form::DEPTH;
        let run = std::thread::spawn(|| -> Result<(), Error> {
            let list = |depth: usize| format!("{}1{}", "[".repeat(depth), "]".repeat(depth));
            let formula =
                |depth: usize| format!("{}?n{}", "(+ 1 : ".repeat(depth), ")".repeat(depth));
            let quoted = |depth: usize| format!("{}x", "'".repeat(depth));
            // `(assert (xyz.note note:1 v: X))` holds X two forms down.
            for deep in [list(DEPTH - 2), quoted(DEPTH - 2)] {
                let mut analyzer = Analyzer::new()?;
                let checked = check(
                    &mut analyzer,
                    &format!("(assert (xyz.note note:1 v: {deep}))"),
                );
                assert!(checked.refusals().next().is_none() || checked.steps.is_empty());
                for tree in &checked.analyzed {
                    let _ = tree.value();
                }
            }
            let mut analyzer = Analyzer::new()?;
            let checked = check(
                &mut analyzer,
                &format!(
                    "(rule (xyz.out ?t n: {}) (xyz.in ?t n: ?n))",
                    formula((DEPTH - 3) / 2)
                ),
            );
            for tree in &checked.analyzed {
                let _ = tree.value();
            }
            for deep in [
                format!("(select (xyz.note ?n v: {}))", list(DEPTH)),
                format!("(select (xyz.note ?n v: {}))", quoted(DEPTH)),
                format!("{}x{}", "(".repeat(2000), ")".repeat(2000)),
            ] {
                let mut analyzer = Analyzer::new()?;
                let checked = check(&mut analyzer, &format!("{deep}\n(select (xyz.a ?a n: ?n))"));
                let refusals: Vec<&Diagnostic> = checked.refusals().collect();
                assert!(
                    matches!(refusals.as_slice(), [refused] if matches!(&refused.refusal, Some(Error::Read(why)) if why.contains("nested"))
                        && refused.span.is_some()),
                    "{refusals:?}"
                );
                let parsed = crate::parse_document(&format!("{deep}\n(select (xyz.a ?a n: ?n))"));
                assert_eq!(parsed.trees.len(), 1, "the form beside it is read");
            }
            Ok(())
        });
        assert!(matches!(run.join(), Ok(Ok(()))));
    }

    /// Every name the notation makes for itself is under one namespace,
    /// `dialog.notation`, and a document that names anything under it is
    /// refused where it does, naming it, with nothing compiled. Every
    /// other name is the author's: `report.quarterly`, `concept.list`,
    /// `?a.b`, `?view-of` and `?source-key` are refused no more. Reading
    /// `syntax.meta`, which is there to be read, is not refused; writing
    /// it is.
    #[test]
    fn it_refuses_only_what_is_under_the_notations_own_namespace() -> Result<(), Error> {
        let refused = [
            (
                r#"(select (dialog.notation.query.row ?x part: "ab"))"#,
                "dialog.notation.query.row",
            ),
            (
                "(rule (dialog.notation.query.row ?x row: 1) (xyz.a ?x))",
                "dialog.notation.query.row",
            ),
            (
                "(select (dialog.notation.select ?x))",
                "dialog.notation.select",
            ),
            (
                "(assert (dialog.notation.once.write once:1 n: 1))",
                "dialog.notation.once.write",
            ),
            (
                "(select (dialog.notation.view.layer ?x row: ?r))",
                "dialog.notation.view.layer",
            ),
            (
                "(select (dialog.notation.report.qabc ?x c0: ?y))",
                "dialog.notation.report.qabc",
            ),
            (
                "(rule (xyz.a ?x) (dialog.notation.concept.list.count-open ?x is: ?n))",
                "dialog.notation.concept.list.count-open",
            ),
            ("(select (dialog.notation ?x))", "dialog.notation"),
            (
                r#"(assert (syntax.meta rule:1 description: "mine"))"#,
                "syntax.meta",
            ),
            (
                "(rule (syntax (dialog.notation.query.row ?x)) (expand (mine ?x)))\n(rule (xyz.out ?a) (mine ?a))",
                "dialog.notation.query.row",
            ),
        ];
        for (text, name) in refused {
            let mut analyzer = Analyzer::new()?;
            let checked = check(&mut analyzer, text);
            let refusals: Vec<&Diagnostic> = checked.refusals().collect();
            assert!(
                refusals
                    .iter()
                    .any(|refused| matches!(&refused.refusal, Some(Error::Reserved(said)) if said == name)),
                "{text}: {refusals:?}"
            );
            assert!(checked.steps.is_empty(), "{text}");
        }
        for text in [
            "(select (report.quarterly ?x total: ?y))",
            "(select (concept.list ?x name: ?y))",
            "(select (xyz.a ?a.b n: ?n))",
            "(select (xyz.a ?a n: ?view-of))",
            "(select (xyz.a ?a n: ?source-key))",
            "(select (xyz.a ?a n: ?collect-0 m: ?query-row))",
            "(select (syntax.meta ?rule description: ?text))",
        ] {
            let mut analyzer = Analyzer::new()?;
            let checked = check(&mut analyzer, text);
            assert!(
                checked.refusals().next().is_none(),
                "{text}: {:?}",
                checked.diagnostics
            );
            assert!(!checked.steps.is_empty(), "{text}");
        }
        // An author's `?a.b` is the author's variable: named only where
        // it must not hold, it is refused as any such variable is, and
        // not taken for one an expansion made.
        let mut analyzer = Analyzer::new()?;
        let checked = check(
            &mut analyzer,
            "(select (xyz.a ?a n: ?n) (unless (xyz.b ?a m: ?a.b)))",
        );
        let plain = check(
            &mut Analyzer::new()?,
            "(select (xyz.a ?a n: ?n) (unless (xyz.b ?a m: ?ab)))",
        );
        assert_eq!(
            checked.refusals().count(),
            plain.refusals().count(),
            "{:?} against {:?}",
            checked.diagnostics,
            plain.diagnostics
        );
        Ok(())
    }

    /// Two sites whose report relations would be named alike are refused,
    /// not read as one: the name is a prefix of the site's reference, and
    /// a test shortens it until two sites of one document meet.
    #[test]
    fn it_refuses_two_sites_named_alike() -> Result<(), Error> {
        crate::compile::SITE_LENGTH.with(|length| length.set(0));
        let mut analyzer = Analyzer::new()?;
        let checked = check(
            &mut analyzer,
            "(select (xyz.out ?a with: ?b total: ?n) (xyz.a ?a n: ?n) (xyz.b ?b n: ?n))
             (select (xyz.out ?a with: ?b other: ?n) (xyz.c ?a n: ?n) (xyz.d ?b n: ?n))",
        );
        crate::compile::SITE_LENGTH.with(|length| length.set(crate::compile::SITE));
        let refusals: Vec<&Diagnostic> = checked.refusals().collect();
        assert!(
            refusals
                .iter()
                .any(|refused| matches!(&refused.refusal, Some(Error::Clash(..)))),
            "{refusals:?}"
        );
        assert!(checked.steps.is_empty());
        Ok(())
    }

    /// A field labelled `this:`, the name every query and rule gives the
    /// entity, is refused where it is written: it panicked the engine,
    /// naming the field and the entity one variable.
    #[test]
    fn it_refuses_a_field_labelled_this() -> Result<(), Error> {
        for text in [
            "(rule (xyz.a ?e this: ?v) (xyz.b ?e w: ?v))",
            "(rule (assert (xyz.a ?e this: ?v)) (xyz.b ?e w: ?v))",
            "(select (xyz.b ?e this: ?v))",
        ] {
            let checked = std::panic::catch_unwind(|| {
                let mut analyzer = Analyzer::new()?;
                Ok::<_, Error>(check(&mut analyzer, text))
            });
            let Ok(checked) = checked else {
                panic!("{text} panicked");
            };
            let checked = checked?;
            let refusals: Vec<&Diagnostic> = checked.refusals().collect();
            assert!(
                matches!(refusals.as_slice(), [refused]
                    if matches!(&refused.refusal, Some(Error::Reserved(name)) if name == "this:")),
                "{text}: {refusals:?}"
            );
            assert!(checked.steps.is_empty(), "{text}");
        }
        Ok(())
    }

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

; Adding a to-do.
(concept (todo/add todo: ?t text: ?x)
  (xyz.todo.add/todo of: ?c is: ?t in: :control)
  (xyz.todo.add/text of: ?c is: ?x in: :control))

; Adding a to-do
; opens it.
(rule (assert (todo ?t text: ?x done: false))
  (todo/add todo: ?t text: ?x)) ; and that is all

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
            "(concept (todo ?t text: ?x)\n  (xyz.todo ?t text: (:last (:text ?x))))\n(concept (todo/add text: ?x)\n  (xyz.todo.add/text of: ?c is: ?x in: :control))\n(assert (todo todo:1 text: \"milk\"))\n(assert (todo/add text: \"eggs\"))\n",
        );
        assert_eq!(checked.refusals().count(), 0);
        let [warning] = checked.diagnostics.as_slice() else {
            panic!("one warning: {:?}", checked.diagnostics);
        };
        assert_eq!(warning.severity(), Severity::Warning);
        assert_eq!(started(warning), (5, 8));
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
            "\n  (select (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))",
        );
        let [refused] = unsupported.diagnostics.as_slice() else {
            panic!("one refusal: {:?}", unsupported.diagnostics);
        };
        assert!(matches!(&refused.refusal, Some(Error::Unsupported(_))));
        assert_eq!(started(refused), (1, 2));
        assert!(unsupported.steps.is_empty());
        Ok(())
    }

    /// The reader gives labels and `...` no meaning, so what they may
    /// not say is the analyzer's to refuse, at the form that says it: a
    /// second part with no label beside labels, `of:` beside a part
    /// with none, and `...` written against what follows it.
    #[dialog_common::test]
    fn it_refuses_what_a_record_and_three_dots_may_not_say() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        for (text, says, at) in [
            (
                "(select (xyz.a ?a n: ?n)\n  (xyz.a ?a ?extra n: ?n))",
                "has 2 parts with no label beside its labelled parts",
                (1, 2),
            ),
            (
                "(select (xyz.a ?a n: ?n)\n  (xyz.a ?a of: ?b))",
                "says `of:` twice",
                (1, 2),
            ),
            (
                "(select (xyz.a ?a n: ?n ...?others))",
                "`...` stands apart from what follows it, `... ?others`",
                (0, 24),
            ),
        ] {
            let checked = check(&mut analyzer, text);
            let [refused] = checked.diagnostics.as_slice() else {
                panic!("one refusal of {text}: {:?}", checked.diagnostics);
            };
            assert!(
                matches!(&refused.refusal, Some(Error::Refused(_))),
                "{text}"
            );
            assert!(refused.message.contains(says), "{}", refused.message);
            assert_eq!(started(refused), at, "{text}");
            assert!(checked.steps.is_empty());
        }
        // `...` and what follows it are no part of their own, and a
        // form with no label is a list of any number of parts.
        let several = "(select (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) \
                       (unless (xyz.b ?a n: ?n) (xyz.c ?a n: ?n)))";
        assert_eq!(check(&mut analyzer, several).refusals().count(), 0);
        Ok(())
    }

    /// A document the compiler refuses is refused whole: the analyzer
    /// keeps none of its definitions.
    #[dialog_common::test]
    fn it_keeps_no_definition_of_a_document_the_compiler_refuses() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let refused = check(
            &mut analyzer,
            "(concept (item ?i n: ?n) (xyz.a ?i n: ?n))\n(select (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))",
        );
        assert!(matches!(
            refused
                .refusals()
                .next()
                .and_then(|refused| refused.refusal.as_ref()),
            Some(Error::Unsupported(_))
        ));
        assert!(refused.steps.is_empty());
        let after = check(&mut analyzer, "(select (item ?i n: ?n))");
        assert!(matches!(
            after
                .refusals()
                .next()
                .and_then(|refused| refused.refusal.as_ref()),
            Some(Error::Unknown(_))
        ));
        Ok(())
    }

    const LIST: &str = "(concept (todo ?t text: ?x list: ?l)
  (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
  (xyz.todo ?t list: (:last ?l)))

(concept (list ?l item: {?k ?i})
  (xyz.list/?k of: ?l is: ?i by: :position))
";

    /// The `the:` of a line's descriptor, as Dialog-DB writes it.
    fn the(line: &crate::declare::Line) -> serde_json::Value {
        let descriptor = line.descriptor.as_ref().expect("a descriptor");
        serde_json::to_value(descriptor).expect("a descriptor writes")["the"].clone()
    }

    /// A line gives the label its value goes by in the concept's head,
    /// and the attribute descriptor it compiles to, a line written as a
    /// read's field and one over a collection alike.
    #[dialog_common::test]
    fn it_gives_each_line_its_label_and_its_descriptor() -> Result<(), Error> {
        let checked = check(&mut Analyzer::new()?, LIST);
        assert_eq!(checked.diagnostics, Vec::new());
        let [Declaration::Concept(todo), Declaration::Concept(list)] =
            checked.declarations.as_slice()
        else {
            panic!("two concepts: {:?}", checked.declarations);
        };
        let labels: Vec<Option<&str>> = todo
            .lines
            .iter()
            .map(|line| line.label.as_deref())
            .collect();
        assert_eq!(labels, vec![Some("text"), Some("list")]);
        assert_eq!(the(&todo.lines[0]), serde_json::json!("xyz.todo/text"));
        assert_eq!(the(&todo.lines[1]), serde_json::json!("xyz.todo/list"));
        let [entries] = list.lines.as_slice() else {
            panic!("one line: {:?}", list.lines);
        };
        assert_eq!(entries.label.as_deref(), Some("item"));
        // The key stands in the entry, and has no label of its own.
        assert_eq!(
            entries.key.as_ref().and_then(|key| key.label.as_deref()),
            None
        );
        assert_eq!(the(entries)["domain"], serde_json::json!("xyz.list"));
        Ok(())
    }

    /// Each declaration and each step says the top-level form it came
    /// of, counted from 0.
    #[dialog_common::test]
    fn it_says_which_form_each_declaration_and_step_came_of() -> Result<(), Error> {
        let text = format!(
            "{LIST}
(assert (todo todo:1 text: \"milk\" list: list:1))

; What is on a list.
(rule (xyz.listed ?t text: ?x) (todo ?t text: ?x list: ?l))
"
        );
        let checked = check(&mut Analyzer::new()?, &text);
        assert_eq!(checked.refusals().count(), 0);
        assert_eq!(checked.declaration_forms, vec![0, 1]);
        assert_eq!(checked.step_forms.len(), checked.steps.len());
        assert!(matches!(checked.steps.first(), Some(Step::Assert(_))));
        assert_eq!(checked.step_forms.first(), Some(&2));
        // The rule, and the description kept of it.
        assert!(checked.step_forms[1..].iter().all(|at| *at == 3));
        assert!(checked.step_forms.len() > 2);
        Ok(())
    }

    /// Checked form by form, a document with a refused form still gives
    /// what its other forms declare and compile to, and the refusal says
    /// which form it is, by its way and its place. `check` refuses the
    /// same document whole.
    #[dialog_common::test]
    fn it_checks_a_document_form_by_form() -> Result<(), Error> {
        let text = format!(
            "{LIST}
(rule (xyz.lost ?t) (nowhere ?t))

; What is on a list.
(rule (xyz.listed ?t text: ?x) (todo ?t text: ?x list: ?l))
"
        );
        let whole = check(&mut Analyzer::new()?, &text);
        assert!(whole.refusals().count() > 0);
        assert!(whole.steps.is_empty());

        let mut analyzer = Analyzer::new()?;
        let checked = check_forms(&mut analyzer, &text);
        let [refusal] = checked.refusals().collect::<Vec<_>>()[..] else {
            panic!("one refusal: {:?}", checked.diagnostics);
        };
        assert!(matches!(refusal.refusal, Some(Error::Unknown(_))));
        assert_eq!(refusal.path.first(), Some(&Segment::Item(2)));
        assert_eq!(started(refusal).0, 7);
        assert_eq!(checked.declaration_forms, vec![0, 1]);
        assert!(!checked.steps.is_empty());
        assert!(checked.step_forms.iter().all(|at| *at == 3));
        // The steps carry the rule's description, as `check` gives it.
        assert!(
            checked
                .steps
                .iter()
                .any(|step| matches!(step, Step::Assert(_)))
        );
        assert_eq!(checked.analyzed.len(), 1);
        // The analyzer keeps what the other forms defined.
        let after = check(&mut analyzer, "(select (todo ?t text: ?x list: ?l))");
        assert_eq!(after.refusals().count(), 0);
        Ok(())
    }

    /// A form the compiler refuses drops only its own steps; what it
    /// declares is still given.
    #[dialog_common::test]
    fn it_keeps_the_other_forms_of_a_document_the_compiler_refuses_a_form_of() -> Result<(), Error>
    {
        let text = "(concept (item ?i n: ?n) (xyz.a ?i n: ?n))
(select (xyz.a ?a n: ?n) (xyz.a ?a n: ?n) (:like ?n : \"x\"))
(assert (item item:1 n: 1))
";
        let checked = check_forms(&mut Analyzer::new()?, text);
        let [refusal] = checked.refusals().collect::<Vec<_>>()[..] else {
            panic!("one refusal: {:?}", checked.diagnostics);
        };
        assert!(matches!(refusal.refusal, Some(Error::Unsupported(_))));
        assert_eq!(refusal.path, vec![Segment::Item(1)]);
        assert_eq!(checked.declaration_forms, vec![0]);
        assert_eq!(checked.step_forms, vec![2]);
        assert!(matches!(checked.steps.as_slice(), [Step::Assert(_)]));
        Ok(())
    }
}
