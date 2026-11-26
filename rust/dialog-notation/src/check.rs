//! The three phases over one text, for whoever shows a document to its
//! author: an editor, a language server, a notebook.
//!
//! [`check`] parses a text, analyzes its trees and compiles the
//! analyzed trees. It gives what the document compiles to, and every
//! diagnostic of every phase with where in the text it is.

use crate::analyze::Analyzer;
use crate::{Error, Syntax};
use std::collections::{BTreeMap, BTreeSet};

use crate::compile::Step;
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
    /// Nothing when any part of the document is refused. They are what
    /// is compiled for Dialog-DB, so a derived attribute in them is its
    /// relation, the hash: [`Analyzer::spelled`] writes one as a person
    /// reads it.
    pub(crate) analyzed: Vec<Syntax>,
    /// What the analyzed trees compile to, in the order written.
    /// Nothing when any part of the document is refused.
    pub(crate) steps: Vec<Step>,
    /// For each of `steps`, the top-level form it came of, counted from
    /// 0 among the forms read.
    pub(crate) step_forms: Vec<usize>,
    /// Each form that came to a rule an earlier form came to as well, a
    /// rule, a concept or a query written again, with that earlier form:
    /// its steps are the earlier form's, given once. A write written
    /// again gives its steps again, in its place.
    pub(crate) repeats: Vec<(usize, usize)>,
    /// Whether the document was checked whole, by [`check`]: a refusal
    /// of any form refuses every form. False for [`check_forms`], where
    /// a refusal refuses its own form alone.
    pub(crate) whole: bool,
    /// What the document declares, in the order written: its concepts
    /// and commands, and its queries. Given whenever analysis refused
    /// nothing, whatever Dialog-DB's compiler refuses, so a back end
    /// that renders a view still reads it.
    pub(crate) declarations: Vec<Declaration>,
    /// For each of `declarations`, the top-level form it came of.
    pub(crate) declaration_forms: Vec<usize>,
    /// What the trees of the document leave out of its text.
    pub(crate) written: Written,
    /// Everything there is to say about the document, each with where
    /// in the text it is.
    pub(crate) diagnostics: Vec<Diagnostic>,
}

impl Checked {
    /// The analyzed trees the document came to, each with what the
    /// comments about the form it came of say as its `description:`.
    /// Nothing when any part of the document is refused.
    ///
    /// Plumbing: an embedder applies a document with [`stage`](crate::stage)
    /// and [`answer`](crate::answer), and should prefer them over this.
    pub fn analyzed(&self) -> &[Syntax] {
        &self.analyzed
    }

    /// What the analyzed trees compile to, in the order written.
    ///
    /// Plumbing: an embedder applies a document with [`stage`](crate::stage)
    /// and [`answer`](crate::answer), and should prefer them over this.
    pub fn steps(&self) -> &[Step] {
        &self.steps
    }

    /// For each of [`steps`](Self::steps), the top-level form it came of,
    /// counted from 0 among the forms read.
    ///
    /// Plumbing: an embedder applies a document with [`stage`](crate::stage)
    /// and [`answer`](crate::answer), and should prefer them over this.
    pub fn step_forms(&self) -> &[usize] {
        &self.step_forms
    }

    /// What the document declares, in the order written: its concepts
    /// and commands, and its queries.
    pub fn declarations(&self) -> &[Declaration] {
        &self.declarations
    }

    /// For each of [`declarations`](Self::declarations), the top-level
    /// form it came of.
    pub fn declaration_forms(&self) -> &[usize] {
        &self.declaration_forms
    }

    /// Everything there is to say about the document, each with where in
    /// the text it is.
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    /// The steps, to change: for the tests that make a step fail.
    #[cfg(feature = "inspect")]
    pub fn steps_mut(&mut self) -> &mut Vec<Step> {
        &mut self.steps
    }

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
                repeats: Vec::new(),
                whole: true,
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
    ///
    /// A concept whose lines or read do not compile is refused, and the
    /// forms refused so are given back.
    fn declare(
        &mut self,
        trees: &[Syntax],
        analyzer: &Analyzer,
        refused: &BTreeSet<usize>,
    ) -> BTreeSet<usize> {
        let mut declared: Vec<(usize, Declaration)> = Vec::new();
        let mut failed = BTreeSet::new();
        for (at, tree) in trees.iter().enumerate() {
            if tree.applies() != Some("concept") || refused.contains(&at) {
                continue;
            }
            match declare::concept(tree, at, &self.written, analyzer) {
                Ok(Some(concept)) => declared.push((at, Declaration::Concept(concept))),
                Ok(None) => {}
                Err(error) => {
                    failed.insert(at);
                    self.diagnostics.push(Diagnostic::refused(
                        Some(tree.clone()),
                        vec![Segment::Item(at)],
                        error,
                    ));
                }
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
        failed
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
        // A refusal is among the diagnostics, which refuse the whole.
        checked.declare(&trees, &trial, &BTreeSet::new());
    }
    let mut steps = Vec::new();
    let mut forms = Vec::new();
    let mut analyzed = Vec::new();
    let mut failed: BTreeSet<usize> = BTreeSet::new();
    for (tree, origin) in analysis.analyzed.iter().zip(&analysis.origin) {
        let tree = described(tree, &checked.written, *origin);
        let compiled = crate::compile::compiled(&tree);
        let shown = tree.to_string();
        analyzed.push(tree);
        match compiled {
            Ok(compiled) => {
                forms.extend(std::iter::repeat_n(*origin, compiled.len()));
                steps.extend(compiled);
            }
            // Said once of a form, however many rules it came to.
            Err(_) if failed.contains(origin) => {}
            Err(error) => {
                failed.insert(*origin);
                checked
                    .diagnostics
                    .push(compile_refusal(error, &shown, &trees, *origin));
            }
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
        spell(diagnostic, &names(&analysis.analyzed));
    }
    if checked.refusals().next().is_none() {
        checked.analyzed = analyzed;
        checked.steps = steps;
        checked.step_forms = forms;
        checked.repeats = analysis.repeats.clone();
        *analyzer = trial;
    }
    checked
}

/// What the compiler refuses of the rule `shown`, which came of the tree
/// at `origin`, said of that tree: nothing yet says which part of it.
/// Where the refusal quotes the rule it compiled, the author's form is
/// quoted. [`check`] and [`check_forms`] both say it so.
fn compile_refusal(error: Error, shown: &str, trees: &[Syntax], origin: usize) -> Diagnostic {
    let written = trees.get(origin).cloned();
    let error = match &written {
        Some(written) => {
            let written = written.to_string();
            error.said(&|text| text.replace(shown, &written))
        }
        None => error,
    };
    Diagnostic::refused(written, vec![Segment::Item(origin)], error)
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
    checked.whole = false;
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
    let failed = checked.declare(&trees, analyzer, &refused);
    refused.extend(failed);
    let mut compiled: Vec<(usize, Syntax, Vec<Step>)> = Vec::new();
    for (tree, origin) in analysis.analyzed.iter().zip(&analysis.origin) {
        if refused.contains(origin) {
            continue;
        }
        let tree = described(tree, &checked.written, *origin);
        // The analyzer refused what no rule may write, so the tree is
        // compiled as `check` compiles it, and a refusal is said alike.
        match crate::compile::compiled(&tree) {
            Ok(steps) => compiled.push((*origin, tree, steps)),
            Err(error) => {
                refused.insert(*origin);
                let shown = tree.to_string();
                checked
                    .diagnostics
                    .push(compile_refusal(error, &shown, &trees, *origin));
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
    checked.repeats = analysis
        .repeats
        .iter()
        .filter(|(at, earlier)| !refused.contains(at) && !refused.contains(earlier))
        .copied()
        .collect();
    analyzer.forget(&refused);
    for diagnostic in &mut checked.diagnostics {
        diagnostic.span = diagnostic.place(&checked.written);
        spell(diagnostic, &names(&analysis.analyzed));
    }
    checked
}

/// `diagnostic` as a person reads it: a derived attribute it names is
/// written as the name it is written by, `names` holding Dialog-DB's
/// name for each the document's trees wrote, never the hash of it.
fn spell(diagnostic: &mut Diagnostic, names: &BTreeMap<String, String>) {
    // A reserved name is said as the document wrote it.
    if matches!(diagnostic.refusal, Some(Error::Reserved(_))) {
        return;
    }
    diagnostic.message = crate::syntax::told_with(&diagnostic.message, names);
    diagnostic.refusal = diagnostic
        .refusal
        .take()
        .map(|error| error.said(&|text| crate::syntax::told_with(text, names)));
}

/// The derived attributes `trees` name, by Dialog-DB's name for each.
fn names(trees: &[Syntax]) -> BTreeMap<String, String> {
    trees
        .iter()
        .flat_map(crate::syntax::derived_names)
        .collect()
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
    /// `dialog.n`, and a document that names anything under it is
    /// refused where it does, naming it, with nothing compiled. Every
    /// other name is the author's: `report.quarterly`, `concept.list`,
    /// `?a.b`, `?view-of` and `?source-key` are refused no more. Reading
    /// `syntax.meta`, which is there to be read, is not refused; writing
    /// it is.
    #[test]
    fn it_refuses_only_what_is_under_the_notations_own_namespace() -> Result<(), Error> {
        let refused = [
            (
                r#"(select (dialog.n.query.row ?x part: "ab"))"#,
                "dialog.n.query.row",
            ),
            (
                "(rule (dialog.n.query.row ?x row: 1) (xyz.a ?x))",
                "dialog.n.query.row",
            ),
            ("(select (dialog.n.select ?x))", "dialog.n.select"),
            (
                "(assert (dialog.n.once.write once:1 n: 1))",
                "dialog.n.once.write",
            ),
            (
                "(select (dialog.n.view.layer ?x row: ?r))",
                "dialog.n.view.layer",
            ),
            (
                "(select (dialog.n.report.qabc ?x c0: ?y))",
                "dialog.n.report.qabc",
            ),
            (
                "(rule (xyz.a ?x) (dialog.n.concept.list.count-open ?x is: ?n))",
                "dialog.n.concept.list.count-open",
            ),
            ("(select (dialog.n ?x))", "dialog.n"),
            (
                "(select (dialog.n.c/list/label ?l ?n))",
                "dialog.n.c/list/label",
            ),
            (
                "(rule (dialog.n.c/list/label ?l ?n) (xyz.a ?l n: ?n))",
                "dialog.n.c/list/label",
            ),
            ("(select (dialog.n.anything ?x))", "dialog.n.anything"),
            (
                r#"(assert (syntax.meta rule:1 description: "mine"))"#,
                "syntax.meta",
            ),
            (
                "(rule (syntax (dialog.n.query.row ?x)) (expand (mine ?x)))\n(rule (xyz.out ?a) (mine ?a))",
                "dialog.n.query.row",
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
            // The engine's own `dialog.node` shares a string prefix with
            // `dialog.n` and no segment: the notation leaves it alone.
            "(select (dialog.node.probe/node ?c ?node))",
            // Nothing under the namespace the notation had is its own.
            "(select (dialog.notation.select ?x n: ?y))",
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
        // A derived attribute's domain is told by whole segments.
        use crate::syntax::{Named, named};
        assert_ne!(named("dialog.node.probe"), Named::Reserved);
        assert_eq!(named("dialog.n.cx"), Named::Reserved);
        assert_eq!(named("dialog.n.c/list/count-open"), Named::Derived);
        assert_eq!(
            named(&crate::syntax::derived("list", "count-open")?),
            Named::Reserved
        );
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
        let short = crate::compile::sites_named_with(0);
        let mut analyzer = Analyzer::new()?;
        let checked = check(
            &mut analyzer,
            "(select (xyz.out ?a with: ?b total: ?n) (xyz.a ?a n: ?n) (xyz.b ?b n: ?n))
             (select (xyz.out ?a with: ?b other: ?n) (xyz.c ?a n: ?n) (xyz.d ?b n: ?n))",
        );
        drop(short);
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

    /// What `text` is, as `check` and `check_forms` say it: for each
    /// form, by its text, what it compiled to or that it was refused and
    /// why; and the declarations. The same for every order of the forms
    /// is what a document means whatever order it is written in.
    fn meaning(text: &str) -> Result<(Vec<String>, Vec<String>), Error> {
        let whole = check(&mut Analyzer::new()?, text);
        let mut said: Vec<String> = match whole.refusals().next() {
            Some(_) => {
                let mut refused: Vec<String> = whole
                    .refusals()
                    .filter_map(|refused| refused.refusal.as_ref())
                    .map(|error| format!("refused: {}", error_kind(error)))
                    .collect();
                refused.sort();
                refused.dedup();
                refused
            }
            None => {
                let mut trees: Vec<String> =
                    whole.analyzed.iter().map(ToString::to_string).collect();
                trees.sort();
                trees
            }
        };
        let forms = check_forms(&mut Analyzer::new()?, text);
        let trees = crate::parse(text)?;
        let mut each: Vec<String> = Vec::new();
        for (at, tree) in trees.iter().enumerate() {
            let refused = forms
                .refusals()
                .any(|refused| refused.path.first() == Some(&Segment::Item(at)));
            let compiled = forms.step_forms.iter().filter(|form| **form == at).count();
            each.push(format!("{tree}: refused {refused}, {compiled} steps"));
        }
        each.sort();
        said.extend(each);
        let mut declared: Vec<String> = forms
            .declarations
            .iter()
            .map(|declaration| format!("{declaration:?}"))
            .collect();
        declared.sort();
        Ok((said, declared))
    }

    fn error_kind(error: &Error) -> &'static str {
        match error {
            Error::Read(_) => "read",
            Error::Unknown(_) => "unknown",
            Error::Ambiguous(..) => "ambiguous",
            Error::Refused(_) => "refused",
            Error::Unsupported(_) => "unsupported",
            Error::Reserved(_) => "reserved",
            Error::Clash(..) => "clash",
            Error::Twice(..) => "twice",
            Error::Unsafe(_) => "unsafe",
            Error::Cycle(..) => "cycle",
            Error::Endless(..) => "endless",
        }
    }

    /// Every order of `forms`, each once: Heap's algorithm.
    fn orders(forms: &[&str]) -> Vec<Vec<String>> {
        let mut forms: Vec<String> = forms.iter().map(ToString::to_string).collect();
        let mut all = vec![forms.clone()];
        let mut counts = vec![0; forms.len()];
        let mut at = 0;
        while at < forms.len() {
            if counts[at] < at {
                forms.swap(if at % 2 == 0 { 0 } else { counts[at] }, at);
                all.push(forms.clone());
                counts[at] += 1;
                at = 0;
            } else {
                counts[at] = 0;
                at += 1;
            }
        }
        all
    }

    /// A collecting read that is refused is named as its author wrote
    /// it: no refusal says a variable the compiler made.
    #[test]
    fn it_names_a_refused_collecting_read_as_it_was_written() -> Result<(), Error> {
        let concept =
            "(concept (n ?n title: ?t) (xyz.n/title of: ?n is: ?t as: :text cardinality: :one))";
        for read in [
            "(nothing ?n block: (distinct ?b))",
            "(n ?n other: (distinct ?b))",
        ] {
            let checked = check(
                &mut Analyzer::new()?,
                &format!(
                    "{concept} (query (n/o ?n is: ?o) (n ?n title: ?t) {read} (is ?o {{:b ?b}}))"
                ),
            );
            let messages: Vec<&str> = checked
                .diagnostics
                .iter()
                .map(|diagnostic| diagnostic.message.as_str())
                .collect();
            assert_eq!(
                messages,
                vec![format!("nothing says what {read} is")],
                "{read}"
            );
        }
        Ok(())
    }

    /// A document means the same in every order of its forms: what it
    /// declares, what each form compiles to, or that it is refused and
    /// why. A name a macro's expansion declares beside a concept of the
    /// same signature, and a query and a concept of one name, are
    /// refused naming both forms, whichever comes first.
    #[test]
    fn it_means_the_same_in_every_order_of_its_forms() -> Result<(), Error> {
        let memo = "(concept (memo ?t text: ?x) (xyz.note ?t text: ?x))";
        let kind = "(rule (syntax (concept (?name ?t text: ?x) (xyz.memo ?t text: ?x))) \
                    (expand (memo-kind ?name)))";
        let documents: Vec<Vec<&str>> = vec![
            vec![
                memo,
                kind,
                "(select (memo ?t text: ?x))",
                "(memo-kind memo)",
            ],
            vec![
                "(query (foo ?t is: ?x) (xyz.a ?t v: ?x))",
                "(concept (foo ?t w: ?w) (xyz.b ?t w: ?w))",
                "(select (xyz.c ?t v: ?v))",
            ],
            vec![
                "(concept (task ?t title: ?x) (xyz.todo ?t text: ?x))",
                "(rule (xyz.out ?t n: ?x) (task ?t title: ?x))",
                "(select (task ?t title: ?x))",
                "(query (task/card ?t is: ?c) (task ?t title: ?x) (is ?c {:title ?x}))",
            ],
            vec![
                "(concept (a ?t v: ?x) (xyz.a ?t v: ?x))",
                "(concept (a ?t v: ?x) (xyz.b ?t v: ?x))",
                "(select (a ?t v: ?x))",
            ],
            // A concept a macro's expansion declares, read by a rule and
            // a select of the same document: whatever the order, the
            // read is the concept's or it is refused.
            vec![
                "(rule (syntax (concept (?name ?t is: ?x) \
                 (xyz.memo/text of: ?t is: ?x as: :text cardinality: :one))) \
                 (expand (memo-kind ?name)))",
                "(memo-kind memo/note)",
                "(rule (xyz.out ?t v: ?x) (memo/note ?t is: ?x))",
                "(select (memo/note ?t is: ?x))",
            ],
            // An attribute named under a concept a macro's expansion
            // declares.
            vec![
                "(rule (syntax (concept (?name ?t name: ?x) \
                 (xyz.list/name of: ?t is: ?x as: :text cardinality: :one))) \
                 (expand (list-kind ?name)))",
                "(list-kind list)",
                "(rule (xyz.out ?t v: ?x) (list/count ?t ?x))",
            ],
            // Two concepts of one name and other labels: an entry field
            // under one, a plain field under the other.
            vec![
                "(concept (notebook/blocks ?n block: {?k ?b}) \
                 (xyz.tonk.notebook/?k of: ?n is: ?b as: :entity cardinality: :many by: :position))",
                "(concept (notebook/blocks ?n note: ?x) \
                 (xyz.tonk.notebook/note of: ?n is: ?x as: :text cardinality: :one))",
                "(concept (notebook/named ?n title: ?t) \
                 (xyz.tonk.notebook/title of: ?n is: ?t as: :text cardinality: :one))",
                "(query (notebook/outline ?n is: ?o) (notebook/named ?n title: ?t) \
                 (notebook/blocks ?n block: (distinct ?blocks)) (is ?o {:title ?t :blocks ?blocks}))",
                "(select (notebook/outline ?n is: ?o))",
            ],
        ];
        // Each order means what the order as listed means, which declares
        // a name above what reads it, a name a macro's expansion declares
        // below what reads it among them: the read is expanded again once
        // the name is declared. No order is told to declare a concept
        // above what reads it, and none means otherwise.
        for forms in documents {
            let first = meaning(&forms.join("\n"))?;
            for order in orders(&forms) {
                let text = order.join("\n");
                let above = check(&mut Analyzer::new()?, &text).refusals().any(|refused| {
                    matches!(&refused.refusal, Some(Error::Refused(why)) if why.contains("declare the concept above"))
                });
                assert!(!above, "{order:#?}");
                assert_eq!(meaning(&text)?, first, "{order:#?} against {forms:#?}");
            }
        }
        // A concept over another concept, under a name with a `/` and
        // under one without, beside what reads it: every order means
        // the same, with no exception, and none is refused.
        let alt = "(concept (alt ?a text: ?x) \
                   (xyz.alt/text of: ?a is: ?x as: :text cardinality: :one))";
        for forms in [
            vec![
                "(concept (doc/plain ?d text: ?x) (alt ?d text: ?x))",
                alt,
                "(select (doc/plain ?d text: ?x))",
            ],
            vec![
                "(concept (plain ?d text: ?x) (alt ?d text: ?x))",
                alt,
                "(rule (plain/len ?d ?x) (plain ?d text: ?x))",
                "(select (plain/len ?d ?x))",
            ],
        ] {
            let first = meaning(&forms.join("\n"))?;
            assert!(
                !first
                    .0
                    .iter()
                    .any(|said| said.starts_with("refused:") || said.contains("refused true")),
                "{forms:#?}: {first:#?}"
            );
            for order in orders(&forms) {
                assert_eq!(meaning(&order.join("\n"))?, first, "{order:#?}");
            }
        }
        // Concepts whose bodies read each other round, two and three:
        // refused as a cycle in every order alike.
        for forms in [
            vec![
                "(concept (a ?t v: ?x) (b ?t v: ?x))",
                "(concept (b ?t v: ?x) (a ?t v: ?x))",
                "(select (a ?t v: ?x))",
            ],
            vec![
                "(concept (p ?t v: ?x) (q ?t v: ?x))",
                "(concept (q ?t v: ?x) (r ?t v: ?x))",
                "(concept (r ?t v: ?x) (p ?t v: ?x))",
            ],
        ] {
            let first = meaning(&forms.join("\n"))?;
            assert!(
                first.0.iter().any(|said| said == "refused: cycle"),
                "{first:#?}"
            );
            for order in orders(&forms) {
                assert_eq!(meaning(&order.join("\n"))?, first, "{order:#?}");
            }
        }
        // The two the reviewer wrote, refused naming both forms.
        for (text, named) in [
            (
                format!("{memo}\n{kind}\n(select (memo ?t text: ?x))\n(memo-kind memo)"),
                ["xyz.note", "memo-kind"],
            ),
            (
                "(query (foo ?t is: ?x) (xyz.a ?t v: ?x))\n(concept (foo ?t w: ?w) (xyz.b ?t w: ?w))"
                    .to_string(),
                ["(query (foo", "(concept (foo"],
            ),
        ] {
            let checked = check(&mut Analyzer::new()?, &text);
            assert!(
                checked.refusals().any(|refused| matches!(&refused.refusal,
                    Some(Error::Twice(_, first, second))
                        if named.iter().all(|name| first.contains(name) || second.contains(name)))),
                "{text}: {:?}",
                checked.diagnostics
            );
        }
        Ok(())
    }

    /// A concept's lines are declared as the primitive forms read them: a type,
    /// a cardinality, an elector, a variant's cases and an entry of a
    /// collection each give the line its type, its cardinality and its
    /// label, and the value under them is what it is.
    #[test]
    fn it_declares_every_line_as_its_primitive_read() -> Result<(), Error> {
        let checked = check(
            &mut Analyzer::new()?,
            r#"(concept (todo ?t text: ?x state: ?s list: ?l)
                 (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
                 (xyz.todo/state of: ?t is: ?s as: [:open :done] cardinality: :many)
                 (xyz.todo/list of: ?t is: ?l as: :entity cardinality: :one))
               (concept (account ?a status: (max ?s))
                 (xyz.account/status of: ?a is: ?s as: [:active :suspended] cardinality: :many))"#,
        );
        assert!(
            checked.refusals().next().is_none(),
            "{:?}",
            checked.diagnostics
        );
        let [Declaration::Concept(todo), Declaration::Concept(account)] =
            checked.declarations.as_slice()
        else {
            panic!("two concepts: {:?}", checked.declarations);
        };
        let line = |concept: &crate::Concept, label: &str| {
            concept
                .lines
                .iter()
                .find(|line| line.label.as_deref() == Some(label))
                .cloned()
                .unwrap_or_else(|| panic!("a line labelled {label}: {:?}", concept.lines))
        };
        let text = line(todo, "text");
        assert_eq!(text.is, Syntax::Variable("x".into()));
        assert_eq!(text.kind.map(|kind| kind.to_string()), Some(":text".into()));
        assert_eq!(text.cardinality.as_deref(), Some("one"));
        assert!(text.descriptor.is_some());
        let state = line(todo, "state");
        assert_eq!(state.is, Syntax::Variable("s".into()));
        assert_eq!(state.cardinality.as_deref(), Some("many"));
        assert_eq!(
            state.kind.map(|kind| kind.to_string()),
            Some("[:open :done]".into())
        );
        let list = line(todo, "list");
        assert_eq!(list.is, Syntax::Variable("l".into()));
        assert_eq!(list.cardinality.as_deref(), Some("one"));
        let status = line(account, "status");
        assert_eq!(status.is, Syntax::Variable("s".into()));
        assert_eq!(status.cardinality.as_deref(), Some("many"));
        Ok(())
    }

    /// A refusal of `(!= …)` names `!=`, as it was written, and not the
    /// `(== …)` it is the absence of.
    #[test]
    fn it_refuses_an_inequality_naming_it_as_written() -> Result<(), Error> {
        let checked = check(
            &mut Analyzer::new()?,
            "(select (xyz.a ?x y: ?y) (xyz.a ?x y: ?y) (!= ?y : 1))",
        );
        let said: Vec<&str> = checked
            .refusals()
            .map(|refused| refused.message.as_str())
            .collect();
        assert!(
            said.iter().any(|said| said.contains("(!= "))
                && !said.iter().any(|said| said.contains("(== ")),
            "{said:?}"
        );
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
  (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one)
  (xyz.todo/done of: ?t is: ?d cardinality: :one))

; Adding a to-do.
(concept (todo/add todo: ?t text: ?x)
  (xyz.todo.add/todo of: ?c is: ?t in: :control cardinality: :many)
  (xyz.todo.add/text of: ?c is: ?x in: :control cardinality: :many))

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
                crate::primitive::Rule::parse(rule)
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
            "(concept (todo ?t text: ?x)\n  (xyz.todo/text of: ?t is: ?x as: :text cardinality: :one))\n(concept (todo/add text: ?x)\n  (xyz.todo.add/text of: ?c is: ?x in: :control cardinality: :many))\n(assert (todo todo:1 text: \"milk\"))\n(assert (todo/add text: \"eggs\"))\n",
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
  (xyz.todo/list of: ?t is: ?l cardinality: :one))

(concept (list ?l item: {?k ?i})
  (xyz.list/?k of: ?l is: ?i by: :position cardinality: :many))
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

    /// The names the notation makes from what a document writes are one
    /// name for one input. A concept named with a dot, `my.list`, is
    /// refused, and so is a derived attribute, `my/list.count`: neither
    /// is declared with a `.`, so no two declared names hash alike. A head's key of an entry, `entry/key`,
    /// is refused where a field of the same head is named so.
    #[test]
    fn it_refuses_two_inputs_that_would_make_one_name() -> Result<(), Error> {
        let my =
            "(concept (my ?l name: ?n) (xyz.my/name of: ?l is: ?n as: :text cardinality: :one))";
        let checked = check(
            &mut Analyzer::new()?,
            "(concept (my.list ?l name: ?n) (xyz.mylist/name of: ?l is: ?n as: :text cardinality: :one))",
        );
        assert!(
            checked.refusals().any(|refused| matches!(&refused.refusal,
                Some(Error::Refused(why)) if why.contains("`my.list`"))),
            "{:?}",
            checked.diagnostics
        );
        let checked = check(
            &mut Analyzer::new()?,
            &format!("{my} (rule (my/list.count ?l ?n) (my ?l name: ?n))"),
        );
        assert!(
            checked.refusals().any(|refused| matches!(&refused.refusal,
                Some(Error::Refused(why)) if why.contains("`my/list.count`") && why.contains("has none"))),
            "{:?}",
            checked.diagnostics
        );
        // What the dotted forms would spell alike is two names.
        let named = crate::syntax::derived("my", "list.count")?;
        assert_ne!(named, crate::syntax::derived("my.list", "count")?);
        assert_ne!(
            crate::syntax::derived("a", "b.c")?,
            crate::syntax::derived("a.b", "c")?
        );
        let checked = check(
            &mut Analyzer::new()?,
            "(concept (bag ?s item: {?i ?b} odd: ?x)
               (xyz.bag/?i of: ?s is: ?b as: :entity cardinality: :many by: :position)
               (xyz.bag/entry/key of: ?s is: ?x as: :text cardinality: :one))
             (rule (bag ?s item: {?i ?b} odd: ?x) (xyz.src ?s i: ?i b: ?b x: ?x))",
        );
        assert!(
            checked.refusals().any(|refused| matches!(&refused.refusal,
                Some(Error::Refused(why)) if why.contains("entry/key"))),
            "{:?}",
            checked.diagnostics
        );
        // The reader never reads a `[` in a variable, so no document
        // writes a variable the notation makes.
        for text in [
            "(select (xyz.a ?x[1] v: 1))",
            "(select (xyz.a ?[q1]x v: 1))",
        ] {
            assert!(
                crate::parse(text)
                    .map(|trees| trees.iter().all(|tree| {
                        let mut names = Vec::new();
                        tree.variables(&mut names);
                        names.iter().all(|name| !crate::syntax::unwritten(name))
                    }))
                    .unwrap_or(true),
                "{text}"
            );
        }
        Ok(())
    }

    /// The derived relations a check made, by the debug form of its steps:
    /// each a domain under the notation's prefix for derived attributes.
    fn derived_in(checked: &Checked) -> Vec<String> {
        let said = format!("{:?}", checked.steps);
        let mut found: Vec<String> = said
            .match_indices("dialog.n.c.")
            .map(|(at, _)| {
                said[at..]
                    .split(['/', '"'])
                    .next()
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();
        found.sort();
        found.dedup();
        found
    }

    /// A derived attribute's relation is named by a hash of its concept's
    /// written name and its own, of one length whatever they are: the
    /// same concept and attribute in two documents make one relation;
    /// long names compile; a
    /// label past what the relation leaves room for is refused saying
    /// so; and no refusal an author sees names the hash.
    #[test]
    fn it_names_a_derived_attribute_by_a_hash_of_what_it_is() -> Result<(), Error> {
        let list = "(concept (list ?l name: ?n) (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))";
        let rule = "(rule (list/count-open ?l ?n) (list ?l name: ?n))";
        let once = derived_in(&check(&mut Analyzer::new()?, &format!("{list} {rule}")));
        let again = derived_in(&check(
            &mut Analyzer::new()?,
            &format!("{list}\n(select (list ?l name: ?n))\n{rule}"),
        ));
        assert_eq!(once.len(), 1, "{once:?}");
        assert_eq!(once, again);
        assert_eq!(once[0].len(), "dialog.n.c.".len() + 26, "{once:?}");
        let long = "abcdefghijabcdefghijabcdefghijabcd";
        let cafe = "caf\u{e9}";
        let text = format!(
            "(concept ({long} ?l name: ?n) (xyz.l/name of: ?l is: ?n as: :text cardinality: :one))
                 (rule ({long}/abcdefghijabcdefghijabcdefghij ?l ?n) ({long} ?l name: ?n))"
        );
        let checked = check(&mut Analyzer::new()?, &text);
        assert!(
            checked.refusals().next().is_none(),
            "{text}: {:?}",
            checked.diagnostics
        );
        assert_eq!(derived_in(&checked).len(), 1, "{text}");
        // A label past the room the relation leaves, and an attribute a
        // line names with a character a relation cannot hold.
        for (text, said) in [
            (
                format!(
                    "{list} (rule (list/count ?l abcdefghijabcdefghijabcdefg: ?n) (list ?l name: ?n))"
                ),
                "abcdefghijabcdefghijabcdefg",
            ),
            (
                format!(
                    "(concept (todo ?l name: ?n) (xyz.{cafe}/name of: ?l is: ?n as: :text cardinality: :one))"
                ),
                "lowercase letters",
            ),
        ] {
            let checked = check(&mut Analyzer::new()?, &text);
            assert!(
                checked.refusals().any(|refused| matches!(&refused.refusal,
                    Some(Error::Refused(why)) if why.contains(said) && !why.contains("untagged"))),
                "{text}: {:?}",
                checked.diagnostics
            );
        }
        // A refusal about a derived attribute names it as it is written.
        let checked = check(
            &mut Analyzer::new()?,
            &format!("{list} (rule (list/count-open ?l (count ?n)) (list ?l name: ?n))"),
        );
        let messages: Vec<&str> = checked
            .diagnostics
            .iter()
            .map(|diagnostic| diagnostic.message.as_str())
            .collect();
        assert!(
            messages
                .iter()
                .any(|message| message.contains("list/count-open"))
                && messages.iter().all(|message| !message.contains("dialog.n")),
            "{messages:?}"
        );
        Ok(())
    }

    /// What compile refuses is said the same by `check` and by
    /// `check_forms`: of the form as the author wrote it, never of the
    /// rule it expanded to.
    #[test]
    fn it_quotes_the_authors_form_form_by_form_as_whole() -> Result<(), Error> {
        let list = "(concept (list ?l name: ?n) \
                    (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))";
        for text in [
            format!("{list} (select (list ?l name: ?n) (unless (list/label ?l ?q)))"),
            format!(
                "{list} (rule (list/label ?l ?n) (list ?l name: ?n)) \
                 (select (list ?l name: ?n) (unless (list/label ?l ?q)))"
            ),
        ] {
            let said = |checked: Checked| {
                checked
                    .diagnostics
                    .iter()
                    .map(|said| (said.form.clone(), said.path.clone(), said.message.clone()))
                    .collect::<Vec<_>>()
            };
            let whole = said(check(&mut Analyzer::new()?, &text));
            let each = said(check_forms(&mut Analyzer::new()?, &text));
            assert_eq!(whole, each, "{text}");
            assert!(
                each.iter()
                    .all(|(_, _, message)| !message.contains("(rule (report")),
                "{each:?}"
            );
        }
        Ok(())
    }

    /// A name is a concept or a derived attribute of one, never both:
    /// a document that declares `list/open` a concept and concludes or
    /// reads it as a derived attribute of `list` is refused at the
    /// derived use, naming the declaration, in either order, whole or
    /// form by form, never as "nothing says what" it is.
    #[test]
    fn it_refuses_a_name_that_is_a_concept_and_a_derived_attribute() -> Result<(), Error> {
        let list = "(concept (list ?l name: ?n) \
                    (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))";
        let concept = "(concept (list/open ?l name: ?n) \
                       (xyz.list/name of: ?l is: ?n cardinality: :one))";
        let rule = "(rule (list/open ?l ?n) (list ?l name: ?n))";
        for text in [
            format!("{list} {rule} {concept}"),
            format!("{list} {concept} {rule}"),
            format!("{list} {concept} (select (list/open ?l ?n))"),
        ] {
            for checked in [
                check(&mut Analyzer::new()?, &text),
                check_forms(&mut Analyzer::new()?, &text),
            ] {
                let refused: Vec<_> = checked
                    .refusals()
                    .map(|said| said.message.clone())
                    .collect();
                assert!(
                    !refused.is_empty()
                        && refused.iter().all(|message| {
                            message.contains("(list/open ?l ?n)")
                                && message.contains("derived attribute of the concept `list`")
                                && message.contains("(concept (list/open ?l name: ?n)")
                                && !message.contains("nothing says what")
                        }),
                    "{text}: {refused:?}"
                );
            }
        }
        Ok(())
    }

    /// A derived attribute is what rules conclude, on the default layer:
    /// one concluded or read on a layer, `in: :control`, the only layer
    /// there is (`primitive::LAYERS`), is refused naming the attribute, in a
    /// head, a select, a premise and `unless`, and in the primitive
    /// spelling, which no document writes.
    #[test]
    fn it_refuses_a_derived_attribute_on_a_layer() -> Result<(), Error> {
        let list = "(concept (list ?l name: ?n) \
                    (xyz.list/name of: ?l is: ?n as: :text cardinality: :one)) \
                    (rule (list/label ?l ?n) (list ?l name: ?n))";
        for layer in crate::primitive::LAYERS {
            for form in [
                format!("(rule (list/label of: ?l is: ?n in: :{layer}) (list ?l name: ?n))"),
                format!("(select (list/label of: ?l is: ?n in: :{layer}))"),
                format!("(rule (xyz.out ?l v: ?n) (list/label of: ?l is: ?n in: :{layer}))"),
                format!(
                    "(select (list ?l name: ?n) (list ?l name: ?n) \
                     (unless (list/label of: ?l is: ?q in: :{layer})))"
                ),
                // The primitive spelling is refused as every wrapper is.
                format!("(select (list/label ?l (:{layer} ?n)))"),
            ] {
                let text = format!("{list} {form}");
                let checked = check(&mut Analyzer::new()?, &text);
                let refused: Vec<_> = checked
                    .refusals()
                    .map(|said| said.message.clone())
                    .collect();
                assert!(
                    refused
                        .iter()
                        .any(|message| message.contains("`list/label`")
                            && message.contains(&format!("the {layer} layer"))
                            && message.contains("default layer")),
                    "{form}: {refused:?}"
                );
                assert!(checked.steps.is_empty(), "{form}");
            }
        }
        Ok(())
    }

    /// A line of a concept or a command is a stored attribute, its domain
    /// dotted: one that names a derived attribute, which rules conclude,
    /// is refused at the declaration, naming the line, its entity written
    /// with no label or under `of:`, in a concept or a command, whether the concept it is
    /// derived under is declared in the same document or an earlier one.
    #[test]
    fn it_refuses_a_line_that_names_a_derived_attribute() -> Result<(), Error> {
        let list = "(concept (list ?l name: ?n) \
                    (xyz.list/name of: ?l is: ?n as: :text cardinality: :one)) \
                    (rule (list/label ?l ?n) (list ?l name: ?n))";
        let declarations = [
            "(concept (memo ?m label: ?x) (list/label ?m is: ?x cardinality: :one))",
            "(concept (memo ?m label: ?x) (list/label of: ?m is: ?x cardinality: :one))",
            "(concept (list/go label: ?x) (list/label ?c is: ?x in: :control cardinality: :many))",
            "(concept (list/go label: ?x) \
             (list/label of: ?c is: ?x in: :control cardinality: :many))",
        ];
        for declaration in declarations {
            let together = check(&mut Analyzer::new()?, &format!("{list} {declaration}"));
            let mut analyzer = Analyzer::new()?;
            let earlier = check(&mut analyzer, list);
            assert!(
                earlier.refusals().next().is_none(),
                "{:?}",
                earlier.diagnostics
            );
            let later = check(&mut analyzer, declaration);
            for checked in [together, later] {
                let refused: Vec<_> = checked
                    .refusals()
                    .map(|said| said.message.clone())
                    .collect();
                assert!(
                    refused
                        .iter()
                        .any(|message| message.contains("(list/label ")
                            && message.contains("derived attribute of the concept `list`")
                            && message.contains(
                                "a line of a concept or a command is a stored attribute"
                            )),
                    "{declaration}: {refused:?}"
                );
            }
        }
        Ok(())
    }

    /// A test that shortens site names gives them back when it ends,
    /// however it ends: tests share a thread on wasm, and every later one
    /// would name its sites short and see two meet. A panic is caught
    /// only natively: on wasm it aborts, so this runs natively only, and
    /// the guard it tests is the same code there, held by
    /// `it_refuses_two_sites_named_alike`, which runs on both.
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn it_names_sites_in_full_after_a_test_that_shortened_them_fails() {
        let failed = std::panic::catch_unwind(|| {
            let _short = crate::compile::sites_named_with(0);
            panic!("a test that shortened site names fails");
        });
        assert!(failed.is_err());
        assert_eq!(
            crate::compile::SITE_LENGTH.with(std::cell::Cell::get),
            crate::compile::SITE
        );
    }

    /// A line is a line however it says what it is: by `cardinality:`,
    /// or by `as:`, `in:` or `by:`. One that names a derived
    /// attribute is refused at the declaration in each form, in a concept
    /// and a command, and never at a use that quotes what it expanded to.
    #[test]
    fn it_refuses_a_line_naming_a_derived_attribute_in_every_form() -> Result<(), Error> {
        let a = "(concept (a ?t n: ?n) (xyz.a/n of: ?t is: ?n as: :text cardinality: :one)) \
                 (rule (a/x ?t ?n) (a ?t n: ?n))";
        for declaration in [
            "(concept (k ?t v: ?v) (a/x ?t is: ?v cardinality: :one))",
            "(concept (k ?t v: ?v) (a/x ?t is: ?v as: :text))",
            "(concept (k/go v: ?v) (a/x ?c is: ?v in: :control))",
            "(concept (k/go v: ?v) (a/x ?c is: ?v cardinality: :many in: :control))",
        ] {
            let text = format!("{a} {declaration} (assert (xyz.a x:2 n: \"w\"))");
            let checked = check(&mut Analyzer::new()?, &text);
            let refused: Vec<_> = checked
                .refusals()
                .map(|said| (said.path.clone(), said.message.clone()))
                .collect();
            assert!(
                !refused.is_empty()
                    && refused.iter().all(|(path, message)| {
                        path.first() == Some(&Segment::Item(2))
                            && message.contains("derived attribute of the concept `a`")
                            && message
                                .contains("a line of a concept or a command is a stored attribute")
                    }),
                "{declaration}: {refused:?}"
            );
        }
        // A macro over a read of the derived attribute is not a line, and
        // `of:` with nothing else a line says is the reader's way to
        // write the entity, so `(a/x of: ?t is: ?v)` is that read too.
        let read = check(
            &mut Analyzer::new()?,
            &format!("{a} (concept (k ?t v: ?v) (a/x ?t is: ?v)) (select (k ?t v: ?v))"),
        );
        assert!(read.refusals().next().is_none(), "{:?}", read.diagnostics);
        Ok(())
    }

    /// `in:` names a layer wherever it is written, so on a derived
    /// attribute it is refused as a layer is, in a head and in a read,
    /// with F1's reason, never kept as a field named `in`.
    #[test]
    fn it_refuses_in_on_a_derived_attribute() -> Result<(), Error> {
        let a = "(concept (a ?t n: ?n) (xyz.a/n of: ?t is: ?n as: :text cardinality: :one)) \
                 (rule (a/x ?t ?n) (a ?t n: ?n))";
        for form in [
            "(rule (a/y ?t is: ?n in: :control) (a ?t n: ?n))",
            "(select (a/x ?t is: ?n in: :control))",
            "(rule (xyz.out ?t v: ?n) (a/x ?t is: ?n in: :control))",
        ] {
            let checked = check(&mut Analyzer::new()?, &format!("{a} {form}"));
            let refused: Vec<_> = checked
                .refusals()
                .map(|said| said.message.clone())
                .collect();
            assert!(
                refused
                    .iter()
                    .any(|message| message.contains("a derived attribute, on the control layer")),
                "{form}: {refused:?}"
            );
            assert!(checked.steps.is_empty(), "{form}");
        }
        Ok(())
    }

    /// A refusal that offers the stored attribute to write instead,
    /// `xyz.…`, offers one that is accepted: each example taken out of
    /// the refusal is read as the stored attribute it names, two-term.
    #[test]
    fn it_offers_a_stored_attribute_that_is_one() -> Result<(), Error> {
        let a = "(concept (a ?t n: ?n) (xyz.a/n of: ?t is: ?n as: :text cardinality: :one)) \
                 (concept (nb/blocks ?t n: ?n) (xyz.nb/n of: ?t is: ?n as: :text cardinality: :one)) \
                 (rule (a/x ?t ?n) (a ?t n: ?n)) \
                 (rule (nb/blocks/count ?t ?n) (nb/blocks ?t n: ?n))";
        let offered = |message: &str| -> Option<String> {
            message
                .split('`')
                .find(|part| part.starts_with("xyz.") && !part.contains('…'))
                .map(str::to_string)
        };
        for form in [
            "(select (foo/x ?t ?v))",
            "(select (foo/bar/baz ?t ?v))",
            "(concept (k ?t v: ?v) (a/x ?t is: ?v cardinality: :one))",
            "(concept (k ?t v: ?v) (nb/blocks/count ?t is: ?v cardinality: :one))",
        ] {
            let checked = check(&mut Analyzer::new()?, &format!("{a} {form}"));
            let examples: Vec<String> = checked
                .refusals()
                .filter_map(|said| offered(&said.message))
                .collect();
            assert!(!examples.is_empty(), "{form}: {:?}", checked.diagnostics);
            for example in examples {
                let read = check(
                    &mut Analyzer::new()?,
                    &format!("(select ({example} ?t ?v))"),
                );
                assert!(
                    read.refusals().next().is_none(),
                    "{form} offers `{example}`: {:?}",
                    read.diagnostics
                );
            }
        }
        Ok(())
    }

    /// A field of a concept read is the concept's: a cardinality or a
    /// type the read wraps it in, which the concept says itself, is
    /// refused as every wrapper a document writes is, saying to read the
    /// field as it is, not as a value "written under two cardinalities"
    /// the author never wrote twice.
    #[test]
    fn it_refuses_a_wrapped_field_of_a_concept_read_naming_it() -> Result<(), Error> {
        let a = "(concept (a ?t n: ?n) (xyz.a/n of: ?t is: ?n as: :text cardinality: :one))";
        for form in [
            "(concept (k ?t v: ?v) (a ?t n: (:one (:text ?v))))",
            "(select (a ?t n: (:one ?v)))",
            "(select (a ?t n: (:text ?v)))",
        ] {
            let checked = check(&mut Analyzer::new()?, &format!("{a} {form}"));
            let refused: Vec<_> = checked
                .refusals()
                .map(|said| said.message.clone())
                .collect();
            assert!(
                refused.len() == 1
                    && refused[0].contains("a document writes no wrapper")
                    && refused[0].contains("a concept's lines say")
                    && refused[0].contains("`n: ?v`")
                    && !refused[0].contains("written under two"),
                "{form}: {refused:?}"
            );
        }
        Ok(())
    }

    /// A select whose only premise must not hold is told that its first
    /// part is its head, which says what an answer is and is not read:
    /// what must hold is written again as a premise after it.
    #[test]
    fn it_tells_a_select_its_first_part_is_its_head() -> Result<(), Error> {
        let a = "(concept (a ?t n: ?n) (xyz.a/n of: ?t is: ?n as: :text cardinality: :one))";
        let checked = check(
            &mut Analyzer::new()?,
            &format!("{a} (select (a ?t n: ?n) (unless (a/hidden ?t ?n)))"),
        );
        let refused: Vec<_> = checked
            .refusals()
            .map(|said| said.message.clone())
            .collect();
        assert!(
            refused.len() == 1
                && refused[0].starts_with("(select (a ?t n: ?n) (unless (a/hidden ?t ?n)))")
                && refused[0].contains("first part of a select is its head")
                && refused[0].contains("is not read"),
            "{refused:?}"
        );
        Ok(())
    }

    /// What the compiler refuses is said once per form, whole or form by
    /// form, however many rules the form expands to: a select of a
    /// concept whose lines are in two domains is two rules, and one
    /// refusal.
    #[test]
    fn it_says_a_compile_refusal_once_per_form() -> Result<(), Error> {
        let text = "(concept (b ?t n: ?n m: ?m) \
                    (xyz.b/Name of: ?t is: ?n as: :text cardinality: :one) \
                    (xyz.c/Mm of: ?t is: ?m as: :text cardinality: :one)) \
                    (select (b ?t n: ?n m: ?m))";
        for checked in [
            check(&mut Analyzer::new()?, text),
            check_forms(&mut Analyzer::new()?, text),
        ] {
            let mut paths: Vec<_> = checked.refusals().map(|said| said.path.clone()).collect();
            let all = paths.len();
            paths.dedup();
            assert_eq!(paths.len(), all, "{:?}", checked.diagnostics);
        }
        Ok(())
    }

    /// A field a declaration declares is a declared name: a concept's, a
    /// command's and a query's head labels and a derived attribute's head
    /// labels are spelled as a stored name's part is, and one that is not
    /// is refused naming the label, before anything is compiled. A select
    /// head's labels, a value's map keys and variables are free.
    #[test]
    fn it_refuses_a_declared_field_spelled_as_no_stored_name_is() -> Result<(), Error> {
        let a = "(concept (a ?t n: ?n) (xyz.a/n of: ?t is: ?n as: :text cardinality: :one))";
        let line = "(xyz.a/n of: ?t is: ?n as: :text cardinality: :one)";
        for (form, says) in [
            (
                format!("(concept (k ?t Name: ?n) {line})"),
                "`Name` starts with `N`",
            ),
            (
                format!("(concept (k ?t na_me: ?n) {line})"),
                "`na_me` holds `_`",
            ),
            (
                "(concept (k/go Name: ?n) \
                 (xyz.k.go/n of: ?c is: ?n as: :text in: :control cardinality: :many))"
                    .to_string(),
                "`Name` starts with `N`",
            ),
            (
                "(query (a/q ?t is: ?v Big: ?n) (a ?t n: ?n) (is ?v ?n))".to_string(),
                "`Big` starts with `B`",
            ),
            (
                "(rule (a/x ?t Big: ?n) (a ?t n: ?n))".to_string(),
                "`Big` starts with `B`",
            ),
            (
                "(rule (a/x ?t caf\u{e9}: ?n) (a ?t n: ?n))".to_string(),
                "`caf\u{e9}` holds `\u{e9}`",
            ),
        ] {
            let checked = check(&mut Analyzer::new()?, &format!("{a} {form}"));
            let refused: Vec<_> = checked
                .refusals()
                .map(|said| said.message.clone())
                .collect();
            assert!(
                refused.len() == 1
                    && refused[0].contains(says)
                    && refused[0].contains("a lowercase letter")
                    && !refused[0].contains("invalid relation"),
                "{form}: {refused:?}"
            );
        }
        for free in [
            "(select (xyz.out ?t Big: ?n) (a ?t n: ?n))",
            "(query (a/q ?t is: ?v) (a ?t n: ?n) (is ?v {:Big ?n :caf\u{e9} ?n})) (select (a/q ?t is: ?v))",
            "(select (a ?Big n: ?caf\u{e9}))",
        ] {
            let checked = check(&mut Analyzer::new()?, &format!("{a} {free}"));
            assert!(
                checked.refusals().next().is_none(),
                "{free}: {:?}",
                checked.diagnostics
            );
        }
        Ok(())
    }

    /// A write written again in a document is a step again, in its
    /// place: a document is a sequence, and the second assert of a claim
    /// after its retract is what leaves the claim. A rule or a concept
    /// written again is one step, as installing it again changes
    /// nothing.
    #[test]
    fn it_keeps_a_write_written_again_and_installs_a_rule_once() -> Result<(), Error> {
        for (text, steps) in [
            (
                r#"(assert (xyz.r x:1 o: "A")) (retract (xyz.r x:1 o: "A")) (assert (xyz.r x:1 o: "A"))"#,
                3,
            ),
            (
                "(assert (xyz.n x:1 v: (+ ?v : 1)) (xyz.n x:1 v: ?v))
                 (assert (xyz.n x:1 v: (+ ?v : 1)) (xyz.n x:1 v: ?v))",
                2,
            ),
            (
                r#"(rule (assert (xyz.m ?t tag: "seen")) (xyz.n ?t v: 3))
                   (rule (assert (xyz.m ?t tag: "seen")) (xyz.n ?t v: 3))"#,
                1,
            ),
        ] {
            let checked = check(&mut Analyzer::new()?, text);
            assert!(
                checked.refusals().next().is_none(),
                "{text}: {:?}",
                checked.diagnostics
            );
            assert_eq!(checked.steps.len(), steps, "{text}: {:?}", checked.steps);
        }
        Ok(())
    }

    /// A read of a derived attribute's label that no conclusion of it
    /// this analyzer knows writes is a warning, never a refusal, said
    /// once the whole document is expanded: in the document that
    /// concludes it, and in a later one. With no conclusion known, which
    /// may live on the branch, nothing is said, so a read before the
    /// document that concludes it is accepted and stays so.
    #[test]
    fn it_warns_of_a_derived_label_no_known_rule_concludes() -> Result<(), Error> {
        let list = r#"(concept (list ?l name: ?n)
              (xyz.list/name of: ?l is: ?n as: :text cardinality: :one))"#;
        let warned = |checked: &Checked| -> Vec<String> {
            checked
                .diagnostics
                .iter()
                .filter(|said| said.refusal.is_none() && said.message.contains("list/label"))
                .map(|said| said.message.clone())
                .collect()
        };
        // Read before anything concludes it, and then concluded.
        let mut analyzer = Analyzer::new()?;
        let early = check(
            &mut analyzer,
            &format!("{list} (select (list/label ?l wat: ?q))"),
        );
        assert!(early.refusals().next().is_none(), "{:?}", early.diagnostics);
        assert!(warned(&early).is_empty(), "{:?}", warned(&early));
        let rule = "(rule (list/label ?l is: ?n other: ?n) (list ?l name: ?n))";
        let concluded = check(&mut analyzer, rule);
        assert!(
            concluded.refusals().next().is_none(),
            "{:?}",
            concluded.diagnostics
        );
        // A later document, and the same one.
        for text in [
            "(select (list/label ?l wat: ?q))".to_string(),
            format!("{list} {rule} (select (list/label ?l wat: ?q))"),
        ] {
            let mut fresh = Analyzer::new()?;
            let analyzer = if text.contains("(rule") {
                &mut fresh
            } else {
                &mut analyzer
            };
            let checked = check(analyzer, &text);
            assert!(
                checked.refusals().next().is_none(),
                "{:?}",
                checked.diagnostics
            );
            let said = warned(&checked);
            assert!(
                said.len() == 1 && said[0].contains("`wat:`") && said[0].contains("`other:`"),
                "{text}: {said:?}"
            );
        }
        let read = check(&mut analyzer, "(select (list/label ?l other: ?q))");
        assert!(warned(&read).is_empty(), "{:?}", warned(&read));
        Ok(())
    }
}
