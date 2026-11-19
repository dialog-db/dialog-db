//! Analysis: the second phase, from syntax trees to analyzed trees.
//!
//! The analyzer is given the trees a document parsed to. A rule about
//! syntax among them is kept as a definition; every other tree is
//! rewritten by the definitions in force until it is a rule of the
//! kernel. What comes back is those rules, the analyzed trees, and
//! what the definitions said about the document.
//!
//! The definitions are held here, in memory. They are to be read from a
//! replica, which is why this phase and not the others is the one that
//! reads a database.

use crate::definition::{Binding, Defined, Definition, Names};
use crate::diagnostic::{Diagnostic, Severity};
use crate::expand::Expander;
use crate::{Error, Syntax};

/// What analysis made of a document.
#[derive(Clone, Debug, PartialEq)]
pub struct Analysis {
    /// The analyzed trees: every one a rule of the kernel, in the order
    /// their forms were written. A form several definitions take is a
    /// rule a definition. None when any tree of the document is
    /// refused.
    pub analyzed: Vec<Syntax>,
    /// For each analyzed tree, which tree of the document it came of.
    pub origin: Vec<usize>,
    /// Everything there is to say about the document, each with the
    /// way to the form it is about: every refusal, in the order of
    /// their trees, and then what the definitions said without
    /// refusing.
    pub diagnostics: Vec<Diagnostic>,
    /// The definitions the document made, each compiled, with the name
    /// and signature it is bound under in mine and its source map: what
    /// is written to a branch for it. None when any tree is refused.
    pub defined: Vec<Defined>,
    /// The bindings the document took away, `(unname todo)`, each with
    /// what it was bound to.
    pub unnamed: Vec<Binding>,
}

impl Analysis {
    /// The refusals: the diagnostics that are errors.
    pub fn refusals(&self) -> impl Iterator<Item = &Diagnostic> {
        self.diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.severity() == Severity::Error)
    }
}

/// The second phase, with the definitions it has been given.
#[derive(Clone, Debug)]
pub struct Analyzer {
    expander: Expander,
}

impl Analyzer {
    /// An analyzer that knows the core forms.
    pub fn new() -> Result<Self, Error> {
        Ok(Analyzer {
            expander: Expander::new()?,
        })
    }

    /// Analyze `trees`: keep the definitions among them, and give every
    /// other analyzed.
    ///
    /// Analysis does not stop at a refusal. It goes on to the end of
    /// the document and says every one, so an editor can show them all.
    /// A document with a refusal in it is refused whole: none of its
    /// trees comes back analyzed and none of its definitions is kept,
    /// so the same document can be analyzed again once it is mended.
    pub fn analyze(&mut self, trees: &[Syntax]) -> Analysis {
        let mut trial = self.expander.clone();
        let (analyzed, diagnostics) = trial.check(trees.to_vec());
        let analysis = Analysis {
            analyzed,
            origin: trial.origin().to_vec(),
            diagnostics,
            defined: trial.defined().to_vec(),
            unnamed: trial.unnamed().to_vec(),
        };
        if analysis.refusals().next().is_some() {
            return Analysis {
                analyzed: Vec::new(),
                origin: Vec::new(),
                defined: Vec::new(),
                unnamed: Vec::new(),
                ..analysis
            };
        }
        self.expander = trial;
        analysis
    }

    /// Analyze `trees` form by form. A form that is refused gives no
    /// analyzed tree, and every other form gives its own; each refusal
    /// says the way to the form it is about. What the forms define is
    /// kept, so the analyzer is not left as it was when a form is
    /// refused, as it is by [`analyze`](Self::analyze).
    pub fn analyze_forms(&mut self, trees: &[Syntax]) -> Analysis {
        let (analyzed, diagnostics) = self.expander.check(trees.to_vec());
        Analysis {
            analyzed,
            origin: self.expander.origin().to_vec(),
            diagnostics,
            defined: self.expander.defined().to_vec(),
            unnamed: self.expander.unnamed().to_vec(),
        }
    }

    /// An analyzer that knows the core forms and `definitions`, each
    /// under the names `bindings` give it, as a branch keeps them.
    pub fn load(
        definitions: impl IntoIterator<Item = Definition>,
        bindings: impl IntoIterator<Item = Binding>,
    ) -> Result<Self, Error> {
        let mut analyzer = Analyzer::new()?;
        analyzer.expander.load(definitions, bindings)?;
        Ok(analyzer)
    }

    /// Take in more definitions and bindings, as [`load`](Self::load)
    /// does: what a pull brought.
    pub fn take(
        &mut self,
        definitions: impl IntoIterator<Item = Definition>,
        bindings: impl IntoIterator<Item = Binding>,
    ) -> Result<(), Error> {
        self.expander.load(definitions, bindings)
    }

    /// What resolving the names of the last document asked for that has
    /// not been answered: the bindings and definitions a driver reads
    /// next, from wherever they are kept, before analyzing it again.
    pub fn wanted(&self) -> Vec<crate::definition::Want> {
        self.expander.wanted()
    }

    /// Ask for every name `trees` write, before they are analyzed: what
    /// a driver reads in its first round, so that a name that stands
    /// behind another in a tree is not found a round later.
    pub fn ask_for(&self, trees: &[Syntax]) {
        fn walk(tree: &Syntax, names: &mut Vec<String>) {
            match tree {
                Syntax::Reference(name) => names.push(name.clone()),
                Syntax::Application(operator, operand) => {
                    walk(operator, names);
                    operand.items.iter().for_each(|item| walk(item, names));
                    operand.terms.values().for_each(|term| walk(term, names));
                }
                Syntax::Map(entries) => entries.iter().for_each(|(key, value)| {
                    walk(key, names);
                    walk(value, names);
                }),
                Syntax::Tuple(items) => items.iter().for_each(|item| walk(item, names)),
                _ => {}
            }
        }
        let mut names = Vec::new();
        trees.iter().for_each(|tree| walk(tree, &mut names));
        names.sort();
        names.dedup();
        for name in names {
            let _ = self.expander.rows(&name);
        }
    }

    /// Take in what a driver read for `wants`, found or not.
    pub fn answer(
        &mut self,
        wants: Vec<crate::definition::Want>,
        definitions: Vec<Definition>,
        bindings: Vec<Binding>,
    ) -> Result<(), Error> {
        self.expander.answer(wants, definitions, bindings)
    }

    /// Forget every binding read, so they are read again: a name bound
    /// anew since is seen. Definitions are kept, each its own content.
    pub fn forget(&mut self) {
        self.expander.forget();
    }

    /// The revision the names of what is defined next are resolved at,
    /// which each source map then holds. None unless the caller says.
    pub fn resolve_at(&mut self, revision: Option<String>) {
        self.expander.resolve_at(revision);
    }

    /// The space's bindings this analyzer has read.
    pub fn names(&self) -> &Names {
        self.expander.names()
    }

    /// Every definition this analyzer knows, by its entity.
    pub fn definitions(&self) -> impl Iterator<Item = &Definition> {
        self.expander.definitions().values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::written::{Point, Segment, Span};
    use crate::{parse, parse_written};

    /// An analyzer given what `text` defines.
    fn defining(text: &str) -> Analyzer {
        let mut analyzer = Analyzer::new().unwrap();
        let analysis = analyzer.analyze(&parse(text).unwrap());
        assert!(
            analysis.diagnostics.is_empty(),
            "{:?}",
            analysis.diagnostics
        );
        analyzer
    }

    fn asked(analyzer: &mut Analyzer, text: &str) -> Result<Vec<String>, Error> {
        let analysis = analyzer.analyze(&parse(text)?);
        match analysis
            .refusals()
            .next()
            .and_then(|refused| refused.refusal.clone())
        {
            Some(error) => Err(error),
            None => Ok(analysis.analyzed.iter().map(Syntax::to_string).collect()),
        }
    }

    const TODO: &str = "(concept (todo ?t title: ?x) (xyz.todo/title of: ?t is: ?x as: :text))";
    const CDATA: &str =
        "(concept (cdata/todo ?t title: ?x) (abc.task/name of: ?t is: ?x as: :text))";

    /// A name is the space's, and a path is a longer name: `cdata/todo`
    /// and `todo` are two names, neither shadowing the other.
    #[test]
    fn it_resolves_a_path_and_a_shorter_name_apart() {
        let mut analyzer = defining(&format!("{TODO} {CDATA}"));
        let bare = asked(&mut analyzer, "(select (todo ?t title: ?x))").unwrap();
        assert!(
            bare[0].contains("xyz.todo") && !bare[0].contains("abc.task"),
            "{bare:?}"
        );
        let path = asked(&mut analyzer, "(select (cdata/todo ?t title: ?x))").unwrap();
        assert!(
            path[0].contains("abc.task") && !path[0].contains("xyz.todo"),
            "{path:?}"
        );
    }

    /// Two signatures of one name are two keys: the labels written say
    /// which.
    #[test]
    fn it_resolves_an_overloaded_name_by_the_labels_written() {
        let mut analyzer = defining(&format!(
            "{TODO} (concept (todo ?t name: ?x) (abc.task/name of: ?t is: ?x as: :text))"
        ));
        let title = asked(&mut analyzer, "(select (todo ?t title: ?x))").unwrap();
        assert!(title[0].contains("xyz.todo"), "{title:?}");
        let name = asked(&mut analyzer, "(select (todo ?t name: ?x))").unwrap();
        assert!(name[0].contains("abc.task"), "{name:?}");
        let rows = analyzer.names().named("todo");
        assert_eq!(rows.len(), 2, "{rows:?}");
    }

    /// A name bound to nothing, where another name ends as it does, is
    /// refused naming it, and nothing of the document is kept.
    #[test]
    fn it_refuses_a_name_bound_to_nothing_and_says_which_ends_so() {
        let mut analyzer = defining(CDATA);
        analyzer
            .expander
            .answer(
                vec![
                    crate::definition::Want::Name("todo".into()),
                    crate::definition::Want::All,
                ],
                Vec::new(),
                Vec::new(),
            )
            .unwrap();
        let before = analyzer.names().clone();
        let analysis = analyzer.analyze(
            &parse(
                "(concept (note ?n text: ?x) (xyz.note/text of: ?n is: ?x))
                 (select (todo ?t title: ?x))",
            )
            .unwrap(),
        );
        let refusal = analysis
            .refusals()
            .next()
            .and_then(|refused| refused.refusal.clone());
        assert!(matches!(&refusal, Some(Error::Unbound(_))), "{refusal:?}");
        assert_eq!(
            refusal.unwrap().to_string(),
            "todo is bound to nothing: cdata/todo (concept (title:)) is"
        );
        assert!(analysis.defined.is_empty() && analysis.analyzed.is_empty());
        assert_eq!(analyzer.names(), &before, "nothing of the document is kept");
    }

    /// A name whose signatures the labels written fit none of is refused,
    /// with the signatures it has and another path whose fits.
    #[test]
    fn it_refuses_a_name_whose_signatures_the_labels_do_not_fit() {
        let mut analyzer = defining(&format!(
            "{TODO} (concept (cdata/todo ?t name: ?x) (abc.task/name of: ?t is: ?x as: :text))"
        ));
        analyzer
            .expander
            .answer(vec![crate::definition::Want::All], Vec::new(), Vec::new())
            .unwrap();
        let refused = asked(&mut analyzer, "(select (todo ?t name: ?x))");
        assert!(matches!(&refused, Err(Error::Unfitting(_))), "{refused:?}");
        let Err(error @ Error::Unfitting(unfitting)) = &refused else {
            unreachable!()
        };
        assert_eq!(unfitting.signatures, vec!["concept (title:)".to_string()]);
        assert_eq!(
            error.to_string(),
            "todo takes concept (title:), not name:: cdata/todo (concept (name:)) does"
        );
    }

    /// A name nothing binds, with nothing ending as it does, is unknown.
    #[test]
    fn it_refuses_a_name_nothing_holds_as_unknown() {
        let mut analyzer = defining(TODO);
        analyzer
            .expander
            .answer(vec![crate::definition::Want::All], Vec::new(), Vec::new())
            .unwrap();
        assert!(matches!(
            asked(&mut analyzer, "(select (nonesuch ?t title: ?x))"),
            Err(Error::Unknown(_))
        ));
    }

    /// `todo#3fa9` is the definition under `todo` whose hash starts so;
    /// one no binding holds any more is reached by its hash too.
    #[test]
    fn it_picks_a_definition_by_a_prefix_of_its_hash() {
        let mut analyzer = defining(TODO);
        let other = "(concept (todo ?t title: ?x) (abc.task/name of: ?t is: ?x as: :text))";
        let replaced = defining(other);
        let theirs = replaced.names().named("todo")[0].definition.clone();
        // Ours is bound; theirs is kept and bound to nothing.
        analyzer
            .take(
                replaced.definitions().cloned().collect::<Vec<_>>(),
                Vec::new(),
            )
            .unwrap();
        let hash = crate::definition::hash_of(&theirs);
        let read = asked(
            &mut analyzer,
            &format!("(select (todo#{} ?t title: ?x))", &hash[..8]),
        )
        .unwrap();
        assert!(read[0].contains("abc.task"), "{read:?}");
        let ours = asked(&mut analyzer, "(select (todo ?t title: ?x))").unwrap();
        assert!(ours[0].contains("xyz.todo"), "{ours:?}");
        assert!(matches!(
            asked(&mut analyzer, "(select (todo#zzzz ?t title: ?x))"),
            Err(Error::Unknown(_))
        ));
    }

    /// What may be meant while typing: every name under a prefix, each
    /// signature with its definition.
    #[test]
    fn it_suggests_the_names_under_a_prefix() {
        let analyzer = defining(&format!(
            "{TODO} {CDATA} (concept (todo-list ?l name: ?x) (xyz.list/name of: ?l is: ?x))"
        ));
        let said: Vec<String> = analyzer
            .names()
            .suggest("cdata/")
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(said, vec!["cdata/todo (concept (title:))"]);
        let todo: Vec<String> = analyzer
            .names()
            .suggest("todo")
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            todo,
            vec!["todo (concept (title:))", "todo-list (concept (name:))"]
        );
    }

    /// A warning says the way to the form it is about, which the
    /// mapping the parser gave turns into a place in the text. An
    /// analyzed tree says which tree it came of.
    #[dialog_common::test]
    fn it_says_where_the_form_a_warning_is_about_is() -> Result<(), Error> {
        let text = "(concept (todo/add text: ?x)
  (xyz.todo.add/text of: ?c is: ?x in: :control))

(rule (assert (todo/add text: ?x))
  (xyz.draft ?t text: ?x))
";
        let (trees, written) = parse_written(text)?;
        let analysis = Analyzer::new()?.analyze(&trees);
        let [warning] = analysis.diagnostics.as_slice() else {
            panic!("one warning: {:?}", analysis.diagnostics);
        };
        assert_eq!(warning.message, "no rule reads this command");
        assert_eq!(
            warning.path,
            vec![Segment::Item(1), Segment::Item(0), Segment::Item(0)]
        );
        assert_eq!(
            written.span(&warning.path),
            Some(Span {
                start: Point {
                    line: 3,
                    column: 14
                },
                end: Point {
                    line: 3,
                    column: 33
                },
            })
        );
        // The concept is kept as a definition, so the one analyzed tree
        // is the rule's.
        assert_eq!(analysis.origin, vec![1]);
        Ok(())
    }

    #[dialog_common::test]
    fn it_gives_an_analyzed_tree_a_form_and_keeps_a_definition() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let analysis = analyzer.analyze(&parse(
            "(concept (task ?t title: ?x)
               (xyz.todo ?t text: ?x))
             (select (task ?t title: ?x))",
        )?);
        assert!(analysis.diagnostics.is_empty());
        assert_eq!(
            analysis.analyzed,
            parse("(rule (report (xyz.todo ?t text: ?x)) (xyz.todo ?t text: ?x))")?
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_says_what_nothing_defines_and_where() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let analysis = analyzer.analyze(&parse("(rule (report (xyz.a ?x n: ?n)) (nonesuch ?x))")?);
        let [refused] = analysis.diagnostics.as_slice() else {
            panic!("one refusal: {:?}", analysis.diagnostics);
        };
        assert_eq!(refused.severity(), Severity::Error);
        assert!(matches!(&refused.refusal, Some(Error::Unknown(_))));
        assert_eq!(refused.path, vec![Segment::Item(0), Segment::Item(1)]);
        assert_eq!(refused.form.as_ref(), parse("(nonesuch ?x)")?.first());
        assert!(analysis.analyzed.is_empty());
        Ok(())
    }

    /// Analysis goes on past a refusal and says every one, each with
    /// where it is. A document with a refusal in it is refused whole:
    /// nothing of it comes back analyzed and no definition of it is
    /// kept, so it can be analyzed again once it is mended.
    #[dialog_common::test]
    fn it_says_every_refusal_of_a_document_and_keeps_nothing_of_it() -> Result<(), Error> {
        let broken = "(concept (todo ?t text: ?x)
  (xyz.todo ?t text: (:last (:text ?x))))

(rule (all (assert (xyz.todo ?t text: ?x))
           (retract (xyz.draft ?t text: ?x)))
  (xyz.draft ?t text: ?x))

(select (todo ?t text: ?x))

(rule (xyz.seen ?t text: ?x)
  (todo ?t text: ?x)
  (frobnicate ?t))
";
        let mut analyzer = Analyzer::new()?;
        let (trees, written) = parse_written(broken)?;
        let analysis = analyzer.analyze(&trees);
        let refusals: Vec<&Diagnostic> = analysis.refusals().collect();
        let [several, unknown] = refusals.as_slice() else {
            panic!("two refusals: {:?}", analysis.diagnostics);
        };
        assert!(
            matches!(&several.refusal, Some(Error::Refused(why)) if why.contains("a rule for each"))
        );
        assert_eq!(several.path, vec![Segment::Item(1), Segment::Item(0)]);
        assert_eq!(
            written.span(&several.path).map(|span| span.start),
            Some(Point { line: 3, column: 6 })
        );
        assert!(matches!(&unknown.refusal, Some(Error::Unknown(_))));
        assert_eq!(unknown.path, vec![Segment::Item(3), Segment::Item(2)]);
        assert_eq!(
            written.span(&unknown.path),
            Some(Span {
                start: Point {
                    line: 11,
                    column: 2
                },
                end: Point {
                    line: 11,
                    column: 17
                },
            })
        );
        // The query in between is fine, and still nothing comes back.
        assert!(analysis.analyzed.is_empty());
        assert!(analysis.origin.is_empty());

        // The concept the refused document defined was not kept.
        let after = analyzer.analyze(&parse("(select (todo ?t text: ?x))")?);
        assert!(matches!(
            after
                .refusals()
                .next()
                .and_then(|refused| refused.refusal.as_ref()),
            Some(Error::Unknown(_))
        ));

        // Mended, the same document is analyzed, and its definition
        // kept.
        let mended = broken
            .replace("(rule (all (assert (xyz.todo ?t text: ?x))\n           (retract (xyz.draft ?t text: ?x)))", "(rule (assert (xyz.todo ?t text: ?x))")
            .replace("\n  (frobnicate ?t))", ")");
        let analysis = analyzer.analyze(&parse(&mended)?);
        assert!(
            analysis.diagnostics.is_empty(),
            "{:?}",
            analysis.diagnostics
        );
        assert_eq!(analysis.origin, vec![1, 2, 3]);
        let after = analyzer.analyze(&parse("(select (todo ?t text: ?x))")?);
        assert!(after.diagnostics.is_empty());
        assert_eq!(after.analyzed.len(), 1);
        Ok(())
    }
}
