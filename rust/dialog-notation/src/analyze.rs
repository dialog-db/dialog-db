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

use dialog_corpus::Entity;

use crate::definition::{Binding, Defined, Definition, Namespaces};
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
        };
        if analysis.refusals().next().is_some() {
            return Analysis {
                analyzed: Vec::new(),
                origin: Vec::new(),
                defined: Vec::new(),
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
        }
    }

    /// The same analyzer with `mine` as the local namespace: the one it
    /// binds what it defines in, and the one `@petname/…` walks from. A
    /// driver gives the authority a branch is opened under, the profile
    /// its session holds; an analyzer given none binds in
    /// [`MINE`](crate::definition::MINE).
    pub fn with_namespace(self, mine: Entity) -> Self {
        Analyzer {
            expander: self.expander.with_namespace(mine),
        }
    }

    /// An analyzer that knows the core forms and `definitions`, each
    /// under the names `bindings` give it, as a branch keeps them: a
    /// fresh analyzer on a branch answers as the one that defined them.
    pub fn load(
        mine: Entity,
        definitions: impl IntoIterator<Item = Definition>,
        bindings: impl IntoIterator<Item = Binding>,
    ) -> Result<Self, Error> {
        let mut analyzer = Analyzer::new()?.with_namespace(mine);
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

    /// The revision the names of what is defined next are resolved at,
    /// which each source map then holds. None unless the caller says.
    pub fn resolve_at(&mut self, revision: Option<String>) {
        self.expander.resolve_at(revision);
    }

    /// Every namespace's bindings this analyzer knows, and which is
    /// mine.
    pub fn namespaces(&self) -> &Namespaces {
        self.expander.namespaces()
    }

    /// Every definition this analyzer knows, by its entity.
    pub fn definitions(&self) -> impl Iterator<Item = &Definition> {
        self.expander.definitions().values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::definition::{Binding, name_space};
    use crate::written::{Point, Segment, Span};
    use crate::{parse, parse_written};
    use std::str::FromStr;

    fn entity(uri: &str) -> Entity {
        Entity::from_str(uri).unwrap()
    }

    /// An analyzer whose namespace is `mine`, given what `text` defines.
    fn defining(mine: &str, text: &str) -> Analyzer {
        let mut analyzer = Analyzer::new().unwrap().with_namespace(entity(mine));
        let analysis = analyzer.analyze(&parse(text).unwrap());
        assert!(
            analysis.diagnostics.is_empty(),
            "{:?}",
            analysis.diagnostics
        );
        analyzer
    }

    /// `other`'s definitions and bindings taken in by `analyzer`, as a
    /// branch both write to would give them.
    fn taking(analyzer: &mut Analyzer, other: &Analyzer) {
        analyzer
            .take(
                other.definitions().cloned().collect::<Vec<_>>(),
                other.namespaces().bindings().collect::<Vec<_>>(),
            )
            .unwrap();
    }

    const MINE: &str = "did:key:mine";
    const CDATA: &str = "did:key:cdata";
    const OURS: &str = "(concept (todo ?t title: ?x) (xyz.todo/title of: ?t is: ?x as: :text))";
    const THEIRS: &str = "(concept (todo ?t title: ?x) (abc.task/name of: ?t is: ?x as: :text))";

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

    /// `@cdata/todo` is two lookups: `cdata` in mine, bound to cdata's
    /// namespace, and `todo` in that. It means cdata's `todo` alone.
    #[test]
    fn it_resolves_a_petname_through_two_bindings() {
        let mut mine = defining(MINE, OURS);
        let theirs = defining(CDATA, THEIRS);
        taking(&mut mine, &theirs);
        mine.take(
            Vec::new(),
            [Binding {
                namespace: entity(MINE),
                key: "cdata".into(),
                referent: entity(CDATA),
            }],
        )
        .unwrap();
        let read = asked(&mut mine, "(select (@cdata/todo ?t title: ?x))").unwrap();
        assert_eq!(read.len(), 1, "{read:?}");
        assert!(read[0].contains("abc.task"), "{read:?}");
        assert!(!read[0].contains("xyz.todo"), "{read:?}");
        // A petname bound to nothing is no namespace.
        assert!(matches!(
            asked(&mut mine, "(select (@nobody/todo ?t title: ?x))"),
            Err(Error::Unknown(_))
        ));
    }

    /// `todo` alone is every namespace's. Two definitions of one
    /// signature, in two namespaces, are refused, each listed by its
    /// namespace and its entity, and nothing of the document is kept.
    #[test]
    fn it_refuses_a_name_two_namespaces_define_under_one_signature() {
        let mut mine = defining(MINE, OURS);
        let theirs = defining(CDATA, THEIRS);
        taking(&mut mine, &theirs);
        let before = mine.namespaces().clone();
        let analysis = mine.analyze(
            &parse(
                "(concept (card ?t is: ?x) (xyz.card/is of: ?t is: ?x))
             (select (todo ?t title: ?x))",
            )
            .unwrap(),
        );
        let refusal = analysis
            .refusals()
            .next()
            .and_then(|refused| refused.refusal.clone());
        let Some(Error::Several(several)) = &refusal else {
            panic!("refused as several: {refusal:?}");
        };
        assert!(matches!(&refusal, Some(Error::Several(_))));
        assert_eq!(several.name, "todo");
        let namespaces: Vec<&str> = several
            .candidates
            .iter()
            .map(|(namespace, _)| namespace.as_str())
            .collect();
        assert_eq!(namespaces, vec![CDATA, MINE]);
        assert!(
            several
                .candidates
                .iter()
                .all(|(_, definition)| definition.starts_with("definition:"))
        );
        assert!(analysis.defined.is_empty() && analysis.analyzed.is_empty());
        assert_eq!(
            mine.namespaces(),
            &before,
            "nothing of the document is kept"
        );
    }

    /// Where the signatures differ, the labels written say which: two
    /// namespaces may each define `todo`, one with `title:` and one with
    /// `name:`.
    #[test]
    fn it_resolves_a_name_by_the_labels_among_namespaces() {
        let mut mine = defining(MINE, OURS);
        let theirs = defining(
            CDATA,
            "(concept (todo ?t name: ?x) (abc.task/name of: ?t is: ?x as: :text))",
        );
        taking(&mut mine, &theirs);
        let title = asked(&mut mine, "(select (todo ?t title: ?x))").unwrap();
        assert!(title[0].contains("xyz.todo"), "{title:?}");
        let name = asked(&mut mine, "(select (todo ?t name: ?x))").unwrap();
        assert!(name[0].contains("abc.task"), "{name:?}");
    }

    /// `todo#3fa9` is the definition under `todo` whose hash starts so,
    /// whichever namespace binds it.
    #[test]
    fn it_picks_a_definition_by_a_prefix_of_its_hash() {
        let mut mine = defining(MINE, OURS);
        let theirs = defining(CDATA, THEIRS);
        taking(&mut mine, &theirs);
        let named = name_space(&entity(CDATA), "todo").unwrap();
        let definition = theirs
            .namespaces()
            .get(&named, "concept (title:)")
            .cloned()
            .expect("bound");
        let hash = crate::definition::hash_of(&definition);
        let read = asked(
            &mut mine,
            &format!("(select (todo#{} ?t title: ?x))", &hash[..8]),
        )
        .unwrap();
        assert!(read[0].contains("abc.task"), "{read:?}");
        assert!(matches!(
            asked(&mut mine, "(select (todo#zzzz ?t title: ?x))"),
            Err(Error::Unknown(_))
        ));
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
