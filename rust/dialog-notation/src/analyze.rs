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

use crate::expand::{Diagnostic, Expander, Severity};
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
        };
        if analysis.refusals().next().is_some() {
            return Analysis {
                analyzed: Vec::new(),
                origin: Vec::new(),
                ..analysis
            };
        }
        self.expander = trial;
        analysis
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::written::{Point, Segment, Span};
    use crate::{parse, parse_written};

    /// A warning says the way to the form it is about, which the
    /// mapping the parser gave turns into a place in the text. An
    /// analyzed tree says which tree it came of.
    #[dialog_common::test]
    fn it_says_where_the_form_a_warning_is_about_is() -> Result<(), Error> {
        let text = "(concept (todo ?t text: ?x)
  (xyz.todo ?t text: (:last (:text ?x))))

(rule (ad (todo ?t text: ?x))
  (xyz.draft ?t text: ?x))
";
        let (trees, written) = parse_written(text)?;
        let analysis = Analyzer::new()?.analyze(&trees);
        let [warning] = analysis.diagnostics.as_slice() else {
            panic!("one warning: {:?}", analysis.diagnostics);
        };
        assert_eq!(warning.message, "no rule reads this command");
        assert_eq!(warning.path, vec![Segment::Item(1), Segment::Item(0)]);
        assert_eq!(
            written.span(&warning.path),
            Some(Span {
                start: Point { line: 3, column: 6 },
                end: Point {
                    line: 3,
                    column: 29
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
             (query (task ?t title: ?x))",
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
        assert_eq!(refused.form, parse("(nonesuch ?x)")?[0]);
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

(rule (all (assert! (xyz.todo ?t text: ?x))
           (retract! (xyz.draft ?t text: ?x)))
  (xyz.draft ?t text: ?x))

(query (todo ?t text: ?x))

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
        let after = analyzer.analyze(&parse("(query (todo ?t text: ?x))")?);
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
            .replace("(rule (all (assert! (xyz.todo ?t text: ?x))\n           (retract! (xyz.draft ?t text: ?x)))", "(rule (assert! (xyz.todo ?t text: ?x))")
            .replace("\n  (frobnicate ?t))", ")");
        let analysis = analyzer.analyze(&parse(&mended)?);
        assert!(
            analysis.diagnostics.is_empty(),
            "{:?}",
            analysis.diagnostics
        );
        assert_eq!(analysis.origin, vec![1, 2, 3]);
        let after = analyzer.analyze(&parse("(query (todo ?t text: ?x))")?);
        assert!(after.diagnostics.is_empty());
        assert_eq!(after.analyzed.len(), 1);
        Ok(())
    }
}
