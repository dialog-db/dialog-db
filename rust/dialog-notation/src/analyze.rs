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

use crate::expand::{Diagnostic, Expander};
use crate::{Error, Syntax};

/// What analysis made of a document.
#[derive(Clone, Debug, PartialEq)]
pub struct Analysis {
    /// The analyzed trees: every one a rule of the kernel, in the order
    /// their forms were written. A form several definitions take is a
    /// rule a definition.
    pub analyzed: Vec<Syntax>,
    /// For each analyzed tree, which tree of the document it came of.
    pub origin: Vec<usize>,
    /// What the definitions said about the document, each with the way
    /// to the form it is about.
    pub diagnostics: Vec<Diagnostic>,
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
    pub fn analyze(&mut self, trees: &[Syntax]) -> Result<Analysis, Error> {
        let (analyzed, diagnostics) = self.expander.expand(trees.to_vec())?;
        Ok(Analysis {
            analyzed,
            origin: self.expander.origin().to_vec(),
            diagnostics,
        })
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
        let analysis = Analyzer::new()?.analyze(&trees)?;
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
        )?)?;
        assert!(analysis.diagnostics.is_empty());
        assert_eq!(
            analysis.analyzed,
            parse("(rule (report (xyz.todo ?t text: ?x)) (xyz.todo ?t text: ?x))")?
        );
        Ok(())
    }

    #[dialog_common::test]
    fn it_says_what_nothing_defines() -> Result<(), Error> {
        let mut analyzer = Analyzer::new()?;
        let analysis = analyzer.analyze(&parse("(rule (report (xyz.a ?x)) (nonesuch ?x))")?);
        assert!(matches!(analysis, Err(Error::Unknown(_))));
        Ok(())
    }
}
