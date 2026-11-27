//! The notation's code for the formulas the engine declares and a crate
//! of dialog's own supplies ([`dialog_query::supplied`]): `notation/read`
//! and `notation/analyze`. The engine owns their names and cells; this
//! module is what they do, held in a query's environment by
//! [`supply`].
//!
//! ```clj
//! (select (cell/form ?c at: ?n line: ?l character: ?h)
//!   (cell ?c source: ?s)
//!   (notation/read text: ?s at: ?n start-line: ?l start-character: ?h))
//! ```
//!
//! A place is said as an editor speaking LSP says it: a line and a
//! character, both from zero, the character counted in UTF-16 units,
//! where the notation's own [`Point`] counts characters.
//!
//! | declared | the notation's code gives a row for |
//! |---|---|
//! | `notation/read` | each form [`parse_document`] reads of the text: `at`, its place among them; `tree`, the form as data ([`Syntax::value`](crate::Syntax::value)); its range |
//! | `notation/analyze` | each thing [`Analyzer::analyze`] says of the forms together: `at`, the form it is about; `severity`, `"error"` or `"warning"`; `code`, its kind as a word; `message`; `path`, the way to the part of the form it is about |
//!
//! A document asks `notation/read` in a `select`, or a `query` a select
//! asks, alone: a rule is stored and read back from its body, and the
//! engine refuses a rule holding a formula whose code is not its own. It
//! does not ask `notation/analyze` yet: its `forms` is a list of a
//! document's trees, and no query a document writes can build one, since
//! nothing gathers rows into an ordered list and a quoted form is not
//! built. A query built in Rust gives it a constant list.
//!
//! **What `notation/analyze` does here is temporary.** It analyzes the
//! forms against the definitions the [`Analyzer`] given to [`supply`]
//! held then, in memory: not the ones the query's revision holds. So
//! what it says does not move with the branch, a subscription over it
//! is not told when a definition changes, and two replicas holding the
//! same facts may be told different things. It reads nothing through
//! the [`Facts`] the engine hands it. It is replaced once definitions are
//! facts on the branch (`design/macros/definitions-plan.md`), by an
//! analysis that resolves what the forms use through `Facts`, so that
//! what it reads is the query's demand.

use dialog_corpus::{Composite, Value};
use dialog_query::error::EvaluationError;
use dialog_query::supplied::{
    Facts, NotationAnalyze, NotationPipeline, NotationRead, Output, Selected,
};

use crate::analyze::Analyzer;
use crate::diagnostic::{Diagnostic, Severity};
use crate::parse::parse_document;
use crate::written::{Point, Segment, Span};
use crate::{Error, Step, Syntax, check_forms, compile};

pub use dialog_query::supplied::{ANALYZE, PIPELINE, READ};
pub(crate) const STAGES: [&str; 3] = ["syntax/parse", "syntax/analyze", "syntax/compile"];

pub(crate) fn phase(name: &str) -> Option<&'static str> {
    match name {
        "syntax/parse" => Some("read"),
        "syntax/analyze" => Some("analyze"),
        "syntax/compile" => Some("compile"),
        _ => None,
    }
}

/// The keyword a formula of `name` is kept by in an analyzed tree, a
/// name an entity's scheme may be: `notation.read`.
pub(crate) fn keyword(name: &str) -> String {
    name.replace('/', ".")
}

/// The declared formula a keyword of an analyzed tree names, where it
/// names one whose code is the notation's.
pub(crate) fn named(keyword: &str) -> Option<&'static str> {
    [READ, ANALYZE, PIPELINE]
        .into_iter()
        .chain(STAGES)
        .find(|name| self::keyword(name) == keyword)
}

/// Hold the notation's code for `notation/read` and `notation/analyze`
/// in `env`, the environment queries run in: the reader, and an
/// analyzer over what `analyzer` defines now (temporary, see the module
/// docs). Asking either where nothing is held fails the query with
/// [`EvaluationError::Unsupplied`].
#[cfg(feature = "repository")]
pub fn supply(env: &(impl dialog_common::Holds + ?Sized), analyzer: &Analyzer) {
    use std::sync::Arc;
    dialog_query::supplied::supply_read(env, Arc::new(Read));
    supply_pipeline(env);
    dialog_query::supplied::supply_analyze(
        env,
        Arc::new(Analyze {
            analyzer: analyzer.clone(),
        }),
    );
}

/// Supply the reader and the stateless pipeline. Analysis context
/// travels in the query and repeated setup keeps the reader's cache warm.
pub fn supply_pipeline(env: &(impl dialog_common::Holds + ?Sized)) {
    dialog_query::supplied::supply_read_once(env, std::sync::Arc::new(Read));
    dialog_query::supplied::supply_pipeline(env, std::sync::Arc::new(Pipeline));
}

/// The reader, analyzer and compiler as independent document phases.
#[derive(Debug)]
pub struct Pipeline;

impl NotationPipeline for Pipeline {
    fn run(
        &self,
        phase: &str,
        input: &Value,
        context: Option<&Value>,
    ) -> Result<Output, EvaluationError> {
        pipeline(phase, input, context).map_err(|error| EvaluationError::Serialization {
            message: error.to_string(),
        })
    }
}

fn pipeline(phase: &str, input: &Value, context: Option<&Value>) -> Result<Output, Error> {
    let mut diagnostics = Vec::new();
    let output = match (phase, input) {
        ("read", Value::String(text)) => {
            let parsed = parse_document(text);
            diagnostics = parsed.diagnostics;
            let lines = Lines::of(text);
            let notes = diagnostics
                .iter()
                .map(|note| {
                    let path = &note.path[..];
                    let at = path.first().unwrap_or(&Segment::Item(0));
                    let mut fields = diagnostic_fields(note, at, path);
                    fields.extend(lines.range(note.span));
                    record(fields)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let forms = parsed
                .trees
                .iter()
                .map(Syntax::value)
                .collect::<Result<Vec<_>, _>>()?;
            return outputs(forms, notes);
        }
        ("analyze", _) => {
            let mut analyzer = Analyzer::new()?;
            if let Some(context) = context {
                let Some(texts) = crate::data::items(context)? else {
                    return Err(Error::Refused(
                        "analysis context must be a list of source texts".into(),
                    ));
                };
                for text in texts {
                    let Value::String(text) = text else {
                        return Err(Error::Refused(
                            "analysis context must contain source texts".into(),
                        ));
                    };
                    check_forms(&mut analyzer, &text);
                }
            }
            let forms = document_forms(input)?;
            let analysis = analyzer.analyze(&forms);
            diagnostics = analysis.diagnostics;
            analysis
                .analyzed
                .iter()
                .map(Syntax::value)
                .collect::<Result<_, _>>()?
        }
        ("compile", _) => {
            let mut compiled = Vec::new();
            for (at, tree) in document_forms(input)?.into_iter().enumerate() {
                match compile(&tree) {
                    Ok(steps) => {
                        for step in steps {
                            compiled.push(compiled_step(at, step)?);
                        }
                    }
                    Err(error) => diagnostics.push(Diagnostic::refused(
                        Some(tree),
                        vec![Segment::Item(at)],
                        error,
                    )),
                }
            }
            compiled
        }
        _ => {
            return Err(Error::Refused(format!(
                "notation/pipeline expects source text for read or syntax data for analyze or compile, got phase {phase}"
            )));
        }
    };
    let notes = diagnostics
        .iter()
        .map(|note| {
            let (at, path) = note.path.split_first().unwrap_or((&Segment::Item(0), &[]));
            record(diagnostic_fields(note, at, path))
        })
        .collect::<Result<Vec<_>, _>>()?;
    outputs(output, notes)
}

pub(crate) fn document_forms(input: &Value) -> Result<Vec<Syntax>, Error> {
    match input {
        Value::Composite(list) if list.is_list() => {
            list.items().map(|tree| Syntax::try_from(&tree)).collect()
        }
        value => Ok(vec![Syntax::try_from(value)?]),
    }
}

fn outputs(forms: Vec<Value>, diagnostics: Vec<Value>) -> Result<Output, Error> {
    Ok(vec![
        ("output", crate::data::list(forms)?),
        ("diagnostics", crate::data::list(diagnostics)?),
    ])
}

fn diagnostic_fields(note: &Diagnostic, at: &Segment, path: &[Segment]) -> Output {
    let at = match at {
        Segment::Item(at) => *at,
        _ => 0,
    };
    row(at, note.severity(), code(note), note.message.clone(), path)
}

fn record(fields: Output) -> Result<Value, Error> {
    crate::data::record(fields)
}

fn compiled_step(at: usize, step: Step) -> Result<Value, Error> {
    fn describe(value: &impl std::fmt::Debug) -> String {
        format!("{value:?}")
    }
    let (kind, detail) = match step {
        Step::Assert(value) => ("assert", describe(&value)),
        Step::Retract(value) => ("retract", describe(&value)),
        Step::Issue(value) => ("issue", describe(&value)),
        Step::Deduce(value) => ("deduce", describe(&value)),
        Step::Induce(value) => ("induce", describe(&value)),
        Step::Transient(value) => ("transient", value.to_string()),
        Step::Query(value) => ("query", describe(&value.select)),
        Step::Write(value) => ("write", describe(&value.query.select)),
        Step::Select(value) => ("select", format!("{value:?}")),
        Step::View(value) => ("view", format!("{value:?}")),
    };
    record(vec![
        ("at", natural(at)),
        ("kind", Value::String(kind.into())),
        ("detail", Value::String(detail)),
    ])
}

/// `notation/read`: the crate's reader.
#[derive(Debug)]
pub struct Read;

impl NotationRead for Read {
    fn read(&self, text: &str) -> Result<Vec<Output>, EvaluationError> {
        Ok(forms(text))
    }
}

/// `notation/analyze`, over the definitions an analyzer held when it was
/// made: temporary, see the module docs.
#[derive(Debug)]
pub struct Analyze {
    analyzer: Analyzer,
}

impl NotationAnalyze for Analyze {
    fn analyze<'s>(&'s self, forms: &'s [Value], _facts: &'s dyn Facts) -> Selected<'s> {
        Box::pin(async move { Ok(said(&self.analyzer, forms)) })
    }
}

/// A row of `notation/read` for each form of `text` read.
fn forms(text: &str) -> Vec<Output> {
    let lines = Lines::of(text);
    let parsed = parse_document(text);
    parsed
        .trees
        .iter()
        .enumerate()
        .filter_map(|(at, tree)| {
            // A tree with no value is one no row can carry; the parser
            // gives none such.
            let value = tree.value().ok()?;
            let span = parsed.written.span(&[Segment::Item(at)]);
            let mut row = vec![("at", natural(at)), ("tree", value)];
            row.extend(lines.range(span));
            Some(row)
        })
        .collect()
}

/// A row of `notation/analyze` for each thing `analyzer` says of `forms`
/// read together. A value that is no form is said to be one, as a read
/// refusal at its place.
fn said(analyzer: &Analyzer, forms: &[Value]) -> Vec<Output> {
    let mut rows = Vec::new();
    let mut trees = Vec::new();
    let mut places = Vec::new();
    for (at, form) in forms.iter().enumerate() {
        match Syntax::try_from(form) {
            Ok(tree) => {
                trees.push(tree);
                places.push(at);
            }
            Err(error) => rows.push(row(
                at,
                Severity::Error,
                "read",
                format!("the value at {at} is not a form: {error}"),
                &[],
            )),
        }
    }
    let analysis = analyzer.clone().analyze(&trees);
    for diagnostic in &analysis.diagnostics {
        // The way to what is said starts at the tree, by its place among
        // the trees analyzed, which is its place among the forms given
        // once those that are no form are left out.
        let (at, path) = match diagnostic.path.split_first() {
            Some((Segment::Item(tree), rest)) => {
                (places.get(*tree).copied().unwrap_or(*tree), rest)
            }
            _ => (0, &diagnostic.path[..]),
        };
        rows.push(row(
            at,
            diagnostic.severity(),
            code(diagnostic),
            diagnostic.message.clone(),
            path,
        ));
    }
    rows
}

fn row(at: usize, severity: Severity, code: &str, message: String, path: &[Segment]) -> Output {
    vec![
        ("at", natural(at)),
        (
            "severity",
            Value::String(
                match severity {
                    Severity::Error => "error",
                    Severity::Warning => "warning",
                }
                .into(),
            ),
        ),
        ("code", Value::String(code.into())),
        ("message", Value::String(message)),
        ("path", way(path)),
    ]
}

/// A path as data: a list, each segment a natural for a part with no
/// label, its label and `:` for a labelled part, `operator`, `key N` or
/// `value N`. A label always ends in `:`, so none is read as another.
fn way(path: &[Segment]) -> Value {
    let segments = path.iter().map(|segment| match segment {
        Segment::Item(at) => natural(*at),
        Segment::Operator => Value::String("operator".into()),
        Segment::Term(label) => Value::String(format!("{label}:")),
        Segment::Key(at) => Value::String(format!("key {at}")),
        Segment::Value(at) => Value::String(format!("value {at}")),
    });
    // A list of naturals and text always encodes.
    Composite::list(segments).map_or(Value::String(String::new()), Value::Composite)
}

/// The kind of what `diagnostic` says, as a word: its refusal's kind,
/// or `warning` for what is said without refusing.
fn code(diagnostic: &Diagnostic) -> &'static str {
    match &diagnostic.refusal {
        None => "warning",
        Some(Error::Read(_)) => "read",
        Some(Error::Unknown(_)) => "unknown",
        Some(Error::Ambiguous(..)) => "ambiguous",
        Some(Error::Refused(_)) => "refused",
        Some(Error::Unsupported(_)) => "unsupported",
        Some(Error::Reserved(_)) => "reserved",
        Some(Error::Clash(..)) => "clash",
        Some(Error::Unsafe(_)) => "unsafe",
        Some(Error::Twice(..)) => "twice",
        Some(Error::Cycle(..)) => "cycle",
        Some(Error::Endless(_)) => "endless",
    }
}

fn natural(n: usize) -> Value {
    Value::UnsignedInt(n as u128)
}

/// A text's lines, to say a place in it in UTF-16 units.
struct Lines<'a> {
    lines: Vec<&'a str>,
}

impl<'a> Lines<'a> {
    fn of(text: &'a str) -> Self {
        Lines {
            lines: text.split('\n').collect(),
        }
    }

    /// The UTF-16 units before `point`'s character on its line. A point
    /// past the end of its line is at the end.
    fn character(&self, point: Point) -> usize {
        self.lines.get(point.line as usize).map_or(0, |line| {
            line.chars()
                .take(point.column as usize)
                .map(char::len_utf16)
                .sum()
        })
    }

    /// The cells of `span`, the start of the text where there is none.
    fn range(&self, span: Option<Span>) -> Output {
        let Span { start, end } = span.unwrap_or_default();
        vec![
            ("start-line", natural(start.line as usize)),
            ("start-character", natural(self.character(start))),
            ("end-line", natural(end.line as usize)),
            ("end-character", natural(self.character(end))),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    fn cell<'a>(row: &'a Output, name: &str) -> &'a Value {
        &row.iter().find(|(cell, _)| *cell == name).unwrap().1
    }

    /// Each form's tree is the form as data, the value
    /// [`Syntax::value`](crate::Syntax::value) gives of it.
    #[test]
    fn it_gives_each_form_as_data() -> Result<(), Error> {
        let text = "(a ?x) (b ?y)";
        let rows = forms(text);
        let trees = parse(text)?;
        assert_eq!(rows.len(), 2);
        for (row, tree) in rows.iter().zip(&trees) {
            assert_eq!(cell(row, "tree"), &tree.value()?);
        }
        Ok(())
    }

    /// A row gives every output its formula declares, each once.
    #[test]
    fn it_gives_every_output_once() -> Result<(), Error> {
        let analyzer = Analyzer::new()?;
        let unknown = parse("(nothing-here ?x)")?[0].value()?;
        for (rows, mut declared) in [
            (
                forms("(a ?x)"),
                vec![
                    "at",
                    "tree",
                    "start-line",
                    "start-character",
                    "end-line",
                    "end-character",
                ],
            ),
            (
                said(&analyzer, &[unknown]),
                vec!["at", "severity", "code", "message", "path"],
            ),
        ] {
            assert!(!rows.is_empty());
            declared.sort_unstable();
            for row in &rows {
                let mut given: Vec<&str> = row.iter().map(|(cell, _)| *cell).collect();
                given.sort_unstable();
                assert_eq!(given, declared);
            }
        }
        Ok(())
    }

    /// The forms are analyzed together, so a rule reading a concept the
    /// document defines above it is told nothing; what is said of a form
    /// is at its place; and a value that is no form is said to be one,
    /// at its place, the forms after it kept at theirs.
    #[test]
    fn it_analyzes_the_forms_together_each_said_at_its_place() -> Result<(), Error> {
        let analyzer = Analyzer::new()?;
        let forms: Vec<Value> = parse(
            "(concept (t ?x name: ?n) (xyz.t/name of: ?x is: ?n as: :text cardinality: :one))
             (select (t ?x name: ?n))",
        )?
        .iter()
        .map(Syntax::value)
        .collect::<Result<_, _>>()?;
        assert!(said(&analyzer, &forms).is_empty());
        let mut with = vec![Value::UnsignedInt(7)];
        with.extend(forms);
        with.push(parse("(nothing-here ?x)")?[0].value()?);
        let rows = said(&analyzer, &with);
        let places: Vec<(&Value, &Value)> = rows
            .iter()
            .map(|row| (cell(row, "at"), cell(row, "code")))
            .collect();
        assert_eq!(
            places,
            vec![
                (&natural(0), &Value::String("read".into())),
                (&natural(3), &Value::String("unknown".into())),
            ]
        );
        Ok(())
    }

    /// A place is in UTF-16 units: a character outside the Basic
    /// Multilingual Plane is two.
    #[test]
    fn it_counts_a_place_in_utf16_units() {
        let lines = Lines::of("ab\n😀c");
        assert_eq!(lines.character(Point { line: 1, column: 1 }), 2);
        assert_eq!(lines.character(Point { line: 1, column: 2 }), 3);
        assert_eq!(lines.character(Point { line: 0, column: 9 }), 2);
        assert_eq!(lines.character(Point { line: 7, column: 0 }), 0);
    }
}
