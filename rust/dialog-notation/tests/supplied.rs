//! The notation's code for the formulas the engine declares and a crate
//! of dialog's own supplies (`dialog_query::supplied`): `notation/read`,
//! the crate's reader, and `notation/analyze`, for now its analyzer over
//! the definitions it holds in memory. Supplied to a query's environment
//! with [`supply`], read is asked by a document's `select` over texts a
//! branch holds, a row a form, each place in an editor's terms, a line
//! and a character from zero, the character counted in UTF-16 units. A
//! select asking it is kept current as the texts change, and a form a
//! rule would hold it in is refused, saying where it may be asked.

#![allow(deprecated)]

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use dialog_corpus::{Composite, Value};
use dialog_notation::live::Live;
use dialog_notation::supplied::{Read, supply};
use dialog_notation::{
    Analyzer, Answer, Applied, ApplyError, Error, Selection, Step, apply, check, compile, parse,
};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_query::error::EvaluationError;
use dialog_query::formula::FormulaCache;
use dialog_query::query::{Output as _, PremiseQuery, PremiseRow};
use dialog_query::supplied::{
    NotationAnalyzeQuery, NotationRead, NotationReadQuery, Output, supply_read,
};
use dialog_query::{FormulaQuery, Parameters, Premise, Proposition, ResolverQuery, Term};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

/// A cell of a notebook, and the text written in it.
const CELLS: &str = r#"
    (concept (cell ?c source: ?s)
      (xyz.cell/source of: ?c is: ?s as: :text cardinality: :one))"#;

/// The forms `notation/read` reads of each cell's text.
const FORMS: &str = r#"
    (select (cell/form ?c at: ?n line: ?l character: ?h to-line: ?tl to-character: ?th)
      (cell ?c source: ?s)
      (notation/read text: ?s at: ?n start-line: ?l start-character: ?h
                     end-line: ?tl end-character: ?th))"#;

/// The places of the forms of each cell's text, as a query a select
/// asks.
const PLACES: &str = r#"
    (query (cell/places ?c is: ?n)
      (cell ?c source: ?s)
      (notation/read text: ?s at: ?n))"#;

/// A branch, the analyzer that read what was applied to it, and who
/// applies, holding the notation's code for what the engine declares.
struct Notebook {
    operator: Peer<VolatileSpace, Session>,
    branch: Branch,
    analyzer: Analyzer,
}

impl Notebook {
    async fn open() -> Result<Self> {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let analyzer = Analyzer::new()?;
        supply(&operator, &analyzer);
        let mut notebook = Notebook {
            operator,
            branch,
            analyzer,
        };
        notebook.apply(CELLS).await?;
        Ok(notebook)
    }

    async fn apply(&mut self, text: &str) -> Result<Applied, ApplyError> {
        let checked = check(&mut self.analyzer, text);
        apply(&self.branch, &checked, &self.operator).await
    }

    /// Write `source` as the text of the cell `cell`.
    async fn write(&mut self, cell: &str, source: &str) -> Result<()> {
        let quoted = serde_json::to_string(source)?;
        self.apply(&format!("(assert (cell {cell} source: {quoted}))"))
            .await?;
        Ok(())
    }

    /// What `select` answers, each as text, sorted.
    async fn answers(&mut self, select: &str) -> Result<Vec<String>> {
        let mut all = Vec::new();
        for form in self.apply(select).await?.answers.forms {
            if let Answer::Rows(rows) = form.answered? {
                all.extend(rows.iter().map(ToString::to_string));
            }
        }
        all.sort();
        Ok(all)
    }

    /// The selection `select` compiles to, as the notebook's analyzer
    /// reads it.
    fn selection(&mut self, select: &str) -> Result<Selection> {
        let analyzed = self.analyzer.analyze(&parse(select)?);
        if let Some(refused) = analyzed.refusals().next() {
            anyhow::bail!("{}", refused.message);
        }
        match compile(&analyzed.analyzed[0])?.pop() {
            Some(Step::Select(selection)) => Ok(*selection),
            other => anyhow::bail!("not a select: {other:?}"),
        }
    }
}

/// The UTF-16 units of `text` before `part`, on its line.
fn utf16_before(text: &str, part: &str) -> usize {
    let at = text.find(part).expect("the part is in the text");
    let line = text[..at].rfind('\n').map_or(0, |newline| newline + 1);
    text[line..at].encode_utf16().count()
}

/// A premise asking `notation/read` of the constant `text`, each cell
/// named as a variable of its own.
fn read_of(text: &str) -> Result<Premise> {
    let mut terms = Parameters::new();
    terms.insert("text".into(), Term::Constant(Value::String(text.into())));
    for cell in [
        "at",
        "tree",
        "start-line",
        "start-character",
        "end-line",
        "end-character",
    ] {
        terms.insert(cell.into(), Term::var(cell));
    }
    Ok(Premise::Assert(Proposition::Formula(
        FormulaQuery::NotationRead(NotationReadQuery::new(terms)?),
    )))
}

fn natural(row: &PremiseRow, cell: &str) -> u128 {
    match row.get(cell) {
        Some(Value::UnsignedInt(n)) => *n,
        other => panic!("{cell} is {other:?}"),
    }
}

/// The notation's reader, supplied to the environment, answers
/// `notation/read` of a constant text asked alone, on a branch that
/// holds nothing: a row a form, its tree the value the crate's parser
/// gives of the form, and its range where the parser wrote it, the
/// character in UTF-16 units.
#[dialog_common::test]
async fn it_reads_a_constant_text_on_an_empty_branch() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    supply(&operator, &Analyzer::new()?);
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let text = "(a ?x)\n  ; 😀\n  (b \"😀\" ?y)";
    let query = PremiseQuery::new(
        vec![read_of(text)?],
        [
            "at",
            "tree",
            "start-line",
            "start-character",
            "end-line",
            "end-character",
        ],
    )?;
    let mut rows: Vec<PremiseRow> = branch
        .query()
        .select(query)
        .perform(&operator)
        .try_vec()
        .await?;
    rows.sort_by_key(|row| natural(row, "at"));
    let trees = parse(text)?;
    assert_eq!(rows.len(), trees.len(), "{rows:?}");
    for (row, tree) in rows.iter().zip(&trees) {
        assert_eq!(row.get("tree"), Some(&tree.value()?));
    }
    assert_eq!(
        [0, 1].map(|at| (
            natural(&rows[at], "start-line"),
            natural(&rows[at], "start-character"),
            natural(&rows[at], "end-line"),
            natural(&rows[at], "end-character"),
        )),
        [
            (0, 0, 0, 6),
            (
                2,
                2,
                2,
                (utf16_before(text, "(b") + "(b \"😀\" ?y)".encode_utf16().count()) as u128
            ),
        ]
    );
    Ok(())
}

/// `notation/analyze`, for now the notation's analyzer over the
/// definitions it held when supplied, answers what it says of a
/// document's forms given as a constant list, asked alone on a branch
/// that holds nothing: a row a thing said, `at` the form it is about. A
/// concept the document defines above a read of it is no unknown name:
/// the analyzer reads the forms together.
#[dialog_common::test]
async fn it_analyzes_a_documents_forms_on_an_empty_branch() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    supply(&operator, &Analyzer::new()?);
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let document =
        "(concept (t ?x name: ?n) (xyz.t/name of: ?x is: ?n as: :text cardinality: :one))
                    (select (t ?x name: ?n))
                    (nothing-here ?x)";
    let forms = parse(document)?
        .iter()
        .map(|tree| tree.value())
        .collect::<Result<Vec<_>, _>>()?;
    let mut terms = Parameters::new();
    terms.insert(
        "forms".into(),
        Term::Constant(Value::Composite(Composite::list(forms)?)),
    );
    for cell in ["at", "severity", "code", "message", "path"] {
        terms.insert(cell.into(), Term::var(cell));
    }
    let premise = Premise::Assert(Proposition::Resolver(Box::new(
        ResolverQuery::NotationAnalyze(NotationAnalyzeQuery::new(terms)?),
    )));
    let query = PremiseQuery::new(vec![premise], ["at", "severity", "code", "message"])?;
    let rows: Vec<PremiseRow> = branch
        .query()
        .select(query)
        .perform(&operator)
        .try_vec()
        .await?;
    let said: Vec<(u128, Option<&Value>, Option<&Value>)> = rows
        .iter()
        .map(|row| (natural(row, "at"), row.get("severity"), row.get("code")))
        .collect();
    assert_eq!(
        said,
        vec![(
            2,
            Some(&Value::String("error".into())),
            Some(&Value::String("unknown".into()))
        )],
        "{rows:?}"
    );
    Ok(())
}

/// A text's every form is a row, at its place among the forms, with the
/// range it takes; a form on its second line starts on line 1.
#[dialog_common::test]
async fn it_answers_each_form_read_reads() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook
        .write(
            "x:1",
            "(cell x:5 source: \"a\")\n  (cell x:6 source: \"b\")",
        )
        .await?;
    let forms = notebook.answers(FORMS).await?;
    assert_eq!(forms.len(), 2, "{forms:?}");
    assert!(
        forms.iter().any(|row| row.contains("at: 0")
            && row.contains("line: 0 ")
            && row.contains("character: 0 ")),
        "{forms:?}"
    );
    assert!(
        forms.iter().any(|row| row.contains("at: 1")
            && row.contains("line: 1 ")
            && row.contains("character: 2 ")),
        "{forms:?}"
    );
    Ok(())
}

/// A character is counted in UTF-16 units, as an editor speaking LSP
/// counts it: a form after text outside the Basic Multilingual Plane on
/// its line starts where the units before it end, not where the
/// characters do.
#[dialog_common::test]
async fn it_counts_a_character_in_utf16_units() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let text = "(cell x:7 source: \"😀😀\") (nothing-here ?x)";
    notebook.write("x:1", text).await?;
    let units = utf16_before(text, "(nothing-here");
    let characters = text[..text.find("(nothing-here").unwrap()].chars().count();
    assert_ne!(units, characters, "the text tells the two counts apart");
    let forms = notebook.answers(FORMS).await?;
    assert!(
        forms
            .iter()
            .any(|row| row.contains("at: 1") && row.contains(&format!("character: {units} "))),
        "a form starting at unit {units}: {forms:?}"
    );
    Ok(())
}

/// A select of a query asking `notation/read`, kept standing, answers
/// again as a cell's text is edited: a form written goes, a form added
/// comes.
#[dialog_common::test]
async fn it_keeps_what_read_reads_current_as_a_text_is_edited() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.apply(PLACES).await?;
    notebook.write("x:1", "(a ?x)").await?;
    let live = Live::new(notebook.branch.clone());
    let mut standing = live.standing(notebook.selection("(select (cell/places ?c is: ?n))")?)?;
    let first = standing.poll(&notebook.operator).await?.unwrap_or_default();
    let first: Vec<String> = first.iter().map(ToString::to_string).collect();
    assert_eq!(first, vec!["(cell/places x:1 is: 0)"]);

    notebook.write("x:1", "(a ?x) (b ?y)").await?;
    let edited = standing
        .poll(&notebook.operator)
        .await?
        .expect("the edit moved what is read");
    let mut edited: Vec<String> = edited.iter().map(ToString::to_string).collect();
    edited.sort();
    assert_eq!(
        edited,
        vec!["(cell/places x:1 is: 0)", "(cell/places x:1 is: 1)"]
    );
    Ok(())
}

#[derive(Debug, Default)]
struct CountedRead(AtomicUsize);

impl NotationRead for CountedRead {
    fn read(&self, text: &str) -> Result<Vec<Output>, EvaluationError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Read.read(text)
    }
}

#[dialog_common::test]
async fn it_reads_only_the_edited_source_after_a_standing_poll() -> Result<()> {
    const CELLS: usize = 50;
    let mut notebook = Notebook::open().await?;
    let reader = Arc::new(CountedRead::default());
    supply_read(&notebook.operator, reader.clone());
    let source = include_str!("fixtures/todo.clj");
    let mut writes = PLACES.to_owned();
    for cell in 0..CELLS {
        let text = serde_json::to_string(&format!("{source}\n; cell {cell}"))?;
        writeln!(writes, "(assert (cell x:{cell} source: {text}))")?;
    }
    notebook.apply(&writes).await?;
    let live = Live::new(notebook.branch.clone());
    let mut standing = live.standing(notebook.selection("(select (cell/places ?c is: ?n))")?)?;
    let initial = standing
        .poll(&notebook.operator)
        .await?
        .expect("initial forms");
    assert_eq!(initial.len(), CELLS * parse(source)?.len());
    assert_eq!(reader.0.load(Ordering::Relaxed), CELLS);

    notebook.write("x:0", &format!("{source}\n(extra)")).await?;
    let edited = standing
        .poll(&notebook.operator)
        .await?
        .expect("one extra form");
    assert_eq!(edited.len(), initial.len() + 1);
    assert_eq!(reader.0.load(Ordering::Relaxed), CELLS + 1);
    Ok(())
}

#[dialog_common::test]
async fn it_bounds_read_results_and_recomputes_for_a_replaced_reader() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let cache = FormulaCache::with_capacity(2);
    cache.hold(&notebook.operator);
    let reader = Arc::new(CountedRead::default());
    supply_read(&notebook.operator, reader.clone());

    let mut expected = Vec::new();
    for source in ["(a ?x)", "(b ?y)", "(c ?z)"] {
        notebook.write("x:1", source).await?;
        expected = notebook.answers(FORMS).await?;
        assert_eq!(expected.len(), 1);
    }
    assert_eq!(cache.len(), 2);
    assert_eq!(notebook.answers(FORMS).await?, expected);

    notebook.write("x:2", "(c ?z)").await?;
    let both = notebook.answers(FORMS).await?;
    assert_eq!(both.len(), 2);
    assert!(both.iter().any(|row| row.starts_with("(cell/form x:1 ")));
    assert!(both.iter().any(|row| row.starts_with("(cell/form x:2 ")));
    assert_eq!(reader.0.load(Ordering::Relaxed), 3);

    let replacement = Arc::new(CountedRead::default());
    supply_read(&notebook.operator, replacement.clone());
    assert_eq!(notebook.answers(FORMS).await?, both);
    assert_eq!(replacement.0.load(Ordering::Relaxed), 1);
    assert_eq!(cache.len(), 2);
    Ok(())
}

#[dialog_common::test]
async fn it_selects_syntax_phases_alone_and_passes_their_values_between_premises() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let parse = notebook
        .answers(r#"(select (syntax/parse "(select (xyz.cell/source of: x:1 is: ?s))" is: ?form))"#)
        .await?;
    assert_eq!(parse.len(), 1);
    assert!(parse[0].contains("application"), "{parse:?}");

    let result = notebook
        .answers(
            r#"(select (syntax.result syntax:result parsed: ?trees analyzed: ?expanded compiled: ?steps
                        messages: ?diagnostics)
                 (syntax/compile ?expanded is: ?steps)
                 (syntax/analyze ?trees is: ?expanded diagnostics: ?diagnostics)
                 (syntax/parse "(select (xyz.cell/source of: x:1 is: ?s))" is: ?trees))"#,
        )
        .await?;
    assert_eq!(result.len(), 1);
    assert!(
        result[0].contains("kind") && result[0].contains("query"),
        "{result:?}"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_stores_parsed_syntax_and_evaluates_saved_analysis_explicitly() -> Result<()> {
    use dialog_query::supplied::{NotationPipeline, NotationPipelineQuery};

    let mut notebook = Notebook::open().await?;
    let quoted = notebook
        .answers(
            r#"(select (syntax/analyze
             '(concept (note ?n text: ?t)
                (xyz.note/text of: ?n is: ?t as: :text cardinality: :one))
             is: ?forms diagnostics: ?messages))"#,
        )
        .await?;
    assert_eq!(quoted.len(), 1);
    assert!(quoted[0].contains("diagnostics: []"), "{quoted:?}");
    assert!(quoted[0].contains(":name /note"), "{quoted:?}");

    let source = r#"(assert (xyz.note/text of: note:1 is: "saved" cardinality: :one))"#;
    let phase = |name: &str, input, output| -> Result<Premise> {
        let mut terms = Parameters::new();
        terms.insert("phase".into(), Term::Constant(Value::String(name.into())));
        terms.insert("input".into(), input);
        terms.insert("output".into(), output);
        Ok(Premise::Assert(Proposition::Resolver(Box::new(
            ResolverQuery::NotationPipeline(NotationPipelineQuery::new(terms)?),
        ))))
    };
    let query = PremiseQuery::new(
        vec![
            phase(
                "read",
                Term::Constant(Value::String(source.into())),
                Term::var("parsed"),
            )?,
            phase("analyze", Term::var("parsed"), Term::var("analyzed"))?,
        ],
        ["parsed", "analyzed"],
    )?;
    let rows = notebook
        .branch
        .query()
        .select(query)
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    let [row]: &[PremiseRow] = rows.as_slice() else {
        anyhow::bail!("one pipeline row")
    };
    let Some(Value::Composite(parsed)) = row.get("parsed") else {
        anyhow::bail!("parsed list")
    };
    let form = parsed.items().next().expect("one parsed form");
    assert_eq!(form, parse(source)?[0].value()?);
    let analyzed = row.get("analyzed").expect("analysis record");
    assert_ne!(&form, analyzed);

    notebook
        .apply(&format!(
            r#"(assert (xyz.syntax/form of: syntax:saved is: ?form cardinality: :one)
                 (notation/read text: {source:?} tree: ?form))
               (assert (xyz.syntax/analyzed of: syntax:saved is: ?analysis cardinality: :one)
                 (xyz.syntax/form of: syntax:saved is: ?form)
                 (syntax/analyze ?form is: ?analysis))"#
        ))
        .await?;

    let selected = notebook
        .answers(
            r#"(select (syntax.result syntax:result compiled: ?steps)
             (xyz.syntax/form of: syntax:saved is: ?stored)
             (syntax/analyze ?stored is: ?analysis)
             (syntax/compile ?analysis is: ?steps))"#,
        )
        .await?;
    assert_eq!(selected.len(), 1);
    assert!(selected[0].contains("assert"), "{selected:?}");
    let compare = notebook
        .answers(
            r#"(select (xyz.syntax/form of: syntax:saved
                     is: '(assert (xyz.note/text of: note:1 is: "saved" cardinality: :one))))"#,
        )
        .await?;
    assert_eq!(compare.len(), 1);
    let select: dialog_query::ConceptQuery = serde_json::from_value(serde_json::json!({
        "assert": { "with": { "is": { "the": "xyz.syntax/analyzed" } } },
        "where": { "this": { "entity:": "syntax:saved" }, "is": { "?": { "name": "is" } } }
    }))?;
    let found: Vec<dialog_query::ConceptConclusion> = notebook
        .branch
        .query()
        .select(select)
        .perform(&notebook.operator)
        .try_vec()
        .await?;
    assert_eq!(found.len(), 1);
    let saved = found[0].source().lookup(&Term::var("is"))?.content()?;
    assert_eq!(&saved, analyzed);
    let saved = dialog_notation::check_analyzed(&saved)?;
    apply(&notebook.branch, &saved, &notebook.operator).await?;
    let notes = notebook
        .answers("(select (xyz.note/text of: note:1 is: ?text))")
        .await?;
    assert!(matches!(notes.as_slice(), [note] if note.contains("saved")));
    // Analyze accepts a single stored form without a source-text round trip.
    assert_eq!(
        dialog_notation::supplied::Pipeline.run("analyze", &parse(source)?[0].value()?, None)?[0].1,
        *analyzed
    );
    Ok(())
}

/// What a query gathers of a query asking `notation/read` is gathered
/// from that query's rows: the places of a cell's forms, as a set.
#[dialog_common::test]
async fn it_gathers_what_read_reads_of_a_text() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    notebook.write("x:1", "(a ?x)\n(b ?y)").await?;
    notebook.write("x:2", "(c ?z)").await?;
    let places = notebook
        .answers(
            r#"(query (cell/place ?c is: ?n)
                 (cell ?c source: ?s)
                 (notation/read text: ?s at: ?n))
               (query (cell/all ?c is: ?places)
                 (cell ?c source: ?s)
                 (cell/place ?c is: (distinct ?places)))
               (select (cell/all ?c is: ?places))"#,
        )
        .await?;
    assert_eq!(
        places,
        vec![
            "(cell/all x:1 is: {: 0 : 1})".to_string(),
            "(cell/all x:2 is: {: 0})".to_string(),
        ]
    );
    Ok(())
}

/// A stored rule or a view layer cannot hold a supplied reader. Refusing
/// either leaves any other writes in the same document unapplied.
#[dialog_common::test]
async fn it_refuses_notation_read_where_a_rule_would_hold_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    for text in [
        "(rule (cell/bad ?c at: ?n) (cell ?c source: ?s) (notation/read text: ?s at: ?n))",
        "(assert (cell x:3 source: \"written\")) \
         (query (cell/shown ?c is: ?v) (cell ?c source: ?s) (notation/read text: ?s at: ?n) \
           (view ?v [:p ?n])) \
         (select (cell/shown ?c is: ?v))",
    ] {
        let refused = match notebook.apply(text).await {
            Err(ApplyError::Refused(refused)) => refused,
            other => panic!(
                "{text} is refused: {:?}",
                other.map(|applied| applied.revision)
            ),
        };
        assert!(
            matches!(&refused.refusal, Some(Error::Refused(why))
                if why.contains("(notation/read …) is available in a `select` or a top-level write")),
            "{text}: {refused:?}"
        );
    }
    let written = notebook.answers("(select (cell x:3 source: ?s))").await?;
    assert_eq!(
        written,
        Vec::<String>::new(),
        "nothing of a refused document is applied"
    );
    Ok(())
}

/// `notation/analyze` is not asked by a document yet: its `forms` is a
/// list of a document's trees, and no query a document writes can build
/// one, since nothing gathers rows into an ordered list and a quoted
/// form is not built. A select asking it is refused, saying so, and
/// nothing is applied.
#[dialog_common::test]
async fn it_refuses_notation_analyze_in_a_document() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    let refused = match notebook
        .apply(
            "(concept (doc ?d forms: ?f) (xyz.doc/forms of: ?d is: ?f cardinality: :one))
             (select (doc/said ?d code: ?k) (doc ?d forms: ?f) (notation/analyze forms: ?f code: ?k))",
        )
        .await
    {
        Err(ApplyError::Refused(refused)) => refused,
        other => panic!("refused: {:?}", other.map(|applied| applied.revision)),
    };
    assert!(
        matches!(&refused.refusal, Some(Error::Unsupported(why))
            if why.contains("(notation/analyze …)") && why.contains("ordered list")),
        "{refused:?}"
    );
    Ok(())
}
