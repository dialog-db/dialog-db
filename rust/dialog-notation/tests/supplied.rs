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

use anyhow::Result;
use dialog_corpus::{Composite, Value};
use dialog_notation::live::Live;
use dialog_notation::supplied::supply;
use dialog_notation::{
    Analyzer, Answer, Applied, ApplyError, Error, Selection, Step, apply, check, compile, parse,
};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_peer::{Peer, Session};
use dialog_query::query::{Output as _, PremiseQuery, PremiseRow};
use dialog_query::supplied::{NotationAnalyzeQuery, NotationReadQuery};
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

/// `notation/read` is asked by a `select`, and by a query a select asks,
/// alone: a rule is stored and read back from its body, and Dialog-DB
/// refuses a rule holding a formula whose code is the notation's, and a
/// write's premises and a view's layer are rules. A rule, a write and a
/// view holding it are refused, saying so, and nothing of the document
/// is applied.
#[dialog_common::test]
async fn it_refuses_notation_read_where_a_rule_would_hold_it() -> Result<()> {
    let mut notebook = Notebook::open().await?;
    for text in [
        "(rule (cell/bad ?c at: ?n) (cell ?c source: ?s) (notation/read text: ?s at: ?n))",
        "(assert (cell x:3 source: \"written\")) \
         (assert (cell x:3 source: ?k) (cell ?c source: ?s) (notation/read text: ?s at: ?k))",
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
                if why.contains("(notation/read …) is asked in a `select` alone")),
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
