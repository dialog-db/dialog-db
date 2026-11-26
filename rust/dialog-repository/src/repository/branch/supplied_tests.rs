//! The formulas the engine declares and a crate of dialog's own supplies,
//! `notation/read` and the resolver `notation/analyze`: each is asked in
//! a premise query through the implementation the environment holds, a
//! premise asking one where none is held fails the query naming it, a
//! rule holding one is refused when it is compiled and leaves nothing
//! on the branch, a row that breaks its declaration fails the query, and
//! a resolver that reads facts through the environment is followed by a
//! subscription as a scan is. Storage is read on the volatile and the
//! filesystem providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Composite, Entity, Fact, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::attribute::Relation;
use dialog_query::concept::query::ConceptQuery;
use dialog_query::error::{EvaluationError, TypeError};
use dialog_query::query::{Output as _, PremiseQuery, PremiseRow};
use dialog_query::supplied::{
    Facts, NotationAnalyze, NotationAnalyzeQuery, NotationRead, NotationReadQuery, Output,
    Selected, supply_analyze, supply_read,
};
use dialog_query::{
    ConceptDescriptor, DeductiveRule, DeductiveRuleDescriptor, FormulaQuery, InductiveRule,
    Parameters, Premise, Proposition, ResolverQuery, Term, the,
};
use futures_util::TryStreamExt as _;

use crate::helpers::test_repo;
use crate::rules::source_attr;
use crate::{Branch, Repository};

/// A reader that takes each word of a text for a form: its place, the
/// word as its tree, and where it is on the one line.
#[derive(Debug)]
struct Words;

impl NotationRead for Words {
    fn read(&self, text: &str) -> Result<Vec<Output>, EvaluationError> {
        let mut rows = Vec::new();
        let mut from = 0;
        for (at, word) in text.split_whitespace().enumerate() {
            let start = text[from..].find(word).map_or(from, |offset| from + offset);
            from = start + word.len();
            rows.push(vec![
                ("at", Value::UnsignedInt(at as u128)),
                ("tree", Value::String(word.into())),
                ("start-line", Value::UnsignedInt(0)),
                ("start-character", Value::UnsignedInt(start as u128)),
                ("end-line", Value::UnsignedInt(0)),
                ("end-character", Value::UnsignedInt(from as u128)),
            ]);
        }
        Ok(rows)
    }
}

/// A reader whose row breaks its declaration as it is told to.
#[derive(Debug)]
enum Broken {
    Mistyped,
    Undeclared,
    Missing,
}

impl NotationRead for Broken {
    fn read(&self, _: &str) -> Result<Vec<Output>, EvaluationError> {
        let range = || {
            vec![
                ("start-line", Value::UnsignedInt(0)),
                ("start-character", Value::UnsignedInt(0)),
                ("end-line", Value::UnsignedInt(0)),
                ("end-character", Value::UnsignedInt(0)),
            ]
        };
        let mut row = range();
        match self {
            Broken::Mistyped => row.extend([
                ("at", Value::String("first".into())),
                ("tree", Value::String("x".into())),
            ]),
            Broken::Undeclared => row.extend([
                ("at", Value::UnsignedInt(0)),
                ("tree", Value::String("x".into())),
                ("text", Value::String("again".into())),
            ]),
            Broken::Missing => row.push(("at", Value::UnsignedInt(0))),
        }
        Ok(vec![row])
    }
}

/// An analyzer that says each form of a document, a word, is unknown
/// where no `def/name` fact names it, at the form's place: it reads the
/// definitions through the environment, and counts the documents it
/// analyzed.
#[derive(Debug, Default)]
struct Defined {
    analyzed: AtomicUsize,
}

impl NotationAnalyze for Defined {
    fn analyze<'s>(&'s self, forms: &'s [Value], facts: &'s dyn Facts) -> Selected<'s> {
        Box::pin(async move {
            self.analyzed.fetch_add(1, Ordering::SeqCst);
            let defined = facts
                .select(
                    Selector::new().the(
                        "def/name"
                            .parse()
                            .map_err(|error| EvaluationError::Store(format!("{error}")))?,
                    ),
                )
                .await?;
            Ok(forms
                .iter()
                .enumerate()
                .filter(|(_, form)| !defined.iter().any(|fact| &fact.is == *form))
                .map(|(at, form)| {
                    vec![
                        ("at", Value::UnsignedInt(at as u128)),
                        ("severity", Value::String("error".into())),
                        ("code", Value::String("unknown".into())),
                        (
                            "message",
                            Value::String(format!("nothing defines {form:?}")),
                        ),
                        ("path", Value::String(String::new())),
                    ]
                })
                .collect())
        })
    }
}

fn text(value: &str) -> Value {
    Value::String(value.into())
}

fn concept(field: &str, relation: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { field: { "the": relation, "as": "text:", "cardinality": "many" } }
    }))
    .expect("a concept")
}

/// `note/text` of `?this` is `?text`.
fn texts() -> Premise {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("text".into(), Term::var("text"));
    Premise::Assert(Proposition::Concept(ConceptQuery {
        predicate: concept("text", "note/text"),
        terms,
    }))
}

/// `notation/read` of `?text`, its `cell` as `?out`.
fn read_premise(cell: &str) -> Result<Premise, TypeError> {
    let mut terms = Parameters::new();
    terms.insert("text".into(), Term::var("text"));
    terms.insert(cell.into(), Term::var("out"));
    Ok(Premise::Assert(Proposition::Formula(
        FormulaQuery::NotationRead(NotationReadQuery::new(terms)?),
    )))
}

/// A document of `forms`, each a word, as the list `notation/analyze`
/// takes.
fn document(forms: &[&str]) -> Value {
    Value::Composite(Composite::list(forms.iter().map(|form| text(form))).expect("a list"))
}

/// `notation/analyze` of the document of the one form `hi`, its
/// `message` as `?out`.
fn analyze_premise() -> Result<Premise, TypeError> {
    analyze_document(&["hi"])
}

/// `notation/analyze` of the document of `forms`, its `message` as
/// `?out` and its `at` as `?at`.
fn analyze_document(forms: &[&str]) -> Result<Premise, TypeError> {
    let mut terms = Parameters::new();
    terms.insert("forms".into(), Term::Constant(document(forms)));
    terms.insert("message".into(), Term::var("out"));
    terms.insert("at".into(), Term::var("at"));
    Ok(Premise::Assert(Proposition::Resolver(Box::new(
        ResolverQuery::NotationAnalyze(NotationAnalyzeQuery::new(terms)?),
    ))))
}

/// The rows of a premise query, as each entity and its `out`.
fn outs<'a>(rows: impl IntoIterator<Item = &'a PremiseRow>) -> Vec<(Entity, Value)> {
    let mut outs: Vec<(Entity, Value)> = rows
        .into_iter()
        .filter_map(|row| Some((row.this.clone(), row.values.get("out")?.clone())))
        .collect();
    outs.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then(format!("{:?}", a.1).cmp(&format!("{:?}", b.1)))
    });
    outs
}

async fn write<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    attribute: Relation,
    of: &Entity,
    value: &str,
) -> Result<()> {
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(attribute.of(of.clone()).is(value.to_string()))
        .commit()
        .publish()
        .perform(session)
        .await?;
    Ok(())
}

async fn read_facts<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    name: &str,
    selector: Selector<Constrained>,
    session: &Peer<S, Session>,
) -> Result<Vec<Fact>> {
    let branch = repo.branch(name).load().perform(session).await?;
    Ok(branch
        .claims()
        .select(selector)
        .to_owned()
        .perform(session)
        .await?
        .try_collect::<Vec<Fact>>()
        .await?)
}

/// A premise query asks `notation/read` through the reader the
/// environment holds: a row for each form it reads of each text.
#[dialog_common::test]
async fn it_asks_the_reader_the_environment_holds() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    supply_read(&session, Arc::new(Words));
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let a: Entity = "id:a".parse()?;
    write(&branch, &session, the!("note/text"), &a, "hello there").await?;
    let query = PremiseQuery::new(vec![texts(), read_premise("tree")?], "this", ["out"])?;
    let rows = branch
        .query()
        .select(query)
        .perform(&session)
        .try_vec()
        .await?;
    assert_eq!(
        outs(&rows),
        vec![(a.clone(), text("hello")), (a, text("there"))]
    );
    Ok(())
}

/// A premise asking a declared formula in an environment that holds no
/// implementation of it fails the query, naming the formula: never a
/// query that answers nothing, with a text to read or with none. So does
/// the resolver.
#[dialog_common::test]
async fn it_fails_a_query_asking_what_no_implementation_is_held_for() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let empty = repo.branch("empty").open().perform(&session).await?;
    for (premise, name) in [
        (read_premise("tree")?, "notation/read"),
        (analyze_premise()?, "notation/analyze"),
    ] {
        let query = PremiseQuery::new(vec![texts(), premise], "this", ["out"])?;
        let answered = empty
            .query()
            .select(query)
            .perform(&session)
            .try_vec()
            .await;
        assert!(
            matches!(&answered, Err(EvaluationError::Unsupplied { formula }) if formula == name),
            "with no text: {answered:?}"
        );
    }
    let branch = repo.branch("main").open().perform(&session).await?;
    write(&branch, &session, the!("note/text"), &"id:a".parse()?, "hi").await?;
    for (premise, name) in [
        (read_premise("tree")?, "notation/read"),
        (analyze_premise()?, "notation/analyze"),
    ] {
        let query = PremiseQuery::new(vec![texts(), premise], "this", ["out"])?;
        let answered = branch
            .query()
            .select(query)
            .perform(&session)
            .try_vec()
            .await;
        assert!(
            matches!(&answered, Err(EvaluationError::Unsupplied { formula }) if formula == name),
            "{answered:?}"
        );
    }
    Ok(())
}

/// A declared formula and resolver have names, so a rule written as data
/// can name either, and each reads back as what it names.
#[dialog_common::test]
fn it_reads_a_declared_formula_written_as_data() -> Result<()> {
    let written = serde_json::json!({
        "assert": "notation/read",
        "where": { "text": { "?": { "name": "text" } }, "tree": { "?": { "name": "out" } } }
    });
    let read: Proposition = serde_json::from_value(written)?;
    assert!(
        matches!(&read, Proposition::Formula(FormulaQuery::NotationRead(_))),
        "{read:?}"
    );
    let written = serde_json::json!({
        "assert": "notation/analyze",
        "where": { "forms": { "?": { "name": "forms" } }, "message": { "?": { "name": "out" } } }
    });
    let analyze: Proposition = serde_json::from_value(written)?;
    assert!(
        matches!(&analyze, Proposition::Resolver(resolver)
            if matches!(**resolver, ResolverQuery::NotationAnalyze(_))),
        "{analyze:?}"
    );
    Ok(())
}

/// A rule holding a declared formula or resolver is refused when it is
/// compiled, as a deductive rule built directly, as one compiled from
/// its descriptor written as data, and as an inductive rule, naming it:
/// its implementation is only where the crate that supplies it is, and
/// what it does would become part of what a stored rule means. The
/// branch holds no `dialog.rule/source` after.
async fn refuses_a_rule_holding_a_supplied_formula<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    supply_read(session, Arc::new(Words));
    supply_analyze(session, Arc::new(Defined::default()));
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    write(
        &branch,
        session,
        the!("note/text"),
        &"id:a".parse()?,
        "hello",
    )
    .await?;
    for (premise, name) in [
        (read_premise("tree")?, "notation/read"),
        (analyze_premise()?, "notation/analyze"),
    ] {
        let premises = || vec![texts(), premise.clone()];
        let refused = |result: &Result<(), TypeError>| matches!(result, Err(TypeError::SuppliedInRule { formula }) if formula == name);

        let deduced = DeductiveRule::new(concept("out", "note/read"), premises()).map(|_| ());
        assert!(refused(&deduced), "{name}: {deduced:?}");

        let mut descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "out": { "the": "note/read", "as": "text:", "cardinality": "many" } } },
            "when": []
        }))?;
        for premise in premises() {
            let Premise::Assert(proposition) = premise else {
                unreachable!("asserted premises");
            };
            let written: Proposition = serde_json::from_value(serde_json::to_value(&proposition)?)?;
            descriptor.when.push(written);
        }
        let compiled = descriptor.compile().map(|_| ());
        assert!(refused(&compiled), "{name}: {compiled:?}");

        let induced = InductiveRule::new(concept("out", "note/read"), premises()).map(|_| ());
        assert!(refused(&induced), "{name}: {induced:?}");
    }
    let sources = read_facts(&repo, "main", Selector::new().the(source_attr()), session).await?;
    assert!(sources.is_empty(), "nothing is installed: {sources:?}");
    Ok(())
}

/// On the volatile provider.
#[dialog_common::test]
async fn it_refuses_a_rule_holding_a_supplied_formula_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    refuses_a_rule_holding_a_supplied_formula(&session, &peer).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the
/// browser a branch is kept in IndexedDB and OPFS, and the web run of
/// `it_refuses_a_rule_holding_a_supplied_formula_on_a_volatile_branch`
/// pins the same refusal there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_refuses_a_rule_holding_a_supplied_formula_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    refuses_a_rule_holding_a_supplied_formula(&session, &peer).await
}

/// A premise query selecting a variable none of its premises binds is
/// refused as it is built, naming it.
#[dialog_common::test]
fn it_refuses_a_premise_query_selecting_what_nothing_binds() {
    let refused = PremiseQuery::new(vec![texts()], "this", ["nowhere"]);
    assert!(
        matches!(&refused, Err(TypeError::UnselectedVariable { variable }) if variable == "nowhere"),
        "{refused:?}"
    );
}

/// A subscription over a premise query asking `notation/read` follows
/// the texts it reads: a commit that changes one retracts the rows of
/// the old text and asserts those of the new.
#[dialog_common::test]
async fn it_keeps_a_subscription_to_what_is_read_current() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    supply_read(&session, Arc::new(Words));
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let a: Entity = "id:a".parse()?;
    write(&branch, &session, the!("note/text"), &a, "hi").await?;
    let query = PremiseQuery::new(vec![texts(), read_premise("tree")?], "this", ["out"])?;
    let mut subscription = branch.subscribe(query);
    let first = subscription.poll(&session).await?.expect("initial");
    assert_eq!(outs(&first.asserted), vec![(a.clone(), text("hi"))]);

    #[allow(deprecated)]
    branch
        .transaction()
        .retract(the!("note/text").of(a.clone()).is("hi".to_string()))
        .assert(the!("note/text").of(a.clone()).is("hello".to_string()))
        .commit()
        .publish()
        .perform(&session)
        .await?;
    let moved = subscription
        .poll(&session)
        .await?
        .expect("the subscription moved");
    assert_eq!(outs(&moved.asserted), vec![(a.clone(), text("hello"))]);
    assert_eq!(outs(&moved.retracted), vec![(a.clone(), text("hi"))]);
    let held: BTreeMap<Entity, Value> = outs(subscription.results()).into_iter().collect();
    assert_eq!(held, BTreeMap::from([(a, text("hello"))]));
    Ok(())
}

/// A resolver that reads facts through the environment records what it
/// read as demand, as a scan does: a subscription over it is left alone
/// by a commit of a fact it did not read, and evaluated again when one
/// it read, a definition, changes.
#[dialog_common::test]
async fn it_follows_the_definitions_a_resolver_reads() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let analyzer = Arc::new(Defined::default());
    supply_analyze(&session, analyzer.clone());
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let a: Entity = "id:a".parse()?;
    write(&branch, &session, the!("note/text"), &a, "hi").await?;
    let query = PremiseQuery::new(vec![texts(), analyze_premise()?], "this", ["out"])?;
    let mut subscription = branch.subscribe(query);
    let first = subscription.poll(&session).await?.expect("initial");
    assert_eq!(first.asserted.len(), 1, "hi is unknown: {first:?}");
    let analyzed = analyzer.analyzed.load(Ordering::SeqCst);

    write(&branch, &session, the!("note/other"), &a, "unrelated").await?;
    let unmoved = subscription.poll(&session).await?;
    assert!(unmoved.is_none(), "a fact nothing read: {unmoved:?}");
    assert_eq!(
        analyzer.analyzed.load(Ordering::SeqCst),
        analyzed,
        "not analyzed again for a fact it did not read"
    );

    write(&branch, &session, the!("def/name"), &"id:d".parse()?, "hi").await?;
    let moved = subscription
        .poll(&session)
        .await?
        .expect("a definition it read changed");
    assert_eq!(moved.retracted.len(), 1, "hi is defined now: {moved:?}");
    assert!(subscription.results().is_empty());
    Ok(())
}

/// `notation/analyze` takes a document's forms at once, so what it says
/// of one form can depend on the others; each row says which form it is
/// about, by its place in the list, from zero, as `notation/read`'s `at`
/// does. Given two forms, the first defined, it answers one row, for the
/// second.
#[dialog_common::test]
async fn it_analyzes_a_document_and_says_which_form_each_row_is_about() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    supply_analyze(&session, Arc::new(Defined::default()));
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let a: Entity = "id:a".parse()?;
    write(&branch, &session, the!("note/text"), &a, "a note").await?;
    write(
        &branch,
        &session,
        the!("def/name"),
        &"id:d".parse()?,
        "known",
    )
    .await?;
    let query = PremiseQuery::new(
        vec![texts(), analyze_document(&["known", "unknown"])?],
        "this",
        ["out", "at"],
    )?;
    let rows = branch
        .query()
        .select(query)
        .perform(&session)
        .try_vec()
        .await?;
    let said: Vec<(Option<&Value>, Option<&Value>)> = rows
        .iter()
        .map(|row| (row.values.get("at"), row.values.get("out")))
        .collect();
    assert_eq!(
        said,
        vec![(
            Some(&Value::UnsignedInt(1)),
            Some(&text("nothing defines String(\"unknown\")"))
        )],
        "{rows:?}"
    );
    Ok(())
}

/// `forms` is a list: a premise giving it another value is refused as it
/// is built, naming the cell.
#[dialog_common::test]
fn it_refuses_an_analysis_of_what_is_not_a_list_of_forms() {
    let mut terms = Parameters::new();
    terms.insert("forms".into(), Term::Constant(text("(concept)")));
    terms.insert("message".into(), Term::var("out"));
    let refused = NotationAnalyzeQuery::new(terms);
    assert!(
        matches!(&refused, Err(TypeError::KindMismatch { binding, .. }) if binding == "forms"),
        "{refused:?}"
    );
}

/// What a premise query asking `broken` over one text answers.
async fn broken_answer(broken: Broken) -> Result<Result<Vec<PremiseRow>, EvaluationError>> {
    let (session, peer) = test_session_with_peer().await;
    supply_read(&session, Arc::new(broken));
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    write(&branch, &session, the!("note/text"), &"id:a".parse()?, "hi").await?;
    let query = PremiseQuery::new(vec![texts(), read_premise("tree")?], "this", ["out"])?;
    Ok(branch
        .query()
        .select(query)
        .perform(&session)
        .try_vec()
        .await)
}

/// A row that gives a cell a value of another type than the engine
/// declares for it fails the query, naming the formula and the cell.
#[dialog_common::test]
async fn it_fails_a_query_whose_reader_mistypes_an_output() -> Result<()> {
    let answered = broken_answer(Broken::Mistyped).await?;
    assert!(
        matches!(&answered, Err(EvaluationError::MistypedOutput { formula, cell, .. })
            if formula == "notation/read" && cell == "at"),
        "{answered:?}"
    );
    Ok(())
}

/// A row that gives a value for a cell the engine does not declare as an
/// output fails the query, naming the formula and the cell.
#[dialog_common::test]
async fn it_fails_a_query_whose_reader_writes_an_undeclared_cell() -> Result<()> {
    let answered = broken_answer(Broken::Undeclared).await?;
    assert!(
        matches!(&answered, Err(EvaluationError::UndeclaredOutput { formula, cell })
            if formula == "notation/read" && cell == "text"),
        "{answered:?}"
    );
    Ok(())
}

/// A row that leaves out an output the engine declares fails the query,
/// naming the formula and the cell.
#[dialog_common::test]
async fn it_fails_a_query_whose_reader_leaves_an_output_out() -> Result<()> {
    let answered = broken_answer(Broken::Missing).await?;
    assert!(
        matches!(&answered, Err(EvaluationError::MissingOutput { formula, cell })
            if formula == "notation/read" && cell == "tree"),
        "{answered:?}"
    );
    Ok(())
}
