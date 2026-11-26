//! A formula an embedder supplies is asked in a premise query and in
//! nothing else: a premise naming one the embedder does not hold is
//! refused when it is built, a query written as data cannot name one, a
//! rule holding one is refused when it is compiled and leaves nothing on
//! the branch, and a premise query asking one is answered, and kept
//! current under a subscription, from the branch's facts. Storage is
//! read on the volatile and the filesystem providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::BTreeMap;
use std::sync::LazyLock;

use anyhow::Result;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Entity, Fact, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::concept::query::ConceptQuery;
use dialog_query::corpus::Type as ValueType;
use dialog_query::error::{EvaluationError, TypeError};
use dialog_query::formula::extension::Output;
use dialog_query::formula::{Bindings, Cells, Extension, Extensions};
use dialog_query::query::{Output as _, PremiseQuery, PremiseRow};
use dialog_query::type_system::Type as Kind;
use dialog_query::{
    ConceptDescriptor, DeductiveRule, DeductiveRuleDescriptor, FormulaQuery, InductiveRule,
    Parameters, Premise, Proposition, Term, the,
};
use futures_util::TryStreamExt as _;

use crate::helpers::test_repo;
use crate::rules::source_attr;
use crate::{Branch, Repository};

/// `test/shout`: the text `of`, upper-cased, with `!`, as `is`.
#[derive(Debug)]
struct Shout;

static SHOUT: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(Kind::from(ValueType::String)))
            .the("what is shouted")
            .required();
        builder
            .cell("is", Some(Kind::from(ValueType::String)))
            .the("it, shouted")
            .output(0);
    })
});

impl Extension for Shout {
    fn name(&self) -> &'static str {
        "test/shout"
    }

    fn cells(&self) -> &'static Cells {
        &SHOUT
    }

    fn outputs(&self) -> &'static [&'static str] {
        &["is"]
    }

    fn compute(&self, bindings: &mut Bindings) -> Result<Vec<Output>, EvaluationError> {
        let of: String = bindings.read("of")?;
        Ok(vec![vec![(
            "is",
            Value::String(format!("{}!", of.to_uppercase())),
        )]])
    }
}

/// `test/words`: a row for each word of the text `of`, as `word`.
#[derive(Debug)]
struct Words;

static WORDS: LazyLock<Cells> = LazyLock::new(|| {
    Cells::define(|builder| {
        builder
            .cell("of", Some(Kind::from(ValueType::String)))
            .the("the text")
            .required();
        builder
            .cell("word", Some(Kind::from(ValueType::String)))
            .the("one of its words")
            .output(0);
    })
});

impl Extension for Words {
    fn name(&self) -> &'static str {
        "test/words"
    }

    fn cells(&self) -> &'static Cells {
        &WORDS
    }

    fn outputs(&self) -> &'static [&'static str] {
        &["word"]
    }

    fn compute(&self, bindings: &mut Bindings) -> Result<Vec<Output>, EvaluationError> {
        let of: String = bindings.read("of")?;
        Ok(of
            .split_whitespace()
            .map(|word| vec![("word", Value::String(word.into()))])
            .collect())
    }
}

/// `test/broken`: `test/shout`'s cells, and a row that breaks them as
/// it is told to: a number for the text `is`, a value for a cell it does
/// not declare, or no value for `is` at all.
#[derive(Debug)]
enum Broken {
    Mistyped,
    Undeclared,
    Missing,
}

impl Extension for Broken {
    fn name(&self) -> &'static str {
        "test/broken"
    }

    fn cells(&self) -> &'static Cells {
        &SHOUT
    }

    fn outputs(&self) -> &'static [&'static str] {
        &["is"]
    }

    fn compute(&self, bindings: &mut Bindings) -> Result<Vec<Output>, EvaluationError> {
        let of: String = bindings.read("of")?;
        Ok(vec![match self {
            Broken::Mistyped => vec![("is", Value::UnsignedInt(3))],
            Broken::Undeclared => vec![("is", Value::String(of)), ("of", text("other"))],
            Broken::Missing => vec![],
        }])
    }
}

fn extensions() -> Extensions {
    Extensions::new().with(Shout).with(Words)
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

/// The premise applying `name` to `?text`, its output `output` as `?out`.
fn applied(extensions: &Extensions, name: &str, output: &str) -> Result<FormulaQuery, TypeError> {
    let mut terms = Parameters::new();
    terms.insert("of".into(), Term::var("text"));
    terms.insert(output.into(), Term::var("out"));
    extensions.apply(name, terms)
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
    of: &Entity,
    value: &str,
) -> Result<()> {
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(the!("note/text").of(of.clone()).is(value.to_string()))
        .commit()
        .publish()
        .perform(session)
        .await?;
    Ok(())
}

async fn read<S: PeerSpace>(
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

/// A premise naming a formula the embedder holds no extension of is
/// refused as it is built, naming it, never evaluated to no rows.
#[dialog_common::test]
fn it_refuses_a_premise_naming_no_extension_it_holds() {
    let refused = applied(&extensions(), "test/whisper", "is");
    assert!(
        matches!(&refused, Err(TypeError::UnknownFormula { name }) if name == "test/whisper"),
        "{refused:?}"
    );
    let unheld = applied(&Extensions::new(), "test/shout", "is");
    assert!(
        matches!(&unheld, Err(TypeError::UnknownFormula { name }) if name == "test/shout"),
        "{unheld:?}"
    );
}

/// A query written as data cannot name an extension, held or not: it
/// does not deserialize. And an extension's premise has no spelling as
/// data, so no stored body or written query can hold one.
#[dialog_common::test]
fn it_gives_an_extension_no_spelling_as_data() -> Result<()> {
    let written = serde_json::json!({
        "assert": "test/shout",
        "where": { "of": { "?": { "name": "text" } }, "is": { "?": { "name": "out" } } }
    });
    assert!(serde_json::from_value::<Proposition>(written.clone()).is_err());
    assert!(serde_json::from_value::<FormulaQuery>(written).is_err());
    let premise = applied(&extensions(), "test/shout", "is")?;
    assert!(serde_json::to_value(&premise).is_err());
    assert!(serde_json::to_value(Proposition::Formula(premise)).is_err());
    Ok(())
}

/// A rule holding an extension is refused when it is compiled, as a
/// deductive rule built directly, as one compiled from its descriptor,
/// and as an inductive rule, naming the extension: no rule holding one
/// exists to be asserted, laid over a query or stored. The branch the
/// author meant to install it on holds no `dialog.rule/source` after.
async fn refuses_a_rule_holding_an_extension<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    write(&branch, session, &"id:a".parse()?, "hello there").await?;
    let shouted = || -> Result<Vec<Premise>> {
        Ok(vec![
            texts(),
            Premise::Assert(Proposition::Formula(applied(
                &extensions(),
                "test/shout",
                "is",
            )?)),
        ])
    };
    let refused = |result: &Result<(), TypeError>| matches!(result, Err(TypeError::EmbedderFormula { formula }) if formula == "test/shout");

    let deduced = DeductiveRule::new(concept("out", "note/shout"), shouted()?).map(|_| ());
    assert!(refused(&deduced), "{deduced:?}");

    let mut descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "out": { "the": "note/shout", "as": "text:", "cardinality": "many" } } },
        "when": []
    }))?;
    for premise in shouted()? {
        let Premise::Assert(proposition) = premise else {
            unreachable!("asserted premises");
        };
        descriptor.when.push(proposition);
    }
    let compiled = descriptor.compile().map(|_| ());
    assert!(refused(&compiled), "{compiled:?}");

    let induced = InductiveRule::new(concept("out", "note/shout"), shouted()?).map(|_| ());
    assert!(refused(&induced), "{induced:?}");

    let sources = read(&repo, "main", Selector::new().the(source_attr()), session).await?;
    assert!(sources.is_empty(), "nothing is installed: {sources:?}");
    Ok(())
}

/// On the volatile provider.
#[dialog_common::test]
async fn it_refuses_a_rule_holding_an_extension_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    refuses_a_rule_holding_an_extension(&session, &peer).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the
/// browser a branch is kept in IndexedDB and OPFS, and the web run of
/// `it_refuses_a_rule_holding_an_extension_on_a_volatile_branch` pins
/// the same refusal there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_refuses_a_rule_holding_an_extension_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    refuses_a_rule_holding_an_extension(&session, &peer).await
}

/// A premise query asks an extension over what a scan of the branch
/// binds, a row for each row the extension gives: `test/shout` one, and
/// `test/words` one a word.
#[dialog_common::test]
async fn it_asks_an_extension_in_a_premise_query() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let a: Entity = "id:a".parse()?;
    let b: Entity = "id:b".parse()?;
    write(&branch, &session, &a, "hello there").await?;
    write(&branch, &session, &b, "bye").await?;

    let shout = PremiseQuery::new(
        vec![
            texts(),
            Premise::Assert(Proposition::Formula(applied(
                &extensions(),
                "test/shout",
                "is",
            )?)),
        ],
        "this",
        ["out"],
    )?;
    let rows = branch
        .query()
        .select(shout)
        .perform(&session)
        .try_vec()
        .await?;
    assert_eq!(
        outs(&rows),
        vec![(a.clone(), text("HELLO THERE!")), (b.clone(), text("BYE!"))]
    );

    let words = PremiseQuery::new(
        vec![
            texts(),
            Premise::Assert(Proposition::Formula(applied(
                &extensions(),
                "test/words",
                "word",
            )?)),
        ],
        "this",
        ["out"],
    )?;
    let rows = branch
        .query()
        .select(words)
        .perform(&session)
        .try_vec()
        .await?;
    assert_eq!(
        outs(&rows),
        vec![
            (a.clone(), text("hello")),
            (a, text("there")),
            (b, text("bye")),
        ]
    );
    Ok(())
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

/// A subscription over a premise query that asks an extension follows the
/// facts its premises read: a commit that changes a text retracts the
/// row of the old one and asserts the row of the new, re-evaluated in
/// full, never left stale.
#[dialog_common::test]
async fn it_keeps_a_premise_query_subscription_current() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let a: Entity = "id:a".parse()?;
    write(&branch, &session, &a, "hi").await?;

    let shout = PremiseQuery::new(
        vec![
            texts(),
            Premise::Assert(Proposition::Formula(applied(
                &extensions(),
                "test/shout",
                "is",
            )?)),
        ],
        "this",
        ["out"],
    )?;
    let mut subscription = branch.subscribe(shout);
    let first = subscription.poll(&session).await?.expect("initial");
    assert_eq!(outs(&first.asserted), vec![(a.clone(), text("HI!"))]);

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
    assert_eq!(outs(&moved.asserted), vec![(a.clone(), text("HELLO!"))]);
    assert_eq!(outs(&moved.retracted), vec![(a.clone(), text("HI!"))]);
    let held: BTreeMap<Entity, Value> = outs(subscription.results()).into_iter().collect();
    assert_eq!(held, BTreeMap::from([(a, text("HELLO!"))]));
    Ok(())
}

/// What a premise query asking `broken` over one text answers.
async fn broken_answer(broken: Broken) -> Result<Result<Vec<PremiseRow>, EvaluationError>> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    write(&branch, &session, &"id:a".parse()?, "hi").await?;
    let held = Extensions::new().with(broken);
    let query = PremiseQuery::new(
        vec![
            texts(),
            Premise::Assert(Proposition::Formula(applied(&held, "test/broken", "is")?)),
        ],
        "this",
        ["out"],
    )?;
    Ok(branch
        .query()
        .select(query)
        .perform(&session)
        .try_vec()
        .await)
}

/// An extension whose row gives a cell a value of another type than the
/// cell declares fails the query, naming the extension and the cell,
/// and never answers the value.
#[dialog_common::test]
async fn it_fails_a_query_an_extension_mistypes_an_output_in() -> Result<()> {
    let answered = broken_answer(Broken::Mistyped).await?;
    assert!(
        matches!(&answered, Err(EvaluationError::MistypedOutput { formula, cell, .. })
            if formula == "test/broken" && cell == "is"),
        "{answered:?}"
    );
    Ok(())
}

/// An extension whose row gives a value for a cell it does not declare
/// as an output fails the query, naming the extension and the cell.
#[dialog_common::test]
async fn it_fails_a_query_an_extension_writes_an_undeclared_cell_in() -> Result<()> {
    let answered = broken_answer(Broken::Undeclared).await?;
    assert!(
        matches!(&answered, Err(EvaluationError::UndeclaredOutput { formula, cell })
            if formula == "test/broken" && cell == "of"),
        "{answered:?}"
    );
    Ok(())
}

/// An extension whose row leaves out an output it declares fails the
/// query, naming the extension and the cell, and is never read as a row
/// that did not match.
#[dialog_common::test]
async fn it_fails_a_query_an_extension_leaves_an_output_out_of() -> Result<()> {
    let answered = broken_answer(Broken::Missing).await?;
    assert!(
        matches!(&answered, Err(EvaluationError::MissingOutput { formula, cell })
            if formula == "test/broken" && cell == "is"),
        "{answered:?}"
    );
    Ok(())
}
