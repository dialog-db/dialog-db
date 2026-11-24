//! A rule that holds a pattern is stored, its body in format 5 of a rule
//! body, and runs from what is stored: committed through a transaction,
//! a batch, or a query layer's changes, read back from a branch loaded
//! afresh, held in the session overlay, and kept current by a
//! subscription. Run on the volatile and the filesystem providers.
//!
//! A rule whose pattern a stored body cannot write, a map pattern keyed by
//! anything but text, is still refused: committing one fails with
//! [`CommitError::Refused`], carrying [`TypeError::UnstorablePattern`] and
//! the rule's entity, and nothing the transaction holds is written.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{
    Changes, Composite, DialogCorpusError, Entity, Fact, Relation as CorpusRelation, Selector,
    Value,
};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::concept::query::ConceptQuery;
use dialog_query::error::TypeError;
use dialog_query::query::Output as _;
use dialog_query::rule::body::format_of;
use dialog_query::types::Any;
use dialog_query::{
    ConceptConclusion, ConceptDescriptor, DeductiveRule, InductiveRule, MapPattern, Parameters,
    Pattern, Premise, Proposition, Term, the,
};
use futures_util::TryStreamExt as _;

use crate::helpers::test_repo;
use crate::rules::source_attr;
use crate::{Branch, CommitError, Repository};

fn text(value: &str) -> Value {
    Value::String(value.into())
}

fn concept(field: &str, relation: &str, value_type: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { field: { "the": relation, "as": value_type, "cardinality": "many" } }
    }))
    .expect("a concept")
}

/// `note/payload` of `?this` matches `{key ?heading ..}`.
fn payload_premise(key: Value) -> Result<Premise> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.set(
        "payload".into(),
        Pattern::Map(
            MapPattern::new([(key, Pattern::Term(Term::var("heading")))])?
                .with_rest(Term::<Any>::blank()),
        ),
    );
    Ok(Premise::Assert(Proposition::Concept(ConceptQuery {
        predicate: concept("payload", "note/payload", "composite:map"),
        terms,
    })))
}

/// `note/payload` of `?this` matches `{"title" ?heading ..}`.
fn titled_premise() -> Result<Premise> {
    payload_premise(text("title"))
}

/// A deductive rule concluding `note/heading` from a pattern.
fn deductive() -> Result<DeductiveRule> {
    Ok(DeductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![titled_premise()?],
    )?)
}

/// An inductive rule writing `note/heading` from a pattern.
fn inductive() -> Result<InductiveRule> {
    Ok(InductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![titled_premise()?],
    )?)
}

/// A deductive rule whose pattern is keyed by the symbol `note/title`,
/// which a stored body cannot write: a map pattern is written with text
/// keys.
fn unwritable() -> Result<DeductiveRule> {
    let key: CorpusRelation = "note/title".parse()?;
    Ok(DeductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![payload_premise(Value::Symbol(key))?],
    )?)
}

/// The facts a branch, loaded afresh, holds under `selector`.
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

/// Whether `error` is the refusal of `rule` for the pattern it holds.
fn refuses(error: &CommitError, rule: &Entity) -> bool {
    matches!(
        error,
        CommitError::Refused(refusal)
            if refusal.entity() == rule
                && matches!(
                    refusal.reason::<TypeError>(),
                    Some(TypeError::UnstorablePattern { .. })
                )
    )
}

fn payload(title: &str) -> Result<Composite> {
    Ok(Composite::map([(text("title"), text(title))])?)
}

/// Commits the branch's first fact: `id:milk` titled "Buy milk".
async fn seed<S: PeerSpace>(branch: &Branch, session: &Peer<S, Session>) -> Result<()> {
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(
            the!("note/payload")
                .of("id:milk".parse::<Entity>()?)
                .is(payload("Buy milk")?),
        )
        .commit()
        .publish()
        .perform(session)
        .await?;
    Ok(())
}

/// Commits `of`'s payload titled `title`.
async fn write<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    of: &Entity,
    title: &str,
) -> Result<()> {
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(the!("note/payload").of(of.clone()).is(payload(title)?))
        .commit()
        .publish()
        .perform(session)
        .await?;
    Ok(())
}

/// `note/heading` of `?this` is `?heading`.
fn headings() -> ConceptQuery {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("heading".into(), Term::var("heading"));
    ConceptQuery {
        predicate: concept("heading", "note/heading", "text:"),
        terms,
    }
}

/// The headings a batch of concept rows binds, by entity.
fn titles<'a>(rows: impl IntoIterator<Item = &'a ConceptConclusion>) -> Vec<(Entity, Value)> {
    let mut titles: Vec<(Entity, Value)> = rows
        .into_iter()
        .filter_map(|row| {
            Some((
                row.entity().clone(),
                row.source().value_of("heading")?.clone(),
            ))
        })
        .collect();
    titles.sort_by(|a, b| a.0.cmp(&b.0));
    titles
}

/// The headings `name`, loaded afresh, derives.
async fn derived<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    name: &str,
    session: &Peer<S, Session>,
) -> Result<Vec<(Entity, Value)>> {
    let branch = repo.branch(name).load().perform(session).await?;
    let rows = branch
        .query()
        .select(headings())
        .perform(session)
        .try_vec()
        .await?;
    Ok(titles(&rows))
}

/// What `name`, loaded afresh, holds of `rule`: its body, in format 5,
/// which reads back as the rule under the entity it is stored under.
async fn stored<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    name: &str,
    session: &Peer<S, Session>,
    rule: &Entity,
) -> Result<Vec<u8>> {
    let sources = read(
        repo,
        name,
        Selector::new().of(rule.clone()).the(source_attr()),
        session,
    )
    .await?;
    let [source] = sources.as_slice() else {
        anyhow::bail!("one body is stored for the rule, found {}", sources.len());
    };
    let Value::Bytes(bytes) = &source.is else {
        anyhow::bail!("the body is bytes");
    };
    assert_eq!(format_of(bytes), Some(5), "the body is in format 5");
    Ok(bytes.clone())
}

/// Asserts `stage` into a transaction on `name` beside a plain fact and
/// commits: the commit lands both, and `name`, loaded afresh, holds the
/// rule's body and derives the heading through it.
async fn committed<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    name: &str,
    session: &Peer<S, Session>,
    rule: &DeductiveRule,
    stage: Changes,
) -> Result<()> {
    let branch = repo.branch(name).open().perform(session).await?;
    seed(&branch, session).await?;
    let before = branch.revision();
    let bystander: Entity = "id:bystander".parse()?;
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(
            the!("note/title")
                .of(bystander.clone())
                .is("kept".to_string()),
        )
        .integrate(stage)
        .commit()
        .publish()
        .perform(session)
        .await?;
    assert_ne!(branch.revision(), before, "the head moved");
    assert_eq!(
        read(repo, name, Selector::new().of(bystander), session)
            .await?
            .len(),
        1,
        "the statement beside the rule was written"
    );
    let body = stored(repo, name, session, &rule.this()).await?;
    assert!(
        DeductiveRule::decode(&body)
            .map_err(anyhow::Error::msg)?
            .stored_as(&rule.this())
    );
    let milk: Entity = "id:milk".parse()?;
    assert_eq!(
        derived(repo, name, session).await?,
        vec![(milk, text("Buy milk"))],
        "the stored rule fires"
    );
    Ok(())
}

/// Asserts `stage` into a transaction beside a plain fact and commits:
/// the commit is refused for `rule`, the head stays, and neither the
/// rule's facts nor the plain fact are written.
async fn refused<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    branch: &Branch,
    session: &Peer<S, Session>,
    rule: &Entity,
    stage: Changes,
) -> Result<()> {
    let before = branch.revision();
    let bystander: Entity = "id:bystander".parse()?;
    #[allow(deprecated)]
    let error = branch
        .transaction()
        .assert(
            the!("note/title")
                .of(bystander.clone())
                .is("kept?".to_string()),
        )
        .integrate(stage)
        .commit()
        .publish()
        .perform(session)
        .await
        .expect_err("a transaction holding an unwritable pattern rule does not commit");
    assert!(refuses(&error, rule), "{error:?}");
    assert_eq!(branch.revision(), before, "the head did not move");
    assert!(
        read(
            repo,
            branch.name(),
            Selector::new().of(rule.clone()),
            session
        )
        .await?
        .is_empty(),
        "no fact of the rule was written"
    );
    assert!(
        read(repo, branch.name(), Selector::new().of(bystander), session)
            .await?
            .is_empty(),
        "nothing else the transaction held was written"
    );
    Ok(())
}

/// A deductive and an inductive rule that hold a pattern commit,
/// asserted on the transaction itself, staged in a batch, or carried in
/// a query layer's changes, and fire from what is stored. A rule whose
/// pattern has no written form is refused the same ways.
async fn scenario<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let rule = deductive()?;
    let milk: Entity = "id:milk".parse()?;
    let call: Entity = "id:call".parse()?;

    // Asserted on the transaction.
    let branch = repo.branch("main").open().perform(session).await?;
    seed(&branch, session).await?;
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(&rule)
        .commit()
        .publish()
        .perform(session)
        .await?;
    let body = stored(&repo, "main", session, &rule.this()).await?;
    assert_eq!(body, rule.encode(), "the body is the rule's encoding");
    assert!(
        DeductiveRule::decode(&body)
            .map_err(anyhow::Error::msg)?
            .stored_as(&rule.this())
    );
    assert_eq!(
        derived(&repo, "main", session).await?,
        vec![(milk.clone(), text("Buy milk"))]
    );
    // A payload written after the rule is read through it.
    write(&branch, session, &call, "Call mum").await?;
    assert_eq!(
        derived(&repo, "main", session).await?,
        vec![
            (call.clone(), text("Call mum")),
            (milk.clone(), text("Buy milk"))
        ]
    );
    // Retracting the rule by value finds its body by encoding it.
    #[allow(deprecated)]
    branch
        .transaction()
        .retract(&rule)
        .commit()
        .publish()
        .perform(session)
        .await?;
    assert!(
        read(&repo, "main", Selector::new().of(rule.this()), session)
            .await?
            .is_empty(),
        "retracting the rule erases every fact of it"
    );
    assert!(derived(&repo, "main", session).await?.is_empty());

    // Beside other statements, through the batch a statement stages.
    let mut staged = Changes::new();
    staged.assert(&rule);
    assert!(staged.refused().is_none());
    committed(&repo, "staged", session, &rule, staged).await?;

    // A query layer's changes carry the body, so a copy into a
    // transaction commits the rule.
    let layer = branch.with(&rule);
    assert!(layer.changes().refused().is_none());
    committed(&repo, "layer", session, &rule, layer.changes().clone()).await?;

    // A copy asserted as a statement commits the same way.
    let statement = repo.branch("statement").open().perform(session).await?;
    seed(&statement, session).await?;
    #[allow(deprecated)]
    statement
        .transaction()
        .assert(layer.changes().clone())
        .commit()
        .publish()
        .perform(session)
        .await?;
    stored(&repo, "statement", session, &rule.this()).await?;
    assert_eq!(
        derived(&repo, "statement", session).await?,
        vec![(milk.clone(), text("Buy milk"))]
    );

    // An inductive rule fires at commit, from its stored body.
    let induced = inductive()?;
    let inducing = repo.branch("induced").open().perform(session).await?;
    #[allow(deprecated)]
    inducing
        .transaction()
        .assert(&induced)
        .commit()
        .publish()
        .perform(session)
        .await?;
    stored(&repo, "induced", session, &induced.this()).await?;
    write(&inducing, session, &call, "Call mum").await?;
    let written = read(
        &repo,
        "induced",
        Selector::new().the("note/heading".parse()?),
        session,
    )
    .await?;
    assert_eq!(
        written
            .iter()
            .map(|fact| (fact.of.clone(), fact.is.clone()))
            .collect::<Vec<_>>(),
        vec![(call, text("Call mum"))],
        "the inductive rule wrote the heading at commit"
    );

    // A rule whose pattern has no written form is still refused.
    let unwritable = unwritable()?;
    let refusing = repo.branch("refused").open().perform(session).await?;
    seed(&refusing, session).await?;
    let before = refusing.revision();
    #[allow(deprecated)]
    let error = refusing
        .transaction()
        .assert(&unwritable)
        .commit()
        .publish()
        .perform(session)
        .await
        .expect_err("an unwritable pattern rule does not commit");
    assert!(refuses(&error, &unwritable.this()), "{error:?}");
    assert_eq!(refusing.revision(), before);
    let mut staged = Changes::new();
    staged.assert(&unwritable);
    assert!(staged.is_empty(), "a durable batch stages nothing for it");
    refused(&repo, &refusing, session, &unwritable.this(), staged).await?;
    let layer = refusing.with(&unwritable);
    assert!(layer.changes().refused().is_some());
    refused(
        &repo,
        &refusing,
        session,
        &unwritable.this(),
        layer.changes().clone(),
    )
    .await?;
    Ok(())
}

/// On the volatile provider.
#[dialog_common::test]
async fn it_commits_a_pattern_rule_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    scenario(&session, &peer).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the browser
/// a branch is kept in IndexedDB and OPFS, and the web run of
/// `it_commits_a_pattern_rule_on_a_volatile_branch` pins the same
/// scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_commits_a_pattern_rule_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    scenario(&session, &peer).await
}

/// The branch's session overlay holds a pattern rule as it holds any
/// rule, its body in format 5: asserted there, or applied from a query
/// layer's changes, it lands, and every query of the branch derives
/// through it. A rule whose pattern has no written form is still refused
/// there with [`DialogCorpusError::Refused`], and nothing lands.
#[dialog_common::test]
async fn it_holds_a_pattern_rule_in_the_session_overlay() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    seed(&branch, &session).await?;
    let rule = deductive()?;
    let milk: Entity = "id:milk".parse()?;

    let before = branch.overlay().revision();
    branch.overlay().assert(&rule)?;
    assert_ne!(branch.overlay().revision(), before, "the rule landed");
    let sources = branch
        .overlay()
        .scan(&Selector::new().of(rule.this()).the(source_attr()));
    let [source] = sources.as_slice() else {
        anyhow::bail!("one body, found {}", sources.len());
    };
    assert!(matches!(&source.is, Value::Bytes(bytes) if format_of(bytes) == Some(5)));
    let rows = branch
        .query()
        .select(headings())
        .perform(&session)
        .try_vec()
        .await?;
    assert_eq!(titles(&rows), vec![(milk.clone(), text("Buy milk"))]);

    // Gone with its facts when it is retracted.
    branch.overlay().retract(&rule)?;
    assert!(
        branch
            .overlay()
            .scan(&Selector::new().of(rule.this()))
            .is_empty()
    );
    let rows = branch
        .query()
        .select(headings())
        .perform(&session)
        .try_vec()
        .await?;
    assert!(rows.is_empty());

    // A query layer's changes apply.
    let layer = branch.with(&rule);
    branch.overlay().apply(layer.changes().clone())?;
    let rows = branch
        .query()
        .select(headings())
        .perform(&session)
        .try_vec()
        .await?;
    assert_eq!(titles(&rows), vec![(milk, text("Buy milk"))]);

    // A rule whose pattern has no written form is refused, typed.
    let unwritable = unwritable()?;
    let refused = |error: &DialogCorpusError| {
        matches!(
            error,
            DialogCorpusError::Refused(refusal)
                if refusal.entity() == &unwritable.this()
                    && matches!(
                        refusal.reason::<TypeError>(),
                        Some(TypeError::UnstorablePattern { .. })
                    )
        )
    };
    let before = branch.overlay().revision();
    let Err(error) = branch.overlay().assert(&unwritable) else {
        panic!("the overlay refuses the unwritable rule");
    };
    assert!(refused(&error), "{error:?}");
    let layer = branch.with(&unwritable);
    let error = branch
        .overlay()
        .apply(layer.changes().clone())
        .expect_err("the overlay refuses a layer's changes");
    assert!(refused(&error), "{error:?}");
    assert_eq!(branch.overlay().revision(), before, "nothing landed");
    assert!(
        branch
            .overlay()
            .scan(&Selector::new().of(unwritable.this()))
            .is_empty()
    );
    Ok(())
}

/// Polls the subscription to `headings` and checks what it asserts and
/// retracts.
macro_rules! polled {
    ($subscription:expr, $session:expr, asserted: $asserted:expr, retracted: $retracted:expr $(,)?) => {{
        let delta = $subscription
            .poll($session)
            .await?
            .expect("the subscription moved");
        assert_eq!(titles(&delta.asserted), $asserted, "asserted");
        assert_eq!(titles(&delta.retracted), $retracted, "retracted");
    }};
}

/// A subscription to the concept a stored pattern rule concludes is kept
/// current: a commit that adds a payload the pattern matches asserts its
/// heading, one that changes the title retracts the old heading and
/// asserts the new, and one that leaves no title retracts it. The rule
/// is committed, so the subscription reads it from the branch; and it is
/// in the session overlay, from which the subscription reads it the
/// same way.
#[dialog_common::test]
async fn it_keeps_a_subscription_through_a_stored_pattern_rule_current() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let rule = deductive()?;
    for overlay in [false, true] {
        let branch = repo
            .branch(if overlay { "overlay" } else { "committed" })
            .open()
            .perform(&session)
            .await?;
        seed(&branch, &session).await?;
        if overlay {
            branch.overlay().assert(&rule)?;
        } else {
            #[allow(deprecated)]
            branch
                .transaction()
                .assert(&rule)
                .commit()
                .publish()
                .perform(&session)
                .await?;
        }
        let milk: Entity = "id:milk".parse()?;
        let call: Entity = "id:call".parse()?;

        let mut subscription = branch.subscribe(headings());
        let first = subscription.poll(&session).await?.expect("initial");
        assert_eq!(
            titles(&first.asserted),
            vec![(milk.clone(), text("Buy milk"))]
        );

        write(&branch, &session, &call, "Call mum").await?;
        polled!(subscription, &session,
            asserted: vec![(call.clone(), text("Call mum"))],
            retracted: vec![]);

        // A payload with an entry the pattern does not name still
        // matches: the blank rest takes it.
        let with_done = Composite::map([
            (text("title"), text("Call dad")),
            (text("done"), Value::Boolean(true)),
        ])?;
        #[allow(deprecated)]
        branch
            .transaction()
            .retract(
                the!("note/payload")
                    .of(call.clone())
                    .is(payload("Call mum")?),
            )
            .assert(the!("note/payload").of(call.clone()).is(with_done.clone()))
            .commit()
            .publish()
            .perform(&session)
            .await?;
        polled!(subscription, &session,
            asserted: vec![(call.clone(), text("Call dad"))],
            retracted: vec![(call.clone(), text("Call mum"))]);

        // A payload with no title is a value the pattern does not match.
        #[allow(deprecated)]
        branch
            .transaction()
            .retract(the!("note/payload").of(call.clone()).is(with_done))
            .assert(
                the!("note/payload")
                    .of(call.clone())
                    .is(Composite::map([(text("note"), text("later"))])?),
            )
            .commit()
            .publish()
            .perform(&session)
            .await?;
        polled!(subscription, &session,
            asserted: vec![],
            retracted: vec![(call, text("Call dad"))]);
    }
    Ok(())
}

/// A standing fold over the concept a stored pattern rule concludes
/// counts, per titled entity, the headings the rule derives for it, and
/// a commit that adds a payload reports the count that started to hold
/// and the one that stopped.
#[dialog_common::test]
async fn it_keeps_a_fold_through_a_stored_pattern_rule_current() -> Result<()> {
    use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
    use dialog_query::{AttributeQuery, Conclusion as _, Fold};

    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    let milk: Entity = "id:milk".parse()?;
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(the!("note/title").of(milk.clone()).is("Milk".to_string()))
        .assert(&deductive()?)
        .commit()
        .publish()
        .perform(&session)
        .await?;

    let mut inputs = headings();
    inputs.terms.insert("this".into(), Term::var("t"));
    let fold = Fold::new(
        AttributeQuery::new(
            Term::from(the!("note/title")),
            Term::<Entity>::var("t"),
            Term::<Any>::var("name"),
            Term::var("cause"),
            None,
        ),
        inputs,
        Reduce::new(
            vec!["t".into()],
            vec![ReduceEntry::new(
                "titled",
                Aggregator::Count,
                Term::var("heading"),
            )],
        ),
    );
    let count = |rows: &[dialog_query::FoldConclusion]| -> Vec<(Entity, Value)> {
        rows.iter()
            .filter_map(|row| Some((row.this().clone(), row.get("titled")?.clone())))
            .collect()
    };

    let mut subscription = branch.subscribe_fold(fold);
    let first = subscription.poll(&session).await?.expect("initial");
    assert_eq!(
        count(&first.asserted),
        vec![(milk.clone(), Value::UnsignedInt(0))]
    );

    write(&branch, &session, &milk, "Buy milk").await?;
    let delta = subscription.poll(&session).await?.expect("a heading added");
    assert_eq!(
        count(&delta.asserted),
        vec![(milk.clone(), Value::UnsignedInt(1))]
    );
    assert_eq!(count(&delta.retracted), vec![(milk, Value::UnsignedInt(0))]);
    Ok(())
}

/// `table/row` of `?this` is `?row`, the row as one record.
fn rows() -> ConceptQuery {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("row".into(), Term::var("row"));
    ConceptQuery {
        predicate: concept("row", "table/row", "composite:map"),
        terms,
    }
}

/// `row(this, row) :- title(this, title), rank(this, rank),
/// row == {title: title, rank: rank}`: a rule whose conclusion builds a
/// record from several of its variables with a map pattern, the whole
/// row as one record under one attribute. It is what a query of more
/// than one column compiles to.
fn row_rule() -> Result<DeductiveRule> {
    let read = |relation: &str, declared: &str, variable: &str| {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("is".into(), Term::var(variable));
        Premise::Assert(Proposition::Concept(ConceptQuery {
            predicate: concept("is", relation, declared),
            terms,
        }))
    };
    let row = Pattern::Map(MapPattern::new([
        (text("title"), Pattern::Term(Term::var("title"))),
        (text("rank"), Pattern::Term(Term::var("rank"))),
    ])?);
    Ok(DeductiveRule::new(
        concept("row", "table/row", "composite:map"),
        vec![
            read("note/title", "text:", "title"),
            read("note/rank", "natural:", "rank"),
            row.is(Term::<Any>::var("row")),
        ],
    )?)
}

/// The rows a batch binds, by entity, each as the record it is.
fn records<'a>(rows: impl IntoIterator<Item = &'a ConceptConclusion>) -> Vec<(Entity, Value)> {
    let mut records: Vec<(Entity, Value)> = rows
        .into_iter()
        .filter_map(|row| Some((row.entity().clone(), row.source().value_of("row")?.clone())))
        .collect();
    records.sort_by(|a, b| a.0.cmp(&b.0));
    records
}

fn record(title: &str, rank: u128) -> Result<Value> {
    Ok(Composite::map([
        (text("title"), text(title)),
        (text("rank"), Value::UnsignedInt(rank)),
    ])?
    .into())
}

/// The case the notation needs: a deductive rule whose conclusion builds
/// a record from several of its variables with a map pattern, committed
/// to a branch and asserted into a session's ephemeral overlay, then
/// subscribed to. Rows arrive when their columns are written, change
/// when a column does, and leave when one is retracted. On a branch
/// loaded afresh the committed rule's body is format 5 and derives the
/// same rows.
#[dialog_common::test]
async fn it_keeps_rows_a_stored_rule_builds_as_records_current() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let rule = row_rule()?;
    assert_eq!(rule.storable(), Ok(()));
    let milk: Entity = "id:milk".parse()?;
    let call: Entity = "id:call".parse()?;

    for overlay in [false, true] {
        let name = if overlay { "overlay" } else { "committed" };
        let branch = repo.branch(name).open().perform(&session).await?;
        #[allow(deprecated)]
        branch
            .transaction()
            .assert(
                the!("note/title")
                    .of(milk.clone())
                    .is("Buy milk".to_string()),
            )
            .assert(the!("note/rank").of(milk.clone()).is(1u64))
            .commit()
            .publish()
            .perform(&session)
            .await?;
        if overlay {
            branch.overlay().assert(&rule)?;
        } else {
            #[allow(deprecated)]
            branch
                .transaction()
                .assert(&rule)
                .commit()
                .publish()
                .perform(&session)
                .await?;
            stored(&repo, name, &session, &rule.this()).await?;
            let loaded = repo.branch(name).load().perform(&session).await?;
            let rows = loaded
                .query()
                .select(rows())
                .perform(&session)
                .try_vec()
                .await?;
            assert_eq!(records(&rows), vec![(milk.clone(), record("Buy milk", 1)?)]);
        }

        let mut subscription = branch.subscribe(rows());
        let first = subscription.poll(&session).await?.expect("initial");
        assert_eq!(
            records(&first.asserted),
            vec![(milk.clone(), record("Buy milk", 1)?)]
        );

        // A row arrives once both its columns are written.
        #[allow(deprecated)]
        branch
            .transaction()
            .assert(
                the!("note/title")
                    .of(call.clone())
                    .is("Call mum".to_string()),
            )
            .assert(the!("note/rank").of(call.clone()).is(2u64))
            .commit()
            .publish()
            .perform(&session)
            .await?;
        let arrived = subscription.poll(&session).await?.expect("a row arrived");
        assert_eq!(
            records(&arrived.asserted),
            vec![(call.clone(), record("Call mum", 2)?)]
        );
        assert!(arrived.retracted.is_empty());

        // A column changes: the old row leaves and the new one arrives.
        #[allow(deprecated)]
        branch
            .transaction()
            .retract(the!("note/rank").of(call.clone()).is(2u64))
            .assert(the!("note/rank").of(call.clone()).is(3u64))
            .commit()
            .publish()
            .perform(&session)
            .await?;
        let changed = subscription.poll(&session).await?.expect("a row changed");
        assert_eq!(
            records(&changed.asserted),
            vec![(call.clone(), record("Call mum", 3)?)]
        );
        assert_eq!(
            records(&changed.retracted),
            vec![(call.clone(), record("Call mum", 2)?)]
        );

        // A column is retracted: the row leaves.
        #[allow(deprecated)]
        branch
            .transaction()
            .retract(
                the!("note/title")
                    .of(milk.clone())
                    .is("Buy milk".to_string()),
            )
            .commit()
            .publish()
            .perform(&session)
            .await?;
        let left = subscription.poll(&session).await?.expect("a row left");
        assert!(left.asserted.is_empty());
        assert_eq!(
            records(&left.retracted),
            vec![(milk.clone(), record("Buy milk", 1)?)]
        );
    }
    Ok(())
}

/// `bytes`, a stored body, stamped with `format` instead.
fn restamped(bytes: &[u8], format: u64) -> Result<Vec<u8>> {
    let mut body: ipld_core::ipld::Ipld = serde_ipld_dagcbor::from_slice(bytes)?;
    let ipld_core::ipld::Ipld::Map(envelope) = &mut body else {
        anyhow::bail!("a body is a map");
    };
    envelope.insert(
        "format".into(),
        ipld_core::ipld::Ipld::Integer(i128::from(format)),
    );
    Ok(serde_ipld_dagcbor::to_vec(&body)?)
}

/// Commits every fact asserting `rule` stages, its body replaced by `body`.
async fn install_as<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    rule: &DeductiveRule,
    body: Vec<u8>,
) -> Result<()> {
    let mut staged = Changes::new();
    staged.assert(rule);
    let instructions: Vec<_> = staged
        .into_instructions()
        .into_iter()
        .map(|instruction| match instruction {
            dialog_corpus::Instruction::Assert(mut fact, cardinality)
                if fact.the == source_attr() =>
            {
                fact.is = Value::Bytes(body.clone());
                dialog_corpus::Instruction::Assert(fact, cardinality)
            }
            other => other,
        })
        .collect();
    branch
        .commit(futures_util::stream::iter(instructions))
        .perform(session)
        .await?;
    Ok(())
}

/// The rule bodies `branch` does not read, as `dialog.rule/invalid`
/// answers: each rule's entity and why.
async fn invalid<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
) -> Result<Vec<(Entity, Value)>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("reason".into(), Term::var("reason"));
    let rows = branch
        .query()
        .select(ConceptQuery {
            predicate: serde_json::from_value(serde_json::json!({ "with": {
                "reason": { "the": "dialog.rule/invalid", "as": "text:" }
            }}))?,
            terms,
        })
        .perform(session)
        .try_vec()
        .await?;
    Ok(rows
        .iter()
        .filter_map(|row| {
            Some((
                row.entity().clone(),
                row.source().value_of("reason")?.clone(),
            ))
        })
        .collect())
}

/// `Branch::upgrade_rules` rewrites a body below format 4 into the
/// format this release writes for its rule, and leaves a body in
/// format 4 or 5 as it is: no earlier format wrote a pattern, so nothing
/// is rewritten into format 5, and a rule of terms is written in format
/// 4. A format 3 body of a rule of terms is rewritten into format 4, the
/// bytes the rule encodes to now, and the pattern rule's format 5 body
/// and the plain rule's format 4 body are not touched; a second run
/// rewrites nothing.
#[dialog_common::test]
async fn it_rewrites_only_bodies_below_format_4_on_upgrade() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    seed(&branch, &session).await?;
    let pattern = deductive()?;
    let plain = DeductiveRule::new(
        concept("label", "note/label", "text:"),
        vec![{
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            terms.insert("is".into(), Term::var("label"));
            Premise::Assert(Proposition::Concept(ConceptQuery {
                predicate: concept("is", "note/title", "text:"),
                terms,
            }))
        }],
    )?;
    let earlier = DeductiveRule::new(
        concept("tag", "note/tag", "text:"),
        vec![{
            let mut terms = Parameters::new();
            terms.insert("this".into(), Term::var("this"));
            terms.insert("is".into(), Term::var("tag"));
            Premise::Assert(Proposition::Concept(ConceptQuery {
                predicate: concept("is", "note/title", "text:"),
                terms,
            }))
        }],
    )?;
    assert_eq!(format_of(&plain.encode()), Some(4));
    assert_eq!(format_of(&pattern.encode()), Some(5));
    #[allow(deprecated)]
    branch
        .transaction()
        .assert(&pattern)
        .assert(&plain)
        .commit()
        .publish()
        .perform(&session)
        .await?;
    install_as(
        &branch,
        &session,
        &earlier,
        restamped(&earlier.encode(), 3)?,
    )
    .await?;
    assert_eq!(
        format_of(&stored_bytes(&repo, &session, &earlier.this()).await?),
        Some(3)
    );

    let upgraded = branch.upgrade_rules().perform(&session).await?;
    assert_eq!(upgraded.rewritten, vec![earlier.this()]);
    assert!(upgraded.reinstalled.is_empty());
    assert!(upgraded.undecodable.is_empty());
    assert_eq!(
        stored_bytes(&repo, &session, &earlier.this()).await?,
        earlier.encode(),
        "rewritten into format 4"
    );
    assert_eq!(
        stored_bytes(&repo, &session, &plain.this()).await?,
        plain.encode()
    );
    assert_eq!(
        stored_bytes(&repo, &session, &pattern.this()).await?,
        pattern.encode()
    );
    branch.refresh(&session).await?;
    let again = branch.upgrade_rules().perform(&session).await?;
    assert!(again.rewritten.is_empty(), "{:?}", again.rewritten);
    assert_eq!(
        derived(&repo, "main", &session).await?,
        vec![("id:milk".parse()?, text("Buy milk"))],
        "the pattern rule still fires"
    );
    Ok(())
}

/// The one body `main` holds for `rule`, in whatever format.
async fn stored_bytes<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    session: &Peer<S, Session>,
    rule: &Entity,
) -> Result<Vec<u8>> {
    let sources = read(
        repo,
        "main",
        Selector::new().of(rule.clone()).the(source_attr()),
        session,
    )
    .await?;
    let [source] = sources.as_slice() else {
        anyhow::bail!("one body is stored for the rule, found {}", sources.len());
    };
    let Value::Bytes(bytes) = &source.is else {
        anyhow::bail!("the body is bytes");
    };
    Ok(bytes.clone())
}

/// A release meets a body in a format later than it reads, as one that
/// reads up to format 4 meets the format 5 body of a pattern rule: it
/// does not read it, so the rule is inert, derives nothing, and is listed
/// in `dialog.rule/invalid` as newer, and no read fails over it. Pinned
/// here with the pattern rule's body stamped format 6, the format after
/// this release's, on the same path.
#[dialog_common::test]
async fn it_leaves_a_body_in_a_later_format_inert() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    seed(&branch, &session).await?;
    let rule = deductive()?;
    install_as(&branch, &session, &rule, restamped(&rule.encode(), 6)?).await?;
    assert!(
        derived(&repo, "main", &session).await?.is_empty(),
        "the rule derives nothing"
    );
    assert_eq!(
        invalid(&branch, &session).await?,
        vec![(
            rule.this(),
            text("newer: the body is in format 6, and this release reads up to 5")
        )]
    );
    Ok(())
}
