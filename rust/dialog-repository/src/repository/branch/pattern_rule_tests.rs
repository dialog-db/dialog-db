//! A rule that holds a pattern has no stored body: committing one is
//! refused with [`CommitError::Refused`], carrying
//! [`TypeError::UnstorablePattern`] and the rule's entity, and nothing the
//! transaction holds is written. Run on the volatile and the filesystem
//! providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Changes, Composite, DialogCorpusError, Entity, Fact, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::concept::query::ConceptQuery;
use dialog_query::error::TypeError;
use dialog_query::query::Output as _;
use dialog_query::types::Any;
use dialog_query::{
    ConceptConclusion, ConceptDescriptor, DeductiveRule, InductiveRule, MapPattern, Parameters,
    Pattern, Premise, Proposition, Term, the,
};
use futures_util::TryStreamExt as _;

use crate::helpers::test_repo;
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

/// `note/payload` of `?this` matches `{"title" ?heading ..}`.
fn titled_premise() -> Result<Premise> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.set(
        "payload".into(),
        Pattern::Map(
            MapPattern::new([(text("title"), Pattern::Term(Term::var("heading")))])?
                .with_rest(Term::<Any>::blank()),
        ),
    );
    Ok(Premise::Assert(Proposition::Concept(ConceptQuery {
        predicate: concept("payload", "note/payload", "composite:map"),
        terms,
    })))
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

/// The facts a branch, loaded afresh, holds under `selector`.
async fn read<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    selector: Selector<Constrained>,
    session: &Peer<S, Session>,
) -> Result<Vec<Fact>> {
    let branch = repo.branch("main").load().perform(session).await?;
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

/// Commits the branch's first fact, so a refusal has a revision to leave
/// where it was.
async fn seed<S: PeerSpace>(branch: &Branch, session: &Peer<S, Session>) -> Result<()> {
    let titled = Composite::map([(text("title"), text("Buy milk"))])?;
    branch
        .transaction()
        .assert(
            the!("note/payload")
                .of("id:milk".parse::<Entity>()?)
                .is(titled),
        )
        .commit()
        .publish()
        .perform(session)
        .await?;
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
        .expect_err("a transaction holding a pattern rule does not commit");
    assert!(refuses(&error, rule), "{error:?}");
    assert_eq!(branch.revision(), before, "the head did not move");
    assert!(
        read(repo, Selector::new().of(rule.clone()), session)
            .await?
            .is_empty(),
        "no fact of the rule was written"
    );
    assert!(
        read(repo, Selector::new().of(bystander), session)
            .await?
            .is_empty(),
        "nothing else the transaction held was written"
    );
    Ok(())
}

/// A deductive and an inductive rule that hold a pattern are refused at
/// commit, asserted on the transaction itself or carried in a batch from
/// a query layer that holds the rule.
async fn scenario<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    seed(&branch, session).await?;

    // Asserted on the transaction.
    let rule = deductive()?;
    let before = branch.revision();
    #[allow(deprecated)]
    let error = branch
        .transaction()
        .assert(&rule)
        .commit()
        .publish()
        .perform(session)
        .await
        .expect_err("a deductive pattern rule does not commit");
    assert!(refuses(&error, &rule.this()), "{error:?}");
    assert_eq!(branch.revision(), before);

    // Beside other statements, through the batch a statement stages.
    let mut staged = Changes::new();
    staged.assert(&rule);
    assert!(staged.is_empty(), "a durable batch stages nothing for it");
    refused(&repo, &branch, session, &rule.this(), staged).await?;

    // The same for an inductive rule: no target holds one.
    let induced = inductive()?;
    let mut staged = Changes::new();
    staged.assert(&induced);
    assert!(staged.is_empty());
    refused(&repo, &branch, session, &induced.this(), staged).await?;

    // A query layer holds the deductive rule and stages its index facts;
    // the batch it hands on carries the refusal, so a copy into a
    // transaction is refused, and its index facts are not written.
    let layer = branch.with(&rule);
    assert!(!layer.changes().is_empty(), "the layer holds the rule");
    assert!(layer.changes().refused().is_some());
    refused(
        &repo,
        &branch,
        session,
        &rule.this(),
        layer.changes().clone(),
    )
    .await?;

    // A copy asserted as a statement is refused the same way.
    let before = branch.revision();
    #[allow(deprecated)]
    let error = branch
        .transaction()
        .assert(layer.changes().clone())
        .commit()
        .publish()
        .perform(session)
        .await
        .expect_err("a copy of the layer's changes does not commit");
    assert!(refuses(&error, &rule.this()), "{error:?}");
    assert_eq!(branch.revision(), before);
    Ok(())
}

/// On the volatile provider.
#[dialog_common::test]
async fn it_refuses_to_commit_a_pattern_rule_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    scenario(&session, &peer).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the browser
/// a branch is kept in IndexedDB and OPFS; the refusal happens before
/// anything reaches a provider, and the web run of
/// `it_refuses_to_commit_a_pattern_rule_on_a_volatile_branch` pins it
/// there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_refuses_to_commit_a_pattern_rule_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    scenario(&session, &peer).await
}

/// The branch's session overlay is a line of facts, exported and read
/// back as facts, so it has no place for a rule's body: a pattern rule
/// asserted there, or a query layer's changes applied there, is refused
/// with [`DialogCorpusError::Refused`] and nothing lands. A reader that
/// does not hold the body, another query of the same branch, sees no
/// trace of the rule, while the layer that holds it derives through it.
#[dialog_common::test]
async fn it_refuses_a_pattern_rule_into_the_session_overlay() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    seed(&branch, &session).await?;
    let rule = deductive()?;
    let refused = |error: &DialogCorpusError| {
        matches!(
            error,
            DialogCorpusError::Refused(refusal)
                if refusal.entity() == &rule.this()
                    && matches!(
                        refusal.reason::<TypeError>(),
                        Some(TypeError::UnstorablePattern { .. })
                    )
        )
    };

    let before = branch.overlay().revision();
    let Err(error) = branch.overlay().assert(&rule) else {
        panic!("the overlay refuses the rule");
    };
    assert!(refused(&error), "{error:?}");
    let layer = branch.with(&rule);
    let error = branch
        .overlay()
        .apply(layer.changes().clone())
        .expect_err("the overlay refuses a layer's changes");
    assert!(refused(&error), "{error:?}");
    assert_eq!(branch.overlay().revision(), before, "nothing landed");
    assert!(
        branch
            .overlay()
            .scan(&Selector::new().of(rule.this()))
            .is_empty()
    );

    // The layer holding the body derives the heading; a query of the
    // branch that does not hold it derives nothing.
    let held = layer.select(headings()).perform(&session).try_vec().await?;
    assert_eq!(held.len(), 1);
    let other = branch
        .query()
        .select(headings())
        .perform(&session)
        .try_vec()
        .await?;
    assert!(other.is_empty(), "no other reader holds the rule");
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

/// `note/payload` of `?this` matches `{"title" ?heading ..}`: a query that
/// holds a pattern, run by itself.
fn titled() -> Result<ConceptQuery> {
    let Premise::Assert(Proposition::Concept(query)) = titled_premise()? else {
        anyhow::bail!("a concept premise");
    };
    Ok(query)
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

/// A subscription to a query that holds a pattern is kept current: a
/// commit that adds a value the pattern matches is asserted, one that
/// changes the part the pattern binds is retracted and asserted, and one
/// that changes the value to no longer match is retracted.
///
/// The rule is not what is subscribed: a subscription reads a branch or
/// a stack, and neither can hold a rule with a pattern, so the pattern
/// is in the query.
#[dialog_common::test]
async fn it_keeps_a_subscription_to_a_pattern_current() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let branch = repo.branch("main").open().perform(&session).await?;
    seed(&branch, &session).await?;
    let milk: Entity = "id:milk".parse()?;
    let call: Entity = "id:call".parse()?;
    let payload =
        |title: &str| -> Result<Composite> { Ok(Composite::map([(text("title"), text(title))])?) };
    let write = |of: Entity, is: Composite| {
        branch
            .transaction()
            .assert(the!("note/payload").of(of).is(is))
            .commit()
            .publish()
    };

    let mut subscription = branch.subscribe(titled()?);
    let first = subscription.poll(&session).await?.expect("initial");
    assert_eq!(
        titles(&first.asserted),
        vec![(milk.clone(), text("Buy milk"))]
    );

    #[allow(deprecated)]
    write(call.clone(), payload("Call mum")?)
        .perform(&session)
        .await?;
    let added = subscription.poll(&session).await?.expect("a match added");
    assert_eq!(
        titles(&added.asserted),
        vec![(call.clone(), text("Call mum"))]
    );
    assert!(added.retracted.is_empty());

    // A payload with an entry the pattern does not name still matches:
    // the blank rest takes it.
    let with_done = Composite::map([
        (text("title"), text("Call dad")),
        (text("done"), Value::Boolean(true)),
    ])?;
    let replaced = branch
        .transaction()
        .retract(
            the!("note/payload")
                .of(call.clone())
                .is(payload("Call mum")?),
        )
        .assert(the!("note/payload").of(call.clone()).is(with_done.clone()));
    #[allow(deprecated)]
    replaced.commit().publish().perform(&session).await?;
    let moved = subscription.poll(&session).await?.expect("the title moved");
    assert_eq!(
        titles(&moved.retracted),
        vec![(call.clone(), text("Call mum"))]
    );
    assert_eq!(
        titles(&moved.asserted),
        vec![(call.clone(), text("Call dad"))]
    );

    // A payload with no title is a value the pattern does not match.
    let untitled = branch
        .transaction()
        .retract(the!("note/payload").of(call.clone()).is(with_done))
        .assert(
            the!("note/payload")
                .of(call.clone())
                .is(Composite::map([(text("note"), text("later"))])?),
        );
    #[allow(deprecated)]
    untitled.commit().publish().perform(&session).await?;
    let gone = subscription.poll(&session).await?.expect("the match left");
    assert_eq!(titles(&gone.retracted), vec![(call, text("Call dad"))]);
    assert!(gone.asserted.is_empty());
    Ok(())
}

/// A standing fold whose inputs hold a pattern counts, per entity with a
/// title, the payloads that match it, and a commit that adds a match
/// reports the count that started to hold and the one that stopped.
#[dialog_common::test]
async fn it_keeps_a_fold_over_a_pattern_current() -> Result<()> {
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
        .commit()
        .publish()
        .perform(&session)
        .await?;

    let mut inputs = titled()?;
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

    #[allow(deprecated)]
    branch
        .transaction()
        .assert(
            the!("note/payload")
                .of(milk.clone())
                .is(Composite::map([(text("title"), text("Buy milk"))])?),
        )
        .commit()
        .publish()
        .perform(&session)
        .await?;
    let delta = subscription.poll(&session).await?.expect("a match added");
    assert_eq!(
        count(&delta.asserted),
        vec![(milk.clone(), Value::UnsignedInt(1))]
    );
    assert_eq!(count(&delta.retracted), vec![(milk, Value::UnsignedInt(0))]);
    Ok(())
}
