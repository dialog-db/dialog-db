//! A rule that holds a pattern has no stored body: committing one is
//! refused with [`CommitError::Refused`], carrying
//! [`TypeError::UnstorablePattern`] and the rule's entity, and nothing the
//! transaction holds is written. Run on the volatile and the filesystem
//! providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Changes, Composite, Entity, Fact, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::concept::query::ConceptQuery;
use dialog_query::error::TypeError;
use dialog_query::types::Any;
use dialog_query::{
    ConceptDescriptor, DeductiveRule, InductiveRule, MapPattern, Parameters, Pattern, Premise,
    Proposition, Term, the,
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
