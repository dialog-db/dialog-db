//! Every rule can be stored.
//!
//! A rule is a statement: asserted into a branch it is written as facts
//! (its body under `dialog.rule/source`, its conclusion and the
//! relations it reads beside it), and a branch loaded afresh reads the
//! rule back from them and derives through it. That needs a body and an
//! identity for every rule, and a rule whose body reads an attribute
//! directly (an attribute premise, which a concept's own rule reads its
//! fields by, and which a caller builds with `AttributeQuery`) has
//! neither today: the attribute premise has no stored spelling.
//!
//! Each case here builds such a rule, asserts it into a branch, loads
//! the branch afresh and reads the rule back: its body decodes to a rule
//! stored under its identity, and a deductive one derives what it did
//! before it was stored. Run on the volatile and the filesystem
//! providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::selector::Constrained;
use dialog_corpus::{Entity, Fact, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::attribute::query::AttributeQuery;
use dialog_query::query::Output as _;
use dialog_query::{
    ConceptConclusion, ConceptDescriptor, ConceptQuery, DeductiveRule, InductiveRule, Parameters,
    Premise, Term, the,
};
use futures_util::TryStreamExt as _;

#[cfg(not(target_arch = "wasm32"))]
use crate::helpers::filesystem_session_with_peer;
use crate::helpers::test_repo;
use crate::rules::source_attr;
use crate::{Branch, Repository};

/// A concept of one text field `name` under `the`.
fn named(the: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { "name": { "the": the, "as": "text:" } }
    }))
    .expect("a concept")
}

/// `person/name` of `?this` is `?name`, read by an attribute premise.
fn name_scan() -> Premise {
    AttributeQuery::new(
        Term::from(the!("person/name")),
        Term::var("this"),
        Term::var("name"),
        Term::blank(),
        None,
    )
    .into()
}

/// A deductive rule concluding `person.display/name` from `person/name`
/// read by an attribute premise.
fn display_rule() -> Result<DeductiveRule> {
    Ok(DeductiveRule::new(
        named("person.display/name"),
        vec![name_scan()],
    )?)
}

/// An inductive rule asserting `person.copy/name` from `person/name`
/// read by an attribute premise.
fn copy_rule() -> Result<InductiveRule> {
    Ok(InductiveRule::new(
        named("person.copy/name"),
        vec![name_scan()],
    )?)
}

fn alice() -> Entity {
    "id:alice".parse().expect("an entity")
}

/// Commits `person/name` of alice, "Alice".
async fn seed<S: PeerSpace>(branch: &Branch, session: &Peer<S, Session>) -> Result<()> {
    branch
        .transaction()
        .assert(the!("person/name").of(alice()).is("Alice".to_string()))
        .commit()
        .publish()
        .perform(session)
        .await?;
    Ok(())
}

/// The body `name`, loaded afresh, stores for the rule `rule`.
async fn stored_body<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    session: &Peer<S, Session>,
    rule: &Entity,
) -> Result<Vec<u8>> {
    let branch = repo.branch("main").load().perform(session).await?;
    let selector: Selector<Constrained> = Selector::new().of(rule.clone()).the(source_attr());
    let sources: Vec<Fact> = branch
        .claims()
        .select(selector)
        .to_owned()
        .perform(session)
        .await?
        .try_collect()
        .await?;
    let [source] = sources.as_slice() else {
        anyhow::bail!("one body is stored for the rule, found {}", sources.len());
    };
    let Value::Bytes(bytes) = &source.is else {
        anyhow::bail!("the body is bytes");
    };
    Ok(bytes.clone())
}

/// The names `the` holds by entity, as `name`, loaded afresh, reads the
/// concept `named(the)`.
async fn names<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    session: &Peer<S, Session>,
    the: &str,
) -> Result<Vec<(Entity, Value)>> {
    let branch = repo.branch("main").load().perform(session).await?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("name".into(), Term::var("name"));
    let rows: Vec<ConceptConclusion> = branch
        .query()
        .select(ConceptQuery {
            predicate: named(the),
            terms,
        })
        .perform(session)
        .try_vec()
        .await?;
    let mut names: Vec<(Entity, Value)> = rows
        .iter()
        .filter_map(|row| Some((row.entity().clone(), row.source().value_of("name")?.clone())))
        .collect();
    names.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(names)
}

/// A deductive rule reading an attribute directly is stored, reads back
/// from a branch loaded afresh under its identity, and derives through
/// it there.
async fn deductive_scenario<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let rule = display_rule()?;
    let identity = rule
        .try_this()
        .ok_or_else(|| anyhow::anyhow!("the rule reading an attribute directly has no identity"))?;
    assert!(
        rule.try_encode().is_some(),
        "the rule reading an attribute directly has a body"
    );

    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    seed(&branch, session).await?;
    branch
        .transaction()
        .assert(&rule)
        .commit()
        .publish()
        .perform(session)
        .await?;

    let body = stored_body(&repo, session, &identity).await?;
    assert!(
        DeductiveRule::decode(&body)
            .map_err(anyhow::Error::msg)?
            .stored_as(&identity),
        "the body reads back as the rule under its identity"
    );
    assert_eq!(
        names(&repo, session, "person.display/name").await?,
        vec![(alice(), Value::String("Alice".into()))],
        "the stored rule derives on a branch loaded afresh"
    );
    Ok(())
}

/// An inductive rule reading an attribute directly is stored, and reads
/// back from a branch loaded afresh under its identity.
async fn inductive_scenario<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let rule = copy_rule()?;
    let identity = rule
        .try_this()
        .ok_or_else(|| anyhow::anyhow!("the rule reading an attribute directly has no identity"))?;

    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    branch
        .transaction()
        .assert(rule)
        .commit()
        .publish()
        .perform(session)
        .await?;

    let body = stored_body(&repo, session, &identity).await?;
    assert!(
        InductiveRule::decode(&body)
            .map_err(anyhow::Error::msg)?
            .stored_as(&identity),
        "the body reads back as the rule under its identity"
    );
    Ok(())
}

/// A concept's own rule, which reads each field by an attribute premise,
/// is stored and reads back from a branch loaded afresh under its
/// identity.
async fn implicit_scenario<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let rule = DeductiveRule::from(&named("person/name"));
    let identity = rule
        .try_this()
        .ok_or_else(|| anyhow::anyhow!("a concept's own rule has no identity"))?;

    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    branch
        .transaction()
        .assert(&rule)
        .commit()
        .publish()
        .perform(session)
        .await?;

    let body = stored_body(&repo, session, &identity).await?;
    assert!(
        DeductiveRule::decode(&body)
            .map_err(anyhow::Error::msg)?
            .stored_as(&identity),
        "the body reads back as the rule under its identity"
    );
    Ok(())
}

#[dialog_common::test]
async fn it_stores_a_deductive_rule_reading_an_attribute_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    deductive_scenario(&session, &peer).await
}

#[dialog_common::test]
async fn it_stores_an_inductive_rule_reading_an_attribute_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    inductive_scenario(&session, &peer).await
}

#[dialog_common::test]
async fn it_stores_a_concepts_own_rule_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    implicit_scenario(&session, &peer).await
}

/// Native only: the filesystem provider is the native one. In the browser
/// a branch is kept in IndexedDB and OPFS, and the web run of
/// `it_stores_a_deductive_rule_reading_an_attribute_on_a_volatile_branch`
/// pins the same scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_stores_a_deductive_rule_reading_an_attribute_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    deductive_scenario(&session, &peer).await
}

/// Native only, as above; the web run of
/// `it_stores_an_inductive_rule_reading_an_attribute_on_a_volatile_branch`
/// pins the same scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_stores_an_inductive_rule_reading_an_attribute_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    inductive_scenario(&session, &peer).await
}

/// Native only, as above; the web run of
/// `it_stores_a_concepts_own_rule_on_a_volatile_branch` pins the same
/// scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_stores_a_concepts_own_rule_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    implicit_scenario(&session, &peer).await
}
