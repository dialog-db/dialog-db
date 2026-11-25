//! A rule whose head declares a type its body binds none of: refused
//! when it compiles and when it is installed, and inert where a replica
//! already holds it. Run on the volatile and the filesystem providers.
//!
//! The stored rule is the one `main` at `f8f2655` installed
//! (`transaction/fixtures/main-f8f2655.json`), written past the check
//! this release makes, as a replica that ran that release holds it.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::{Entity, Relation as CorpusRelation, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::concept::query::ConceptQuery;
use dialog_query::query::Output as _;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{
    AttributeStatement, Cardinality, DeductiveRule, Parameters, Relation, Term, TypeError,
};
use futures_util::TryStreamExt as _;
use serde_json::{Value as Json, json};

use crate::helpers::test_repo;
use crate::{Branch, Repository};

fn fixture() -> Json {
    serde_json::from_str(include_str!("transaction/fixtures/main-f8f2655.json"))
        .expect("the fixture parses")
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&text[at..at + 2], 16).expect("hex"))
        .collect()
}

fn value(json: &Json) -> Value {
    if let Some(bytes) = json.get("bytes").and_then(Json::as_str) {
        return Value::Bytes(unhex(bytes));
    }
    if let Some(entity) = json.get("entity").and_then(Json::as_str) {
        return Value::Entity(entity.parse().expect("an entity"));
    }
    panic!("not a captured value: {json}")
}

/// The rule's facts, as the release installed them.
fn installed_by_release() -> Vec<AttributeStatement> {
    fixture()["rules"]["text-into-integer"]["facts"]
        .as_array()
        .expect("captured facts")
        .iter()
        .map(|fact| AttributeStatement {
            the: Relation::from(
                fact["the"]
                    .as_str()
                    .expect("an attribute")
                    .parse::<CorpusRelation>()
                    .expect("an attribute"),
            ),
            of: fact["of"]
                .as_str()
                .expect("an entity")
                .parse()
                .expect("an entity"),
            is: value(&fact["is"]),
            cause: None,
            cardinality: Some(Cardinality::Many),
        })
        .collect()
}

/// The entity the release stored the rule under.
fn stored_by_release() -> Entity {
    fixture()["rules"]["text-into-integer"]["entity"]
        .as_str()
        .expect("captured")
        .parse()
        .expect("an entity")
}

/// The body the release stored.
fn source_from_release() -> Vec<u8> {
    installed_by_release()
        .into_iter()
        .find(|fact| fact.the.to_string() == "dialog.rule/source")
        .map(|fact| match fact.is {
            Value::Bytes(bytes) => bytes,
            other => panic!("a source is bytes, not {other:?}"),
        })
        .expect("a source fact")
}

/// The rule the release stored, as this release reads its descriptor.
fn descriptor() -> Result<DeductiveRuleDescriptor> {
    Ok(serde_json::from_value(json!({
        "deduce": { "with": { "value": { "the": "note/body", "as": "integer:", "cardinality": "many" } } },
        "when": [{
            "assert": { "with": { "value": { "the": "source/body", "as": "text:", "cardinality": "many" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?)
}

/// Whether `reason` names the head's attribute, its declared type and
/// the type the body binds. A stored body spells the head field
/// canonically (`~0`), so the attribute is what names it to a reader.
fn names_the_mismatch(reason: &str) -> bool {
    reason.contains("note/body") && reason.contains("SignedInt") && reason.contains("String")
}

/// Commits `statements` to `branch`.
async fn commit<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    statements: Vec<AttributeStatement>,
) -> Result<()> {
    #[allow(deprecated)]
    let mut transaction = branch.transaction();
    for statement in statements {
        transaction = transaction.assert(statement);
    }
    #[allow(deprecated)]
    transaction.commit().publish().perform(session).await?;
    Ok(())
}

/// A text under `source/body`, which the rule would read, and the
/// integer `5` stored under `note/body` itself.
fn seeded() -> Result<Vec<AttributeStatement>> {
    let statement = |the: &str, is: Value| -> Result<AttributeStatement> {
        Ok(AttributeStatement {
            the: Relation::from(the.parse::<CorpusRelation>()?),
            of: Entity::new()?,
            is,
            cause: None,
            cardinality: None,
        })
    };
    Ok(vec![
        statement("source/body", Value::String("hello".into()))?,
        statement("note/body", Value::SignedInt(5))?,
    ])
}

/// What a read of `note/body` with no type answers on `branch`.
async fn bodies<S: PeerSpace>(branch: &Branch, session: &Peer<S, Session>) -> Result<Vec<Value>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("value".into(), Term::var("value"));
    let rows = branch
        .query()
        .select(ConceptQuery {
            predicate: serde_json::from_value(json!({ "with": {
                "value": { "the": "note/body", "cardinality": "many" }
            }}))?,
            terms,
        })
        .perform(session)
        .try_vec()
        .await?;
    Ok(rows
        .iter()
        .filter_map(|row| row.source().value_of("value").cloned())
        .collect())
}

/// The rule bodies `branch` does not read, as `dialog.rule/invalid`
/// answers: each rule's entity and why.
async fn invalid<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
) -> Result<Vec<(Entity, String)>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("reason".into(), Term::var("reason"));
    let rows = branch
        .query()
        .select(ConceptQuery {
            predicate: serde_json::from_value(json!({ "with": {
                "reason": { "the": "dialog.rule/invalid", "as": "text:" }
            }}))?,
            terms,
        })
        .perform(session)
        .try_vec()
        .await?;
    Ok(rows
        .iter()
        .filter_map(|row| match row.source().value_of("reason")? {
            Value::String(reason) => Some((row.entity().clone(), reason.clone())),
            _ => None,
        })
        .collect())
}

/// What `branch` holds of the rule's entity.
async fn held<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    name: &str,
    session: &Peer<S, Session>,
) -> Result<usize> {
    let branch = repo.branch(name).load().perform(session).await?;
    let facts: Vec<dialog_corpus::Fact> = branch
        .claims()
        .select(Selector::new().of(stored_by_release()))
        .to_owned()
        .perform(session)
        .await?
        .try_collect()
        .await?;
    Ok(facts.len())
}

/// On a branch loaded afresh: the rule derives nothing, a read of what
/// it would derive answers the stored value and does not fail, the rule
/// is listed as invalid with a reason naming the head field and both
/// types, and a commit succeeds.
async fn inert<S: PeerSpace>(
    repo: &Repository<dialog_credentials::Credential>,
    name: &str,
    session: &Peer<S, Session>,
    how: &str,
) -> Result<()> {
    let branch = repo.branch(name).load().perform(session).await?;
    let stored = |values: &[Value]| {
        !values.is_empty() && values.iter().all(|value| *value == Value::UnsignedInt(5))
    };
    let before = bodies(&branch, session).await?;
    assert!(
        stored(&before),
        "{how}: the read answers the stored values, derives no text and does not fail: {before:?}"
    );
    let listed = invalid(&branch, session).await?;
    assert!(
        matches!(
            listed.as_slice(),
            [(entity, reason)]
                if *entity == stored_by_release()
                    && reason.starts_with("undecodable: ")
                    && names_the_mismatch(reason)
        ),
        "{how}: the rule is listed as invalid with why: {listed:?}"
    );
    commit(&branch, session, seeded()?).await?;
    let after = bodies(&branch, session).await?;
    assert!(
        stored(&after) && after.len() == before.len() + 1,
        "{how}: a commit succeeds and the rule still derives nothing: {after:?}"
    );
    Ok(())
}

async fn scenario<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;

    // Compiling the rule is refused with the head field, its declared
    // type and the type the body binds, and so is decoding the body the
    // release stored.
    let refused = descriptor()?.compile();
    assert!(
        matches!(
            &refused,
            Err(error @ TypeError::HeadField { field, .. })
                if field == "value" && names_the_mismatch(&error.to_string())
        ),
        "compiling is refused: {:?}",
        refused.map(|rule| rule.this())
    );
    let decoded = DeductiveRule::decode(&source_from_release());
    assert!(
        matches!(&decoded, Err(reason) if names_the_mismatch(reason)),
        "decoding the stored body is refused: {:?}",
        decoded.map(|rule| rule.this())
    );

    // Installing it is refused before anything is written: only a
    // compiled rule can be asserted, and the branch holds nothing of it.
    let refusing = repo.branch("refusing").open().perform(session).await?;
    commit(&refusing, session, seeded()?).await?;
    assert_eq!(
        held(&repo, "refusing", session).await?,
        0,
        "nothing of the rule is written"
    );

    // A branch holding the rule as the release installed it.
    let holding = repo.branch("holding").open().perform(session).await?;
    commit(&holding, session, installed_by_release()).await?;
    commit(&holding, session, seeded()?).await?;
    assert!(
        held(&repo, "holding", session).await? > 0,
        "the rule is held"
    );
    inert(&repo, "holding", session, "loaded").await?;

    // Upgrading the branch's rules leaves it as it is, listed as
    // undecodable, and it stays inert.
    let upgrading = repo.branch("holding").load().perform(session).await?;
    let upgraded = upgrading.upgrade_rules().perform(session).await?;
    assert_eq!(
        upgraded.undecodable,
        vec![stored_by_release()],
        "the upgrade lists the rule as undecodable"
    );
    assert!(
        upgraded.reinstalled.is_empty() && upgraded.rewritten.is_empty(),
        "the upgrade rewrites nothing: {upgraded:?}"
    );
    inert(&repo, "holding", session, "upgraded").await?;

    // A branch that pulls the rule from it.
    let pulling = repo.branch("pulling").open().perform(session).await?;
    pulling.set_upstream(&holding).perform(session).await?;
    pulling
        .pull(crate::Download::MinimalRanges)
        .perform(session)
        .await?;
    assert!(
        held(&repo, "pulling", session).await? > 0,
        "the rule is pulled"
    );
    inert(&repo, "pulling", session, "pulled").await?;
    Ok(())
}

/// On the volatile provider.
#[dialog_common::test]
async fn a_rule_whose_head_its_body_cannot_bind_is_refused_and_inert_on_a_volatile_branch()
-> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    scenario(&session, &peer).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the browser
/// a branch is kept in IndexedDB and OPFS, and the web run of
/// `a_rule_whose_head_its_body_cannot_bind_is_refused_and_inert_on_a_volatile_branch`
/// pins the same scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn a_rule_whose_head_its_body_cannot_bind_is_refused_and_inert_on_a_filesystem_branch()
-> Result<()> {
    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    scenario(&session, &peer).await
}
