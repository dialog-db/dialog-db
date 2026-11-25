//! A rule that feeds itself under cardinality one.
//!
//! `xyz.n/v` is an integer of cardinality one. The rule reads `v` of an
//! entity and, while it is below a bound, asserts `v + 1` for it. One
//! commit asserts `v = 0`: each round of induction reads the value the
//! round before wrote and writes the next, so the commit counts up to the
//! bound in as many rounds, and a write under cardinality one succeeds
//! the claim it replaces, a claim an earlier round of the same commit
//! wrote included. The cell is left with one claim, the bound.
//!
//! Induction stops a commit whose rules still write after
//! [`MAX_ROUNDS`](super::MAX_ROUNDS) rounds. Counting to `bound` takes
//! `bound` rounds that write and one that finds nothing left to do, so
//! a bound of `MAX_ROUNDS - 1` is the largest a commit settles, and a
//! bound of `MAX_ROUNDS` fails it with `InductionDivergence`.
//!
//! Run on the volatile and the filesystem providers.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::MAX_ROUNDS;
#[cfg(not(target_arch = "wasm32"))]
use crate::helpers::filesystem_session_with_peer;
use crate::helpers::test_repo;
use crate::{Branch, CommitError};
use anyhow::Result;
use dialog_corpus::{Cardinality, Changes, Entity, Relation, Selector, Update, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::InductiveRule;
use futures_util::StreamExt as _;
use serde_json::{Value as Json, json};

fn var(name: &str) -> Json {
    json!({ "?": { "name": name } })
}

/// `xyz.n/v`, an integer of cardinality one.
fn v() -> Json {
    json!({ "the": "xyz.n/v", "as": "integer:", "cardinality": "one" })
}

fn relation() -> Relation {
    "xyz.n/v".parse().expect("a test attribute parses")
}

fn entity() -> Entity {
    "n:1".parse().expect("a test entity parses")
}

/// `assert! xyz.n{v: ?prev + 1} when xyz.n{this: ?this, v: ?prev},
/// ?prev < bound`.
fn count_up(bound: i64) -> Result<InductiveRule> {
    Ok(serde_json::from_value(json!({
        "assert!": { "with": { "v": v() } },
        "when": [
            {
                "assert": { "with": { "v": v() } },
                "where": { "this": var("this"), "v": var("prev") }
            },
            { "assert": "<", "where": { "of": var("prev"), "with": bound } },
            {
                "assert": "math/sum",
                "where": { "of": var("prev"), "with": 1, "is": var("v") }
            }
        ]
    }))?)
}

/// Every claim of `v` of the entity the branch holds.
async fn claims<S: PeerSpace>(branch: &Branch, session: &Peer<S, Session>) -> Result<Vec<Value>> {
    branch.refresh(session).await?;
    let selector = Selector::new().the(relation()).of(entity());
    let stream = branch.claims().select(selector).perform(session).await?;
    let facts: Vec<_> = stream.collect().await;
    facts
        .into_iter()
        .map(|item| Ok(item.and_then(|view| view.to_owned())?.is))
        .collect()
}

/// Installs the rule counting up to `bound`, then commits `v = 0`.
async fn count<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
    bound: i64,
) -> Result<(Branch, Result<(), CommitError>)> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    branch
        .transaction()
        .assert(count_up(bound)?)
        .commit()
        .publish()
        .perform(session)
        .await?;
    branch.refresh(session).await?;

    let mut zero = Changes::new();
    zero.associate(relation(), entity(), Value::SignedInt(0), Cardinality::One);
    let committed = branch
        .transaction()
        .assert(zero)
        .commit()
        .publish()
        .perform(session)
        .await
        .map(|_| ());
    Ok((branch, committed))
}

/// Counting up within the round bound leaves one claim, the bound.
async fn counts_to_the_bound<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    let largest = i64::from(MAX_ROUNDS) - 1;
    for bound in [3, 5, 8, largest] {
        let (branch, committed) = count(session, peer, bound).await?;
        committed?;
        assert_eq!(
            claims(&branch, session).await?,
            // A non-negative integer is stored as a natural.
            vec![Value::UnsignedInt(u128::try_from(bound)?)],
            "counting up to {bound} leaves one claim, {bound}"
        );
    }
    Ok(())
}

/// Counting past the round bound fails the commit, and writes nothing.
async fn fails_past_the_round_bound<S: PeerSpace>(
    session: &Peer<S, Session>,
    peer: &Peer<S>,
) -> Result<()> {
    for bound in [i64::from(MAX_ROUNDS), 20] {
        let (branch, committed) = count(session, peer, bound).await?;
        assert!(
            matches!(committed, Err(CommitError::InductionDivergence(_))),
            "counting up to {bound} fails with InductionDivergence, got {committed:?}"
        );
        assert_eq!(
            claims(&branch, session).await?,
            Vec::<Value>::new(),
            "the failed commit writes nothing"
        );
    }
    Ok(())
}

#[dialog_common::test]
async fn it_counts_to_the_bound_under_cardinality_one_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    counts_to_the_bound(&session, &peer).await
}

#[dialog_common::test]
async fn it_fails_past_the_round_bound_on_a_volatile_branch() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    fails_past_the_round_bound(&session, &peer).await
}

/// Native only: the filesystem provider is the native one. In the browser
/// a branch is kept in IndexedDB and OPFS, and the web run of
/// `it_counts_to_the_bound_under_cardinality_one_on_a_volatile_branch`
/// pins the same scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_counts_to_the_bound_under_cardinality_one_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    counts_to_the_bound(&session, &peer).await
}

/// Native only, as above; the web run of
/// `it_fails_past_the_round_bound_on_a_volatile_branch` pins the same
/// scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn it_fails_past_the_round_bound_on_a_filesystem_branch() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    fails_past_the_round_bound(&session, &peer).await
}
