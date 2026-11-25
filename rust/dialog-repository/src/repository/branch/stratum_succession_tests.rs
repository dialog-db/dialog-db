//! A cardinality-one write succeeds only the claims of its own stratum,
//! on the volatile provider and on the filesystem provider.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::{Entity, Relation as CorpusRelation, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::{AttributeStatement, Cardinality, Relation};
use futures_util::TryStreamExt as _;

use crate::helpers::test_repo;

/// A natural, then a text, then a float, each written under cardinality
/// one in a commit of its own, to one cell: the text stands beside the
/// float, which succeeded the natural, numbers being one stratum.
async fn scenario<S: PeerSpace>(session: &Peer<S, Session>, peer: &Peer<S>) -> Result<()> {
    let repo = test_repo(session, peer).await;
    let branch = repo.branch("main").open().perform(session).await?;
    let of = Entity::new()?;
    let the = Relation::from("note/value".parse::<CorpusRelation>()?);
    for is in [
        Value::UnsignedInt(3),
        Value::String("hello".into()),
        Value::Float(2.5),
    ] {
        #[allow(deprecated)]
        let transaction = branch.transaction().assert(AttributeStatement {
            the: the.clone(),
            of: of.clone(),
            is,
            cause: None,
            cardinality: Some(Cardinality::One),
        });
        #[allow(deprecated)]
        transaction.commit().publish().perform(session).await?;
    }
    let branch = repo.branch("main").load().perform(session).await?;
    let facts: Vec<dialog_corpus::Fact> = branch
        .claims()
        .select(Selector::new().of(of))
        .to_owned()
        .perform(session)
        .await?
        .try_collect()
        .await?;
    let mut held: Vec<String> = facts.iter().map(|fact| format!("{:?}", fact.is)).collect();
    held.sort();
    assert_eq!(
        held,
        vec![
            format!("{:?}", Value::Float(2.5)),
            format!("{:?}", Value::String("hello".into())),
        ]
    );
    Ok(())
}

/// On the volatile provider.
#[dialog_common::test]
async fn a_cardinality_one_write_succeeds_only_its_own_stratum_on_a_volatile_branch() -> Result<()>
{
    let (session, peer) = test_session_with_peer().await;
    scenario(&session, &peer).await
}

/// On the filesystem provider.
///
/// Native only: the filesystem provider is the native one. In the browser
/// a branch is kept in IndexedDB and OPFS, and the web run of
/// `a_cardinality_one_write_succeeds_only_its_own_stratum_on_a_volatile_branch`
/// pins the same scenario there.
#[cfg(not(target_arch = "wasm32"))]
#[dialog_common::test]
async fn a_cardinality_one_write_succeeds_only_its_own_stratum_on_a_filesystem_branch() -> Result<()>
{
    let root = tempfile::tempdir()?;
    use crate::helpers::filesystem_session_with_peer;

    let (session, peer) = filesystem_session_with_peer(root.path()).await?;
    scenario(&session, &peer).await
}
