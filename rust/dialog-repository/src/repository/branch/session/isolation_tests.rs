//! A query reads one revision: the one its lines stood at when it began.
//!
//! A branch's head lives in a memory cell every clone of the branch
//! shares, so a commit through one handle moves what another handle's
//! reads see. One query performs many selects on its environment (a
//! conjunction looks its second premise up per row of the first, a
//! resolver re-reads a reference per outer row), so a query environment
//! that takes a line's root afresh for each select reads some of the
//! query at one revision and the rest at another, and the query yields
//! rows no revision held. Reproduces dialog-db/dialog-db#757.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_artifacts::{ArtifactSelector, Entity, Relation};
use dialog_capability::Provider;
use dialog_effects::authority::Identify;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::{Attribute, Concept, Query};
use futures_util::TryStreamExt as _;

use super::{QueryEnv, Source};
use crate::helpers::test_repo;

/// A report's display name (`report/name`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("report")]
pub struct Name(pub String);

/// The two selects one query performs on its environment read the same
/// revision, whatever is committed to the branch between them: the one
/// the branch stood at when the query began.
#[dialog_common::test]
async fn a_query_reads_the_revision_it_started_at() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let report: Entity = "id:report".parse()?;
    branch
        .transaction()
        .assert(Name::of(report.clone()).is("Robin".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    // One query's environment, built as `SelectQuery::perform` builds it.
    let layer = branch.query();
    let identity = Identify.perform(&operator).await?;
    let overlay = layer.overlay(&identity);
    let sources: Vec<Source> = layer
        .sources
        .iter()
        .map(|source| source.to_source())
        .collect();
    let query = QueryEnv::new(sources, overlay, &operator);

    let name: Relation = "report/name".parse()?;
    let selector = ArtifactSelector::new().the(name).of(report.clone());
    let first = Provider::<dialog_artifacts::Select<'_>>::execute(&query, selector.clone())
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(first.len(), 1, "the query's first select sees the name");

    // Another handle of the same branch moves its head while the query
    // is under way.
    branch
        .transaction()
        .retract(Name::of(report.clone()).is("Robin".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let second = Provider::<dialog_artifacts::Select<'_>>::execute(&query, selector)
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    assert_eq!(
        second.len(),
        first.len(),
        "the query's second select read a revision its first did not"
    );
    Ok(())
}

/// Someone with a name: what the subscription below stands over.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct Named {
    /// The entity.
    pub this: Entity,
    /// Their name.
    pub name: Name,
}

/// Pinning is per evaluation, not per subscription: a standing query
/// reads one revision within a poll and the branch's current revision
/// at the next, so a commit through another handle of the branch is
/// picked up by the poll after it.
#[dialog_common::test]
async fn a_subscription_follows_the_branch_across_polls() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let report: Entity = "id:report".parse()?;
    branch
        .transaction()
        .assert(Name::of(report.clone()).is("Robin".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut standing = branch.subscribe(Query::<Named>::default());
    let first = standing
        .poll(&operator)
        .await?
        .expect("the first poll evaluates");
    assert_eq!(
        first
            .asserted
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Robin"]
    );
    assert!(
        standing.poll(&operator).await?.is_none(),
        "nothing moved, so nothing is re-evaluated"
    );

    // Another handle of the branch moves its head.
    branch
        .clone()
        .transaction()
        .retract(Name::of(report.clone()).is("Robin".to_string()))
        .assert(Name::of(report).is("Sam".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let second = standing.poll(&operator).await?.expect("the head moved");
    assert_eq!(
        second
            .asserted
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Sam"]
    );
    assert_eq!(
        second
            .retracted
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Robin"]
    );
    assert_eq!(
        standing
            .results()
            .iter()
            .map(|row| row.name.0.as_str())
            .collect::<Vec<_>>(),
        ["Sam"],
        "the subscription stands at the branch's current revision"
    );
    Ok(())
}
