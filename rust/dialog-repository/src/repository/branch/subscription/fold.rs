//! A query that folds, read through a branch: selected once, and under a
//! subscription.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::collections::BTreeMap;

use crate::helpers::test_repo;
use dialog_corpus::{Entity, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::attribute::Relation;
use dialog_query::reduce::{Aggregator, Reduce, ReduceEntry};
use dialog_query::types::Any;
use dialog_query::{AttributeQuery, Conclusion, Fold, FoldConclusion, Output as _, Term, the};

fn read(the: Term<Relation>, is: &str, cause: &str) -> AttributeQuery {
    AttributeQuery::new(
        the,
        Term::<Entity>::var("t"),
        Term::<Any>::var(is),
        Term::var(cause),
        None,
    )
}

/// Each to-do with a title, and how many tags it has.
fn counted() -> Fold<AttributeQuery, AttributeQuery> {
    Fold::new(
        read(Term::from(the!("todo/title")), "title", "c1"),
        read(Term::from(the!("todo/tag")), "g", "c2"),
        Reduce::new(
            vec!["t".into()],
            vec![ReduceEntry::new("tags", Aggregator::Count, Term::var("g"))],
        ),
    )
}

fn counts(rows: &[FoldConclusion]) -> BTreeMap<Entity, Value> {
    rows.iter()
        .filter_map(|row| Some((row.this().clone(), row.get("tags")?.clone())))
        .collect()
}

/// Selected through a branch, a fold gives each group, and 0 for the
/// to-do with no tag; under a subscription a tag added and a tag taken
/// away each report the count that started to hold and the one that
/// stopped. The fold cannot be restricted to one to-do, so each poll
/// asks it again whole.
#[dialog_common::test]
async fn it_counts_through_a_branch_and_under_a_subscription() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let one: Entity = "id:todo-1".parse()?;
    let two: Entity = "id:todo-2".parse()?;
    branch
        .transaction()
        .assert(the!("todo/title").of(one.clone()).is("Buy milk".to_string()))
        .assert(the!("todo/tag").of(one.clone()).is("home".to_string()))
        .assert(the!("todo/title").of(two.clone()).is("Call mum".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let selected = branch
        .query()
        .select(counted())
        .perform(&operator)
        .try_vec()
        .await?;
    assert_eq!(
        counts(&selected),
        BTreeMap::from([
            (one.clone(), Value::UnsignedInt(1)),
            (two.clone(), Value::UnsignedInt(0)),
        ])
    );

    let mut subscription = branch.subscribe(counted());
    subscription.poll(&operator).await?.expect("initial");
    let before = subscription.recomputes();

    branch
        .transaction()
        .assert(the!("todo/tag").of(two.clone()).is("call".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a tag added");
    assert_eq!(
        counts(&delta.asserted),
        BTreeMap::from([(two.clone(), Value::UnsignedInt(1))])
    );
    assert_eq!(
        counts(&delta.retracted),
        BTreeMap::from([(two.clone(), Value::UnsignedInt(0))])
    );

    branch
        .transaction()
        .retract(the!("todo/tag").of(one.clone()).is("home".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let delta = subscription.poll(&operator).await?.expect("a tag taken away");
    assert_eq!(
        counts(&delta.asserted),
        BTreeMap::from([(one.clone(), Value::UnsignedInt(0))])
    );
    assert_eq!(
        counts(&delta.retracted),
        BTreeMap::from([(one.clone(), Value::UnsignedInt(1))])
    );
    assert_eq!(subscription.recomputes(), before + 2);
    Ok(())
}
