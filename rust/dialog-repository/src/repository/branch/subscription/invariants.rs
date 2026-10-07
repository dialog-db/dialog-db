//! Invariants the pull request's subscription changes claim, pinned as
//! tests.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::test_repo;
use dialog_artifacts::Entity;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{Attribute, Concept, Query};

/// A badge number (`credential/badge`).
#[derive(Attribute, Clone, PartialEq)]
#[domain("credential")]
pub struct Badge(pub String);

/// Someone holding a badge.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct BadgeHolder {
    /// The badge holder entity.
    pub this: Entity,
    /// Their badge number.
    pub badge: Badge,
}

/// "Subscription demand records one `derives` slice per attribute of
/// the subscribed concept, so ... a rule landing on a subscribed one
/// does" wake it. Bob has a legacy code and no badge when the
/// subscription first polls. A rule deriving `credential/badge` from
/// the legacy code then lands, and the next poll asserts bob. The pull
/// request removed the one test that installed a rule after a first
/// poll.
#[dialog_common::test]
async fn a_rule_landing_on_a_subscribed_attribute_wakes_the_subscription() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let bob: Entity = "id:bob".parse()?;
    branch
        .transaction()
        .assert(Badge::of(alice.clone()).is("A-1"))
        .assert(
            dialog_query::the!("credential/legacy-code")
                .of(bob.clone())
                .is("B-2".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut subscription = branch.subscribe(Query::<BadgeHolder>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(
        initial.asserted,
        vec![BadgeHolder {
            this: alice.clone(),
            badge: Badge("A-1".into()),
        }]
    );

    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": {
            "badge": { "the": "credential/badge", "as": "Text" }
        }},
        "when": [{
            "assert": { "with": {
                "code": { "the": "credential/legacy-code", "as": "Text" }
            }},
            "where": {
                "this": { "?": { "name": "this" } },
                "code": { "?": { "name": "badge" } }
            }
        }]
    }))?;
    branch
        .transaction()
        .assert(&descriptor.compile()?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let delta = subscription
        .poll(&operator)
        .await?
        .expect("the rule's landing changes the result");
    assert_eq!(
        delta.asserted,
        vec![BadgeHolder {
            this: bob,
            badge: Badge("B-2".into()),
        }]
    );
    Ok(())
}
