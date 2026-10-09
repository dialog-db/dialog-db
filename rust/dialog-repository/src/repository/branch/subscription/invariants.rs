//! Invariants the pull request's subscription changes claim, pinned as
//! tests.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::test_repo;
use dialog_artifacts::Entity;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{Attribute, Concept, Query, The};

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

/// A value the relation `x/a` derives (`x/a(e) := v :- x/src1(e) = v`,
/// the same from `x/src2` and `x/src3`, and `x/a(e) := v :- x/a(e) =
/// v`, which makes the relation recursive).
#[derive(Attribute, Clone, PartialEq)]
#[domain("x")]
pub struct A(pub String);

/// An entity's `x/a`, read under `last`.
#[derive(Concept, Debug, Clone, PartialEq)]
pub struct HeldA {
    /// The entity.
    pub this: Entity,
    /// Its elected `x/a`.
    pub a: A,
}

/// "A derived value stands by the fact that bound it", kept by a
/// subscription that maintains the relation's fixpoint across polls
/// rather than rebuilding it. `v` is derived from `x/src1` (oldest) and
/// later from `x/src3` too; `w` from `x/src2`, in between. A poll after
/// `x/src3` lands reads `v`: the known row stands newer than before. A
/// poll after `x/src3` is retracted reads `w`: `v` keeps its older
/// derivation, and stands as that one again.
#[dialog_common::test]
async fn a_maintained_fixpoint_keeps_each_row_at_its_newest_surviving_derivation()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let e: Entity = "id:e".parse()?;
    let rule = |from: &str| -> anyhow::Result<_> {
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "a": { "the": "x/a", "as": "Text" } } },
            "when": [{
                "assert": { "with": { "v": { "the": from, "as": "Text" } } },
                "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "a" } } }
            }]
        }))?;
        Ok(descriptor.compile()?)
    };
    let source = |attribute: &str, value: &str| {
        attribute
            .parse::<The>()
            .expect("attribute")
            .of(e.clone())
            .is(value.to_string())
    };
    branch
        .transaction()
        .assert(&rule("x/src1")?)
        .assert(&rule("x/src2")?)
        .assert(&rule("x/src3")?)
        .assert(&rule("x/a")?)
        .assert(source("x/src1", "v"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch
        .transaction()
        .assert(source("x/src2", "w"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let held = |value: &str| HeldA {
        this: e.clone(),
        a: A(value.into()),
    };
    let mut subscription = branch.subscribe(Query::<HeldA>::default());
    let initial = subscription.poll(&operator).await?.expect("initial");
    assert_eq!(initial.asserted, vec![held("w")], "w stands newest");

    branch
        .transaction()
        .assert(source("x/src3", "v"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let raised = subscription
        .poll(&operator)
        .await?
        .expect("v now stands newest");
    assert_eq!(raised.asserted, vec![held("v")]);
    assert_eq!(raised.retracted, vec![held("w")]);

    branch
        .transaction()
        .retract(source("x/src3", "v"))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let lowered = subscription
        .poll(&operator)
        .await?
        .expect("v falls back to its older derivation");
    assert_eq!(lowered.asserted, vec![held("w")]);
    assert_eq!(lowered.retracted, vec![held("v")]);
    assert!(
        subscription.maintenances() >= 1,
        "the fixpoint was maintained across a poll, not only rebuilt"
    );
    Ok(())
}
