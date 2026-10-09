//! Which rule a cycle through negation sets aside on a branch.
//!
//! Two rules close a cycle through an absence test:
//! `test/p(x) := base :- test/base(x) = base, unless test/q(x) = true` and
//! `test/q(x) := p :- test/p(x) = p`. Either alone is fine; together
//! `p` holds exactly when it does not. The rule installed last is set
//! aside, since every read was well defined before it arrived, and
//! replicas that installed the two concurrently set aside the same one.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::Branch;
use crate::helpers::test_repo;
use anyhow::Result;
use dialog_artifacts::Entity;
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::query::Output as _;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::types::Any;
use dialog_query::{ConceptDescriptor, ConceptQuery, DeductiveRule, Parameters, Term};
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

/// `test/p(x) := base :- test/base(x) = base, unless test/q(x) = true`.
fn negating() -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "p": { "the": "test/p", "as": "boolean:" } } },
        "when": [{
            "assert": { "with": { "base": { "the": "test/base", "as": "boolean:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "base": { "?": { "name": "p" } }
            }
        }],
        "unless": [{
            "assert": { "with": { "q": { "the": "test/q", "as": "boolean:" } } },
            "where": { "this": { "?": { "name": "this" } }, "q": true }
        }]
    }))?;
    Ok(descriptor.compile()?)
}

/// `test/q(x) := p :- test/p(x) = p`.
fn closing() -> Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "q": { "the": "test/q", "as": "boolean:" } } },
        "when": [{
            "assert": { "with": { "p": { "the": "test/p", "as": "boolean:" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "p": { "?": { "name": "q" } }
            }
        }]
    }))?;
    Ok(descriptor.compile()?)
}

/// The values `branch` reads for `test/<name>` of `of`.
async fn read(branch: &Branch, operator: &Operator, of: &Entity, name: &str) -> Result<Vec<bool>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        name: { "the": format!("test/{name}"), "as": "boolean:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
    terms.insert(name.to_string(), Term::<Any>::var(name));
    let rows = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut values: Vec<bool> = rows
        .iter()
        .map(|row| row.get::<bool>(name))
        .collect::<Result<_, _>>()?;
    values.sort();
    Ok(values)
}

/// Commits `rule` on `branch`.
async fn install(branch: &Branch, operator: &Operator, rule: DeductiveRule) -> Result<()> {
    branch
        .transaction()
        .assert(rule)
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(())
}

/// A branch holding `test/base(alice) = true`, and alice.
async fn seeded(branch: &Branch, operator: &Operator) -> Result<Entity> {
    let alice: Entity = "id:alice".parse()?;
    branch
        .transaction()
        .assert(dialog_query::the!("test/base").of(alice.clone()).is(true))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok(alice)
}

/// Installing the closing rule after the negating one sets the closing
/// rule aside: `p` reads as it did before, and nothing derives `q`.
#[dialog_common::test]
async fn it_sets_aside_the_closing_rule_installed_last() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice = seeded(&branch, &operator).await?;

    install(&branch, &operator, negating()?).await?;
    assert_eq!(read(&branch, &operator, &alice, "p").await?, vec![true]);

    install(&branch, &operator, closing()?).await?;
    assert_eq!(
        read(&branch, &operator, &alice, "p").await?,
        vec![true],
        "p reads as before the closing rule arrived"
    );
    assert_eq!(
        read(&branch, &operator, &alice, "q").await?,
        Vec::<bool>::new(),
        "the closing rule is set aside"
    );
    Ok(())
}

/// Installing the negating rule after the closing one sets the negating
/// rule aside: nothing derives `p`, so nothing derives `q`, as before.
#[dialog_common::test]
async fn it_sets_aside_the_negating_rule_installed_last() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice = seeded(&branch, &operator).await?;

    install(&branch, &operator, closing()?).await?;
    install(&branch, &operator, negating()?).await?;
    assert_eq!(
        read(&branch, &operator, &alice, "p").await?,
        Vec::<bool>::new(),
        "the negating rule is set aside"
    );
    assert_eq!(
        read(&branch, &operator, &alice, "q").await?,
        Vec::<bool>::new()
    );
    Ok(())
}

/// Two replicas each install one rule of the cycle without seeing the
/// other. After they merge, both set aside the same rule, so they read
/// the same values.
#[dialog_common::test]
async fn replicas_installing_a_cycle_concurrently_set_aside_the_same_rule() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let a = repo.branch("a").open().perform(&operator).await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    let alice = seeded(&a, &operator).await?;
    b.pull().from(&a).perform(&operator).await?;

    install(&a, &operator, negating()?).await?;
    install(&b, &operator, closing()?).await?;
    let mut quiesced = false;
    for _ in 0..4 {
        let pulled_a = a.pull().from(&b).perform(&operator).await?;
        let pulled_b = b.pull().from(&a).perform(&operator).await?;
        if pulled_a.is_none() && pulled_b.is_none() {
            quiesced = true;
            break;
        }
    }
    assert!(quiesced, "mutual pulls reach a fixed point");

    let on_a = (
        read(&a, &operator, &alice, "p").await?,
        read(&a, &operator, &alice, "q").await?,
    );
    let on_b = (
        read(&b, &operator, &alice, "p").await?,
        read(&b, &operator, &alice, "q").await?,
    );
    assert_eq!(on_a, on_b, "both replicas set aside the same rule");
    assert!(
        on_a == (vec![true], vec![]) || on_a == (vec![], vec![]),
        "exactly one rule of the cycle is set aside: {on_a:?}"
    );
    Ok(())
}

/// The rules `branch` sets aside, read as `dialog.rule/quarantined`.
async fn quarantined(branch: &Branch, operator: &Operator) -> Result<Vec<Entity>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({ "with": {
        "concept": { "the": "dialog.rule/quarantined", "as": "entity:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert("concept".to_string(), Term::<Any>::var("concept"));
    let rows = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut rules: Vec<Entity> = rows
        .iter()
        .map(|row| row.get::<Entity>("this"))
        .collect::<Result<_, _>>()?;
    rules.sort();
    Ok(rules)
}

/// A query reads which rules are set aside: none while the rules are
/// well defined, the rule installed last once it closes the cycle, and
/// none again once that rule is retracted.
#[dialog_common::test]
async fn it_reads_the_rules_it_sets_aside() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    seeded(&branch, &operator).await?;

    install(&branch, &operator, negating()?).await?;
    assert_eq!(quarantined(&branch, &operator).await?, Vec::<Entity>::new());

    let closing = closing()?;
    install(&branch, &operator, closing.clone()).await?;
    assert_eq!(
        quarantined(&branch, &operator).await?,
        vec![closing.this()],
        "the closing rule is read as set aside"
    );

    branch
        .transaction()
        .retract(&closing)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    assert_eq!(
        quarantined(&branch, &operator).await?,
        Vec::<Entity>::new(),
        "retracting the closing rule lifts its quarantine"
    );
    Ok(())
}
