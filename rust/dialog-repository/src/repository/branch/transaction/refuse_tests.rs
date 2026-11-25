//! A commit refuses a rule body that does not compile here.
//!
//! A rule installed through the API compiles before it is written. A
//! transaction can also write `dialog.rule/source` as a plain fact, and
//! such a body, when it is no rule this release reads as it stands under
//! its entity, is refused at commit with
//! [`CommitError::InvalidRule`](crate::CommitError::InvalidRule), and the
//! commit writes nothing. A body that arrives from elsewhere (a replica on
//! another release, another implementation) is not refused: it is inert
//! where it is read, and answered under `dialog.rule/error`.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::test_repo;
use crate::{Branch, CommitError, RuleError};
use anyhow::Result;
use dialog_corpus::{Cardinality, Changes, Entity, Selector, Update as _, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, Session};
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::StreamExt as _;
use serde_json::json;

type Operator = Peer<VolatileSpace, Session>;

/// `doc/tag(x) := label :- doc/label(x) = label`, its body.
fn body() -> Result<Vec<u8>> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "tag": { "the": "doc/tag", "as": "text:" } } },
        "when": [{
            "assert": { "with": { "label": { "the": "doc/label", "as": "text:" } } },
            "where": { "this": { "?": { "name": "this" } }, "label": { "?": { "name": "tag" } } }
        }]
    }))?;
    Ok(descriptor.compile()?.encode())
}

/// A body that decodes and does not compile: an `integer:` head whose
/// variable the body binds from a `text:` field.
fn ill_typed() -> Result<Vec<u8>> {
    Ok(serde_ipld_dagcbor::to_vec(&json!({
        "format": 4,
        "rule": {
            "deduce": { "with": { "n": { "the": "org/n", "as": "integer:" } } },
            "when": [{
                "assert": { "with": { "label": { "the": "doc/label", "as": "text:" } } },
                "where": { "this": { "?": { "name": "this" } }, "label": { "?": { "name": "n" } } }
            }]
        }
    }))?)
}

/// A branch holding one plain fact.
async fn branch() -> Result<(Operator, Branch)> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(
            dialog_query::the!("doc/label")
                .of("doc:1".parse::<Entity>()?)
                .is("draft".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    Ok((operator, branch))
}

/// Commits `bytes` as the body of `rule` beside a plain fact, through a
/// transaction.
async fn write(
    branch: &Branch,
    operator: &Operator,
    rule: &Entity,
    bytes: Vec<u8>,
) -> Result<(), CommitError> {
    let mut changes = Changes::new();
    changes.associate(
        "dialog.rule/source".parse().expect("an attribute"),
        rule.clone(),
        Value::Bytes(bytes),
        Cardinality::Many,
    );
    changes.associate(
        "doc/label".parse().expect("an attribute"),
        "doc:2".parse().expect("an entity"),
        Value::String("final".into()),
        Cardinality::Many,
    );
    branch
        .transaction()
        .assert(changes)
        .commit()
        .publish()
        .perform(operator)
        .await
        .map(|_| ())
}

/// The bodies `branch` stores, and whether `doc:2` was written.
async fn written(branch: &Branch, operator: &Operator) -> Result<(usize, bool)> {
    branch.refresh(operator).await?;
    let count = |selector| async move {
        let rows: Vec<_> = branch
            .claims()
            .select(selector)
            .perform(operator)
            .await?
            .collect()
            .await;
        Ok::<_, anyhow::Error>(rows.len())
    };
    let bodies = count(Selector::new().the("dialog.rule/source".parse()?)).await?;
    let beside = count(Selector::new().of("doc:2".parse()?)).await?;
    Ok((bodies, beside > 0))
}

/// Bytes that are no body are refused as undecodable, and nothing of the
/// commit is written.
#[dialog_common::test]
async fn it_refuses_bytes_that_are_no_rule_body() -> Result<()> {
    let (operator, branch) = branch().await?;
    let before = branch.revision();
    let refused = write(
        &branch,
        &operator,
        &"rule:garbage".parse()?,
        vec![0xff, 0x00],
    )
    .await;
    assert!(
        matches!(
            &refused,
            Err(CommitError::InvalidRule {
                reason: RuleError::Undecodable { .. },
                ..
            })
        ),
        "{refused:?}"
    );
    assert_eq!(branch.revision(), before, "no revision is published");
    assert_eq!(
        written(&branch, &operator).await?,
        (0, false),
        "nothing is written"
    );
    Ok(())
}

/// A rule's body stored under an entity it does not hash to is refused
/// as forged, and nothing of the commit is written.
#[dialog_common::test]
async fn it_refuses_a_forged_rule_body() -> Result<()> {
    let (operator, branch) = branch().await?;
    let forged: Entity = "rule:forged".parse()?;
    let refused = write(&branch, &operator, &forged, body()?).await;
    assert!(
        matches!(
            &refused,
            Err(CommitError::InvalidRule { rule, reason: RuleError::Forged }) if *rule == forged
        ),
        "{refused:?}"
    );
    assert_eq!(
        written(&branch, &operator).await?,
        (0, false),
        "nothing is written"
    );
    Ok(())
}

/// A body that decodes and does not type-check is refused as ill-typed,
/// and nothing of the commit is written.
#[dialog_common::test]
async fn it_refuses_an_ill_typed_rule_body() -> Result<()> {
    let (operator, branch) = branch().await?;
    let refused = write(&branch, &operator, &"rule:ill-typed".parse()?, ill_typed()?).await;
    assert!(
        matches!(
            &refused,
            Err(CommitError::InvalidRule {
                reason: RuleError::IllTyped { .. },
                ..
            })
        ),
        "{refused:?}"
    );
    assert_eq!(
        written(&branch, &operator).await?,
        (0, false),
        "nothing is written"
    );
    Ok(())
}

/// A body in a format newer than this release reads is refused, and
/// nothing of the commit is written.
#[dialog_common::test]
async fn it_refuses_a_rule_body_in_a_newer_format() -> Result<()> {
    let (operator, branch) = branch().await?;
    let newer = serde_ipld_dagcbor::to_vec(&json!({ "format": 99 }))?;
    let refused = write(&branch, &operator, &"rule:newer".parse()?, newer).await;
    assert!(
        matches!(
            &refused,
            Err(CommitError::InvalidRule {
                reason: RuleError::Newer { format: 99 },
                ..
            })
        ),
        "{refused:?}"
    );
    assert_eq!(
        written(&branch, &operator).await?,
        (0, false),
        "nothing is written"
    );
    Ok(())
}

/// A rule's own body under its own entity is written as a plain fact.
#[dialog_common::test]
async fn it_writes_a_rule_body_stored_under_its_identity() -> Result<()> {
    let (operator, branch) = branch().await?;
    let bytes = body()?;
    let rule = dialog_query::DeductiveRule::decode(&bytes).map_err(anyhow::Error::msg)?;
    write(&branch, &operator, &rule.this(), bytes).await?;
    assert_eq!(written(&branch, &operator).await?, (1, true));
    Ok(())
}
