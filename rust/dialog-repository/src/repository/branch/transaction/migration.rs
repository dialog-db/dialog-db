//! What a replica that ran the previous release holds, read, upgraded
//! and written by this one.
//!
//! Every test here replays facts captured from `main` at `120fba8`
//! (`fixtures/main-120fba8.json`, produced as `fixtures/README.md`
//! says), so it tests the bytes and identities a real replica holds,
//! not this release's encoding of the same rules.
//!
//! A rule the previous release stored sits under the hash of its bytes
//! and is inert here until [`Branch::upgrade_rules`] re-installs it
//! under its identity. Attribute and concept identities are unchanged
//! for every attribute read under `last` or `all`, so what is keyed by
//! them (a transient marker, an application's references) needs no
//! upgrade.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::test_repo;
use crate::{Branch, Transient};
use anyhow::Result;
use dialog_artifacts::{ArtifactSelector, Attribute, Entity, Policy, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::attribute::The;
use dialog_query::query::Output as _;
use dialog_query::rule::{DeductiveRule, InductiveRule};
use dialog_query::types::Any;
use dialog_query::{
    AttributeDescriptor, AttributeStatement, Cardinality, ConceptConclusion, ConceptDescriptor,
    ConceptQuery, Parameters, Term, Type,
};
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::StreamExt as _;
use serde_json::{Value as Json, json};

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn fixture() -> Json {
    serde_json::from_str(include_str!("fixtures/main-120fba8.json")).expect("the fixture parses")
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

/// A fact as a statement this release writes as stored, under `all`.
fn statement(the: &str, of: &Entity, is: Value) -> AttributeStatement {
    AttributeStatement {
        the: The::from(the.parse::<Attribute>().expect("an attribute")),
        of: of.clone(),
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
        policy: Some(Policy::All),
    }
}

/// The facts `main` wrote to install the rule named `name`.
fn installed_by_main(name: &str) -> Vec<AttributeStatement> {
    fixture()["rules"][name]["facts"]
        .as_array()
        .expect("captured facts")
        .iter()
        .map(|fact| {
            statement(
                fact["the"].as_str().expect("an attribute"),
                &fact["of"]
                    .as_str()
                    .expect("an entity")
                    .parse()
                    .expect("an entity"),
                value(&fact["is"]),
            )
        })
        .collect()
}

/// The body `main` stored for the rule named `name`.
fn source_from_main(name: &str) -> Vec<u8> {
    fixture()["rules"][name]["facts"]
        .as_array()
        .expect("captured facts")
        .iter()
        .find(|fact| fact["the"] == "dialog.rule/source")
        .map(|fact| match value(&fact["is"]) {
            Value::Bytes(bytes) => bytes,
            other => panic!("a source is bytes, not {other:?}"),
        })
        .expect("a source fact")
}

async fn install(
    branch: &Branch,
    operator: &Operator,
    facts: Vec<AttributeStatement>,
) -> Result<()> {
    let mut transaction = branch.transaction();
    for fact in facts {
        transaction = transaction.assert(fact);
    }
    transaction.commit().publish().perform(operator).await?;
    branch.refresh(operator).await?;
    Ok(())
}

/// Run the rule upgrade, then reopen the branch at its new head.
async fn upgrade(branch: &Branch, operator: &Operator) -> Result<crate::RulesUpgraded> {
    let upgraded = branch.upgrade_rules().perform(operator).await?;
    branch.refresh(operator).await?;
    Ok(upgraded)
}

/// The entity `main` stored the rule named `name` under.
fn stored_by_main(name: &str) -> Entity {
    fixture()["rules"][name]["entity"]
        .as_str()
        .expect("captured")
        .parse()
        .expect("an entity")
}

async fn commit(branch: &Branch, operator: &Operator, fact: AttributeStatement) -> Result<()> {
    branch
        .transaction()
        .assert(fact)
        .commit()
        .publish()
        .perform(operator)
        .await?;
    branch.refresh(operator).await?;
    Ok(())
}

/// The stored values of `the` for `of`, sorted by their debug spelling.
async fn stored(
    branch: &Branch,
    operator: &Operator,
    the: &str,
    of: &Entity,
) -> Result<Vec<Value>> {
    let selector = ArtifactSelector::new().the(the.parse()?).of(of.clone());
    let stream = branch.claims().select(selector).perform(operator).await?;
    let mut values: Vec<Value> = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|item| item.and_then(|view| view.to_owned()))
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .map(|artifact| artifact.is)
        .collect();
    values.sort_by_key(|value| format!("{value:?}"));
    Ok(values)
}

/// `org/salary` of `of` read under `select`, stored and derived, sorted.
async fn salary(
    branch: &Branch,
    operator: &Operator,
    of: &Entity,
    select: &str,
) -> Result<Vec<u64>> {
    let predicate: ConceptDescriptor = serde_json::from_value(json!({ "with": {
        "salary": { "the": "org/salary", "as": "UnsignedInteger", "select": select }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
    terms.insert("salary".to_string(), Term::<Any>::var("salary"));
    let rows: Vec<ConceptConclusion> = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut read: Vec<u64> = rows
        .iter()
        .map(|row| row.get::<u64>("salary"))
        .collect::<Result<_, _>>()?;
    read.sort();
    Ok(read)
}

fn unsigned(the: &str, of: &Entity, value: u32, policy: Policy) -> AttributeStatement {
    AttributeStatement {
        cardinality: Some(if policy == Policy::All {
            Cardinality::Many
        } else {
            Cardinality::One
        }),
        policy: Some(policy),
        ..statement(the, of, Value::UnsignedInt(value.into()))
    }
}

/// `main`'s `org/salary := org/bonus`, installed as `main` installed
/// it, derives nothing here: no read pays to find a rule under the hash
/// of its bytes. The upgrade re-installs it under its identity, and it
/// derives alice's salary from her bonus.
#[dialog_common::test]
async fn a_rule_the_previous_release_installed_derives_once_upgraded() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    commit(
        &branch,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    assert_eq!(
        salary(&branch, &operator, &alice, "all").await?,
        Vec::<u64>::new(),
        "inert before the upgrade"
    );
    let upgraded = upgrade(&branch, &operator).await?;
    assert_eq!(upgraded.reinstalled.len(), 1);
    assert_eq!(
        upgraded.reinstalled[0].0,
        stored_by_main("salary_from_bonus")
    );
    assert_eq!(salary(&branch, &operator, &alice, "all").await?, vec![500]);
    Ok(())
}

/// The upgrade is idempotent: a second run finds nothing to re-install
/// and commits nothing.
#[dialog_common::test]
async fn an_upgrade_runs_twice_as_once() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    install(&branch, &operator, installed_by_main("tagger")).await?;
    let first = upgrade(&branch, &operator).await?;
    assert_eq!(first.reinstalled.len(), 2);
    let head = branch.revision();
    let second = upgrade(&branch, &operator).await?;
    assert_eq!(second, crate::RulesUpgraded::default());
    assert_eq!(branch.revision(), head, "nothing committed");
    Ok(())
}

/// Two replicas holding `main`'s rule upgrade concurrently and then
/// sync. Each retracted the old entity's facts and asserted the rule
/// under its identity, which is the same on both: after the merge the
/// rule derives on both and the old entity holds no rule fact.
#[dialog_common::test]
async fn concurrent_upgrades_converge() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let alice: Entity = "id:alice".parse()?;
    let a = repo.branch("a").open().perform(&operator).await?;
    install(&a, &operator, installed_by_main("salary_from_bonus")).await?;
    commit(
        &a,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    let b = repo.branch("b").open().perform(&operator).await?;
    b.pull().from(&a).perform(&operator).await?;
    b.refresh(&operator).await?;

    upgrade(&a, &operator).await?;
    upgrade(&b, &operator).await?;
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
    let old = stored_by_main("salary_from_bonus");
    for branch in [&a, &b] {
        branch.refresh(&operator).await?;
        assert_eq!(salary(branch, &operator, &alice, "all").await?, vec![500]);
        for the in [
            "dialog.rule/source",
            "dialog.rule/conclusion",
            "dialog.rule/reads",
        ] {
            assert!(
                stored(branch, &operator, the, &old).await?.is_empty(),
                "no {the} fact left under the entity main stored the rule as"
            );
        }
    }
    Ok(())
}

/// A replica still on the previous release writes the rule again under
/// the old entity after this one upgraded, and sync brings it in. It is
/// inert beside the upgraded rule, and the next upgrade re-installs it,
/// which leaves the rule as the first upgrade did.
#[dialog_common::test]
async fn a_rule_reintroduced_by_the_previous_release_is_inert_until_the_next_upgrade() -> Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    upgrade(&branch, &operator).await?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    commit(
        &branch,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    assert_eq!(
        salary(&branch, &operator, &alice, "all").await?,
        vec![500],
        "the upgraded rule derives; the reintroduced copy is inert"
    );
    let again = upgrade(&branch, &operator).await?;
    assert_eq!(again.reinstalled.len(), 1);
    assert!(
        stored(
            &branch,
            &operator,
            "dialog.rule/source",
            &stored_by_main("salary_from_bonus")
        )
        .await?
        .is_empty()
    );
    assert_eq!(salary(&branch, &operator, &alice, "all").await?, vec![500]);
    Ok(())
}

/// `main`'s inductive `derived/tag := doc/title`, installed as `main`
/// installed it, fires once upgraded.
#[dialog_common::test]
async fn a_rule_the_previous_release_installed_fires_at_commit_once_upgraded() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let doc: Entity = "doc:1".parse()?;
    install(&branch, &operator, installed_by_main("tagger")).await?;
    upgrade(&branch, &operator).await?;
    commit(
        &branch,
        &operator,
        statement("doc/title", &doc, Value::String("hello".into())),
    )
    .await?;
    assert_eq!(
        stored(&branch, &operator, "derived/tag", &doc).await?,
        vec![Value::String("hello".into())],
        "the rule main installed fires"
    );
    Ok(())
}

/// A rule this release installs reads a relation a rule the previous
/// release installed derives, once upgraded: `assert! derived/paid :=
/// s when org/salary(x) = s` over `main`'s `org/salary := org/bonus`.
/// A bonus landing derives a salary, and the inductive rule pays it.
#[dialog_common::test]
async fn a_rule_the_previous_release_installed_feeds_induction_once_upgraded() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    upgrade(&branch, &operator).await?;
    let paid: InductiveRule = serde_json::from_value(json!({
        "assert!": {
            "with": { "paid": { "the": "derived/paid", "as": "UnsignedInteger" } }
        },
        "when": [{
            "assert": {
                "with": { "salary": { "the": "org/salary", "as": "UnsignedInteger" } }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "salary": { "?": { "name": "paid" } }
            }
        }]
    }))?;
    branch
        .transaction()
        .assert(paid)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    commit(
        &branch,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    assert_eq!(
        stored(&branch, &operator, "derived/paid", &alice).await?,
        vec![Value::UnsignedInt(500)],
        "the salary main's rule derives is paid"
    );
    Ok(())
}

/// "If the read elects a candidate a rule derives, nothing is
/// retracted." Alice's stored salary is 100, her bonus 500, and
/// `main`'s rule, upgraded, derives her salary from her bonus, so a
/// `max` read elects the derived 500 and a `max` write of 200 lands
/// beside the stored 100 without retracting it.
#[dialog_common::test]
async fn a_choosing_write_beside_an_upgraded_rule() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    upgrade(&branch, &operator).await?;
    commit(
        &branch,
        &operator,
        unsigned("org/salary", &alice, 100, Policy::All),
    )
    .await?;
    commit(
        &branch,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    assert_eq!(salary(&branch, &operator, &alice, "max").await?, vec![500]);
    commit(
        &branch,
        &operator,
        unsigned("org/salary", &alice, 200, Policy::Max),
    )
    .await?;
    assert_eq!(
        stored(&branch, &operator, "org/salary", &alice).await?,
        vec![Value::UnsignedInt(100), Value::UnsignedInt(200)],
        "the read elected the derived 500, so no stored claim is succeeded"
    );
    assert_eq!(salary(&branch, &operator, &alice, "max").await?, vec![500]);
    Ok(())
}

/// Retracting a rule uninstalls it. A writer holding `main`'s rule,
/// decoded from what `main` stored, retracts it once the branch is
/// upgraded: the retraction lands under the rule's identity, where the
/// upgrade put it.
#[dialog_common::test]
async fn retracting_an_upgraded_rule_uninstalls_it() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    install(&branch, &operator, installed_by_main("salary_from_bonus")).await?;
    upgrade(&branch, &operator).await?;
    commit(
        &branch,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    assert_eq!(salary(&branch, &operator, &alice, "all").await?, vec![500]);

    let rule = DeductiveRule::decode(&source_from_main("salary_from_bonus"))
        .map_err(anyhow::Error::msg)?;
    branch
        .transaction()
        .retract(&rule)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    assert_eq!(
        salary(&branch, &operator, &alice, "all").await?,
        Vec::<u64>::new(),
        "the retracted rule derives nothing"
    );
    Ok(())
}

/// A concept `main` marked transient stays transient, with no upgrade:
/// `main` keyed the marker by the concept's identity, and a concept
/// over attributes read under `last` or `all` has the identity `main`
/// gave it.
#[dialog_common::test]
async fn a_concept_the_previous_release_marked_transient_stays_transient() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let stage: Entity = fixture()["concepts"]["{target: cmd.stage/target Entity}"]
        .as_str()
        .expect("a captured concept")
        .parse()?;
    let rule = |from: &str, to: &str| -> Result<InductiveRule> {
        Ok(serde_json::from_value(json!({
            "assert!": { "with": { "target": { "the": to, "as": "Entity" } } },
            "when": [{
                "assert": { "with": { "target": { "the": from, "as": "Entity" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "target": { "?": { "name": "target" } }
                }
            }]
        }))?)
    };
    branch
        .transaction()
        .assert(Transient(stage))
        .assert(rule("cmd.start/target", "cmd.stage/target")?)
        .assert(rule("cmd.stage/target", "result/target")?)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;

    let command: Entity = "cmd:start".parse()?;
    let target: Entity = "doc:1".parse()?;
    branch
        .transaction()
        .dispatch(
            dialog_query::the!("cmd.start/target")
                .of(command.clone())
                .is(target.clone()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    assert_eq!(
        stored(&branch, &operator, "result/target", &command).await?,
        vec![Value::Entity(target)],
        "the cascade lands its durable head"
    );
    assert!(
        stored(&branch, &operator, "cmd.stage/target", &command)
            .await?
            .is_empty(),
        "the transient intermediate never reaches the branch"
    );
    Ok(())
}

/// An attribute and a concept keep the identities the previous release
/// gave them. Every stored fact keyed by one (a transient marker, a
/// rule's conclusion, an application's own references) depends on it.
#[dialog_common::test]
fn an_attribute_and_a_concept_keep_the_identities_the_previous_release_gave_them() -> Result<()> {
    let fixture = fixture();
    let salary = AttributeDescriptor::new(
        "org/salary".parse()?,
        "",
        Cardinality::One,
        Some(Type::UnsignedInt),
    );
    let tag = AttributeDescriptor::new(
        "org/tag".parse()?,
        "",
        Cardinality::Many,
        Some(Type::String),
    );
    let concept =
        ConceptDescriptor::try_from(vec![("salary", salary.clone()), ("tag", tag.clone())])?;
    let captured = |section: &str, key: &str| -> String {
        fixture[section][key]
            .as_str()
            .expect("captured")
            .to_string()
    };
    assert_eq!(
        (salary.to_uri(), tag.to_uri(), concept.this().to_string()),
        (
            captured("attributes", "org/salary one UnsignedInteger"),
            captured("attributes", "org/tag many Text"),
            captured(
                "concepts",
                "{salary: org/salary one UnsignedInteger, tag: org/tag many Text}"
            ),
        )
    );
    Ok(())
}

/// "Bytes stored under any other entity stay inert." This release's
/// `org/salary := org/bonus`, with every fact an install writes, under
/// an entity that is not its identity, derives nothing on the query
/// path, as on the commit path.
#[dialog_common::test]
async fn rule_facts_under_an_entity_that_is_not_their_address_derive_nothing() -> Result<()> {
    use dialog_artifacts::{Change, Changes};
    use dialog_query::Statement as _;

    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let alice: Entity = "id:alice".parse()?;
    let rule = DeductiveRule::decode(&source_from_main("salary_from_bonus"))
        .map_err(anyhow::Error::msg)?;
    let forged: Entity = "rule:forged".parse()?;
    let mut changes = Changes::new();
    (&rule).assert(&mut changes);
    let facts: Vec<AttributeStatement> = changes
        .iter()
        .filter_map(|(_, the, change)| match change {
            Change::Assert(value, _) => Some(statement(the.as_str(), &forged, value.clone())),
            Change::Retract(_) => None,
        })
        .collect();
    install(&branch, &operator, facts).await?;
    commit(
        &branch,
        &operator,
        unsigned("org/bonus", &alice, 500, Policy::All),
    )
    .await?;
    assert_eq!(
        salary(&branch, &operator, &alice, "all").await?,
        Vec::<u64>::new(),
        "a rule under an entity its body does not hash to derives nothing"
    );
    Ok(())
}
