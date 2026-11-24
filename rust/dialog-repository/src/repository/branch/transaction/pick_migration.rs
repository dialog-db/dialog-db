//! What a replica that ran a release reading attributes under picks
//! holds, read, upgraded and written by this one.
//!
//! Every test here replays facts captured from the `v0.4.0` release
//! (`fixtures/v0.4.0.json`, produced as `fixtures/README.md` says), so it
//! tests the bytes and identities a real replica holds, not this
//! release's encoding of the same rules.
//!
//! That release spelled every attribute of a rule body under a pick and
//! hashed the pick into the attribute's identity. This release refuses an
//! attribute spelled with a pick, so the rule is inert: no read decodes
//! it and no read fails over it. [`Branch::upgrade_rules`] rewrites a
//! rule whose attributes read under `last` or `all` with cardinality one
//! or many, and one reading under `max`, `min` or `top` with a
//! cardinality-many field electing with that elector, and leaves a rule
//! reading over a chain of relations inert and reported.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::helpers::test_repo;
use crate::{Branch, Transient};
use anyhow::Result;
use dialog_corpus::{Entity, Relation as CorpusRelation, Selector, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_query::attribute::Relation;
use dialog_query::migration::{Unmapped, attribute_uri_v1, concept_identity_v1};
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
    serde_json::from_str(include_str!("fixtures/v0.4.0.json")).expect("the fixture parses")
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
    if let Some(symbol) = json.get("symbol").and_then(Json::as_str) {
        return Value::Symbol(symbol.parse().expect("a symbol"));
    }
    if let Some(text) = json.get("string").and_then(Json::as_str) {
        return Value::String(text.to_string());
    }
    panic!("not a captured value: {json}")
}

/// A fact as a statement this release writes as stored, under
/// cardinality many.
fn statement(the: &str, of: &Entity, is: Value) -> AttributeStatement {
    AttributeStatement {
        the: Relation::from(the.parse::<CorpusRelation>().expect("an attribute")),
        of: of.clone(),
        is,
        cause: None,
        cardinality: Some(Cardinality::Many),
    }
}

/// The facts `v0.4.0` wrote to install the rule named `name`.
fn installed_by_release(name: &str) -> Vec<AttributeStatement> {
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

/// The entity `v0.4.0` stored the rule named `name` under.
fn stored_by_release(name: &str) -> Entity {
    fixture()["rules"][name]["entity"]
        .as_str()
        .expect("captured")
        .parse()
        .expect("an entity")
}

/// The body `v0.4.0` stored for the rule named `name`.
fn source_from_release(name: &str) -> Vec<u8> {
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

async fn install(branch: &Branch, operator: &Operator, names: &[&str]) -> Result<()> {
    let mut transaction = branch.transaction();
    for name in names {
        for fact in installed_by_release(name) {
            transaction = transaction.assert(fact);
        }
    }
    transaction.commit().publish().perform(operator).await?;
    branch.refresh(operator).await?;
    Ok(())
}

async fn write(
    branch: &Branch,
    operator: &Operator,
    facts: Vec<(&str, &Entity, Value)>,
) -> Result<()> {
    let mut transaction = branch.transaction();
    for (the, of, is) in facts {
        transaction = transaction.assert(statement(the, of, is));
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

/// Every value of `the` for `of` a read under `cardinality` returns,
/// stored and derived, sorted by their debug spelling.
async fn read(
    branch: &Branch,
    operator: &Operator,
    the: &str,
    as_type: &str,
    cardinality: &str,
    of: &Entity,
) -> Result<Vec<Value>> {
    let predicate: ConceptDescriptor = serde_json::from_value(json!({ "with": {
        "value": { "the": the, "as": as_type, "cardinality": cardinality }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::constant(of.clone()));
    terms.insert("value".to_string(), Term::<Any>::var("value"));
    let rows: Vec<ConceptConclusion> = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut values: Vec<Value> = rows
        .iter()
        .map(|row| -> Result<Value> {
            Ok(match as_type {
                "natural:" => Value::UnsignedInt(row.get::<u64>("value")?.into()),
                _ => Value::String(row.get::<String>("value")?),
            })
        })
        .collect::<Result<_>>()?;
    values.sort_by_key(|value| format!("{value:?}"));
    Ok(values)
}

/// The facts the branch holds under `of` in the `dialog.rule/` domain.
async fn rule_facts(branch: &Branch, operator: &Operator, of: &Entity) -> Result<usize> {
    let stream = branch
        .claims()
        .select(Selector::new().of(of.clone()))
        .perform(operator)
        .await?;
    let facts = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|item| item.and_then(|view| view.to_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(facts
        .iter()
        .filter(|fact| fact.the.as_str().starts_with("dialog.rule/"))
        .count())
}

/// A rule `v0.4.0` installed spells its picks as entities (`all:`),
/// which this release refuses to read: the stored body decodes to an error,
/// and the rule derives nothing until the upgrade rewrites it. A read
/// of its relation does not fail over it.
#[dialog_common::test]
async fn a_rule_the_pick_release_stored_is_refused_and_inert() -> Result<()> {
    let refused = DeductiveRule::decode(&source_from_release("tag_from_label"))
        .expect_err("a body spelling picks is refused");
    assert!(refused.contains("a field's `pick` is"), "{refused}");

    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let doc: Entity = "doc:1".parse()?;
    install(&branch, &operator, &["tag_from_label"]).await?;
    write(
        &branch,
        &operator,
        vec![("doc/label", &doc, Value::String("draft".into()))],
    )
    .await?;
    assert!(
        read(&branch, &operator, "org/tag", "text:", "many", &doc)
            .await?
            .is_empty(),
        "the inert rule derives nothing"
    );
    Ok(())
}

/// The upgrade rewrites a rule whose attributes read under `last` and
/// `all` into cardinality one and many, re-installs it under its
/// identity now, and withdraws every fact the release wrote for it. A
/// rule spelling no pick (`salary_from_bonus`, whose fields read under
/// the implied `last`) decodes as it was written and is re-installed,
/// since its identity moved with the cardinality it hashes.
#[dialog_common::test]
async fn the_upgrade_rewrites_last_and_all_into_cardinality() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let names = ["salary_from_bonus", "tag_from_label", "state_from_raw"];
    install(&branch, &operator, &names).await?;
    let (alice, doc, job): (Entity, Entity, Entity) =
        ("id:alice".parse()?, "doc:1".parse()?, "job:1".parse()?);
    write(
        &branch,
        &operator,
        vec![
            ("org/bonus", &alice, Value::UnsignedInt(500)),
            ("doc/label", &doc, Value::String("draft".into())),
            ("doc/label", &doc, Value::String("urgent".into())),
            ("job/raw", &job, Value::String("todo".into())),
        ],
    )
    .await?;

    let upgraded = upgrade(&branch, &operator).await?;
    let reinstalled: Vec<&Entity> = upgraded.reinstalled.iter().map(|(from, _)| from).collect();
    for name in names {
        assert!(
            reinstalled.contains(&&stored_by_release(name)),
            "{name} is re-installed: {upgraded:?}"
        );
        assert_eq!(
            rule_facts(&branch, &operator, &stored_by_release(name)).await?,
            0,
            "every fact the release wrote for {name} is withdrawn"
        );
    }
    assert!(upgraded.unmapped.is_empty(), "{upgraded:?}");
    assert!(upgraded.undecodable.is_empty(), "{upgraded:?}");

    assert_eq!(
        read(&branch, &operator, "org/salary", "natural:", "one", &alice).await?,
        vec![Value::UnsignedInt(500)]
    );
    assert_eq!(
        read(&branch, &operator, "org/tag", "text:", "many", &doc).await?,
        vec![
            Value::String("draft".into()),
            Value::String("urgent".into())
        ],
        "`all` is cardinality many: every label is a tag"
    );
    assert_eq!(
        read(&branch, &operator, "job/state", "text:", "one", &job).await?,
        vec![Value::String("todo".into())],
        "`last` over a listed domain is cardinality one over that domain"
    );

    let again = upgrade(&branch, &operator).await?;
    assert!(again.reinstalled.is_empty() && again.rewritten.is_empty());
    assert_eq!(again.revision, None, "a second run commits nothing");
    Ok(())
}

/// A rule reading an attribute under `max`, or under `top` (a listed
/// `as` without a pick), is rewritten to read a cardinality-many field
/// with that pick, and reads as that release read it: the greatest
/// salary, and the state listed first, the listing kept as it was. A
/// rule over
/// a chain of relations has no such spelling: it is left as it was,
/// inert, and reported, and a second run reports it again and commits
/// nothing.
#[dialog_common::test]
async fn the_upgrade_rewrites_ranked_reads_to_electors_and_reports_chains() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let names = [
        "top_from_salary",
        "flag_from_ranked_state",
        "reach_from_contact",
    ];
    install(&branch, &operator, &names).await?;
    let (alice, job): (Entity, Entity) = ("id:alice".parse()?, "job:1".parse()?);
    write(
        &branch,
        &operator,
        vec![
            ("org/salary", &alice, Value::UnsignedInt(300)),
            ("org/salary", &alice, Value::UnsignedInt(700)),
            ("org/salary", &alice, Value::UnsignedInt(500)),
            ("job/state", &job, Value::String("todo".into())),
            ("job/state", &job, Value::String("done".into())),
            (
                "user/email",
                &alice,
                Value::String("alice@example.com".into()),
            ),
        ],
    )
    .await?;
    let chained = rule_facts(&branch, &operator, &stored_by_release("reach_from_contact")).await?;

    let upgraded = upgrade(&branch, &operator).await?;
    assert_eq!(upgraded.reinstalled.len(), 2, "{upgraded:?}");
    assert!(upgraded.revision.is_some(), "the rewrite is committed");
    assert!(
        matches!(
            upgraded.unmapped.as_slice(),
            [(of, why)] if *of == stored_by_release("reach_from_contact")
                && matches!(why.as_slice(), [Unmapped::Chain { relations }] if relations.len() == 2)
        ),
        "{upgraded:?}"
    );
    assert_eq!(
        read(&branch, &operator, "org/top", "natural:", "one", &alice).await?,
        vec![Value::UnsignedInt(700)],
        "`max` reads the greatest salary"
    );
    assert_eq!(
        read(&branch, &operator, "job/flag", "text:", "one", &job).await?,
        vec![Value::String("todo".into())],
        "`top` reads the state that release ranked first"
    );
    assert_eq!(
        rule_facts(&branch, &operator, &stored_by_release("reach_from_contact")).await?,
        chained,
        "the chain is left as the release stored it"
    );
    assert!(
        read(&branch, &operator, "user/reach", "text:", "one", &alice)
            .await?
            .is_empty()
    );

    let again = upgrade(&branch, &operator).await?;
    assert_eq!(again.unmapped.len(), 1, "still reported: {again:?}");
    assert_eq!(again.revision, None);
    Ok(())
}

/// A concept `v0.4.0` marked transient stays transient once upgraded: the
/// release keyed the marker by the concept's identity then, and the
/// upgrade moves it to the identity of the concept the rule concludes.
#[dialog_common::test]
async fn a_concept_the_pick_release_marked_transient_stays_transient_once_upgraded() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let stage: Entity = fixture()["concepts"]["{target: cmd.stage/target entity}"]
        .as_str()
        .expect("a captured concept")
        .parse()?;
    let rule = |from: &str, to: &str| -> Result<InductiveRule> {
        Ok(serde_json::from_value(json!({
            "assert!": { "with": { "target": { "the": to, "as": "entity:" } } },
            "when": [{
                "assert": { "with": { "target": { "the": from, "as": "entity:" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "target": { "?": { "name": "target" } }
                }
            }]
        }))?)
    };
    branch
        .transaction()
        .assert(Transient(stage.clone()))
        .assert(
            dialog_query::StorableRule::try_from(rule("cmd.start/target", "cmd.stage/target")?)
                .expect("a storable rule"),
        )
        .assert(
            dialog_query::StorableRule::try_from(rule("cmd.stage/target", "result/target")?)
                .expect("a storable rule"),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    branch.refresh(&operator).await?;
    let upgraded = upgrade(&branch, &operator).await?;
    assert_eq!(
        upgraded.remarked.len(),
        1,
        "the marker moved once: {upgraded:?}"
    );
    assert_eq!(upgraded.remarked[0].0, stage);
    Ok(())
}

/// The identities `v0.4.0` gave attributes and a concept are reproduced
/// exactly, so a migration finds what it keyed by them. The current
/// identities differ.
#[dialog_common::test]
fn the_pick_release_identities_are_reproduced_for_migration() -> Result<()> {
    let fixture = fixture();
    let captured = |section: &str, key: &str| -> String {
        fixture[section][key]
            .as_str()
            .expect("captured")
            .to_string()
    };
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
    let state = AttributeDescriptor::new(
        "job/state".parse()?,
        "",
        Cardinality::One,
        Some(Type::String),
    )
    .with_domain(vec![
        Value::String("todo".into()),
        Value::String("done".into()),
    ]);
    let concept =
        ConceptDescriptor::try_from(vec![("salary", salary.clone()), ("tag", tag.clone())])?;
    assert_eq!(
        attribute_uri_v1(&salary),
        captured("attributes", "org/salary last natural")
    );
    assert_eq!(
        attribute_uri_v1(&tag),
        captured("attributes", "org/tag all text")
    );
    assert_eq!(
        attribute_uri_v1(&state),
        captured("attributes", "job/state last [todo, done]")
    );
    assert_eq!(
        concept_identity_v1(&concept).to_string(),
        captured(
            "concepts",
            "{salary: org/salary last natural, tag: org/tag all text}"
        )
    );
    assert_ne!(
        salary.to_uri(),
        attribute_uri_v1(&salary),
        "the current identity differs"
    );
    Ok(())
}

/// The rule bodies `branch` does not read, as `dialog.rule/invalid`
/// answers: each rule's entity and why, sorted by entity.
async fn invalid(branch: &Branch, operator: &Operator) -> Result<Vec<(Entity, String)>> {
    let predicate: ConceptDescriptor = serde_json::from_value(json!({ "with": {
        "reason": { "the": "dialog.rule/invalid", "as": "text:" }
    }}))?;
    let mut terms = Parameters::new();
    terms.insert("this".to_string(), Term::<Any>::var("this"));
    terms.insert("reason".to_string(), Term::<Any>::var("reason"));
    let rows: Vec<ConceptConclusion> = branch
        .select(ConceptQuery { predicate, terms })
        .perform(operator)
        .try_vec()
        .await?;
    let mut listed: Vec<(Entity, String)> = rows
        .iter()
        .map(|row| Ok((row.get::<Entity>("this")?, row.get::<String>("reason")?)))
        .collect::<Result<_>>()?;
    listed.sort();
    Ok(listed)
}

/// "A rule a branch cannot read is skipped and listed, and no read
/// fails over it." Four bodies no evaluation reads: one `v0.4.0`
/// stored, in an older format; one naming a format this release does
/// not know; bytes that are no rule; and a rule's own body stored under
/// an entity it does not hash to, which anyone can write. A query over
/// the relation the stored rule would derive answers as if none were
/// there, `dialog.rule/invalid` names each with why, and once the
/// upgrade rewrites the older one it is no longer listed.
#[dialog_common::test]
async fn a_branch_lists_the_rule_bodies_it_does_not_read() -> Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let doc: Entity = "doc:1".parse()?;
    install(&branch, &operator, &["tag_from_label"]).await?;
    let older = stored_by_release("tag_from_label");
    assert_eq!(
        invalid(&branch, &operator).await?,
        vec![(
            older.clone(),
            "older: the body is in format 3, which `upgrade_rules` rewrites to 4".to_string()
        )]
    );

    let newer: Entity = "rule:newer".parse()?;
    let garbage: Entity = "rule:garbage".parse()?;
    let forged: Entity = "rule:forged".parse()?;
    let future = serde_ipld_dagcbor::to_vec(&json!({ "format": 99 }))?;
    upgrade(&branch, &operator).await?;
    let rewritten = branch
        .claims()
        .select(Selector::new().the("dialog.rule/source".parse()?))
        .perform(&operator)
        .await?
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|item| item.and_then(|view| view.to_owned()))
        .collect::<Result<Vec<_>, _>>()?;
    let [current] = rewritten.as_slice() else {
        anyhow::bail!("the upgrade leaves one rule body: {}", rewritten.len());
    };
    assert_eq!(
        invalid(&branch, &operator).await?,
        vec![],
        "the rewritten rule is read"
    );
    write(
        &branch,
        &operator,
        vec![
            ("dialog.rule/source", &newer, Value::Bytes(future)),
            (
                "dialog.rule/source",
                &garbage,
                Value::Bytes(vec![0xff, 0x00]),
            ),
            ("dialog.rule/source", &forged, current.is.clone()),
            ("doc/label", &doc, Value::String("draft".into())),
        ],
    )
    .await?;

    let listed = invalid(&branch, &operator).await?;
    let reason = |entity: &Entity| {
        listed
            .iter()
            .find(|(listed, _)| listed == entity)
            .map(|(_, reason)| reason.as_str())
            .unwrap_or_default()
    };
    assert_eq!(listed.len(), 3, "{listed:?}");
    assert_eq!(
        reason(&newer),
        "newer: the body is in format 99, and this release reads up to 5"
    );
    assert!(reason(&garbage).starts_with("undecodable: "), "{listed:?}");
    assert_eq!(
        reason(&forged),
        "forged: the body does not hash to the identity it is stored under"
    );
    assert_eq!(
        read(&branch, &operator, "org/tag", "text:", "many", &doc).await?,
        vec![Value::String("draft".into())],
        "the rule that reads still derives, beside three that do not"
    );
    Ok(())
}
