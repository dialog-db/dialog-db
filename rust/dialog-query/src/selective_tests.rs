//! Concept fields that elect their claim, pinned as tests.
//!
//! A field's attribute says what a write does; its aggregate says which
//! of the cell's claims a read returns. An account's `status` is
//! written under cardinality many, so every state an account was put
//! in stays a claim, and the concept elects the greatest under the
//! order its attribute lists them in. That holds whether the read names
//! the account, leaves it free, or names the status, and whether the
//! claims are stored or derived by a rule.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::{AttributeStatement, Relation};
use crate::concept::descriptor::ConceptFieldDescriptor;
use crate::error::TypeError;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::selective::Selective;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{
    AttributeDescriptor, Cardinality, ConceptDescriptor, ConceptQuery, Entity, Match, Parameters,
    Term, Type, Value,
};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn text(text: &str) -> Value {
    Value::String(text.into())
}

/// The states an account's status lists: `min` (and the deprecated
/// `top`) reads the first present.
fn states() -> serde_json::Value {
    serde_json::json!(["suspended", "active", "registered"])
}

/// An account: its name, read newest, and its status, elected by
/// `elect` among every state written.
fn account(elect: &str) -> ConceptDescriptor {
    account_over(elect, states())
}

/// [`account`], its status listing `listed`.
fn account_over(elect: &str, listed: serde_json::Value) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": {
            "name": { "the": "account/name", "as": "text:" },
            "status": {
                "the": "account/status",
                "as": listed,
                "cardinality": "many",
                "pick": elect
            }
        }
    }))
    .expect("an aggregating account is well formed")
}

/// A claim of `the` on `of`, added beside the others.
fn claim(the: Relation, of: &Entity, is: Value) -> AttributeStatement {
    AttributeStatement {
        the,
        of: of.clone(),
        is,
        cause: None,
        cardinality: None,
    }
}

/// Two accounts: `alice`, put in every state in turn, and `bob`,
/// registered and then active.
async fn store_accounts(branch: &Branch, operator: &Operator) -> anyhow::Result<(Entity, Entity)> {
    let alice = Entity::new()?;
    let bob = Entity::new()?;
    branch
        .transaction()
        .assert(claim(the!("account/name"), &alice, text("alice")))
        .assert(claim(the!("account/name"), &bob, text("bob")))
        .assert(claim(the!("account/status"), &alice, text("active")))
        .assert(claim(the!("account/status"), &alice, text("suspended")))
        .assert(claim(the!("account/status"), &alice, text("registered")))
        .assert(claim(the!("account/status"), &bob, text("registered")))
        .assert(claim(the!("account/status"), &bob, text("active")))
        .commit()
        .publish()
        .perform(operator)
        .await?;
    Ok((alice, bob))
}

/// Each account `concept` reads, with `this` and `status` given as
/// `terms` leaves them, as `(name, status)` sorted by name.
async fn statuses(
    source: &TestEnv<'_>,
    concept: ConceptDescriptor,
    this: Term<Any>,
    status: Term<Any>,
) -> anyhow::Result<Vec<(String, String)>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), this);
    terms.insert("name".into(), Term::var("name"));
    terms.insert("status".into(), status.clone());
    let rows = ConceptQuery {
        terms,
        predicate: concept,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let mut read = Vec::new();
    for row in rows {
        let name = row.lookup(&Term::<Any>::var("name"))?.content()?;
        let status = match &status {
            Term::Constant(value) => value.clone(),
            term => row.lookup(term)?.content()?,
        };
        let (Value::String(name), Value::String(status)) = (name, status) else {
            anyhow::bail!("an account's name and status are text");
        };
        read.push((name, status));
    }
    read.sort();
    Ok(read)
}

fn pair(name: &str, status: &str) -> (String, String) {
    (name.to_string(), status.to_string())
}

/// `top` reads the state listed first among those an account holds,
/// whichever was written last.
#[dialog_common::test]
async fn it_elects_the_greatest_listed_claim() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (alice, _) = store_accounts(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::var("this"),
            Term::var("status")
        )
        .await?,
        vec![pair("alice", "suspended"), pair("bob", "active")],
        "a free account elects per account"
    );
    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::Constant(Value::Entity(alice)),
            Term::var("status")
        )
        .await?,
        vec![pair("alice", "suspended")],
        "a named account elects its own"
    );
    Ok(())
}

/// A read naming the status finds the accounts whose elected status it
/// is, and not one that merely holds it among lesser or greater claims.
#[dialog_common::test]
async fn it_matches_a_named_status_against_the_elected_one() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_accounts(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::var("this"),
            Term::Constant(text("active"))
        )
        .await?,
        vec![pair("bob", "active")],
        "alice holds `active` but is elected `suspended`"
    );
    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::var("this"),
            Term::Constant(text("registered"))
        )
        .await?,
        vec![],
        "both hold `registered`, and neither is elected it"
    );
    Ok(())
}

/// Retracting the greatest claim leaves the next: un-suspending an
/// account is retracting `suspended`.
#[dialog_common::test]
async fn it_elects_the_next_claim_once_the_greatest_is_retracted() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (alice, _) = store_accounts(&branch, &operator).await?;
    branch
        .transaction()
        .retract(claim(the!("account/status"), &alice, text("suspended")))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::var("this"),
            Term::var("status")
        )
        .await?,
        vec![pair("alice", "active"), pair("bob", "active")]
    );
    Ok(())
}

/// `max` and `min` over an attribute listing no values elect by value.
#[dialog_common::test]
async fn it_elects_by_value_under_max_and_min() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let player = Entity::new()?;
    branch
        .transaction()
        .assert(claim(the!("game/score"), &player, Value::UnsignedInt(30)))
        .assert(claim(the!("game/score"), &player, Value::UnsignedInt(90)))
        .assert(claim(the!("game/score"), &player, Value::UnsignedInt(10)))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for (elect, expected) in [("max", 90), ("min", 10)] {
        let concept: ConceptDescriptor = serde_json::from_value(serde_json::json!({
            "with": { "score": {
                "the": "game/score",
                "as": "natural:",
                "cardinality": "many",
                "pick": elect
            } }
        }))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("score".into(), Term::var("score"));
        let rows = ConceptQuery {
            terms,
            predicate: concept,
        }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
        let scores = rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("score"))?.content())
            .collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(scores, vec![Value::UnsignedInt(expected)], "{elect}");
    }
    Ok(())
}

/// A rule deriving a status adds a candidate the aggregate ranks with the
/// stored claims: an account a moderator suspends is suspended however
/// it was activated.
#[dialog_common::test]
async fn it_elects_among_stored_and_derived_claims() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (_, bob) = store_accounts(&branch, &operator).await?;
    branch
        .transaction()
        .assert(claim(the!("moderation/status"), &bob, text("suspended")))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let status = serde_json::json!({
        "the": "account/status",
        "as": states(),
        "cardinality": "many"
    });
    let moderated = serde_json::json!({ "the": "moderation/status", "as": states() });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "is": status } },
        "when": [{
            "assert": { "with": { "is": moderated } },
            "where": {
                "this": { "?": { "name": "this" } },
                "is": { "?": { "name": "is" } }
            }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);

    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::var("this"),
            Term::var("status")
        )
        .await?,
        vec![pair("alice", "suspended"), pair("bob", "suspended")],
        "bob's derived `suspended` outranks his stored `active`"
    );
    assert_eq!(
        statuses(
            &source,
            account("last"),
            Term::var("this"),
            Term::var("status")
        )
        .await?,
        vec![pair("alice", "registered"), pair("bob", "suspended")],
        "under `last` the newest claim, stored or derived, stands"
    );
    Ok(())
}

/// What a read under each aggregate needs of its attribute is refused
/// where the attribute lacks it.
#[dialog_common::test]
fn it_refuses_an_aggregator_its_attribute_cannot_be_read_under() {
    let listed = |cardinality: Cardinality| {
        serde_json::from_value::<AttributeDescriptor>(serde_json::json!({
            "the": "account/status",
            "as": states(),
            "cardinality": cardinality.name()
        }))
        .expect("a listed attribute is well formed")
    };
    let refused = |field: Result<ConceptFieldDescriptor, TypeError>| {
        matches!(field, Err(TypeError::SelectiveAggregate { .. }))
    };

    assert!(
        refused(ConceptFieldDescriptor::required(listed(Cardinality::One)).picking(Selective::Top)),
        "a ranked field reads a cardinality-many attribute"
    );
    let unlisted = AttributeDescriptor::new(
        the!("account/status"),
        "",
        Cardinality::Many,
        Some(Type::String),
    );
    assert!(
        refused(ConceptFieldDescriptor::required(unlisted).picking(Selective::Top)),
        "`top` ranks by a listing"
    );
    let flags = AttributeDescriptor::new(
        the!("account/flag"),
        "",
        Cardinality::Many,
        Some(Type::Boolean),
    );
    assert!(
        refused(ConceptFieldDescriptor::required(flags.clone()).picking(Selective::Max)),
        "`max` needs an order"
    );
    assert!(
        ConceptFieldDescriptor::required(flags)
            .picking(Selective::Last)
            .is_ok()
    );
    let listed_flags = serde_json::from_value::<AttributeDescriptor>(serde_json::json!({
        "the": "account/flag",
        "as": [false, true],
        "cardinality": "many"
    }))
    .expect("a listed attribute is well formed");
    for pick in [Selective::Max, Selective::Min] {
        assert!(
            ConceptFieldDescriptor::required(listed_flags.clone())
                .picking(pick)
                .is_ok(),
            "`{pick}` over a listing ranks by it, whatever the listed values' type"
        );
    }
    assert!(
        ConceptFieldDescriptor::required(listed(Cardinality::Many))
            .picking(Selective::Top)
            .is_ok()
    );
    assert!(
        serde_json::from_value::<ConceptDescriptor>(serde_json::json!({
            "with": { "status": {
                "the": "account/status",
                "as": states(),
                "cardinality": "one",
                "pick": "top"
            } }
        }))
        .is_err(),
        "the wire refuses what the builder does"
    );
}

/// The aggregator is part of the concept: a field aggregating with `top` and one
/// with `last` over the same attribute make two concepts, and
/// neither is the attribute's relation.
#[dialog_common::test]
fn it_hashes_the_aggregator_into_the_concept() {
    let top = account("top");
    let last = account("last");
    assert_ne!(top.this(), last.this());

    let single = |elect: Option<&str>| -> ConceptDescriptor {
        let mut status = serde_json::json!({
            "the": "account/status",
            "as": states(),
            "cardinality": "many"
        });
        if let Some(elect) = elect {
            status["pick"] = elect.into();
        }
        serde_json::from_value(serde_json::json!({ "with": { "status": status } }))
            .expect("a one-field concept is well formed")
    };
    assert!(single(None).attribute_field().is_some());
    assert!(single(Some("top")).attribute_field().is_none());
    assert_ne!(single(None).this(), single(Some("top")).this());
}

/// A field's aggregate round-trips through the notation.
#[dialog_common::test]
fn it_round_trips_an_aggregating_field() -> anyhow::Result<()> {
    let concept = account("top");
    let encoded = serde_json::to_value(&concept)?;
    assert_eq!(encoded["with"]["status"]["pick"], "top");
    let decoded: ConceptDescriptor = serde_json::from_value(encoded)?;
    assert_eq!(decoded, concept);
    assert_eq!(decoded.this(), concept.this());
    Ok(())
}

/// "A rule deriving a relation from the claim a field elects of that
/// relation closes a cycle through the election, and is set aside."
/// `account/status(a) := "suspended" :- Account(a, status: "active")`
/// reads the elected status and derives a claim that outranks it: left
/// in, the claim would make its own premise false. The analysis sets
/// the rule aside, and the stored states read as they did without it.
#[dialog_common::test]
async fn it_sets_aside_a_rule_deriving_what_its_own_election_reads() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_accounts(&branch, &operator).await?;
    let status = serde_json::json!({
        "the": "account/status",
        "as": states(),
        "cardinality": "many"
    });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "is": status } },
        "when": [{
            "assert": serde_json::to_value(account("top"))?,
            "where": {
                "this": { "?": { "name": "this" } },
                "name": { "?": { "name": "name" } },
                "status": "active"
            }
        }, {
            "assert": "==",
            "where": { "this": "suspended", "is": { "?": { "name": "is" } } }
        }]
    }))?;
    let rule = rule.compile()?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.clone())?;
    let analysis = registry.analysis()?;
    let quarantined: Vec<Entity> = analysis
        .quarantined()
        .iter()
        .map(|set_aside| set_aside.rule.clone())
        .collect();
    assert_eq!(quarantined, vec![rule.this()]);
    let source = TestEnv::new(&branch, &operator, registry);
    assert_eq!(
        statuses(
            &source,
            account("top"),
            Term::var("this"),
            Term::var("status")
        )
        .await?,
        vec![pair("alice", "suspended"), pair("bob", "active")],
        "bob stays active: nothing is derived from his own election"
    );
    Ok(())
}

/// "A field's cardinality is derived from its pick unless it is
/// written." `last` reads cardinality one and every other pick many; a
/// cardinality written beside a pick is kept when the pick can read it
/// and refused when it cannot; and what is written back leaves out a
/// cardinality the pick implies.
#[dialog_common::test]
fn it_derives_a_fields_cardinality_from_its_pick() -> anyhow::Result<()> {
    let field = |entries: serde_json::Value| -> Result<ConceptDescriptor, serde_json::Error> {
        let mut status = serde_json::json!({ "the": "account/status", "as": states() });
        for (key, value) in entries.as_object().expect("entries") {
            status[key] = value.clone();
        }
        serde_json::from_value(serde_json::json!({ "with": { "status": status } }))
    };
    let read = |concept: &ConceptDescriptor| {
        let (_, field) = concept.with().iter().next().expect("one field");
        (field.cardinality(), field.pick())
    };
    for (written, cardinality, pick, back) in [
        (
            serde_json::json!({}),
            Cardinality::One,
            None,
            serde_json::json!({}),
        ),
        (
            serde_json::json!({ "pick": "last" }),
            Cardinality::One,
            None,
            serde_json::json!({}),
        ),
        (
            serde_json::json!({ "pick": "all" }),
            Cardinality::Many,
            None,
            serde_json::json!({ "cardinality": "many" }),
        ),
        (
            serde_json::json!({ "pick": "top" }),
            Cardinality::Many,
            Some(Selective::Top),
            serde_json::json!({ "pick": "top" }),
        ),
        (
            serde_json::json!({ "pick": "top", "cardinality": "many" }),
            Cardinality::Many,
            Some(Selective::Top),
            serde_json::json!({ "pick": "top" }),
        ),
        (
            serde_json::json!({ "pick": "last", "cardinality": "many" }),
            Cardinality::Many,
            Some(Selective::Last),
            serde_json::json!({ "pick": "last", "cardinality": "many" }),
        ),
    ] {
        let concept = field(written.clone())?;
        assert_eq!(read(&concept), (cardinality, pick), "{written}");
        let mut expected = serde_json::json!({ "the": "account/status", "as": states() });
        for (key, value) in back.as_object().expect("entries") {
            expected[key] = value.clone();
        }
        let encoded = serde_json::to_value(&concept)?;
        assert_eq!(encoded["with"]["status"], expected, "{written}");
        let bytes = serde_ipld_dagcbor::to_vec(&concept)?;
        let decoded: ConceptDescriptor = serde_ipld_dagcbor::from_slice(&bytes)?;
        assert_eq!(decoded.this(), concept.this(), "{written}");
    }
    assert_eq!(
        field(serde_json::json!({ "pick": "all" }))?.this(),
        field(serde_json::json!({ "cardinality": "many" }))?.this(),
        "two spellings of one concept"
    );
    for refused in [
        serde_json::json!({ "pick": "top", "cardinality": "one" }),
        serde_json::json!({ "pick": "max", "cardinality": "one" }),
        serde_json::json!({ "pick": "all", "cardinality": "one" }),
        serde_json::json!({ "pick": "sum" }),
    ] {
        assert!(field(refused.clone()).is_err(), "{refused}");
    }
    Ok(())
}

/// The levels a task's priority lists, least first. Their bytes order
/// them `high`, `low`, `medium`, so a read by value and a read by the
/// listing differ under both `max` and `min`.
fn levels() -> serde_json::Value {
    serde_json::json!(["low", "medium", "high"])
}

/// A task: its title, read newest, and its priority, elected by `pick`
/// among every level claimed, over `listed` (`null` for no listing).
fn task(pick: &str, listed: serde_json::Value) -> ConceptDescriptor {
    let as_ = if listed.is_null() {
        serde_json::json!("text:")
    } else {
        listed
    };
    serde_json::from_value(serde_json::json!({
        "with": {
            "name": { "the": "task/title", "as": "text:" },
            "status": {
                "the": "task/priority",
                "as": as_,
                "cardinality": "many",
                "pick": pick
            }
        }
    }))
    .expect("a picking task is well formed")
}

/// Store `tasks`, each a title and the priorities claimed for it, all
/// in one transaction, and return their entities in order.
async fn store_tasks(
    branch: &Branch,
    operator: &Operator,
    tasks: &[(&str, &[&str])],
) -> anyhow::Result<Vec<Entity>> {
    let mut entities = Vec::new();
    let mut transaction = branch.transaction();
    for (title, priorities) in tasks {
        let entity = Entity::new()?;
        transaction = transaction.assert(claim(the!("task/title"), &entity, text(title)));
        for priority in *priorities {
            transaction = transaction.assert(claim(the!("task/priority"), &entity, text(priority)));
        }
        entities.push(entity);
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(entities)
}

/// "Over a listed type, `max` reads the claim holding the last listed
/// value present and `min` the first." Over `[low, medium, high]`,
/// `max` reads `high` when all three are claimed and `medium` when
/// `high` is not, and `min` reads `low`, whether the read names the
/// task or leaves it free.
#[dialog_common::test]
async fn it_reads_max_and_min_over_a_listing_least_first() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let tasks = store_tasks(
        &branch,
        &operator,
        &[
            ("all", &["medium", "high", "low"]),
            ("no high", &["low", "medium"]),
        ],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for (pick, expected) in [
        ("max", [pair("all", "high"), pair("no high", "medium")]),
        ("min", [pair("all", "low"), pair("no high", "low")]),
    ] {
        assert_eq!(
            statuses(
                &source,
                task(pick, levels()),
                Term::var("this"),
                Term::var("status")
            )
            .await?,
            expected.to_vec(),
            "`{pick}` over a free task"
        );
        for (entity, expected) in tasks.iter().zip(&expected) {
            assert_eq!(
                statuses(
                    &source,
                    task(pick, levels()),
                    Term::Constant(Value::Entity(entity.clone())),
                    Term::var("status")
                )
                .await?,
                vec![expected.clone()],
                "`{pick}` over a named task"
            );
        }
    }
    Ok(())
}

/// "`top` reads the first listed value present, which is `min` over the
/// same list, and `max` over the list written the other way": all three
/// read the same claim of every account, and `max` over the list as
/// written the last listed present.
#[dialog_common::test]
async fn it_reads_the_same_claim_under_top_and_min() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_accounts(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let read = |concept: ConceptDescriptor| {
        let source = &source;
        async move { statuses(source, concept, Term::var("this"), Term::var("status")).await }
    };
    let reversed = serde_json::json!(["registered", "active", "suspended"]);
    let top = read(account("top")).await?;
    assert_eq!(top, vec![pair("alice", "suspended"), pair("bob", "active")]);
    assert_eq!(read(account("min")).await?, top, "`min` over the same list");
    assert_eq!(
        read(account_over("max", reversed)).await?,
        top,
        "`max` over the list reversed"
    );
    assert_eq!(
        read(account("max")).await?,
        vec![pair("alice", "registered"), pair("bob", "registered")]
    );
    Ok(())
}

/// "A value the listing does not hold loses to every listed one, under
/// `max` and `min` as under `top`, and a cell holding no listed value
/// reads its newest claim." A task claiming `urgent` beside `low` reads
/// `low` under all three; a task claiming only unlisted values reads
/// the one claimed last.
#[dialog_common::test]
async fn it_ranks_an_unlisted_value_below_every_listed_one() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let tasks = store_tasks(
        &branch,
        &operator,
        &[("mixed", &["urgent", "low"]), ("unlisted", &["urgent"])],
    )
    .await?;
    branch
        .transaction()
        .assert(claim(the!("task/priority"), &tasks[1], text("later")))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for pick in ["top", "min", "max"] {
        assert_eq!(
            statuses(
                &source,
                task(pick, levels()),
                Term::var("this"),
                Term::var("status")
            )
            .await?,
            vec![pair("mixed", "low"), pair("unlisted", "later")],
            "`{pick}`"
        );
    }
    Ok(())
}

/// "Over an attribute with no list, `max` and `min` are unchanged":
/// they compare the values, so over the same claims as
/// [`it_reads_max_and_min_over_a_listing_least_first`] text orders by
/// its bytes, `medium` the greatest and `high` the least.
#[dialog_common::test]
async fn it_reads_max_and_min_by_value_where_nothing_is_listed() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_tasks(&branch, &operator, &[("all", &["medium", "high", "low"])]).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for (pick, expected) in [("max", "medium"), ("min", "high")] {
        assert_eq!(
            statuses(
                &source,
                task(pick, serde_json::Value::Null),
                Term::var("this"),
                Term::var("status")
            )
            .await?,
            vec![pair("all", expected)],
            "`{pick}`"
        );
    }
    Ok(())
}

/// A rule deriving a priority adds a candidate `max` and `min` rank by
/// the listing with the stored claims, and a rule reading the task
/// concept concludes the claim the field elects.
#[dialog_common::test]
async fn it_reads_max_and_min_over_a_listing_through_rules() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let tasks = store_tasks(&branch, &operator, &[("escalated", &["medium"])]).await?;
    branch
        .transaction()
        .assert(claim(the!("escalation/priority"), &tasks[0], text("high")))
        .assert(claim(the!("escalation/priority"), &tasks[0], text("low")))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let priority = serde_json::json!({
        "the": "task/priority",
        "as": levels(),
        "cardinality": "many"
    });
    let escalated = serde_json::json!({
        "the": "escalation/priority",
        "as": levels(),
        "cardinality": "many"
    });
    let derive: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "is": priority } },
        "when": [{
            "assert": { "with": { "is": escalated } },
            "where": {
                "this": { "?": { "name": "this" } },
                "is": { "?": { "name": "is" } }
            }
        }]
    }))?;

    for (pick, expected) in [("max", "high"), ("min", "low")] {
        let level = serde_json::json!({ "the": format!("report/{pick}"), "as": "text:" });
        let report: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "is": level } },
            "when": [{
                "assert": serde_json::to_value(task(pick, levels()))?,
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } },
                    "status": { "?": { "name": "is" } }
                }
            }]
        }))?;
        let mut registry = RuleRegistry::new();
        registry.register(derive.clone().compile()?)?;
        registry.register(report.compile()?)?;
        let source = TestEnv::new(&branch, &operator, registry);

        assert_eq!(
            statuses(
                &source,
                task(pick, levels()),
                Term::var("this"),
                Term::var("status")
            )
            .await?,
            vec![pair("escalated", expected)],
            "`{pick}` ranks the derived claims with the stored one"
        );
        let reported: ConceptDescriptor =
            serde_json::from_value(serde_json::json!({ "with": { "is": level } }))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("is".into(), Term::var("is"));
        let rows = ConceptQuery {
            terms,
            predicate: reported,
        }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
        let concluded = rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("is"))?.content())
            .collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(
            concluded,
            vec![text(expected)],
            "a rule reading the concept under `{pick}` concludes its claim"
        );
    }
    Ok(())
}

/// "`max` and `min` over a listed type are accepted whatever the list's
/// value type": over `[false, true]`, whose type has no order of its
/// own, `max` reads `true` and `min` `false`, a stored claim ranked with
/// a derived one.
#[dialog_common::test]
async fn it_reads_max_and_min_over_a_listing_of_an_unordered_type() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let flagged = Entity::new()?;
    branch
        .transaction()
        .assert(claim(the!("review/flag"), &flagged, Value::Boolean(false)))
        .assert(claim(the!("audit/flag"), &flagged, Value::Boolean(true)))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let flag = serde_json::json!({
        "the": "review/flag",
        "as": [false, true],
        "cardinality": "many"
    });
    let audit = serde_json::json!({ "the": "audit/flag", "as": "boolean:" });
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "is": flag } },
        "when": [{
            "assert": { "with": { "is": audit } },
            "where": {
                "this": { "?": { "name": "this" } },
                "is": { "?": { "name": "is" } }
            }
        }]
    }))?;
    let mut registry = RuleRegistry::new();
    registry.register(rule.compile()?)?;
    let source = TestEnv::new(&branch, &operator, registry);

    for (pick, expected) in [("max", true), ("min", false)] {
        let mut field = flag.clone();
        field["pick"] = pick.into();
        let concept: ConceptDescriptor =
            serde_json::from_value(serde_json::json!({ "with": { "flag": field } }))?;
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("flag".into(), Term::var("flag"));
        let rows = ConceptQuery {
            terms,
            predicate: concept,
        }
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
        let flags = rows
            .iter()
            .map(|row| row.lookup(&Term::<Any>::var("flag"))?.content())
            .collect::<Result<Vec<Value>, _>>()?;
        assert_eq!(flags, vec![Value::Boolean(expected)], "`{pick}`");
    }
    Ok(())
}
