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

/// The states an account's status lists, best first.
fn states() -> serde_json::Value {
    serde_json::json!(["suspended", "active", "registered"])
}

/// An account: its name, read newest, and its status, elected by
/// `elect` among every state written.
fn account(elect: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": {
            "name": { "the": "account/name", "as": "text:" },
            "status": {
                "the": "account/status",
                "as": states(),
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

/// `top` reads the state listed last among those an account holds,
/// whichever was written last, and `last` the one written last.
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

/// `max` and `min` elect by value.
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
