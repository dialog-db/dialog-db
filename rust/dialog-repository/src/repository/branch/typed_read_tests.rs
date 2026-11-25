//! A typed read of a relation a rule deduces, through a branch, pinned
//! as tests.
//!
//! A field's declared type reads a value a rule concludes as it reads a
//! stored one, wherever the rule is held: committed on the branch, held
//! in its session overlay, or carried by one query's layer, and with the
//! session overlay holding values of the relation itself beside it.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use anyhow::Result;
use dialog_corpus::{Entity, Value};
use dialog_peer::helpers::test_session_with_peer;
use dialog_peer::{Peer, PeerSpace, Session};
use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::concept::query::ConceptQuery;
use dialog_query::query::Output as _;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::{AttributeStatement, DeductiveRule, Parameters, Term, the};

use crate::Branch;
use crate::helpers::test_repo;

fn text(text: &str) -> Value {
    Value::String(text.into())
}

/// The types each read is made under.
const TYPES: [&str; 7] = [
    "text:",
    "entity:",
    "boolean:",
    "natural:",
    "integer:",
    "float:",
    "natural:8",
];

/// The cardinalities each read is made under.
const CARDINALITIES: [Option<&str>; 3] = [None, Some("many"), Some("one")];

/// A field over `the`, declared `as` (none for no type) and read under
/// `cardinality` (none for the default).
fn field(the: &str, declared: Option<&str>, cardinality: Option<&str>) -> serde_json::Value {
    let mut field = serde_json::Map::new();
    field.insert("the".into(), the.into());
    if let Some(declared) = declared {
        field.insert("as".into(), declared.into());
    }
    if let Some(cardinality) = cardinality {
        field.insert("cardinality".into(), cardinality.into());
    }
    serde_json::Value::Object(field)
}

/// `twin/deduced` concluded from `twin/source`, both with no type.
fn copying() -> Result<DeductiveRule> {
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": field("twin/deduced", None, Some("many")) } },
        "when": [{
            "assert": { "with": { "value": field("twin/source", None, Some("many")) } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    Ok(rule.compile()?)
}

/// A statement of `the` of `of` holding `is`.
fn statement(the: dialog_query::Relation, of: &Entity, is: Value) -> AttributeStatement {
    AttributeStatement {
        the,
        of: of.clone(),
        is,
        cause: None,
        cardinality: None,
    }
}

/// Commits a text, a natural, a negative integer and a fraction, each
/// under `twin/stored` and `twin/source` of an entity of its own.
async fn seed<S: PeerSpace>(branch: &Branch, session: &Peer<S, Session>) -> Result<()> {
    #[allow(deprecated)]
    let mut transaction = branch.transaction();
    for value in [
        text("hello"),
        Value::UnsignedInt(3),
        Value::SignedInt(-3),
        Value::Float(2.5),
    ] {
        let of = Entity::new()?;
        transaction = transaction
            .assert(statement(the!("twin/stored"), &of, value.clone()))
            .assert(statement(the!("twin/source"), &of, value));
    }
    #[allow(deprecated)]
    transaction.commit().publish().perform(session).await?;
    Ok(())
}

/// What a read of `the` under `declared` and `cardinality` answers on
/// `branch`, with `rule` carried by the query's layer when given, each
/// row as its entity and value with their types, sorted.
async fn read<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    the: &str,
    declared: &str,
    cardinality: Option<&str>,
    rule: Option<&DeductiveRule>,
) -> Result<Vec<String>> {
    let predicate: ConceptDescriptor = serde_json::from_value(serde_json::json!({
        "with": { "value": field(the, Some(declared), cardinality) }
    }))?;
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("value".into(), Term::var("value"));
    let query = ConceptQuery { predicate, terms };
    let rows = match rule {
        Some(rule) => {
            branch
                .query()
                .with(rule)
                .select(query)
                .perform(session)
                .try_vec()
                .await?
        }
        None => {
            branch
                .query()
                .select(query)
                .perform(session)
                .try_vec()
                .await?
        }
    };
    let mut answers: Vec<String> = rows
        .iter()
        .map(|row| {
            format!(
                "{:?} {:?}",
                row.entity(),
                row.source().value_of("value").cloned()
            )
        })
        .collect();
    answers.sort();
    Ok(answers)
}

/// Every read of `twin/deduced` on `branch` that answers other than
/// the identical read of `twin/stored`.
async fn differences<S: PeerSpace>(
    branch: &Branch,
    session: &Peer<S, Session>,
    rule: Option<&DeductiveRule>,
    case: &str,
) -> Result<Vec<String>> {
    let mut wrong = Vec::new();
    for cardinality in CARDINALITIES {
        for declared in TYPES {
            let stored = read(branch, session, "twin/stored", declared, cardinality, rule).await?;
            let deduced =
                read(branch, session, "twin/deduced", declared, cardinality, rule).await?;
            if stored != deduced {
                wrong.push(format!(
                    "{case}, {declared} under {cardinality:?}: stored {stored:?}, deduced {deduced:?}"
                ));
            }
        }
    }
    Ok(wrong)
}

/// A rule committed on the branch, held in its session overlay, or
/// carried by a query's layer: a typed read of what it concludes reads
/// as a stored one, also while the session overlay holds values of the
/// relation itself.
#[dialog_common::test]
async fn a_typed_read_of_a_relation_a_branch_rule_deduces_reads_as_a_stored_one() -> Result<()> {
    let (session, peer) = test_session_with_peer().await;
    let repo = test_repo(&session, &peer).await;
    let rule = copying()?;
    let mut wrong = Vec::new();

    let committed = repo.branch("committed").open().perform(&session).await?;
    seed(&committed, &session).await?;
    #[allow(deprecated)]
    committed
        .transaction()
        .assert(&rule)
        .commit()
        .publish()
        .perform(&session)
        .await?;
    wrong.extend(differences(&committed, &session, None, "committed").await?);

    let overlaid = repo.branch("overlaid").open().perform(&session).await?;
    seed(&overlaid, &session).await?;
    overlaid.overlay().assert(&rule)?;
    wrong.extend(differences(&overlaid, &session, None, "in the session overlay").await?);

    let layered = repo.branch("layered").open().perform(&session).await?;
    seed(&layered, &session).await?;
    wrong.extend(differences(&layered, &session, Some(&rule), "in a query layer").await?);

    // The session overlay holds values of the relation the committed
    // rule concludes: the stored candidates and the deduced ones meet.
    for value in [text("stored"), Value::UnsignedInt(9), Value::Float(7.5)] {
        let of = Entity::new()?;
        committed
            .overlay()
            .assert(statement(the!("twin/stored"), &of, value.clone()))?;
        committed
            .overlay()
            .assert(statement(the!("twin/deduced"), &of, value))?;
    }
    wrong.extend(differences(&committed, &session, None, "beside an overlay").await?);

    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}
