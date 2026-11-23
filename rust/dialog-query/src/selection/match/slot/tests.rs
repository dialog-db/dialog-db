//! What reading a scanned slot costs: asking whether a variable is
//! bound must not hydrate the value it is bound to.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::{Part, Slot};
use crate::Binding;
use crate::attribute::{AttributeStatement, Relation};
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::{DynamicAttributeQuery, Entity, Match, Selection, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};

/// `Match::contains` on a variable a scan bound in place answers from
/// the slot, as `Match::absent` and `Match::is_present` do, and leaves
/// the scanned value unmaterialized. It goes through `find`, which
/// reads the slot's binding, so the value cell is filled just to learn
/// the variable is bound.
#[dialog_common::test]
async fn contains_does_not_hydrate_a_scanned_value() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(AttributeStatement {
            the: "slot/text".parse()?,
            of: Entity::new()?,
            is: Value::String("hello".into()),
            cause: None,
            cardinality: None,
        })
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let rows = DynamicAttributeQuery::new(
        Term::from("slot/text".parse::<Relation>()?),
        Term::<Entity>::var("e"),
        Term::var("v"),
        Term::var("cause"),
        None,
    )
    .evaluate(&source, Match::new().seed())
    .try_vec()
    .await?;
    let [row] = rows.as_slice() else {
        panic!("expected one row, got {}", rows.len());
    };
    let (scanned, part) = row.scanned("v").expect("the scan binds v in place");
    assert_eq!(part, Part::Is);
    assert!(scanned.is.get().is_none(), "nothing has read v yet");

    assert!(row.contains(&Term::var("v")), "v is bound");
    assert!(
        scanned.is.get().is_none(),
        "expected contains to leave v unmaterialized, but it hydrated it"
    );
    Ok(())
}

/// `Slot::agrees` settles a comparison by bytes only when the bytes
/// settle it: whenever it answers, it answers as comparing the
/// materialized bindings does. Over every pairing of scanned and bound
/// slots, in both orders, of values whose bytes coincide across types:
/// an entity and a text of one URI, a symbol and a text of one name,
/// bytes and a record of one buffer, empty texts and buffers, numbers
/// of several types, and each scanned row's attribute and entity.
#[dialog_common::test]
async fn agrees_answers_as_the_bindings_compare() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let values = vec![
        Value::String("user:bob".into()),
        Value::Entity("user:bob".parse()?),
        Value::String("slot/a".into()),
        Value::Symbol("slot/a".parse()?),
        Value::Bytes(vec![1, 2]),
        Value::Record(vec![1, 2]),
        Value::Bytes(vec![]),
        Value::String(String::new()),
        Value::Record(vec![]),
        Value::String("a\0b".into()),
        Value::UnsignedInt(3),
        Value::SignedInt(3),
        Value::Float(3.0),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Boolean(true),
        Value::Boolean(false),
    ];
    let of: Entity = "user:bob".parse()?;
    let mut transaction = branch.transaction();
    for (index, value) in values.iter().enumerate() {
        transaction = transaction.assert(AttributeStatement {
            the: format!("slot/v{index}").parse()?,
            of: of.clone(),
            is: value.clone(),
            cause: None,
            cardinality: None,
        });
    }
    transaction.commit().publish().perform(&operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let rows = DynamicAttributeQuery::new(
        Term::<Relation>::var("the"),
        Term::Constant(Value::Entity(of.clone())),
        Term::var("v"),
        Term::var("cause"),
        None,
    )
    .evaluate(&source, Match::new().seed())
    .try_vec()
    .await?;
    assert_eq!(rows.len(), values.len(), "every value is scanned");

    let mut slots: Vec<(String, Slot)> = Vec::new();
    for row in &rows {
        for name in ["v", "the"] {
            let (scanned, part) = row.scanned(name).expect("bound in place");
            slots.push((format!("scanned {part:?}"), Slot::scanned(scanned, part)));
        }
    }
    for value in &values {
        slots.push(("bound".into(), Slot::Bound(Binding::Present(value.clone()))));
    }
    slots.push(("bound".into(), Slot::Bound(Binding::Absent)));

    let mut wrong = Vec::new();
    for (left_name, left) in &slots {
        for (right_name, right) in &slots {
            let Some(agrees) = left.agrees(right) else {
                continue;
            };
            let equal = left.binding() == right.binding();
            if agrees != equal {
                wrong.push(format!(
                    "{left_name} {:?} vs {right_name} {:?}: agrees says {agrees}, the bindings \
                     compare {equal}",
                    left.binding(),
                    right.binding()
                ));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "expected agrees to answer as the bindings compare; {} pairings differ:\n{}",
        wrong.len(),
        wrong.join("\n")
    );
    Ok(())
}
