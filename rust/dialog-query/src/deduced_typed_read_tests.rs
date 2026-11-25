//! A typed read of a relation a rule deduces, pinned as tests.
//!
//! A field's declared type says which values it reads, for a stored
//! attribute and for one a rule concludes alike. Each test reads a
//! stored value under a field of each type first, then reads a deduced
//! value under the identical field, and asserts the two answer the
//! same values, each in the same type.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::AttributeStatement;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::types::Any;
use crate::{ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};

fn text(text: &str) -> Value {
    Value::String(text.into())
}

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

/// The one-field concept over `field`.
fn concept(field: serde_json::Value) -> anyhow::Result<ConceptDescriptor> {
    Ok(serde_json::from_value(
        serde_json::json!({ "with": { "value": field } }),
    )?)
}

/// What a read of `concept` binds its field to, for every entity.
async fn values(source: &TestEnv<'_>, concept: ConceptDescriptor) -> anyhow::Result<Vec<Value>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery {
        terms,
        predicate: concept,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    Ok(rows
        .iter()
        .map(|row| row.lookup(&Term::<Any>::var("value"))?.content())
        .collect::<Result<Vec<Value>, crate::EvaluationError>>()?)
}

/// `note/body` of `head` (a field over it) concluded from `source/body`.
fn copying(head: serde_json::Value) -> anyhow::Result<RuleRegistry> {
    let mut registry = RuleRegistry::new();
    copy(&mut registry, head, "source/body")?;
    Ok(registry)
}

/// The relation of `head` (a field over it) concluded from `from`, read
/// with no type.
fn copy(registry: &mut RuleRegistry, head: serde_json::Value, from: &str) -> anyhow::Result<()> {
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": head } },
        "when": [{
            "assert": { "with": { "value": { "the": from, "cardinality": "many" } } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    registry.register(rule.compile()?)?;
    Ok(())
}

/// `probe/hit` of every entity whose `note/body`, read under
/// `declared`, is anything: a join through the typed read.
fn probing(registry: &mut RuleRegistry, declared: &str) -> anyhow::Result<()> {
    let rule: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
        "deduce": { "with": { "value": { "the": "probe/hit", "cardinality": "many" } } },
        "when": [{
            "assert": { "with": { "value": field("note/body", Some(declared), Some("many")) } },
            "where": {
                "this": { "?": { "name": "this" } },
                "value": { "?": { "name": "value" } }
            }
        }]
    }))?;
    registry.register(rule.compile()?)?;
    Ok(())
}

/// The types a field over the text `hello` is read under, and what each
/// answers.
fn reads() -> Vec<(&'static str, Vec<Value>)> {
    vec![
        ("text:", vec![text("hello")]),
        ("integer:", vec![]),
        ("natural:", vec![]),
        ("entity:", vec![]),
        ("boolean:", vec![]),
    ]
}

/// The cardinalities each read is made under.
const CARDINALITIES: [Option<&str>; 3] = [None, Some("many"), Some("one")];

/// A field of a type reads a stored text when it is text and nothing
/// when it is another type, under each cardinality: the twin the
/// deduced reads below answer the same as.
#[dialog_common::test]
async fn a_typed_field_reads_a_stored_text_only_as_text() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    branch
        .transaction()
        .assert(the!("note/body").of(Entity::new()?).is("hello".to_string()))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for cardinality in CARDINALITIES {
        for (declared, expected) in reads() {
            assert_eq!(
                values(
                    &source,
                    concept(field("note/body", Some(declared), cardinality))?
                )
                .await?,
                expected,
                "a stored text read as {declared} under {cardinality:?}"
            );
        }
    }
    Ok(())
}

/// A rule whose body reads the relation under a type joins through
/// that read as it does through a stored attribute: the stored text
/// and the deduced text each reach `probe/hit` through a text read and
/// neither does through a read of another type. This held before a
/// read applied its field's type to deduced values, because the body's
/// variable takes the field's type by inference and the read filters
/// by the variable's type; it is pinned so it keeps holding.
#[dialog_common::test]
async fn a_join_through_a_typed_read_of_a_deduced_text_answers_as_a_stored_one()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (stored, deduced) = (Entity::new()?, Entity::new()?);
    branch
        .transaction()
        .assert(the!("note/body").of(stored.clone()).is("hello".to_string()))
        .assert(
            the!("source/body")
                .of(deduced.clone())
                .is("hello".to_string()),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let hits = concept(field("probe/hit", None, Some("many")))?;
    for (declared, expected) in reads() {
        let mut stored_only = RuleRegistry::new();
        probing(&mut stored_only, declared)?;
        let source = TestEnv::new(&branch, &operator, stored_only);
        assert_eq!(
            values(&source, hits.clone()).await?,
            expected,
            "a join through a stored text read as {declared}"
        );

        let mut both = copying(field("note/body", None, Some("many")))?;
        probing(&mut both, declared)?;
        let source = TestEnv::new(&branch, &operator, both);
        let doubled: Vec<Value> = expected
            .iter()
            .flat_map(|v| [v.clone(), v.clone()])
            .collect();
        assert_eq!(
            values(&source, hits.clone()).await?,
            doubled,
            "a join through a stored and a deduced text read as {declared}"
        );
    }
    Ok(())
}

/// What a read of `concept` binds its field to, for every entity, each
/// value with its type: a number read as another type comes back as
/// that type, so the debug rendering tells `3` from `3.0`.
async fn rendered(source: &TestEnv<'_>, concept: ConceptDescriptor) -> anyhow::Result<Vec<String>> {
    let mut rendered: Vec<String> = values(source, concept)
        .await?
        .iter()
        .map(|value| format!("{value:?}"))
        .collect();
    rendered.sort();
    Ok(rendered)
}

/// How a read of `twin/deduced` is evaluated.
#[derive(Debug, Clone, Copy)]
enum Shape {
    /// One rule derives the relation and nothing is stored under it:
    /// the rule is the read's whole answer.
    Exact,
    /// A value is stored under the relation too: the stored scan and
    /// the rule each offer candidates.
    Relation,
    /// A second rule derives the relation from itself: the read goes
    /// through the fixpoint.
    Fixpoint,
}

/// Every read of `twin/deduced` under each of `declared` and each
/// cardinality that answers other than the identical read of
/// `twin/stored`. Each of `values` is stored under `twin/stored` and
/// `twin/source` of an entity of its own, and a rule concludes
/// `twin/deduced` from `twin/source` with a head of no type, so the
/// two relations hold the same values. Under [`Shape::Relation`] an
/// entity holds `extra` under both `twin/stored` and `twin/deduced`.
async fn twin(
    values: &[Value],
    extra: Value,
    declared: &[&str],
    shape: Shape,
) -> anyhow::Result<Vec<String>> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut statements = Vec::new();
    for value in values {
        let of = Entity::new()?;
        for the in [the!("twin/stored"), the!("twin/source")] {
            statements.push((the, of.clone(), value.clone()));
        }
    }
    if let Shape::Relation = shape {
        let of = Entity::new()?;
        for the in [the!("twin/stored"), the!("twin/deduced")] {
            statements.push((the, of.clone(), extra.clone()));
        }
    }
    let mut transaction = branch.transaction();
    for (the, of, is) in statements {
        transaction = transaction.assert(AttributeStatement {
            the,
            of,
            is,
            cause: None,
            cardinality: None,
        });
    }
    transaction.commit().publish().perform(&operator).await?;

    let mut registry = RuleRegistry::new();
    let deduced = field("twin/deduced", None, Some("many"));
    copy(&mut registry, deduced.clone(), "twin/source")?;
    if let Shape::Fixpoint = shape {
        copy(&mut registry, deduced, "twin/deduced")?;
        assert!(
            registry.analysis()?.quarantined().is_empty(),
            "the self-reading rule makes the relation recursive"
        );
    }
    let source = TestEnv::new(&branch, &operator, registry);

    let mut wrong = Vec::new();
    for cardinality in CARDINALITIES {
        for declared in declared {
            let read = |the: &str| concept(field(the, Some(declared), cardinality));
            let stored = rendered(&source, read("twin/stored")?).await?;
            let deduced = rendered(&source, read("twin/deduced")?).await?;
            if stored != deduced {
                wrong.push(format!(
                    "{shape:?}, {declared} under {cardinality:?}: stored {stored:?}, deduced {deduced:?}"
                ));
            }
        }
    }
    Ok(wrong)
}

/// A field of a type reads a deduced text as it reads a stored one:
/// when it is text, and not at all when it is another type, under each
/// cardinality and however the read is evaluated.
#[dialog_common::test]
async fn a_typed_field_reads_a_deduced_text_only_as_text() -> anyhow::Result<()> {
    let types: Vec<&str> = reads().into_iter().map(|(declared, _)| declared).collect();
    let mut wrong = Vec::new();
    for shape in [Shape::Exact, Shape::Relation, Shape::Fixpoint] {
        wrong.extend(twin(&[text("hello")], text("stored"), &types, shape).await?);
    }
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}

/// A field of a number type reads a deduced number as it reads a
/// stored one: the same numbers, each in the type the stored read
/// gives it (a natural read as a float is a float, a fraction read as
/// a natural is floored, a negative is not a natural), under each
/// cardinality and however the read is evaluated.
#[dialog_common::test]
async fn a_typed_field_reads_a_deduced_number_as_a_stored_one() -> anyhow::Result<()> {
    let numbers = [
        Value::UnsignedInt(3),
        Value::SignedInt(-3),
        Value::Float(2.5),
        Value::Float(4.0),
        text("hello"),
    ];
    let types = [
        "natural:",
        "integer:",
        "float:",
        "number:",
        "natural:8",
        "integer:8",
        "float:32",
    ];
    let mut wrong = Vec::new();
    for shape in [Shape::Exact, Shape::Relation, Shape::Fixpoint] {
        wrong.extend(twin(&numbers, Value::Float(7.5), &types, shape).await?);
    }
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}
