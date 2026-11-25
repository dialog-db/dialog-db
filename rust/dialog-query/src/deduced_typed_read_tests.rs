//! A typed read of a relation a rule deduces, pinned as tests.
//!
//! A field's declared type says which values it reads, for a stored
//! attribute and for one a rule concludes alike. Each test reads a
//! stored value under a field of each type first, then reads a deduced
//! value under the identical field, and asserts the two answer the
//! same values, each in the same type.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::attribute::{AttributeStatement, Relation};
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{ConceptDescriptor, ConceptQuery, Entity, Match, Parameters, Term, Value};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

/// An attribute, the entity it is of, and the value it holds.
type Statement = (Relation, Entity, Value);

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

/// What a read of `concept` answers under `this` and `value`, each row
/// as its entity and value with their types, sorted.
async fn answers(
    source: &TestEnv<'_>,
    concept: ConceptDescriptor,
    this: Term<Any>,
    value: Term<Any>,
) -> anyhow::Result<Vec<String>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), this.clone());
    terms.insert("value".into(), value.clone());
    let rows = ConceptQuery {
        terms,
        predicate: concept,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let mut answers = rows
        .iter()
        .map(|row| {
            let this = match &this {
                Term::Constant(this) => this.clone(),
                _ => row.lookup(&this)?.content()?,
            };
            let value = match &value {
                Term::Constant(value) => value.clone(),
                _ => row.lookup(&value)?.content()?,
            };
            Ok(format!("{this:?} {value:?}"))
        })
        .collect::<Result<Vec<String>, crate::EvaluationError>>()?;
    answers.sort();
    Ok(answers)
}

/// The types a read is made under in [`differences`].
const TYPES: [&str; 9] = [
    "text:",
    "entity:",
    "boolean:",
    "natural:",
    "integer:",
    "float:",
    "number:",
    "natural:8",
    "float:32",
];

/// The constants a read binds its value to in [`differences`]: one of
/// each type, the wrong type for every read but one.
fn constants() -> Vec<Value> {
    vec![
        text("hello"),
        Value::Boolean(true),
        Value::UnsignedInt(3),
        Value::SignedInt(-3),
        Value::Float(3.0),
        Value::Float(2.5),
    ]
}

/// How a field reads in [`differences`]: under each cardinality with
/// no aggregator.
fn plain() -> Vec<(Option<&'static str>, Option<&'static str>)> {
    CARDINALITIES
        .iter()
        .map(|cardinality| (*cardinality, None))
        .collect()
}

/// Every read of `twin/deduced` that answers other than it should,
/// under each of [`TYPES`] and each of `reads` (a cardinality and a
/// `pick`): with the entity unbound, bound to each of `entities`, and
/// with the value bound to each of [`constants`]. A read answers what
/// the identical read of `twin/stored` answers, except that a read
/// whose value is bound to a constant its type does not read answers
/// nothing: no fact of the field holds a value of another type (the
/// stored read answers there, which
/// [`known_divergence_a_stored_read_answers_a_constant_its_type_does_not_read`]
/// pins). A number constant finds a number of another number type,
/// stored or deduced alike.
async fn differences(
    source: &TestEnv<'_>,
    entities: &[Entity],
    case: &str,
    reads: &[(Option<&str>, Option<&str>)],
    types: &[&str],
) -> anyhow::Result<Vec<String>> {
    let mut terms: Vec<(String, Term<Any>, Term<Any>)> =
        vec![("unbound".into(), Term::var("who"), Term::var("value"))];
    for of in entities {
        terms.push((
            format!("this {of}"),
            Term::Constant(Value::Entity(of.clone())),
            Term::var("value"),
        ));
    }
    for constant in constants() {
        terms.push((
            format!("value {constant:?}"),
            Term::var("who"),
            Term::Constant(constant),
        ));
    }
    let mut wrong = Vec::new();
    for (cardinality, pick) in reads {
        for declared in types {
            let read = |the: &str| {
                let mut field = field(the, Some(declared), *cardinality);
                if let Some(pick) = pick {
                    field["pick"] = (*pick).into();
                }
                concept(field)
            };
            let kind = read("twin/stored")?
                .with()
                .iter()
                .next()
                .and_then(|(_, field)| field.content_type())
                .map(Kind::from);
            for (how, this, value) in &terms {
                let unread = match (value, &kind) {
                    (Term::Constant(constant), Some(kind)) => !kind.reads(constant),
                    _ => false,
                };
                let expected = if unread {
                    Vec::new()
                } else {
                    answers(source, read("twin/stored")?, this.clone(), value.clone()).await?
                };
                let deduced =
                    answers(source, read("twin/deduced")?, this.clone(), value.clone()).await?;
                if expected != deduced {
                    wrong.push(format!(
                        "{case}, {declared} under {cardinality:?} {pick:?}, {how}: expected {expected:?}, deduced {deduced:?}"
                    ));
                }
            }
        }
    }
    Ok(wrong)
}

/// `statements` committed to a fresh branch, read with `rules`, and
/// checked by [`differences`] over `entities`.
async fn case(
    case: &str,
    statements: Vec<Statement>,
    rules: Vec<serde_json::Value>,
    entities: &[Entity],
) -> anyhow::Result<Vec<String>> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
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
    for rule in rules {
        let rule: DeductiveRuleDescriptor = serde_json::from_value(rule)?;
        registry.register(rule.compile()?)?;
    }
    let source = TestEnv::new(&branch, &operator, registry);
    differences(&source, entities, case, &plain(), &TYPES).await
}

fn var(name: &str) -> serde_json::Value {
    serde_json::json!({ "?": { "name": name } })
}

/// The entity, attribute and value of each of `values`, each under
/// `twin/stored` and `from` of an entity of its own. The entities.
fn twinned(
    from: Relation,
    values: Vec<(Value, Value)>,
) -> anyhow::Result<(Vec<Statement>, Vec<Entity>)> {
    let mut statements = Vec::new();
    let mut entities = Vec::new();
    for (source, concluded) in values {
        let of = Entity::new()?;
        statements.push((from.clone(), of.clone(), source));
        statements.push((the!("twin/stored"), of.clone(), concluded));
        entities.push(of);
    }
    Ok((statements, entities))
}

/// A rule whose head binds a constant: every entity with `twin/source`
/// concludes the integer `-3`.
#[dialog_common::test]
async fn a_typed_read_reads_a_constant_a_rule_concludes_as_a_stored_one() -> anyhow::Result<()> {
    let (statements, entities) = twinned(
        the!("twin/source"),
        vec![
            (text("a"), Value::SignedInt(-3)),
            (text("b"), Value::SignedInt(-3)),
        ],
    )?;
    let rule = serde_json::json!({
        "deduce": { "with": { "value": field("twin/deduced", None, Some("many")) } },
        "when": [
            {
                "assert": { "with": { "flag": field("twin/source", None, Some("many")) } },
                "where": { "this": var("this"), "flag": var("flag") }
            },
            { "assert": "==", "where": { "this": var("value"), "is": -3 } }
        ]
    });
    let wrong = case("a constant head", statements, vec![rule], &entities).await?;
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}

/// A rule whose head field is a formula's output: `twin/deduced` is
/// `twin/source` plus one.
#[dialog_common::test]
async fn a_typed_read_reads_a_formula_output_a_rule_concludes_as_a_stored_one() -> anyhow::Result<()>
{
    let (statements, entities) = twinned(
        the!("twin/source"),
        vec![
            (Value::UnsignedInt(3), Value::UnsignedInt(4)),
            (Value::UnsignedInt(7), Value::UnsignedInt(8)),
        ],
    )?;
    let rule = serde_json::json!({
        "deduce": { "with": { "value": field("twin/deduced", None, Some("many")) } },
        "when": [
            {
                "assert": { "with": { "n": field("twin/source", Some("natural:"), Some("many")) } },
                "where": { "this": var("this"), "n": var("n") }
            },
            {
                "assert": "math/sum",
                "where": { "of": var("n"), "with": 1, "is": var("value") }
            }
        ]
    });
    let wrong = case("a formula head", statements, vec![rule], &entities).await?;
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}

/// A field that picks among the claims of a deduced relation, which
/// the engine answers with a selecting rule that folds them, reads
/// through the field's type as over a stored relation. Each entity
/// holds one value here: what a pick elects among several of them is
/// [`known_divergence_how_a_typed_read_elects_among_values_of_several_types`].
#[dialog_common::test]
async fn a_typed_pick_reads_a_deduced_relation_as_a_stored_one() -> anyhow::Result<()> {
    let (statements, entities) = twinned(
        the!("twin/source"),
        [
            text("hello"),
            Value::UnsignedInt(3),
            Value::SignedInt(-3),
            Value::Float(2.5),
        ]
        .into_iter()
        .map(|value| (value.clone(), value))
        .collect(),
    )?;
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    commit(&branch, &operator, statements).await?;
    let mut registry = RuleRegistry::new();
    copy(
        &mut registry,
        field("twin/deduced", None, Some("many")),
        "twin/source",
    )?;
    let source = TestEnv::new(&branch, &operator, registry);
    let mut wrong = differences(
        &source,
        &entities,
        "a pick",
        &[(None, Some("last"))],
        &TYPES,
    )
    .await?;
    let numbers = &TYPES[3..];
    let ordered = [(None, Some("max")), (None, Some("min"))];
    wrong.extend(differences(&source, &entities, "a pick", &ordered, numbers).await?);
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}

/// Commits `statements`, one transaction for all of them.
async fn commit(
    branch: &dialog_repository::Branch,
    operator: &Operator,
    statements: Vec<Statement>,
) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for (the, of, is) in statements {
        transaction = transaction.assert(AttributeStatement {
            the,
            of,
            is,
            cause: None,
            cardinality: Some(crate::Cardinality::Many),
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// What a read of `the` under `declared` and `cardinality` answers with
/// its value bound to `constant`.
async fn bound(
    source: &TestEnv<'_>,
    the: &str,
    declared: &str,
    cardinality: Option<&str>,
    constant: &Value,
) -> anyhow::Result<Vec<String>> {
    answers(
        source,
        concept(field(the, Some(declared), cardinality))?,
        Term::var("who"),
        Term::Constant(constant.clone()),
    )
    .await
}

/// A source holding the natural `3` under `twin/stored` and
/// `twin/source` of one entity and the integer `-3` under both of
/// another, with `twin/deduced` copied from `twin/source`. The two
/// entities.
async fn numbers(
    branch: &dialog_repository::Branch,
    operator: &Operator,
) -> anyhow::Result<(Entity, Entity)> {
    let (natural, negative) = (Entity::new()?, Entity::new()?);
    let mut statements = Vec::new();
    for (of, value) in [
        (&natural, Value::UnsignedInt(3)),
        (&negative, Value::SignedInt(-3)),
    ] {
        for the in [the!("twin/stored"), the!("twin/source")] {
            statements.push((the, of.clone(), value.clone()));
        }
    }
    commit(branch, operator, statements).await?;
    Ok((natural, negative))
}

/// A row of [`answers`]: `of` holding `value`.
fn row(of: &Entity, value: &Value) -> String {
    format!("{:?} {value:?}", Value::Entity(of.clone()))
}

/// A read whose value is bound to a number of another number type than
/// the value the relation holds (a `float:` field bound to the natural
/// `3`, a `natural:` or `number:` field bound to the float `3.0`, over a
/// natural `3`) finds the fact holding that number, stored or deduced,
/// under every cardinality: a number is one value whatever type holds
/// it.
#[dialog_common::test]
async fn a_read_bound_to_a_number_of_another_type_finds_it_stored_or_deduced() -> anyhow::Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (natural, negative) = numbers(&branch, &operator).await?;
    let mut registry = RuleRegistry::new();
    copy(
        &mut registry,
        field("twin/deduced", None, Some("many")),
        "twin/source",
    )?;
    let source = TestEnv::new(&branch, &operator, registry);

    for (declared, constant, of) in [
        ("float:", Value::UnsignedInt(3), &natural),
        ("float:", Value::SignedInt(-3), &negative),
        ("natural:", Value::Float(3.0), &natural),
        ("integer:", Value::Float(3.0), &natural),
        ("number:", Value::Float(3.0), &natural),
    ] {
        let found = vec![row(of, &constant)];
        for cardinality in CARDINALITIES {
            assert_eq!(
                bound(&source, "twin/stored", declared, cardinality, &constant).await?,
                found,
                "the stored {declared} read under {cardinality:?} finds {constant:?}"
            );
            assert_eq!(
                bound(&source, "twin/deduced", declared, cardinality, &constant).await?,
                found,
                "the deduced {declared} read under {cardinality:?} finds {constant:?}"
            );
        }
    }
    Ok(())
}

/// A known divergence, pinned as it is today, and a wrong answer on the
/// stored side: a read whose value is bound to a constant its type does
/// not read (a `text:` field bound to the integer `-3`) answers the
/// stored fact holding that constant, though no fact of the field holds
/// a value of another type. The same read of a deduced relation holding
/// it answers nothing. A fix makes the stored read answer nothing, and
/// flips this pin.
#[dialog_common::test]
async fn known_divergence_a_stored_read_answers_a_constant_its_type_does_not_read()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (_, negative) = numbers(&branch, &operator).await?;
    let mut registry = RuleRegistry::new();
    copy(
        &mut registry,
        field("twin/deduced", None, Some("many")),
        "twin/source",
    )?;
    let source = TestEnv::new(&branch, &operator, registry);

    let constant = Value::SignedInt(-3);
    assert_eq!(
        bound(&source, "twin/stored", "text:", None, &constant).await?,
        vec![row(&negative, &constant)],
        "the stored text: read answers the integer it was bound to"
    );
    assert_eq!(
        bound(&source, "twin/deduced", "text:", None, &constant).await?,
        Vec::<String>::new(),
        "the deduced text: read answers nothing"
    );
    Ok(())
}

/// A known divergence, pinned as it is today: how a typed read elects
/// among the values of one cell when they are of several types. One
/// entity holds four values, written under cardinality many one commit
/// each, stored and deduced alike, in one of two orders:
///
/// - A: `hello`, `3`, `-3`, `2.5` (the newest is `2.5`)
/// - B: `3`, `-3`, `2.5`, `hello` (the newest is `hello`)
///
/// | read                     | A stored | A deduced | B stored | B deduced |
/// |--------------------------|----------|-----------|----------|-----------|
/// | `natural:`, `pick: max`  | 2        | 3         | 2        | 3         |
/// | `float:`, `pick: max`    | 2.5      | 3.0       | 2.5      | 3.0       |
/// | `natural:`, no pick      | 2        | 2         | nothing  | 2         |
/// | `text:`, `pick: last`    | nothing  | hello     | hello    | hello     |
///
/// Two questions are open, and their answers flip named assertions:
///
/// - A plain cardinality-one read: the stored read elects the newest
///   value of all and then reads it through the type (B answers
///   nothing); the deduced read reads every value through the type and
///   elects the newest of those, as a read through a typed variable
///   does (B answers `2`).
/// - A pick: the stored read drops what the type does not read, keeps
///   the newest of the rest and picks among that one (`max` answers
///   `2`); the deduced read picks among every value the type reads
///   (`max` answers `3`).
#[dialog_common::test]
async fn known_divergence_how_a_typed_read_elects_among_values_of_several_types()
-> anyhow::Result<()> {
    let nothing = None;
    let natural = |n: u128| Some(Value::UnsignedInt(n));
    let float = |n: f64| Some(Value::Float(n));
    let hello = Some(text("hello"));
    // The read, then what each of A stored, A deduced, B stored and B
    // deduced answers.
    let table = [
        (
            ("natural:", Some("max")),
            [natural(2), natural(3), natural(2), natural(3)],
        ),
        (
            ("float:", Some("max")),
            [float(2.5), float(3.0), float(2.5), float(3.0)],
        ),
        (
            ("natural:", None),
            [natural(2), natural(2), nothing.clone(), natural(2)],
        ),
        (
            ("text:", Some("last")),
            [nothing, hello.clone(), hello.clone(), hello],
        ),
    ];
    let orders = [
        (
            "A",
            [
                text("hello"),
                Value::UnsignedInt(3),
                Value::SignedInt(-3),
                Value::Float(2.5),
            ],
        ),
        (
            "B",
            [
                Value::UnsignedInt(3),
                Value::SignedInt(-3),
                Value::Float(2.5),
                text("hello"),
            ],
        ),
    ];
    for (index, (order, values)) in orders.into_iter().enumerate() {
        let (operator, profile) = test_session_with_peer().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let of = Entity::new()?;
        for value in values {
            commit(
                &branch,
                &operator,
                vec![
                    (the!("twin/stored"), of.clone(), value.clone()),
                    (the!("twin/source"), of.clone(), value),
                ],
            )
            .await?;
        }
        let mut registry = RuleRegistry::new();
        copy(
            &mut registry,
            field("twin/deduced", None, Some("many")),
            "twin/source",
        )?;
        let source = TestEnv::new(&branch, &operator, registry);
        for ((declared, pick), answers_of) in &table {
            for (side, the) in ["stored", "deduced"]
                .into_iter()
                .zip(["twin/stored", "twin/deduced"])
            {
                let mut field = field(the, Some(declared), None);
                if let Some(pick) = pick {
                    field["pick"] = (*pick).into();
                }
                let expected: Vec<String> = answers_of[index * 2 + usize::from(side == "deduced")]
                    .iter()
                    .map(|value| row(&of, value))
                    .collect();
                assert_eq!(
                    answers(
                        &source,
                        concept(field)?,
                        Term::var("who"),
                        Term::var("value")
                    )
                    .await?,
                    expected,
                    "order {order}, {side}, {declared} pick {pick:?}"
                );
            }
        }
    }
    Ok(())
}

/// A rule that concludes some entities only, beside entities whose
/// value is stored under the relation itself: the stored candidates and
/// the deduced ones are each read through the type.
#[dialog_common::test]
async fn a_typed_read_reads_a_rule_concluding_some_entities_as_a_stored_one() -> anyhow::Result<()>
{
    let (mut statements, mut entities) = twinned(
        the!("twin/source"),
        vec![
            (text("hello"), text("hello")),
            (Value::UnsignedInt(3), Value::UnsignedInt(3)),
            (Value::Float(2.5), Value::Float(2.5)),
        ],
    )?;
    for value in [Value::SignedInt(-3), text("stored"), Value::UnsignedInt(9)] {
        let of = Entity::new()?;
        statements.push((the!("twin/stored"), of.clone(), value.clone()));
        statements.push((the!("twin/deduced"), of.clone(), value));
        entities.push(of);
    }
    let wrong = case(
        "some entities",
        statements,
        vec![serde_json::json!({
            "deduce": { "with": { "value": field("twin/deduced", None, Some("many")) } },
            "when": [{
                "assert": { "with": { "value": field("twin/source", None, Some("many")) } },
                "where": { "this": var("this"), "value": var("value") }
            }]
        })],
        &entities,
    )
    .await?;
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}

/// A rule registered after a read of the relation was planned and its
/// bundle cached: the next read takes the rule, and reads its values
/// through the type, whether the rule is the read's whole answer or
/// stands beside a fixpoint.
#[dialog_common::test]
async fn a_typed_read_reads_a_rule_installed_after_a_cached_read_as_a_stored_one()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let (statements, entities) = twinned(
        the!("twin/source"),
        vec![
            (text("hello"), text("hello")),
            (Value::UnsignedInt(3), Value::UnsignedInt(3)),
            (Value::SignedInt(-3), Value::SignedInt(-3)),
        ],
    )?;
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
    let source = TestEnv::new(&branch, &operator, registry.clone());
    for declared in TYPES {
        for cardinality in CARDINALITIES {
            let read = concept(field("twin/deduced", Some(declared), cardinality))?;
            assert!(
                answers(&source, read, Term::var("who"), Term::var("value"))
                    .await?
                    .is_empty(),
                "nothing derives the relation yet"
            );
        }
    }
    let deduced = field("twin/deduced", None, Some("many"));
    copy(&mut registry, deduced.clone(), "twin/source")?;
    let mut wrong = differences(&source, &entities, "installed after", &plain(), &TYPES).await?;
    copy(&mut registry, deduced, "twin/deduced")?;
    wrong.extend(
        differences(
            &source,
            &entities,
            "recursive, installed after",
            &plain(),
            &TYPES,
        )
        .await?,
    );
    assert!(
        wrong.is_empty(),
        "reads that differ from the stored twin: {wrong:#?}"
    );
    Ok(())
}
