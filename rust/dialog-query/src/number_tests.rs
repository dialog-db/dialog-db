//! Numbers through the query engine, pinned as tests.
//!
//! The store keys every number in one band by value: the natural `3`,
//! the integer `+3` and the float `3.0` are one value, and a raw read
//! gives the narrowest type holding the number. The number types are
//! lenses over that one domain: `natural:` floors a fraction and does
//! not see a negative, a sized type sees only its range, a float type
//! sees every number (`float:32` through `f32`). Writes are strict:
//! a field takes only what its type holds exactly. Each test here
//! states one of those claims and checks it end to end, from a stored
//! fact through a scan, a concept field, a rule or an encoding.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::artifact::{ArtifactsRelation, Type};
use crate::attribute::query::all::AttributeQueryAll;
use crate::attribute::{AttributeStatement, Relation};
use crate::concept::descriptor::ConceptStatement;
use crate::query::Application as _;
use crate::query::Output as _;
use crate::rule::DeductiveRuleDescriptor;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::type_system::{Primitive, Type as Kind};
use crate::types::Any;
use crate::{
    Cardinality, Claim, ConceptDescriptor, ConceptQuery, DeductiveRule, DynamicAttributeQuery,
    Entity, Match, Parameters, Term, TypeError, Value,
};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn natural(number: u128) -> Value {
    Value::UnsignedInt(number)
}

fn integer(number: i128) -> Value {
    Value::SignedInt(number)
}

fn float(number: f64) -> Value {
    Value::Float(number)
}

/// `values` in one order on every target: by their debug rendering,
/// since [`Value`] has no total order.
fn sorted(mut values: Vec<Value>) -> Vec<Value> {
    values.sort_by_key(|value| format!("{value:?}"));
    values
}

/// The relation `name` names, composed at runtime.
fn relation(name: &str) -> anyhow::Result<Relation> {
    Ok(Relation::from(ArtifactsRelation::try_from(
        name.to_string(),
    )?))
}

/// Store each of `values` under `the` of its own fresh entity, in one
/// commit, and return the entities in the order of the values.
async fn store(
    branch: &Branch,
    operator: &Operator,
    the: &Relation,
    values: &[Value],
) -> anyhow::Result<Vec<Entity>> {
    let mut entities = Vec::new();
    let mut transaction = branch.transaction();
    for value in values {
        let of = Entity::new()?;
        entities.push(of.clone());
        transaction = transaction.assert(AttributeStatement {
            the: the.clone(),
            of,
            is: value.clone(),
            cause: None,
            cardinality: None,
            pick: None,
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(entities)
}

/// Every value an `AttributeQueryAll` over `the` yields into the `is`
/// term, sorted.
async fn scan(source: &TestEnv<'_>, the: &Relation, is: Term<Any>) -> anyhow::Result<Vec<Value>> {
    let query = AttributeQueryAll::new(
        Term::from(the.clone()),
        Term::<Entity>::var("e"),
        is,
        Term::var("cause"),
    );
    let results = query.perform(source).try_vec().await?;
    Ok(sorted(
        results.iter().map(|claim| claim.is().clone()).collect(),
    ))
}

/// A one-field concept over `the`, declared `as` (none for an untyped
/// field) and read under `pick` (none for the default).
fn field_concept(the: &str, declared: Option<&str>, pick: Option<&str>) -> ConceptDescriptor {
    let mut field = serde_json::Map::new();
    field.insert("the".into(), serde_json::Value::String(the.into()));
    if let Some(declared) = declared {
        field.insert("as".into(), serde_json::Value::String(declared.into()));
    }
    if let Some(pick) = pick {
        field.insert("pick".into(), serde_json::Value::String(pick.into()));
    }
    serde_json::from_value(serde_json::json!({ "with": { "value": field } }))
        .expect("a one-field concept is well formed")
}

/// The values the field of `concept` binds for every entity, sorted.
async fn field_values(
    source: &TestEnv<'_>,
    concept: &ConceptDescriptor,
) -> anyhow::Result<Vec<Value>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::var("value"));
    let rows = ConceptQuery {
        terms,
        predicate: concept.clone(),
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let values = rows
        .iter()
        .map(|row| row.lookup(&Term::<Any>::var("value"))?.content())
        .collect::<Result<Vec<Value>, crate::EvaluationError>>()?;
    Ok(sorted(values))
}

/// How many entities the field of `concept` has equal to `constant`.
async fn field_matches(
    source: &TestEnv<'_>,
    concept: &ConceptDescriptor,
    constant: Value,
) -> anyhow::Result<usize> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("who"));
    terms.insert("value".into(), Term::Constant(constant));
    let rows = ConceptQuery {
        terms,
        predicate: concept.clone(),
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    Ok(rows.len())
}

/// The statement writing `value` into the one field of `concept` for a
/// fresh entity, type-checked as the concept's builder checks it (the
/// builder takes typed Rust values; this takes a [`Value`]).
fn typed_write(concept: &ConceptDescriptor, value: Value) -> Result<ConceptStatement, TypeError> {
    let (name, field) = concept.with().iter().next().expect("a concept has a field");
    let attribution = field
        .descriptor()
        .resolve(value)
        .map_err(|error| error.at(name.to_string()))?;
    Ok(ConceptStatement {
        this: Entity::new().expect("a fresh entity"),
        with: vec![attribution],
    })
}

/// The live claims of `the` of `of`: the one a cardinality-one read
/// elects.
async fn live_claims(
    source: &TestEnv<'_>,
    the: &Relation,
    of: &Entity,
) -> anyhow::Result<Vec<Claim>> {
    Ok(DynamicAttributeQuery::new(
        Term::from(the.clone()),
        Term::from(of.clone()),
        Term::var("v"),
        Term::var("cause"),
        Some(Cardinality::One),
    )
    .perform(source)
    .try_vec()
    .await?)
}

fn compile(json: serde_json::Value) -> anyhow::Result<DeductiveRule> {
    let descriptor: DeductiveRuleDescriptor = serde_json::from_value(json)?;
    Ok(descriptor.compile()?)
}

/// `head(this) := v :- body(this) = v`, with the head declared `head_as`
/// and the body field read `body_as`.
fn copy_rule(
    head: &str,
    head_as: &str,
    body: &str,
    body_as: &str,
) -> anyhow::Result<DeductiveRule> {
    compile(serde_json::json!({
        "deduce": { "with": { "v": { "the": head, "as": head_as, "pick": "all:" } } },
        "when": [{
            "assert": { "with": { "v": { "the": body, "as": body_as, "pick": "all:" } } },
            "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "v" } } }
        }]
    }))
}

/// A constant names a number, not a spelling, through a cardinality-one
/// scan as through a cardinality-many one: `+41` and `41.0` find the
/// stored natural `41`, `-41` and `41.5` find nothing, and the claim
/// found carries the number as the constant's type views it (the
/// integer `41` for `+41`, the float `41.0` for `41.0`), as a typed
/// bound's scan does.
#[dialog_common::test]
async fn a_constant_of_any_number_type_finds_the_number_through_a_cardinality_one_scan()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(&branch, &operator, &the!("person/age"), &[natural(41)]).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for (constant, found) in [
        (natural(41), 1),
        (integer(41), 1),
        (float(41.0), 1),
        (integer(-41), 0),
        (float(41.5), 0),
        (natural(42), 0),
    ] {
        for cardinality in [Cardinality::One, Cardinality::Many] {
            let query = DynamicAttributeQuery::new(
                Term::from(the!("person/age")),
                Term::<Entity>::var("e"),
                Term::Constant(constant.clone()),
                Term::var("cause"),
                Some(cardinality),
            );
            let results = query.perform(&source).try_vec().await?;
            assert_eq!(results.len(), found, "{constant:?} under {cardinality:?}");
            for claim in results {
                assert_eq!(
                    claim.is(),
                    &constant,
                    "the claim holds the number as the constant views it"
                );
            }
        }
    }
    Ok(())
}

/// A numeric constant matches through the lens of its own type, at
/// every layer: over stored `4`, `4.0`, `4.3`, `-3` and `-2.5`, the
/// natural `4` and the integer `4` find `4`, `4.0` and `4.3` (the
/// whole lenses floor `4.3` to `4`), the float `4.0` finds the number
/// four only, the natural `2` finds nothing (it does not see `-3`), and
/// the integer `-3` finds `-3` and `-2.5` (floored to `-3`). The same
/// counts come through a cardinality-many scan and a cardinality-one
/// scan.
fn lens_cases() -> Vec<(Value, usize)> {
    vec![
        (natural(4), 3),
        (integer(4), 3),
        (float(4.0), 2),
        (float(4.3), 1),
        (natural(2), 0),
        (integer(-3), 2),
        (float(-3.0), 1),
    ]
}

/// The rows [`lens_cases`] match against.
async fn store_lens_rows(branch: &Branch, operator: &Operator) -> anyhow::Result<()> {
    store(
        branch,
        operator,
        &the!("score/value"),
        &[natural(4), float(4.0), float(4.3), integer(-3), float(-2.5)],
    )
    .await?;
    Ok(())
}

/// A numeric constant matches through the lens of its type through a
/// cardinality-many scan and a cardinality-one scan alike (see
/// [`lens_cases`]).
#[dialog_common::test]
async fn a_constant_matches_through_the_lens_of_its_type_in_both_scans() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_lens_rows(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for (constant, found) in lens_cases() {
        for cardinality in [Cardinality::Many, Cardinality::One] {
            let query = DynamicAttributeQuery::new(
                Term::from(the!("score/value")),
                Term::<Entity>::var("e"),
                Term::Constant(constant.clone()),
                Term::var("cause"),
                Some(cardinality),
            );
            let results = query.perform(&source).try_vec().await?;
            assert_eq!(results.len(), found, "{constant:?} under {cardinality:?}");
        }
    }
    Ok(())
}

/// A numeric constant matches through the lens of its own type through
/// a concept field too (see [`lens_cases`]), whether the field is
/// declared `natural:`, `number:` or left untyped. The field's type is
/// the lens the stored numbers are read through first, so under a
/// `natural:` field no negative number is there to match: the integer
/// `-3` and the float `-3.0` find nothing, where under `number:` or an
/// untyped field they find `-3` and `-2.5`, and `-3` alone.
#[dialog_common::test]
async fn a_constant_matches_through_the_lens_of_its_type_in_a_concept_field() -> anyhow::Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store_lens_rows(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for declared in [Some("natural:"), Some("number:"), None] {
        let concept = field_concept("score/value", declared, Some("all:"));
        for (constant, found) in lens_cases() {
            let found = if declared == Some("natural:")
                && constant.clone().viewed_as(Type::UnsignedInt).is_none()
            {
                0
            } else {
                found
            };
            assert_eq!(
                field_matches(&source, &concept, constant.clone()).await?,
                found,
                "{constant:?} under {declared:?}"
            );
        }
    }
    Ok(())
}

/// A constant finds a stored number through a concept field whatever
/// the field declares: under `natural:`, under `number:` and under no
/// type at all, `+41` and `41.0` find the natural `41`, and `41.5` and
/// `-41` find nothing.
#[dialog_common::test]
async fn a_constant_of_any_number_type_finds_the_number_through_a_concept_field()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(&branch, &operator, &the!("person/age"), &[natural(41)]).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    for declared in [Some("natural:"), Some("number:"), None] {
        let concept = field_concept("person/age", declared, None);
        for (constant, found) in [
            (natural(41), 1),
            (integer(41), 1),
            (float(41.0), 1),
            (float(41.5), 0),
            (integer(-41), 0),
        ] {
            assert_eq!(
                field_matches(&source, &concept, constant.clone()).await?,
                found,
                "{constant:?} under {declared:?}"
            );
        }
    }
    Ok(())
}

/// A typed variable over mixed rows admits what its lens sees and binds
/// the viewed value: `natural:` floors `2.5` to `2` and drops `-3` and
/// infinity, `natural:8` drops `300`, `float:32` rounds `1e30` through
/// `f32`, `integer:` sees `-3` and `number:` reads every row raw. `-0.0`
/// is the number zero, so it reads as the natural `0`; `1e30` is a whole
/// number, so it reads as the natural `10^30`.
#[dialog_common::test]
async fn typed_variables_read_mixed_rows_through_their_lens() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        &the!("score/value"),
        &[
            natural(5),
            float(2.5),
            integer(-3),
            natural(300),
            float(1e30),
            float(-0.0),
            float(f64::INFINITY),
        ],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let big: u128 = 10u128.pow(30);

    let cases: Vec<(Kind, Vec<Value>)> = vec![
        (
            Kind::from(Type::UnsignedInt),
            vec![
                natural(5),
                natural(2),
                natural(300),
                natural(big),
                natural(0),
            ],
        ),
        (
            Kind::from(Type::SignedInt),
            vec![
                integer(5),
                integer(2),
                integer(-3),
                integer(300),
                integer(big as i128),
                integer(0),
            ],
        ),
        (
            Kind::from(Type::Float),
            vec![
                float(5.0),
                float(2.5),
                float(-3.0),
                float(300.0),
                float(1e30),
                float(0.0),
                float(f64::INFINITY),
            ],
        ),
        (
            Kind::from(Type::U8),
            vec![natural(5), natural(2), natural(0)],
        ),
        (
            Kind::from(Type::I8),
            vec![integer(5), integer(2), integer(-3), integer(0)],
        ),
        (
            Kind::from(Type::I32),
            vec![
                integer(5),
                integer(2),
                integer(-3),
                integer(300),
                integer(0),
            ],
        ),
        (
            Kind::from(Type::F32),
            vec![
                float(5.0),
                float(2.5),
                float(-3.0),
                float(300.0),
                float(f64::from(1e30f32)),
                float(0.0),
                float(f64::INFINITY),
            ],
        ),
        (
            Kind::from(Primitive::NUMERIC),
            vec![
                natural(5),
                float(2.5),
                integer(-3),
                natural(300),
                natural(big),
                natural(0),
                float(f64::INFINITY),
            ],
        ),
    ];
    for (kind, expected) in cases {
        let actual = scan(
            &source,
            &the!("score/value"),
            Term::var("v").with_kind(kind.clone()),
        )
        .await?;
        assert_eq!(actual, sorted(expected), "{kind}");
    }
    Ok(())
}

/// A bound on a kind applies to what the kind's lens reads: natural
/// `>= 2` sees `2.5` (as `2`), `3` and `5` and not `1.5` or `-3`; natural
/// `<= 2` sees `2.5` (as `2`) and `1.5` (as `1`) and not `3`; integer
/// `>= -3` sees `-2.5` (as `-3`); float `>= 2` sees `2.5`, `3.0` and
/// `5.0` as they are; a strict bound drops the bound itself; a sized kind
/// restricts the rows to its range too; and a `number:` kind compares
/// every number as it is.
#[dialog_common::test]
async fn kind_bounds_apply_through_the_lens() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        &the!("score/value"),
        &[
            float(1.5),
            float(2.5),
            natural(3),
            natural(5),
            integer(-3),
            float(-2.5),
            natural(300),
        ],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let cases: Vec<(Kind, Vec<Value>)> = vec![
        (
            Kind::from(Type::UnsignedInt)
                .with_interval(&natural(2), true, true)
                .expect("numeric admits an interval"),
            vec![natural(2), natural(3), natural(5), natural(300)],
        ),
        (
            Kind::from(Type::UnsignedInt)
                .with_interval(&natural(3), false, true)
                .expect("numeric admits an interval"),
            vec![natural(5), natural(300)],
        ),
        (
            Kind::from(Type::UnsignedInt)
                .with_interval(&natural(2), true, false)
                .expect("numeric admits an interval"),
            vec![natural(1), natural(2)],
        ),
        (
            Kind::from(Type::UnsignedInt)
                .with_interval(&natural(2), false, false)
                .expect("numeric admits an interval"),
            vec![natural(1)],
        ),
        (
            Kind::from(Type::Float)
                .with_interval(&natural(2), true, false)
                .expect("numeric admits an interval"),
            vec![float(1.5), float(-3.0), float(-2.5)],
        ),
        (
            Kind::from(Primitive::NUMERIC)
                .with_interval(&natural(2), true, false)
                .expect("numeric admits an interval"),
            vec![float(1.5), integer(-3), float(-2.5)],
        ),
        (
            Kind::from(Type::SignedInt)
                .with_interval(&integer(-3), true, true)
                .expect("numeric admits an interval"),
            vec![
                integer(-3),
                integer(-3),
                integer(1),
                integer(2),
                integer(3),
                integer(5),
                integer(300),
            ],
        ),
        (
            Kind::from(Type::Float)
                .with_interval(&natural(2), true, true)
                .expect("numeric admits an interval"),
            vec![float(2.5), float(3.0), float(5.0), float(300.0)],
        ),
        (
            Kind::from(Type::U8)
                .with_interval(&natural(2), true, true)
                .expect("numeric admits an interval"),
            vec![natural(2), natural(3), natural(5)],
        ),
        (
            Kind::from(Primitive::NUMERIC)
                .with_interval(&natural(2), true, true)
                .expect("numeric admits an interval"),
            vec![float(2.5), natural(3), natural(5), natural(300)],
        ),
    ];
    for (kind, expected) in cases {
        let actual = scan(
            &source,
            &the!("score/value"),
            Term::var("v").with_kind(kind.clone()),
        )
        .await?;
        assert_eq!(actual, sorted(expected), "{kind}");
    }
    Ok(())
}

/// Both sides of an interval apply through the lens together: natural in
/// `[2, 5)` sees `2.5` (as `2`) and `3`, not `5`; integer in `(-3, 2]`
/// sees `1.5` (as `1`) and `2.5` (as `2`), and not `-2.5`, which the
/// integer lens reads as `-3`, the excluded bound.
#[dialog_common::test]
async fn two_sided_kind_intervals_bracket_the_number_band() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        &the!("score/value"),
        &[
            float(1.5),
            float(2.5),
            natural(3),
            natural(5),
            integer(-3),
            float(-2.5),
        ],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let natural_band = Kind::from(Type::UnsignedInt)
        .with_interval(&natural(2), true, true)
        .expect("numeric admits an interval")
        .with_interval(&natural(5), false, false)
        .expect("the meet keeps both bounds");
    assert_eq!(
        scan(
            &source,
            &the!("score/value"),
            Term::var("v").with_kind(natural_band)
        )
        .await?,
        sorted(vec![natural(2), natural(3)])
    );

    let integer_band = Kind::from(Type::SignedInt)
        .with_interval(&integer(-3), false, true)
        .expect("numeric admits an interval")
        .with_interval(&integer(2), true, false)
        .expect("the meet keeps both bounds");
    assert_eq!(
        scan(
            &source,
            &the!("score/value"),
            Term::var("v").with_kind(integer_band)
        )
        .await?,
        sorted(vec![integer(1), integer(2)])
    );
    Ok(())
}

/// A concept field reads mixed stored values through the type it
/// declares: `natural:` reads `2.5` as `2` and skips `-3`, `natural:8`
/// skips `300`, `float:` reads `3` as `3.0`, `integer:32` skips `2^40`,
/// and `number:` yields each number as its narrowest type.
#[dialog_common::test]
async fn concept_fields_read_mixed_stored_values_through_their_declared_type() -> anyhow::Result<()>
{
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        &the!("score/value"),
        &[
            float(2.5),
            integer(-3),
            natural(300),
            natural(3),
            natural(1 << 40),
        ],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let cases: Vec<(&str, Vec<Value>)> = vec![
        (
            "natural:",
            vec![natural(2), natural(300), natural(3), natural(1 << 40)],
        ),
        ("natural:8", vec![natural(2), natural(3)]),
        (
            "float:",
            vec![
                float(2.5),
                float(-3.0),
                float(300.0),
                float(3.0),
                float((1u64 << 40) as f64),
            ],
        ),
        (
            "integer:32",
            vec![integer(2), integer(-3), integer(300), integer(3)],
        ),
        (
            "number:",
            vec![
                float(2.5),
                integer(-3),
                natural(300),
                natural(3),
                natural(1 << 40),
            ],
        ),
    ];
    for (declared, expected) in cases {
        let concept = field_concept("score/value", Some(declared), Some("all:"));
        assert_eq!(
            field_values(&source, &concept).await?,
            sorted(expected),
            "{declared}"
        );
    }
    Ok(())
}

/// Through a typed concept field a stored number outside the type's
/// range is not observable at all, and one inside it is observed at the
/// type's precision: each width's maximum and minimum are seen, one past
/// either is not, a half past the maximum floors onto it for an integer
/// type, a fraction floors, and the float types see every number their
/// precision reaches.
#[dialog_common::test]
async fn sized_concept_fields_observe_only_their_range_at_their_precision() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let big: u128 = 10u128.pow(30);
    let cases: Vec<(&str, &str, Vec<Value>, Vec<Value>)> = vec![
        (
            "n8",
            "natural:8",
            vec![
                natural(0),
                integer(-1),
                float(2.5),
                natural(255),
                float(255.5),
                natural(256),
            ],
            vec![natural(0), natural(2), natural(255), natural(255)],
        ),
        (
            "n16",
            "natural:16",
            vec![
                natural(65_535),
                float(65_535.5),
                natural(65_536),
                integer(-1),
                float(0.5),
            ],
            vec![natural(65_535), natural(65_535), natural(0)],
        ),
        (
            "n32",
            "natural:32",
            vec![
                natural(4_294_967_295),
                float(4_294_967_295.5),
                natural(4_294_967_296),
                float(-0.5),
            ],
            vec![natural(4_294_967_295), natural(4_294_967_295)],
        ),
        (
            "n64",
            "natural:64",
            vec![
                natural(u64::MAX as u128),
                natural(1 << 64),
                integer(-1),
                natural(0),
                float(1.9),
            ],
            vec![natural(u64::MAX as u128), natural(0), natural(1)],
        ),
        (
            "n128",
            "natural:128",
            vec![
                natural(u128::MAX),
                integer(-1),
                float(2.5),
                float(1e30),
                float(f64::MAX),
            ],
            vec![natural(u128::MAX), natural(2), natural(big)],
        ),
        (
            "i8",
            "integer:8",
            vec![
                natural(127),
                float(127.5),
                natural(128),
                integer(-128),
                float(-128.5),
                integer(-129),
                float(2.5),
                float(-2.5),
            ],
            vec![
                integer(127),
                integer(127),
                integer(-128),
                integer(2),
                integer(-3),
            ],
        ),
        (
            "i16",
            "integer:16",
            vec![
                natural(32_767),
                natural(32_768),
                integer(-32_768),
                integer(-32_769),
                float(0.5),
            ],
            vec![integer(32_767), integer(-32_768), integer(0)],
        ),
        (
            "i32",
            "integer:32",
            vec![
                natural(2_147_483_647),
                natural(2_147_483_648),
                integer(-2_147_483_648),
                integer(-2_147_483_649),
                natural(1 << 40),
            ],
            vec![integer(2_147_483_647), integer(-2_147_483_648)],
        ),
        (
            "i64",
            "integer:64",
            vec![
                natural(i64::MAX as u128),
                natural(1 << 63),
                integer(i64::MIN as i128),
                integer(i64::MIN as i128 - 1),
                float(2.5),
            ],
            vec![
                integer(i64::MAX as i128),
                integer(i64::MIN as i128),
                integer(2),
            ],
        ),
        (
            "i128",
            "integer:128",
            vec![
                integer(i128::MIN),
                natural(i128::MAX as u128),
                natural(u128::MAX),
                float(-2.5),
                float(1e30),
                float(f64::MAX),
            ],
            vec![
                integer(i128::MIN),
                integer(i128::MAX),
                integer(-3),
                integer(big as i128),
            ],
        ),
        (
            "f32",
            "float:32",
            vec![
                float(0.1),
                natural(16_777_217),
                float(1e30),
                float(f64::from(f32::MAX)),
                float(3.402_823_7e38),
                float(f64::MAX),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
                float(2.5),
            ],
            vec![
                float(f64::from(0.1f32)),
                float(16_777_216.0),
                float(f64::from(1e30f32)),
                float(f64::from(f32::MAX)),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
                float(2.5),
            ],
        ),
        (
            "f64",
            "float:64",
            vec![
                float(0.1),
                float(2.5),
                integer(-3),
                natural(u64::MAX as u128),
                float(f64::MAX),
                float(f64::INFINITY),
                float(1e30),
            ],
            vec![
                float(0.1),
                float(2.5),
                float(-3.0),
                float(u64::MAX as f64),
                float(f64::MAX),
                float(f64::INFINITY),
                float(1e30),
            ],
        ),
        (
            "natural",
            "natural:",
            vec![
                natural(0),
                integer(-1),
                float(2.5),
                float(-0.5),
                natural(u128::MAX),
                float(f64::MAX),
                float(f64::INFINITY),
            ],
            vec![natural(0), natural(2), natural(u128::MAX)],
        ),
        (
            "integer",
            "integer:",
            vec![
                integer(-1),
                float(2.5),
                float(-2.5),
                natural(u128::MAX),
                integer(i128::MIN),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
            ],
            vec![integer(-1), integer(2), integer(-3), integer(i128::MIN)],
        ),
        (
            "float",
            "float:",
            vec![
                integer(-1),
                float(2.5),
                natural(u128::MAX),
                integer(i128::MIN),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
                natural(0),
            ],
            vec![
                float(-1.0),
                float(2.5),
                float(u128::MAX as f64),
                float(i128::MIN as f64),
                float(f64::INFINITY),
                float(f64::NEG_INFINITY),
                float(0.0),
            ],
        ),
    ];
    for (name, _, values, _) in &cases {
        store(
            &branch,
            &operator,
            &relation(&format!("bound/{name}"))?,
            values,
        )
        .await?;
    }
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    for (name, declared, _, expected) in cases {
        let concept = field_concept(&format!("bound/{name}"), Some(declared), Some("all:"));
        assert_eq!(
            field_values(&source, &concept).await?,
            sorted(expected),
            "{declared}"
        );
    }
    Ok(())
}

/// A conclusion realized from a `natural:` field over a stored `2.5`
/// gives `2` to a typed getter: `ConceptConclusion::get::<u64>` reads the
/// viewed value, and a float getter of the same conclusion is refused,
/// because the row holds a natural.
#[dialog_common::test]
async fn a_conclusion_reads_a_natural_field_as_the_floored_natural() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entities = store(&branch, &operator, &the!("score/value"), &[float(2.5)]).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let mut terms = Parameters::new();
    terms.insert(
        "this".into(),
        Term::Constant(Value::Entity(entities[0].clone())),
    );
    terms.insert("value".into(), Term::var("value"));
    let query = ConceptQuery {
        terms,
        predicate: field_concept("score/value", Some("natural:"), None),
    };
    let rows = query
        .clone()
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    assert_eq!(rows.len(), 1);
    let conclusion = query.realize(rows[0].clone())?;
    assert_eq!(conclusion.get::<u64>("value")?, 2);
    assert_eq!(conclusion.get::<u8>("value")?, 2);
    assert!(
        matches!(
            conclusion.get::<f64>("value"),
            Err(crate::EvaluationError::UnboundVariable { .. })
        ),
        "the row holds the natural 2, which is no float"
    );
    Ok(())
}

/// A rule body reading a typed field derives the viewed values: a head
/// over `natural:` fed by a `natural:` read of mixed rows derives `2`
/// from `2.5`, skips `-3`, and keeps `300` and `3`; a head over `float:`
/// fed by a `float:` read derives every row as a float.
#[dialog_common::test]
async fn a_rule_body_reads_a_typed_field_through_its_lens() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        &the!("score/value"),
        &[float(2.5), integer(-3), natural(300), natural(3)],
    )
    .await?;
    let mut registry = RuleRegistry::new();
    registry.register(copy_rule(
        "score/whole",
        "natural:",
        "score/value",
        "natural:",
    )?)?;
    registry.register(copy_rule("score/real", "float:", "score/value", "float:")?)?;
    registry.register(copy_rule(
        "score/small",
        "natural:8",
        "score/value",
        "natural:8",
    )?)?;
    let source = TestEnv::new(&branch, &operator, registry);

    assert_eq!(
        field_values(
            &source,
            &field_concept("score/whole", Some("natural:"), Some("all:"))
        )
        .await?,
        sorted(vec![natural(2), natural(300), natural(3)])
    );
    assert_eq!(
        field_values(
            &source,
            &field_concept("score/real", Some("float:"), Some("all:"))
        )
        .await?,
        sorted(vec![float(2.5), float(-3.0), float(300.0), float(3.0)])
    );
    assert_eq!(
        field_values(
            &source,
            &field_concept("score/small", Some("natural:8"), Some("all:"))
        )
        .await?,
        sorted(vec![natural(2), natural(3)])
    );
    Ok(())
}

/// A typed field takes only what its type holds exactly, whatever type
/// spells the number: `natural:` takes `41`, `+41` and `41.0` and refuses
/// `2.5` and `-1`; `natural:8` refuses `256` and `2.5` and takes `255`
/// and `255.0`; `float:` takes `3.0`, `3` and `2.5`; `float:32` takes
/// what an `f32` holds exactly and refuses `0.1`; `integer:` takes `-1`
/// and `1`; `number:` takes any number and refuses text. Each refusal is
/// a `TypeError::TypeMismatch` naming the field, raised before anything
/// is written, and an accepted value is written as it was spelled.
#[dialog_common::test]
async fn typed_concept_writes_take_only_what_the_type_holds() -> anyhow::Result<()> {
    let concept = |declared: &str| field_concept("write/value", Some(declared), None);
    let accepted: Vec<(&str, Value)> = vec![
        ("natural:", natural(41)),
        ("natural:", integer(41)),
        ("natural:", float(41.0)),
        ("natural:", natural(u128::MAX)),
        ("natural:8", natural(255)),
        ("natural:8", float(255.0)),
        ("natural:8", integer(7)),
        ("natural:64", integer(41)),
        ("integer:", integer(-1)),
        ("integer:", natural(1)),
        ("integer:", integer(i128::MIN)),
        ("integer:8", integer(-128)),
        ("integer:8", natural(127)),
        ("float:", float(3.0)),
        ("float:", natural(3)),
        ("float:", float(2.5)),
        ("float:", float(f64::INFINITY)),
        ("float:64", natural(3)),
        ("float:64", float(0.1)),
        ("float:32", float(f64::from(0.1f32))),
        ("number:", float(2.5)),
        ("number:", integer(-1)),
        ("number:", natural(u128::MAX)),
    ];
    for (declared, value) in accepted {
        let statement = typed_write(&concept(declared), value.clone());
        assert!(statement.is_ok(), "{declared} holds {value:?}");
        let statement = statement.expect("checked");
        assert_eq!(statement.with.len(), 1);
        assert_eq!(
            statement.with[0].is, value,
            "the value is written as given, the band makes it one number"
        );
    }

    let refused: Vec<(&str, Value)> = vec![
        ("natural:", float(2.5)),
        ("natural:", integer(-1)),
        ("natural:8", natural(256)),
        ("natural:8", float(2.5)),
        ("natural:8", integer(-1)),
        ("integer:8", integer(-129)),
        ("integer:8", natural(128)),
        ("integer:", float(2.5)),
        ("integer:", natural(u128::MAX)),
        ("float:32", float(0.1)),
        ("float:32", float(3.402_823_7e38)),
        ("number:", Value::String("3".into())),
    ];
    for (declared, value) in refused {
        let statement = typed_write(&concept(declared), value.clone());
        assert!(
            matches!(
                &statement,
                Err(TypeError::TypeMismatch { binding, .. }) if binding == "value"
            ),
            "{declared} refuses {value:?}: {statement:?}"
        );
    }
    Ok(())
}

/// A refused write writes nothing, and an accepted write of a float
/// into `natural:8` lands in the number band as the natural it names:
/// `255.0` reads back raw as the natural `255`.
#[dialog_common::test]
async fn a_refused_typed_write_leaves_the_store_untouched() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let concept = field_concept("write/value", Some("natural:8"), None);
    let who = Entity::new()?;

    let refused = concept.edit(who.clone()).with("value", 256u16).build();
    assert!(matches!(refused, Err(TypeError::TypeMismatch { .. })));
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    assert_eq!(
        scan(&source, &the!("write/value"), Term::var("v")).await?,
        Vec::<Value>::new(),
        "nothing was written"
    );

    let accepted = concept.edit(who.clone()).with("value", 255.0f64).build()?;
    branch
        .transaction()
        .assert(accepted)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    assert_eq!(
        scan(&source, &the!("write/value"), Term::var("v")).await?,
        vec![natural(255)],
        "the float 255.0 is the number 255"
    );
    Ok(())
}

/// A cardinality-one typed write of what the field already observes is
/// a no-op, and of another number a supersession: an `integer:` field
/// over a stored `4.3` observes `4`; asserting `4` through the field
/// changes nothing (the store still holds `4.3`, a float reader of the
/// attribute still sees `4.3`); asserting `5` supersedes `4.3` with `5`,
/// which the integer reader observes as `5` and the float reader as
/// `5.0`.
#[dialog_common::test]
async fn a_typed_cardinality_one_write_of_the_observed_number_is_a_no_op() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(
            the!("cell/value")
                .of(who.clone())
                .is(4.3f64)
                .cardinality(Cardinality::One),
        )
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let whole = field_concept("cell/value", Some("integer:"), None);
    let real = field_concept("cell/value", Some("float:"), None);
    {
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        assert_eq!(field_values(&source, &whole).await?, vec![integer(4)]);
        assert_eq!(field_values(&source, &real).await?, vec![float(4.3)]);
    }

    let same = whole.edit(who.clone()).with("value", 4i64).build()?;
    branch
        .transaction()
        .assert(same)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    {
        let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
        assert_eq!(field_values(&source, &whole).await?, vec![integer(4)]);
        assert_eq!(
            field_values(&source, &real).await?,
            vec![float(4.3)],
            "asserting the observed 4 wrote nothing over 4.3"
        );
        let claims = live_claims(&source, &the!("cell/value"), &who).await?;
        assert_eq!(claims.len(), 1);
        assert_eq!(
            claims[0].is(),
            &float(4.3),
            "the cell's live claim is still 4.3"
        );
    }

    let other = whole.edit(who.clone()).with("value", 5i64).build()?;
    branch
        .transaction()
        .assert(other)
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    assert_eq!(field_values(&source, &whole).await?, vec![integer(5)]);
    assert_eq!(field_values(&source, &real).await?, vec![float(5.0)]);
    let claims = live_claims(&source, &the!("cell/value"), &who).await?;
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].is(), &natural(5), "5 superseded 4.3");
    Ok(())
}

/// Two rules deriving one `number:` relation, one from a `natural:` read
/// of a stored `3` and one from a `float:` read of a stored `3.0`, derive
/// one number, so the reader sees one row, through the plain union and
/// through a fixpoint alike: derived rows are keyed by the number they
/// hold, not the type that holds it, as two such facts are one fact in
/// storage.
#[dialog_common::test]
async fn derived_rows_of_one_number_under_two_types_are_one_row() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(the!("x/n").of(who.clone()).is(3u64))
        .assert(the!("x/f").of(who.clone()).is(3.0f64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;

    let mut registry = RuleRegistry::new();
    registry.register(copy_rule("x/v", "number:", "x/n", "natural:")?)?;
    registry.register(copy_rule("x/v", "number:", "x/f", "float:")?)?;
    {
        let source = TestEnv::new(&branch, &operator, registry.clone());
        let values = field_values(
            &source,
            &field_concept("x/v", Some("number:"), Some("all:")),
        )
        .await?;
        assert_eq!(values.len(), 1, "the union of the two rules: {values:?}");
        assert!(values[0].same_number(&natural(3)), "{values:?}");
    }

    registry.register(copy_rule("x/v", "number:", "x/v", "number:")?)?;
    assert!(
        registry.analysis()?.quarantined().is_empty(),
        "the self-reading rule makes the relation recursive"
    );
    let source = TestEnv::new(&branch, &operator, registry);
    let values = field_values(
        &source,
        &field_concept("x/v", Some("number:"), Some("all:")),
    )
    .await?;
    assert_eq!(values.len(), 1, "the fixpoint keeps one: {values:?}");
    assert!(values[0].same_number(&natural(3)), "{values:?}");
    Ok(())
}

/// Raw reads of a stored `3` and a stored `3.0` are both the natural
/// `3`, so a sum over them is the natural `6`, and two `float:` reads
/// sum to the float `6.0`; a `natural:` read and a `float:` read cannot
/// feed one sum: a formula's scheme variable takes one type, so the rule
/// is refused when it compiles (`TypeError::TypeInference`), never
/// evaluated with a promotion.
#[dialog_common::test]
async fn a_sum_adds_numbers_of_one_type_only() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let who = Entity::new()?;
    branch
        .transaction()
        .assert(the!("x/n").of(who.clone()).is(3u64))
        .assert(the!("x/f").of(who.clone()).is(3.0f64))
        .commit()
        .publish()
        .perform(&operator)
        .await?;
    let sum_rule = |n_as: Option<&str>, f_as: Option<&str>| -> Result<DeductiveRule, TypeError> {
        let mut n = serde_json::json!({ "the": "x/n" });
        let mut f = serde_json::json!({ "the": "x/f" });
        if let Some(declared) = n_as {
            n["as"] = serde_json::Value::String(declared.into());
        }
        if let Some(declared) = f_as {
            f["as"] = serde_json::Value::String(declared.into());
        }
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": { "sum": { "the": "x/sum", "as": "number:" } } },
            "when": [
                {
                    "assert": { "with": { "n": n, "f": f } },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "n": { "?": { "name": "n" } },
                        "f": { "?": { "name": "f" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "n" } },
                        "with": { "?": { "name": "f" } },
                        "is": { "?": { "name": "sum" } }
                    }
                }
            ]
        }))
        .expect("a well-formed rule");
        descriptor.compile()
    };
    let sums = field_concept("x/sum", Some("number:"), Some("all:"));

    let mut raw = RuleRegistry::new();
    raw.register(sum_rule(None, None)?)?;
    let source = TestEnv::new(&branch, &operator, raw);
    assert_eq!(field_values(&source, &sums).await?, vec![natural(6)]);

    let mut floats = RuleRegistry::new();
    floats.register(sum_rule(Some("float:"), Some("float:"))?)?;
    let source = TestEnv::new(&branch, &operator, floats);
    assert_eq!(field_values(&source, &sums).await?, vec![float(6.0)]);

    let mixed = sum_rule(Some("natural:"), Some("float:"));
    assert!(
        matches!(&mixed, Err(TypeError::TypeInference { .. })),
        "a natural and a float share no scheme type: {mixed:?}"
    );
    Ok(())
}

/// The edge values every encoding has to carry.
fn edge_values() -> Vec<Value> {
    vec![
        natural(0),
        float(-0.0),
        natural(1),
        integer(-1),
        natural(2),
        float(2.5),
        float(-2.5),
        float(0.1),
        natural(255),
        natural(256),
        integer(-128),
        integer(-129),
        natural(u64::MAX as u128),
        natural(1 << 64),
        natural(u128::MAX),
        integer(i128::MIN),
        float(1e30),
        float(f64::MAX),
        float(f64::INFINITY),
        float(f64::NEG_INFINITY),
        integer(3),
        float(3.0),
    ]
}

/// The edge values past 64 bits: JSON (without arbitrary precision)
/// reads an integer past `u64`/`i64` as a float, so these are pinned
/// apart (see the report).
fn past_sixty_four_bits(value: &Value) -> bool {
    matches!(value, Value::UnsignedInt(n) if *n > u64::MAX as u128)
        || matches!(value, Value::SignedInt(i) if *i < i64::MIN as i128)
}

/// Tagged JSON keeps every finite edge value within 64 bits with its
/// type: the natural `3`, the integer `+3` and the float `3.0` come back
/// as three values, `-0.0` keeps its sign and `u64::MAX` comes back
/// whole. An infinity has no JSON spelling: it writes as `null`, which
/// reads as no value.
#[dialog_common::test]
fn tagged_json_keeps_the_value_and_the_type() {
    for value in edge_values() {
        if past_sixty_four_bits(&value) {
            continue;
        }
        let Value::Float(f) = &value else {
            let json = serde_json::to_string(&value).expect("encodes");
            let back: Value = serde_json::from_str(&json).expect("decodes");
            assert_eq!(back, value, "{json}");
            continue;
        };
        let json = serde_json::to_string(&value).expect("encodes");
        if f.is_finite() {
            let back: Value = serde_json::from_str(&json).expect("decodes");
            assert_eq!(back, value, "{json}");
        } else {
            assert_eq!(json, "null", "an infinity has no JSON spelling");
            let error = serde_json::from_str::<Value>(&json).expect_err("null is no value");
            assert!(
                error
                    .to_string()
                    .contains("a value, tagged by its type entity or bare"),
                "{error}"
            );
        }
    }
}

/// Written bare, a value keeps its number and the first type the
/// payload fits: a non-negative integer reads back as a natural, so the
/// integer `+3` comes back as the natural `3` (one number in the band),
/// and every other finite edge value comes back as itself.
#[dialog_common::test]
fn untagged_json_keeps_the_number() {
    for value in edge_values() {
        if matches!(&value, Value::Float(f) if !f.is_finite()) || past_sixty_four_bits(&value) {
            continue;
        }
        let json = serde_json::to_string(&value.untagged()).expect("encodes");
        let back: Value = serde_json::from_str(&json).expect("decodes");
        let expected = match &value {
            Value::SignedInt(i) if *i >= 0 => natural(*i as u128),
            other => other.clone(),
        };
        assert_eq!(back, expected, "{json}");
        assert!(
            back.same_number(&value),
            "{json} is still the number {value:?}"
        );
    }
}

/// A tagged number is checked against its tag: `natural:` refuses a
/// negative, `natural:8` refuses `256` and `2.5` and reads `3.0` as the
/// natural `3`, `float:64` reads `3` as the float `3.0`, `number:` reads
/// a number as its own type, a float tag refuses text, and NaN has no
/// spelling (a bare `NaN` is not JSON; a tagged one is refused).
#[dialog_common::test]
fn tagged_json_checks_a_number_against_its_tag() {
    let read = |json: serde_json::Value| serde_json::from_value::<Value>(json);
    assert_eq!(
        read(serde_json::json!({ "natural:8": 3.0 })).expect("3.0 is 3"),
        natural(3)
    );
    assert_eq!(
        read(serde_json::json!({ "float:64": 3 })).expect("3 is 3.0"),
        float(3.0)
    );
    assert_eq!(
        read(serde_json::json!({ "float:32": 0.5 })).expect("0.5 is an f32"),
        float(0.5)
    );
    assert_eq!(
        read(serde_json::json!({ "number:": -1 })).expect("a number"),
        integer(-1)
    );
    assert_eq!(
        read(serde_json::json!({ "number:": 2.5 })).expect("a number"),
        float(2.5)
    );
    assert_eq!(
        read(serde_json::json!({ "integer:": 3 })).expect("an integer"),
        integer(3)
    );
    assert_eq!(
        read(serde_json::json!({ "natural:": 3 })).expect("a natural"),
        natural(3)
    );
    for (json, reason) in [
        (
            serde_json::json!({ "natural:": -1 }),
            "-1 is not a natural number",
        ),
        (
            serde_json::json!({ "natural:8": 256 }),
            "is not a natural:8",
        ),
        (
            serde_json::json!({ "natural:8": 2.5 }),
            "is not a natural:8",
        ),
        (
            serde_json::json!({ "integer:8": -129 }),
            "is not a integer:8",
        ),
        (serde_json::json!({ "float:32": 0.1 }), "is not a float:32"),
        (serde_json::json!({ "float:": "NaN" }), "invalid type"),
        (serde_json::json!({ "number:": "3" }), "is not a number:"),
    ] {
        let error = read(json.clone()).expect_err("refused");
        assert!(error.to_string().contains(reason), "{json}: {error}");
    }
    assert!(
        serde_json::from_str::<Value>("NaN").is_err(),
        "a bare NaN is not JSON"
    );
}

/// dag-cbor keeps every finite edge value with its type that a CBOR
/// integer reaches: the natural `3`, the integer `+3` and the float `3.0`
/// are three values, `-0.0` keeps its sign, `f64::MAX` and `u64::MAX`
/// are whole. A natural past `u64::MAX`, an integer below `i64::MIN`
/// and an infinity have no dag-cbor spelling and are refused on encode
/// (`EncodeError::Msg`) rather than narrowed through a float (see the
/// report: the spec wants infinities to survive).
#[dialog_common::test]
fn dag_cbor_keeps_the_value_and_the_type_cbor_integers_reach() {
    for value in edge_values() {
        let encoded = serde_ipld_dagcbor::to_vec(&value);
        if past_sixty_four_bits(&value) || matches!(&value, Value::Float(f) if !f.is_finite()) {
            assert!(
                matches!(encoded, Err(serde_ipld_dagcbor::EncodeError::Msg(_))),
                "{value:?} has no dag-cbor spelling: {encoded:?}"
            );
            continue;
        }
        let bytes = encoded.expect("encodes");
        let back: Value = serde_ipld_dagcbor::from_slice(&bytes).expect("decodes");
        assert_eq!(back, value);
    }
}

/// NaN is not a value: a bare CBOR float NaN is refused on decode with
/// "NaN is not a value", and a NaN is refused on encode.
#[dialog_common::test]
fn dag_cbor_refuses_nan() {
    // The 64-bit float `0x7ff8000000000000`.
    let bare = vec![0xfb, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0];
    let decoded = serde_ipld_dagcbor::from_slice::<Value>(&bare);
    assert!(
        matches!(&decoded, Err(serde_ipld_dagcbor::DecodeError::Msg(reason)) if reason == "NaN is not a value"),
        "{decoded:?}"
    );
    assert!(
        matches!(
            serde_ipld_dagcbor::to_vec(&float(f64::NAN)),
            Err(serde_ipld_dagcbor::EncodeError::Msg(_))
        ),
        "a NaN is not written either"
    );
}

/// The human notation names a value by its type and spelling: `uint:41`,
/// `sint:41`, `float:41` round trip, a `+` sign is accepted and not kept
/// (`sint:+41` is `sint:41`), `float:inf` is the infinity, and
/// `float:NaN` is refused: NaN is not a value.
#[dialog_common::test]
fn human_notation_round_trips_numbers() -> anyhow::Result<()> {
    for (value, text) in [
        (natural(41), "uint:41"),
        (integer(41), "sint:41"),
        (integer(-41), "sint:-41"),
        (float(41.0), "float:41"),
        (float(2.5), "float:2.5"),
        (
            natural(u128::MAX),
            "uint:340282366920938463463374607431768211455",
        ),
        (
            integer(i128::MIN),
            "sint:-170141183460469231731687303715884105728",
        ),
        (float(f64::INFINITY), "float:inf"),
        (float(f64::NEG_INFINITY), "float:-inf"),
    ] {
        assert_eq!(value.to_utf8(), text);
        assert_eq!(text.parse::<Value>()?, value);
    }
    assert_eq!("sint:+41".parse::<Value>()?, integer(41));
    assert_eq!("uint:+41".parse::<Value>()?, natural(41));
    assert_eq!("float:+41".parse::<Value>()?, float(41.0));
    assert_eq!("float:-0".parse::<Value>()?, float(-0.0));
    assert!(
        matches!(
            "float:NaN".parse::<Value>(),
            Err(crate::DialogArtifactsError::InvalidValue(reason)) if reason == "NaN is not a value"
        ),
        "NaN is not a value"
    );
    Ok(())
}
