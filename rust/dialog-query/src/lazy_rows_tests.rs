//! A scan binding its rows in place against the same scan binding them
//! eagerly, as `main` does: seeded with `Match::new()` a scan binds a
//! row's components as scanned slots, seeded with `Match::eager()` it
//! materializes every row. Both must read the same rows the same way.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use std::iter::repeat_n;

use crate::attribute::{AttributeStatement, Relation};
use crate::corpus::Type;
use crate::query::Application as _;
use crate::selection::Selection;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::type_system::Type as Kind;
use crate::types::Any;
use crate::{
    Binding, Cardinality, DynamicAttributeQuery, Entity, EvaluationError, JoinKey, Match, Term,
    Value, merge_join,
};
use dialog_capability::identity::Uri;
use dialog_corpus::encode_value_owned;
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;
use futures_util::stream;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn relation(name: &str) -> Relation {
    name.parse().expect("a well-formed relation")
}

/// Asserts each `(the, value)` on an entity of its own, returning the
/// entities in that order.
async fn store(
    branch: &Branch,
    operator: &Operator,
    facts: Vec<(&str, Value)>,
) -> anyhow::Result<Vec<Entity>> {
    let mut entities = Vec::new();
    let mut transaction = branch.transaction();
    for (the, is) in facts {
        let of = Entity::new()?;
        entities.push(of.clone());
        transaction = transaction.assert(AttributeStatement {
            the: relation(the),
            of,
            is,
            cause: None,
            cardinality: None,
            pick: None,
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(entities)
}

/// A text long enough to spill out of its key into a block.
fn spilled_text(fill: char, zero: bool) -> String {
    let mut text: String = repeat_n(fill, 4096).collect();
    if zero {
        text.insert(100, '\0');
    }
    text
}

/// The rows `query` yields from `seed`.
async fn rows(
    source: &TestEnv<'_>,
    query: &DynamicAttributeQuery,
    seed: Match,
) -> Result<Vec<Match>, EvaluationError> {
    query.clone().evaluate(source, seed.seed()).try_vec().await
}

/// The texts every `(scanned, bound)` pairing is checked over: with and
/// without zero bytes, empty, and spilled.
fn texts() -> Vec<String> {
    vec![
        String::new(),
        "a".into(),
        "a\0".into(),
        "a\0b".into(),
        "a\0\u{1}".into(),
        "ab".into(),
        "\0".into(),
        spilled_text('m', false),
        spilled_text('m', true),
    ]
}

/// The join key of `v` on a row that binds it as a bound value.
fn bound_key(value: &Value) -> JoinKey {
    let mut row = Match::new();
    row.bind(&Term::var("v"), value.clone())
        .expect("binds a fresh variable");
    row.join_key("v").expect("a present value has a join key")
}

/// A scan's join key orders every pairing of scanned and bound values
/// exactly as `main` keyed them, by `encode_value_owned`: a merge join
/// on a variable one side binds in place and the other binds as a value
/// (a constant, a formula's result, an eager seed) matches equal values
/// and orders the rest as the index does. Texts with zero bytes and
/// spilled texts are the cases `JoinKey`'s comment assumes away.
#[dialog_common::test]
async fn join_keys_order_scanned_and_bound_values_as_their_encodings() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let texts = texts();
    store(
        &branch,
        &operator,
        texts
            .iter()
            .map(|text| ("join/text", Value::String(text.clone())))
            .collect(),
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let query = DynamicAttributeQuery::new(
        Term::from(relation("join/text")),
        Term::<Entity>::var("e"),
        Term::var("v"),
        Term::var("cause"),
        None,
    );
    let scanned = rows(&source, &query, Match::new()).await?;
    assert_eq!(scanned.len(), texts.len(), "every text is scanned");

    let mut mismatches = Vec::new();
    for row in &scanned {
        let Some(Binding::Present(left)) = row.get("v").cloned() else {
            panic!("the scan binds v");
        };
        assert!(row.scanned("v").is_some(), "the scan binds v in place");
        let scanned_key = row.join_key("v").expect("a scanned value has a join key");
        for right in &texts {
            let right = Value::String(right.clone());
            let expected = encode_value_owned(&left).cmp(&encode_value_owned(&right));
            for (how, actual) in [
                ("scanned against bound", scanned_key.cmp(&bound_key(&right))),
                (
                    "bound against scanned",
                    bound_key(&right).cmp(&scanned_key).reverse(),
                ),
            ] {
                if actual != expected {
                    mismatches.push(format!(
                        "{how}: {:?} vs {:?}: expected {expected:?}, got {actual:?}",
                        abbreviate(&left),
                        abbreviate(&right)
                    ));
                }
            }
        }
        for other in &scanned {
            let Some(Binding::Present(right)) = other.get("v").cloned() else {
                continue;
            };
            let expected = encode_value_owned(&left).cmp(&encode_value_owned(&right));
            let actual = scanned_key.cmp(&other.join_key("v").expect("a join key"));
            if actual != expected {
                mismatches.push(format!(
                    "scanned against scanned: {:?} vs {:?}: expected {expected:?}, got {actual:?}",
                    abbreviate(&left),
                    abbreviate(&right)
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "expected JoinKey to order as encode_value_owned does; {} pairings differ:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
    Ok(())
}

/// A merge join of a scan binding `v` in place with rows binding `v` to
/// the same texts as values joins every text with itself: a text with
/// a zero byte, inline or spilled, included.
#[dialog_common::test]
async fn a_merge_join_matches_a_scanned_text_with_a_zero_byte() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let mut texts = texts();
    store(
        &branch,
        &operator,
        texts
            .iter()
            .map(|text| ("join/text", Value::String(text.clone())))
            .collect(),
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let query = DynamicAttributeQuery::new(
        Term::from(relation("join/text")),
        Term::<Entity>::var("e"),
        Term::var("v"),
        Term::var("cause"),
        None,
    );
    let mut left = rows(&source, &query, Match::new()).await?;
    let key = |row: &Match| row.join_key("v");
    left.sort_by_key(key);
    texts.sort_by(|a, b| {
        encode_value_owned(&Value::String(a.clone()))
            .cmp(&encode_value_owned(&Value::String(b.clone())))
    });
    let right: Vec<Match> = texts
        .iter()
        .map(|text| {
            let mut row = Match::new();
            row.bind(&Term::var("v"), Value::String(text.clone()))
                .expect("binds");
            row
        })
        .collect();
    let joined = merge_join(
        stream::iter(left.into_iter().map(Ok)),
        stream::iter(right.into_iter().map(Ok)),
        key,
    )
    .try_vec()
    .await?;
    let mut matched: Vec<String> = joined
        .iter()
        .filter_map(|row| match row.get("v") {
            Some(Binding::Present(Value::String(text))) => Some(abbreviate_text(text)),
            _ => None,
        })
        .collect();
    matched.sort();
    let mut expected: Vec<String> = texts.iter().map(|text| abbreviate_text(text)).collect();
    expected.sort();
    assert_eq!(
        matched, expected,
        "expected the merge join to match every text with itself"
    );
    Ok(())
}

fn abbreviate(value: &Value) -> String {
    match value {
        Value::String(text) => abbreviate_text(text),
        other => format!("{other:?}"),
    }
}

fn abbreviate_text(text: &str) -> String {
    if text.len() > 16 {
        format!(
            "{:?}..({} bytes, zero: {})",
            &text[..4],
            text.len(),
            text.contains('\0')
        )
    } else {
        format!("{text:?}")
    }
}

/// An entity whose URI is not the canonical rendering of itself, as a
/// peer that does not normalize could store one: `Uri::stored_unchecked`
/// adopts the string as it is.
fn uncanonical(uri: &str) -> Value {
    Value::Entity(Entity::try_from(Uri::stored_unchecked(uri)).expect("any string has key bytes"))
}

/// The values of `v` a scan of `the` yields from `seed`, or the error
/// the scan fails with.
async fn values(
    source: &TestEnv<'_>,
    the: &str,
    seed: Match,
) -> Result<Vec<Value>, EvaluationError> {
    let query = DynamicAttributeQuery::new(
        Term::from(relation(the)),
        Term::<Entity>::var("e"),
        Term::var("v"),
        Term::var("cause"),
        None,
    );
    Ok(rows(source, &query, seed)
        .await?
        .iter()
        .filter_map(|row| row.get("v").and_then(Binding::as_value).cloned())
        .collect())
}

/// A stored entity value another writer left uncanonical reads the same
/// to a scan binding in place as to one binding eagerly. The eager read
/// (`to_owned`, `decode_value`) parses an entity value and normalizes
/// it; `checked` requires the canonical rendering and calls the row
/// `CorruptEntry`, which the in-place scan passes over. So the same
/// stored row reads in one scan of a query and is missing from another.
#[dialog_common::test]
async fn an_uncanonical_entity_value_reads_alike_in_place_and_eagerly() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        vec![("link/to", uncanonical("User:bob"))],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let eager = values(&source, "link/to", Match::eager()).await;
    let lazy = values(&source, "link/to", Match::new()).await;
    assert!(
        matches!(&eager, Ok(values) if values.len() == 1),
        "the eager scan reads the row, got {eager:?}"
    );
    assert_eq!(
        format!("{lazy:?}"),
        format!("{eager:?}"),
        "expected the in-place scan to read what the eager scan reads"
    );
    Ok(())
}

/// A stored entity value that is not a URI at all fails an eager scan
/// (`decode_value` refuses it: `InvalidValue`, not `CorruptEntry`) and,
/// as `checked` claims to classify as `to_owned` does, fails the scan
/// binding in place too, rather than being passed over by it.
#[dialog_common::test]
async fn an_entity_value_that_is_no_uri_fails_in_place_as_it_fails_eagerly() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        vec![("link/to", uncanonical("not a uri"))],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let eager = values(&source, "link/to", Match::eager()).await;
    let lazy = values(&source, "link/to", Match::new()).await;
    assert!(
        matches!(eager, Err(EvaluationError::Store(_))),
        "the eager scan fails on the row, got {eager:?}"
    );
    assert!(
        matches!(lazy, Err(EvaluationError::Store(_))),
        "expected the in-place scan to fail as the eager scan fails ({eager:?}), got {lazy:?}"
    );
    Ok(())
}

/// One scan to run both ways, from a seed built by `bind` on an empty
/// lazy or eager row.
struct Scenario {
    name: &'static str,
    query: DynamicAttributeQuery,
    bind: fn(&mut Match),
}

fn scan(
    name: &'static str,
    the: Term<Relation>,
    of: Term<Entity>,
    is: Term<Any>,
    cardinality: Option<Cardinality>,
    bind: fn(&mut Match),
) -> Scenario {
    Scenario {
        name,
        query: DynamicAttributeQuery::new(the, of, is, Term::var("cause"), cardinality),
        bind,
    }
}

fn nothing(_: &mut Match) {}

fn bind(row: &mut Match, name: &str, value: Value) {
    row.bind(&Term::var(name), value).expect("binds");
}

/// Everything a consumer reads off a row, materialized, to compare a
/// lazy row with an eager one.
fn observe(query: &DynamicAttributeQuery, row: &Match) -> Vec<String> {
    let mut seen = Vec::new();
    for name in ["the", "e", "v", "cause"] {
        let got = row.get(name).cloned();
        let viewed = row
            .view(name)
            .map(|view| view.map(|view| view.to_value().map_err(|e| format!("{e}"))));
        let viewed_matches = match (&got, &viewed) {
            (None, None) => true,
            (Some(Binding::Absent), Some(None)) => true,
            (Some(Binding::Present(value)), Some(Some(Ok(view)))) => view == value,
            _ => false,
        };
        seen.push(format!(
            "{name}: {got:?} (view agrees with get: {viewed_matches}), standing {:?}",
            row.standing_of(name)
        ));
    }
    seen.push(format!("claim: {:?}", query.realize(row.clone())));
    seen.push(format!("standing: {:?}", row.standing()));
    seen
}

/// Every scan reads the same rows, bindings, views, claims and
/// standings whether it binds rows in place or eagerly: a typed number
/// term read through another number's view, a typed term the stored
/// value does not inhabit, a value variable already bound to an equal,
/// a number-equal, a different and an absent value, cardinality-one
/// scans, a named attribute, and spilled texts.
#[dialog_common::test]
async fn every_scan_reads_alike_in_place_and_eagerly() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entities = store(
        &branch,
        &operator,
        vec![
            ("eq/number", Value::UnsignedInt(3)),
            ("eq/number", Value::SignedInt(-2)),
            ("eq/number", Value::Float(2.5)),
            ("eq/number", Value::Float(3.0)),
            ("eq/text", Value::String("a".into())),
            ("eq/text", Value::String("a\0b".into())),
            ("eq/text", Value::String(spilled_text('s', false))),
            ("eq/text", Value::String(spilled_text('s', true))),
            ("eq/text", Value::String(String::new())),
            ("eq/link", Value::Entity("user:bob".parse()?)),
        ],
    )
    .await?;
    // A second value on the first text's entity, for cardinality one.
    let mut transaction = branch.transaction();
    transaction = transaction.assert(AttributeStatement {
        the: relation("eq/text"),
        of: entities[4].clone(),
        is: Value::String("b".into()),
        cause: None,
        cardinality: None,
        pick: None,
    });
    transaction.commit().publish().perform(&operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let number = || Term::from(relation("eq/number"));
    let text = || Term::from(relation("eq/text"));
    let entity = || Term::<Entity>::var("e");
    let v = || Term::<Any>::var("v");
    let typed = |kind: Type| Term::<Any>::var("v").with_kind(Kind::from(kind));
    let scenarios = vec![
        scan("numbers untyped", number(), entity(), v(), None, nothing),
        scan(
            "numbers as floats",
            number(),
            entity(),
            typed(Type::Float),
            None,
            nothing,
        ),
        scan(
            "numbers as integers",
            number(),
            entity(),
            typed(Type::SignedInt),
            None,
            nothing,
        ),
        scan(
            "numbers as naturals",
            number(),
            entity(),
            typed(Type::UnsignedInt),
            None,
            nothing,
        ),
        scan(
            "numbers as texts",
            number(),
            entity(),
            typed(Type::String),
            None,
            nothing,
        ),
        scan(
            "numbers, v bound to the natural 3",
            number(),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::UnsignedInt(3)),
        ),
        scan(
            "numbers, v bound to the float 3.0",
            number(),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::Float(3.0)),
        ),
        scan(
            "numbers, v bound to the integer -2",
            number(),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::SignedInt(-2)),
        ),
        scan(
            "numbers as floats, v bound to the natural 3",
            number(),
            entity(),
            typed(Type::Float),
            None,
            |row| bind(row, "v", Value::UnsignedInt(3)),
        ),
        scan("texts untyped", text(), entity(), v(), None, nothing),
        scan(
            "texts as texts",
            text(),
            entity(),
            typed(Type::String),
            None,
            nothing,
        ),
        scan(
            "texts as entities",
            text(),
            entity(),
            typed(Type::Entity),
            None,
            nothing,
        ),
        scan(
            "texts, v bound to an equal text",
            text(),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::String("a".into())),
        ),
        scan(
            "texts, v bound to a different text",
            text(),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::String("zzz".into())),
        ),
        scan(
            "texts, v bound to a spilled text",
            text(),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::String(spilled_text('s', true))),
        ),
        scan(
            "texts, v bound to a symbol",
            text(),
            entity(),
            v(),
            None,
            |row| {
                bind(
                    row,
                    "v",
                    Value::Symbol("eq/text".parse().expect("relation")),
                )
            },
        ),
        scan(
            "texts, v bound absent",
            text(),
            entity(),
            v(),
            None,
            |row| row.bind_absent(&Term::var("v")).expect("binds absent"),
        ),
        scan(
            "texts, e bound absent",
            text(),
            entity(),
            v(),
            None,
            |row| row.bind_absent(&Term::var("e")).expect("binds absent"),
        ),
        scan(
            "texts, cardinality one",
            text(),
            entity(),
            v(),
            Some(Cardinality::One),
            nothing,
        ),
        scan(
            "texts, cardinality one, a constant value",
            text(),
            entity(),
            Term::Constant(Value::String("a".into())),
            Some(Cardinality::One),
            nothing,
        ),
        scan(
            "texts, cardinality one, v bound",
            text(),
            entity(),
            v(),
            Some(Cardinality::One),
            |row| bind(row, "v", Value::String("b".into())),
        ),
        scan(
            "links untyped",
            Term::from(relation("eq/link")),
            entity(),
            v(),
            None,
            nothing,
        ),
        scan(
            "links, v bound to the same text as a text",
            Term::from(relation("eq/link")),
            entity(),
            v(),
            None,
            |row| bind(row, "v", Value::String("user:bob".into())),
        ),
        scan(
            "a named attribute over one entity",
            Term::<Relation>::var("the"),
            Term::Constant(Value::Entity(entities[4].clone())),
            v(),
            None,
            nothing,
        ),
        scan(
            "a named attribute, the bound to the symbol",
            Term::<Relation>::var("the"),
            Term::Constant(Value::Entity(entities[4].clone())),
            v(),
            None,
            |row| {
                bind(
                    row,
                    "the",
                    Value::Symbol("eq/text".parse().expect("relation")),
                )
            },
        ),
    ];

    let mut differences = Vec::new();
    for scenario in &scenarios {
        let mut lazy_seed = Match::new();
        (scenario.bind)(&mut lazy_seed);
        let mut eager_seed = Match::eager();
        (scenario.bind)(&mut eager_seed);
        let lazy = rows(&source, &scenario.query, lazy_seed).await;
        let eager = rows(&source, &scenario.query, eager_seed).await;
        match (&lazy, &eager) {
            (Ok(lazy), Ok(eager)) => {
                let observed = |rows: &[Match]| -> Vec<Vec<String>> {
                    let mut seen: Vec<_> = rows
                        .iter()
                        .map(|row| observe(&scenario.query, row))
                        .collect();
                    seen.sort();
                    seen
                };
                let (lazy_seen, eager_seen) = (observed(lazy), observed(eager));
                if lazy_seen != eager_seen {
                    differences.push(format!(
                        "{}: in place {lazy_seen:#?}\neagerly {eager_seen:#?}",
                        scenario.name
                    ));
                }
                let unequal = lazy.len() != eager.len()
                    || lazy
                        .iter()
                        .any(|row| !eager.iter().any(|other| other == row));
                if unequal {
                    differences.push(format!(
                        "{}: rows unequal by Match == ({} in place, {} eagerly)",
                        scenario.name,
                        lazy.len(),
                        eager.len()
                    ));
                }
            }
            (Err(lazy), Err(eager)) if format!("{lazy:?}") == format!("{eager:?}") => {}
            _ => differences.push(format!(
                "{}: in place {:?}, eagerly {:?}",
                scenario.name,
                lazy.as_ref().map(Vec::len),
                eager.as_ref().map(Vec::len)
            )),
        }
    }
    assert!(
        differences.is_empty(),
        "expected every scan to read alike in place and eagerly; {} differ:\n{}",
        differences.len(),
        differences.join("\n")
    );
    Ok(())
}
