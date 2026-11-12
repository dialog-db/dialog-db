//! Patterns, pinned as tests: first by themselves, a pattern against a
//! value and back, then through a branch, where a query takes a stored
//! record apart, a rule builds one, the planner runs a pattern either
//! way round, and inference types what a pattern binds.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::*;
use crate::attribute::{AttributeStatement, Relation};
use crate::corpus::{Composite, Entity};
use crate::error::TypeError;
use crate::formula::Formula;
use crate::formula::math::Sum;
use crate::planner::{Plan, Planner};
use crate::premise::reading;
use crate::query::Application as _;
use crate::rule::body;
use crate::session::RuleRegistry;
use crate::source::test::TestEnv;
use crate::the;
use crate::{ConceptDescriptor, DeductiveRule, FormulaQuery, Parameters};
use dialog_peer::helpers::{test_repo, test_session_with_peer};
use dialog_repository::Branch;
use dialog_storage::provider::storage::VolatileSpace;

type Operator = dialog_peer::Peer<VolatileSpace, dialog_peer::Session>;

fn text(text: &str) -> Value {
    Value::String(text.into())
}

fn var(name: &str) -> Pattern {
    Pattern::Term(Term::var(name))
}

fn note() -> Value {
    Composite::map([
        (text("title"), text("Buy milk")),
        (text("done"), Value::Boolean(false)),
        (text("priority"), Value::UnsignedInt(2)),
    ])
    .expect("a map")
    .into()
}

fn unify(pattern: &Pattern, value: &Value) -> Option<Match> {
    let mut row = Match::new();
    pattern
        .unify(value, &mut row)
        .expect("matches or not")
        .then_some(row)
}

/// A map pattern with no rest is the map of exactly its entries: it
/// takes such a map apart, and matches no map that holds another.
#[dialog_common::test]
fn it_takes_a_map_apart_by_exactly_its_entries() {
    let all = Pattern::map([
        ("title".to_string(), var("title")),
        ("done".to_string(), var("done")),
        ("priority".to_string(), var("priority")),
    ])
    .expect("a pattern");
    let row = unify(&all, &note()).expect("matches");
    assert_eq!(row.value_of("title"), Some(&text("Buy milk")));
    assert_eq!(row.value_of("done"), Some(&Value::Boolean(false)));
    assert_eq!(row.value_of("priority"), Some(&Value::UnsignedInt(2)));

    let some = Pattern::map([("title".to_string(), var("title"))]).expect("a pattern");
    assert!(unify(&some, &note()).is_none(), "the map holds more");

    let other = Pattern::map([("author".to_string(), var("author"))]).expect("a pattern");
    assert!(unify(&other, &note()).is_none(), "the map holds no author");
}

/// A map pattern with a rest binds the entries it did not name, as
/// a map. A blank rest takes them and binds nothing.
#[dialog_common::test]
fn it_binds_the_entries_a_map_pattern_did_not_name() {
    let Pattern::Map(named) =
        Pattern::map([("title".to_string(), var("title"))]).expect("a pattern")
    else {
        panic!("a map pattern");
    };
    let pattern = Pattern::Map(named.clone().with_rest(Term::var("rest")));
    let row = unify(&pattern, &note()).expect("matches");
    assert_eq!(row.value_of("title"), Some(&text("Buy milk")));
    let rest = row.value_of("rest").expect("the rest is bound");
    assert_eq!(
        rest.composite(),
        Ok(Some(
            Composite::map([
                (text("done"), Value::Boolean(false)),
                (text("priority"), Value::UnsignedInt(2)),
            ])
            .expect("a map")
        ))
    );

    let open = Pattern::Map(named.with_rest(Term::blank()));
    let row = unify(&open, &note()).expect("matches");
    assert_eq!(row.value_of("title"), Some(&text("Buy milk")));
}

/// A constant inside a pattern matches the value it reads as, and a
/// variable bound to another value does not match.
#[dialog_common::test]
fn it_matches_constants_and_bound_variables() {
    let Pattern::Map(done) =
        Pattern::map([("done".to_string(), Pattern::from(Value::Boolean(false)))])
            .expect("a pattern")
    else {
        panic!("a map pattern");
    };
    let pattern = Pattern::Map(done.with_rest(Term::blank()));
    assert!(unify(&pattern, &note()).is_some());

    let Pattern::Map(done) =
        Pattern::map([("done".to_string(), Pattern::from(Value::Boolean(true)))])
            .expect("a pattern")
    else {
        panic!("a map pattern");
    };
    assert!(unify(&Pattern::Map(done.with_rest(Term::blank())), &note()).is_none());

    let Pattern::Map(title) =
        Pattern::map([("title".to_string(), var("title"))]).expect("a pattern")
    else {
        panic!("a map pattern");
    };
    let pattern = Pattern::Map(title.with_rest(Term::blank()));
    let mut row = Match::new();
    row.bind(&Term::var("title"), text("Call home"))
        .expect("binds");
    assert!(!pattern.unify(&note(), &mut row).expect("decides"));
}

/// A list pattern takes a list apart by its items, binds a run as a
/// list, and with no run matches a list of exactly its items.
#[dialog_common::test]
fn it_takes_a_list_apart_and_binds_a_run() {
    let list: Value = Composite::list([text("a"), text("b"), text("c")]).into();

    let exact = Pattern::list([var("x"), var("y"), var("z")]);
    let row = unify(&exact, &list).expect("matches");
    assert_eq!(row.value_of("y"), Some(&text("b")));
    assert!(unify(&Pattern::list([var("x")]), &list).is_none());
    assert!(
        unify(
            &Pattern::list([var("a"), var("b"), var("c"), var("d")]),
            &list
        )
        .is_none()
    );

    let Pattern::List(head) = Pattern::list([var("first")]) else {
        panic!("a list pattern");
    };
    let row = unify(&Pattern::List(head.with_run(Term::var("others"))), &list).expect("matches");
    assert_eq!(row.value_of("first"), Some(&text("a")));
    assert_eq!(
        row.value_of("others"),
        Some(&Composite::list([text("b"), text("c")]).into())
    );

    // A list is not a tuple, and neither is a map.
    assert!(unify(&Pattern::tuple([var("x"), var("y"), var("z")]), &list).is_none());
    assert!(unify(&exact, &note()).is_none());
    assert!(unify(&exact, &text("abc")).is_none());
}

/// A tuple pattern takes a tuple of exactly its items apart, and
/// patterns nest as values do.
#[dialog_common::test]
fn it_takes_nested_shapes_apart() {
    let place: Value = Composite::map([
        (text("name"), text("Home")),
        (
            text("at"),
            Composite::tuple([Value::Float(45.5), Value::Float(-122.5)]).into(),
        ),
        (
            text("tags"),
            Composite::list([text("quiet"), text("green")]).into(),
        ),
    ])
    .expect("a map")
    .into();

    let Pattern::List(tags) = Pattern::list([var("tag")]) else {
        panic!("a list pattern");
    };
    let pattern = Pattern::map([
        ("name".to_string(), var("name")),
        (
            "at".to_string(),
            Pattern::tuple([var("latitude"), var("longitude")]),
        ),
        (
            "tags".to_string(),
            Pattern::List(tags.with_run(Term::blank())),
        ),
    ])
    .expect("a pattern");
    let row = unify(&pattern, &place).expect("matches");
    assert_eq!(row.value_of("latitude"), Some(&Value::Float(45.5)));
    assert_eq!(row.value_of("longitude"), Some(&Value::Float(-122.5)));
    assert_eq!(row.value_of("tag"), Some(&text("quiet")));

    let wrong = Pattern::map([
        ("name".to_string(), var("name")),
        ("at".to_string(), Pattern::tuple([var("latitude")])),
        ("tags".to_string(), var("tags")),
    ])
    .expect("a pattern");
    assert!(
        unify(&wrong, &place).is_none(),
        "a tuple of two is not one of one"
    );
}

/// A pattern whose variables are all bound is a value: the one that
/// taking it apart gave the bindings from.
#[dialog_common::test]
fn it_is_the_value_once_its_variables_are_bound() {
    let Pattern::Map(named) =
        Pattern::map([("title".to_string(), var("title"))]).expect("a pattern")
    else {
        panic!("a map pattern");
    };
    let pattern = Pattern::Map(named.with_rest(Term::var("rest")));

    assert_eq!(pattern.build(&Match::new()), None, "nothing is bound yet");
    let row = unify(&pattern, &note()).expect("matches");
    assert_eq!(pattern.build(&row), Some(note()));

    let exact = Pattern::map([
        ("title".to_string(), var("title")),
        ("done".to_string(), Pattern::from(Value::Boolean(false))),
    ])
    .expect("a pattern");
    let mut row = Match::new();
    row.bind(&Term::var("title"), text("Buy milk"))
        .expect("binds");
    assert_eq!(
        exact.build(&row),
        Some(
            Composite::map([
                (text("title"), text("Buy milk")),
                (text("done"), Value::Boolean(false)),
            ])
            .expect("a map")
            .into()
        )
    );

    let Pattern::List(head) = Pattern::list([var("title")]) else {
        panic!("a list pattern");
    };
    let mut row = Match::new();
    row.bind(&Term::var("title"), text("a")).expect("binds");
    row.bind(&Term::var("others"), Composite::list([text("b")]).into())
        .expect("binds");
    assert_eq!(
        Pattern::List(head.with_run(Term::var("others"))).build(&row),
        Some(Composite::list([text("a"), text("b")]).into())
    );
}

/// A pattern that holds a blank is not a value, and neither is one
/// whose rest holds a key it names or is not a map.
#[dialog_common::test]
fn it_is_no_value_where_its_parts_make_none() {
    let Pattern::Map(named) =
        Pattern::map([("title".to_string(), var("title"))]).expect("a pattern")
    else {
        panic!("a map pattern");
    };
    let mut row = Match::new();
    row.bind(&Term::var("title"), text("Buy milk"))
        .expect("binds");

    let open = Pattern::Map(named.clone().with_rest(Term::blank()));
    assert!(open.has_blank());
    assert_eq!(open.build(&row), None);

    let pattern = Pattern::Map(named.with_rest(Term::var("rest")));
    let mut clash = row.clone();
    clash
        .bind(
            &Term::var("rest"),
            Composite::map([(text("title"), text("Other"))])
                .expect("a map")
                .into(),
        )
        .expect("binds");
    assert_eq!(pattern.build(&clash), None, "the rest names the title too");

    let mut scalar = row.clone();
    scalar
        .bind(&Term::var("rest"), text("nope"))
        .expect("binds");
    assert_eq!(pattern.build(&scalar), None, "the rest is not a map");
}

/// A pattern with no variable is a value, like any constant.
#[dialog_common::test]
fn it_is_a_constant_with_no_variable() {
    let constant =
        Pattern::map([("text".to_string(), Pattern::from(text("hi")))]).expect("a pattern");
    assert!(constant.is_constant());
    assert_eq!(
        constant.as_value(),
        Some(
            Composite::map([(text("text"), text("hi"))])
                .expect("a map")
                .into()
        )
    );
    let open = Pattern::map([("text".to_string(), var("x"))]).expect("a pattern");
    assert!(!open.is_constant());
    assert_eq!(open.as_value(), None);
}

/// A record under a version this release does not read is refused
/// by its number, where a record of before simply does not match.
#[dialog_common::test]
fn it_refuses_a_record_of_a_later_version_by_its_number() {
    let pattern = Pattern::map([("title".to_string(), var("title"))]).expect("a pattern");

    let Value::Record(mut bytes) = note() else {
        panic!("a record");
    };
    bytes[0] = 9;
    let mut row = Match::new();
    let refused = pattern.unify(&Value::Record(bytes), &mut row);
    assert!(
        matches!(refused, Err(EvaluationError::RecordVersion { version: 9 })),
        "{refused:?}"
    );

    let before = Value::Record(vec![0xa1, 0x61, 0x61, 0x01]);
    assert!(unify(&pattern, &before).is_none());
    // A leaf takes any record whole, whatever its version.
    let mut row = Match::new();
    assert!(var("x").unify(&before, &mut row).expect("decides"));
}

/// A pattern knows the variables inside it, and whether they are
/// all bound.
#[dialog_common::test]
fn it_knows_its_variables() {
    let Pattern::Map(named) = Pattern::map([
        ("title".to_string(), var("title")),
        (
            "at".to_string(),
            Pattern::tuple([var("x"), Pattern::from(Value::UnsignedInt(1)), var("x")]),
        ),
    ])
    .expect("a pattern") else {
        panic!("a map pattern");
    };
    let pattern = Pattern::Map(named.with_rest(Term::var("rest")));
    let mut names = pattern.variables();
    names.sort();
    assert_eq!(names, vec!["rest", "title", "x"]);

    let mut env = Environment::new();
    env.add("title").add("x");
    assert!(!pattern.is_bound(&env));
    env.add("rest");
    assert!(pattern.is_bound(&env));

    let mut bound = Environment::new();
    pattern.bind(&mut bound);
    assert_eq!(bound.len(), 3);
}

/// A pattern is written the same whatever order its map's entries
/// were given in, and refuses a key given twice.
#[dialog_common::test]
fn it_is_written_one_way() {
    let forward = Pattern::map([("a".to_string(), var("x")), ("b".to_string(), var("y"))])
        .expect("a pattern");
    let backward = Pattern::map([("b".to_string(), var("y")), ("a".to_string(), var("x"))])
        .expect("a pattern");
    assert_eq!(forward, backward);
    assert_eq!(forward.to_string(), backward.to_string());
    assert_eq!(forward.to_string(), r#"{String("a"): ?x, String("b"): ?y}"#);

    assert!(matches!(
        Pattern::map([("a".to_string(), var("x")), ("a".to_string(), var("y"))]),
        Err(CompositeError::DuplicateKey(_))
    ));
}

/// A leaf is written to the wire as a term is. A map, a list or a
/// tuple has no wire form and is refused.
#[dialog_common::test]
fn it_has_no_wire_form_above_a_leaf() {
    assert_eq!(
        serde_json::to_value(var("x")).expect("a leaf is written"),
        serde_json::json!({"?": {"name": "x"}})
    );
    let read: Pattern =
        serde_json::from_value(serde_json::json!({"?": {"name": "x"}})).expect("reads");
    assert_eq!(read, var("x"));

    let composite = Pattern::map([("a".to_string(), var("x"))]).expect("a pattern");
    let refused = serde_json::to_value(&composite).expect_err("no wire form");
    assert!(refused.to_string().contains("no wire form"), "{refused}");
}

fn entity(uri: &str) -> Entity {
    uri.parse().expect("an entity")
}

fn map(entries: &[(&str, Value)]) -> Value {
    Composite::map(
        entries
            .iter()
            .map(|(key, value)| (text(key), value.clone())),
    )
    .expect("a map")
    .into()
}

/// A map pattern over text keys.
fn shape(entries: &[(&str, Pattern)]) -> MapPattern {
    MapPattern::new(
        entries
            .iter()
            .map(|(key, pattern)| (text(key), pattern.clone())),
    )
    .expect("a pattern")
}

/// A concept of one field over `the`, declared `as`.
fn concept(field: &str, the: &str, declared: &str) -> ConceptDescriptor {
    serde_json::from_value(serde_json::json!({
        "with": { field: { "the": the, "as": declared, "pick": "all:" } }
    }))
    .expect("a concept")
}

/// A premise reading `the` of `?this` into `pattern`, under a field
/// declared `as`.
fn read(the: &str, declared: &str, pattern: impl Into<Pattern>) -> Premise {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.set("is".into(), pattern);
    Premise::Assert(Proposition::Concept(ConceptQuery {
        predicate: concept("is", the, declared),
        terms,
    }))
}

/// Assert each `(the, of, is)` in one commit.
async fn store(
    branch: &Branch,
    operator: &Operator,
    facts: &[(Relation, &Entity, Value)],
) -> anyhow::Result<()> {
    let mut transaction = branch.transaction();
    for (the, of, is) in facts {
        transaction = transaction.assert(AttributeStatement {
            the: the.clone(),
            of: (*of).clone(),
            is: is.clone(),
            cause: None,
            cardinality: None,
            pick: None,
        });
    }
    transaction.commit().publish().perform(operator).await?;
    Ok(())
}

/// Three notes: one of a title and whether it is done, one of those
/// and a priority, and one whose payload is a list.
async fn notes(branch: &Branch, operator: &Operator) -> anyhow::Result<()> {
    store(
        branch,
        operator,
        &[
            (
                the!("note/payload"),
                &entity("id:milk"),
                map(&[("title", text("Buy milk")), ("done", Value::Boolean(false))]),
            ),
            (
                the!("note/payload"),
                &entity("id:call"),
                map(&[
                    ("title", text("Call home")),
                    ("done", Value::Boolean(true)),
                    ("priority", Value::UnsignedInt(2)),
                ]),
            ),
            (
                the!("note/payload"),
                &entity("id:list"),
                Composite::list([text("not"), text("a"), text("map")]).into(),
            ),
        ],
    )
    .await
}

/// The rows of a query for `note/payload` of `?this` by `pattern`,
/// each as the entity and the row, in entity order.
async fn payloads(
    source: &TestEnv<'_>,
    pattern: impl Into<Pattern>,
) -> anyhow::Result<Vec<(Entity, Match)>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.set("payload".into(), pattern);
    let rows = ConceptQuery {
        predicate: concept("payload", "note/payload", "record:"),
        terms,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let mut rows: Vec<(Entity, Match)> = rows
        .into_iter()
        .map(|row| {
            let this = Entity::try_from(row.value_of("this").expect("this is bound").clone())
                .expect("an entity");
            (this, row)
        })
        .collect();
    rows.sort_by_key(|row| row.0.to_string());
    Ok(rows)
}

/// A query takes a stored record apart by a map pattern. Without a
/// rest the pattern is the map of exactly its entries, so the note
/// that holds a priority too does not match, and neither does the
/// list. With a rest every map that holds the named entries matches,
/// and the rest is bound to the others, as a map.
#[dialog_common::test]
async fn it_takes_a_stored_record_apart_by_a_map_pattern() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    notes(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let named = shape(&[("title", var("title")), ("done", var("done"))]);

    let rows = payloads(&source, named.clone()).await?;
    assert_eq!(
        rows.len(),
        1,
        "one note is the map of exactly these entries"
    );
    let (this, row) = &rows[0];
    assert_eq!(*this, entity("id:milk"));
    assert_eq!(row.value_of("title"), Some(&text("Buy milk")));
    assert_eq!(row.value_of("done"), Some(&Value::Boolean(false)));

    let rows = payloads(&source, named.clone().with_rest(Term::var("rest"))).await?;
    assert_eq!(rows.len(), 2, "both maps hold a title and whether done");
    let (this, row) = &rows[0];
    assert_eq!(*this, entity("id:call"));
    assert_eq!(row.value_of("title"), Some(&text("Call home")));
    assert_eq!(row.value_of("done"), Some(&Value::Boolean(true)));
    assert_eq!(
        row.value_of("rest"),
        Some(&map(&[("priority", Value::UnsignedInt(2))]))
    );
    let (this, row) = &rows[1];
    assert_eq!(*this, entity("id:milk"));
    assert_eq!(
        row.value_of("rest"),
        Some(&map(&[])),
        "nothing is left over"
    );

    // A blank rest takes the others and binds nothing, and a constant
    // inside the pattern is matched.
    let done = shape(&[("done", Pattern::from(Value::Boolean(true)))]).with_rest(Term::blank());
    let rows = payloads(&source, done).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, entity("id:call"));
    Ok(())
}

/// A value of no shape does not match and is not an error: under a
/// field that declares no type, text, a number and a record of before
/// stored beside a map are passed over, by a query run by itself and by
/// a rule's body alike.
#[dialog_common::test]
async fn it_passes_over_a_value_of_no_shape() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    store(
        &branch,
        &operator,
        &[
            (
                the!("mixed/value"),
                &entity("id:map"),
                map(&[("title", text("Buy milk"))]),
            ),
            (the!("mixed/value"), &entity("id:text"), text("Buy milk")),
            (
                the!("mixed/value"),
                &entity("id:number"),
                Value::UnsignedInt(7),
            ),
            (
                the!("mixed/value"),
                &entity("id:before"),
                Value::Record(vec![0xa1, 0x61, 0x61, 0x01]),
            ),
        ],
    )
    .await?;
    let untyped: ConceptDescriptor = serde_json::from_value(serde_json::json!({
        "with": { "is": { "the": "mixed/value", "pick": "all:" } }
    }))?;
    let reading = || {
        let mut terms = Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.set("is".into(), shape(&[("title", var("title"))]));
        ConceptQuery {
            predicate: untyped.clone(),
            terms,
        }
    };
    let expected = vec![vec![
        Some(Value::Entity(entity("id:map"))),
        Some(text("Buy milk")),
    ]];

    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());
    let rows = reading()
        .evaluate(Match::new().seed(), &source)
        .try_vec()
        .await?;
    let rows: Vec<Vec<Option<Value>>> = rows
        .iter()
        .map(|row| {
            vec![
                row.value_of("this").cloned(),
                row.value_of("title").cloned(),
            ]
        })
        .collect();
    assert_eq!(rows, expected, "run by itself");

    let (rows, _) = solve(
        &source,
        vec![Premise::Assert(Proposition::Concept(reading()))],
        &Environment::new(),
        Match::new(),
        &["this", "title"],
    )
    .await?;
    assert_eq!(rows, expected, "planned, as a rule's body is");
    Ok(())
}

/// A pattern with no variable is a value, like any constant: a query
/// by it finds the fact that holds that value.
#[dialog_common::test]
async fn it_reads_a_pattern_with_nothing_to_bind_as_a_constant() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    notes(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let constant = shape(&[
        ("title", Pattern::from(text("Buy milk"))),
        ("done", Pattern::from(Value::Boolean(false))),
    ]);
    let mut terms = Parameters::new();
    terms.set("payload".into(), constant.clone());
    assert!(!terms.has_shapes(), "it is held as the value it is");
    assert_eq!(
        terms.get("payload"),
        Some(&Term::Constant(map(&[
            ("title", text("Buy milk")),
            ("done", Value::Boolean(false)),
        ])))
    );

    let rows = payloads(&source, constant).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, entity("id:milk"));
    Ok(())
}

/// The head of the rule below: a card, kept as a record.
fn card() -> ConceptDescriptor {
    concept("card", "note/card", "record:")
}

/// `card(this, card) :- title(this, title), body(this, body),
/// card == {title: title, body: body}`, its premises in `order`.
fn card_rule(order: [usize; 3]) -> Result<DeductiveRule, TypeError> {
    let premises = [
        reading(
            the!("note/title"),
            Term::var("this"),
            Term::var("title"),
            None,
        ),
        reading(
            the!("note/body"),
            Term::var("this"),
            Term::var("body"),
            None,
        ),
        Pattern::Map(shape(&[("title", var("title")), ("body", var("body"))]))
            .is(Term::<Any>::var("card")),
    ];
    DeductiveRule::new(
        card(),
        order.iter().map(|at| premises[*at].clone()).collect(),
    )
}

/// The cards a rule derives, by entity.
async fn cards(source: &TestEnv<'_>) -> anyhow::Result<Vec<(Entity, Value)>> {
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("card".into(), Term::var("card"));
    let rows = ConceptQuery {
        predicate: card(),
        terms,
    }
    .evaluate(Match::new().seed(), source)
    .try_vec()
    .await?;
    let mut cards: Vec<(Entity, Value)> = rows
        .iter()
        .map(|row| {
            let this = Entity::try_from(row.value_of("this").expect("this is bound").clone())
                .expect("an entity");
            (this, row.value_of("card").expect("card is bound").clone())
        })
        .collect();
    cards.sort_by_key(|card| card.0.to_string());
    Ok(cards)
}

async fn titles_and_bodies(branch: &Branch, operator: &Operator) -> anyhow::Result<()> {
    store(
        branch,
        operator,
        &[
            (the!("note/title"), &entity("id:milk"), text("Buy milk")),
            (the!("note/body"), &entity("id:milk"), text("Two litres")),
            (the!("note/title"), &entity("id:call"), text("Call home")),
            (the!("note/body"), &entity("id:call"), text("After six")),
            (the!("note/title"), &entity("id:untold"), text("No body")),
        ],
    )
    .await
}

/// A rule builds a record from two bound variables and concludes a
/// fact holding it: the map of the title and the body it read. A note
/// with no body derives no card.
#[dialog_common::test]
async fn it_builds_a_record_from_two_bound_variables_and_concludes_a_fact_holding_it()
-> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    titles_and_bodies(&branch, &operator).await?;

    let mut registry = RuleRegistry::new();
    registry.register(card_rule([0, 1, 2])?)?;
    let source = TestEnv::new(&branch, &operator, registry);

    assert_eq!(
        cards(&source).await?,
        vec![
            (
                entity("id:call"),
                map(&[("title", text("Call home")), ("body", text("After six"))])
            ),
            (
                entity("id:milk"),
                map(&[("title", text("Buy milk")), ("body", text("Two litres"))])
            ),
        ]
    );
    Ok(())
}

/// The rule that builds a record derives the same cards whatever order
/// its premises are written in, the equality first among them, and is
/// one rule under every order.
#[dialog_common::test]
async fn it_builds_a_record_in_either_order_of_premises() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    titles_and_bodies(&branch, &operator).await?;

    let written = card_rule([0, 1, 2])?;
    let mut derived = Vec::new();
    for order in [[0, 1, 2], [2, 0, 1], [1, 2, 0], [2, 1, 0]] {
        let rule = card_rule(order)?;
        assert_eq!(
            rule.try_this(),
            written.try_this(),
            "one rule, in the order {order:?}"
        );
        let mut registry = RuleRegistry::new();
        registry.register(rule)?;
        let source = TestEnv::new(&branch, &operator, registry);
        derived.push(cards(&source).await?);
    }
    assert_eq!(derived[0].len(), 2);
    assert!(
        derived.iter().all(|cards| *cards == derived[0]),
        "every order derives the same cards"
    );
    Ok(())
}

/// The rows a conjunction of `premises` yields from `seed`, each as
/// what it binds `names` to, sorted.
async fn solve(
    source: &TestEnv<'_>,
    premises: Vec<Premise>,
    scope: &Environment,
    seed: Match,
    names: &[&str],
) -> anyhow::Result<(Vec<Vec<Option<Value>>>, Vec<&'static str>)> {
    let plan = Planner::from(premises).plan(scope)?;
    let steps = plan
        .steps
        .iter()
        .map(|step| match step {
            Plan::Concept(..) => "concept",
            Plan::Constraint(..) => "constraint",
            _ => "other",
        })
        .collect();
    let rows = plan.evaluate(seed.seed(), source).try_vec().await?;
    let mut rows: Vec<Vec<Option<Value>>> = rows
        .iter()
        .map(|row| {
            names
                .iter()
                .map(|name| row.value_of(name).cloned())
                .collect()
        })
        .collect();
    rows.sort_by_key(|row| format!("{row:?}"));
    Ok((rows, steps))
}

/// The planner runs a pattern either way round. A premise that binds a
/// parameter holding a pattern has what it yields matched against the
/// pattern, which binds the variables inside it and drops a row that
/// does not match. Where those variables are bound already the value
/// is built first and the premise is read by it. Both give the rows
/// the other does, in either order of the premises as written.
#[dialog_common::test]
async fn it_takes_a_record_apart_in_either_order_of_premises() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    notes(&branch, &operator).await?;
    store(
        &branch,
        &operator,
        &[
            (the!("search/title"), &entity("id:search"), text("Buy milk")),
            (the!("search/title"), &entity("id:other"), text("Nowhere")),
        ],
    )
    .await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    // The notes whose payload is exactly a title some search asks for
    // and not done.
    let payload = || {
        read(
            "note/payload",
            "record:",
            shape(&[
                ("title", var("title")),
                ("done", Pattern::from(Value::Boolean(false))),
            ]),
        )
    };
    let search = || {
        reading(
            the!("search/title"),
            Term::var("search"),
            Term::var("title"),
            None,
        )
    };
    let expected = vec![vec![
        Some(Value::Entity(entity("id:milk"))),
        Some(text("Buy milk")),
        Some(Value::Entity(entity("id:search"))),
    ]];
    let names = ["this", "title", "search"];

    for premises in [vec![payload(), search()], vec![search(), payload()]] {
        let (rows, _) = solve(&source, premises, &Environment::new(), Match::new(), &names).await?;
        assert_eq!(rows, expected);
    }

    // With nothing bound the concept is read first and what it yields
    // is taken apart.
    let (rows, steps) = solve(
        &source,
        vec![payload()],
        &Environment::new(),
        Match::new(),
        &["this", "title"],
    )
    .await?;
    assert_eq!(steps, vec!["concept", "constraint"]);
    assert_eq!(
        rows,
        vec![vec![
            Some(Value::Entity(entity("id:milk"))),
            Some(text("Buy milk"))
        ]]
    );

    // With the title bound the record is built first and the concept
    // is read by it.
    let mut scope = Environment::new();
    scope.add("title");
    let mut seed = Match::new();
    seed.bind(&Term::var("title"), text("Buy milk"))?;
    let (rows, steps) = solve(&source, vec![payload()], &scope, seed, &["this", "title"]).await?;
    assert_eq!(steps, vec!["constraint", "concept"]);
    assert_eq!(
        rows,
        vec![vec![
            Some(Value::Entity(entity("id:milk"))),
            Some(text("Buy milk"))
        ]]
    );
    Ok(())
}

/// A rule takes a stored record apart: it concludes the title held
/// inside each payload that is a map with one, whatever else the map
/// holds, and nothing for a payload of another shape.
#[dialog_common::test]
async fn it_derives_a_fact_from_inside_a_stored_record() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    notes(&branch, &operator).await?;

    let heading = concept("heading", "note/heading", "text:");
    let rule = DeductiveRule::new(
        heading.clone(),
        vec![read(
            "note/payload",
            "record:",
            shape(&[("title", var("heading"))]).with_rest(Term::blank()),
        )],
    )?;
    let mut registry = RuleRegistry::new();
    registry.register(rule)?;
    let source = TestEnv::new(&branch, &operator, registry);

    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("this"));
    terms.insert("heading".into(), Term::var("heading"));
    let rows = ConceptQuery {
        predicate: heading,
        terms,
    }
    .evaluate(Match::new().seed(), &source)
    .try_vec()
    .await?;
    let mut headings: Vec<Value> = rows
        .iter()
        .filter_map(|row| row.value_of("heading").cloned())
        .collect();
    headings.sort_by_key(|value| format!("{value:?}"));
    assert_eq!(headings, vec![text("Buy milk"), text("Call home")]);
    Ok(())
}

/// A negated premise holds a pattern as a positive one does: a row
/// passes when no fact matches the pattern, a blank inside it standing
/// for any value.
#[dialog_common::test]
async fn it_negates_a_premise_that_holds_a_pattern() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    notes(&branch, &operator).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    // Every note whose payload is not a map that says it is done.
    let done = shape(&[("done", Pattern::from(Value::Boolean(true)))]).with_rest(Term::blank());
    let premises = vec![
        read("note/payload", "record:", var("payload")),
        !read("note/payload", "record:", done),
    ];
    let (rows, _) = solve(
        &source,
        premises,
        &Environment::new(),
        Match::new(),
        &["this"],
    )
    .await?;
    assert_eq!(
        rows,
        vec![
            vec![Some(Value::Entity(entity("id:list")))],
            vec![Some(Value::Entity(entity("id:milk")))],
        ]
    );
    Ok(())
}

/// The same over enough candidates that the negated relation is read
/// once and joined (from sixteen up): the join keys on the variables
/// inside the pattern, so a note passes exactly when its payload does
/// not hold its own label.
#[dialog_common::test]
async fn it_negates_a_pattern_over_many_candidates() -> anyhow::Result<()> {
    let (operator, profile) = test_session_with_peer().await;
    let repo = test_repo(&operator, &profile).await;
    let branch = repo.branch("main").open().perform(&operator).await?;
    let entities: Vec<Entity> = (0..24)
        .map(|at| entity(&format!("id:note-{at:02}")))
        .collect();
    let mut facts = Vec::new();
    for (at, note) in entities.iter().enumerate() {
        let held = text(&format!("label-{at:02}"));
        let label = if at % 2 == 0 {
            held.clone()
        } else {
            text("other")
        };
        facts.push((the!("note/payload"), note, map(&[("label", held)])));
        facts.push((the!("note/label"), note, label));
    }
    store(&branch, &operator, &facts).await?;
    let source = TestEnv::new(&branch, &operator, RuleRegistry::new());

    let premises = vec![
        reading(
            the!("note/label"),
            Term::var("this"),
            Term::var("label"),
            None,
        ),
        !read(
            "note/payload",
            "record:",
            shape(&[("label", var("label"))]).with_rest(Term::blank()),
        ),
    ];
    let (rows, _) = solve(
        &source,
        premises,
        &Environment::new(),
        Match::new(),
        &["this"],
    )
    .await?;
    let expected: Vec<Vec<Option<Value>>> = entities
        .iter()
        .enumerate()
        .filter(|(at, _)| at % 2 == 1)
        .map(|(_, note)| vec![Some(Value::Entity(note.clone()))])
        .collect();
    assert_eq!(rows, expected);
    Ok(())
}

/// A rule whose pattern can never be bound is refused when it
/// compiles, naming the variable nothing binds: here the body, which
/// the record is to be built from and no premise reads.
#[dialog_common::test]
fn it_refuses_a_rule_whose_pattern_can_never_be_bound() {
    let premises = vec![
        reading(
            the!("note/title"),
            Term::var("this"),
            Term::var("title"),
            None,
        ),
        Pattern::Map(shape(&[("title", var("title")), ("body", var("body"))]))
            .is(Term::<Any>::var("card")),
    ];
    let refused = DeductiveRule::new(card(), premises).expect_err("nothing binds the body");
    match refused {
        TypeError::RequiredBindings { required } => {
            assert!(required.contains("body"), "names the body: {required}");
            assert!(
                !required.contains("title"),
                "the title is bound: {required}"
            );
        }
        other => panic!("expected RequiredBindings, got {other:?}"),
    }

    // A negated premise never binds, so a variable inside its pattern
    // that nothing else binds is refused the same way.
    let premises = vec![
        reading(
            the!("note/title"),
            Term::var("this"),
            Term::var("heading"),
            None,
        ),
        !read(
            "note/payload",
            "record:",
            shape(&[("title", var("unbound"))]),
        ),
    ];
    let refused = DeductiveRule::new(concept("heading", "note/heading", "text:"), premises)
        .expect_err("nothing binds the variable inside the negated pattern");
    assert!(
        matches!(&refused, TypeError::RequiredBindings { required } if required.contains("unbound")),
        "{refused:?}"
    );
}

/// `total == of + 1`, as a premise.
fn add_one(of: Term<Any>) -> Premise {
    let mut terms = Parameters::new();
    terms.insert("of".into(), of);
    terms.insert("with".into(), Term::constant(1u32));
    terms.insert("is".into(), Term::var("total"));
    Premise::from(FormulaQuery::from(
        Sum::apply(terms).expect("a sum applies"),
    ))
}

/// A pattern's type follows from its shape and its leaves' types. A
/// rule that binds a text from inside a map and adds a number to it is
/// refused, as one that adds a number to a text read from a text field
/// is: by inference, naming the variable.
#[dialog_common::test]
fn it_refuses_adding_a_number_to_a_text_bound_inside_a_map() {
    let total = concept("total", "note/total", "number:");

    let today = DeductiveRule::new(
        total.clone(),
        vec![
            read("note/title", "text:", var("title")),
            add_one(Term::var("title")),
        ],
    )
    .expect_err("a number is not added to a text");
    assert!(
        matches!(&today, TypeError::TypeInference { reason } if reason.contains("title")),
        "{today:?}"
    );

    let title: Term<Any> = Term::<String>::var("title").into();
    let inside = DeductiveRule::new(
        total.clone(),
        vec![
            read(
                "note/payload",
                "record:",
                shape(&[("title", Pattern::Term(title))]).with_rest(Term::blank()),
            ),
            add_one(Term::var("title")),
        ],
    )
    .expect_err("a number is not added to a text bound inside a map");
    assert!(
        matches!(&inside, TypeError::TypeInference { reason } if reason.contains("title")),
        "{inside:?}"
    );

    // A leaf written with no type is any present value, so the same
    // rule over a number inside the map compiles.
    let count: Term<Any> = Term::<u32>::var("count").into();
    DeductiveRule::new(
        total,
        vec![
            read(
                "note/payload",
                "record:",
                shape(&[("count", Pattern::Term(count))]).with_rest(Term::blank()),
            ),
            add_one(Term::var("count")),
        ],
    )
    .expect("a number is added to a number bound inside a map");
}

/// A map, a list or a tuple stands only where a record can be read:
/// under a field that reads text it is refused, and so is a rest that
/// another premise needs to be a number.
#[dialog_common::test]
fn it_types_a_pattern_by_its_shape() {
    let heading = concept("heading", "note/heading", "text:");
    let refused = DeductiveRule::new(
        heading.clone(),
        vec![
            read("note/title", "text:", var("heading")),
            read("note/title", "text:", shape(&[("title", var("x"))])),
        ],
    )
    .expect_err("a text field holds no map");
    assert!(
        matches!(refused, TypeError::TypeInference { .. }),
        "{refused:?}"
    );

    // What an equality sets equal to a map is not narrowed to a
    // record, and neither is a rest: either may still be read from a
    // field that declares no type, where a value of another type is a
    // row to drop.
    card_rule([0, 1, 2]).expect("compiles");
    DeductiveRule::new(
        card(),
        vec![read(
            "note/payload",
            "record:",
            shape(&[("title", var("title"))]).with_rest(Term::var("card")),
        )],
    )
    .expect("compiles");

    // A rest is a map, so it is not added to.
    let refused = DeductiveRule::new(
        concept("total", "note/total", "number:"),
        vec![
            read(
                "note/payload",
                "record:",
                shape(&[("title", var("title"))]).with_rest(Term::var("rest")),
            ),
            add_one(Term::var("rest")),
        ],
    )
    .expect_err("a number is not added to a map");
    assert!(
        matches!(&refused, TypeError::TypeInference { reason } if reason.contains("rest")),
        "{refused:?}"
    );
}

/// A rule that holds a pattern runs where it was built and cannot be
/// stored: format 3 of a rule body has no way to write a pattern, and
/// says so. The rule has an identity, by its parts, and no body.
#[dialog_common::test]
fn it_refuses_to_store_a_rule_that_holds_a_pattern() {
    let rule = card_rule([0, 1, 2]).expect("compiles");
    assert!(rule.try_this().is_some(), "it has an identity");
    assert_eq!(rule.try_encode(), None, "and no stored body");
    let refused = rule.storable().expect_err("it holds a pattern");
    assert!(
        matches!(&refused, TypeError::UnstorablePattern { format, .. } if *format == body::FORMAT),
        "{refused:?}"
    );
    let reason = refused.to_string();
    assert!(reason.contains("format 3"), "{reason}");
    assert!(reason.contains("no way to write a pattern"), "{reason}");

    // Two rules that differ only inside their patterns are two rules.
    let other = DeductiveRule::new(
        card(),
        vec![
            reading(
                the!("note/title"),
                Term::var("this"),
                Term::var("title"),
                None,
            ),
            reading(
                the!("note/body"),
                Term::var("this"),
                Term::var("body"),
                None,
            ),
            Pattern::Map(shape(&[("heading", var("title")), ("body", var("body"))]))
                .is(Term::<Any>::var("card")),
        ],
    )
    .expect("compiles");
    assert_ne!(other.try_this(), rule.try_this());

    // A rule with no pattern is stored as before.
    let plain = DeductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![read("note/title", "text:", var("heading"))],
    )
    .expect("compiles");
    assert_eq!(plain.storable(), Ok(()));
    assert!(plain.try_encode().is_some());
}

/// Installing a rule that holds a pattern is refused, with the reason:
/// asserting it stages nothing and panics, since an assertion has no
/// error to return.
///
/// Native only: the test catches the panic, and a wasm test cannot
/// unwind. What a wasm build does is the same refusal, which
/// [`it_refuses_to_store_a_rule_that_holds_a_pattern`] pins on both
/// targets through `storable`, the check the assertion makes.
#[cfg(not(target_arch = "wasm32"))]
#[test]
fn it_refuses_to_install_a_rule_that_holds_a_pattern() {
    use crate::statement::Changes;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let rule = card_rule([0, 1, 2]).expect("compiles");
    let mut changes = Changes::new();
    let refused = catch_unwind(AssertUnwindSafe(|| {
        changes.assert(&rule);
    }))
    .expect_err("the assertion is refused");
    let reason = refused
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(reason.contains("no way to write a pattern"), "{reason}");
    assert_eq!(changes, Changes::new(), "nothing was staged");
}

/// Parameters hold a pattern under a name. A premise reads a term
/// there whatever it holds: a leaf as it is, and for a map the
/// variable standing for the whole value.
#[dialog_common::test]
fn it_holds_a_pattern_under_a_name() {
    let pattern = Pattern::Map(shape(&[("title", var("title"))]).with_rest(Term::var("rest")));
    let mut terms = Parameters::new();
    terms.insert("this".into(), Term::var("note"));
    terms.set("payload".into(), pattern.clone());

    assert_eq!(terms.get("this"), Some(&Term::var("note")));
    assert_eq!(terms.pattern("this"), Some(var("note")));
    assert_eq!(terms.shape("this"), None);

    assert_eq!(terms.shape("payload"), Some(&pattern));
    assert_eq!(terms.pattern("payload"), Some(pattern.clone()));
    assert_eq!(terms.get("payload"), Some(&Shape::whole_of(&pattern)));
    assert!(terms.has_shapes());

    let mut names: Vec<&str> = terms
        .leaves()
        .into_iter()
        .filter_map(|term| term.name())
        .collect();
    names.sort();
    assert_eq!(names, vec!["note", "rest", "title"]);

    // Run with, every parameter is a term, and the pattern is beside.
    let (lowered, shapes) = terms.lowered();
    assert!(!lowered.has_shapes());
    assert_eq!(shapes.len(), 1);
    assert_eq!(lowered.get("payload"), Some(&shapes[0].whole));
    assert_eq!(*shapes[0].pattern, pattern);

    // Renaming reaches inside the pattern, and the whole follows.
    let renamed = terms.map_terms(|term| match term.name() {
        Some("title") => Term::var("heading"),
        _ => term.clone(),
    });
    let expected = Pattern::Map(shape(&[("title", var("heading"))]).with_rest(Term::var("rest")));
    assert_eq!(renamed.shape("payload"), Some(&expected));
    assert_eq!(renamed.get("payload"), Some(&Shape::whole_of(&expected)));

    // A pattern has no wire form, so the parameters holding one are
    // not written.
    let refused = serde_json::to_value(&terms).expect_err("no wire form");
    assert!(refused.to_string().contains("no wire form"), "{refused}");
}

/// Two premises holding one pattern share its whole, since with its
/// variables bound it is one value in both. A pattern that holds a
/// blank is not one value, so each premise gets a whole of its own.
#[dialog_common::test]
fn it_shares_a_whole_only_where_a_pattern_is_one_value() {
    let closed = Arc::new(Pattern::Map(shape(&[("title", var("title"))])));
    assert_eq!(Shape::of(closed.clone()).whole, Shape::of(closed).whole);

    let open = Arc::new(Pattern::Map(
        shape(&[("title", var("title"))]).with_rest(Term::blank()),
    ));
    assert_ne!(Shape::of(open.clone()).whole, Shape::of(open).whole);
}
