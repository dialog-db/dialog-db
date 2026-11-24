//! Patterns, pinned as tests: first by themselves, a pattern against a
//! value and back, then through a branch, where a query takes a stored
//! record apart, a rule builds one, the planner runs a pattern either
//! way round, and inference types what a pattern binds.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use super::*;
use crate::attribute::{AttributeStatement, Relation};
use crate::corpus::{Composite, CorpusRelation, Entity};
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

/// Inside a map or a list a number matches by the number it is, and
/// not as a scan reads one, through a type. Read that way `{n: 4}`
/// would match a record that holds 4.3 and build one that holds 4, so
/// a rule would hold or not by which of its premises ran first. Both
/// were found by `property_tests`, over patterns drawn at random.
#[dialog_common::test]
fn it_matches_a_number_in_a_record_only_where_it_is_that_number() {
    let constant = |value: Value| Pattern::Term(Term::Constant(value));
    let record = map(&[("n", Value::Float(4.3)), ("m", text("x"))]);
    let whole = map(&[("n", Value::Float(4.0)), ("m", text("x"))]);

    // A constant. Alone it is matched as a scan reads: the natural 4
    // reads 4.3 as 4.
    let four = Pattern::Map(shape(&[
        ("n", constant(Value::UnsignedInt(4))),
        ("m", var("m")),
    ]));
    assert!(unify(&four, &record).is_none());
    assert!(unify(&four, &whole).is_some());
    assert!(unify(&constant(Value::UnsignedInt(4)), &Value::Float(4.3)).is_some());

    // A number written two ways is one number in a record, which keeps
    // `-0.0` as 0. The pattern builds that record, and matches it.
    let zero =
        Pattern::Map(shape(&[("n", constant(Value::Float(-0.0)))]).with_rest(Term::var("r")));
    let built = zero
        .build(&unify(&zero, &map(&[("n", Value::UnsignedInt(0))])).expect("matches"))
        .expect("builds");
    assert_eq!(built, map(&[("n", Value::UnsignedInt(0))]));

    // A variable typed a natural binds the natural a part is, and no
    // part it would have to read as one.
    let natural: Term<Any> = Term::<u32>::var("n").into();
    let typed = Pattern::Map(shape(&[("n", Pattern::Term(natural)), ("m", var("m"))]));
    assert!(unify(&typed, &record).is_none());
    let row = unify(&typed, &whole).expect("matches");
    assert_eq!(row.value_of("n"), Some(&Value::UnsignedInt(4)));
    assert_eq!(typed.build(&row), Some(whole));
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
    let Value::Composite(rest) = rest else {
        panic!("the rest is a composite")
    };
    assert_eq!(
        rest.entries().collect::<Vec<_>>(),
        Composite::map([
            (text("done"), Value::Boolean(false)),
            (text("priority"), Value::UnsignedInt(2)),
        ])
        .expect("a map")
        .entries()
        .collect::<Vec<_>>()
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
    let list: Value = Composite::list([text("a"), text("b"), text("c")])
        .expect("a list")
        .into();

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
        Some(
            &Composite::list([text("b"), text("c")])
                .expect("a list")
                .into()
        )
    );

    // A map is no list.
    assert!(unify(&exact, &note()).is_none());
    assert!(unify(&exact, &text("abc")).is_none());
}

/// A list pattern with no run takes a list of exactly its items apart,
/// and patterns nest as values do.
#[dialog_common::test]
fn it_takes_nested_shapes_apart() {
    let place: Value = Composite::map([
        (text("name"), text("Home")),
        (
            text("at"),
            Composite::list([Value::Float(45.5), Value::Float(-122.5)])
                .expect("a list")
                .into(),
        ),
        (
            text("tags"),
            Composite::list([text("quiet"), text("green")])
                .expect("a list")
                .into(),
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
            Pattern::list([var("latitude"), var("longitude")]),
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
        ("at".to_string(), Pattern::list([var("latitude")])),
        ("tags".to_string(), var("tags")),
    ])
    .expect("a pattern");
    assert!(
        unify(&wrong, &place).is_none(),
        "a list of two is not one of one"
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
    row.bind(
        &Term::var("others"),
        Composite::list([text("b")]).expect("a list").into(),
    )
    .expect("binds");
    assert_eq!(
        Pattern::List(head.with_run(Term::var("others"))).build(&row),
        Some(
            Composite::list([text("a"), text("b")])
                .expect("a list")
                .into()
        )
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

/// Opaque record bytes are not parsed as composites and do not match
/// a structural pattern.
#[dialog_common::test]
fn it_does_not_take_apart_opaque_record_bytes() {
    let pattern = Pattern::map([("title".to_string(), var("title"))]).expect("a pattern");

    let mut row = Match::new();
    let record = Value::Record(vec![0xa1, 0x61, 0x61, 0x01]);
    assert!(!pattern.unify(&record, &mut row).expect("decides"));
    // A leaf takes any opaque record whole.
    let mut row = Match::new();
    assert!(var("x").unify(&record, &mut row).expect("decides"));
}

/// A pattern knows the variables inside it, and whether they are
/// all bound.
#[dialog_common::test]
fn it_knows_its_variables() {
    let Pattern::Map(named) = Pattern::map([
        ("title".to_string(), var("title")),
        (
            "at".to_string(),
            Pattern::list([var("x"), Pattern::from(Value::UnsignedInt(1)), var("x")]),
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
        Err(MapPatternError::DuplicateKey(_))
    ));
}

/// Which keys are one is the map's to say: two keys that are one number
/// are one key, and a pattern that names both is refused, where a set
/// of `Value`s would take them for two and the pattern would build a
/// map it does not match.
#[dialog_common::test]
fn it_refuses_two_keys_a_map_holds_as_one() {
    let pairs = [
        (Value::UnsignedInt(3), Value::Float(3.0)),
        (Value::SignedInt(3), Value::Float(3.0)),
        (Value::UnsignedInt(3), Value::SignedInt(3)),
        (Value::SignedInt(-2), Value::Float(-2.0)),
    ];
    for (one, other) in pairs {
        assert_ne!(one, other, "two values");
        let refused = MapPattern::new([(one.clone(), var("a")), (other.clone(), var("b"))]);
        assert!(
            matches!(refused, Err(MapPatternError::DuplicateKey(_))),
            "{one:?} and {other:?}: {refused:?}"
        );
        // A map of the two holds one key.
        let held = Composite::map([(one, Value::UnsignedInt(1)), (other, Value::UnsignedInt(2))])
            .expect("a map");
        let keys: Vec<Value> = held.entries().map(|(key, _)| key).collect();
        assert_eq!(keys.len(), 2, "two values");
        assert_eq!(keys[0], keys[1], "under one key");
    }

    // Keys a map holds apart stay apart.
    let apart = MapPattern::new([
        (Value::UnsignedInt(3), var("a")),
        (Value::Float(3.5), var("b")),
        (Value::String("3".into()), var("c")),
    ])
    .expect("three keys");
    assert_eq!(apart.entries().len(), 3);
}

/// A leaf is written as a term is. A map or a list is written under
/// `match` as a composite value is written for a human reader: a map as
/// its entries, each a key and a pattern, and a list as its items, with
/// what takes the rest beside them under `...`. Inside a pattern a map or
/// a list is `{ "map": .. }` or `{ "list": .. }` again, and a constant is
/// written under the name of its type.
#[dialog_common::test]
fn it_writes_a_map_and_a_list_as_their_entries_under_match() {
    assert_eq!(
        serde_json::to_value(var("x")).expect("a leaf is written"),
        serde_json::json!({"?": {"name": "x"}})
    );
    let read: Pattern =
        serde_json::from_value(serde_json::json!({"?": {"name": "x"}})).expect("reads");
    assert_eq!(read, var("x"));

    // {author: ?who, pinned: true, ...?others}
    let map = Pattern::Map(
        shape(&[
            ("author", var("who")),
            (
                "pinned",
                Pattern::Term(Term::Constant(Value::Boolean(true))),
            ),
        ])
        .with_rest(Term::var("others")),
    );
    let written = serde_json::to_value(&map).expect("a map is written");
    assert_eq!(
        written,
        serde_json::json!({ "match": {
            "map": [
                [{ "text:": "author" }, { "?": { "name": "who" } }],
                [{ "text:": "pinned" }, { "boolean:": true }],
            ],
            "...": { "?": { "name": "others" } },
        } })
    );
    assert_eq!(
        serde_json::from_value::<Pattern>(written).expect("reads"),
        map
    );

    // [?first, "math", ...?more]
    let list = Pattern::List(
        ListPattern::new([var("first"), Pattern::Term(Term::Constant(text("math")))])
            .with_run(Term::var("more")),
    );
    let written = serde_json::to_value(&list).expect("a list is written");
    assert_eq!(
        written,
        serde_json::json!({ "match": {
            "list": [{ "?": { "name": "first" } }, { "text:": "math" }],
            "...": { "?": { "name": "more" } },
        } })
    );
    assert_eq!(
        serde_json::from_value::<Pattern>(written).expect("reads"),
        list
    );

    // Inside one another there is no `match` again, and `...` with
    // nothing named after it is a blank.
    let nested = Pattern::map([
        (
            text("tags"),
            Pattern::List(ListPattern::new([var("first")]).with_run(Term::blank())),
        ),
        (text("by"), Pattern::Map(shape(&[("name", var("name"))]))),
    ])
    .expect("a pattern");
    let written = serde_json::to_value(&nested).expect("is written");
    assert_eq!(
        written,
        serde_json::json!({ "match": { "map": [
            [{ "text:": "by" }, { "map": [[{ "text:": "name" }, { "?": { "name": "name" } }]] }],
            [{ "text:": "tags" }, { "list": [{ "?": { "name": "first" } }], "...": { "?": {} } }],
        ] } })
    );
    assert_eq!(
        serde_json::from_value::<Pattern>(written).expect("reads"),
        nested
    );

    // A constant that is a map is a constant still, under the name of
    // its type, alone and inside a pattern.
    let constant = Pattern::Term(Term::Constant(self::map(&[("a", text("b"))])));
    let written = serde_json::to_value(&constant).expect("a constant is written");
    assert_eq!(
        serde_json::from_value::<Pattern>(written).expect("reads"),
        constant
    );
    let holding = Pattern::list([constant, var("x")]);
    let written = serde_json::to_value(&holding).expect("is written");
    assert_eq!(
        serde_json::from_value::<Pattern>(written).expect("reads"),
        holding
    );
}

/// Every pattern reads back as itself, as JSON and as dag-cbor: the
/// empty ones, a constant of each kind inside one, a map or a list at any
/// place inside another, and a map keyed by every kind of key.
#[dialog_common::test]
fn it_reads_back_every_pattern_it_writes() {
    let constant = |value: Value| Pattern::Term(Term::Constant(value));
    let symbol: CorpusRelation = "xyz/open".parse().expect("a symbol");
    let patterns = [
        Pattern::map::<Value, Pattern>([]).expect("the map of nothing"),
        Pattern::list::<Pattern>([]),
        Pattern::Map(shape(&[]).with_rest(Term::var("all"))),
        Pattern::Map(shape(&[]).with_rest(Term::blank())),
        Pattern::List(ListPattern::new([]).with_run(Term::var("all"))),
        Pattern::list([
            constant(text("text")),
            constant(Value::UnsignedInt(7)),
            constant(Value::SignedInt(-7)),
            constant(Value::Float(1.5)),
            constant(Value::Boolean(false)),
            constant(Value::Entity(entity("note:1"))),
            constant(Value::Symbol(symbol.clone())),
            constant(map(&[("a", text("b"))])),
        ]),
        Pattern::list([
            Pattern::list([Pattern::list::<Pattern>([])]),
            Pattern::Map(shape(&[("a", Pattern::list([var("x")]))])),
        ]),
        // A map of one key, of any key, is a map pattern.
        Pattern::Map(shape(&[("name", var("x"))])),
        Pattern::Map(shape(&[("?", var("x"))])),
        Pattern::Map(shape(&[("...", var("x"))])),
        Pattern::Map(shape(&[("text:", var("x"))])),
        Pattern::Map(shape(&[("composite:map", var("x"))])),
        Pattern::Map(shape(&[("map", var("x")), ("list", var("y"))])),
        Pattern::Map(shape(&[("match", var("x"))])),
        Pattern::map([
            (Value::Symbol(symbol.clone()), var("s")),
            (text("xyz/open"), var("t")),
            (Value::UnsignedInt(3), var("n")),
            (Value::Boolean(true), var("b")),
            (Value::Entity(entity("note:1")), var("e")),
        ])
        .expect("five keys"),
        // A list that ends in a map of a rest alone, and one with a run.
        Pattern::list([var("a"), Pattern::Map(shape(&[]).with_rest(Term::var("r")))]),
        Pattern::List(ListPattern::new([var("a")]).with_run(Term::var("r"))),
        // A constant run.
        Pattern::List(ListPattern::new([var("first")]).with_run(Term::Constant(
            Composite::list([text("z")]).expect("a list").into(),
        ))),
    ];
    for pattern in patterns {
        let json = serde_json::to_value(&pattern).expect("is written");
        let read: Pattern = serde_json::from_value(json.clone()).expect("reads");
        assert_eq!(read, pattern, "{json}");
        // A composite constant is written in dag-cbor as its bytes, which
        // a term is not read back from: a rule holding one inside a
        // pattern is not stored
        // (`it_refuses_to_store_a_composite_constant_inside_a_pattern`).
        let composite = pattern
            .terms()
            .into_iter()
            .any(|term| matches!(term, Term::Constant(Value::Composite(_))));
        if composite {
            continue;
        }
        let bytes = serde_ipld_dagcbor::to_vec(&pattern).expect("is written");
        let read: Pattern = serde_ipld_dagcbor::from_slice(&bytes).expect("reads");
        assert_eq!(read, pattern, "{json}");
    }
}

/// A map pattern is written one way: its entries in the order the map
/// holds them, whatever order they were given in, so one rule is one
/// body. A symbol key and the text that spells it are two keys, and stay
/// two when read back.
#[dialog_common::test]
fn it_writes_a_map_pattern_one_way_whatever_its_keys() {
    let lat: CorpusRelation = "geo/lat".parse().expect("an attribute");
    let entries = [
        (Value::Symbol(lat.clone()), var("by_symbol")),
        (text("geo/lat"), var("by_text")),
        (Value::UnsignedInt(1), var("by_number")),
    ];
    let forward = Pattern::map(entries.clone()).expect("a pattern");
    let mut reversed = entries;
    reversed.reverse();
    let backward = Pattern::map(reversed).expect("a pattern");
    let bytes = serde_ipld_dagcbor::to_vec(&forward).expect("is written");
    assert_eq!(
        serde_ipld_dagcbor::to_vec(&backward).expect("is written"),
        bytes
    );
    let read: Pattern = serde_ipld_dagcbor::from_slice(&bytes).expect("reads");
    let Pattern::Map(read) = read else {
        panic!("a map pattern");
    };
    assert_eq!(read.entries().len(), 3);
    assert!(
        read.entries()
            .iter()
            .any(|(key, _)| key == &Value::Symbol(lat.clone()))
    );
    assert!(
        read.entries()
            .iter()
            .any(|(key, _)| key == &text("geo/lat"))
    );
}

/// What is under `match` and is no pattern is refused, and so is
/// anything beside it. A constant written bare inside a pattern is read,
/// as a term alone reads one.
#[dialog_common::test]
fn it_refuses_what_is_under_match_and_is_no_pattern() {
    for unread in [
        // A term under `match`.
        serde_json::json!({ "match": { "?": { "name": "x" } } }),
        serde_json::json!({ "match": 1 }),
        // A map and a list at once, and a word beside either.
        serde_json::json!({ "match": { "map": [], "list": [] } }),
        serde_json::json!({ "match": { "map": [], "rest": 1 } }),
        // Entries that are not pairs, and a key that is a variable.
        serde_json::json!({ "match": { "map": [[{ "text:": "a" }]] } }),
        serde_json::json!({ "match": { "map": [{ "text:": "a" }] } }),
        serde_json::json!({ "match": { "map": [[{ "?": { "name": "k" } }, 1]] } }),
        // A key given twice.
        serde_json::json!({ "match": { "map": [[{ "text:": "a" }, 1], [{ "text:": "a" }, 2]] } }),
        // Parts that are no list.
        serde_json::json!({ "match": { "list": { "a": 1 } } }),
        // A type that is none.
        serde_json::json!({ "match": { "list": [{ "nothing:": 1 }] } }),
        // Something beside `match`.
        serde_json::json!({ "match": { "list": [] }, "rest": { "?": {} } }),
    ] {
        assert!(
            serde_json::from_value::<Pattern>(unread.clone()).is_err(),
            "{unread}"
        );
    }
    let read: Pattern = serde_json::from_value(serde_json::json!({ "match": { "map": [
        ["title", "Buy milk"], ["priority", 2], ["done", false]
    ] } }))
    .expect("reads");
    let expected = Pattern::Map(shape(&[
        ("title", Pattern::Term(Term::Constant(text("Buy milk")))),
        (
            "priority",
            Pattern::Term(Term::Constant(Value::UnsignedInt(2))),
        ),
        ("done", Pattern::Term(Term::Constant(Value::Boolean(false)))),
    ]));
    assert_eq!(read, expected);
}

/// A map pattern matches a map that holds keys a pattern is written with
/// beside others, its rest taking them unnamed.
#[dialog_common::test]
fn it_matches_a_map_that_holds_keys_a_pattern_is_written_with() {
    let held = map(&[("text:", text("kept")), ("title", text("Buy milk"))]);
    let pattern = Pattern::Map(shape(&[("title", var("title"))]).with_rest(Term::var("rest")));
    let row = unify(&pattern, &held).expect("matches");
    assert_eq!(row.value_of("title"), Some(&text("Buy milk")));
    assert_eq!(row.value_of("rest"), Some(&map(&[("text:", text("kept"))])));
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
        "with": { field: { "the": the, "as": declared, "cardinality": "many" } }
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
                Composite::list([text("not"), text("a"), text("map")])
                    .expect("a list")
                    .into(),
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
        predicate: concept("payload", "note/payload", "composite:map"),
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
        "with": { "is": { "the": "mixed/value", "cardinality": "many" } }
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
    concept("card", "note/card", "composite:map")
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
            "composite:map",
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
            "composite:map",
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
        read("note/payload", "composite:map", var("payload")),
        !read("note/payload", "composite:map", done),
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
            "composite:map",
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
            "composite:map",
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
                "composite:map",
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
                "composite:map",
                shape(&[("count", Pattern::Term(count))]).with_rest(Term::blank()),
            ),
            add_one(Term::var("count")),
        ],
    )
    .expect("a number is added to a number bound inside a map");
}

/// A map or a list stands only where a record can be read:
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
            "composite:map",
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
                "composite:map",
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

/// A rule that holds a pattern is stored: format 5 of a rule body
/// writes a map or a list of terms where a parameter stands. The body
/// names format 5, reads back as the rule it was, under the name it had
/// before it could be written, and encodes to the bytes it was read
/// from. A rule that holds no pattern is still written in format 4.
#[dialog_common::test]
fn it_stores_a_rule_that_holds_a_pattern() {
    let rule = card_rule([0, 1, 2]).expect("compiles");
    let named = rule.try_this().expect("it has an identity");
    assert_eq!(rule.storable(), Ok(()));
    let stored = rule.try_encode().expect("and a stored body");
    assert_eq!(body::format_of(&stored), Some(5));
    let decoded = DeductiveRule::decode(&stored).expect("the body reads back");
    assert_eq!(decoded.try_this(), Some(named.clone()));
    assert_eq!(decoded.try_encode(), Some(stored.clone()));

    // However its premises were given, it is one body.
    let reordered = card_rule([2, 0, 1]).expect("compiles");
    assert_eq!(reordered.try_encode(), Some(stored.clone()));

    // Two rules that differ only inside their patterns are two rules,
    // of two bodies.
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
    assert_ne!(other.try_this(), Some(named));
    assert_ne!(other.try_encode(), Some(stored));

    // A rule with no pattern is stored as before, in format 4.
    let plain = DeductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![read("note/title", "text:", var("heading"))],
    )
    .expect("compiles");
    assert_eq!(plain.storable(), Ok(()));
    let stored = plain.try_encode().expect("a body");
    assert_eq!(body::format_of(&stored), Some(4));
}

/// A rule that compiles is stored, and the stored body holds integers
/// of at most 64 bits and finite floats. So a constant no body carries
/// is refused where the rule is made, inside a map or a list as
/// alone: found by writing patterns drawn at random
/// (`it_reads_back_every_pattern_drawn_as_it_was_written`).
#[dialog_common::test]
fn it_refuses_a_constant_inside_a_pattern_that_no_stored_body_carries() {
    let holding = |constant: Value| {
        DeductiveRule::new(
            card(),
            vec![
                reading(
                    the!("note/title"),
                    Term::var("this"),
                    Term::var("title"),
                    None,
                ),
                Pattern::Map(shape(&[
                    ("title", var("title")),
                    ("n", Pattern::Term(Term::Constant(constant))),
                ]))
                .is(Term::<Any>::var("card")),
            ],
        )
    };
    for constant in [
        Value::UnsignedInt(u128::from(u64::MAX) + 1),
        Value::SignedInt(i128::from(i64::MIN) - 1),
        Value::Float(f64::INFINITY),
    ] {
        assert!(
            matches!(
                holding(constant.clone()),
                Err(TypeError::UnencodableConstant { .. })
            ),
            "{constant:?}"
        );
    }
    let kept = holding(Value::UnsignedInt(u128::from(u64::MAX))).expect("compiles");
    let stored = kept.try_encode().expect("and has a body");
    assert_eq!(
        DeductiveRule::decode(&stored)
            .expect("which reads back")
            .try_this(),
        kept.try_this()
    );
}

/// Asserting a rule that holds a pattern stages it with its body, as
/// asserting any rule does.
#[dialog_common::test]
fn it_installs_a_rule_that_holds_a_pattern() {
    use crate::statement::Changes;

    let rule = card_rule([0, 1, 2]).expect("compiles");
    let mut changes = Changes::new();
    changes.assert(&rule);
    assert_ne!(changes, Changes::new(), "the rule is staged");
    assert!(changes.refused().is_none(), "and not refused");
    assert!(
        serde_json::to_string(&changes).is_ok(),
        "a batch that holds it is written"
    );

    let mut again = Changes::new();
    again.assert(card_rule([2, 1, 0]).expect("compiles"));
    assert_eq!(again, changes, "under one name, with one body");
}

/// A rule that builds a record keyed by a symbol, a map whose keys are
/// attributes, is stored as any other: format 5 writes a map pattern's
/// key whatever value it is, and the body reads back as the rule.
#[dialog_common::test]
fn it_stores_a_rule_that_builds_a_record_keyed_by_symbols() {
    let rule = symbol_rule();
    assert_eq!(rule.storable(), Ok(()));
    let stored = rule.try_encode().expect("a body");
    assert_eq!(body::format_of(&stored), Some(5));
    let decoded = DeductiveRule::decode(&stored).expect("reads back");
    assert!(decoded.stored_as(&rule.this()));
    assert_eq!(decoded.try_encode(), Some(stored));
}

/// A rule that holds a constant map inside a pattern is stored: format 5
/// writes the constant as its canonical bytes, and the body reads back as
/// the rule, under its name, and encodes to the bytes it was read from.
#[dialog_common::test]
fn it_stores_a_rule_that_holds_a_composite_constant_inside_a_pattern() {
    let rule = constant_rule();
    assert_eq!(rule.storable(), Ok(()));
    let stored = rule.try_encode().expect("a body");
    assert_eq!(body::format_of(&stored), Some(5));
    let decoded = DeductiveRule::decode(&stored).expect("the body reads back");
    assert!(decoded.stored_as(&rule.this()), "under its name");
    assert_eq!(decoded.try_encode(), Some(stored));
}

/// Asserting a rule that holds a constant map inside a pattern stages it
/// with its body, as asserting any rule does, and refuses nothing.
#[dialog_common::test]
fn it_installs_a_rule_that_holds_a_composite_constant_inside_a_pattern() {
    use crate::statement::Changes;

    let rule = constant_rule();
    let mut changes = Changes::new();
    changes.assert(&rule);
    assert!(changes.refused().is_none(), "nothing is refused");
    assert!(!changes.is_empty(), "the rule is staged");
    assert!(
        serde_json::to_string(&changes).is_ok(),
        "a batch that holds it is written"
    );
}

/// `card(this, card) :- title(this, title),
/// card == {title: title, by: {name: "me"}}`: a constant map inside a
/// pattern.
fn constant_rule() -> DeductiveRule {
    DeductiveRule::new(
        card(),
        vec![
            reading(
                the!("note/title"),
                Term::var("this"),
                Term::var("title"),
                None,
            ),
            Pattern::Map(shape(&[
                ("title", var("title")),
                (
                    "by",
                    Pattern::Term(Term::Constant(map(&[("name", text("me"))]))),
                ),
            ]))
            .is(Term::<Any>::var("card")),
        ],
    )
    .expect("compiles")
}

/// A body stamped with a format below 5 that holds a pattern was
/// written by no release, since none below 5 writes one: it is refused
/// when it is read, and the same descriptor under format 5 reads.
#[dialog_common::test]
fn it_refuses_a_pattern_in_a_body_of_an_earlier_format() {
    let rule = card_rule([0, 1, 2]).expect("compiles");
    for format in [0, 3, 4] {
        let stamped = body::encode_as(format, &rule.canonical_descriptor()).expect("a body");
        let error = DeductiveRule::decode(&stamped).expect_err("refused");
        assert!(error.contains("writes no pattern"), "{format}: {error}");
    }
    let stamped = body::encode_as(5, &rule.canonical_descriptor()).expect("a body");
    assert_eq!(
        DeductiveRule::decode(&stamped).expect("reads").try_this(),
        rule.try_this()
    );
}

/// `geo(this, at) :- lat(this, lat), at == {geo/lat: lat}`: a rule that
/// builds a record keyed by a symbol.
fn symbol_rule() -> DeductiveRule {
    let lat: CorpusRelation = "geo/lat".parse().expect("an attribute");
    DeductiveRule::new(
        concept("at", "geo/at", "composite:map"),
        vec![
            reading(the!("geo/lat"), Term::var("this"), Term::var("lat"), None),
            Pattern::map([(Value::Symbol(lat), var("lat"))])
                .expect("a pattern")
                .is(Term::<Any>::var("at")),
        ],
    )
    .expect("compiles")
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

    let mut names: Vec<&str> = terms.leaves().filter_map(|term| term.name()).collect();
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

    // Written, each name holds what it was given: a term as a term is
    // written, and the pattern under `match`.
    let written = serde_json::to_value(&terms).expect("parameters are written");
    assert_eq!(
        written,
        serde_json::json!({
            "this": { "?": { "name": "note" } },
            "payload": { "match": {
                "map": [[{ "text:": "title" }, { "?": { "name": "title" } }]],
                "...": { "?": { "name": "rest" } },
            } },
        })
    );
    let read: Parameters = serde_json::from_value(written).expect("and read back");
    assert_eq!(read, terms);
    let bytes = serde_ipld_dagcbor::to_vec(&terms).expect("as dag-cbor too");
    let read: Parameters = serde_ipld_dagcbor::from_slice(&bytes).expect("and read back");
    assert_eq!(read, terms);
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

/// A map can hold a key more than once, a value under it each time. A
/// map pattern names a key for one value: naming a key that holds
/// several never matches, built or taken apart. A rest takes such a key
/// whole, every value under it, and a blank rest passes it by.
#[dialog_common::test]
fn it_matches_no_key_that_holds_several_values_and_a_rest_takes_it_whole() {
    let tagged = Value::Composite(
        Composite::map([
            (text("title"), text("Buy milk")),
            (text("tag"), text("errand")),
            (text("tag"), text("home")),
        ])
        .expect("a map"),
    );

    let naming = Pattern::Map(shape(&[("tag", var("tag"))]).with_rest(Term::blank()));
    assert!(unify(&naming, &tagged).is_none(), "a key of two values");

    let rest = Pattern::Map(shape(&[("title", var("title"))]).with_rest(Term::var("rest")));
    let row = unify(&rest, &tagged).expect("the rest takes the key whole");
    let Some(Value::Composite(others)) = row.value_of("rest") else {
        panic!("the rest is a map");
    };
    assert_eq!(others.get(&text("tag")), vec![text("errand"), text("home")]);
    // Built again from what it bound, it is the value it took apart.
    assert_eq!(rest.build(&row), Some(tagged.clone()));

    let blank = Pattern::Map(shape(&[("title", var("title"))]).with_rest(Term::blank()));
    assert!(unify(&blank, &tagged).is_some());

    let exact = Pattern::Map(shape(&[("title", var("title"))]));
    assert!(unify(&exact, &tagged).is_none(), "the others are left over");
}

/// A record is a map whose keys are attributes, held as symbols. A map
/// pattern reads it through symbol keys alone: the attribute's name as
/// text is another key. A pattern of symbol keys builds what
/// `is_record` says is a record, and opaque record bytes match no map.
#[dialog_common::test]
fn it_reads_a_record_through_symbol_keys() {
    let lat: CorpusRelation = "geo/lat".parse().expect("an attribute");
    let record = Value::Composite(
        Composite::record([(lat.clone(), Value::Float(37.77))]).expect("a record"),
    );

    let by_symbol = Pattern::map([(Value::Symbol(lat.clone()), var("lat"))]).expect("a pattern");
    let row = unify(&by_symbol, &record).expect("a record matches by symbol");
    assert_eq!(row.value_of("lat"), Some(&Value::Float(37.77)));
    let Some(Value::Composite(built)) = by_symbol.build(&row) else {
        panic!("a composite is built");
    };
    assert!(built.is_record(), "symbol keys build a record");
    assert_eq!(Value::Composite(built), record);

    let by_text = Pattern::map([(text("geo/lat"), var("lat"))]).expect("a pattern");
    assert!(unify(&by_text, &record).is_none(), "text is another key");
    let Some(Value::Composite(built)) = by_text.build(&row) else {
        panic!("a composite is built");
    };
    assert!(!built.is_record(), "text keys build a map");
}

/// A variable named twice in one pattern is one variable: `[?a ?a ?b]`
/// matches `[1 1 2]` and binds `?a` once, and refuses `[1 2 2]`.
#[dialog_common::test]
fn it_binds_a_variable_named_twice_once() {
    let pattern = Pattern::list([var("a"), var("a"), var("b")]);
    let list = |items: [u128; 3]| {
        Value::Composite(Composite::list(items.map(Value::UnsignedInt)).expect("a list"))
    };

    let row = unify(&pattern, &list([1, 1, 2])).expect("a repeats");
    assert_eq!(row.value_of("a"), Some(&Value::UnsignedInt(1)));
    assert_eq!(row.value_of("b"), Some(&Value::UnsignedInt(2)));
    assert_eq!(pattern.build(&row), Some(list([1, 1, 2])));

    assert!(unify(&pattern, &list([1, 2, 2])).is_none());
}

/// An empty map pattern matches the empty map alone, and an empty list
/// pattern the empty list alone: neither matches the other kind, nor a
/// composite that holds anything.
#[dialog_common::test]
fn it_matches_an_empty_composite_of_its_own_kind_alone() {
    let empty_map = Value::Composite(Composite::map([]).expect("a map"));
    let empty_list = Value::Composite(Composite::list([]).expect("a list"));
    let no_entries = Pattern::map(Vec::<(Value, Pattern)>::new()).expect("a pattern");
    let no_items = Pattern::list(Vec::<Pattern>::new());

    assert!(unify(&no_entries, &empty_map).is_some());
    assert!(unify(&no_entries, &empty_list).is_none());
    assert!(unify(&no_entries, &map(&[("a", text("x"))])).is_none());
    assert!(unify(&no_items, &empty_list).is_some());
    assert!(unify(&no_items, &empty_map).is_none());
    assert!(
        unify(
            &no_items,
            &Value::Composite(Composite::list([text("x")]).expect("a list"))
        )
        .is_none()
    );
    assert_eq!(no_entries.as_value(), Some(empty_map));
    assert_eq!(no_items.as_value(), Some(empty_list));
}

/// A map pattern with no variable is a constant: it is the value it
/// builds, matches that value alone and binds nothing.
#[dialog_common::test]
fn it_is_a_constant_when_it_holds_no_variable() {
    let pattern = Pattern::map([
        ("title".to_string(), Pattern::from(text("Buy milk"))),
        ("done".to_string(), Pattern::from(Value::Boolean(false))),
    ])
    .expect("a pattern");
    assert!(pattern.is_constant());
    assert!(pattern.variables().is_empty());
    let value = map(&[("title", text("Buy milk")), ("done", Value::Boolean(false))]);
    assert_eq!(pattern.as_value(), Some(value.clone()));
    assert_eq!(pattern.build(&Match::new()), Some(value.clone()));

    let row = unify(&pattern, &value).expect("it matches what it is");
    assert_eq!(row, Match::new(), "and binds nothing");
    assert!(
        unify(
            &pattern,
            &map(&[("title", text("Buy milk")), ("done", Value::Boolean(true))])
        )
        .is_none()
    );
}

/// A constant map or list inside a pattern is written as its canonical
/// bytes in a stored body, and as its entries in JSON, and reads back
/// from both: a rule holding one, at any depth, is stored in format 5 and
/// its body reads back as the rule.
#[dialog_common::test]
fn it_stores_a_composite_constant_inside_a_pattern() {
    for (constant, _) in composite_constants() {
        let pattern = Pattern::map([
            (text("by"), Pattern::Term(Term::Constant(constant.clone()))),
            (text("title"), var("heading")),
        ])
        .expect("a pattern");
        let json = serde_json::to_value(&pattern).expect("is written");
        assert_eq!(
            serde_json::from_value::<Pattern>(json).expect("reads"),
            pattern
        );
        let rule = DeductiveRule::new(
            concept("heading", "note/heading", "text:"),
            vec![read("note/payload", "composite:map", pattern)],
        )
        .expect("compiles");
        assert_eq!(rule.storable(), Ok(()));
        let stored = rule.try_encode().expect("a body");
        assert_eq!(body::format_of(&stored), Some(5), "{constant:?}");
        let decoded = DeductiveRule::decode(&stored).expect("the body reads back");
        assert!(decoded.stored_as(&rule.this()));
        assert_eq!(decoded.try_encode(), Some(stored));
    }
}

/// A negated premise keeps its pattern, where a positive one is read
/// through an equality beside it, so the stored body writes the pattern
/// inside the concept application: a rule that negates one reads back as
/// the rule it was, under its name, and writes the same body again.
#[dialog_common::test]
fn it_stores_a_rule_that_negates_a_pattern() {
    let done = shape(&[("done", Pattern::from(Value::Boolean(true)))]).with_rest(Term::blank());
    let rule = DeductiveRule::new(
        concept("open", "note/open", "composite:map"),
        vec![
            read("note/payload", "composite:map", var("open")),
            !read("note/payload", "composite:map", done),
        ],
    )
    .expect("compiles");
    assert_eq!(rule.storable(), Ok(()));
    let stored = rule.try_encode().expect("a body");
    assert_eq!(body::format_of(&stored), Some(5));
    let decoded = DeductiveRule::decode(&stored).expect("reads back");
    assert!(decoded.stored_as(&rule.this()), "under its name");
    assert_eq!(decoded.try_encode(), Some(stored));
}

/// The composite constants a rule can hold, a map, a list, and a map
/// nested in a list in a map, each beside the pattern of its parts,
/// which holds no variable and so is that constant.
fn composite_constants() -> Vec<(Value, Pattern)> {
    let list: Value = Composite::list([text("a"), Value::UnsignedInt(1)])
        .expect("a list")
        .into();
    let nested: Value = Composite::list([map(&[("c", text("d"))]), text("e")])
        .expect("a list")
        .into();
    let parts = |entries: Vec<(&str, Pattern)>| {
        Pattern::map(
            entries
                .into_iter()
                .map(|(key, pattern)| (text(key), pattern)),
        )
        .expect("a pattern")
    };
    vec![
        (
            map(&[("a", text("b"))]),
            parts(vec![("a", Pattern::from(text("b")))]),
        ),
        (
            list,
            Pattern::list([
                Pattern::from(text("a")),
                Pattern::from(Value::UnsignedInt(1)),
            ]),
        ),
        (
            map(&[("inner", nested), ("n", Value::UnsignedInt(2))]),
            parts(vec![
                (
                    "inner",
                    Pattern::list([
                        parts(vec![("c", Pattern::from(text("d")))]),
                        Pattern::from(text("e")),
                    ]),
                ),
                ("n", Pattern::from(Value::UnsignedInt(2))),
            ]),
        ),
    ]
}

/// A rule that holds a composite constant, a map or a list with no
/// variable in it, reads back from its stored body. A pattern with no
/// variable in it is that constant, so this is also how such a pattern
/// is stored, in format 4 as no pattern is held.
#[dialog_common::test]
fn it_reads_back_a_rule_that_holds_a_composite_constant() {
    for (constant, pattern) in composite_constants() {
        assert_eq!(pattern.as_value(), Some(constant.clone()));
        for held in [Pattern::from(constant.clone()), pattern] {
            let rule = DeductiveRule::new(
                concept("heading", "note/heading", "text:"),
                vec![
                    read("note/title", "text:", var("heading")),
                    read("note/payload", "composite:map", held),
                ],
            )
            .expect("compiles");
            let stored = rule.try_encode().expect("a body");
            assert_eq!(body::format_of(&stored), Some(4), "no pattern is held");
            let decoded = DeductiveRule::decode(&stored).expect("the body reads back");
            assert!(decoded.stored_as(&rule.this()));
            assert_eq!(decoded.try_encode(), Some(stored));
        }
    }
}

/// An inductive rule that holds a composite constant reads back from its
/// stored body too.
#[dialog_common::test]
fn it_reads_back_an_inductive_rule_that_holds_a_composite_constant() {
    use crate::rule::InductiveRule;

    for (constant, _) in composite_constants() {
        let rule = InductiveRule::new(
            concept("heading", "note/heading", "text:"),
            vec![
                read("note/title", "text:", var("heading")),
                read(
                    "note/payload",
                    "composite:map",
                    Pattern::Term(Term::Constant(constant.clone())),
                ),
            ],
        )
        .expect("compiles");
        let stored = rule.try_encode().expect("a body");
        let decoded = InductiveRule::decode(&stored).expect("the body reads back");
        assert!(decoded.stored_as(&rule.this()), "{constant:?}");
        assert_eq!(decoded.try_encode(), Some(stored));
    }
}

/// A stored body that holds no composite constant is buffered as
/// `serde_json::Value` buffers it, part for part, so every body that read
/// before reads as it did: rules of terms in format 4, rules that hold a
/// pattern in format 5, and the bodies of earlier formats.
#[dialog_common::test]
fn it_buffers_a_body_without_composites_as_json_does() {
    let plain = DeductiveRule::new(
        concept("heading", "note/heading", "text:"),
        vec![read("note/title", "text:", var("heading"))],
    )
    .expect("compiles");
    let mut bodies = vec![
        plain.try_encode().expect("a body"),
        card_rule([0, 1, 2])
            .expect("compiles")
            .try_encode()
            .expect("a body"),
        symbol_rule().try_encode().expect("a body"),
    ];
    for format in [0, 2, 3] {
        bodies.push(body::encode_as(format, &plain.canonical_descriptor()).expect("a body"));
    }
    for stored in bodies {
        let json: serde_json::Value = serde_ipld_dagcbor::from_slice(&stored).expect("reads");
        let crate::term::Buffered(buffered) =
            serde_ipld_dagcbor::from_slice(&stored).expect("buffers");
        assert_eq!(buffered, json);
    }
}

/// Bytes in a stored body that are no map or list are refused, by an
/// error and not a panic.
#[dialog_common::test]
fn it_refuses_bytes_that_are_no_composite() {
    let stored = serde_ipld_dagcbor::to_vec(&ipld_core::ipld::Ipld::Bytes(vec![0xff, 0x00]))
        .expect("encodes");
    let refused = serde_ipld_dagcbor::from_slice::<crate::term::Buffered>(&stored)
        .err()
        .expect("refused");
    assert!(refused.to_string().contains("no map or list"), "{refused}");
}
