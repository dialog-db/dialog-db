//! A rule's constants survive storage and its identity tells them apart.
//!
//! A rule is stored, and hashed into its identity, as the dag-cbor of its
//! descriptor, and a constant term serializes as its untagged `Value`.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_query::artifact::Value;
use dialog_query::proposition::Proposition;
use dialog_query::rule::{DeductiveRule, DeductiveRuleDescriptor};
use dialog_query::term::Term;
use serde_json::json;

/// The title of a page whose `page/url`, read as `kind` or untyped when
/// `kind` is `None`, is `url`.
fn rule_matching(url: Value, kind: Option<&str>) -> DeductiveRule {
    let url_field = match kind {
        Some(kind) => json!({ "the": "page/url", "as": kind }),
        None => json!({ "the": "page/url" }),
    };
    let mut descriptor: DeductiveRuleDescriptor = serde_json::from_value(json!({
        "deduce": { "with": { "title": { "the": "page/home-title", "as": "text:" } } },
        "when": [{
            "assert": { "with": {
                "url": url_field,
                "title": { "the": "page/title", "as": "text:" }
            } },
            "where": {
                "this": { "?": { "name": "this" } },
                "url": "placeholder",
                "title": { "?": { "name": "title" } }
            }
        }]
    }))
    .expect("a rule descriptor");
    let Some(Proposition::Concept(query)) = descriptor.when.first_mut() else {
        panic!("a concept premise");
    };
    query.terms.insert("url".into(), Term::Constant(url));
    descriptor.compile().expect("the rule compiles")
}

/// The constant a stored rule's premise matches.
fn constant_of(rule: &DeductiveRule) -> Value {
    let descriptor = rule.descriptor();
    let Some(Proposition::Concept(query)) = descriptor.when.first() else {
        panic!("a concept premise");
    };
    match query.terms.get("url") {
        Some(Term::Constant(value)) => value.clone(),
        other => panic!("a constant, found {other:?}"),
    }
}

/// A rule matching the text `https://example.com` still matches that
/// text after it is stored and read back.
#[dialog_common::test]
fn it_keeps_a_text_constant_text_through_storage() {
    let text = Value::String("https://example.com".into());
    let stored = DeductiveRule::decode(&rule_matching(text.clone(), Some("text:")).encode())
        .expect("decodes");
    assert_eq!(constant_of(&stored), text);
}

/// Two rules that match different values over an untyped attribute,
/// the text `foo:` and the entity `foo:`, are two rules.
#[dialog_common::test]
fn it_tells_rules_apart_by_the_type_of_their_constants() {
    let text = rule_matching(Value::String("foo:".into()), None);
    let entity = rule_matching(Value::Entity("foo:".parse().expect("an entity")), None);
    assert_ne!(text.this(), entity.this());
}

/// `page/rank`, an integer, matched against `rank`, and `page/slug`, text,
/// against `slug`, as JSON writes them: bare.
fn bare_rule() -> serde_json::Value {
    json!({
        "deduce": { "with": { "title": { "the": "page/home-title", "as": "text:" } } },
        "when": [{
            "assert": { "with": {
                "rank": { "the": "page/rank", "as": "integer:" },
                "slug": { "the": "page/slug", "as": "text:" },
                "title": { "the": "page/title", "as": "text:" }
            } },
            "where": {
                "this": { "?": { "name": "this" } },
                "rank": 1,
                "slug": "home:",
                "title": { "?": { "name": "title" } }
            }
        }]
    })
}

fn constant(rule: &DeductiveRule, name: &str) -> Value {
    let descriptor = rule.descriptor();
    let Some(Proposition::Concept(query)) = descriptor.when.first() else {
        panic!("a concept premise");
    };
    match query.terms.get(name) {
        Some(Term::Constant(value)) => value.clone(),
        other => panic!("a constant, found {other:?}"),
    }
}

/// A bare constant takes the type its field declares: `1` under an
/// integer is a signed integer, and `home:` under text is text, though
/// it parses as a URI.
#[dialog_common::test]
fn it_conforms_bare_constants_to_the_types_their_fields_declare() {
    let descriptor: DeductiveRuleDescriptor =
        serde_json::from_value(bare_rule()).expect("a rule descriptor");
    let rule = descriptor.compile().expect("the rule compiles");
    assert_eq!(constant(&rule, "rank"), Value::SignedInt(1));
    assert_eq!(constant(&rule, "slug"), Value::String("home:".into()));
}

/// A body 0.2.0 stored, in format 1 with its constants bare, decodes with
/// each constant under the type its field declares, so the rule a branch
/// upgrade re-installs from it matches what it was written to match.
#[dialog_common::test]
fn it_reads_a_format_1_body_with_bare_constants() {
    let body =
        serde_ipld_dagcbor::to_vec(&json!({ "format": 1, "rule": bare_rule() })).expect("encodes");
    let rule = DeductiveRule::decode(&body).expect("decodes");
    assert_eq!(constant(&rule, "rank"), Value::SignedInt(1));
    assert_eq!(constant(&rule, "slug"), Value::String("home:".into()));
}
