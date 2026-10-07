//! Invariants the canonical rule identity claims, pinned as tests. A
//! failing test here is a place where a rule's identity, or what is
//! keyed by it, is not what the design says.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::Environment;
use crate::concept::query::PlanCache;
use crate::concept::query::adornment::Adornment;
use crate::rule::deductive::descriptor::DeductiveRuleDescriptor;
use crate::rule::deductive::{DeductiveRule, legacy_identity};
use serde_json::{Value, json};

fn compile(json: Value) -> DeductiveRule {
    let descriptor: DeductiveRuleDescriptor =
        serde_json::from_value(json).expect("descriptor parses");
    descriptor.compile().expect("rule compiles")
}

/// A premise over `social/knows`, its field called `field`.
fn knows_as(field: &str, of: &str, is: &str) -> Value {
    json!({
        "assert": { "with": { field: { "the": "social/knows", "as": "Entity" } } },
        "where": {
            "this": { "?": { "name": of } },
            field: { "?": { "name": is } }
        }
    })
}

fn friend_rule(when: Vec<Value>) -> DeductiveRule {
    compile(json!({
        "deduce": { "with": { "friend": { "the": "social/friend", "as": "Entity" } } },
        "when": when,
    }))
}

/// "A body stored before identities were canonical sits under the
/// hash of its bytes; hydration accepts that identity too, so such a
/// rule stays live." The bytes spell every field as the older release
/// did, with its `cardinality` and an empty `description`; the
/// repository's migration tests replay bytes captured from `main`.
/// The check hashes this release's re-encoding of the decoded rule,
/// which spells neither, so the stored identity is never recognised
/// and the rule goes inert on the commit path.
#[dialog_common::test]
fn a_rule_stored_by_the_older_release_is_recognised_under_its_stored_identity() {
    let legacy = json!({
        "deduce": {
            "with": {
                "friend": {
                    "description": "",
                    "the": "social/friend",
                    "cardinality": "one",
                    "as": "Entity"
                }
            }
        },
        "when": [{
            "assert": { "with": {
                "knows": {
                    "description": "",
                    "the": "social/knows",
                    "cardinality": "one",
                    "as": "Entity"
                }
            }},
            "where": {
                "this": { "?": { "name": "this" } },
                "knows": { "?": { "name": "friend" } }
            }
        }]
    });
    let bytes = serde_ipld_dagcbor::to_vec(&legacy).expect("the legacy form encodes");
    let stored_under = legacy_identity(Some(bytes.clone())).expect("an identity");
    let rule = DeductiveRule::decode(&bytes).expect("a legacy rule decodes");
    assert!(
        rule.stored_as(&stored_under),
        "the rule is live under the hash of the bytes it was stored as"
    );
}

/// "Two authors writing one rule under different names, for the head
/// and the body alike, ... install one rule." A premise's field name
/// only ties a body variable to the attribute, so calling the field
/// `knows` or `acquaintance` is one rule.
#[dialog_common::test]
fn a_premise_field_name_does_not_distinguish_a_rule() {
    let knows = friend_rule(vec![
        knows_as("knows", "this", "x"),
        knows_as("knows", "x", "friend"),
    ]);
    let acquaintance = friend_rule(vec![
        knows_as("acquaintance", "this", "x"),
        knows_as("acquaintance", "x", "friend"),
    ]);
    assert_eq!(knows.this(), acquaintance.this());
}

/// A rule's plan is keyed by its identity and the spelling of its head.
/// Two rules of one identity whose heads spell the same operand names
/// but pair them with the other attribute are two working spellings:
/// `H{a: attr1, b: attr2} :- P{p: ?a}, Q{q: ?b}` and `H{b: attr1, a:
/// attr2} :- P{p: ?b}, Q{q: ?a}`. Their plans bind `a` from different
/// premises, so the cache must plan each; it hands the second the
/// first's plan.
#[dialog_common::test]
fn a_plan_is_cached_by_the_working_spelling_it_was_planned_for() {
    let head = |a: &str, b: &str| {
        json!({ "with": {
            a: { "the": "x/attr1", "as": "Entity" },
            b: { "the": "x/attr2", "as": "Entity" }
        }})
    };
    let premise = |the: &str, field: &str, var: &str| {
        json!({
            "assert": { "with": { field: { "the": the, "as": "Entity" } } },
            "where": { "this": { "?": { "name": "this" } }, field: { "?": { "name": var } } }
        })
    };
    let one = compile(json!({
        "deduce": head("a", "b"),
        "when": [premise("x/p", "p", "a"), premise("x/q", "q", "b")],
    }));
    let other = compile(json!({
        "deduce": head("b", "a"),
        "when": [premise("x/p", "p", "b"), premise("x/q", "q", "a")],
    }));
    assert_eq!(one.this(), other.this(), "one rule, two spellings");
    assert_eq!(one.spelling(), other.spelling(), "the same operand names");
    let scope = Environment::new();
    let adornment = Adornment::binding(&one.conclusion().sorted_operands(), &scope);
    let cache = PlanCache::default();
    let planned_one = cache.get_or_plan(&one, adornment, || one.plan(&scope));
    assert_eq!(planned_one, one.plan(&scope));
    let planned_other = cache.get_or_plan(&other, adornment, || other.plan(&scope));
    assert_eq!(
        planned_other,
        other.plan(&scope),
        "the other spelling gets its own plan, not the first's"
    );
}
