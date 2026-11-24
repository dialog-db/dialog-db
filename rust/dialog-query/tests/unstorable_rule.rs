//! A rule that cannot be stored cannot be asserted.
//!
//! A concept's implicit rule, or any rule built from a raw attribute
//! scan, has no stored form and so no identity. Only a
//! [`StorableRule`] is a statement, and converting such a rule into one
//! is refused with [`TypeError::RuleWithoutIdentity`], where asserting
//! the rule itself panicked on the identity it does not have.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_query::attribute::query::AttributeQuery;
use dialog_query::error::TypeError;
use dialog_query::{
    Changes, ConceptDescriptor, DeductiveRule, InductiveRule, Premise, Rule, StorableRule, Term,
    the,
};
use serde_json::json;

/// A person with a name.
fn person() -> ConceptDescriptor {
    serde_json::from_value(json!({
        "with": { "name": { "the": "person/name", "as": "text:" } }
    }))
    .expect("a concept")
}

/// `person/name` of `?this` is `?name`, read by a raw scan.
fn scan() -> Vec<Premise> {
    vec![
        AttributeQuery::new(
            Term::from(the!("person/name")),
            Term::var("this"),
            Term::var("name"),
            Term::blank(),
            None,
        )
        .into(),
    ]
}

/// Converting `rule` into a statement is refused with
/// `RuleWithoutIdentity` for the person concept, and nothing is
/// written into the changes the statement would have gone to.
fn assert_refused(rule: Rule) {
    let mut changes = Changes::new();
    let storable = StorableRule::try_from(&rule);
    assert!(
        matches!(
            &storable,
            Err(TypeError::RuleWithoutIdentity { concept })
                if *concept == person().this().to_string()
        ),
        "{storable:?}"
    );
    if let Ok(storable) = storable {
        changes.assert(storable);
    }
    assert!(changes.is_empty(), "nothing was written");
}

/// A concept's implicit rule, which reads each field by a raw scan.
#[dialog_common::test]
fn it_refuses_to_assert_the_implicit_rule_of_a_concept() {
    assert_refused(DeductiveRule::from(&person()).into());
}

/// A deductive rule built through `new` from a raw attribute scan.
#[dialog_common::test]
fn it_refuses_to_assert_a_rule_read_by_a_raw_scan() {
    let rule = DeductiveRule::new(person(), scan()).expect("the rule compiles");
    assert_refused(rule.into());
}

/// An inductive rule built through `new` from a raw attribute scan.
#[dialog_common::test]
fn it_refuses_to_assert_an_inductive_rule_read_by_a_raw_scan() {
    let rule = InductiveRule::new(person(), scan()).expect("the rule compiles");
    assert_refused(rule.into());
}
