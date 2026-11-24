//! A rule that cannot be stored cannot be asserted.
//!
//! A concept's implicit rule, or any rule built from a raw attribute
//! scan, has no stored form and so no identity. Asserting one into
//! changes panicked looking up the identity it does not have.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use dialog_query::attribute::query::AttributeQuery;
use dialog_query::{Changes, ConceptDescriptor, DeductiveRule, Term, the};
use serde_json::json;

/// A person with a name.
fn person() -> ConceptDescriptor {
    serde_json::from_value(json!({
        "with": { "name": { "the": "person/name", "as": "text:" } }
    }))
    .expect("a concept")
}

/// A concept's implicit rule, which reads each field by a raw scan,
/// is asserted into changes, and nothing is written.
#[dialog_common::test]
fn it_refuses_to_assert_the_implicit_rule_of_a_concept() {
    let rule = DeductiveRule::from(&person());
    let mut changes = Changes::new();
    changes.assert(&rule);
    assert!(changes.is_empty(), "nothing was written");
}

/// A rule built through `new` from a raw attribute scan is asserted
/// into changes, and nothing is written.
#[dialog_common::test]
fn it_refuses_to_assert_a_rule_read_by_a_raw_scan() {
    let rule = DeductiveRule::new(
        person(),
        vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::var("this"),
                Term::var("name"),
                Term::blank(),
                None,
            )
            .into(),
        ],
    )
    .expect("the rule compiles");
    let mut changes = Changes::new();
    changes.assert(&rule);
    assert!(changes.is_empty(), "nothing was written");
}
