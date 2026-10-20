//! How a rule's head reads the numbers its body binds, under the band
//! numbers are stored in: what compiles, what is refused, and what the
//! refusal says.

#[cfg(target_arch = "wasm32")]
wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

use crate::InductiveRule;
use crate::artifact::Type as ValueType;
use crate::error::TypeError;
use crate::rule::InductiveRuleDescriptor;
use crate::type_system::Primitive;
use serde_json::{Value as Json, json};

/// A counter rule: `count` read from `counter/count` declared `read`,
/// stepped by `step`, written to a head field declared `write`.
fn counter(read: &str, write: &str, step: Json) -> Result<InductiveRule, TypeError> {
    let mut when = vec![
        json!({
            "assert": {
                "with": { "counter": { "the": "cmd.step/counter", "as": "entity:" } }
            },
            "where": { "counter": { "?": { "name": "this" } } }
        }),
        json!({
            "assert": {
                "with": { "count": { "the": "counter/count", "as": read } }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "count": { "?": { "name": "prev" } }
            }
        }),
    ];
    when.extend(step.as_array().expect("steps").iter().cloned());
    let descriptor: InductiveRuleDescriptor = serde_json::from_value(json!({
        "assert!": {
            "with": { "count": { "the": "counter/count", "as": write } }
        },
        "when": when
    }))
    .expect("descriptor parses");
    descriptor.compile()
}

fn decrement() -> Json {
    json!([{
        "assert": "math/difference",
        "where": {
            "of": { "?": { "name": "prev" } },
            "subtract": 1,
            "is": { "?": { "name": "count" } }
        }
    }])
}

fn increment() -> Json {
    json!([{
        "assert": "math/sum",
        "where": {
            "of": { "?": { "name": "prev" } },
            "with": 1,
            "is": { "?": { "name": "count" } }
        }
    }])
}

fn guarded(step: Json, relation: &str, bound: Json) -> Json {
    let mut steps = step.as_array().expect("steps").clone();
    steps.push(json!({
        "assert": relation,
        "where": { "of": { "?": { "name": "count" } }, "with": bound }
    }));
    Json::Array(steps)
}

fn inferred(rule: &InductiveRule, variable: &str) -> Primitive {
    rule.analysis()
        .types
        .get(variable)
        .unwrap_or_else(|| panic!("{variable} is inferred"))
        .clone()
        .required()
        .primitive_part()
}

fn head_field_error(result: Result<InductiveRule, TypeError>) -> TypeError {
    match result {
        Err(error @ TypeError::HeadField { .. }) => error,
        Err(other) => panic!("expected a head field error, got {other}"),
        Ok(_) => panic!("expected a head field error, the rule compiled"),
    }
}

/// A natural minus one may be negative, so a natural head does not
/// take it. The error says which field, what the variable is, why the
/// field does not read it, what to do, and which premise made it so.
#[dialog_common::test]
fn it_refuses_a_difference_into_a_natural_head() {
    let error = head_field_error(counter("natural:", "natural:", decrement()));
    let message = error.to_string();
    for fragment in [
        "head field \"count\" is UnsignedInt",
        "?count is SignedInt",
        "may be negative",
        "`?count >= 0`",
        "declare the field integer",
        "bound by math/difference",
    ] {
        assert!(message.contains(fragment), "{message:?} lacks {fragment:?}");
    }
}

/// A natural head reads an integer the body proved non-negative: the
/// comparison narrows the difference back to a natural, and the rule
/// compiles with `count` a natural.
#[dialog_common::test]
fn it_takes_a_difference_a_comparison_proves_non_negative() {
    for (relation, bound) in [(">=", json!(0)), (">", json!(-1)), (">=", json!(0.0))] {
        let rule = counter(
            "natural:",
            "natural:",
            guarded(decrement(), relation, bound.clone()),
        )
        .unwrap_or_else(|error| panic!("`?count {relation} {bound}` compiles: {error}"));
        assert_eq!(
            inferred(&rule, "count"),
            Primitive::singleton(ValueType::UnsignedInt),
            "`?count {relation} {bound}` makes the difference a natural"
        );
    }
}

/// A lower bound that admits a negative number proves nothing: the
/// difference stays an integer and the natural head still refuses it.
#[dialog_common::test]
fn it_refuses_a_difference_a_comparison_leaves_negative() {
    for (relation, bound) in [(">=", json!(-1)), (">", json!(-2)), ("<=", json!(10))] {
        let error = head_field_error(counter(
            "natural:",
            "natural:",
            guarded(decrement(), relation, bound.clone()),
        ));
        assert!(
            error.to_string().contains("may be negative"),
            "`?count {relation} {bound}` does not make the difference a natural: {error}"
        );
    }
}

/// An integer head takes the difference of naturals as it is: the
/// band reads a natural as an integer. A float head takes it too.
#[dialog_common::test]
fn it_takes_a_difference_into_a_wider_head() {
    let rule = counter("natural:", "integer:", decrement()).expect("an integer head compiles");
    assert_eq!(
        inferred(&rule, "count"),
        Primitive::singleton(ValueType::SignedInt)
    );
    counter("natural:", "float:", decrement()).expect("a float head compiles");
    let rule = counter("integer:", "integer:", decrement()).expect("integers compile");
    assert_eq!(
        inferred(&rule, "count"),
        Primitive::singleton(ValueType::SignedInt)
    );
    let rule = counter("float:", "float:", decrement()).expect("floats compile");
    assert_eq!(
        inferred(&rule, "count"),
        Primitive::singleton(ValueType::Float),
        "a difference of floats is a float"
    );
}

/// A sum of naturals is a natural, as before: the increment rule
/// compiles with `count` a natural.
#[dialog_common::test]
fn it_takes_a_sum_of_naturals_into_a_natural_head() {
    let rule = counter("natural:", "natural:", increment()).expect("the increment compiles");
    assert_eq!(
        inferred(&rule, "count"),
        Primitive::singleton(ValueType::UnsignedInt)
    );
}

/// An integer read from an integer field may be negative whatever is
/// done to it: a natural head refuses the sum of it and one, naming
/// the sum as what made `count` an integer, since the sum shares one
/// type with the integer it reads. A float head takes it, since a
/// float reads every integer.
#[dialog_common::test]
fn it_refuses_an_integer_field_into_a_natural_head() {
    let error = head_field_error(counter("integer:", "natural:", increment()));
    let message = error.to_string();
    assert!(message.contains("may be negative"), "{message}");
    assert!(
        message.contains("bound by math/sum"),
        "the premise that made ?count an integer is named: {message}"
    );
    counter("integer:", "float:", increment()).expect("a float head reads integers");
}

/// A float may have a fraction, so an integer head does not take it
/// and the error names the rounding formulas. Rounded through one of
/// them, the rule compiles with the field an integer.
#[dialog_common::test]
fn it_refuses_a_float_into_an_integer_head_until_rounded() {
    let error = head_field_error(counter("float:", "integer:", increment()));
    let message = error.to_string();
    for fragment in [
        "?count is Float",
        "may have a fraction",
        "math/floor, math/round or math/truncate",
        "declare the field float",
    ] {
        assert!(message.contains(fragment), "{message:?} lacks {fragment:?}");
    }
    for formula in ["math/floor", "math/round", "math/truncate"] {
        let rounded = json!([{
            "assert": formula,
            "where": {
                "of": { "?": { "name": "prev" } },
                "is": { "?": { "name": "count" } }
            }
        }]);
        let rule = counter("float:", "integer:", rounded)
            .unwrap_or_else(|error| panic!("{formula} into an integer head compiles: {error}"));
        assert_eq!(
            inferred(&rule, "count"),
            Primitive::singleton(ValueType::SignedInt),
            "{formula} yields an integer"
        );
    }
    let error = head_field_error(counter(
        "float:",
        "natural:",
        json!([{
            "assert": "math/floor",
            "where": {
                "of": { "?": { "name": "prev" } },
                "is": { "?": { "name": "count" } }
            }
        }]),
    ));
    assert!(
        error.to_string().contains("may be negative"),
        "a floor is an integer, not a natural: {error}"
    );
}

/// A number the body leaves open (read through a field with no
/// declared type, stepped by a polymorphic literal) is narrowed by the head:
/// a natural head makes it a natural, so the scan feeding it reads
/// naturals, and the rule compiles.
#[dialog_common::test]
fn it_narrows_an_open_number_from_the_head() {
    let descriptor: InductiveRuleDescriptor = serde_json::from_value(json!({
        "assert!": {
            "with": { "count": { "the": "counter/count", "as": "natural:" } }
        },
        "when": [
            {
                "assert": {
                    "with": { "counter": { "the": "cmd.step/counter", "as": "entity:" } }
                },
                "where": { "counter": { "?": { "name": "this" } } }
            },
            {
                "assert": "math/sum",
                "where": {
                    "of": { "?": { "name": "prev" } },
                    "with": 1,
                    "is": { "?": { "name": "count" } }
                }
            },
            {
                "assert": { "with": { "count": { "the": "counter/count" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "count": { "?": { "name": "prev" } }
                }
            }
        ]
    }))
    .expect("descriptor parses");
    let rule = descriptor
        .compile()
        .expect("the head narrows the open number");
    assert_eq!(
        inferred(&rule, "count"),
        Primitive::singleton(ValueType::UnsignedInt)
    );
    // The sum shares one type, so the operand (renamed in the rule's
    // canonical form) narrows with the result: every number the body
    // binds is a natural.
    let numbers: Vec<(String, Primitive)> = rule
        .analysis()
        .types
        .iter()
        .map(|(name, kind)| (name.clone(), kind.clone().required().primitive_part()))
        .filter(|(_, membership)| membership.intersect(Primitive::NUMERIC) == Some(*membership))
        .collect();
    assert_eq!(numbers.len(), 2, "the operand and the result: {numbers:?}");
    for (name, membership) in numbers {
        assert_eq!(
            membership,
            Primitive::singleton(ValueType::UnsignedInt),
            "?{name} is a natural"
        );
    }
}
