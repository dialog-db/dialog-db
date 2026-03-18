//! End-to-end tests verifying that the dialog-query engine correctly
//! serializes and deserializes the formal notation defined in the
//! Dialog Notation System specification.
//!
//! Each test corresponds to an example or requirement from the spec.

use dialog_query::concept::descriptor::ConceptDescriptor;
use dialog_query::constraint::Constraint;
use dialog_query::formula::query::FormulaQuery;
use dialog_query::proposition::Proposition;
use dialog_query::rule::DeductiveRuleDescriptor;
use dialog_query::term::Term;
use dialog_query::types::Any;
use serde_json::json;

// Attribute

#[dialog_common::test]
fn it_parses_attribute_formal_notation() {
    let json = json!({
        "description": "Name of the person",
        "the": "io.gozala.person/name",
        "cardinality": "one",
        "as": "Text"
    });

    let attr: dialog_query::AttributeDescriptor =
        serde_json::from_value(json.clone()).expect("Should parse attribute");

    assert_eq!(attr.domain(), "io.gozala.person");
    assert_eq!(attr.name(), "name");
    assert_eq!(attr.description(), "Name of the person");
    assert_eq!(
        attr.content_type(),
        Some(dialog_query::artifact::Type::String)
    );
    assert_eq!(attr.cardinality(), dialog_query::Cardinality::One);

    let reserialized = serde_json::to_value(&attr).unwrap();
    assert_eq!(reserialized["the"], "io.gozala.person/name");
    assert_eq!(reserialized["as"], "Text");
    assert_eq!(reserialized["cardinality"], "one");
}

#[dialog_common::test]
fn it_parses_attribute_value_types() {
    let types = [
        ("Bytes", dialog_query::artifact::Type::Bytes),
        ("Entity", dialog_query::artifact::Type::Entity),
        ("Boolean", dialog_query::artifact::Type::Boolean),
        ("Text", dialog_query::artifact::Type::String),
        ("UnsignedInteger", dialog_query::artifact::Type::UnsignedInt),
        ("SignedInteger", dialog_query::artifact::Type::SignedInt),
        ("Float", dialog_query::artifact::Type::Float),
        ("Symbol", dialog_query::artifact::Type::Symbol),
    ];

    for (type_str, expected_type) in types {
        let json = json!({
            "the": "test/field",
            "as": type_str
        });
        let attr: dialog_query::AttributeDescriptor =
            serde_json::from_value(json).unwrap_or_else(|e| panic!("Failed for {type_str}: {e}"));
        assert_eq!(
            attr.content_type(),
            Some(expected_type),
            "Type mismatch for {type_str}"
        );
    }
}

#[dialog_common::test]
fn it_parses_minimal_attribute() {
    let json = json!({ "the": "task/status" });
    let attr: dialog_query::AttributeDescriptor =
        serde_json::from_value(json).expect("Minimal attribute should parse");
    assert_eq!(attr.domain(), "task");
    assert_eq!(attr.name(), "status");
    assert_eq!(attr.cardinality(), dialog_query::Cardinality::One);
    assert_eq!(attr.content_type(), None);
}

#[dialog_common::test]
fn it_parses_attribute_cardinality_many() {
    let json = json!({
        "the": "post/tags",
        "cardinality": "many",
        "as": "Text"
    });
    let attr: dialog_query::AttributeDescriptor = serde_json::from_value(json).unwrap();
    assert_eq!(attr.cardinality(), dialog_query::Cardinality::Many);
}

// Concept

#[dialog_common::test]
fn it_parses_concept_formal_notation() {
    let json = json!({
        "description": "Description of the person",
        "with": {
            "name": {
                "description": "Name of the person",
                "the": "io.gozala.person/name",
                "cardinality": "one",
                "as": "Text"
            },
            "address": {
                "description": "Address of the person",
                "the": "io.gozala.person/address",
                "cardinality": "one",
                "as": "Text"
            }
        }
    });

    let concept: ConceptDescriptor = serde_json::from_value(json).expect("Should parse concept");

    assert_eq!(concept.description(), Some("Description of the person"));
    assert_eq!(concept.with().iter().count(), 2);

    let name_attr = concept.with().iter().find(|(k, _)| *k == "name").unwrap().1;
    assert_eq!(name_attr.domain(), "io.gozala.person");
    assert_eq!(name_attr.name(), "name");

    let addr_attr = concept
        .with()
        .iter()
        .find(|(k, _)| *k == "address")
        .unwrap()
        .1;
    assert_eq!(addr_attr.domain(), "io.gozala.person");
    assert_eq!(addr_attr.name(), "address");
}

#[dialog_common::test]
fn it_computes_concept_structural_identity() {
    let concept_a: ConceptDescriptor = serde_json::from_value(json!({
        "with": {
            "field_a": { "the": "person/name", "as": "Text" },
            "field_b": { "the": "person/age", "as": "UnsignedInteger" }
        }
    }))
    .unwrap();

    let concept_b: ConceptDescriptor = serde_json::from_value(json!({
        "description": "Different description",
        "with": {
            "different_name_1": { "the": "person/name", "as": "Text" },
            "different_name_2": { "the": "person/age", "as": "UnsignedInteger" }
        }
    }))
    .unwrap();

    assert_eq!(
        concept_a.this(),
        concept_b.this(),
        "Same attributes with different field names/descriptions should be the same concept"
    );
}

#[dialog_common::test]
fn it_accepts_concept_maybe_field() {
    let json = json!({
        "description": "A cooking step",
        "with": {
            "instruction": {
                "description": "What to do in this step",
                "the": "diy.cook.recipe-step/instruction",
                "as": "Text"
            }
        },
        "maybe": {
            "after": {
                "description": "Step that must be completed before this one",
                "the": "diy.cook.recipe-step/after",
                "as": "Entity"
            },
            "duration": {
                "description": "Time in minutes this step takes",
                "the": "diy.cook.recipe-step/duration",
                "as": "UnsignedInteger"
            }
        }
    });

    let concept: ConceptDescriptor =
        serde_json::from_value(json).expect("Should accept maybe field");
    assert_eq!(concept.with().iter().count(), 1);
}

// Variables

#[dialog_common::test]
fn it_parses_named_variable() {
    let json = json!({ "?": { "name": "person" } });
    let term: Term<Any> = serde_json::from_value(json).unwrap();
    assert_eq!(term.name(), Some("person"));
    assert!(term.is_variable());
}

#[dialog_common::test]
fn it_parses_blank_variable() {
    let json = json!({ "?": {} });
    let term: Term<Any> = serde_json::from_value(json).unwrap();
    assert!(term.is_blank());
}

#[dialog_common::test]
fn it_parses_string_constant() {
    let json = json!("Alice");
    let term: Term<Any> = serde_json::from_value(json).unwrap();
    assert!(term.is_constant());
}

#[dialog_common::test]
fn it_parses_integer_constant() {
    let json = json!(42);
    let term: Term<Any> = serde_json::from_value(json).unwrap();
    assert!(term.is_constant());
}

#[dialog_common::test]
fn it_parses_boolean_constant() {
    let json = json!(true);
    let term: Term<Any> = serde_json::from_value(json).unwrap();
    assert!(term.is_constant());
}

// Equality Constraint

#[dialog_common::test]
fn it_parses_equality_constraint() {
    let json = json!({
        "assert": "==",
        "where": {
            "this": { "?": { "name": "name" } },
            "is": "Alice"
        }
    });

    let prop: Proposition = serde_json::from_value(json.clone()).unwrap();
    match &prop {
        Proposition::Constraint(Constraint::Equality(eq)) => {
            assert_eq!(eq.this, Term::<Any>::var("name"));
        }
        other => panic!("Expected Constraint(Equality), got {:?}", other),
    }

    let reserialized = serde_json::to_value(&prop).unwrap();
    assert_eq!(reserialized["assert"], "==");
    assert!(reserialized["where"]["this"].is_object());
}

#[dialog_common::test]
fn it_parses_equality_constraint_with_two_variables() {
    let json = json!({
        "assert": "==",
        "where": {
            "this": { "?": { "name": "x" } },
            "is": { "?": { "name": "y" } }
        }
    });

    let prop: Proposition = serde_json::from_value(json).unwrap();
    assert!(matches!(
        prop,
        Proposition::Constraint(Constraint::Equality(_))
    ));
}

// Math Formulas

#[dialog_common::test]
fn it_parses_math_sum() {
    let json = json!({
        "assert": "math/sum",
        "where": {
            "of": { "?": { "name": "a" } },
            "with": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let prop: Proposition = serde_json::from_value(json).unwrap();
    match &prop {
        Proposition::Formula(fq) => {
            assert_eq!(fq.name(), "math/sum");
            let params = fq.parameters();
            assert_eq!(params.get("of").and_then(|t| t.name()), Some("a"));
            assert_eq!(params.get("with").and_then(|t| t.name()), Some("b"));
            assert_eq!(params.get("is").and_then(|t| t.name()), Some("result"));
        }
        other => panic!("Expected Formula, got {:?}", other),
    }
}

#[dialog_common::test]
fn it_parses_math_sum_with_constants() {
    let json = json!({
        "assert": "math/sum",
        "where": {
            "of": { "?": { "name": "int" } },
            "with": 10,
            "is": { "?": { "name": "total" } }
        }
    });

    let prop: Proposition = serde_json::from_value(json).unwrap();
    assert!(matches!(prop, Proposition::Formula(_)));
}

#[dialog_common::test]
fn it_parses_math_difference() {
    let json = json!({
        "assert": "math/difference",
        "where": {
            "of": { "?": { "name": "a" } },
            "subtract": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "math/difference");
}

#[dialog_common::test]
fn it_parses_math_product() {
    let json = json!({
        "assert": "math/product",
        "where": {
            "of": { "?": { "name": "a" } },
            "times": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "math/product");
}

#[dialog_common::test]
fn it_parses_math_quotient() {
    let json = json!({
        "assert": "math/quotient",
        "where": {
            "of": { "?": { "name": "a" } },
            "by": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "math/quotient");
}

#[dialog_common::test]
fn it_parses_math_modulo() {
    let json = json!({
        "assert": "math/modulo",
        "where": {
            "of": { "?": { "name": "a" } },
            "by": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "math/modulo");
}

// Text Formulas

#[dialog_common::test]
fn it_parses_text_concatenate() {
    let json = json!({
        "assert": "text/concatenate",
        "where": {
            "first": { "?": { "name": "a" } },
            "second": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "text/concatenate");
}

#[dialog_common::test]
fn it_parses_text_length() {
    let json = json!({
        "assert": "text/length",
        "where": {
            "of": { "?": { "name": "text" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "text/length");
}

#[dialog_common::test]
fn it_parses_text_upper_case() {
    let json = json!({
        "assert": "text/upper-case",
        "where": {
            "of": { "?": { "name": "text" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "text/upper-case");
}

#[dialog_common::test]
fn it_parses_text_lower_case() {
    let json = json!({
        "assert": "text/lower-case",
        "where": {
            "of": { "?": { "name": "text" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "text/lower-case");
}

#[dialog_common::test]
fn it_parses_text_like() {
    let json = json!({
        "assert": "text/like",
        "where": {
            "text": { "?": { "name": "input" } },
            "pattern": "*@*.*",
            "is": { "?": { "name": "matched" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "text/like");
}

// Logic Formulas

#[dialog_common::test]
fn it_parses_boolean_and() {
    let json = json!({
        "assert": "boolean/and",
        "where": {
            "left": { "?": { "name": "a" } },
            "right": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "boolean/and");
}

#[dialog_common::test]
fn it_parses_boolean_or() {
    let json = json!({
        "assert": "boolean/or",
        "where": {
            "left": { "?": { "name": "a" } },
            "right": { "?": { "name": "b" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "boolean/or");
}

#[dialog_common::test]
fn it_parses_boolean_not() {
    let json = json!({
        "assert": "boolean/not",
        "where": {
            "value": { "?": { "name": "a" } },
            "is": { "?": { "name": "result" } }
        }
    });

    let fq: FormulaQuery = serde_json::from_value(json).unwrap();
    assert_eq!(fq.name(), "boolean/not");
}

// Concept Premise (the assert+where pattern from rules)

#[dialog_common::test]
fn it_parses_concept_premise() {
    let json = json!({
        "assert": {
            "with": {
                "name": { "the": "diy.cook/ingredient-name", "as": "Text" }
            }
        },
        "where": {
            "this": { "?": { "name": "this" } },
            "name": { "?": { "name": "name" } }
        }
    });

    let prop: Proposition = serde_json::from_value(json).unwrap();
    match &prop {
        Proposition::Concept(cq) => {
            assert_eq!(cq.terms.get("this"), Some(&Term::<Any>::var("this")));
            assert_eq!(cq.terms.get("name"), Some(&Term::<Any>::var("name")));

            let attr = cq
                .predicate
                .with()
                .iter()
                .find(|(k, _)| *k == "name")
                .unwrap()
                .1;
            assert_eq!(attr.domain(), "diy.cook");
            assert_eq!(attr.name(), "ingredient-name");
        }
        other => panic!("Expected Concept, got {:?}", other),
    }
}

#[dialog_common::test]
fn it_parses_conjunction_premise_array() {
    let premises = json!([
        {
            "assert": {
                "with": {
                    "name": { "the": "diy.cook/ingredient-name", "as": "Text" }
                }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "name": { "?": { "name": "name" } }
            }
        },
        {
            "assert": {
                "with": {
                    "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
                }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "quantity": { "?": { "name": "quantity" } }
            }
        },
        {
            "assert": {
                "with": {
                    "unit": { "the": "diy.cook/unit", "as": "Text" }
                }
            },
            "where": {
                "this": { "?": { "name": "this" } },
                "unit": { "?": { "name": "unit" } }
            }
        }
    ]);

    let props: Vec<Proposition> = serde_json::from_value(premises).unwrap();
    assert_eq!(props.len(), 3);
    assert!(matches!(&props[0], Proposition::Concept(_)));
    assert!(matches!(&props[1], Proposition::Concept(_)));
    assert!(matches!(&props[2], Proposition::Concept(_)));
}

// Full rule structure (deduce + when)

#[dialog_common::test]
fn it_parses_rule_structure_ingredient_example() {
    #[derive(serde::Deserialize)]
    struct Rule {
        deduce: ConceptDescriptor,
        when: Vec<Proposition>,
    }

    let json = json!({
        "deduce": {
            "description": "An ingredient",
            "with": {
                "name": {
                    "description": "Ingredient name",
                    "the": "diy.cook/ingredient-name",
                    "as": "Text"
                },
                "quantity": {
                    "description": "Amount needed",
                    "the": "diy.cook/quantity",
                    "as": "UnsignedInteger"
                },
                "unit": {
                    "description": "Unit of measurement",
                    "the": "diy.cook/unit",
                    "as": "Text"
                }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "name": { "the": "diy.cook/ingredient-name", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            },
            {
                "assert": {
                    "with": {
                        "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "quantity": { "?": { "name": "quantity" } }
                }
            },
            {
                "assert": {
                    "with": {
                        "unit": { "the": "diy.cook/unit", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "unit": { "?": { "name": "unit" } }
                }
            }
        ]
    });

    let rule: Rule = serde_json::from_value(json).unwrap();
    assert_eq!(rule.deduce.description(), Some("An ingredient"));
    assert_eq!(rule.deduce.with().iter().count(), 3);
    assert_eq!(rule.when.len(), 3);
}

#[dialog_common::test]
fn it_parses_rule_with_equality_constraint() {
    #[derive(serde::Deserialize)]
    struct Rule {
        when: Vec<Proposition>,
    }

    let json = json!({
        "when": [
            {
                "assert": {
                    "with": {
                        "name": { "the": "org.employee/name", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "person" } },
                    "name": { "?": { "name": "name" } }
                }
            },
            {
                "assert": "==",
                "where": {
                    "this": { "?": { "name": "name" } },
                    "is": "Alice"
                }
            }
        ]
    });

    let rule: Rule = serde_json::from_value(json).unwrap();
    assert_eq!(rule.when.len(), 2);
    assert!(matches!(&rule.when[0], Proposition::Concept(_)));
    assert!(matches!(
        &rule.when[1],
        Proposition::Constraint(Constraint::Equality(_))
    ));
}

#[dialog_common::test]
fn it_parses_rule_with_formula() {
    #[derive(serde::Deserialize)]
    struct Rule {
        when: Vec<Proposition>,
    }

    let json = json!({
        "when": [
            {
                "assert": {
                    "with": {
                        "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "entity" } },
                    "quantity": { "?": { "name": "int" } }
                }
            },
            {
                "assert": "math/sum",
                "where": {
                    "of": { "?": { "name": "int" } },
                    "with": 10,
                    "is": { "?": { "name": "total" } }
                }
            }
        ]
    });

    let rule: Rule = serde_json::from_value(json).unwrap();
    assert_eq!(rule.when.len(), 2);
    assert!(matches!(&rule.when[0], Proposition::Concept(_)));
    assert!(matches!(&rule.when[1], Proposition::Formula(_)));
}

#[dialog_common::test]
fn it_parses_rule_with_unless() {
    #[derive(serde::Deserialize)]
    struct Rule {
        when: Vec<Proposition>,
        unless: Vec<Proposition>,
    }

    let json = json!({
        "when": [
            {
                "assert": {
                    "with": {
                        "attendee": { "the": "diy.planner/attendee", "as": "Entity" },
                        "recipe": { "the": "diy.planner/recipe", "as": "Entity" },
                        "occasion": { "the": "diy.planner/occasion", "as": "Entity" }
                    }
                },
                "where": {
                    "attendee": { "?": { "name": "person" } },
                    "recipe": { "?": { "name": "recipe" } },
                    "occasion": { "?": { "name": "occasion" } }
                }
            }
        ],
        "unless": [
            {
                "assert": {
                    "with": {
                        "person": { "the": "diy.planner/person", "as": "Entity" },
                        "recipe": { "the": "diy.planner/recipe", "as": "Entity" }
                    }
                },
                "where": {
                    "person": { "?": { "name": "person" } },
                    "recipe": { "?": { "name": "recipe" } }
                }
            }
        ]
    });

    let rule: Rule = serde_json::from_value(json).unwrap();
    assert_eq!(rule.when.len(), 1);
    assert_eq!(rule.unless.len(), 1);
}

// Proposition discrimination

#[dialog_common::test]
fn it_discriminates_concept_vs_formula_vs_constraint() {
    let concept_json = json!({
        "assert": {
            "with": {
                "name": { "the": "person/name", "as": "Text" }
            }
        },
        "where": {
            "name": { "?": { "name": "n" } }
        }
    });

    let formula_json = json!({
        "assert": "math/sum",
        "where": {
            "of": { "?": { "name": "x" } },
            "with": { "?": { "name": "y" } },
            "is": { "?": { "name": "r" } }
        }
    });

    let constraint_json = json!({
        "assert": "==",
        "where": {
            "this": { "?": { "name": "x" } },
            "is": { "?": { "name": "y" } }
        }
    });

    assert!(matches!(
        serde_json::from_value::<Proposition>(concept_json).unwrap(),
        Proposition::Concept(_)
    ));
    assert!(matches!(
        serde_json::from_value::<Proposition>(formula_json).unwrap(),
        Proposition::Formula(_)
    ));
    assert!(matches!(
        serde_json::from_value::<Proposition>(constraint_json).unwrap(),
        Proposition::Constraint(_)
    ));
}

// Selector format validation

#[dialog_common::test]
fn it_accepts_valid_domain_formats() {
    let valid_domains = ["person", "diy.cook", "io.gozala.person", "org.example.hr"];

    for domain in valid_domains {
        let selector = format!("{domain}/name");
        let json = json!({ "the": selector });
        let result = serde_json::from_value::<dialog_query::AttributeDescriptor>(json);
        assert!(
            result.is_ok(),
            "Domain '{domain}' should be valid, got: {:?}",
            result.err()
        );
    }
}

// Round-trip: concept → JSON → concept

#[dialog_common::test]
fn it_round_trips_concept() {
    let original_json = json!({
        "description": "A recipe ingredient with quantity and unit",
        "with": {
            "quantity": {
                "the": "diy.cook/quantity",
                "description": "How much of this ingredient",
                "cardinality": "one",
                "as": "UnsignedInteger"
            },
            "name": {
                "the": "diy.cook/ingredient-name",
                "description": "Name of the ingredient",
                "as": "Text"
            }
        }
    });

    let concept: ConceptDescriptor = serde_json::from_value(original_json).unwrap();
    let reserialized = serde_json::to_value(&concept).unwrap();

    assert_eq!(
        reserialized["description"],
        "A recipe ingredient with quantity and unit"
    );
    assert_eq!(reserialized["with"]["quantity"]["the"], "diy.cook/quantity");
    assert_eq!(
        reserialized["with"]["name"]["the"],
        "diy.cook/ingredient-name"
    );
}

// Round-trip: formula → JSON → formula

#[dialog_common::test]
fn it_round_trips_all_formula_selectors() {
    let cases: Vec<(&str, serde_json::Value)> = vec![
        (
            "math/sum",
            json!({"of": {"?":{"name":"a"}}, "with": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "math/difference",
            json!({"of": {"?":{"name":"a"}}, "subtract": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "math/product",
            json!({"of": {"?":{"name":"a"}}, "times": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "math/quotient",
            json!({"of": {"?":{"name":"a"}}, "by": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "math/modulo",
            json!({"of": {"?":{"name":"a"}}, "by": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "text/concatenate",
            json!({"first": {"?":{"name":"a"}}, "second": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "text/length",
            json!({"of": {"?":{"name":"a"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "text/upper-case",
            json!({"of": {"?":{"name":"a"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "text/lower-case",
            json!({"of": {"?":{"name":"a"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "text/like",
            json!({"text": {"?":{"name":"a"}}, "pattern": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "boolean/and",
            json!({"left": {"?":{"name":"a"}}, "right": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "boolean/or",
            json!({"left": {"?":{"name":"a"}}, "right": {"?":{"name":"b"}}, "is": {"?":{"name":"r"}}}),
        ),
        (
            "boolean/not",
            json!({"value": {"?":{"name":"a"}}, "is": {"?":{"name":"r"}}}),
        ),
    ];

    for (selector, where_clause) in cases {
        let json = json!({
            "assert": selector,
            "where": where_clause
        });

        let fq: FormulaQuery =
            serde_json::from_value(json).unwrap_or_else(|e| panic!("Failed for {selector}: {e}"));
        assert_eq!(fq.name(), selector, "Selector should survive round-trip");

        let reserialized = serde_json::to_value(&fq).unwrap();
        assert_eq!(
            reserialized["assert"], selector,
            "Serialized selector should match"
        );
    }
}

// Rule Definition round-trips

#[dialog_common::test]
fn it_round_trips_ingredient_rule() {
    let json = json!({
        "deduce": {
            "description": "An ingredient",
            "with": {
                "name": {
                    "description": "Ingredient name",
                    "the": "diy.cook/ingredient-name",
                    "as": "Text"
                },
                "quantity": {
                    "description": "Amount needed",
                    "the": "diy.cook/quantity",
                    "as": "UnsignedInteger"
                },
                "unit": {
                    "description": "Unit of measurement",
                    "the": "diy.cook/unit",
                    "as": "Text"
                }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "name": { "the": "diy.cook/ingredient-name", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            },
            {
                "assert": {
                    "with": {
                        "quantity": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "quantity": { "?": { "name": "quantity" } }
                }
            },
            {
                "assert": {
                    "with": {
                        "unit": { "the": "diy.cook/unit", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "unit": { "?": { "name": "unit" } }
                }
            }
        ]
    });

    let def: DeductiveRuleDescriptor = serde_json::from_value(json.clone()).unwrap();
    let reserialized = serde_json::to_value(&def).unwrap();
    let _reparsed: DeductiveRuleDescriptor =
        serde_json::from_value(reserialized.clone()).expect("Round-tripped JSON should parse back");

    assert_eq!(reserialized["deduce"]["description"], "An ingredient");
    assert_eq!(reserialized["when"].as_array().unwrap().len(), 3);
}

#[dialog_common::test]
fn it_round_trips_rule_with_formula() {
    let json = json!({
        "deduce": {
            "with": {
                "quantity": {
                    "the": "diy.cook.doubled-quantity/quantity",
                    "as": "UnsignedInteger"
                }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "is": { "the": "diy.cook/quantity", "as": "UnsignedInteger" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "is": { "?": { "name": "qty" } }
                }
            },
            {
                "assert": "math/sum",
                "where": {
                    "of": { "?": { "name": "qty" } },
                    "with": { "?": { "name": "qty" } },
                    "is": { "?": { "name": "quantity" } }
                }
            }
        ]
    });

    let def: DeductiveRuleDescriptor = serde_json::from_value(json).unwrap();
    let reserialized = serde_json::to_value(&def).unwrap();
    let _reparsed: DeductiveRuleDescriptor =
        serde_json::from_value(reserialized.clone()).expect("Round-tripped JSON should parse back");

    assert_eq!(reserialized["when"][1]["assert"], "math/sum");
}

#[dialog_common::test]
fn it_round_trips_rule_with_negation() {
    let json = json!({
        "description": "A safe meal",
        "deduce": {
            "with": {
                "attendee": { "the": "diy.planner.safe-meal/attendee", "as": "Entity" },
                "recipe": { "the": "diy.planner.safe-meal/recipe", "as": "Entity" },
                "occasion": { "the": "diy.planner.safe-meal/occasion", "as": "Entity" }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "attendee": { "the": "diy.planner/attendee", "as": "Entity" },
                        "recipe": { "the": "diy.planner/recipe", "as": "Entity" },
                        "occasion": { "the": "diy.planner/occasion", "as": "Entity" }
                    }
                },
                "where": {
                    "attendee": { "?": { "name": "person" } },
                    "recipe": { "?": { "name": "recipe" } },
                    "occasion": { "?": { "name": "occasion" } }
                }
            }
        ],
        "unless": [
            {
                "assert": {
                    "with": {
                        "person": { "the": "diy.planner/person", "as": "Entity" },
                        "recipe": { "the": "diy.planner/recipe", "as": "Entity" }
                    }
                },
                "where": {
                    "person": { "?": { "name": "person" } },
                    "recipe": { "?": { "name": "recipe" } }
                }
            }
        ]
    });

    let def: DeductiveRuleDescriptor = serde_json::from_value(json).unwrap();
    let reserialized = serde_json::to_value(&def).unwrap();
    let _reparsed: DeductiveRuleDescriptor =
        serde_json::from_value(reserialized.clone()).expect("Round-tripped JSON should parse back");

    assert_eq!(reserialized["unless"].as_array().unwrap().len(), 1);
    assert!(
        reserialized.get("description").is_some(),
        "Description should be preserved"
    );
}

#[dialog_common::test]
fn it_compiles_valid_rule() {
    let json = json!({
        "deduce": {
            "with": {
                "name": { "the": "person/name", "as": "Text" },
                "age": { "the": "person/age", "as": "UnsignedInteger" }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "name": { "the": "person/name", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            },
            {
                "assert": {
                    "with": {
                        "age": { "the": "person/age", "as": "UnsignedInteger" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "age": { "?": { "name": "age" } }
                }
            }
        ]
    });

    let def: DeductiveRuleDescriptor = serde_json::from_value(json).unwrap();
    let rule = def.compile().expect("Valid rule should compile");
    assert_eq!(rule.conclusion().with().iter().count(), 2);
}

#[dialog_common::test]
fn it_rejects_rule_with_unbound_variable() {
    let json = json!({
        "deduce": {
            "with": {
                "name": { "the": "person/name", "as": "Text" },
                "age": { "the": "person/age", "as": "UnsignedInteger" }
            }
        },
        "when": [
            {
                "assert": {
                    "with": {
                        "name": { "the": "person/name", "as": "Text" }
                    }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }
        ]
    });

    let def: DeductiveRuleDescriptor = serde_json::from_value(json).unwrap();
    let result = def.compile();
    assert!(
        result.is_err(),
        "Should reject rule where 'age' conclusion variable is unbound"
    );
}
