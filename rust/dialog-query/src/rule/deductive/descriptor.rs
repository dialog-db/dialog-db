use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::premise::Premise;
use crate::proposition::Proposition;
use serde::{Deserialize, Serialize};

use super::DeductiveRule;

/// A rule definition in the formal notation, suitable for serialization.
///
/// This corresponds directly to the JSON/YAML schema defined in the
/// Dialog Notation System specification:
///
/// ```json
/// {
///   "description": "...",
///   "deduce": { "with": { ... } },
///   "when":   [ { "assert": ..., "where": ... }, ... ],
///   "unless": [ { "assert": ..., "where": ... }, ... ]
/// }
/// ```
///
/// A `DeductiveRuleDescriptor` can be compiled into a [`DeductiveRule`] for execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeductiveRuleDescriptor {
    /// Human-readable description of the rule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The conclusion: a concept the rule derives when its body is satisfied.
    pub deduce: ConceptDescriptor,

    /// Conjunction of premises. All must be satisfied for the rule to fire.
    pub when: Vec<Proposition>,

    /// Exclusion patterns. If any can be satisfied, the result is filtered out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unless: Vec<Proposition>,
}

impl DeductiveRuleDescriptor {
    /// Compiles this definition into a [`DeductiveRule`] ready for execution.
    ///
    /// Converts the `when` and `unless` propositions into premises, plans
    /// their execution order, and validates that every conclusion variable
    /// is grounded by a positive premise.
    pub fn compile(self) -> Result<DeductiveRule, TypeError> {
        let mut premises: Vec<Premise> = self.when.into_iter().map(Premise::Assert).collect();

        for proposition in self.unless {
            premises.push(Premise::Unless(Negation::not(proposition)));
        }

        DeductiveRule::new(self.deduce, premises)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[dialog_common::test]
    fn it_deserializes_ingredient_rule() {
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

        let def: DeductiveRuleDescriptor = serde_json::from_value(json).unwrap();
        assert_eq!(def.deduce.description(), Some("An ingredient"));
        assert_eq!(def.deduce.with().iter().count(), 3);
        assert_eq!(def.when.len(), 3);
        assert!(def.unless.is_empty());
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

        let def: DeductiveRuleDescriptor = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(def.when.len(), 2);
        assert!(matches!(&def.when[0], Proposition::Concept(_)));
        assert!(matches!(&def.when[1], Proposition::Formula(_)));

        let reserialized = serde_json::to_value(&def).unwrap();
        assert_eq!(reserialized["when"][1]["assert"], "math/sum");

        let reparsed: DeductiveRuleDescriptor = serde_json::from_value(reserialized).unwrap();
        assert_eq!(reparsed.when.len(), 2);
    }

    #[dialog_common::test]
    fn it_round_trips_rule_with_equality() {
        let json = json!({
            "deduce": {
                "with": {
                    "name": { "the": "org.example.employee/name", "as": "Text" },
                    "role": { "the": "org.example.employee/role", "as": "Text" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "org.example.person/name", "as": "Text" },
                            "title": { "the": "org.example.person/title", "as": "Text" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "name": { "?": { "name": "name" } },
                        "title": { "?": { "name": "role" } }
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

        let def: DeductiveRuleDescriptor = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(def.when.len(), 2);
        assert!(matches!(&def.when[1], Proposition::Constraint(_)));

        let reserialized = serde_json::to_value(&def).unwrap();
        assert_eq!(reserialized["when"][1]["assert"], "==");

        let reparsed: DeductiveRuleDescriptor = serde_json::from_value(reserialized).unwrap();
        assert_eq!(reparsed.when.len(), 2);
    }

    #[dialog_common::test]
    fn it_round_trips_rule_with_negation() {
        let json = json!({
            "deduce": {
                "description": "A safe meal",
                "with": {
                    "attendee": {
                        "the": "diy.planner.safe-meal/attendee",
                        "as": "Entity"
                    },
                    "recipe": {
                        "the": "diy.planner.safe-meal/recipe",
                        "as": "Entity"
                    },
                    "occasion": {
                        "the": "diy.planner.safe-meal/occasion",
                        "as": "Entity"
                    }
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

        let def: DeductiveRuleDescriptor = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(def.when.len(), 1);
        assert_eq!(def.unless.len(), 1);

        let reserialized = serde_json::to_value(&def).unwrap();
        assert!(reserialized["unless"].is_array());
        assert_eq!(reserialized["unless"].as_array().unwrap().len(), 1);

        let reparsed: DeductiveRuleDescriptor = serde_json::from_value(reserialized).unwrap();
        assert_eq!(reparsed.unless.len(), 1);
    }

    #[dialog_common::test]
    fn it_omits_unless_when_empty() {
        let json = json!({
            "deduce": {
                "with": {
                    "name": { "the": "person/name", "as": "Text" }
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
        let reserialized = serde_json::to_value(&def).unwrap();
        assert!(
            reserialized.get("unless").is_none(),
            "Empty unless should be omitted"
        );
    }

    #[dialog_common::test]
    fn it_compiles_to_deductive_rule() {
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
        let rule = def.compile();
        assert!(rule.is_ok(), "Rule should compile: {:?}", rule.err());

        let rule = rule.unwrap();
        assert_eq!(rule.conclusion().with().iter().count(), 2);
    }

    #[dialog_common::test]
    fn it_compiles_rule_with_formula() {
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
        let rule = def.compile();
        assert!(
            rule.is_ok(),
            "Rule with formula should compile: {:?}",
            rule.err()
        );
    }

    #[dialog_common::test]
    fn it_rejects_rule_with_unbound_conclusion_variable() {
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
        assert!(result.is_err(), "Should reject rule where 'age' is unbound");
    }

    #[dialog_common::test]
    fn it_preserves_description_through_round_trip() {
        let json = json!({
            "description": "Find safe meals for attendees",
            "deduce": {
                "with": {
                    "name": { "the": "person/name", "as": "Text" }
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
        assert_eq!(
            def.description.as_deref(),
            Some("Find safe meals for attendees")
        );

        let reserialized = serde_json::to_value(&def).unwrap();
        assert_eq!(reserialized["description"], "Find safe meals for attendees");
    }

    #[dialog_common::test]
    fn it_serializes_deductive_rule() {
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
        let rule = def.compile().unwrap();

        let serialized = serde_json::to_value(&rule).unwrap();
        assert!(serialized["deduce"]["with"].is_object());
        assert!(serialized["when"].is_array());
        assert_eq!(serialized["when"].as_array().unwrap().len(), 2);
    }

    #[dialog_common::test]
    fn it_deserializes_deductive_rule() {
        use super::super::DeductiveRule;

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

        let rule: DeductiveRule = serde_json::from_value(json).unwrap();
        assert_eq!(rule.conclusion().with().iter().count(), 2);
    }

    #[dialog_common::test]
    fn it_round_trips_deductive_rule() {
        use super::super::DeductiveRule;

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

        let rule: DeductiveRule = serde_json::from_value(json).unwrap();
        let serialized = serde_json::to_value(&rule).unwrap();

        // Should produce valid JSON that can be parsed back
        let _reparsed: DeductiveRule = serde_json::from_value(serialized.clone())
            .expect("Serialized DeductiveRule should deserialize back");

        // Formula selector should be preserved
        let formulas: Vec<_> = serialized["when"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["assert"].is_string() && p["assert"] != "==")
            .collect();
        assert_eq!(formulas.len(), 1);
        assert_eq!(formulas[0]["assert"], "math/sum");
    }

    #[dialog_common::test]
    fn it_rejects_unbound_conclusion_variable_on_deserialize() {
        use super::super::DeductiveRule;

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

        let result: Result<DeductiveRule, _> = serde_json::from_value(json);
        assert!(
            result.is_err(),
            "Should reject rule where conclusion variable 'age' is never bound by any premise"
        );
    }

    #[dialog_common::test]
    fn it_rejects_unbound_variable_in_negation_on_deserialize() {
        use super::super::DeductiveRule;

        // The unless clause references ?z which is never bound by a positive premise
        let json = json!({
            "deduce": {
                "with": {
                    "name": { "the": "person/name", "as": "Text" }
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
            ],
            "unless": [
                {
                    "assert": {
                        "with": {
                            "blocked": { "the": "person/blocked", "as": "Boolean" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "z" } },
                        "blocked": true
                    }
                }
            ]
        });

        let result: Result<DeductiveRule, _> = serde_json::from_value(json);
        assert!(
            result.is_err(),
            "Should reject rule where negation references unbound variable ?z"
        );
    }
}
