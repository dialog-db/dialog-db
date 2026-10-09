use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::premise::Premise;
use crate::proposition::Proposition;
use crate::reduce::ReduceSpec;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
///   "unless": [ { "assert": ..., "where": ... }, ... ],
///   "reduce": { "total": { "apply": "sum", "of": { "?": { "name": "salary" } } } }
/// }
/// ```
///
/// The optional `reduce` block turns the rule into a *reducing* rule
/// (`notes/aggregation.md`): a name-keyed map from head field to the
/// fold that defines it. Head fields not in `reduce` are the derived
/// grouping fields. An absent block is a plain rule and serializes
/// exactly as before, so existing content addresses are preserved.
/// Deserialization validates that every reduce key names a head
/// field — the map is keyed by head field, so an unknown key is
/// rejected at this earliest structural point.
///
/// A `DeductiveRuleDescriptor` can be compiled into a [`DeductiveRule`] for execution.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RawDescriptor")]
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

    /// The `reduce` clause: head field name to the fold defining it.
    /// Empty for a plain rule (and omitted from serialization).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub reduce: BTreeMap<String, ReduceSpec>,
}

/// The unvalidated wire shape of [`DeductiveRuleDescriptor`]:
/// deserialization goes through this mirror and
/// [`TryFrom`], so a `reduce` key naming no head field never
/// constructs a descriptor.
#[derive(Deserialize)]
struct RawDescriptor {
    #[serde(default)]
    description: Option<String>,
    deduce: ConceptDescriptor,
    when: Vec<Proposition>,
    #[serde(default)]
    unless: Vec<Proposition>,
    #[serde(default)]
    reduce: BTreeMap<String, ReduceSpec>,
}

impl TryFrom<RawDescriptor> for DeductiveRuleDescriptor {
    type Error = TypeError;

    fn try_from(raw: RawDescriptor) -> Result<Self, TypeError> {
        if let Some(field) = raw
            .reduce
            .keys()
            .find(|field| !raw.deduce.with().keys().any(|name| name == field.as_str()))
        {
            return Err(TypeError::ReducedFieldNotInHead {
                field: field.clone(),
            });
        }
        Ok(DeductiveRuleDescriptor {
            description: raw.description,
            deduce: raw.deduce,
            when: raw.when,
            unless: raw.unless,
            reduce: raw.reduce,
        })
    }
}

impl DeductiveRuleDescriptor {
    /// Compiles this definition into a [`DeductiveRule`] ready for execution.
    ///
    /// Converts the `when` and `unless` propositions into premises, plans
    /// their execution order, and validates that every conclusion variable
    /// is grounded by a positive premise (reduced fields are defined by
    /// their folds instead — see
    /// [`DeductiveRule::with_reduce`]).
    pub fn compile(self) -> Result<DeductiveRule, TypeError> {
        let mut premises: Vec<Premise> = self.when.into_iter().map(Premise::Assert).collect();

        for proposition in self.unless {
            premises.push(Premise::Unless(Negation::not(proposition)));
        }

        DeductiveRule::with_reduce(self.deduce, premises, self.reduce)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Pick;
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
                        "as": "text:"
                    },
                    "quantity": {
                        "description": "Amount needed",
                        "the": "diy.cook/quantity",
                        "as": "natural:"
                    },
                    "unit": {
                        "description": "Unit of measurement",
                        "the": "diy.cook/unit",
                        "as": "text:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "diy.cook/ingredient-name", "as": "text:" }
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
                            "quantity": { "the": "diy.cook/quantity", "as": "natural:" }
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
                            "unit": { "the": "diy.cook/unit", "as": "text:" }
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

    /// A rule stored by the release before picks decodes: its
    /// attributes spell `cardinality` and a single `the`, which read as
    /// `last` and `all` over that relation. The bytes are the dag-cbor
    /// of the descriptor as that release wrote it, which is what a
    /// replica holds for every rule a user installed under it.
    #[dialog_common::test]
    fn it_decodes_a_rule_stored_before_picks() {
        let legacy = json!({
            "deduce": {
                "description": "An ingredient",
                "with": {
                    "name": {
                        "description": "Ingredient name",
                        "the": "diy.cook/ingredient-name",
                        "cardinality": "one",
                        "as": "text:"
                    },
                    "tags": {
                        "description": "",
                        "the": "diy.cook/tag",
                        "cardinality": "many",
                        "as": "text:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": {
                                "the": "diy.cook/ingredient-name",
                                "cardinality": "one",
                                "as": "text:"
                            }
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
                            "tags": {
                                "the": "diy.cook/tag",
                                "cardinality": "many",
                                "as": "text:"
                            }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "tags": { "?": { "name": "tags" } }
                    }
                }
            ]
        });
        let bytes = serde_ipld_dagcbor::to_vec(&legacy).expect("the legacy form encodes");
        let rule = super::super::DeductiveRule::decode(&bytes).expect("a legacy rule decodes");
        let head = rule.conclusion();
        let field = |wanted: &str| {
            head.with()
                .iter()
                .find(|(name, _)| *name == wanted)
                .map(|(_, field)| field)
                .expect("the field")
        };
        let (name, tags) = (field("name"), field("tags"));
        assert_eq!(name.descriptor().pick(), &Pick::Last);
        assert_eq!(tags.descriptor().pick(), &Pick::All);
        assert_eq!(name.descriptor().cardinality(), crate::Cardinality::One);
        assert_eq!(tags.descriptor().cardinality(), crate::Cardinality::Many);
        // Re-encoded, the rule never writes the older spelling back.
        let again = rule.encode();
        let decoded: serde_json::Value =
            serde_ipld_dagcbor::from_slice(&again).expect("the re-encoding is a map");
        assert!(
            decoded.to_string().contains("\"pick\"")
                || !decoded.to_string().contains("cardinality"),
            "the branch spells picks as `pick`: {decoded}"
        );
    }

    #[dialog_common::test]
    fn it_round_trips_rule_with_formula() {
        let json = json!({
            "deduce": {
                "with": {
                    "quantity": {
                        "the": "diy.cook.doubled-quantity/quantity",
                        "as": "natural:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "is": { "the": "diy.cook/quantity", "as": "natural:" }
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
                    "name": { "the": "org.example.employee/name", "as": "text:" },
                    "role": { "the": "org.example.employee/role", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "org.example.person/name", "as": "text:" },
                            "title": { "the": "org.example.person/title", "as": "text:" }
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
                        "as": "entity:"
                    },
                    "recipe": {
                        "the": "diy.planner.safe-meal/recipe",
                        "as": "entity:"
                    },
                    "occasion": {
                        "the": "diy.planner.safe-meal/occasion",
                        "as": "entity:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "attendee": { "the": "diy.planner/attendee", "as": "entity:" },
                            "recipe": { "the": "diy.planner/recipe", "as": "entity:" },
                            "occasion": { "the": "diy.planner/occasion", "as": "entity:" }
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
                            "person": { "the": "diy.planner/person", "as": "entity:" },
                            "recipe": { "the": "diy.planner/recipe", "as": "entity:" }
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
                    "name": { "the": "person/name", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                    "name": { "the": "person/name", "as": "text:" },
                    "age": { "the": "person/age", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                            "age": { "the": "person/age", "as": "natural:" }
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
                        "as": "natural:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "is": { "the": "diy.cook/quantity", "as": "natural:" }
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
                    "name": { "the": "person/name", "as": "text:" },
                    "age": { "the": "person/age", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                    "name": { "the": "person/name", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                    "name": { "the": "person/name", "as": "text:" },
                    "age": { "the": "person/age", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                            "age": { "the": "person/age", "as": "natural:" }
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
                    "name": { "the": "person/name", "as": "text:" },
                    "age": { "the": "person/age", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                            "age": { "the": "person/age", "as": "natural:" }
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
                        "as": "natural:"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "is": { "the": "diy.cook/quantity", "as": "natural:" }
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
                    "name": { "the": "person/name", "as": "text:" },
                    "age": { "the": "person/age", "as": "natural:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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

    /// The employee body shared by the reduce fixtures below: an
    /// anonymous employee concept premise binding the department
    /// entity as `?this` (the head groups per department) and the
    /// salary as `?salary`.
    fn employee_body(salary_var: &str) -> serde_json::Value {
        json!([{
            "assert": { "with": {
                "dept": { "the": "org.employee/dept", "as": "entity:" },
                "salary": { "the": "org.employee/salary", "as": "natural:" }
            }},
            "where": {
                "this": { "?": { "name": "employee" } },
                "dept": { "?": { "name": "this" } },
                "salary": { "?": { "name": salary_var } }
            }
        }])
    }

    /// `DeptTotal { total: sum(?salary) }` grouped by the department
    /// entity (`?this`).
    fn dept_total_json() -> serde_json::Value {
        json!({
            "deduce": { "with": {
                "total": { "the": "org.dept/total", "as": "natural:" }
            }},
            "when": employee_body("salary"),
            "reduce": { "total": { "apply": "sum", "of": { "?": { "name": "salary" } } } }
        })
    }

    /// A plain rule serializes with no `reduce` key at all, so
    /// pre-aggregation content addresses are preserved.
    #[dialog_common::test]
    fn it_omits_reduce_when_empty() {
        let plain: DeductiveRuleDescriptor = serde_json::from_value(json!({
            "deduce": { "with": { "name": { "the": "person/name", "as": "text:" } } },
            "when": [{
                "assert": { "with": { "name": { "the": "person/name", "as": "text:" } } },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }]
        }))
        .unwrap();
        let serialized = serde_json::to_value(&plain).unwrap();
        assert!(serialized.get("reduce").is_none());
    }

    /// A reduce key naming no head field is rejected at the earliest
    /// structural point: deserialization.
    #[dialog_common::test]
    fn it_rejects_reduce_key_not_in_head() {
        let mut bad = dept_total_json();
        bad["reduce"] = json!({
            "grand": { "apply": "sum", "of": { "?": { "name": "salary" } } }
        });
        let result = serde_json::from_value::<DeductiveRuleDescriptor>(bad);
        let error = result
            .expect_err("unknown reduce key must not parse")
            .to_string();
        assert!(
            error.contains("grand"),
            "the error names the unknown field, got: {error}"
        );
    }

    #[dialog_common::test]
    fn it_rejects_unbound_variable_in_negation_on_deserialize() {
        use super::super::DeductiveRule;

        // The unless clause references ?z which is never bound by a positive premise
        let json = json!({
            "deduce": {
                "with": {
                    "name": { "the": "person/name", "as": "text:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "name": { "the": "person/name", "as": "text:" }
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
                            "blocked": { "the": "person/blocked", "as": "boolean:" }
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
