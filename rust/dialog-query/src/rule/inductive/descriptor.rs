use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
use crate::premise::Premise;
use crate::proposition::Proposition;
use serde::{Deserialize, Serialize};

use super::InductiveRule;

/// An inductive-rule definition in the formal notation, suitable
/// for serialization.
///
/// Mirrors [`DeductiveRuleDescriptor`](crate::rule::DeductiveRuleDescriptor)
/// modulo the head field name: deductive rules use `assert` (the
/// head is *derived* on query, no commit); inductive rules use
/// `assert!` (the head is *asserted* into the branch when the body
/// matches, mirroring the tonk-yaml `!` convention for
/// mutation-producing operations).
///
/// ```json
/// {
///   "description": "...",
///   "assert!": { "with": { ... } },
///   "when":    [ { "assert": ..., "where": ... }, ... ],
///   "unless":  [ { "assert": ..., "where": ... }, ... ]
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InductiveRuleDescriptor {
    /// Human-readable description of the rule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The head: a concept the rule asserts when its body is
    /// satisfied. Serialized as `assert!` to mark the
    /// transaction-time commit semantics.
    #[serde(rename = "assert!")]
    pub assert: ConceptDescriptor,

    /// Conjunction of positive premises. All must be satisfied for
    /// the rule to fire.
    pub when: Vec<Proposition>,

    /// Negative premises. If any can be satisfied, the firing is
    /// filtered out.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unless: Vec<Proposition>,
}

impl InductiveRuleDescriptor {
    /// Compile this definition into an [`InductiveRule`] ready for
    /// evaluation.
    pub fn compile(self) -> Result<InductiveRule, TypeError> {
        let mut premises: Vec<Premise> = self.when.into_iter().map(Premise::Assert).collect();

        for proposition in self.unless {
            premises.push(Premise::Unless(Negation::not(proposition)));
        }

        InductiveRule::new(self.assert, premises)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[dialog_common::test]
    fn it_deserializes_increment_counter_rule() {
        // Realistic shape: read existing counter's count into ?prev,
        // derive ?count = ?prev + 1, assert a new counter row with
        // the incremented count.
        let json = json!({
            "description": "Increment a counter on increment command",
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "with": 1,
                        "is": { "?": { "name": "count" } }
                    }
                }
            ]
        });

        let def: InductiveRuleDescriptor = serde_json::from_value(json).unwrap();
        assert_eq!(
            def.description.as_deref(),
            Some("Increment a counter on increment command")
        );
        assert_eq!(def.assert.with().iter().count(), 1);
        assert_eq!(def.when.len(), 2);
        assert!(def.unless.is_empty());
    }

    #[dialog_common::test]
    fn it_compiles_to_inductive_rule() {
        // Asserts a counter row whose count is the previous count
        // plus one — derived via `math/sum`. The head differs from
        // the body premise, so each firing produces a genuinely
        // new fact.
        let json = json!({
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "with": 1,
                        "is": { "?": { "name": "count" } }
                    }
                }
            ]
        });

        let def: InductiveRuleDescriptor = serde_json::from_value(json).unwrap();
        let rule = def.compile().expect("rule should compile");
        assert_eq!(rule.conclusion().with().iter().count(), 1);
    }

    #[dialog_common::test]
    fn it_round_trips_rule_with_unless() {
        let json = json!({
            "description": "Promote pending todos that aren't blocked",
            "assert!": {
                "with": {
                    "status": { "the": "todo/status", "as": "Text" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "status": { "the": "todo/status", "as": "Text" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "status": { "?": { "name": "status" } }
                    }
                }
            ],
            "unless": [
                {
                    "assert": {
                        "with": {
                            "blocked": { "the": "todo/blocked", "as": "Boolean" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "blocked": true
                    }
                }
            ]
        });

        let def: InductiveRuleDescriptor = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(def.when.len(), 1);
        assert_eq!(def.unless.len(), 1);

        let reserialized = serde_json::to_value(&def).unwrap();
        assert!(reserialized["unless"].is_array());
        assert_eq!(reserialized["unless"].as_array().unwrap().len(), 1);

        let reparsed: InductiveRuleDescriptor = serde_json::from_value(reserialized).unwrap();
        assert_eq!(reparsed.unless.len(), 1);
    }

    #[dialog_common::test]
    fn it_rejects_unbound_variable_in_unless_on_deserialize() {
        // The unless clause references ?other which is never bound
        // by a positive premise — analysis should reject.
        let json = json!({
            "assert!": {
                "with": {
                    "status": { "the": "todo/status", "as": "Text" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "status": { "the": "todo/status", "as": "Text" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "status": { "?": { "name": "status" } }
                    }
                }
            ],
            "unless": [
                {
                    "assert": {
                        "with": {
                            "blocked": { "the": "todo/blocked", "as": "Boolean" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "other" } },
                        "blocked": true
                    }
                }
            ]
        });

        let result: Result<InductiveRule, _> = serde_json::from_value(json);
        assert!(
            result.is_err(),
            "Should reject rule where unless references unbound variable ?other"
        );
    }

    #[dialog_common::test]
    fn it_round_trips_inductive_rule() {
        let json = json!({
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "with": 1,
                        "is": { "?": { "name": "count" } }
                    }
                }
            ]
        });

        let rule: InductiveRule = serde_json::from_value(json).unwrap();
        let serialized = serde_json::to_value(&rule).unwrap();
        assert!(serialized["assert!"]["with"].is_object());
        assert!(serialized["when"].is_array());
        assert_eq!(serialized["when"].as_array().unwrap().len(), 2);

        let reparsed: InductiveRule = serde_json::from_value(serialized).unwrap();
        assert_eq!(reparsed.conclusion().with().iter().count(), 1);
    }

    #[dialog_common::test]
    fn it_rejects_unbound_head_on_deserialize() {
        let json = json!({
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger" },
                    "name": { "the": "counter/name", "as": "Text" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "count" } }
                    }
                }
            ]
        });

        let result: Result<InductiveRule, _> = serde_json::from_value(json);
        assert!(result.is_err(), "Expected unbound 'name' to fail");
    }
}
