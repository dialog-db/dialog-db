/// Serializable rule descriptor matching the formal notation.
pub mod descriptor;

pub use crate::concept::descriptor::ConceptDescriptor;
use crate::error::TypeError;
use crate::negation::Negation;
pub use crate::planner::Plan;
pub use crate::planner::{Conjunction, Planner};
pub use crate::premise::Premise;
pub use crate::{Attribute, Cardinality, Parameters, Proposition, Requirement, Value};
use crate::{Environment, Term, Type};
use descriptor::DeductiveRuleDescriptor;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    conclusion: ConceptDescriptor,
    /// Execution plan for the rule's premises, ordered for optimal
    /// evaluation. Produced by [`Planner::plan`] during compilation.
    join: Conjunction,
}
impl DeductiveRule {
    /// Compile a rule from a conclusion and premises.
    ///
    /// Plans the optimal premise execution order and validates that every
    /// conclusion variable is grounded by at least one positive premise.
    pub fn new(conclusion: ConceptDescriptor, premises: Vec<Premise>) -> Result<Self, TypeError> {
        // Plan the order of premises in a scope where none of the rule
        // parameters are bound to find the optimal execution order, or to
        // discover unsatisfiable premises (e.g. a formula whose required
        // cell is never derived by another premise).
        let join = Planner::from(premises).plan(&Environment::new())?;
        let rule = DeductiveRule { conclusion, join };

        // Verify that every conclusion parameter is derived by one of the
        // premises; otherwise the rule could never fully bind its output.
        let unbound = rule
            .conclusion
            .operands()
            .find(|name| !rule.join.binds.contains(name))
            .map(String::from);

        if let Some(variable) = unbound {
            return Err(TypeError::UnboundVariable {
                rule: Box::new(rule),
                variable,
            });
        }

        Ok(rule)
    }

    /// Returns the conclusion predicate for this rule.
    pub fn conclusion(&self) -> &ConceptDescriptor {
        &self.conclusion
    }

    /// Re-plan this rule's premises against a new scope.
    ///
    /// If replanning with the new bindings fails, falls back to the
    /// original compiled join plan.
    pub fn plan(&self, scope: &Environment) -> Conjunction {
        self.join.plan(scope).unwrap_or_else(|_| self.join.clone())
    }

    /// Returns an iterator over the operand names of this rule's conclusion.
    pub fn operands(&self) -> impl Iterator<Item = &str> {
        self.conclusion.operands()
    }
    /// Returns the names of the parameters for this rule.
    pub fn parameters(&self) -> impl Iterator<Item = &str> {
        self.conclusion.operands()
    }

    /// Creates a rule application by binding the provided terms to this rule's parameters.
    /// Validates that all required parameters are provided and returns an error if the
    /// application would be invalid.
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, TypeError> {
        self.conclusion.apply(parameters)
    }

    /// Converts this compiled rule back into a serializable [`DeductiveRuleDescriptor`].
    ///
    /// Reconstructs the `when`/`unless` split from the compiled premises.
    pub fn descriptor(&self) -> DeductiveRuleDescriptor {
        let mut when = Vec::new();
        let mut unless = Vec::new();

        for step in &self.join.steps {
            match &step.premise {
                Premise::Assert(proposition) => when.push(proposition.clone()),
                Premise::Unless(Negation(proposition)) => unless.push(proposition.clone()),
            }
        }

        DeductiveRuleDescriptor {
            description: None,
            deduce: self.conclusion.clone(),
            when,
            unless,
        }
    }
}

impl Serialize for DeductiveRule {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.descriptor().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for DeductiveRule {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let definition = DeductiveRuleDescriptor::deserialize(deserializer)?;
        definition.compile().map_err(D::Error::custom)
    }
}

impl Display for DeductiveRule {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{} {{", self.conclusion.this())?;
        write!(f, "this: {},", Type::Entity)?;
        for (name, attribute) in self.conclusion.with().iter() {
            match attribute.content_type() {
                Some(ty) => write!(f, "{}: {},", name, ty)?,
                None => write!(f, "{}: Any,", name)?,
            }
        }
        write!(f, "}}")
    }
}

impl From<&ConceptDescriptor> for DeductiveRule {
    fn from(concept: &ConceptDescriptor) -> Self {
        use crate::artifact::Entity;
        use crate::attribute::query::AttributeQuery;

        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");
        for (name, attribute) in concept.with().iter() {
            premises.push(
                AttributeQuery::new(
                    Term::Constant(Value::from(attribute.the().clone())),
                    this.clone(),
                    Term::var(name),
                    Term::blank(),
                    Some(attribute.cardinality()),
                )
                .into(),
            );
        }

        DeductiveRule::new(concept.clone(), premises).expect("Concept should compile")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::{Entity, Type};
    use crate::attribute::AttributeDescriptor;
    use crate::attribute::query::AttributeQuery;
    use crate::the;

    #[dialog_common::test]
    fn it_compiles_with_valid_premises() {
        let conclusion = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);
        let this = Term::<Entity>::var("this");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            AttributeQuery::new(
                Term::from(the!("user/age")),
                this,
                Term::var("age"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok());
    }

    #[dialog_common::test]
    fn it_rejects_unconstrained_fact() {
        let conclusion = ConceptDescriptor::from(vec![
            (
                "key",
                AttributeDescriptor::new(
                    the!("person/key"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "value",
                AttributeDescriptor::new(
                    the!("person/value"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ]);
        let premises = vec![
            AttributeQuery::new(
                Term::var("the"),
                Term::var("user"),
                Term::var("value"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        assert!(DeductiveRule::new(conclusion, premises).is_err());
    }

    #[dialog_common::test]
    fn it_rejects_unconstrained_relation() {
        let conclusion = ConceptDescriptor::from(vec![
            (
                "key",
                AttributeDescriptor::new(
                    the!("person/key"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "value",
                AttributeDescriptor::new(
                    the!("person/value"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ]);

        // All terms are variables — no constants at all.
        // The planner should reject this at install time.
        let premises = vec![
            AttributeQuery::new(
                Term::var("the"),
                Term::var("user"),
                Term::var("value"),
                Term::var("cause"),
                None,
            )
            .into(),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Rule with fully unconstrained relation premise should fail at install time"
        );
    }

    #[dialog_common::test]
    fn it_rejects_unused_parameter() {
        let conclusion = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                Term::var("this"),
                Term::var("name"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_err());
        if let Err(TypeError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "age", "Should report 'age' as unbound");
        }
    }

    #[dialog_common::test]
    fn it_rejects_empty_premises() {
        let conclusion = ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ]);
        assert!(DeductiveRule::new(conclusion, vec![]).is_err());
    }

    #[dialog_common::test]
    fn it_compiles_with_chained_dependencies() {
        let conclusion = ConceptDescriptor::from(vec![
            (
                "key",
                AttributeDescriptor::new(
                    the!("result/key"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "value",
                AttributeDescriptor::new(
                    the!("result/value"),
                    "",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
        ]);
        let this = Term::<Entity>::var("this");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                this.clone(),
                Term::constant("jack".to_string()),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            // Use ?key as the the variable
            // to ensure the conclusion parameter "key" gets bound.
            AttributeQuery::new(
                Term::var("key"),
                this,
                Term::var("value"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
        assert_eq!(result.unwrap().join.steps.len(), 2);
    }

    #[dialog_common::test]
    fn it_rejects_mismatched_parameter_name() {
        let conclusion = ConceptDescriptor::from(vec![(
            "key",
            AttributeDescriptor::new(the!("result/key"), "", Cardinality::One, Some(Type::String)),
        )]);

        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("user/name")),
                Term::<Entity>::var("this"),
                Term::var("key_var"),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should fail when variable name doesn't match parameter name"
        );
        if let Err(TypeError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "key", "Should report 'key' as unbound");
        }
    }

    #[dialog_common::test]
    fn it_rejects_negated_constraint_with_unbound_variable() {
        use crate::attribute::query::AttributeQuery;

        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        let name = Term::<String>::var("name");
        let z = Term::<String>::var("z");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                name.clone().into(),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            // ?z is never bound by any premise — should fail to compile
            !name.is(z),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should reject rule with negated constraint referencing unbound variable ?z"
        );
    }

    #[dialog_common::test]
    fn it_rejects_negated_constraint_with_unbound_variable_on_left() {
        use crate::attribute::query::AttributeQuery;

        let conclusion = ConceptDescriptor::from(vec![(
            "name",
            AttributeDescriptor::new(
                the!("person/name"),
                "",
                Cardinality::One,
                Some(Type::String),
            ),
        )]);

        let name = Term::<String>::var("name");
        let z = Term::<String>::var("z");
        let premises = vec![
            AttributeQuery::new(
                Term::from(the!("person/name")),
                Term::<Entity>::var("this"),
                name.clone().into(),
                Term::var("cause"),
                Some(Cardinality::One),
            )
            .into(),
            // flipped: ?z (unbound) on the left, ?name (bound) on the right
            !z.is(name),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should reject rule with negated constraint referencing unbound variable ?z (flipped)"
        );
    }
}
