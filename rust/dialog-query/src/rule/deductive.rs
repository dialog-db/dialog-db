pub use crate::concept::descriptor::ConceptDescriptor;
use crate::error::{CompileError, SchemaError};
pub use crate::planner::Plan;
pub use crate::planner::{Conjunction, Planner};
pub use crate::premise::Premise;
pub use crate::{Attribute, Cardinality, Parameters, Proposition, Requirement, Value};
use crate::{Environment, Term, Type};
use std::fmt::Display;

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
    /// Create a new uncompiled rule from a conclusion and premises
    pub fn new(
        conclusion: ConceptDescriptor,
        premises: Vec<Premise>,
    ) -> Result<Self, CompileError> {
        // Convert premises to an intermediate form, then compile
        let uncompiled = UncompiledDeductiveRule {
            conclusion,
            premises,
        };
        uncompiled.compile()
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
    pub fn apply(&self, parameters: Parameters) -> Result<Proposition, SchemaError> {
        self.conclusion.apply(parameters)
    }
}

/// Internal helper for rules before compilation
pub struct UncompiledDeductiveRule {
    conclusion: ConceptDescriptor,
    premises: Vec<Premise>,
}

impl UncompiledDeductiveRule {
    /// Compiles the rule by planning premise execution order and validating bindings.
    pub fn compile(self) -> Result<DeductiveRule, CompileError> {
        // We attempt to plan the order of premises in a scope where none of the
        // rule parameters are bound in order to identify most optimal execution
        // order in such scenario or to discover that some premise in the rule
        // is not satisfiable e.g. if formula uses rule parameter in the required
        // cell which is not derived from any other premise.
        let join = Planner::from(self.premises).plan(&Environment::new())?;

        // We also verify that every rule parameter was derived by one of the
        // rule premises, otherwise we produce an error since rule evaluation
        // would not be able to bind such parameter.
        for name in self.conclusion.operands() {
            if !join.binds.contains(&Term::<Value>::var(name)) {
                // Create a temporary rule for the error message
                let temp_rule = DeductiveRule {
                    conclusion: self.conclusion.clone(),
                    join: join.clone(),
                };
                Err(CompileError::UnboundVariable {
                    rule: temp_rule,
                    variable: name.to_string(),
                })?;
            }
        }

        Ok(DeductiveRule {
            conclusion: self.conclusion,
            join,
        })
    }
}

impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        use crate::relation::descriptor::RelationDescriptor;
        use crate::relation::query::RelationQuery;

        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");
        for (name, attribute) in concept.with().iter() {
            premises.push(
                RelationQuery::new(
                    Term::Constant(attribute.domain().to_string()),
                    Term::Constant(attribute.name().to_string()),
                    this.clone(),
                    Term::var(name),
                    Term::var("cause"),
                    Some(RelationDescriptor::new(
                        attribute.content_type(),
                        attribute.cardinality(),
                    )),
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
    use crate::relation::descriptor::RelationDescriptor;
    use crate::relation::query::RelationQuery;
    use crate::the;

    #[dialog_common::test]
    fn test_rule_compiles_with_valid_premises() {
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
            RelationQuery::new(
                Term::Constant("user".to_string()),
                Term::Constant("name".to_string()),
                this.clone(),
                Term::var("name"),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
            RelationQuery::new(
                Term::Constant("user".to_string()),
                Term::Constant("age".to_string()),
                this,
                Term::var("age"),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok());
    }

    #[dialog_common::test]
    fn test_rule_fails_with_unconstrained_fact() {
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
            RelationQuery::new(
                Term::var("key_ns"),
                Term::var("key_name"),
                Term::<Entity>::var("user"),
                Term::var("value"),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
        ];
        assert!(DeductiveRule::new(conclusion, premises).is_err());
    }

    #[dialog_common::test]
    fn test_rule_fails_with_unconstrained_relation() {
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
            RelationQuery::new(
                Term::var("ns"),
                Term::var("attr"),
                Term::<Entity>::var("user"),
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
    fn test_rule_fails_with_unused_parameter() {
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
            RelationQuery::new(
                Term::Constant("user".to_string()),
                Term::Constant("name".to_string()),
                Term::<Entity>::var("this"),
                Term::var("name"),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_err());
        if let Err(CompileError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "age", "Should report 'age' as unbound");
        }
    }

    #[dialog_common::test]
    fn test_rule_fails_with_no_premises() {
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
    fn test_rule_compiles_with_chained_dependencies() {
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
            RelationQuery::new(
                Term::Constant("user".to_string()),
                Term::Constant("name".to_string()),
                this.clone(),
                Term::Constant(Value::String("jack".to_string())),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
            // Use explicit domain/name with ?key as the name variable
            // to ensure the conclusion parameter "key" gets bound.
            RelationQuery::new(
                Term::var("key"),
                Term::blank(),
                this,
                Term::var("value"),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());
        assert_eq!(result.unwrap().join.steps.len(), 2);
    }

    #[dialog_common::test]
    fn test_rule_parameter_name_vs_variable_name() {
        let conclusion = ConceptDescriptor::from(vec![(
            "key",
            AttributeDescriptor::new(the!("result/key"), "", Cardinality::One, Some(Type::String)),
        )]);

        let premises = vec![
            RelationQuery::new(
                Term::Constant("user".to_string()),
                Term::Constant("name".to_string()),
                Term::<Entity>::var("this"),
                Term::var("key_var"),
                Term::var("cause"),
                Some(RelationDescriptor::new(None, Cardinality::One)),
            )
            .into(),
        ];

        let result = DeductiveRule::new(conclusion, premises);
        assert!(
            result.is_err(),
            "Should fail when variable name doesn't match parameter name"
        );
        if let Err(CompileError::UnboundVariable { variable, .. }) = result {
            assert_eq!(variable, "key", "Should report 'key' as unbound");
        }
    }
}
