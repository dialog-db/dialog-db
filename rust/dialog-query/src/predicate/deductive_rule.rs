pub use crate::analyzer::Plan;
pub use crate::application::FactApplication;
use crate::error::{CompileError, SchemaError};
pub use crate::planner::Join;
pub use crate::predicate::ConceptDescriptor;
pub use crate::premise::Premise;
pub use crate::{Application, Attribute, Cardinality, Parameters, Requirement, Value};
use crate::{Term, Type};
use std::fmt::Display;

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    pub conclusion: ConceptDescriptor,
    /// Premises that must hold for rule to reach it's conclusion. Typically
    /// datalog calls these rule body. These are guaranteed to be viable plans
    /// after compilation.
    pub premises: Vec<Plan>,
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

    /// Returns the operator name identifying this rule's conclusion concept.
    pub fn operator(&self) -> String {
        self.conclusion.operator()
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
    pub fn apply(&self, parameters: Parameters) -> Result<Application, SchemaError> {
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
        let plan = Join::try_from(self.premises)?;

        // We also verify that every rule parameter was derived by one of the
        // rule premises, otherwise we produce an error since rule evaluation
        // would not be able to bind such parameter.
        for name in self.conclusion.operands() {
            if !plan.binds.contains(&Term::<Value>::var(name)) {
                // Create a temporary rule for the error message
                let temp_rule = DeductiveRule {
                    conclusion: self.conclusion.clone(),
                    premises: plan.steps.clone(),
                };
                Err(CompileError::UnboundVariable {
                    rule: temp_rule,
                    variable: name.to_string(),
                })?;
            }
        }

        Ok(DeductiveRule {
            conclusion: self.conclusion,
            premises: plan.steps,
        })
    }
}

impl Display for DeductiveRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {{", self.operator())?;
        write!(f, "this: {},", Type::Entity)?;
        for (name, attribute) in self.conclusion.attributes().iter() {
            match attribute.content_type {
                Some(ty) => write!(f, "{}: {},", name, ty)?,
                None => write!(f, "{}: Any,", name)?,
            }
        }
        write!(f, "}}")
    }
}

impl From<&ConceptDescriptor> for DeductiveRule {
    fn from(concept: &ConceptDescriptor) -> Self {
        use crate::application::RelationApplication;
        use crate::artifact::Entity;
        use crate::predicate::RelationDescriptor;

        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");
        for (name, attribute) in concept.attributes().iter() {
            premises.push(
                RelationApplication::new(
                    Term::Constant(attribute.namespace.to_string()),
                    Term::Constant(attribute.name.to_string()),
                    this.clone(),
                    Term::var(name),
                    Term::var("cause"),
                    Some(RelationDescriptor::new(
                        attribute.content_type,
                        attribute.cardinality,
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

    #[dialog_common::test]
    fn test_rule_compiles_with_valid_premises() {
        use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    crate::attribute::AttributeSchema::new("person", "name", "", Type::String),
                ),
                (
                    "age",
                    crate::attribute::AttributeSchema::new("person", "age", "", Type::UnsignedInt),
                ),
            ]
            .into(),
        };
        let this = Term::<Entity>::var("this");
        let premises = vec![
            FactApplication::new(
                Term::Constant("user/name".parse::<ArtifactAttribute>().unwrap()),
                this.clone(),
                Term::var("name"),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
            )
            .into(),
            FactApplication::new(
                Term::Constant("user/age".parse::<ArtifactAttribute>().unwrap()),
                this,
                Term::var("age"),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok());
    }

    #[dialog_common::test]
    fn test_rule_fails_with_unconstrained_fact() {
        use crate::artifact::{Entity, Type};
        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "key",
                    crate::attribute::AttributeSchema::new("person", "key", "", Type::String),
                ),
                (
                    "value",
                    crate::attribute::AttributeSchema::new("person", "value", "", Type::String),
                ),
            ]
            .into(),
        };
        let premises = vec![
            FactApplication::new(
                Term::var("key"),
                Term::<Entity>::var("user"),
                Term::var("value"),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
            )
            .into(),
        ];
        assert!(DeductiveRule::new(conclusion, premises).is_err());
    }

    #[dialog_common::test]
    fn test_rule_fails_with_unconstrained_relation() {
        use crate::application::RelationApplication;
        use crate::artifact::{Entity, Type};

        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "key",
                    crate::attribute::AttributeSchema::new("person", "key", "", Type::String),
                ),
                (
                    "value",
                    crate::attribute::AttributeSchema::new("person", "value", "", Type::String),
                ),
            ]
            .into(),
        };

        // All terms are variables — no constants at all.
        // The planner should reject this at install time.
        let premises = vec![
            RelationApplication::new(
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
        use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    crate::attribute::AttributeSchema::new("person", "name", "", Type::String),
                ),
                (
                    "age",
                    crate::attribute::AttributeSchema::new("person", "age", "", Type::UnsignedInt),
                ),
            ]
            .into(),
        };
        let premises = vec![
            FactApplication::new(
                Term::Constant("user/name".parse::<ArtifactAttribute>().unwrap()),
                Term::<Entity>::var("this"),
                Term::var("name"),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
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
        use crate::artifact::Type;
        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "name",
                    crate::attribute::AttributeSchema::new("person", "name", "", Type::String),
                ),
                (
                    "age",
                    crate::attribute::AttributeSchema::new("person", "age", "", Type::UnsignedInt),
                ),
            ]
            .into(),
        };
        assert!(DeductiveRule::new(conclusion, vec![]).is_err());
    }

    #[dialog_common::test]
    fn test_rule_compiles_with_chained_dependencies() {
        use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![
                (
                    "key",
                    crate::attribute::AttributeSchema::new("result", "key", "", Type::String),
                ),
                (
                    "value",
                    crate::attribute::AttributeSchema::new("result", "value", "", Type::String),
                ),
            ]
            .into(),
        };
        let this = Term::<Entity>::var("this");
        let premises = vec![
            FactApplication::new(
                Term::Constant("user/name".parse::<ArtifactAttribute>().unwrap()),
                this.clone(),
                Term::Constant(Value::String("jack".to_string())),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
            )
            .into(),
            FactApplication::new(
                Term::var("key"),
                this,
                Term::var("value"),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
            )
            .into(),
        ];
        let result = DeductiveRule::new(conclusion, premises);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().premises.len(), 2);
    }

    #[dialog_common::test]
    fn test_rule_parameter_name_vs_variable_name() {
        use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
        let conclusion = ConceptDescriptor::Dynamic {
            description: String::new(),
            attributes: vec![(
                "key",
                crate::attribute::AttributeSchema::new("result", "key", "", Type::String),
            )]
            .into(),
        };

        let premises = vec![
            FactApplication::new(
                Term::Constant("user/name".parse::<ArtifactAttribute>().unwrap()),
                Term::<Entity>::var("this"),
                Term::var("key_var"),
                crate::attribute::Term::var("cause"),
                Cardinality::One,
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
