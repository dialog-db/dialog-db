pub use crate::analyzer::Plan;
pub use crate::application::FactApplication;
use crate::error::{CompileError, SchemaError};
pub use crate::planner::Join;
pub use crate::predicate::Concept;
pub use crate::premise::Premise;
pub use crate::{Application, Attribute, Cardinality, Parameters, Requirement, Value};
use crate::{Term, Type};
use std::fmt::Display;

/// Represents a deductive rule that can be applied creating a premise.
#[derive(Debug, Clone, PartialEq)]
pub struct DeductiveRule {
    /// Conclusion that this rule reaches if all premises hold. This is
    /// typically what datalog calls rule head.
    pub conclusion: Concept,
    /// Premises that must hold for rule to reach it's conclusion. Typically
    /// datalog calls these rule body. These are guaranteed to be viable plans
    /// after compilation.
    pub premises: Vec<Plan>,
}
impl DeductiveRule {
    /// Create a new uncompiled rule from a conclusion and premises
    pub fn new(conclusion: Concept, premises: Vec<Premise>) -> Result<Self, CompileError> {
        // Convert premises to an intermediate form, then compile
        let uncompiled = UncompiledDeductiveRule {
            conclusion,
            premises,
        };
        uncompiled.compile()
    }

    pub fn operator(&self) -> &str {
        &self.conclusion.operator()
    }
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
    conclusion: Concept,
    premises: Vec<Premise>,
}

impl UncompiledDeductiveRule {
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

impl From<&Concept> for DeductiveRule {
    fn from(concept: &Concept) -> Self {
        use crate::artifact::Entity;

        let mut premises = Vec::new();

        let this = Term::<Entity>::var("this");
        for (name, attribute) in concept.attributes().iter() {
            let attr_str = attribute.the();
            let the = Term::Constant(
                attr_str
                    .parse::<crate::artifact::Attribute>()
                    .expect("Failed to parse attribute name"),
            );
            premises.push(
                FactApplication::new(
                    the,
                    this.clone(),
                    Term::var(name),
                    Term::var("cause"),
                    attribute.cardinality,
                )
                .into(),
            );
        }

        DeductiveRule::new(concept.clone(), premises).expect("Conceupt should compile")
    }
}

#[test]
fn test_rule_compiles_with_valid_premises() {
    use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
    // Rule: person(name, age) :- fact(user/name, ?user, name), fact(user/age, ?user, age)
    let conclusion = Concept::Dynamic {
        operator: "person".to_string(),
        attributes: vec![
            (
                "name",
                crate::attribute::Attribute::new("person", "name", "", Type::String),
            ),
            (
                "age",
                crate::attribute::Attribute::new("person", "age", "", Type::UnsignedInt),
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

#[test]
fn test_rule_fails_with_unconstrained_fact() {
    use crate::artifact::{Entity, Type};
    // Rule: person(key, value) :- fact(key, ?user, value) - all params unconstrained
    let conclusion = Concept::Dynamic {
        operator: "person".to_string(),
        attributes: vec![
            (
                "key",
                crate::attribute::Attribute::new("person", "key", "", Type::String),
            ),
            (
                "value",
                crate::attribute::Attribute::new("person", "value", "", Type::String),
            ),
        ]
        .into(),
    };
    let premises = vec![FactApplication::new(
        Term::var("key"),
        Term::<Entity>::var("user"),
        Term::var("value"),
        crate::attribute::Term::var("cause"),
        Cardinality::One,
    )
    .into()];
    assert!(DeductiveRule::new(conclusion, premises).is_err());
}

#[test]
fn test_rule_fails_with_unused_parameter() {
    use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
    // Rule: person(name, age) :- fact(user/name, ?this, name) - 'age' unused
    let conclusion = Concept::Dynamic {
        operator: "person".to_string(),
        attributes: vec![
            (
                "name",
                crate::attribute::Attribute::new("person", "name", "", Type::String),
            ),
            (
                "age",
                crate::attribute::Attribute::new("person", "age", "", Type::UnsignedInt),
            ),
        ]
        .into(),
    };
    let premises = vec![FactApplication::new(
        Term::Constant("user/name".parse::<ArtifactAttribute>().unwrap()),
        Term::<Entity>::var("this"),
        Term::var("name"),
        crate::attribute::Term::var("cause"),
        Cardinality::One,
    )
    .into()];
    let result = DeductiveRule::new(conclusion, premises);
    assert!(result.is_err());
    if let Err(CompileError::UnboundVariable { variable, .. }) = result {
        assert_eq!(variable, "age", "Should report 'age' as unbound");
    }
}

#[test]
fn test_rule_fails_with_no_premises() {
    use crate::artifact::Type;
    // Rule: person(name, age) :- (empty)
    let conclusion = Concept::Dynamic {
        operator: "person".to_string(),
        attributes: vec![
            (
                "name",
                crate::attribute::Attribute::new("person", "name", "", Type::String),
            ),
            (
                "age",
                crate::attribute::Attribute::new("person", "age", "", Type::UnsignedInt),
            ),
        ]
        .into(),
    };
    assert!(DeductiveRule::new(conclusion, vec![]).is_err());
}

#[test]
fn test_rule_compiles_with_chained_dependencies() {
    use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
    // Rule: result(key, value) :- fact(user/name, ?user, "jack"), fact(key, ?user, value)
    // First fact constrains ?user, allowing second fact to be planned
    let conclusion = Concept::Dynamic {
        operator: "result".to_string(),
        attributes: vec![
            (
                "key",
                crate::attribute::Attribute::new("result", "key", "", Type::String),
            ),
            (
                "value",
                crate::attribute::Attribute::new("result", "value", "", Type::String),
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

#[test]
fn test_rule_parameter_name_vs_variable_name() {
    use crate::artifact::{Attribute as ArtifactAttribute, Entity, Type};
    // This test ensures we correctly track variable names, not parameter names
    // Rule: result(key, value) :- fact(user/name, ?entity, key_var)
    // Parameter "is" maps to variable "key_var", not "key"
    let conclusion = Concept::Dynamic {
        operator: "result".to_string(),
        attributes: vec![(
            "key",
            crate::attribute::Attribute::new("result", "key", "", Type::String),
        )]
        .into(),
    };

    // The premise binds variable "key_var" via parameter "is"
    // But conclusion expects parameter "key" to be bound
    let premises = vec![FactApplication::new(
        Term::Constant("user/name".parse::<ArtifactAttribute>().unwrap()),
        Term::<Entity>::var("this"),
        Term::var("key_var"), // Variable name is "key_var", not "key"
        crate::attribute::Term::var("cause"),
        Cardinality::One,
    )
    .into()];

    let result = DeductiveRule::new(conclusion, premises);
    // Should fail because conclusion needs "key" but premise binds "key_var"
    assert!(
        result.is_err(),
        "Should fail when variable name doesn't match parameter name"
    );
    if let Err(CompileError::UnboundVariable { variable, .. }) = result {
        assert_eq!(variable, "key", "Should report 'key' as unbound");
    }
}
