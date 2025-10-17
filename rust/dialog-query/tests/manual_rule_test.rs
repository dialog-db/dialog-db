use dialog_query::artifact::{Entity, Type, Value};
use dialog_query::attribute::{Attribute, Cardinality};
use dialog_query::concept::{Concept, Instance, Instructions, Match as ConceptMatch};
use dialog_query::dsl::Quarriable;
use dialog_query::predicate::fact::Fact;
use dialog_query::rule::{Match, Premises, Rule, When};
use dialog_query::term::Term;
use dialog_query::types::Scalar;
use dialog_query::Application;
use dialog_query::Premise;
use std::marker::PhantomData;

/// Manual implementation of Person struct with Concept and Rule traits
/// This serves as a template for what the derive macro should generate
#[derive(Debug, Clone)]
pub struct Person {
    pub this: Entity,
    /// Name of the person
    pub name: String,
    /// Age of the person
    pub age: u32,
}

/// Match pattern for Person - has Term-wrapped fields for querying
#[derive(Debug, Clone)]
pub struct PersonMatch {
    /// The entity being matched
    pub this: Term<Entity>,
    /// Name term - can be a variable or concrete value
    pub name: Term<String>,
    /// Age term - can be a variable or concrete value
    pub age: Term<u32>,
}

/// Assert pattern for Person - used in rule conclusions
#[derive(Debug, Clone)]
pub struct PersonAssert {
    pub name: Term<String>,
    pub age: Term<u32>,
}

/// Retract pattern for Person - used for removing facts
#[derive(Debug, Clone)]
pub struct PersonRetract {
    pub name: Term<String>,
    pub age: Term<u32>,
}

/// Attributes pattern for Person - enables fluent query building
#[derive(Debug, Clone)]
pub struct PersonTerms;
impl PersonTerms {
    pub fn this() -> Term<Entity> {
        Term::var("this")
    }
    pub fn name() -> Term<String> {
        Term::var("name")
    }
    pub fn age() -> Term<u32> {
        Term::var("age")
    }
}

// Module to hold Person-related constants and attributes
pub mod person {
    use super::*;

    /// The namespace for Person attributes
    pub const NAMESPACE: &str = "person";

    /// Static attribute definitions
    pub static NAME_ATTR: Attribute<String> = Attribute {
        namespace: NAMESPACE,
        name: "name",
        description: "Name of the person",
        cardinality: Cardinality::One,
        content_type: Some(Type::String),
        marker: PhantomData,
    };

    pub static AGE_ATTR: Attribute<u32> = Attribute {
        namespace: NAMESPACE,
        name: "age",
        description: "Age of the person",
        cardinality: Cardinality::One,
        content_type: Some(Type::UnsignedInt),
        marker: PhantomData,
    };

    /// All attributes as Attribute<Value> for the attributes() method
    pub static ATTRIBUTES: &[Attribute<Value>] = &[
        Attribute {
            namespace: NAMESPACE,
            name: "name",
            description: "Name of the person",
            cardinality: Cardinality::One,
            content_type: Some(Type::String),
            marker: PhantomData,
        },
        Attribute {
            namespace: NAMESPACE,
            name: "age",
            description: "Age of the person",
            cardinality: Cardinality::One,
            content_type: Some(Type::UnsignedInt),
            marker: PhantomData,
        },
    ];

    /// Attribute tuples for the Attributes trait implementation
    pub static ATTRIBUTE_TUPLES: &[(&str, Attribute<Value>)] = &[
        (
            "name",
            Attribute {
                namespace: NAMESPACE,
                name: "name",
                description: "Name of the person",
                cardinality: Cardinality::One,
                content_type: Some(Type::String),
                marker: PhantomData,
            },
        ),
        (
            "age",
            Attribute {
                namespace: NAMESPACE,
                name: "age",
                description: "Age of the person",
                cardinality: Cardinality::One,
                content_type: Some(Type::UnsignedInt),
                marker: PhantomData,
            },
        ),
    ];
}

impl Concept for Person {
    type Instance = Person;
    type Match = PersonMatch;
    type Assert = PersonAssert;
    type Retract = PersonRetract;
    type Term = PersonTerms;

    fn concept() -> dialog_query::predicate::concept::Concept {
        static ATTRS: dialog_query::predicate::concept::Attributes =
            dialog_query::predicate::concept::Attributes::Static(&[]);

        dialog_query::predicate::concept::Concept::Static {
            operator: "person",
            attributes: &ATTRS,
        }
    }
}

impl Quarriable for Person {
    type Query = PersonMatch;
}

impl TryFrom<dialog_query::selection::Answer> for Person {
    type Error = dialog_query::error::InconsistencyError;

    fn try_from(source: dialog_query::selection::Answer) -> Result<Self, Self::Error> {
        Ok(Person {
            this: source.get(&PersonTerms::this())?,
            name: source.get(&PersonTerms::name())?,
            age: source.get(&PersonTerms::age())?,
        })
    }
}

impl From<PersonMatch> for dialog_query::Parameters {
    fn from(source: PersonMatch) -> Self {
        let mut terms = Self::new();

        terms.insert("this".into(), source.this.as_unknown());
        terms.insert("name".into(), source.name.as_unknown());
        terms.insert("age".into(), source.age.as_unknown());

        terms
    }
}

impl ConceptMatch for PersonMatch {
    type Concept = Person;
    type Instance = Person;

    fn realize(
        &self,
        source: dialog_query::selection::Answer,
    ) -> Result<Self::Instance, dialog_query::QueryError> {
        Ok(Self::Instance {
            this: source.get(&self.this)?,
            name: source.get(&self.name)?,
            age: source.get(&self.age)?,
        })
    }
}

// TODO: Attributes trait no longer exists - replaced by ConceptType
// impl Attributes for PersonAttributes {
//     fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
//         person::ATTRIBUTE_TUPLES
//     }
//
//     fn of<T: Into<Term<Entity>>>(entity: T) -> Self {
//         let entity = entity.into();
//         PersonAttributes {
//             this: entity.clone(),
//             name: person::NAME_ATTR.of(entity.clone()),
//             age: person::AGE_ATTR.of(entity),
//         }
//     }
// }

impl Instance for Person {
    fn this(&self) -> Entity {
        // For now panic - proper implementation would store entity
        panic!("Instance trait implementation requires an entity field")
    }
}

impl Premises for PersonMatch {
    type IntoIter = std::vec::IntoIter<Premise>;

    fn premises(self) -> Self::IntoIter {
        Person::when(self).into_iter()
    }
}

impl Rule for Person {
    fn when(terms: Match<Self>) -> When {
        // Create fact selectors for each attribute
        // We need to convert the typed terms to Term<Value>
        let name_value_term = match &terms.name {
            Term::Variable { name, .. } => Term::Variable {
                name: name.clone(),
                content_type: Default::default(),
            },
            Term::Constant(value) => Term::Constant(value.as_value()),
        };

        let age_value_term = match &terms.age {
            Term::Variable { name, .. } => Term::Variable {
                name: name.clone(),
                content_type: Default::default(),
            },
            Term::Constant(value) => Term::Constant(value.as_value()),
        };

        let name_fact = Fact::select()
            .the("person/name")
            .of(terms.this.clone())
            .is(name_value_term);

        let age_fact = Fact::select()
            .the("person/age")
            .of(terms.this.clone())
            .is(age_value_term);

        // Return When collection with both facts
        [name_fact, age_fact].into()
    }
}

// Implement Instructions for PersonAssert
impl Instructions for PersonAssert {
    type IntoIter = std::vec::IntoIter<dialog_artifacts::Instruction>;

    fn instructions(self) -> Self::IntoIter {
        // For now, return empty vec as placeholder
        // In real implementation, this would generate Assert instructions
        vec![].into_iter()
    }
}

// Implement Instructions for PersonRetract
impl Instructions for PersonRetract {
    type IntoIter = std::vec::IntoIter<dialog_artifacts::Instruction>;

    fn instructions(self) -> Self::IntoIter {
        // For now, return empty vec as placeholder
        // In real implementation, this would generate Retract instructions
        vec![].into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_person_rule_when() {
        let entity = Term::var("person_entity");
        let person_match = PersonMatch {
            this: entity.clone(),
            name: Term::var("person_name"),
            age: Term::var("person_age"),
        };

        let when_statements = Person::when(person_match);

        // Should have 2 statements - one for each attribute
        assert_eq!(when_statements.len(), 2);

        // Each statement should be a FactSelector
        for statement in &when_statements {
            match statement {
                Premise::Apply(Application::Fact(selector)) => {
                    assert!(selector.parameters().get("the").is_some());
                    assert!(selector.parameters().get("of").is_some());
                    assert!(selector.parameters().get("is").is_some());
                }
                Premise::Apply(Application::Formula(_)) => {
                    panic!("Unexpected ApplyFormula premise in test");
                }
                Premise::Apply(Application::Concept(_)) => {
                    panic!("Unexpected Realize premise in test");
                }
                Premise::Exclude(_) => {
                    panic!("Unexpected Exclude premise in test");
                }
            }
        }
    }

    #[test]
    fn test_person_match_as_statements() {
        let entity = Term::var("entity");
        let person_match = PersonMatch {
            this: entity,
            name: Term::from("John".to_string()),
            age: Term::from(25u32),
        };

        // Test that PersonMatch can be used as Statements
        let statements: Vec<Premise> = person_match.premises().collect();
        assert_eq!(statements.len(), 2);

        // Verify the generated statements
        for statement in statements {
            match statement {
                Premise::Apply(Application::Fact(selector)) => {
                    assert!(selector.parameters().get("the").is_some());
                    assert!(selector.parameters().get("of").is_some());
                    assert!(selector.parameters().get("is").is_some());
                }
                Premise::Apply(Application::Formula(_)) => {
                    panic!("Unexpected ApplyFormula premise in test");
                }
                Premise::Apply(Application::Concept(_)) => {
                    panic!("Unexpected Realize premise in test");
                }
                Premise::Exclude(_) => {
                    panic!("Unexpected Exclude premise in test");
                }
            }
        }
    }

    #[test]
    fn test_usage_pattern() {
        // This test demonstrates the expected usage pattern

        // 1. Create a match pattern with variables
        let entity = Term::var("p");
        let match_pattern = PersonMatch {
            this: entity.clone(),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        // 2. Generate When conditions
        let conditions = Person::when(match_pattern);
        assert_eq!(conditions.len(), 2);
    }
}
