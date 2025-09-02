use dialog_query::artifact::{Entity, Value, ValueDataType};
use dialog_query::attribute::{Attribute, Cardinality, Match};
use dialog_query::concept::{Attributes, Concept, Instance, Instructions, Match as ConceptMatch};
use dialog_query::fact_selector::FactSelector;
use dialog_query::rule::{Rule, Statements, When};
use dialog_query::statement::Statement;
use dialog_query::term::Term;
use dialog_query::types::Scalar;
use std::marker::PhantomData;

/// Manual implementation of Person struct with Concept and Rule traits
/// This serves as a template for what the derive macro should generate
#[derive(Debug, Clone)]
pub struct Person {
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
pub struct PersonAttributes {
    pub this: Term<Entity>,
    pub name: Match<String>,
    pub age: Match<u32>,
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
        data_type: ValueDataType::String,
        marker: PhantomData,
    };

    pub static AGE_ATTR: Attribute<u32> = Attribute {
        namespace: NAMESPACE,
        name: "age",
        description: "Age of the person",
        cardinality: Cardinality::One,
        data_type: ValueDataType::UnsignedInt,
        marker: PhantomData,
    };

    /// All attributes as Attribute<Value> for the attributes() method
    pub static ATTRIBUTES: &[Attribute<Value>] = &[
        Attribute {
            namespace: NAMESPACE,
            name: "name",
            description: "Name of the person",
            cardinality: Cardinality::One,
            data_type: ValueDataType::String,
            marker: PhantomData,
        },
        Attribute {
            namespace: NAMESPACE,
            name: "age",
            description: "Age of the person",
            cardinality: Cardinality::One,
            data_type: ValueDataType::UnsignedInt,
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
                data_type: ValueDataType::String,
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
                data_type: ValueDataType::UnsignedInt,
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
    type Attributes = PersonAttributes;

    fn name() -> &'static str {
        "person"
    }

    fn r#match<T: Into<Term<Entity>>>(this: T) -> Self::Attributes {
        let entity = this.into();
        PersonAttributes {
            this: entity.clone(),
            name: person::NAME_ATTR.of(entity.clone()),
            age: person::AGE_ATTR.of(entity),
        }
    }
}

impl ConceptMatch for PersonMatch {
    type Instance = Person;
    type Attributes = PersonAttributes;

    fn term_for(&self, _name: &str) -> Option<&Term<Value>> {
        // For now return None - proper implementation would need term conversion
        None
    }

    fn this(&self) -> Term<Entity> {
        self.this.clone()
    }
}

impl Attributes for PersonAttributes {
    fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
        person::ATTRIBUTE_TUPLES
    }
}

impl Instance for Person {
    fn this(&self) -> Entity {
        // For now panic - proper implementation would store entity
        panic!("Instance trait implementation requires an entity field")
    }
}

impl Statements for PersonMatch {
    type IntoIter = std::vec::IntoIter<Statement>;

    fn statements(self) -> Self::IntoIter {
        Person::when(self).into_iter()
    }
}

impl Rule for Person {
    fn when(terms: Self::Match) -> When {
        // Create fact selectors for each attribute
        // We need to convert the typed terms to Term<Value>
        let name_value_term = match &terms.name {
            Term::Variable { name, .. } => Term::Variable {
                name: name.clone(),
                _type: Default::default(),
            },
            Term::Constant(value) => Term::Constant(value.as_value()),
        };

        let age_value_term = match &terms.age {
            Term::Variable { name, .. } => Term::Variable {
                name: name.clone(),
                _type: Default::default(),
            },
            Term::Constant(value) => Term::Constant(value.as_value()),
        };

        let name_selector = FactSelector::<Value> {
            the: Some(Term::from(
                "person/name"
                    .parse::<dialog_artifacts::Attribute>()
                    .unwrap(),
            )),
            of: Some(terms.this.clone()),
            is: Some(name_value_term),
            fact: None,
        };

        let age_selector = FactSelector::<Value> {
            the: Some(Term::from(
                "person/age".parse::<dialog_artifacts::Attribute>().unwrap(),
            )),
            of: Some(terms.this.clone()),
            is: Some(age_value_term),
            fact: None,
        };

        // Return When collection with both selectors
        [name_selector, age_selector].into()
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
    fn test_manual_person_implementation() {
        // Test that Person implements Concept
        assert_eq!(Person::name(), "person");

        // Test attributes metadata using PersonAttributes
        let attrs = PersonAttributes::attributes();
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].0, "name");
        assert_eq!(attrs[1].0, "age");
        assert_eq!(attrs[1].1.cardinality, Cardinality::One);

        // Test that Attribute<Value> has data_type() method
        // Now returns the stored data_type field
        assert_eq!(attrs[0].1.data_type(), Some(ValueDataType::String));
        assert_eq!(attrs[1].1.data_type(), Some(ValueDataType::UnsignedInt));
    }

    #[test]
    fn test_person_match_creation() {
        let entity = Term::var("person_entity");
        let attributes = Person::r#match(entity.clone());

        // Verify the attributes object is created correctly
        assert_eq!(attributes.name.the(), "person/name");
        assert_eq!(attributes.age.the(), "person/age");

        // Test fluent API with is()
        let name_query = attributes.name.is("Alice");
        assert_eq!(
            name_query
                .the
                .as_ref()
                .map(|t| t.as_constant().cloned())
                .flatten(),
            Some(
                "person/name"
                    .parse::<dialog_artifacts::Attribute>()
                    .unwrap()
            )
        );
        assert!(name_query.is.is_some());

        let age_query = attributes.age.is(30u32);
        assert_eq!(
            age_query
                .the
                .as_ref()
                .map(|t| t.as_constant().cloned())
                .flatten(),
            Some("person/age".parse::<dialog_artifacts::Attribute>().unwrap())
        );
        assert!(age_query.is.is_some());
    }

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
                Statement::Select(selector) => {
                    assert!(selector.the.is_some());
                    assert!(selector.of.is_some());
                    assert!(selector.is.is_some());
                    assert!(selector.fact.is_none());
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
        let statements: Vec<Statement> = person_match.statements().collect();
        assert_eq!(statements.len(), 2);

        // Verify the generated statements
        for statement in statements {
            match statement {
                Statement::Select(selector) => {
                    assert!(selector.the.is_some());
                    assert!(selector.of.is_some());
                    assert!(selector.is.is_some());
                }
            }
        }
    }

    #[test]
    fn test_attribute_value_pattern() {
        // This test demonstrates the Attribute<Value> pattern after reversion
        let attrs = PersonAttributes::attributes(); // Returns &[(&str, Attribute<Value>)]

        for attr in attrs {
            // Attributes now have data_type() return the stored type
            assert!(attr.1.data_type().is_some());

            // We can still access all the basic attribute properties
            assert_eq!(attr.1.namespace, "person");
            assert!(attr.0 == "name" || attr.0 == "age");
            assert!(attr.1.description.contains("person"));
            println!("Found attribute: {}/{}", attr.1.namespace, attr.1.name);
        }

        // Verify we can find attributes by name
        let name_attr = attrs.iter().find(|attr| attr.0 == "name").unwrap();
        assert_eq!(name_attr.1.name, "name");
        assert_eq!(name_attr.1.description, "Name of the person");

        let age_attr = attrs.iter().find(|attr| attr.0 == "age").unwrap();
        assert_eq!(age_attr.1.name, "age");
        assert_eq!(age_attr.1.description, "Age of the person");
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

        // 3. Alternative: Use the fluent API
        let query = Person::r#match(Term::var("person"));
        let name_selector = query.name.is("Alice");
        let age_selector = query.age.is(30u32);

        // Both selectors should be properly formed
        assert!(name_selector.the.is_some());
        assert!(age_selector.the.is_some());
    }
}
