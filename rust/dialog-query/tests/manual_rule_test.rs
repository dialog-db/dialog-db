use dialog_query::application::ConceptApplication;
use dialog_query::artifact::{Entity, Type, Value};
use dialog_query::attribute::Cardinality;
use dialog_query::concept::{Concept, Instance, Match as ConceptMatch};
use dialog_query::dsl::Quarriable;
use dialog_query::rule::Match;
use dialog_query::term::Term;
use dialog_query::{Application, Premise};
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

impl Default for PersonMatch {
    fn default() -> Self {
        Self {
            this: Term::var("this"),
            name: Term::var("name"),
            age: Term::var("age"),
        }
    }
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
    use dialog_query::attribute::AttributeSchema;

    use super::*;

    /// The namespace for Person attributes
    pub const NAMESPACE: &str = "person";

    /// Static attribute definitions
    pub static NAME_ATTR: AttributeSchema<String> = AttributeSchema {
        namespace: NAMESPACE,
        name: "name",
        description: "Name of the person",
        cardinality: Cardinality::One,
        content_type: Some(Type::String),
        marker: PhantomData,
    };

    pub static AGE_ATTR: AttributeSchema<u32> = AttributeSchema {
        namespace: NAMESPACE,
        name: "age",
        description: "Age of the person",
        cardinality: Cardinality::One,
        content_type: Some(Type::UnsignedInt),
        marker: PhantomData,
    };

    /// All attributes as Attribute<Value> for the attributes() method
    pub static ATTRIBUTES: &[AttributeSchema<Value>] = &[
        AttributeSchema {
            namespace: NAMESPACE,
            name: "name",
            description: "Name of the person",
            cardinality: Cardinality::One,
            content_type: Some(Type::String),
            marker: PhantomData,
        },
        AttributeSchema {
            namespace: NAMESPACE,
            name: "age",
            description: "Age of the person",
            cardinality: Cardinality::One,
            content_type: Some(Type::UnsignedInt),
            marker: PhantomData,
        },
    ];

    /// Attribute tuples for the Attributes trait implementation
    pub static ATTRIBUTE_TUPLES: &[(&str, AttributeSchema<Value>)] = &[
        (
            "name",
            AttributeSchema {
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
            AttributeSchema {
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

    const CONCEPT: dialog_query::predicate::concept::Concept = {
        const ATTRS: dialog_query::predicate::concept::Attributes =
            dialog_query::predicate::concept::Attributes::Static(person::ATTRIBUTE_TUPLES);

        dialog_query::predicate::concept::Concept::Static {
            operator: "person",
            attributes: &ATTRS,
        }
    };
}

impl IntoIterator for Person {
    type Item = dialog_query::Relation;
    type IntoIter = std::vec::IntoIter<dialog_query::Relation>;

    fn into_iter(self) -> Self::IntoIter {
        use dialog_query::types::Scalar;

        vec![
            dialog_query::Relation::new(
                "person/name"
                    .parse()
                    .expect("Failed to parse person/name attribute"),
                self.this.clone(),
                self.name.as_value(),
            ),
            dialog_query::Relation::new(
                "person/age"
                    .parse()
                    .expect("Failed to parse person/age attribute"),
                self.this.clone(),
                self.age.as_value(),
            ),
        ]
        .into_iter()
    }
}

impl dialog_query::claim::Claim for Person {
    fn assert(self, transaction: &mut dialog_query::Transaction) {
        use dialog_query::types::Scalar;
        dialog_query::Relation::new(
            "person/name"
                .parse()
                .expect("Failed to parse person/name attribute"),
            self.this.clone(),
            self.name.as_value(),
        )
        .assert(transaction);
        dialog_query::Relation::new(
            "person/age"
                .parse()
                .expect("Failed to parse person/age attribute"),
            self.this.clone(),
            self.age.as_value(),
        )
        .assert(transaction);
    }

    fn retract(self, transaction: &mut dialog_query::Transaction) {
        use dialog_query::types::Scalar;
        dialog_query::Relation::new(
            "person/name"
                .parse()
                .expect("Failed to parse person/name attribute"),
            self.this.clone(),
            self.name.as_value(),
        )
        .retract(transaction);
        dialog_query::Relation::new(
            "person/age"
                .parse()
                .expect("Failed to parse person/age attribute"),
            self.this.clone(),
            self.age.as_value(),
        )
        .retract(transaction);
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

impl From<PersonMatch> for ConceptApplication {
    fn from(source: PersonMatch) -> Self {
        ConceptApplication {
            terms: source.into(),
            concept: Person::CONCEPT,
        }
    }
}

impl From<PersonMatch> for Application {
    fn from(source: PersonMatch) -> Self {
        Application::Concept(source.into())
    }
}

impl From<PersonMatch> for Premise {
    fn from(source: PersonMatch) -> Self {
        Premise::Apply(source.into())
    }
}

impl Instance for Person {
    fn this(&self) -> Entity {
        // For now panic - proper implementation would store entity
        panic!("Instance trait implementation requires an entity field")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_install_rule_api() {
        use dialog_query::{Session, When};
        use dialog_storage::MemoryStorageBackend;

        // Define a rule function using the clean API
        fn person_rule(person: Match<Person>) -> impl When {
            (Match::<Person> {
                this: person.this,
                name: person.name,
                age: person.age,
            },)
        }

        // Create a session
        let storage = MemoryStorageBackend::default();
        let artifacts = dialog_query::artifact::Artifacts::anonymous(storage)
            .await
            .unwrap();

        let result = Session::open(artifacts).install(person_rule);
        assert!(result.is_ok(), "install should work");
    }
}
