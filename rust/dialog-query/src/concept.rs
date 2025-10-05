use crate::application::ConceptApplication;
pub use crate::predicate::concept::{Attributes, ConceptType};
use crate::query::{Output, Source};
use crate::{predicate, QueryError};
use crate::{Application, Premise};
use crate::{Entity, Parameters};
use dialog_artifacts::Instruction;
use dialog_common::ConditionalSend;
use futures_util::StreamExt;

pub type Apply<T: Concept> = T::Match;

/// Concept is a set of attributes associated with entity representing an
/// abstract idea. It is a tool for the domain modeling and in some regard
/// similar to a table in relational database or a collection in the document
/// database, but unlike them it is disconnected from how information is
/// organized, in that sense it is more like view into which you can also insert.
///
/// Concepts are used to describe conclusions of the rules, providing a mapping
/// between conclusions and facts. In that sense you concepts are on-demand
/// cache of all the conclusions from the associated rules.
pub trait Concept: Clone + std::fmt::Debug + predicate::concept::ConceptType {
    type Instance: Instance;
    /// Type representing a query of this concept. It is a set of terms
    /// corresponding to the set of attributes defined by this concept.
    /// It is used as premise of the rule.
    type Match: Match<Concept = Self, Instance = Self::Instance>;

    type Term;
    /// Type representing an assertion of this concept. It is used in the
    /// inductive rules that describe how state of the concept changes
    /// (or persists) over time.
    type Assert;
    /// Type representing a retraction of this concept. It is used in the
    /// inductive rules to describe conditions for the of the concepts lifecycle.
    type Retract;

    fn concept() -> predicate::concept::Concept {
        predicate::concept::Concept {
            operator: Self::operator().into(),
            attributes: Self::attributes().clone(),
        }
    }
}

/// Every assertion or retraction can be decomposed into a set of
/// assertion / retraction.
///
/// This trait enables us to define each Concpet::Assert and Concpet::Retract
/// such that it could be decomposed into a set of instructions which can be
/// then be committed.
pub trait Instructions {
    type IntoIter: IntoIterator<Item = Instruction>;
    fn instructions(self) -> Self::IntoIter;
}

/// Concepts can be matched and this trait describes an abstract match for the
/// concept. Each match should be translatable into a set of statements making
/// it possible to spread it into a query.
pub trait Match: Sized + Clone + ConditionalSend + Into<Parameters> + 'static {
    type Concept: Concept + ConceptType;
    /// Instance of the concept that this match can produce.
    type Instance: Instance + ConditionalSend + Clone;

    fn realize(&self, source: crate::selection::Match) -> Result<Self::Instance, QueryError>;

    fn conpect() -> predicate::Concept {
        use predicate::concept::ConceptType;
        predicate::Concept {
            operator: Self::Concept::operator().into(),
            attributes: Self::Concept::attributes().clone(),
        }
    }

    fn query<S: Source>(&self, source: S) -> impl Output<Self::Instance> {
        let application: ConceptApplication = self.into();
        let cloned = self.clone();
        application
            .query(source)
            .map(move |input| cloned.realize(input?))
    }
}

// Blanket impl for &T -> Parameters that uses the generated From<T> impl
impl<T: Match + Clone> From<&T> for Parameters {
    fn from(source: &T) -> Self {
        source.clone().into()
    }
}

impl<T: Match + Clone> From<&T> for Premise {
    fn from(source: &T) -> Self {
        Premise::Apply(source.into())
    }
}

impl<T: Match + Clone> From<&T> for Application {
    fn from(source: &T) -> Self {
        Application::Concept(source.into())
    }
}

impl<T: Match + Clone> From<&T> for ConceptApplication {
    fn from(source: &T) -> Self {
        ConceptApplication {
            terms: source.into(),
            concept: T::conpect(),
        }
    }
}

/// Describes an instance of a concept. It is expected that each concept is
/// can be materialized from the selection::Match.
pub trait Instance: ConditionalSend {
    /// Each instance has a corresponding entity and this method
    /// returns a reference to it.
    fn this(&self) -> Entity;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::artifact::Value;
    use crate::artifact::{Artifacts, Attribute as ArtifactAttribute};
    use crate::concept::Concept;
    use crate::selection::SelectionExt;
    use crate::term::Term;
    use crate::Fact;
    use crate::Session;
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    // Define a Person concept for testing using raw concept API
    // This mirrors what the #[derive(Rule)] macro generates
    #[derive(Debug, Clone)]
    struct Person {
        pub this: Entity,
        pub name: String,
        pub age: u32,
    }

    // PersonMatch for querying - contains Term-wrapped fields
    // Macro generates typed Terms (Term<String>, Term<u32>) not Term<Value>
    #[derive(Debug, Clone)]
    struct PersonMatch {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub age: Term<u32>,
    }

    struct PersonTerms;

    impl PersonTerms {
        pub fn this() -> Term<Entity> {
            Term::<Entity>::var("this")
        }
        pub fn name() -> Term<String> {
            Term::<String>::var("name")
        }
        pub fn age() -> Term<u32> {
            Term::<u32>::var("age")
        }
    }

    // PersonAssert for assertions - uses typed Terms, no 'this' field
    #[derive(Debug, Clone)]
    struct PersonAssert {
        pub name: Term<String>,
        pub age: Term<u32>,
    }

    // PersonRetract for retractions - uses typed Terms, no 'this' field
    #[derive(Debug, Clone)]
    struct PersonRetract {
        pub name: Term<String>,
        pub age: Term<u32>,
    }

    // Implement ConceptType for Person
    impl predicate::concept::ConceptType for Person {
        fn operator() -> &'static str {
            "person"
        }

        fn attributes() -> &'static predicate::concept::Attributes {
            use crate::artifact::{Type, Value};
            use crate::attribute::{Attribute, Cardinality};
            use std::marker::PhantomData;

            static ATTRIBUTE_TUPLES: &[(&str, Attribute<Value>)] = &[
                (
                    "name",
                    Attribute {
                        namespace: "person",
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
                        namespace: "person",
                        name: "age",
                        description: "Age of the person",
                        cardinality: Cardinality::One,
                        content_type: Some(Type::UnsignedInt),
                        marker: PhantomData,
                    },
                ),
            ];

            static ATTRS: predicate::concept::Attributes =
                predicate::concept::Attributes::Static(ATTRIBUTE_TUPLES);
            &ATTRS
        }
    }

    // Implement Concept for Person
    impl Concept for Person {
        type Instance = Person;
        type Match = PersonMatch;
        type Assert = PersonAssert;
        type Retract = PersonRetract;
        type Term = PersonTerms;
    }

    // Implement TryFrom<selection::Match> for Person
    // This extracts values from the match by field name
    impl TryFrom<crate::selection::Match> for Person {
        type Error = crate::error::InconsistencyError;

        fn try_from(input: crate::selection::Match) -> Result<Self, Self::Error> {
            Ok(Person {
                this: input.get(&<Self as Concept>::Term::this())?,
                name: input.get(&<Self as Concept>::Term::name())?,
                age: input.get(&<Self as Concept>::Term::age())?,
            })
        }
    }

    // Implement Instance for Person
    impl Instance for Person {
        fn this(&self) -> Entity {
            self.this.clone()
        }
    }

    // Implement From<PersonMatch> for Parameters
    // This mirrors what the macro generates
    impl From<PersonMatch> for Parameters {
        fn from(source: PersonMatch) -> Self {
            use crate::types::Scalar;
            let mut terms = Self::new();

            // Convert this field: Term<Entity> -> Term<Value>
            let this_term = match source.this {
                Term::Variable { name, .. } => Term::Variable {
                    name: name.clone(),
                    content_type: Default::default(),
                },
                Term::Constant(entity) => Term::Constant(Value::Entity(entity)),
            };
            terms.insert("this".into(), this_term);

            // Convert attribute fields: Term<T> -> Term<Value> using Scalar::as_value()
            let name_term = match source.name {
                Term::Variable { name, .. } => Term::Variable {
                    name: name.clone(),
                    content_type: Default::default(),
                },
                Term::Constant(value) => Term::Constant(Scalar::as_value(&value)),
            };
            terms.insert("name".into(), name_term);

            let age_term = match source.age {
                Term::Variable { name, .. } => Term::Variable {
                    name: name.clone(),
                    content_type: Default::default(),
                },
                Term::Constant(value) => Term::Constant(Scalar::as_value(&value)),
            };
            terms.insert("age".into(), age_term);

            terms
        }
    }

    // Implement Match for PersonMatch
    impl Match for PersonMatch {
        type Concept = Person;
        type Instance = Person;

        fn realize(
            &self,
            source: crate::selection::Match,
        ) -> std::result::Result<Self::Instance, QueryError> {
            Ok(Self::Instance {
                this: source.get(&self.this)?,
                name: source.get(&self.name)?,
                age: source.get(&self.age)?,
            })
        }
    }

    // TODO: Fix FactSelector vs FactApplication mismatch
    // Implement Rule for Person to support when() method
    // impl crate::rule::Rule for Person {
    //     fn when(terms: Self::Match) -> crate::rule::When {
    //         use crate::fact_selector::FactSelector;
    //
    //         // Create fact selectors for each attribute
    //         let name_selector = FactSelector::<Value> {
    //             the: Some(Term::from(
    //                 "person/name".parse::<crate::artifact::Attribute>().unwrap(),
    //             )),
    //             of: Some(terms.this.clone()),
    //             is: Some(terms.name),
    //             fact: None,
    //         };
    //
    //         let age_selector = FactSelector::<Value> {
    //             the: Some(Term::from(
    //                 "person/age".parse::<crate::artifact::Attribute>().unwrap(),
    //             )),
    //             of: Some(terms.this),
    //             is: Some(terms.age),
    //             fact: None,
    //         };
    //
    //         [name_selector, age_selector].into()
    //     }
    // }

    // Implement Premises for PersonMatch
    // TODO: Fix after Rule trait is properly implemented
    // impl crate::rule::Premises for PersonMatch {
    //     type IntoIter = std::vec::IntoIter<crate::premise::Premise>;

    //     fn premises(self) -> Self::IntoIter {
    //         use crate::rule::Rule;
    //         Person::when(self).into_iter()
    //     }
    // }

    // TODO: The old Attributes trait no longer exists - it was replaced by ConceptType
    // Commenting out this implementation for now
    // impl Attributes for PersonAttributes {
    //     fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
    //         use std::sync::LazyLock;
    //
    //         static PERSON_ATTRIBUTES: LazyLock<[(&'static str, Attribute<Value>); 2]> =
    //             LazyLock::new(|| {
    //                 [
    //                     (
    //                         "name",
    //                         Attribute::new("person", "name", "Person's name", Type::String),
    //                     ),
    //                     (
    //                         "age",
    //                         Attribute::new("person", "age", "Person's age", Type::UnsignedInt),
    //                     ),
    //                 ]
    //             });
    //         &*PERSON_ATTRIBUTES
    //     }
    //
    //     fn of<T: Into<Term<Entity>>>(entity: T) -> Self {
    //         PersonAttributes {
    //             entity: entity.into(),
    //         }
    //     }
    // }

    #[test]
    fn test_person_concept_creation() {
        use predicate::concept::ConceptType;

        // Test that the Person concept has the expected properties
        assert_eq!(Person::operator(), "person");

        // Test Person has 2 attributes (name and age)
        let attributes = Person::attributes();
        assert_eq!(attributes.count(), 2);

        // Verify attribute names
        let attr_names: Vec<&str> = attributes.iter().map(|(name, _)| name).collect();
        assert!(attr_names.contains(&"name"));
        assert!(attr_names.contains(&"age"));
    }

    #[test]
    fn test_person_match_creation() {
        // Test creating a PersonMatch for querying
        let entity_var = Term::var("person_entity");
        let name_var = Term::var("person_name");
        let age_var = Term::var("person_age");

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_var.clone(),
            age: age_var.clone(),
        };

        // Test that fields are accessible
        assert_eq!(person_match.this, entity_var);
        assert_eq!(person_match.name, name_var);
        assert_eq!(person_match.age, age_var);
    }

    #[test]
    fn test_person_match_with_constants() {
        // Test querying for a specific person with constant values
        let entity_var = Term::var("alice_entity");
        let name_const = Term::from("Alice".to_string());
        let age_const = Term::from(30u32);

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_const.clone(),
            age: age_const.clone(),
        };

        // Verify the constants are preserved
        assert_eq!(person_match.name, name_const);
        assert_eq!(person_match.age, age_const);

        // Test that constants are properly identified
        assert!(person_match.name.is_constant());
        assert!(person_match.age.is_constant());
    }

    #[test]
    fn test_person_match_mixed_terms() {
        // Test mixing variables and constants in a match pattern
        let entity_var = Term::var("person_entity");
        let name_const = Term::from("Bob".to_string());
        let age_var = Term::var("any_age");

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_const.clone(),
            age: age_var.clone(),
        };

        // Name should be constant, age should be variable
        assert!(person_match.name.is_constant());
        assert!(person_match.age.is_variable());
        assert_eq!(person_match.age.name(), Some("any_age"));
    }

    #[test]
    fn test_person_instance_creation() {
        // Test creating a Person instance
        let entity = Entity::new().unwrap();
        let person = Person {
            this: entity.clone(),
            name: "Charlie".to_string(),
            age: 25,
        };

        // Test Instance trait - should return the same entity
        assert_eq!(person.this(), entity);
    }

    #[test]
    fn test_concept_name_consistency() {
        // Test that concept name is consistent across different access patterns
        assert_eq!(Person::operator(), "person");

        // The concept should have consistent naming
        let _person = Person {
            this: Entity::new().unwrap(),
            name: "Test".to_string(),
            age: 1,
        };

        // Instance should have the same concept name
        // (though our current Instance impl doesn't store concept info)
        assert_eq!(Person::operator(), "person");
    }

    #[test]
    #[ignore] // TODO: Fix after Premises trait is properly implemented - test body commented out to allow compilation
    fn test_match_premise_planning() {
        // Test body commented out due to Premises trait not being implemented
        /*
        // Test that PersonMatch can be used through Premises trait
        use crate::rule::Premises;

        // Create a PersonMatch with all constants to avoid dependency resolution issues
        let entity_const = Term::from(Entity::new().unwrap());
        let name_const = Term::from(Value::String("Alice".to_string()));
        let age_const = Term::from(Value::UnsignedInt(30));

        let person_match = PersonMatch {
            this: entity_const,
            name: name_const,
            age: age_const,
        };

        // PersonMatch should implement Premises trait and generate individual premises
        let premises: Vec<_> = person_match.clone().premises().collect();

        // Should have premises for each attribute (name and age)
        assert_eq!(premises.len(), 2);

        // Test that PersonMatch correctly implements Match trait methods
        assert_eq!(person_match.term_for("name").unwrap().is_constant(), true);
        assert_eq!(person_match.term_for("age").unwrap().is_constant(), true);
        */
    }

    #[test]
    fn test_person_match_fields() {
        // Test that PersonMatch has the expected fields
        let entity_var = Term::var("entity");
        let name_var = Term::var("name");
        let age_var = Term::var("age");

        let person_match = PersonMatch {
            this: entity_var.clone(),
            name: name_var.clone(),
            age: age_var.clone(),
        };

        // Should have this, name, and age fields
        assert_eq!(person_match.this, entity_var);
        assert_eq!(person_match.name, name_var);
        assert_eq!(person_match.age, age_var);
    }

    #[test]
    fn test_concept_debug_output() {
        // Test that our derived Debug implementations work
        let person = Person {
            this: Entity::new().unwrap(),
            name: "Debug Test".to_string(),
            age: 42,
        };

        let debug_output = format!("{:?}", person);
        assert!(debug_output.contains("Person"));
        assert!(debug_output.contains("Debug Test"));
        assert!(debug_output.contains("42"));
    }

    #[test]
    fn test_concept_clone() {
        // Test that our derived Clone implementations work
        let entity = Entity::new().unwrap();
        let person1 = Person {
            this: entity.clone(),
            name: "Original".to_string(),
            age: 35,
        };

        let person2 = person1.clone();
        assert_eq!(person1.this, person2.this);
        assert_eq!(person1.name, person2.name);
        assert_eq!(person1.age, person2.age);

        // Test PersonMatch clone
        let entity_var = Term::var("entity");
        let match1 = PersonMatch {
            this: entity_var.clone(),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        let match2 = match1.clone();
        assert_eq!(match1.this, match2.this);
        assert_eq!(match1.name, match2.name);
        assert_eq!(match1.age, match2.age);
    }

    #[tokio::test]
    #[ignore] // Legacy manual concept implementation - needs migration to new API
    async fn test_person_match_query() -> Result<()> {
        // Test that actually uses PersonMatch to query - this should work with the concept system

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        // Create test data
        let facts = vec![
            Fact::assert(
                "person/name".parse::<ArtifactAttribute>()?,
                alice.clone(),
                Value::String("Alice".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<ArtifactAttribute>()?,
                alice.clone(),
                Value::UnsignedInt(25),
            ),
            Fact::assert(
                "person/name".parse::<ArtifactAttribute>()?,
                bob.clone(),
                Value::String("Bob".to_string()),
            ),
            Fact::assert(
                "person/age".parse::<ArtifactAttribute>()?,
                bob.clone(),
                Value::UnsignedInt(30),
            ),
            Fact::assert(
                "person/name".parse::<ArtifactAttribute>()?,
                mallory.clone(),
                Value::String("Mallory".to_string()),
            ),
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // This is the real test - using PersonMatch to query for people
        let person_query = PersonMatch {
            this: Term::var("person"),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        // This should work with the planner fix
        let session = Session::open(artifacts);
        let results = Output::try_collect(person_query.query(session)).await?;

        // Should find both Alice and Bob (not Mallory who has no age)
        assert_eq!(results.len(), 2, "Should find both people");

        // Verify we got the right people
        // assert!(results.contains_binding("name", &Value::String("Alice".to_string())));
        // assert!(results.contains_binding("name", &Value::String("Bob".to_string())));
        // assert!(results.contains_binding("age", &Value::UnsignedInt(25)));
        // assert!(results.contains_binding("age", &Value::UnsignedInt(30)));

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_match_structure() -> Result<()> {
        // Test that PersonMatch correctly implements the Match trait
        // This doesn't require actual querying, just tests the structure

        let alice = Entity::new()?;

        // Test 1: Create a PersonMatch with mixed terms
        let person_match = PersonMatch {
            this: Term::from(alice.clone()),
            name: Term::from("Alice".to_string()),
            age: Term::var("age"),
        };

        // Test that we can convert to Parameters
        let params: Parameters = person_match.clone().into();
        assert!(params.get("this").is_some());
        assert!(params.get("name").is_some());
        assert!(params.get("age").is_some());

        // Test 2: Verify concept attributes are accessible
        let attrs = Person::attributes();
        assert_eq!(attrs.count(), 2); // name and age

        // Verify we can find specific attributes
        let name_attr = attrs.iter().find(|(name, _)| *name == "name");
        assert!(name_attr.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_query_no_matches() -> Result<()> {
        // Test that individual fact selectors work for non-matching queries

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        // Create minimal test data
        let facts = vec![Fact::assert(
            "person/name".parse::<ArtifactAttribute>()?,
            alice.clone(),
            Value::String("Alice".to_string()),
        )];

        let mut session = Session::open(artifacts.clone());
        session.transact(facts).await?;

        // Test: Search for non-existent person using individual fact selector
        let missing_query = Fact::<Value>::select()
            .the("person/name")
            .of(Term::var("person"))
            .is(Value::String("NonExistent".to_string()))
            .build()?;

        let session = Session::open(artifacts);
        let no_results = missing_query.query(&session)?.collect_set().await?;
        assert_eq!(no_results.len(), 0, "Should find no non-existent people");

        Ok(())
    }
}
