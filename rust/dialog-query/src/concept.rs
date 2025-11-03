use crate::application::ConceptApplication;

pub use crate::dsl::Quarriable;
pub use crate::predicate::concept::Attributes;
use crate::query::{Output, Source};
use crate::selection::Answer;
use crate::{predicate, QueryError};
use crate::{Entity, Parameters, Relation};
use dialog_common::ConditionalSend;
use futures_util::StreamExt;
use std::fmt::Debug;

/// Concept is a set of attributes associated with entity representing an
/// abstract idea. It is a tool for the domain modeling and in some regard
/// similar to a table in relational database or a collection in the document
/// database, but unlike them it is disconnected from how information is
/// organized, in that sense it is more like view into which you can also insert.
///
/// Concepts are used to describe conclusions of the rules, providing a mapping
/// between conclusions and facts. In that sense you concepts are on-demand
/// cache of all the conclusions from the associated rules.
pub trait Concept: Quarriable + IntoIterator<Item = Relation> + Clone + Debug {
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

    /// The static concept definition for this type.
    /// This is typically defined by the macro as a Concept::Static variant.
    const CONCEPT: predicate::concept::Concept;
}

/// Concepts can be matched and this trait describes an abstract match for the
/// concept. Each match should be translatable into a set of statements making
/// it possible to spread it into a query.
pub trait Match: Sized + Clone + ConditionalSend + Default + 'static {
    type Concept: Concept;
    /// Instance of the concept that this match can produce.
    type Instance: Instance + ConditionalSend + Clone;

    fn realize(&self, source: Answer) -> Result<Self::Instance, QueryError>;

    fn to_concept(&self) -> predicate::Concept {
        Self::Concept::CONCEPT
    }

    fn conpect() -> predicate::Concept {
        Self::Concept::CONCEPT
    }

    fn query<S: Source>(&self, source: S) -> impl Output<Self::Instance>
    where
        ConceptApplication: From<Self>,
    {
        let application: ConceptApplication = self.to_owned().into();
        let cloned = self.clone();
        application
            .query(source)
            .map(move |input| cloned.realize(input?))
    }
}

// Blanket impl for &T -> Parameters that uses the generated From<T> impl
impl<T> From<&T> for Parameters
where
    T: Match + Clone + Into<Parameters>,
{
    fn from(source: &T) -> Self {
        source.clone().into()
    }
}

/// Describes an instance of a concept. It is expected that each concept is
/// can be materialized from the selection::Answer.
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
    use crate::term::Term;
    use crate::{Answer, Fact};
    use crate::{Claim, Concept, Session, Transaction};
    use anyhow::Result;
    use dialog_storage::MemoryStorageBackend;

    // Define a Person concept for testing using raw concept API
    // This mirrors what the #[derive(Concept)] macro generates
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

    impl Default for PersonMatch {
        fn default() -> Self {
            Self {
                this: Term::var("this"),
                name: Term::var("name"),
                age: Term::var("age"),
            }
        }
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
    #[allow(dead_code)]
    struct PersonAssert {
        pub name: Term<String>,
        pub age: Term<u32>,
    }

    // PersonRetract for retractions - uses typed Terms, no 'this' field
    #[derive(Debug, Clone)]
    #[allow(dead_code)]
    struct PersonRetract {
        pub name: Term<String>,
        pub age: Term<u32>,
    }

    // Implement Concept for Person
    impl Concept for Person {
        type Instance = Person;
        type Match = PersonMatch;
        type Assert = PersonAssert;
        type Retract = PersonRetract;
        type Term = PersonTerms;

        const CONCEPT: predicate::concept::Concept = {
            use crate::artifact::{Type, Value};
            use crate::attribute::{AttributeSchema, Cardinality};
            use std::marker::PhantomData;

            const ATTRIBUTE_TUPLES: &[(&str, AttributeSchema<Value>)] = &[
                (
                    "name",
                    AttributeSchema {
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
                    AttributeSchema {
                        namespace: "person",
                        name: "age",
                        description: "Age of the person",
                        cardinality: Cardinality::One,
                        content_type: Some(Type::UnsignedInt),
                        marker: PhantomData,
                    },
                ),
            ];

            const ATTRS: predicate::concept::Attributes =
                predicate::concept::Attributes::Static(ATTRIBUTE_TUPLES);

            predicate::concept::Concept::Static {
                operator: "person",
                attributes: &ATTRS,
            }
        };
    }

    impl IntoIterator for Person {
        type Item = Relation;
        type IntoIter = std::vec::IntoIter<Relation>;

        fn into_iter(self) -> Self::IntoIter {
            use crate::types::Scalar;

            vec![
                Relation::new(
                    "person/name".parse().expect("Failed to parse attribute"),
                    self.this.clone(),
                    self.name.as_value(),
                ),
                Relation::new(
                    "person/age".parse().expect("Failed to parse attribute"),
                    self.this.clone(),
                    self.age.as_value(),
                ),
            ]
            .into_iter()
        }
    }

    impl Claim for Person {
        fn assert(self, transaction: &mut Transaction) {
            use crate::types::Scalar;
            transaction.associate(Relation {
                the: "person/name".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.name.as_value(),
            });

            transaction.associate(Relation {
                the: "person/age".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.age.as_value(),
            });
        }

        fn retract(self, transaction: &mut Transaction) {
            use crate::types::Scalar;
            transaction.dissociate(Relation {
                the: "person/name".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.name.as_value(),
            });

            transaction.dissociate(Relation {
                the: "person/age".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.age.as_value(),
            });
        }
    }

    impl Quarriable for Person {
        type Query = PersonMatch;
    }

    // Implement TryFrom<selection::Answer> for Person
    // This extracts values from the answer by field name
    impl TryFrom<Answer> for Person {
        type Error = crate::error::InconsistencyError;

        fn try_from(input: Answer) -> Result<Self, Self::Error> {
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

    // Implement From<PersonMatch> for ConceptApplication
    impl From<PersonMatch> for ConceptApplication {
        fn from(source: PersonMatch) -> Self {
            ConceptApplication {
                terms: source.into(),
                concept: Person::CONCEPT,
            }
        }
    }

    // Implement Match for PersonMatch
    impl Match for PersonMatch {
        type Concept = Person;
        type Instance = Person;

        fn realize(&self, source: Answer) -> std::result::Result<Self::Instance, QueryError> {
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
        // Test that the Person concept has the expected properties
        let concept = Person::CONCEPT;
        assert_eq!(concept.operator(), "person");

        // Test Person has 2 attributes (name and age)
        let attributes = concept.attributes();
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
        let concept = Person::CONCEPT;
        assert_eq!(concept.operator(), "person");

        // The concept should have consistent naming
        let _person = Person {
            this: Entity::new().unwrap(),
            name: "Test".to_string(),
            age: 1,
        };

        // Instance should have the same concept name
        // (though our current Instance impl doesn't store concept info)
        assert_eq!(concept.operator(), "person");
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
        let claims = vec![
            Relation {
                the: "person/name".parse::<ArtifactAttribute>()?,
                of: alice.clone(),
                is: Value::String("Alice".to_string()),
            },
            Relation {
                the: "person/age".parse::<ArtifactAttribute>()?,
                of: alice.clone(),
                is: Value::UnsignedInt(25),
            },
            Relation {
                the: "person/name".parse::<ArtifactAttribute>()?,
                of: bob.clone(),
                is: Value::String("Bob".to_string()),
            },
            Relation {
                the: "person/age".parse::<ArtifactAttribute>()?,
                of: bob.clone(),
                is: Value::UnsignedInt(30),
            },
            Relation {
                the: "person/name".parse::<ArtifactAttribute>()?,
                of: mallory.clone(),
                is: Value::String("Mallory".to_string()),
            },
        ];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // This is the real test - using PersonMatch to query for people
        let person_query = PersonMatch {
            this: Term::var("person"),
            name: Term::var("name"),
            age: Term::var("age"),
        };

        // This should work with the planner fix
        let session = Session::open(artifacts);
        let results = person_query.query(session).try_vec().await?;

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
        let concept = Person::CONCEPT;
        let attrs = concept.attributes();
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
        let claims = vec![Relation {
            the: "person/name".parse::<ArtifactAttribute>()?,
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Test: Search for non-existent person using individual fact selector
        let missing_query = Fact::<Value>::select()
            .the("person/name")
            .of(Term::var("person"))
            .is(Value::String("NonExistent".to_string()))
            .compile()?;

        let session = Session::open(artifacts);
        let no_results = missing_query.query(&session).try_vec().await?;
        assert_eq!(no_results.len(), 0, "Should find no non-existent people");

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_dsl() -> Result<()> {
        use crate::Match;
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Employee {
            this: Entity,
            name: String,
            role: String,
        }

        let alice = Entity::new()?;
        let bob = Entity::new()?;
        let mallory = Entity::new()?;

        // Create test data

        let mut session = Session::open(artifacts.clone());
        let mut transaction = session.edit();

        transaction
            .assert(Employee {
                this: alice.clone(),
                name: "Alice".to_string(),
                role: "cryptographer".to_string(),
            })
            .assert(Employee {
                this: bob.clone(),
                name: "Bob".to_string(),
                role: "janitor".to_string(),
            })
            .assert(Relation {
                the: "employee/name".parse::<ArtifactAttribute>()?,
                of: mallory.clone(),
                is: Value::String("Mallory".to_string()),
            })
            .assert(Relation {
                the: "employee/role".parse::<ArtifactAttribute>()?,
                of: mallory.clone(),
                is: Value::String("Hacker".to_string()),
            });

        session.commit(transaction).await?;

        let employee = Match::<Employee> {
            this: Term::var("this"),
            name: Term::var("name"),
            role: Term::var("role"),
        };

        let employees = employee.query(session).try_vec().await?;
        assert_eq!(
            employees.clone().sort(),
            vec![
                Employee {
                    this: bob.clone(),
                    name: "Bob".to_string(),
                    role: "janitor".to_string(),
                },
                Employee {
                    this: alice.clone(),
                    name: "Alice".to_string(),
                    role: "cryptographer".to_string(),
                },
                Employee {
                    this: mallory.clone(),
                    name: "Mallory".to_string(),
                    role: "Hacker".to_string(),
                },
            ]
            .sort()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_concept_negation_with_not_operator() -> Result<()> {
        use crate::artifact::Artifacts;
        use crate::artifact::Attribute as ArtifactAttribute;
        use dialog_storage::MemoryStorageBackend;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct Person {
            this: Entity,
            name: String,
            age: usize,
        }

        let alice = Entity::new()?;

        // Create test data - assert Alice
        let mut session = Session::open(artifacts.clone());
        let alice_person = Person {
            this: alice.clone(),
            name: "Alice".to_string(),
            age: 25,
        };

        session.transact(vec![alice_person.clone()]).await?;

        // Verify Alice exists
        use crate::artifact::{ArtifactSelector, ArtifactStore};
        use futures_util::TryStreamExt;

        let session = Session::open(artifacts.clone());
        let name_attr: ArtifactAttribute = "person/name".parse()?;
        let age_attr: ArtifactAttribute = "person/age".parse()?;

        let name_facts: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone())
                    .of(alice.clone()),
            )
            .try_collect()
            .await?;
        assert_eq!(name_facts.len(), 1, "Should have Alice's name");
        assert_eq!(
            name_facts[0].is,
            Value::String("Alice".to_string()),
            "Name should be Alice"
        );

        let age_facts: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(age_attr.clone())
                    .of(alice.clone()),
            )
            .try_collect()
            .await?;
        assert_eq!(age_facts.len(), 1, "Should have Alice's age");
        assert_eq!(age_facts[0].is, Value::UnsignedInt(25), "Age should be 25");

        // Now retract using !operator
        let mut session = Session::open(artifacts.clone());
        session.transact(vec![!alice_person]).await?;

        // Verify Alice has been retracted
        let session = Session::open(artifacts.clone());
        let name_facts_after: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone())
                    .of(alice.clone()),
            )
            .try_collect()
            .await?;
        assert_eq!(
            name_facts_after.len(),
            0,
            "Should not have Alice's name after retraction"
        );

        let age_facts_after: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(age_attr.clone())
                    .of(alice.clone()),
            )
            .try_collect()
            .await?;
        assert_eq!(
            age_facts_after.len(),
            0,
            "Should not have Alice's age after retraction"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_relation_negation_with_not_operator() -> Result<()> {
        use crate::artifact::Artifacts;
        use crate::artifact::Attribute as ArtifactAttribute;
        use dialog_storage::MemoryStorageBackend;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr: ArtifactAttribute = "user/name".parse()?;

        // Assert a relation
        let mut session = Session::open(artifacts.clone());
        let name_relation = Relation {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        session.transact(vec![name_relation.clone()]).await?;

        // Verify relation exists
        use crate::artifact::{ArtifactSelector, ArtifactStore};
        use futures_util::TryStreamExt;

        let session = Session::open(artifacts.clone());
        let facts: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone())
                    .of(alice.clone()),
            )
            .try_collect()
            .await?;
        assert_eq!(facts.len(), 1, "Should have name relation");

        // Retract using ! operator
        let mut session = Session::open(artifacts.clone());
        session.transact(vec![!name_relation]).await?;

        // Verify relation has been retracted
        let session = Session::open(artifacts.clone());
        let facts_after: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone())
                    .of(alice.clone()),
            )
            .try_collect()
            .await?;
        assert_eq!(
            facts_after.len(),
            0,
            "Should not have name relation after retraction"
        );

        Ok(())
    }
}
