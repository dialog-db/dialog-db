/// Concept application for querying entities that match a concept pattern.
pub mod application;
/// Concept descriptors for entity-centric queries.
pub mod descriptor;

pub use application::ConceptQuery;
pub use descriptor::ConceptDescriptor;

#[cfg(test)]
use crate::Association;
pub use crate::predicate::Predicate;
#[cfg(test)]
use crate::query::Output;
use crate::{Entity, Parameters};
use dialog_common::ConditionalSend;
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
///
/// Note: IntoIterator is not a bound on this trait to allow attributes to
/// implement Concept by delegating to their instance types (e.g., Title
/// delegates to its AttributeStatement type). Conclusion types still implement IntoIterator.
pub trait Concept: Predicate + Clone + Debug
where
    Self::Conclusion: Conclusion,
{
    /// Typed term accessors for building queries (e.g. `PersonTerms::name()`).
    type Term;

    /// Returns a description of this concept.
    fn description() -> &'static str {
        ""
    }

    /// Content-addressed identity for this concept.
    fn this(&self) -> Entity;
}

// Blanket impl for &T -> Parameters that uses the generated From<T> impl
impl<T> From<&T> for Parameters
where
    T: Clone + Into<Parameters>,
{
    fn from(source: &T) -> Self {
        source.clone().into()
    }
}

/// A materialized concept — a concrete record whose fields have been
/// resolved from a query [`Answer`].
///
/// Every concept struct carries a `this: Entity` field that identifies the
/// entity it describes. This trait surfaces that field, serving two purposes:
///
/// 1. **Compile-time enforcement** — the `#[derive(Concept)]` macro generates
///    a `Conclusion` impl whose return type is `&Entity`. If the `this` field
///    is missing the macro emits an error; if it has the wrong type the
///    generated impl produces a type mismatch.
/// 2. **Uniform entity access** — any code generic over `Conclusion` can
///    retrieve the underlying entity without knowing the concrete concept
///    type.
///
/// ```compile_fail
/// use dialog_query::{Concept, Entity};
/// use dialog_macros::Attribute;
///
/// mod attrs {
///     #[derive(dialog_macros::Attribute, Clone, PartialEq)]
///     pub struct Name(pub String);
/// }
///
/// /// Concept without a `this` field — should fail.
/// #[derive(Concept, Debug, Clone)]
/// pub struct BadConcept {
///     pub name: attrs::Name,
/// }
/// ```
///
/// ```compile_fail
/// use dialog_query::{Concept, Entity};
/// use dialog_macros::Attribute;
///
/// mod attrs {
///     #[derive(dialog_macros::Attribute, Clone, PartialEq)]
///     pub struct Name(pub String);
/// }
///
/// /// Concept with wrong type for `this` — should fail.
/// #[derive(Concept, Debug, Clone)]
/// pub struct BadConcept {
///     pub this: String,
///     pub name: attrs::Name,
/// }
/// ```
pub trait Conclusion: ConditionalSend {
    /// Each instance has a corresponding entity and this method
    /// returns a reference to it.
    fn this(&self) -> &Entity;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Query;
    use crate::artifact::{
        ArtifactSelector, ArtifactStore, Artifacts, Attribute as ArtifactAttribute, Type, Value,
    };
    use crate::attribute::{Attribute as _, AttributeDescriptor};

    use crate::relation::query::RelationQuery;
    use crate::term::Term;
    use crate::the;
    use crate::types::Scalar;
    use crate::{
        Answer, Cardinality, Concept, Parameter, QueryError, Session, Statement, Transaction,
    };
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

    fn person_predicate() -> ConceptDescriptor {
        ConceptDescriptor::from(vec![
            (
                "name",
                AttributeDescriptor::new(
                    the!("person/name"),
                    "Name of the person",
                    Cardinality::One,
                    Some(Type::String),
                ),
            ),
            (
                "age",
                AttributeDescriptor::new(
                    the!("person/age"),
                    "Age of the person",
                    Cardinality::One,
                    Some(Type::UnsignedInt),
                ),
            ),
        ])
    }

    impl From<Person> for ConceptDescriptor {
        fn from(_: Person) -> Self {
            person_predicate()
        }
    }

    impl From<PersonMatch> for ConceptDescriptor {
        fn from(_: PersonMatch) -> Self {
            person_predicate()
        }
    }

    // Implement Concept for Person
    impl Concept for Person {
        type Term = PersonTerms;

        fn this(&self) -> Entity {
            let predicate: ConceptDescriptor = self.clone().into();
            predicate.this()
        }
    }

    impl IntoIterator for Person {
        type Item = Association;
        type IntoIter = std::vec::IntoIter<Association>;

        fn into_iter(self) -> Self::IntoIter {
            vec![
                Association::new(
                    "person/name".parse().expect("Failed to parse attribute"),
                    self.this.clone(),
                    self.name.as_value(),
                ),
                Association::new(
                    "person/age".parse().expect("Failed to parse attribute"),
                    self.this.clone(),
                    self.age.as_value(),
                ),
            ]
            .into_iter()
        }
    }

    impl Statement for Person {
        fn assert(self, transaction: &mut Transaction) {
            transaction.associate(Association {
                the: "person/name".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.name.as_value(),
            });

            transaction.associate(Association {
                the: "person/age".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.age.as_value(),
            });
        }

        fn retract(self, transaction: &mut Transaction) {
            transaction.dissociate(Association {
                the: "person/name".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.name.as_value(),
            });

            transaction.dissociate(Association {
                the: "person/age".parse().expect("Failed to parse attribute"),
                of: self.this.clone(),
                is: self.age.as_value(),
            });
        }
    }

    impl Predicate for Person {
        type Conclusion = Person;
        type Application = PersonMatch;
        type Descriptor = ConceptDescriptor;
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
    impl Conclusion for Person {
        fn this(&self) -> &Entity {
            &self.this
        }
    }

    // Implement From<PersonMatch> for Parameters
    // This mirrors what the macro generates
    impl From<PersonMatch> for Parameters {
        fn from(source: PersonMatch) -> Self {
            let mut terms = Self::new();
            terms.insert("this".into(), Parameter::from(source.this));
            terms.insert("name".into(), Parameter::from(source.name));
            terms.insert("age".into(), Parameter::from(source.age));
            terms
        }
    }

    // Implement From<PersonMatch> for ConceptQuery
    impl From<PersonMatch> for ConceptQuery {
        fn from(source: PersonMatch) -> Self {
            let predicate: ConceptDescriptor = source.clone().into();
            ConceptQuery {
                terms: source.into(),
                predicate,
            }
        }
    }

    // Implement Queryable for PersonMatch
    impl crate::query::Application for PersonMatch {
        type Conclusion = Person;

        fn evaluate<S: crate::query::Source, M: crate::selection::Answers>(
            self,
            answers: M,
            source: &S,
        ) -> impl crate::selection::Answers {
            let application: ConceptQuery = self.into();
            application.evaluate(answers, source)
        }

        fn realize(&self, source: Answer) -> std::result::Result<Self::Conclusion, QueryError> {
            Ok(Person {
                this: source.get(&self.this)?,
                name: source.get(&self.name)?,
                age: source.get(&self.age)?,
            })
        }
    }

    #[dialog_common::test]
    fn it_creates_person_concept() {
        // Test that the Person concept has the expected properties
        let concept = person_predicate();
        // Operator is now a URI based on the hash of the concept's attributes
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // Test Person has 2 attributes (name and age)
        assert_eq!(concept.with().iter().count(), 2);

        // Verify attribute names
        let attr_names: Vec<&str> = concept.with().iter().map(|(name, _)| name).collect();
        assert!(attr_names.contains(&"name"));
        assert!(attr_names.contains(&"age"));
    }

    #[dialog_common::test]
    fn it_creates_person_match() {
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

    #[dialog_common::test]
    fn it_creates_match_with_constant_values() {
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

    #[dialog_common::test]
    fn it_creates_match_with_mixed_terms() {
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

    #[dialog_common::test]
    fn it_creates_person_instance() {
        // Test creating a Person instance
        let entity = Entity::new().unwrap();
        let person = Person {
            this: entity.clone(),
            name: "Charlie".to_string(),
            age: 25,
        };

        // Test Instance trait - should return the same entity
        assert_eq!(Conclusion::this(&person), &entity);
    }

    #[dialog_common::test]
    fn it_maintains_concept_name_consistency() {
        // Test that concept identifier is consistent across different access patterns
        let concept = person_predicate();
        // Operator is now a URI based on the hash of the concept's attributes
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // The concept should have consistent naming
        let _person = Person {
            this: Entity::new().unwrap(),
            name: "Test".to_string(),
            age: 1,
        };

        // Instance should have the same concept identifier
        // (though our current Instance impl doesn't store concept info)
        // Verify the identifier is still consistent
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );
    }

    #[dialog_common::test]
    fn it_exposes_match_fields() {
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

    #[dialog_common::test]
    fn it_formats_debug_output() {
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

    #[dialog_common::test]
    fn it_clones_concept() {
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

    #[dialog_common::test]
    async fn it_matches_concept_structure() -> Result<()> {
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
        let concept = person_predicate();
        assert_eq!(concept.with().iter().count(), 2); // name and age

        // Verify we can find specific attributes
        let name_attr = concept.with().iter().find(|(name, _)| *name == "name");
        assert!(name_attr.is_some());

        Ok(())
    }

    #[dialog_common::test]
    async fn it_returns_empty_for_no_matches() -> Result<()> {
        // Test that individual fact selectors work for non-matching queries

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;

        // Create minimal test data
        let claims = vec![Association {
            the: the!("person/name"),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        }];

        let mut session = Session::open(artifacts.clone());
        session.transact(claims).await?;

        // Test: Search for non-existent person using individual fact selector
        let missing_query = RelationQuery::new(
            Term::Constant(the!("person/name")),
            Term::var("person"),
            Parameter::from("NonExistent".to_string()),
            Term::blank(),
            None,
        );

        let session = Session::open(artifacts);
        let no_results = missing_query.perform(&session).try_vec().await?;
        assert_eq!(no_results.len(), 0, "Should find no non-existent people");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_with_concept_dsl() -> Result<()> {
        use crate::Query;

        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        mod employee {
            use crate::Attribute;

            #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Name(pub String);

            #[derive(Attribute, Clone, PartialEq, Eq, PartialOrd, Ord)]
            pub struct Role(pub String);
        }

        #[derive(Concept, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
        pub struct Employee {
            this: Entity,
            name: employee::Name,
            role: employee::Role,
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
                name: employee::Name("Alice".to_string()),
                role: employee::Role("cryptographer".to_string()),
            })
            .assert(Employee {
                this: bob.clone(),
                name: employee::Name("Bob".to_string()),
                role: employee::Role("janitor".to_string()),
            })
            .assert(Association {
                the: the!("employee/name"),
                of: mallory.clone(),
                is: Value::String("Mallory".to_string()),
            })
            .assert(Association {
                the: the!("employee/role"),
                of: mallory.clone(),
                is: Value::String("Hacker".to_string()),
            });

        session.commit(transaction).await?;

        let employee = Query::<Employee> {
            this: Term::var("this"),
            name: Term::var("name"),
            role: Term::var("role"),
        };

        let mut employees = employee.perform(&session).try_vec().await?;
        employees.sort();
        let mut expected = vec![
            Employee {
                this: bob.clone(),
                name: employee::Name("Bob".to_string()),
                role: employee::Role("janitor".to_string()),
            },
            Employee {
                this: alice.clone(),
                name: employee::Name("Alice".to_string()),
                role: employee::Role("cryptographer".to_string()),
            },
            Employee {
                this: mallory.clone(),
                name: employee::Name("Mallory".to_string()),
                role: employee::Role("Hacker".to_string()),
            },
        ];
        expected.sort();
        assert_eq!(employees, expected);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_negates_concept_with_not_operator() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        mod person {
            use crate::Attribute;

            #[derive(Attribute, Clone, PartialEq)]
            pub struct Name(pub String);

            #[derive(Attribute, Clone, PartialEq)]
            pub struct Age(pub usize);
        }

        #[derive(Concept, Debug, Clone, PartialEq)]
        pub struct Person {
            this: Entity,
            name: person::Name,
            age: person::Age,
        }

        let alice = Entity::new()?;

        // Create test data - assert Alice
        let mut session = Session::open(artifacts.clone());
        let alice_person = Person {
            this: alice.clone(),
            name: person::Name("Alice".to_string()),
            age: person::Age(25),
        };

        session.transact(vec![alice_person.clone()]).await?;

        // Verify Alice exists
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

    #[dialog_common::test]
    async fn it_negates_relation_with_not_operator() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let name_attr = the!("user/name");

        // Assert a relation
        let mut session = Session::open(artifacts.clone());
        let name_relation = Association {
            the: name_attr.clone(),
            of: alice.clone(),
            is: Value::String("Alice".to_string()),
        };

        session.transact(vec![name_relation.clone()]).await?;

        // Verify relation exists
        use futures_util::TryStreamExt;

        let session = Session::open(artifacts.clone());
        let facts: Vec<_> = session
            .select(
                ArtifactSelector::new()
                    .the(name_attr.clone().into())
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
                    .the(name_attr.clone().into())
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

    // Tests migrated from tests/attribute_concept_test.rs
    mod person_attr_concept {
        use crate::Attribute;

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Birthday(pub u32);

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Email(pub String);
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct DerivedPerson {
        pub this: Entity,
        pub name: person_attr_concept::Name,
        pub birthday: person_attr_concept::Birthday,
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct PersonWithEmail {
        pub this: Entity,
        pub name: person_attr_concept::Name,
        pub email: person_attr_concept::Email,
    }

    #[dialog_common::test]
    async fn it_asserts_concept_with_attribute_fields() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let mut session = Session::open(store.clone());
        session.transact(vec![alice.clone()]).await?;

        let name_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/name")),
            Term::Constant(alice_id.clone()),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let birthday_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/birthday")),
            Term::Constant(alice_id.clone()),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let birthday_facts: Vec<_> = birthday_query
            .perform(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        assert_eq!(birthday_facts.len(), 1);

        assert_eq!(name_facts[0].is, Value::String("Alice".to_string()));
        assert_eq!(birthday_facts[0].is, Value::UnsignedInt(19830703));

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_concept_with_attribute_fields() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice_id = Entity::new()?;
        let bob_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let bob = DerivedPerson {
            this: bob_id.clone(),
            name: person_attr_concept::Name("Bob".to_string()),
            birthday: person_attr_concept::Birthday(19900515),
        };

        let mut session = Session::open(store.clone());
        session.transact(vec![alice, bob]).await?;

        let query = Query::<DerivedPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
            birthday: Term::var("birthday"),
        };

        let session = Session::open(store);
        let results: Vec<DerivedPerson> = query.perform(&session).try_collect().await?;

        assert_eq!(results.len(), 2);

        let alice_result = results.iter().find(|p| p.name.value() == "Alice");
        let bob_result = results.iter().find(|p| p.name.value() == "Bob");

        assert!(alice_result.is_some());
        assert!(bob_result.is_some());

        assert_eq!(alice_result.unwrap().birthday.value(), &19830703u32);
        assert_eq!(bob_result.unwrap().birthday.value(), &19900515u32);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_concept_with_constant_term() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice_id = Entity::new()?;
        let bob_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let bob = DerivedPerson {
            this: bob_id.clone(),
            name: person_attr_concept::Name("Bob".to_string()),
            birthday: person_attr_concept::Birthday(19900515),
        };

        let mut session = Session::open(store.clone());
        session.transact(vec![alice, bob]).await?;

        let query = Query::<DerivedPerson> {
            this: Term::var("person"),
            name: Term::from("Alice"),
            birthday: Term::var("birthday"),
        };

        let session = Session::open(store);
        let results: Vec<DerivedPerson> = query.perform(&session).try_collect().await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.value(), "Alice");
        assert_eq!(results[0].birthday.value(), &19830703u32);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_reuses_attributes_across_concepts() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice_id = Entity::new()?;

        let alice_with_email = PersonWithEmail {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            email: person_attr_concept::Email("alice@example.com".to_string()),
        };

        let mut session = Session::open(store.clone());
        session.transact(vec![alice_with_email]).await?;

        let alice_with_birthday = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let mut session = Session::open(store.clone());
        session.transact(vec![alice_with_birthday]).await?;

        let name_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/name")),
            Term::Constant(alice_id.clone()),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let email_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/email")),
            Term::Constant(alice_id.clone()),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let birthday_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/birthday")),
            Term::Constant(alice_id.clone()),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let email_facts: Vec<_> = email_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let birthday_facts: Vec<_> = birthday_query
            .perform(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 1);
        assert_eq!(email_facts.len(), 1);
        assert_eq!(birthday_facts.len(), 1);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_retracts_concept_with_attributes() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;

        let alice_id = Entity::new()?;

        let alice = DerivedPerson {
            this: alice_id.clone(),
            name: person_attr_concept::Name("Alice".to_string()),
            birthday: person_attr_concept::Birthday(19830703),
        };

        let mut session = Session::open(store.clone());
        session.transact(vec![alice.clone()]).await?;

        let mut session = Session::open(store.clone());
        session.transact(vec![!alice]).await?;

        let name_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/name")),
            Term::Constant(alice_id.clone()),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let birthday_query = RelationQuery::new(
            Term::Constant(the!("person-attr-concept/birthday")),
            Term::Constant(alice_id),
            Parameter::blank(),
            Term::blank(),
            None,
        );

        let name_facts: Vec<_> = name_query
            .perform(&Session::open(store.clone()))
            .try_collect()
            .await?;

        let birthday_facts: Vec<_> = birthday_query
            .perform(&Session::open(store))
            .try_collect()
            .await?;

        assert_eq!(name_facts.len(), 0);
        assert_eq!(birthday_facts.len(), 0);

        Ok(())
    }

    // Tests migrated from tests/concept_query_shortcut_test.rs
    mod shortcut_employee {
        use crate::Attribute;

        #[derive(Attribute, Clone)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone)]
        pub struct Job(pub String);
    }

    #[derive(Concept, Debug, Clone)]
    pub struct ShortcutEmployee {
        pub this: Entity,
        pub name: shortcut_employee::Name,
        pub job: shortcut_employee::Job,
    }

    #[dialog_common::test]
    async fn it_queries_concept_via_shortcut() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut edit = session.edit();
        edit.assert(ShortcutEmployee {
            this: alice.clone(),
            name: shortcut_employee::Name("Alice".into()),
            job: shortcut_employee::Job("Engineer".into()),
        })
        .assert(ShortcutEmployee {
            this: bob.clone(),
            name: shortcut_employee::Name("Bob".into()),
            job: shortcut_employee::Job("Designer".into()),
        });
        session.commit(edit).await?;

        let employees_shortcut: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&session)
            .try_collect()
            .await?;

        let employees_explicit: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&session)
            .try_collect()
            .await?;

        assert_eq!(employees_shortcut.len(), 2);
        assert_eq!(employees_explicit.len(), 2);

        let mut found_alice = false;
        let mut found_bob = false;

        for emp in &employees_shortcut {
            if emp.name.value() == "Alice" {
                assert_eq!(emp.job.value(), "Engineer");
                found_alice = true;
            } else if emp.name.value() == "Bob" {
                assert_eq!(emp.job.value(), "Designer");
                found_bob = true;
            }
        }

        assert!(found_alice, "Should find Alice");
        assert!(found_bob, "Should find Bob");

        Ok(())
    }

    #[dialog_common::test]
    async fn it_filters_concept_query_via_shortcut() -> Result<()> {
        use futures_util::TryStreamExt;

        let backend = MemoryStorageBackend::default();
        let store = Artifacts::anonymous(backend).await?;
        let mut session = Session::open(store);

        let alice = Entity::new()?;

        let mut edit = session.edit();
        edit.assert(ShortcutEmployee {
            this: alice.clone(),
            name: shortcut_employee::Name("Alice".into()),
            job: shortcut_employee::Job("Engineer".into()),
        });
        session.commit(edit).await?;

        let result1: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&session)
            .try_collect()
            .await?;

        let result2: Vec<ShortcutEmployee> = Query::<ShortcutEmployee> {
            this: Term::var("this"),
            name: Term::var("name"),
            job: Term::var("job"),
        }
        .perform(&session)
        .try_collect()
        .await?;

        let result3: Vec<ShortcutEmployee> = Query::<ShortcutEmployee>::default()
            .perform(&session)
            .try_collect()
            .await?;

        assert_eq!(result1.len(), 1);
        assert_eq!(result2.len(), 1);
        assert_eq!(result3.len(), 1);

        assert_eq!(result1[0].name.value(), result2[0].name.value());
        assert_eq!(result2[0].name.value(), result3[0].name.value());

        Ok(())
    }

    // Tests migrated from tests/query_helper_comprehensive_test.rs
    mod helper_person {
        use crate::Attribute;

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);
    }

    mod helper_employee {
        use crate::Attribute;

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        #[derive(Attribute, Clone, PartialEq)]
        pub struct Department(pub String);
    }

    #[derive(Concept, Debug, Clone)]
    pub struct HelperPerson {
        pub this: Entity,
        pub name: helper_person::Name,
    }

    #[derive(Concept, Debug, Clone, PartialEq)]
    pub struct HelperEmployee {
        pub this: Entity,
        pub name: helper_employee::Name,
        pub department: helper_employee::Department,
    }

    #[dialog_common::test]
    async fn it_queries_single_attribute() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut session = Session::open(artifacts.clone());
        let mut transaction = session.edit();
        transaction.assert(helper_person::Name::of(alice).is("Alice"));
        transaction.assert(helper_person::Name::of(bob).is("Bob"));
        session.commit(transaction).await?;

        let alice_query = Query::<HelperPerson> {
            this: Term::var("person"),
            name: Term::from("Alice".to_string()),
        };

        let session = Session::open(artifacts.clone());
        let results = alice_query.perform(&session).try_vec().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.value(), "Alice");

        let all_people_query = Query::<HelperPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
        };

        let session = Session::open(artifacts);
        let all_results = all_people_query.perform(&session).try_vec().await?;
        assert_eq!(all_results.len(), 2);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_queries_multi_attribute_with_constants() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut session = Session::open(artifacts.clone());
        let mut transaction = session.edit();
        transaction.assert(helper_employee::Name::of(alice.clone()).is("Alice"));
        transaction.assert(helper_employee::Department::of(alice.clone()).is("Engineering"));
        transaction.assert(helper_employee::Name::of(bob.clone()).is("Bob"));
        transaction.assert(helper_employee::Department::of(bob).is("Sales"));
        session.commit(transaction).await?;

        let alice_engineering_query = Query::<HelperEmployee> {
            this: Term::var("employee"),
            name: Term::from("Alice".to_string()),
            department: Term::from("Engineering".to_string()),
        };

        let session = Session::open(artifacts);

        let results = alice_engineering_query.perform(&session).try_vec().await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.value(), "Alice");
        assert_eq!(results[0].department.value(), "Engineering");
        assert_eq!(results[0].this, alice);

        Ok(())
    }

    #[dialog_common::test]
    async fn it_handles_multi_attribute_variable_limitation() -> Result<()> {
        let storage_backend = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage_backend).await?;

        let alice = Entity::new()?;
        let bob = Entity::new()?;

        let mut session = Session::open(artifacts.clone());
        let mut transaction = session.edit();
        transaction.assert(helper_employee::Name::of(alice.clone()).is("Alice"));
        transaction.assert(helper_employee::Department::of(alice.clone()).is("Engineering"));
        transaction.assert(helper_employee::Name::of(bob.clone()).is("Bob"));
        transaction.assert(helper_employee::Department::of(bob.clone()).is("Sales"));
        session.commit(transaction).await?;

        let engineering_query = Query::<HelperEmployee> {
            this: Term::var("employee"),
            name: Term::var("name"),
            department: Term::from("Engineering".to_string()),
        };

        let session = Session::open(artifacts);
        let results = engineering_query.perform(&session).try_vec().await?;
        assert_eq!(
            results,
            vec![HelperEmployee {
                this: alice.clone(),
                name: helper_employee::Name("Alice".into()),
                department: helper_employee::Department("Engineering".into())
            }]
        );

        Ok(())
    }
}
