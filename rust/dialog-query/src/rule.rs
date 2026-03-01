//! Rule-based deduction system
//!
//! This module implements the core rule system for dialog-query, allowing
//! declarative specification of derived facts through logical rules.
//!
//! The design is based on the TypeScript implementation in @query/src/plan/rule.js
//! and follows the patterns described in the design document at notes/rules.md.

/// Deductive rule definitions for deriving new facts.
pub mod deductive;
/// Premises collection type.
pub mod premises;
/// When trait and tuple implementations.
pub mod when;

pub use deductive::DeductiveRule;
pub use premises::*;
pub use when::*;

/// Macro for creating When collections with clean array-like syntax
///
/// This macro provides the most concise way to create rule conditions:
///
/// ```rust
/// use dialog_query::{when, When, Term, artifact::Value, the};
/// use dialog_query::relation::query::RelationQuery;
///
/// fn example() -> impl When {
///     let r1 = RelationQuery::new(
///         Term::Constant(the!("ns/attr1")),
///         Term::var("entity"),
///         Term::from(Value::String("value1".to_string())),
///         Term::blank(),
///         None,
///     );
///     let r2 = RelationQuery::new(
///         Term::Constant(the!("ns/attr2")),
///         Term::var("entity"),
///         Term::var("value2"),
///         Term::blank(),
///         None,
///     );
///
///     when![r1, r2]
/// }
/// ```
#[macro_export]
macro_rules! when {
    [$($item:expr),* $(,)?] => {
        $crate::rule::Premises::from(vec![$($item),*])
    };
}

#[cfg(test)]
mod tests {
    extern crate self as dialog_query;

    use super::*;
    use crate::artifact::{Artifacts, Entity, Type};
    use crate::attribute::{AttributeDescriptor, Cardinality};
    use crate::concept::application::ConceptQuery;
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::concept::{Concept, Conclusion};
    use crate::error::InconsistencyError;
    use crate::predicate::Predicate;
    use crate::premise::Premise;
    use crate::selection::Answer;
    use crate::statement::Statement;
    use crate::term::Term;
    use crate::the;
    use crate::types::Scalar;
    use crate::{Association, Parameters, Proposition, Query, QueryError, Session, Transaction};

    // Manual implementation of Person struct with Concept and Rule traits
    // This serves as a template for what the derive macro should generate
    #[derive(Debug, Clone)]
    pub struct Person {
        pub this: Entity,
        /// Name of the person
        pub name: String,
        /// Age of the person
        pub age: u32,
    }

    /// Query pattern for Person - has Term-wrapped fields for querying
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
                Association::new(the!("person/name"), self.this.clone(), self.name.as_value()),
                Association::new(the!("person/age"), self.this.clone(), self.age.as_value()),
            ]
            .into_iter()
        }
    }

    impl Statement for Person {
        fn assert(self, transaction: &mut Transaction) {
            Association::new(the!("person/name"), self.this.clone(), self.name.as_value())
                .assert(transaction);
            Association::new(the!("person/age"), self.this.clone(), self.age.as_value())
                .assert(transaction);
        }

        fn retract(self, transaction: &mut Transaction) {
            Association::new(the!("person/name"), self.this.clone(), self.name.as_value())
                .retract(transaction);
            Association::new(the!("person/age"), self.this.clone(), self.age.as_value())
                .retract(transaction);
        }
    }

    impl Predicate for Person {
        type Conclusion = Person;
        type Application = PersonMatch;
        type Descriptor = ConceptDescriptor;
    }

    impl TryFrom<Answer> for Person {
        type Error = InconsistencyError;

        fn try_from(source: Answer) -> Result<Self, Self::Error> {
            Ok(Person {
                this: source.get(&PersonTerms::this())?,
                name: source.get(&PersonTerms::name())?,
                age: source.get(&PersonTerms::age())?,
            })
        }
    }

    impl From<PersonMatch> for Parameters {
        fn from(source: PersonMatch) -> Self {
            let mut terms = Self::new();

            terms.insert("this".into(), source.this);
            terms.insert("name".into(), source.name);
            terms.insert("age".into(), source.age);

            terms
        }
    }

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

        fn realize(&self, source: Answer) -> Result<Self::Conclusion, QueryError> {
            Ok(Person {
                this: source.get(&self.this)?,
                name: source.get(&self.name)?,
                age: source.get(&self.age)?,
            })
        }
    }

    impl From<PersonMatch> for ConceptQuery {
        fn from(source: PersonMatch) -> Self {
            let predicate: ConceptDescriptor = source.clone().into();
            ConceptQuery {
                terms: source.into(),
                predicate,
            }
        }
    }

    impl From<PersonMatch> for Proposition {
        fn from(source: PersonMatch) -> Self {
            Proposition::Concept(source.into())
        }
    }

    impl From<PersonMatch> for Premise {
        fn from(source: PersonMatch) -> Self {
            Premise::Assert(source.into())
        }
    }

    impl Conclusion for Person {
        fn this(&self) -> &Entity {
            panic!("Instance trait implementation requires an entity field")
        }
    }

    #[dialog_common::test]
    async fn it_installs_rule() {
        use dialog_storage::MemoryStorageBackend;

        // Define a rule function using the clean API
        fn person_rule(person: Query<Person>) -> impl When {
            (Query::<Person> {
                this: person.this,
                name: person.name,
                age: person.age,
            },)
        }

        // Create a session
        let storage = MemoryStorageBackend::default();
        let artifacts = Artifacts::anonymous(storage).await.unwrap();

        let result = Session::open(artifacts).install(person_rule);
        assert!(result.is_ok(), "install should work");
    }

    mod macro_person {
        use crate::Attribute;

        /// Name of the person
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Name(pub String);

        /// Birthday of the person
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Birthday(pub u32);
    }

    #[derive(crate::Concept, Debug, Clone)]
    pub struct MacroPerson {
        /// Person entity
        pub this: Entity,

        /// Name of the person
        pub name: macro_person::Name,

        /// Birthday of the person
        pub birthday: macro_person::Birthday,
    }

    #[dialog_common::test]
    fn it_generates_derived_rule_types() {
        // Test that the generated module and types exist
        let entity = Term::var("person_entity");

        // Test the generated Query struct
        let _person_match = Query::<MacroPerson> {
            this: entity.clone(),
            name: Term::var("person_name"),
            birthday: Term::var("person_birthday"),
        };

        // Test that MacroPerson implements Concept
        let concept: ConceptDescriptor = Query::<MacroPerson>::default().into();
        // Operator is now a computed URI
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // Test the attributes() method
        let attrs = concept.with().iter().collect::<Vec<_>>();

        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].0, "name");
        assert_eq!(attrs[0].1.domain(), "macro-person");
        assert_eq!(attrs[0].1.name(), "name");
        assert_eq!(attrs[0].1.description(), "Name of the person");
        assert_eq!(attrs[0].1.content_type(), Some(Type::String));
        assert_eq!(attrs[1].0, "birthday");
        assert_eq!(attrs[1].1.domain(), "macro-person");
        assert_eq!(attrs[1].1.name(), "birthday");
        assert_eq!(attrs[1].1.description(), "Birthday of the person");
        assert_eq!(attrs[1].1.content_type(), Some(Type::UnsignedInt));

        // Test that MacroPerson implements Rule
        let test_match = Query::<MacroPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
            birthday: Term::var("birthday"),
        };

        let when_result = MacroPerson::when(test_match);
        assert_eq!(when_result.len(), 2); // Should have 2 field statements
    }

    #[dialog_common::test]
    fn it_exposes_attribute_descriptors() {
        // Test that attribute descriptors are accessible via inherent methods
        let name_desc = macro_person::Name::descriptor();
        let birthday_desc = macro_person::Birthday::descriptor();
        assert_eq!(name_desc.domain(), "macro-person");
        assert_eq!(name_desc.name(), "name");
        assert_eq!(birthday_desc.domain(), "macro-person");
        assert_eq!(birthday_desc.name(), "birthday");
    }
}
