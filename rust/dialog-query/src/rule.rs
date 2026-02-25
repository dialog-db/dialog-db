//! Rule-based deduction system
//!
//! This module implements the core rule system for dialog-query, allowing
//! declarative specification of derived facts through logical rules.
//!
//! The design is based on the TypeScript implementation in @query/src/plan/rule.js
//! and follows the patterns described in the design document at notes/rules.md.

pub use crate::dsl::Match;
use crate::premise::Premise;

/// Trait for types that can be converted into a When collection
///
/// This trait enables ergonomic rule definitions by allowing various types
/// to be used as rule premises:
/// - Single items: `Into<Premise>` types
/// - Tuples: `(Match<A>, Match<B>, ...)`
/// - Arrays: `[Match<A>; N]`
/// - Vectors: `Vec<Match<A>>`
///
/// # Examples
///
/// ```rs
/// // Return a tuple of different Match types
/// fn my_rule(emp: Match<Employee>) -> impl When {
///     (
///         Match::<Stuff> { this: emp.this, name: emp.name },
///         Match::<OtherStuff> { this: emp.this, value: emp.value },
///     )
/// }
/// ```
pub trait When {
    /// Convert this collection into a set of premises
    fn into_premises(self) -> Premises;
}

/// An ordered collection of premises used in rule definitions
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Premises(Vec<Premise>);

impl Premises {
    /// Create a new empty When collection
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the number of statements
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get an iterator over the statements
    pub fn iter(&self) -> impl Iterator<Item = &Premise> {
        self.0.iter()
    }

    /// Add a statement-producing item to this When
    pub fn extend<T: When>(&mut self, items: T) {
        self.0.extend(items.into_premises());
    }

    /// Get the inner Vec for compatibility
    pub fn into_vec(self) -> Vec<Premise> {
        self.0
    }

    /// Get reference to inner Vec for compatibility
    pub fn as_vec(&self) -> &Vec<Premise> {
        &self.0
    }
}

impl IntoIterator for Premises {
    type Item = Premise;
    type IntoIter = std::vec::IntoIter<Premise>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Premises {
    type Item = &'a Premise;
    type IntoIter = std::slice::Iter<'a, Premise>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<T: Into<Premise>> From<Vec<T>> for Premises {
    fn from(source: Vec<T>) -> Self {
        let mut premises = vec![];
        for each in source {
            premises.push(each.into());
        }
        Premises(premises)
    }
}

impl<T: Into<Premise>, const N: usize> From<[T; N]> for Premises {
    fn from(source: [T; N]) -> Self {
        let mut premises = vec![];
        for each in source {
            premises.push(each.into());
        }
        Premises(premises)
    }
}

// Implement IntoWhen for When itself
impl When for Premises {
    fn into_premises(self) -> Premises {
        self
    }
}

// Implement IntoWhen for arrays
impl<T: Into<Premise>, const N: usize> When for [T; N] {
    fn into_premises(self) -> Premises {
        self.into()
    }
}

// Implement IntoWhen for Vec
impl<T: Into<Premise>> When for Vec<T> {
    fn into_premises(self) -> Premises {
        self.into()
    }
}

// Implement IntoWhen for tuples of different sizes
// This allows heterogeneous premise types in a single rule

impl<T1> When for (T1,)
where
    T1: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![self.0.into()])
    }
}

impl<T1, T2> When for (T1, T2)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![self.0.into(), self.1.into()])
    }
}

impl<T1, T2, T3> When for (T1, T2, T3)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![self.0.into(), self.1.into(), self.2.into()])
    }
}

impl<T1, T2, T3, T4> When for (T1, T2, T3, T4)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5> When for (T1, T2, T3, T4, T5)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6> When for (T1, T2, T3, T4, T5, T6)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7> When for (T1, T2, T3, T4, T5, T6, T7)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> When for (T1, T2, T3, T4, T5, T6, T7, T8)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> When for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> When for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
    T10: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
            self.9.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> When
    for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
    T10: Into<Premise>,
    T11: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
            self.9.into(),
            self.10.into(),
        ])
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> When
    for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
where
    T1: Into<Premise>,
    T2: Into<Premise>,
    T3: Into<Premise>,
    T4: Into<Premise>,
    T5: Into<Premise>,
    T6: Into<Premise>,
    T7: Into<Premise>,
    T8: Into<Premise>,
    T9: Into<Premise>,
    T10: Into<Premise>,
    T11: Into<Premise>,
    T12: Into<Premise>,
{
    fn into_premises(self) -> Premises {
        Premises(vec![
            self.0.into(),
            self.1.into(),
            self.2.into(),
            self.3.into(),
            self.4.into(),
            self.5.into(),
            self.6.into(),
            self.7.into(),
            self.8.into(),
            self.9.into(),
            self.10.into(),
            self.11.into(),
        ])
    }
}

/// Macro for creating When collections with clean array-like syntax
///
/// This macro provides the most concise way to create rule conditions:
///
/// ```rust
/// use dialog_query::{when, When, Term, artifact::Value};
/// use dialog_query::proposition::relation::RelationApplication;
///
/// fn example() -> impl When {
///     let r1 = RelationApplication::new(
///         Term::Constant("ns".into()),
///         Term::Constant("attr1".into()),
///         Term::var("entity"),
///         Term::from(Value::String("value1".to_string())),
///         Term::blank(),
///         None,
///     );
///     let r2 = RelationApplication::new(
///         Term::Constant("ns".into()),
///         Term::Constant("attr2".into()),
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
    use crate::claim::Claim;
    use crate::concept::{Concept, ConceptProof};
    use crate::dsl::Predicate;
    use crate::error::InconsistencyError;
    use crate::predicate::concept::ConceptPredicate;
    use crate::proposition::ConceptApplication;
    use crate::selection::Answer;
    use crate::term::Term;
    use crate::the;
    use crate::types::Scalar;
    use crate::{Assertion, Parameters, Premise, Proposition, QueryError, Session, Transaction};

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

    fn person_predicate() -> ConceptPredicate {
        ConceptPredicate::from(vec![
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

    impl From<Person> for ConceptPredicate {
        fn from(_: Person) -> Self {
            person_predicate()
        }
    }

    impl From<PersonMatch> for ConceptPredicate {
        fn from(_: PersonMatch) -> Self {
            person_predicate()
        }
    }

    impl Concept for Person {
        type Term = PersonTerms;

        fn this(&self) -> Entity {
            let predicate: ConceptPredicate = self.clone().into();
            predicate.this()
        }
    }

    impl IntoIterator for Person {
        type Item = Assertion;
        type IntoIter = std::vec::IntoIter<Assertion>;

        fn into_iter(self) -> Self::IntoIter {
            vec![
                Assertion::new(
                    "person/name"
                        .parse()
                        .expect("Failed to parse person/name attribute"),
                    self.this.clone(),
                    self.name.as_value(),
                ),
                Assertion::new(
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

    impl Claim for Person {
        fn assert(self, transaction: &mut Transaction) {
            Assertion::new(
                "person/name"
                    .parse()
                    .expect("Failed to parse person/name attribute"),
                self.this.clone(),
                self.name.as_value(),
            )
            .assert(transaction);
            Assertion::new(
                "person/age"
                    .parse()
                    .expect("Failed to parse person/age attribute"),
                self.this.clone(),
                self.age.as_value(),
            )
            .assert(transaction);
        }

        fn retract(self, transaction: &mut Transaction) {
            Assertion::new(
                "person/name"
                    .parse()
                    .expect("Failed to parse person/name attribute"),
                self.this.clone(),
                self.name.as_value(),
            )
            .retract(transaction);
            Assertion::new(
                "person/age"
                    .parse()
                    .expect("Failed to parse person/age attribute"),
                self.this.clone(),
                self.age.as_value(),
            )
            .retract(transaction);
        }
    }

    impl Predicate for Person {
        type Proof = Person;
        type Application = PersonMatch;
        type Descriptor = ConceptPredicate;
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

            terms.insert("this".into(), source.this.as_unknown());
            terms.insert("name".into(), source.name.as_unknown());
            terms.insert("age".into(), source.age.as_unknown());

            terms
        }
    }

    impl crate::query::Application for PersonMatch {
        type Proof = Person;

        fn evaluate<S: crate::query::Source, M: crate::selection::Answers>(
            self,
            answers: M,
            source: &S,
        ) -> impl crate::selection::Answers {
            let application: ConceptApplication = self.into();
            application.evaluate(answers, source)
        }

        fn realize(&self, source: Answer) -> Result<Self::Proof, QueryError> {
            Ok(Person {
                this: source.get(&self.this)?,
                name: source.get(&self.name)?,
                age: source.get(&self.age)?,
            })
        }
    }

    impl From<PersonMatch> for ConceptApplication {
        fn from(source: PersonMatch) -> Self {
            let predicate: ConceptPredicate = source.clone().into();
            ConceptApplication {
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
            Premise::When(source.into())
        }
    }

    impl ConceptProof for Person {
        fn this(&self) -> &Entity {
            panic!("Instance trait implementation requires an entity field")
        }
    }

    #[dialog_common::test]
    async fn test_install_rule_api() {
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
    fn test_derive_rule_generates_types() {
        // Test that the generated module and types exist
        let entity = Term::var("person_entity");

        // Test the generated Match struct
        let _person_match = Match::<MacroPerson> {
            this: entity.clone(),
            name: Term::var("person_name"),
            birthday: Term::var("person_birthday"),
        };

        // Test that MacroPerson implements Concept
        let concept: ConceptPredicate = Match::<MacroPerson>::default().into();
        // Operator is now a computed URI
        assert!(
            concept.this().to_string().starts_with("concept:"),
            "Operator should be a concept URI"
        );

        // Test the attributes() method
        let attrs = concept.iter().collect::<Vec<_>>();

        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].0, "name");
        assert_eq!(attrs[0].1.namespace(), "macro-person");
        assert_eq!(attrs[0].1.name(), "name");
        assert_eq!(attrs[0].1.description(), "Name of the person");
        assert_eq!(attrs[0].1.content_type(), Some(Type::String));
        assert_eq!(attrs[1].0, "birthday");
        assert_eq!(attrs[1].1.namespace(), "macro-person");
        assert_eq!(attrs[1].1.name(), "birthday");
        assert_eq!(attrs[1].1.description(), "Birthday of the person");
        assert_eq!(attrs[1].1.content_type(), Some(Type::UnsignedInt));

        // Test that MacroPerson implements Rule
        let test_match = Match::<MacroPerson> {
            this: Term::var("person"),
            name: Term::var("name"),
            birthday: Term::var("birthday"),
        };

        let when_result = MacroPerson::when(test_match);
        assert_eq!(when_result.len(), 2); // Should have 2 field statements
    }

    #[dialog_common::test]
    fn test_attribute_descriptors() {
        use crate::attribute::Attribute;
        // Test that attribute descriptors are accessible via the Attribute trait
        let name_desc = macro_person::Name::descriptor();
        let birthday_desc = macro_person::Birthday::descriptor();
        assert_eq!(name_desc.namespace(), "macro-person");
        assert_eq!(name_desc.name(), "name");
        assert_eq!(birthday_desc.namespace(), "macro-person");
        assert_eq!(birthday_desc.name(), "birthday");
    }
}
