//! Rule-based deduction system
//!
//! This module implements the core rule system for dialog-query, allowing
//! declarative specification of derived facts through logical rules.
//!
//! The design follows the patterns described in notes/rules.md.

/// Deductive rule definitions for deriving new facts.
pub mod deductive;
/// Premises collection type.
pub mod premises;
/// When trait and tuple implementations.
pub mod when;

pub use deductive::DeductiveRule;
pub use deductive::descriptor::DeductiveRuleDescriptor;
pub use premises::*;
pub use when::*;

/// Macro for creating When collections with clean array-like syntax
///
/// This macro provides the most concise way to create rule conditions:
///
/// ```rust
/// use dialog_query::{when, When, Term, Any, artifact::Value, the};
/// use dialog_query::AttributeQuery;
///
/// fn example() -> impl When {
///     let r1 = AttributeQuery::new(
///         Term::from(the!("ns/attr1")),
///         Term::var("entity"),
///         Term::<Any>::constant("value1".to_string()),
///         Term::blank(),
///         None,
///     );
///     let r2 = AttributeQuery::new(
///         Term::from(the!("ns/attr2")),
///         Term::var("entity"),
///         Term::<Any>::var("value2"),
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
    use std::vec::IntoIter;

    use super::*;
    use crate::artifact::{Entity, Type};
    use crate::attribute::{AttributeDescriptor, Cardinality};
    use crate::concept::descriptor::ConceptDescriptor;
    use crate::concept::query::ConceptQuery;
    use crate::concept::{Concept, Conclusion};
    use crate::predicate::Predicate;
    use crate::premise::Premise;
    use crate::query::Application;
    use crate::selection::Match;
    use crate::selection::Selection;
    use crate::source::SelectRules;
    use crate::statement::Statement;
    use crate::term::Term;
    use crate::the;
    use crate::{AttributeStatement, EvaluationError, Parameters, Proposition, Query};
    use dialog_artifacts::Select;
    use dialog_capability::Provider;
    use dialog_common::ConditionalSync;

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
    pub struct PersonQuery {
        /// The entity being matched
        pub this: Term<Entity>,
        /// Name term - can be a variable or concrete value
        pub name: Term<String>,
        /// Age term - can be a variable or concrete value
        pub age: Term<u32>,
    }

    impl Default for PersonQuery {
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

    impl From<PersonQuery> for ConceptDescriptor {
        fn from(_: PersonQuery) -> Self {
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
        type Item = AttributeStatement;
        type IntoIter = IntoIter<AttributeStatement>;

        fn into_iter(self) -> Self::IntoIter {
            vec![
                the!("person/name")
                    .of(self.this.clone())
                    .is(self.name.clone())
                    .into(),
                the!("person/age").of(self.this.clone()).is(self.age).into(),
            ]
            .into_iter()
        }
    }

    impl Statement for Person {
        fn assert(self, update: &mut impl dialog_artifacts::Update) {
            the!("person/name")
                .of(self.this.clone())
                .is(self.name.clone())
                .assert(update);
            the!("person/age")
                .of(self.this.clone())
                .is(self.age)
                .assert(update);
        }

        fn retract(self, update: &mut impl dialog_artifacts::Update) {
            the!("person/name")
                .of(self.this.clone())
                .is(self.name.clone())
                .retract(update);
            the!("person/age")
                .of(self.this.clone())
                .is(self.age)
                .retract(update);
        }
    }

    impl Predicate for Person {
        type Conclusion = Person;
        type Application = PersonQuery;
        type Descriptor = ConceptDescriptor;
    }

    impl TryFrom<Match> for Person {
        type Error = EvaluationError;

        fn try_from(source: Match) -> Result<Self, Self::Error> {
            Ok(Person {
                this: Entity::try_from(
                    source
                        .lookup(&Term::from(&PersonTerms::this()))?
                        .content()?,
                )?,
                name: String::try_from(
                    source
                        .lookup(&Term::from(&PersonTerms::name()))?
                        .content()?,
                )?,
                age: u32::try_from(source.lookup(&Term::from(&PersonTerms::age()))?.content()?)?,
            })
        }
    }

    impl From<PersonQuery> for Parameters {
        fn from(source: PersonQuery) -> Self {
            let mut terms = Self::new();

            terms.insert("this".into(), source.this.into());
            terms.insert("name".into(), source.name.into());
            terms.insert("age".into(), source.age.into());

            terms
        }
    }

    impl Application for PersonQuery {
        type Conclusion = Person;

        fn evaluate<'a, Env, M: Selection + 'a>(
            self,
            selection: M,
            env: &'a Env,
        ) -> impl Selection + 'a
        where
            Env: Provider<Select<'a>> + Provider<SelectRules> + ConditionalSync,
        {
            let application: ConceptQuery = self.into();
            application.evaluate(selection, env)
        }

        fn realize(&self, source: Match) -> Result<Self::Conclusion, EvaluationError> {
            Ok(Person {
                this: Entity::try_from(source.lookup(&Term::from(&self.this))?.content()?)?,
                name: String::try_from(source.lookup(&Term::from(&self.name))?.content()?)?,
                age: u32::try_from(source.lookup(&Term::from(&self.age))?.content()?)?,
            })
        }
    }

    impl From<PersonQuery> for ConceptQuery {
        fn from(source: PersonQuery) -> Self {
            let predicate: ConceptDescriptor = source.clone().into();
            ConceptQuery {
                terms: source.into(),
                predicate,
            }
        }
    }

    impl From<PersonQuery> for Proposition {
        fn from(source: PersonQuery) -> Self {
            Proposition::Concept(source.into())
        }
    }

    impl From<PersonQuery> for Premise {
        fn from(source: PersonQuery) -> Self {
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
        // Define a rule function using the clean API
        fn person_rule(person: Query<Person>) -> impl When {
            (Query::<Person> {
                this: person.this,
                name: person.name,
                age: person.age,
            },)
        }

        // Verify rule installs into registry
        use crate::ConceptDescriptor;
        use crate::rule::deductive::DeductiveRule;
        use crate::session::RuleRegistry;
        let mut rules = RuleRegistry::new();
        let person_query = Query::<Person>::default();
        let concept: ConceptDescriptor = person_query.into();
        let premises = person_rule(Query::<Person>::default())
            .into_premises()
            .into_vec();
        let rule = DeductiveRule::new(concept, premises).unwrap();
        let result = rules.register(rule);
        assert!(result.is_ok(), "register should work");
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

    mod macro_employee {
        use crate::Attribute;

        /// Person's first name
        #[derive(Attribute, Clone, PartialEq)]
        pub struct GivenName(pub String);

        /// Person's preferred nickname
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Nickname(pub String);

        /// Person's age in years
        #[derive(Attribute, Clone, PartialEq)]
        pub struct Age(pub u32);
    }

    /// Concept with both required and optional fields. Exercises the
    /// `Option<T>` branch of the `#[derive(Concept)]` macro: typed
    /// `Term<Option<U>>` query field, optional descriptor pair into
    /// `with_maybe`, and optional realize via `Binding`.
    #[derive(crate::Concept, Debug, Clone)]
    pub struct MacroEmployee {
        /// Employee entity
        pub this: Entity,
        /// Required given name
        pub given_name: macro_employee::GivenName,
        /// Optional preferred nickname
        pub nickname: Option<macro_employee::Nickname>,
        /// Optional age
        pub age: Option<macro_employee::Age>,
    }

    #[dialog_common::test]
    fn it_emits_typed_optional_terms_in_macro() {
        // The query struct must accept `Term<Option<String>>` and
        // `Term<Option<u32>>` for the optional fields. Constructing the
        // query with these types is itself a compile-time test.
        let _query = Query::<MacroEmployee> {
            this: Term::var("emp"),
            given_name: Term::<String>::var("emp_given"),
            nickname: Term::<Option<String>>::var("emp_nickname"),
            age: Term::<Option<u32>>::var("emp_age"),
        };

        // Default-constructed query: every field is a named variable
        // including the optional ones.
        let default = Query::<MacroEmployee>::default();
        assert!(matches!(default.this, Term::Variable { .. }));
        assert!(matches!(default.given_name, Term::Variable { .. }));
        assert!(matches!(default.nickname, Term::Variable { .. }));
        assert!(matches!(default.age, Term::Variable { .. }));

        // Concept descriptor: required field flows into `with`,
        // optional fields flow into `maybe`.
        let concept: ConceptDescriptor = default.into();
        let with: Vec<_> = concept.with().iter().collect();
        assert_eq!(with.len(), 1);
        assert_eq!(with[0].0, "given_name");

        let maybe = concept.maybe().expect("concept has maybe attributes");
        let maybe: Vec<_> = maybe.iter().collect();
        assert_eq!(maybe.len(), 2);
        assert_eq!(maybe[0].0, "nickname");
        assert_eq!(maybe[1].0, "age");

        // Rule body emits one attribute query per field (required
        // *and* optional), with the resolution chosen by each
        // field's `<F as ConceptField>::RESOLUTION` const. For
        // MacroEmployee (1 required + 2 optional fields), `when()`
        // returns 3 attribute queries.
        let when_result = MacroEmployee::when(Query::<MacroEmployee>::default());
        assert_eq!(when_result.len(), 3);
    }

    #[dialog_common::test]
    fn it_persists_only_some_optional_values() {
        // IntoIterator emits a relation per field: required always,
        // optional only when `Some(_)`. With one Some and one None,
        // we should see exactly two statements (1 required + 1 Some).
        let entity = Entity::new().unwrap();
        let employee = MacroEmployee {
            this: entity,
            given_name: macro_employee::GivenName("Ada".into()),
            nickname: Some(macro_employee::Nickname("AL".into())),
            age: None,
        };

        let statements: Vec<AttributeStatement> = employee.into_iter().collect();
        assert_eq!(statements.len(), 2);
    }

    /// Aliasing `Option` to another name still routes through the
    /// `ConceptField` impl for `Option<N>`. Proves the macro does
    /// not depend on syntactic detection of the literal `Option`
    /// identifier — Rust's type system resolves the alias to the
    /// underlying `Option<N>` shape at type-check time, picking
    /// up the right blanket impl.
    #[dialog_common::test]
    fn it_routes_optional_through_alias_via_concept_field() {
        use core::option::Option as Maybe;

        #[derive(crate::Concept, Debug, Clone)]
        #[allow(dead_code)]
        pub struct AliasedConcept {
            /// Entity
            pub this: Entity,
            /// Required name
            pub given_name: macro_employee::GivenName,
            /// Optional nickname, spelled via an aliased Option
            pub nickname: Maybe<macro_employee::Nickname>,
        }

        // Concept descriptor must route `nickname` into `maybe`,
        // not `with`. If the macro were doing syntactic Option
        // detection by ident name, `Maybe` would not match and
        // `nickname` would land in `with` — wrong.
        let concept: ConceptDescriptor = Query::<AliasedConcept>::default().into();
        let with: Vec<_> = concept.with().iter().collect();
        assert_eq!(with.len(), 1);
        assert_eq!(with[0].0, "given_name");

        let maybe = concept
            .maybe()
            .expect("AliasedConcept must have a `maybe` slot");
        let maybe_entries: Vec<_> = maybe.iter().collect();
        assert_eq!(maybe_entries.len(), 1);
        assert_eq!(maybe_entries[0].0, "nickname");
    }
}
