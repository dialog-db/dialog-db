use dialog_query::rule::Premises;
use dialog_query::{Entity, Term};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub this: Entity,
    pub name: String,
    pub age: u32,
}

mod person {
    pub use super::Person;
    use dialog_query::artifact::{Entity, Value};
    use dialog_query::attribute::{Attribute, Cardinality};

    use dialog_query::predicate::concept::Attributes;
    use dialog_query::term::Term;
    use dialog_query::{concept, Parameters, Type};
    use std::marker::PhantomData;

    pub const NAMESPACE: &'static str = "person";

    const PERSON_ATTRIBUTES: Attributes = Attributes::Static(&[
        (
            "name",
            Attribute {
                namespace: NAMESPACE,
                name: "name",
                description: "person name",
                content_type: Type::String,
                cardinality: Cardinality::One,
                marker: PhantomData::<Value>,
            },
        ),
        (
            "age",
            Attribute {
                namespace: NAMESPACE,
                name: "age",
                description: "person age",
                content_type: Type::SignedInt,
                cardinality: Cardinality::One,
                marker: PhantomData::<Value>,
            },
        ),
    ]);

    impl concept::Instance for Person {
        fn this(&self) -> Entity {
            self.this.clone()
        }
    }

    impl TryFrom<dialog_query::selection::Match> for Person {
        type Error = dialog_query::error::InconsistencyError;

        fn try_from(source: dialog_query::selection::Match) -> Result<Self, Self::Error> {
            Ok(Person {
                this: source.get(&PersonTerms::this())?,
                name: source.get(&PersonTerms::name())?,
                age: source.get(&PersonTerms::age())?,
            })
        }
    }

    pub struct Assert;
    pub struct Retract;

    #[derive(Debug, Clone, PartialEq)]
    pub struct Match {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub age: Term<u32>,
    }
    impl concept::Match for Match {
        type Concept = Person;
        type Instance = Person;

        fn realize(
            &self,
            source: dialog_query::selection::Match,
        ) -> Result<Self::Instance, dialog_query::QueryError> {
            Ok(Self::Instance {
                this: source.get(&self.this)?,
                name: source.get(&self.name)?,
                age: source.get(&self.age)?,
            })
        }
    }

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

    impl From<Match> for Parameters {
        fn from(person: Match) -> Self {
            let mut params = Parameters::new();
            params.insert("this".into(), person.this.as_unknown());
            params.insert("name".into(), person.name.as_unknown());
            params.insert("age".into(), person.age.as_unknown());
            params
        }
    }

    impl concept::Concept for Person {
        type Instance = Person;
        type Match = Match;
        type Term = PersonTerms;
        type Assert = Assert;
        type Retract = Retract;
    }

    impl concept::ConceptType for Person {
        fn operator() -> &'static str {
            &NAMESPACE
        }

        fn attributes() -> &'static Attributes {
            &PERSON_ATTRIBUTES
        }
    }

    impl dialog_query::rule::Premises for Match {
        type IntoIter = std::vec::IntoIter<dialog_query::Premise>;

        fn premises(self) -> Self::IntoIter {
            // For now return empty - proper implementation would convert Match to statements
            vec![].into_iter()
        }
    }
}

fn main() {
    let alice = person::Match {
        this: Term::var("person"),
        name: Term::var("Alice"),
        age: Term::blank(),
    };

    let _statements = alice.premises();
    // TODO: Implement main function
}
