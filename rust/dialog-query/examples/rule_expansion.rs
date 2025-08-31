use dialog_query::artifact::Entity;
use dialog_query::attribute::Attribute;
use dialog_query::fact::Scalar;
use dialog_query::rule::{Match, Rule};
use dialog_query::{Statement, Statements, Term};
use std::marker::PhantomData;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub name: String,
    pub age: u32,
}

mod person {
    pub use super::Person;
    use dialog_query::artifact::{Entity, Value, ValueDataType};
    use dialog_query::attribute::{Attribute, Cardinality};
    use dialog_query::concept::Concept;
    use dialog_query::fact::Scalar;
    use dialog_query::rule::Rule;
    use dialog_query::term::Term;
    use dialog_query::{concept, selection};
    use std::marker::PhantomData;

    pub const NAMESPACE: &'static str = "person";
    const ATTRIBUTES: &'static [(&'static str, Attribute<Value>)] = &[
        (
            "name",
            Attribute {
                namespace: NAMESPACE,
                name: "name",
                description: "person name",
                data_type: ValueDataType::String,
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
                data_type: ValueDataType::SignedInt,
                cardinality: Cardinality::One,
                marker: PhantomData::<Value>,
            },
        ),
    ];

    impl concept::Instance for Person {
        fn this(&self) -> Entity {
            // TODO: This in not gonig to fly we need to find a better way to
            // manage this relation.
            Entity::new().unwrap()
        }
    }

    pub struct Attributes;
    impl concept::Attributes for Attributes {
        fn attributes() -> &'static [(&'static str, Attribute<Value>)] {
            ATTRIBUTES
        }
    }

    pub struct Assert;
    pub struct Retract;

    pub struct Match {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub age: Term<u32>,
    }
    impl concept::Match for Match {
        type Instance = Person;
        type Attributes = Attributes;

        fn term_for(&self, name: &str) -> Option<Term<Value>> {
            match name {
                "this" => Some(self.this.as_unknown()),
                "name" => Some(self.name.as_unknown()),
                "age" => Some(self.age.as_unknown()),
                _ => None,
            }
        }

        fn this(&self) -> &Term<Entity> {
            &self.this
        }
    }

    impl concept::Concept for Person {
        type Instance = Person;
        type Attributes = Attributes;
        type Match = Match;
        type Assert = Assert;
        type Retract = Retract;

        fn name() -> &'static str {
            NAMESPACE
        }

        fn r#match<T: Into<Term<Entity>>>(this: T) -> Self::Attributes {
            Attributes
        }
    }
}

fn main() {
    let alice = Match::<Person> {
        this: Term::var("person"),
        name: Term::var("Alice"),
        age: Term::blank(),
    };

    let statements = alice.statements();
    // TODO: Implement main function
}
