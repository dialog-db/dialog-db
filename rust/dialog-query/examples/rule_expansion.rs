use dialog_query::rule::Premises;
use dialog_query::Term;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Person {
    pub name: String,
    pub age: u32,
}

mod person {
    pub use super::Person;
    use dialog_query::artifact::{Entity, Value, ValueDataType};
    use dialog_query::attribute::{Attribute, Cardinality};

    use dialog_query::concept;
    use dialog_query::term::Term;
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

        fn of<T: Into<Term<Entity>>>(_entity: T) -> Self {
            Attributes
        }
    }

    pub struct Assert;
    pub struct Retract;

    pub struct Match {
        pub this: Term<Value>,
        pub name: Term<Value>,
        pub age: Term<Value>,
    }
    impl concept::Match for Match {
        type Instance = Person;
        type Attributes = Attributes;

        fn term_for(&self, name: &str) -> Option<&Term<Value>> {
            match name {
                "this" => Some(&self.this),
                "name" => Some(&self.name),
                "age" => Some(&self.age),
                _ => None,
            }
        }

        fn this(&self) -> Term<Entity> {
            match &self.this {
                Term::Constant(v) => Term::Constant(v.clone().try_into().unwrap()),
                Term::Variable { name, .. } => {
                    if let Some(name) = name {
                        Term::var(name)
                    } else {
                        Term::blank()
                    }
                }
            }
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
    }

    impl dialog_query::rule::Premises for Match {
        type IntoIter = std::vec::IntoIter<dialog_query::deductive_rule::Premise>;

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
