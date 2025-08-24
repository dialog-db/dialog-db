use std::fmt::Debug;

use dialog_query::fact::Scalar;
use dialog_query::Entity;
pub use dialog_query::{FactSelector, IntoValueDataType, Term};
use std::marker::PhantomData;
pub use std::ops::Deref;

/// Utility type that simply gets associated type for the relation.
#[allow(type_alias_bounds)]
pub type Match<T: Relation> = T::Match;
#[allow(type_alias_bounds)]
pub type Claim<T: Relation> = T::Claim;
#[allow(type_alias_bounds)]
pub type Attributes<T: Relation> = T::Attributes;

pub trait Relation {
    type Match;
    type Claim;
    type Attributes;

    fn name() -> &'static str;
}

pub trait Rule<T: Relation>: Relation {}

pub struct Person {
    pub person: Entity,
    pub name: String,
    pub birthday: String,
}

#[allow(non_snake_case)]
pub fn Person<Of: Into<Term<Entity>>>(term: Of) -> person::Attributes {
    Person::r#match(term)
}

impl Person {
    pub fn r#match<Of: Into<Term<Entity>>>(term: Of) -> person::Attributes {
        let of: Term<Entity> = term.into();
        person::Attributes {
            name: MatchAttribute::new(person::NAME, "name", of.clone()),
            birthday: MatchAttribute::new(person::NAME, "birthday", of.clone()),
        }
    }

    pub fn name<T: Into<Term<Entity>>>(of: T) -> MatchAttribute<String> {
        MatchAttribute::new(person::NAME, "name", of.into())
    }
    pub fn birthday<T: Into<Term<Entity>>>(of: T) -> MatchAttribute<u32> {
        MatchAttribute::new(person::NAME, "birthday", of.into())
    }
}
impl Relation for Person {
    type Match = person::Match;
    type Attributes = person::Attributes;
    type Claim = person::Claim;

    fn name() -> &'static str {
        person::NAME
    }
}

pub struct Attribute<T: IntoValueDataType + Clone + Debug + 'static> {
    pub namespace: &'static str,
    pub name: &'static str,
    pub marker: PhantomData<T>,
}

impl<T: IntoValueDataType + Clone + Debug + 'static> Attribute<T> {
    pub fn new(namespace: &'static str, name: &'static str) -> Self {
        Self {
            namespace,
            name,
            marker: PhantomData,
        }
    }
    pub fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
    pub fn of<Of: Into<Term<Entity>>>(&self, term: Of) -> MatchAttribute<T> {
        MatchAttribute {
            attribute: Attribute {
                namespace: person::NAME,
                name: "name",
                marker: PhantomData,
            },
            of: term.into(),
        }
    }
}

pub struct MatchAttribute<T: IntoValueDataType + Clone + Debug + 'static> {
    pub attribute: Attribute<T>,
    pub of: Term<Entity>,
}

impl<T: Scalar> MatchAttribute<T> {
    pub fn new(namespace: &'static str, name: &'static str, of: Term<Entity>) -> Self {
        Self {
            attribute: Attribute::new(namespace, name),
            of,
        }
    }
    pub fn of(&self) -> Term<Entity> {
        self.of.clone()
    }
    pub fn the(&self) -> String {
        self.attribute.the()
    }

    pub fn is<Is: Into<Term<T>>>(self, term: Is) -> FactSelector<T> {
        FactSelector::new()
            .the(self.the())
            .of(self.of())
            .is(term.into())
    }
    pub fn not<Is: Into<Term<T>>>(self, term: Is) -> FactSelector<T> {
        FactSelector::new()
            .the(self.the())
            .of(self.of())
            .is(term.into())
    }
}

impl MatchAttribute<u32> {
    pub fn greater<T: Into<Term<u32>>>(&self, other: T) -> FactSelector<u32> {
        FactSelector::new()
            .the(self.the())
            .of(self.of())
            .is(other.into())
    }
}

pub mod person {
    use crate::MatchAttribute;
    use dialog_query::Entity;
    pub use dialog_query::{FactSelector, Term};
    pub const NAME: &'static str = "person";

    pub struct Attributes {
        pub name: MatchAttribute<String>,
        pub birthday: MatchAttribute<u32>,
    }

    pub struct Match {
        pub this: Term<Entity>,
        pub name: Term<String>,
        pub birthday: Term<u32>,
    }

    pub struct Claim {
        pub name: Term<String>,
        pub birthday: Term<u32>,
    }

    impl From<Match> for Term<Entity> {
        fn from(source: Match) -> Self {
            source.this
        }
    }

    impl Default for Match {
        fn default() -> Self {
            Self {
                this: Term::var("this"),
                name: Term::var("name"),
                birthday: Term::var("birthday"),
            }
        }
    }

    pub mod name {
        pub use dialog_query::Term;
        pub fn is<Is: Into<Term<String>>>(_term: Is) -> Term<String> {
            Term::blank()
        }
    }

    pub mod birthday {
        pub use dialog_query::Term;
        pub fn is<Is: Into<Term<String>>>(_term: Is) -> Term<String> {
            Term::blank()
        }
    }
}

pub fn john(person: Match<Person>) -> FactSelector<String> {
    Person(person).name.is("John")
}

fn main() {
    let person = Term::var("person");
    let _named_john = Person(&person).name.is("John");
    let _birthday = Person(&person).birthday.is(1983_07_03 as u32);
    let _john = Match::<Person> {
        this: person.clone(),
        name: Term::from("John"),
        birthday: Term::from(1983_07_03 as u32),
    };

    let _named_alice = Person(Term::var("alice")).name.is("Alice");
    let _named_bob = Person(Term::var("bob")).name.is("Bob");
    let _above_42 = Person(&person).birthday.greater(42u32);

    let _t = Person::name(&person).is("John");
    let _birthday = Person::birthday(&person).is(1983_07_03u32);

    let out = Person(&person).name.is("John");

    let claim = Claim::<Person> {
        name: Term::from("John"),
        birthday: Term::from(1983_07_03u32),
    };
}
