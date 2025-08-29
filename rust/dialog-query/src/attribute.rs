pub use crate::artifact::Entity;
pub use crate::fact_selector::FactSelector;
pub use crate::term::Term;
pub use crate::types::Scalar;
pub use std::marker::PhantomData;

/// Cardinality indicates whether an attribute can have one or many values
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cardinality {
    One,
    Many,
}

#[derive(Clone, Debug)]
pub struct Attribute<T: Scalar> {
    pub namespace: &'static str,
    pub name: &'static str,
    pub description: &'static str,
    pub cardinality: Cardinality,
    pub marker: PhantomData<T>,
}

impl<T: Scalar> Attribute<T> {
    pub fn new(namespace: &'static str, name: &'static str, description: &'static str) -> Self {
        Self {
            namespace,
            name,
            description,
            cardinality: Cardinality::One,
            marker: PhantomData,
        }
    }
    pub fn the(&self) -> String {
        format!("{}/{}", self.namespace, self.name)
    }
    pub fn of<Of: Into<Term<Entity>>>(&self, term: Of) -> Match<T> {
        Match {
            attribute: self.clone(),
            of: term.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct Match<T: Scalar> {
    pub attribute: Attribute<T>,
    pub of: Term<Entity>,
}

impl<T: Scalar> Match<T> {
    pub fn new(
        namespace: &'static str,
        name: &'static str,
        description: &'static str,
        of: Term<Entity>,
    ) -> Self {
        Self {
            attribute: Attribute::new(namespace, name, description),
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
