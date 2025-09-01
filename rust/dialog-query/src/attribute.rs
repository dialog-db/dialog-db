pub use crate::artifact::{Entity, Value};
use crate::artifact::ValueDataType;
pub use crate::fact_selector::FactSelector;
pub use crate::term::Term;
pub use crate::types::{IntoValueDataType, Scalar};
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
    pub data_type: ValueDataType,
    pub marker: PhantomData<T>,
}

impl<T: Scalar> Attribute<T> {
    pub fn new(
        namespace: &'static str,
        name: &'static str,
        description: &'static str,
        data_type: ValueDataType,
    ) -> Self {
        Self {
            namespace,
            name,
            description,
            cardinality: Cardinality::One,
            data_type,
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

    /// Get the data type for this attribute
    ///
    /// Returns the stored ValueDataType for this attribute.
    pub fn data_type(&self) -> Option<ValueDataType> {
        Some(self.data_type)
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
        data_type: ValueDataType,
        of: Term<Entity>,
    ) -> Self {
        Self {
            attribute: Attribute::new(namespace, name, description, data_type),
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
