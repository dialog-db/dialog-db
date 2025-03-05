use crate::{Literal, Reference, XQueryError};

use super::{Attribute, make_reference};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fragment {
    Entity(Reference),
    Attribute((Reference, Reference)),
    Value(Reference),
}

impl Fragment {
    pub fn as_entity(&self) -> Result<&Reference, XQueryError> {
        match self {
            Fragment::Entity(fragment) => Ok(fragment),
            _ => Err(XQueryError::InvalidReference(format!(
                "Expected fragment to be an entity reference"
            ))),
        }
    }

    pub fn as_attribute(&self) -> Result<&(Reference, Reference), XQueryError> {
        match self {
            Fragment::Attribute(fragment) => Ok(fragment),
            _ => Err(XQueryError::InvalidReference(format!(
                "Expected fragment to be an attribute reference"
            ))),
        }
    }

    pub fn as_value(&self) -> Result<&Reference, XQueryError> {
        match self {
            Fragment::Value(fragment) => Ok(fragment),
            _ => Err(XQueryError::InvalidReference(format!(
                "Expected fragment to be a value reference"
            ))),
        }
    }
}

impl From<&Literal> for Fragment {
    fn from(value: &Literal) -> Self {
        match value {
            Literal::Entity(entity) => Fragment::Entity(**entity),
            Literal::Attribute(attribute) => Fragment::Attribute(attribute.clone().into()),
            Literal::Value(value) => Fragment::Value(make_reference(&value)),
        }
    }
}

impl From<Attribute> for Fragment {
    fn from(value: Attribute) -> Self {
        Self::from(Literal::Attribute(value))
    }
}

impl From<Literal> for Fragment {
    fn from(value: Literal) -> Self {
        Fragment::from(&value)
    }
}
