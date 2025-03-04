use crate::{Literal, Reference};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Fragment {
    Entity(Reference),
    Attribute((Reference, Reference)),
    Value(Reference),
}

impl From<&Literal> for Fragment {
    fn from(value: &Literal) -> Self {
        match value {
            Literal::Entity(entity) => Fragment::Entity(**entity),
            Literal::Attribute(attribute) => Fragment::Attribute(attribute.clone().into()),
            Literal::Value(value) => Fragment::Value(blake3::hash(&value).as_bytes().to_owned()),
        }
    }
}

impl From<Literal> for Fragment {
    fn from(value: Literal) -> Self {
        Fragment::from(&value)
    }
}
