use super::{Field, Reference};

#[derive(Debug, Clone)]
pub enum Fragment {
    Entity(Reference),
    Attribute((Reference, Reference)),
    Value(Reference),
}

impl From<Field> for Fragment {
    fn from(value: Field) -> Self {
        match value {
            Field::Entity(entity) => Fragment::Entity(*entity),
            Field::Attribute(attribute) => Fragment::Attribute(attribute.into()),
            Field::Value(value) => Fragment::Value(blake3::hash(&value).as_bytes().to_owned()),
        }
    }
}
