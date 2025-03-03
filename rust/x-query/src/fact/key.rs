use super::{Attribute, Entity, Reference};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PrimaryKey {
    pub entity: Reference,
    pub attribute: (Reference, Reference),
    pub value: Reference,
}

impl<A, V> From<(Entity, A, V)> for PrimaryKey
where
    Attribute: From<A>,
    V: AsRef<[u8]>,
{
    fn from((entity, attribute, value): (Entity, A, V)) -> Self {
        PrimaryKey {
            entity,
            attribute: Attribute::from(attribute).into(),
            value: blake3::hash(value.as_ref()).as_bytes().to_owned(),
        }
    }
}
