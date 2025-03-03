use crate::Reference;

#[derive(Debug, Clone)]
pub struct Attribute {
    pub namespace: String,
    pub predicate: String,
}

impl From<Attribute> for (Reference, Reference) {
    fn from(value: Attribute) -> Self {
        (
            blake3::hash(value.namespace.as_bytes())
                .as_bytes()
                .to_owned(),
            blake3::hash(value.predicate.as_bytes())
                .as_bytes()
                .to_owned(),
        )
    }
}
