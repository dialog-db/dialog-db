use std::str::FromStr;

use crate::{Reference, XQueryError};

#[derive(Debug, Clone)]
pub struct Attribute {
    pub namespace: String,
    pub predicate: String,
}

impl FromStr for Attribute {
    type Err = XQueryError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split(":").collect::<Vec<&str>>();

        let predicate = parts
            .pop()
            .ok_or_else(|| XQueryError::InvalidAttribute(format!("{s}")))?
            .to_string();
        let namespace = parts.join(":");

        Ok(Attribute {
            namespace,
            predicate,
        })
    }
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
