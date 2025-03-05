use std::str::FromStr;

use crate::{Reference, XQueryError};

use super::make_reference;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

impl From<&Attribute> for Attribute {
    fn from(value: &Attribute) -> Self {
        value.clone()
    }
}

impl From<Attribute> for (Reference, Reference) {
    fn from(value: Attribute) -> Self {
        (
            make_reference(value.namespace.as_bytes()),
            make_reference(value.predicate.as_bytes()),
        )
    }
}
