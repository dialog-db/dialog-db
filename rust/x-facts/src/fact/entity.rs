use std::{fmt::Display, ops::Deref};

use crate::{RawEntity, make_reference, make_seed};

/// An [`Entity`] is the subject part of a semantic triple. Internally, an
/// [`Entity`] is represented as a unique 32-byte hash.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entity(RawEntity);

impl Entity {
    /// Generate a new, unique [`Entity`].
    pub fn new() -> Self {
        Self(make_reference(make_seed()))
    }
}

impl Deref for Entity {
    type Target = RawEntity;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RawEntity> for Entity {
    fn from(value: RawEntity) -> Self {
        Entity(value)
    }
}

impl From<Entity> for RawEntity {
    fn from(value: Entity) -> Self {
        value.0
    }
}

impl Display for Entity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#{}...",
            self.0
                .iter()
                .take(6)
                .map(|byte| format!("{:X}", byte))
                .collect::<Vec<String>>()
                .concat()
        )
    }
}
