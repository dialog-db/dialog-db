use std::ops::Deref;

use crate::Reference;

use super::{make_reference, make_seed};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entity(Reference);

impl Default for Entity {
    fn default() -> Self {
        Self::new()
    }
}

impl Entity {
    pub fn new() -> Self {
        Self(make_reference(make_seed()))
    }
}

impl Deref for Entity {
    type Target = Reference;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<[u8; 32]> for Entity {
    fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<Reference> for Entity {
    fn from(value: Reference) -> Self {
        Entity(value)
    }
}

impl From<Entity> for Reference {
    fn from(value: Entity) -> Self {
        value.0
    }
}
