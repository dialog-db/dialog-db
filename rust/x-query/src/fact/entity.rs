use std::ops::Deref;

use ulid::Generator as UlidGenerator;

use crate::Reference;

use super::make_reference;

#[derive(Clone, Debug)]
pub struct Entity(Reference);

impl Entity {
    pub fn new() -> Self {
        let seed = UlidGenerator::new()
            .generate()
            .expect("Random bit overflow!?")
            .to_bytes();

        Self(make_reference(seed))
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
