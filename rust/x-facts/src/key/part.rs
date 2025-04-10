use std::fmt::Display;

use crate::{
    ATTRIBUTE_LENGTH, Attribute, ENTITY_LENGTH, Entity, MAXIMUM_ATTRIBUTE, MAXIMUM_ENTITY,
    MAXIMUM_VALUE_REFERENCE, MINIMUM_ATTRIBUTE, MINIMUM_ENTITY, MINIMUM_VALUE_REFERENCE,
    VALUE_REFERENCE_LENGTH, XFactsError,
};

/// A wrapper around a slice reference that corresponds to the [`Entity`] part
/// of a [`KeyType`]
#[repr(transparent)]
pub struct EntityKeyPart<'a>(pub &'a [u8; ENTITY_LENGTH]);

impl<'a> EntityKeyPart<'a> {
    /// An [`EntityKeyPart`] where all bits are zero
    pub fn min() -> Self {
        Self(&MINIMUM_ENTITY)
    }

    /// An [`EntityKeyPart`] where all bits are one
    pub fn max() -> Self {
        Self(&MAXIMUM_ENTITY)
    }
}

impl<'a> AsRef<[u8]> for EntityKeyPart<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> From<&'a Entity> for EntityKeyPart<'a> {
    fn from(value: &'a Entity) -> Self {
        Self(&*value)
    }
}

impl<'a> From<EntityKeyPart<'a>> for Entity {
    fn from(value: EntityKeyPart<'a>) -> Self {
        Self::from(value.0.to_owned())
    }
}

/// A wrapper around a slice reference that corresponds to the [`Attribute`]
/// part of a [`KeyType`]
#[repr(transparent)]
pub struct AttributeKeyPart<'a>(pub &'a [u8; ATTRIBUTE_LENGTH]);

impl<'a> AttributeKeyPart<'a> {
    /// An [`AttributeKeyPart`] where all bits are zero
    pub fn min() -> Self {
        Self(&MINIMUM_ATTRIBUTE)
    }

    /// An [`AttributeKeyPart`] where all bits are one
    pub fn max() -> Self {
        Self(&MAXIMUM_ATTRIBUTE)
    }
}

impl<'a> From<&'a Attribute> for AttributeKeyPart<'a> {
    fn from(value: &'a Attribute) -> Self {
        AttributeKeyPart(value.key_bytes())
    }
}

impl<'a> TryFrom<AttributeKeyPart<'a>> for Attribute {
    type Error = XFactsError;

    fn try_from(value: AttributeKeyPart<'a>) -> Result<Self, Self::Error> {
        Attribute::try_from(value.to_string())
    }
}

impl<'a> Display for AttributeKeyPart<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parsed = String::from_utf8_lossy(self.0.as_ref())
            .split('\u{0000}')
            .take(1)
            .collect::<String>();

        write!(f, "{parsed}")
    }
}

/// A wrapper around a slice reference that corresponds to the [`Value`]
/// part of a [`KeyType`]
#[repr(transparent)]
pub struct ValueReferenceKeyPart<'a>(pub &'a [u8; VALUE_REFERENCE_LENGTH]);

impl<'a> ValueReferenceKeyPart<'a> {
    /// A [`ValueReferenceKeyPart`] where all bits are zero
    pub fn min() -> Self {
        Self(&MINIMUM_VALUE_REFERENCE)
    }

    /// A [`ValueReferenceKeyPart`] where all bits are one
    pub fn max() -> Self {
        Self(&MAXIMUM_VALUE_REFERENCE)
    }
}
