use std::fmt::{Display, Formatter, Result as FmtResult};

use crate::{
    Attribute, DialogArtifactsError, Entity, MAXIMUM_VALUE_REFERENCE, MINIMUM_VALUE_REFERENCE,
    VALUE_REFERENCE_LENGTH,
};

/// The empty byte string: the minimum value of any variable-length component.
const EMPTY: &[u8] = &[];

/// A wrapper around a slice reference that corresponds to the [`Entity`] part
/// of a [`KeyType`].
///
/// The slice is the entity's raw bytes (the full URI, losslessly), variable
/// length.
#[repr(transparent)]
pub struct EntityKeyPart<'a>(pub &'a [u8]);

impl EntityKeyPart<'_> {
    /// An [`EntityKeyPart`] that is the smallest possible entity (empty).
    pub fn min() -> Self {
        Self(EMPTY)
    }

    /// The raw bytes represented by this [`EntityKeyPart`].
    pub fn raw(&self) -> &[u8] {
        self.0
    }
}

impl AsRef<[u8]> for EntityKeyPart<'_> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

impl<'a> From<&'a Entity> for EntityKeyPart<'a> {
    fn from(value: &'a Entity) -> Self {
        Self(value.as_str().as_bytes())
    }
}

/// A wrapper around a slice reference that corresponds to the [`Attribute`]
/// part of a [`KeyType`].
///
/// The slice is the attribute's raw `namespace/predicate` bytes, variable
/// length.
#[repr(transparent)]
pub struct AttributeKeyPart<'a>(pub &'a [u8]);

impl AttributeKeyPart<'_> {
    /// An [`AttributeKeyPart`] that is the smallest possible attribute (empty).
    pub fn min() -> Self {
        Self(EMPTY)
    }

    /// The raw bytes represented by this [`AttributeKeyPart`].
    pub fn raw(&self) -> &[u8] {
        self.0
    }
}

impl<'a> From<&'a Attribute> for AttributeKeyPart<'a> {
    fn from(value: &'a Attribute) -> Self {
        AttributeKeyPart(value.as_str().as_bytes())
    }
}

impl<'a> TryFrom<AttributeKeyPart<'a>> for Attribute {
    type Error = DialogArtifactsError;

    fn try_from(value: AttributeKeyPart<'a>) -> Result<Self, Self::Error> {
        Attribute::try_from(value.to_string())
    }
}

impl Display for AttributeKeyPart<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        write!(f, "{}", String::from_utf8_lossy(self.0))
    }
}

/// A wrapper around a slice reference that corresponds to the [`Value`]
/// part of a [`KeyType`]
#[repr(transparent)]
pub struct ValueReferenceKeyPart<'a>(pub &'a [u8; VALUE_REFERENCE_LENGTH]);

impl ValueReferenceKeyPart<'_> {
    /// A [`ValueReferenceKeyPart`] where all bits are zero
    pub fn min() -> Self {
        Self(&MINIMUM_VALUE_REFERENCE)
    }

    /// A [`ValueReferenceKeyPart`] where all bits are one
    pub fn max() -> Self {
        Self(&MAXIMUM_VALUE_REFERENCE)
    }

    /// The internal array represented by this [`ValueReferenceKeyPart`]
    pub fn raw(&self) -> &[u8; VALUE_REFERENCE_LENGTH] {
        self.0
    }
}
