use std::ops::Deref;

use dialog_prolly_tree::ValueType;

use crate::{Blake3Hash, DialogArtifactsError, HASH_SIZE, make_reference};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity, Value};

/// The primitive representation of an [`Entity`]: 32 bytes
pub type RawEntity = [u8; 32];
/// The primitive representation of a [`Value`]: a buffer of bytes
pub type RawValue = Vec<u8>;
/// The primitive representation of [`Attribute`]: a UTF-8 string
pub type RawAttribute = String;

/// An [`EntityDatum`] is the layout of data stored in the value index of
/// [`Artifacts`]
#[derive(Clone, Debug)]
pub struct EntityDatum {
    /// The raw representation of the [`Entity`] associated with this
    /// [`EntityDatum`]
    pub entity: RawEntity,
}

impl Deref for EntityDatum {
    type Target = RawEntity;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl ValueType for EntityDatum {
    fn to_vec(&self) -> Vec<u8> {
        self.entity.to_vec()
    }
}

impl TryFrom<Vec<u8>> for EntityDatum {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            entity: value.try_into().map_err(|value: Vec<u8>| {
                DialogArtifactsError::InvalidValue(format!(
                    "Wrong byte length for entity; expected {HASH_SIZE}, got {}",
                    value.len()
                ))
            })?,
        })
    }
}

/// A [`ValueDatum`] is the layout of data stored in the entity and attribute
/// indexes of [`Artifacts`]
#[derive(Clone, Debug)]
pub struct ValueDatum {
    /// The raw representation of the [`Value`] asscoiated with this [`ValueDatum`]
    pub value: RawValue,

    // TODO: We automatically hash values when the `ValueDatum` is created. Ideally
    // we would only compute the hash lazily if it is requested (and then memoize it).
    reference: Blake3Hash,
}

impl ValueDatum {
    /// The hash reference that corresponds to this [`Value`]
    pub fn reference(&self) -> &Blake3Hash {
        &self.reference
    }
}

impl ValueType for ValueDatum {
    fn to_vec(&self) -> Vec<u8> {
        self.value.to_vec()
    }
}

impl TryFrom<Vec<u8>> for ValueDatum {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            reference: make_reference(&value),
            value,
        })
    }
}
