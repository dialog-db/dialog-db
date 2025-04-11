use crate::{HASH_SIZE, XFactsError};

use super::Blake3Hash;

/// A [`Version`] represents the root of [`Facts`] for a given set of data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(Blake3Hash, Blake3Hash, Blake3Hash);

impl Version {
    /// The component of the [`Version`] that corresponds to the [`Entity`] index
    pub fn entity(&self) -> &Blake3Hash {
        &self.0
    }

    /// The component of the [`Version`] that corresponds to the [`Attribute`] index
    pub fn attribute(&self) -> &Blake3Hash {
        &self.1
    }

    /// The component of the [`Version`] that corresponds to the [`Value`] index
    pub fn value(&self) -> &Blake3Hash {
        &self.2
    }
}

impl From<Version> for Vec<u8> {
    fn from(value: Version) -> Self {
        [value.0, value.1, value.2].concat()
    }
}

impl From<(Blake3Hash, Blake3Hash, Blake3Hash)> for Version {
    fn from((entity, attribute, value): (Blake3Hash, Blake3Hash, Blake3Hash)) -> Self {
        Self(entity, attribute, value)
    }
}

impl TryFrom<&[u8]> for Version {
    type Error = XFactsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 3 * HASH_SIZE {
            return Err(XFactsError::InvalidVersion(format!(
                "Not a valid version length (expected {} bytes, got {} bytes)",
                3 * HASH_SIZE,
                value.len()
            )));
        }

        let entity_version: [u8; 32] = value[0..HASH_SIZE]
            .try_into()
            .map_err(|error| XFactsError::InvalidVersion(format!("{error}")))?;

        let attribute_version: [u8; 32] = value[HASH_SIZE..2 * HASH_SIZE]
            .try_into()
            .map_err(|error| XFactsError::InvalidVersion(format!("{error}")))?;

        let value_version: [u8; 32] = value[2 * HASH_SIZE..3 * HASH_SIZE]
            .try_into()
            .map_err(|error| XFactsError::InvalidVersion(format!("{error}")))?;

        Ok(Self(entity_version, attribute_version, value_version))
    }
}

impl TryFrom<Vec<u8>> for Version {
    type Error = XFactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}
