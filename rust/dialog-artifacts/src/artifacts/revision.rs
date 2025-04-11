use crate::{DialogArtifactsError, HASH_SIZE};

use super::Blake3Hash;

/// A [`Version`] represents the root of [`Artifacts`] for a given set of data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Revision(Blake3Hash, Blake3Hash, Blake3Hash);

impl Revision {
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

impl From<Revision> for Vec<u8> {
    fn from(value: Revision) -> Self {
        [value.0, value.1, value.2].concat()
    }
}

impl From<(Blake3Hash, Blake3Hash, Blake3Hash)> for Revision {
    fn from((entity, attribute, value): (Blake3Hash, Blake3Hash, Blake3Hash)) -> Self {
        Self(entity, attribute, value)
    }
}

impl TryFrom<&[u8]> for Revision {
    type Error = DialogArtifactsError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.len() != 3 * HASH_SIZE {
            return Err(DialogArtifactsError::InvalidVersion(format!(
                "Not a valid version length (expected {} bytes, got {} bytes)",
                3 * HASH_SIZE,
                value.len()
            )));
        }

        let entity_version: [u8; 32] = value[0..HASH_SIZE]
            .try_into()
            .map_err(|error| DialogArtifactsError::InvalidVersion(format!("{error}")))?;

        let attribute_version: [u8; 32] = value[HASH_SIZE..2 * HASH_SIZE]
            .try_into()
            .map_err(|error| DialogArtifactsError::InvalidVersion(format!("{error}")))?;

        let value_version: [u8; 32] = value[2 * HASH_SIZE..3 * HASH_SIZE]
            .try_into()
            .map_err(|error| DialogArtifactsError::InvalidVersion(format!("{error}")))?;

        Ok(Self(entity_version, attribute_version, value_version))
    }
}

impl TryFrom<Vec<u8>> for Revision {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}
