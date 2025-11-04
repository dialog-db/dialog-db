use dialog_storage::{Blake3Hash, HashType};

/// Produces a [Reference], which is a type-alias for a 32-byte array; in practice, these
/// bytes are the BLAKE3 hash of the inputs to this function
pub fn make_reference<B>(bytes: B) -> Blake3Hash
where
    B: AsRef<[u8]>,
{
    blake3::hash(bytes.as_ref()).as_bytes().to_owned()
}

// TODO: We only have one "reference type" now, maybe deconstruct this macro
macro_rules! reference_type {
    ( $struct:ident ) => {
        impl From<Blake3Hash> for $struct {
            fn from(value: Blake3Hash) -> Self {
                Self(value)
            }
        }

        impl From<$struct> for Blake3Hash {
            fn from(value: $struct) -> Self {
                value.0
            }
        }

        impl std::ops::Deref for $struct {
            type Target = Blake3Hash;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<Vec<u8>> for $struct {
            type Error = crate::DialogArtifactsError;

            fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
                Ok(Self(value.try_into().map_err(|value: Vec<u8>| {
                    crate::DialogArtifactsError::InvalidReference(format!(
                        "Incorrect length (expected {}, got {})",
                        Blake3Hash::SIZE,
                        value.len()
                    ))
                })?))
            }
        }
    };
}

pub(crate) use reference_type;
