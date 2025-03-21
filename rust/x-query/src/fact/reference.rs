use base58::ToBase58;
use std::ops::Deref;

use crate::XQueryError;

/// A type-alias for a 32-byte array
pub type RawReference = [u8; 32];

#[repr(transparent)]
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Default)]
pub struct Reference(RawReference);

impl Reference {
    pub const BYTE_LENGTH: usize = 32;

    const RAW_NULL_REFERENCE: [u8; 32] = [0; 32];

    pub const fn from_raw(inner: RawReference) -> Self {
        Reference(inner)
    }

    pub const fn null() -> Self {
        Self(Self::RAW_NULL_REFERENCE)
    }
}

impl TryFrom<&[u8]> for Reference {
    type Error = XQueryError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let raw = RawReference::try_from(value).map_err(|error| {
            XQueryError::InvalidReference(format!("Cannot convert bytes into reference: {}", error))
        })?;
        Ok(Self(raw))
    }
}

impl AsRef<RawReference> for Reference {
    fn as_ref(&self) -> &RawReference {
        &self.0
    }
}

impl Deref for Reference {
    type Target = RawReference;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<RawReference> for Reference {
    fn from(value: RawReference) -> Self {
        Reference(value)
    }
}

impl From<Reference> for RawReference {
    fn from(value: Reference) -> Self {
        value.0
    }
}

impl std::fmt::Debug for Reference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", format_reference(&self.0))
    }
}

/// The low bound of valid [Reference]s (all zeroes)
// pub const REFERENCE_MIN: [u8; 32] = [u8::MIN; 32];
pub const REFERENCE_MIN: Reference = Reference::from_raw([u8::MIN; 32]);
/// The high bound of valid [Reference]s (every bit set to 1)
pub const REFERENCE_MAX: Reference = Reference::from_raw([u8::MAX; 32]);

pub fn format_reference(reference: &RawReference) -> String {
    format!("{}...", &reference.to_base58()[0..6])
}

/// Produces a [Reference], which is a type-alias for a 32-byte array; in practice, these
/// bytes are the BLAKE3 hash of the inputs to this function
pub fn make_reference<B>(bytes: B) -> Reference
where
    B: AsRef<[u8]>,
{
    blake3::hash(bytes.as_ref()).as_bytes().to_owned().into()
}
