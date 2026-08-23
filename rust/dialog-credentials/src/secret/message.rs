//! The sealed secret wire format.

use super::SecretError;

/// Length of the ephemeral X25519 public key.
const EPHEMERAL_KEY_LEN: usize = 32;
/// Length of the AES-GCM nonce.
const NONCE_LEN: usize = 12;
/// Length of the AES-GCM authentication tag.
const TAG_LEN: usize = 16;
/// Smallest possible encoding: the header plus an empty, authenticated payload.
const MIN_LEN: usize = EPHEMERAL_KEY_LEN + NONCE_LEN + TAG_LEN;

/// A secret concealed to one identity.
///
/// Produced by [`Seal::conceal`] and opened by [`Secret::reveal`]. Carries the
/// sender's ephemeral public key so the recipient can complete the key
/// agreement; the ciphertext is authenticated, so tampering is detected rather
/// than yielding garbage.
///
/// The encoded form is
/// `ephemeral_public_key(32) || nonce(12) || ciphertext || tag(16)`, which is
/// 92 bytes for a 32-byte secret.
///
/// [`Seal::conceal`]: super::Seal::conceal
/// [`Secret::reveal`]: super::Secret::reveal
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSecret {
    /// The sender's single-use public key.
    pub(super) ephemeral_public_key: [u8; EPHEMERAL_KEY_LEN],
    /// The AES-GCM nonce.
    pub(super) nonce: [u8; NONCE_LEN],
    /// The ciphertext with its authentication tag appended.
    pub(super) ciphertext: Vec<u8>,
}

impl SealedSecret {
    /// Encode to the wire format.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(EPHEMERAL_KEY_LEN + NONCE_LEN + self.ciphertext.len());
        bytes.extend_from_slice(&self.ephemeral_public_key);
        bytes.extend_from_slice(&self.nonce);
        bytes.extend_from_slice(&self.ciphertext);
        bytes
    }

    /// Decode from the wire format.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::Malformed`] if `bytes` is too short to hold a
    /// sealed secret.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SecretError> {
        if bytes.len() < MIN_LEN {
            return Err(SecretError::Malformed);
        }

        let (ephemeral, rest) = bytes.split_at(EPHEMERAL_KEY_LEN);
        let (nonce, ciphertext) = rest.split_at(NONCE_LEN);

        Ok(Self {
            ephemeral_public_key: ephemeral.try_into().map_err(|_| SecretError::Malformed)?,
            nonce: nonce.try_into().map_err(|_| SecretError::Malformed)?,
            ciphertext: ciphertext.to_vec(),
        })
    }
}

impl serde::Serialize for SealedSecret {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(self.to_bytes().as_slice(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for SealedSecret {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}
