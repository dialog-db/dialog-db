//! X-Wing keys: the post-quantum half of a recipient.

use super::SecretError;
use x_wing::{
    DecapsulationKeyRejectNonContrib, Decapsulator, ENCAPSULATION_KEY_SIZE, EncapsulationKey,
    KeyExport,
};

/// Length of an X-Wing private key seed.
pub const X_WING_SEED_LEN: usize = x_wing::DECAPSULATION_KEY_SIZE;
/// Length of an encoded [`XWingPublicKey`].
pub const X_WING_PUBLIC_KEY_LEN: usize = ENCAPSULATION_KEY_SIZE;

/// An X-Wing private key, which reveals secrets concealed to its public key.
///
/// X-Wing combines X25519 and ML-KEM-768 (FIPS 203), so a secret concealed to
/// it holds as long as either does: ML-KEM against a quantum computer, X25519
/// against a flaw found in the much younger ML-KEM.
///
/// The key is a 32-byte seed, and it is independent of any Ed25519 identity.
/// It is not derived from one on purpose: a quantum computer recovers an
/// identity's scalar from its `did:key`, and with it anything derived from
/// that scalar, so a derived X-Wing key would be no stronger than X25519.
///
/// Its holder stores the seed. WebCrypto has no ML-KEM yet, so in the browser
/// the key is held in wasm memory, not as a non-extractable `CryptoKey`.
///
/// `Debug` is redacted: it shows the public key only.
#[derive(Clone)]
pub struct XWingSecretKey(DecapsulationKeyRejectNonContrib);

impl XWingSecretKey {
    /// Generate a fresh key.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::Crypto`] if the platform has no randomness.
    pub fn generate() -> Result<Self, SecretError> {
        let mut seed = [0u8; X_WING_SEED_LEN];
        dialog_common::random::fill(&mut seed).map_err(|e| SecretError::Crypto(e.to_string()))?;
        Ok(Self::from_seed(seed))
    }

    /// Restore a key from its seed.
    #[must_use]
    pub fn from_seed(seed: [u8; X_WING_SEED_LEN]) -> Self {
        Self(DecapsulationKeyRejectNonContrib::from(seed))
    }

    /// The seed this key was generated or restored from.
    ///
    /// # Security Warning
    ///
    /// The seed is the key: anyone holding it reveals every secret concealed
    /// to [`Self::public_key`]. Store it as you would the identity's own key.
    #[must_use]
    pub fn to_seed(&self) -> [u8; X_WING_SEED_LEN] {
        let mut seed = [0u8; X_WING_SEED_LEN];
        seed.copy_from_slice(&self.0.to_bytes());
        seed
    }

    /// The public key to publish, which others conceal to.
    #[must_use]
    pub fn public_key(&self) -> XWingPublicKey {
        XWingPublicKey(self.0.encapsulation_key().clone())
    }

    pub(super) fn decapsulation_key(&self) -> &DecapsulationKeyRejectNonContrib {
        &self.0
    }
}

impl std::fmt::Debug for XWingSecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("XWingSecretKey")
            .field("public_key", &self.public_key())
            .finish_non_exhaustive()
    }
}

/// An X-Wing public key, which secrets are concealed to.
///
/// 1216 bytes: an ML-KEM-768 encapsulation key followed by an X25519 public
/// key. Too long to ride in a `did:key`, so it is published beside the DID.
///
/// # Security Warning
///
/// Nothing in this key names its owner. A sender has to get it from a source
/// that vouches for it on the recipient's behalf, such as a statement the
/// recipient's DID signed: a key taken from anywhere else is a key whoever
/// handed it over may hold.
#[derive(Clone, PartialEq, Eq)]
pub struct XWingPublicKey(EncapsulationKey);

impl XWingPublicKey {
    /// Encode as 1216 bytes.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; X_WING_PUBLIC_KEY_LEN] {
        let mut bytes = [0u8; X_WING_PUBLIC_KEY_LEN];
        bytes.copy_from_slice(&self.0.to_bytes());
        bytes
    }

    /// Decode from 1216 bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::InvalidRecipient`] if `bytes` is not an X-Wing
    /// public key.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SecretError> {
        EncapsulationKey::try_from(bytes)
            .map(Self)
            .map_err(|_| SecretError::InvalidRecipient)
    }

    pub(super) fn encapsulation_key(&self) -> &EncapsulationKey {
        &self.0
    }
}

impl std::fmt::Debug for XWingPublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The X25519 tail is enough to tell keys apart without 1.2 KB of hex.
        let bytes = self.to_bytes();
        f.debug_tuple("XWingPublicKey")
            .field(&&bytes[X_WING_PUBLIC_KEY_LEN - 32..])
            .finish()
    }
}

impl serde::Serialize for XWingPublicKey {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(self.to_bytes().as_slice(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for XWingPublicKey {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = serde_bytes::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}
