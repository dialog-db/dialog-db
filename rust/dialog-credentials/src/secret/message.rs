//! The sealed secret wire format.

use super::SecretError;

/// Length of the ephemeral X25519 public key.
const EPHEMERAL_KEY_LEN: usize = 32;
/// Length of an X-Wing ciphertext.
const X_WING_CIPHERTEXT_LEN: usize = x_wing::CIPHERTEXT_SIZE;
/// Length of the AES-GCM nonce.
const NONCE_LEN: usize = 12;
/// Length of the AES-GCM authentication tag.
const TAG_LEN: usize = 16;

/// The prefix marking a versioned encoding.
///
/// The first format had no header: it opens with the sender's ephemeral X25519
/// key. All zeros is the encoding of the point `u = 0`, which has low order, so
/// an agreement against it is non-contributory and reveal rejects it. No
/// secret in the first format that could ever be revealed opens with these
/// bytes, which is what lets this prefix tell the formats apart without a
/// guess.
const VERSIONED: [u8; EPHEMERAL_KEY_LEN] = [0u8; EPHEMERAL_KEY_LEN];
/// The version byte, after [`VERSIONED`], of a secret concealed with X-Wing.
const X_WING: u8 = 0x01;

/// The key encapsulation a [`SealedSecret`] was concealed with.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scheme {
    /// X25519 against the key the recipient's `did:key` carries.
    ///
    /// Not post-quantum: a quantum computer recovers the recipient's private
    /// key from its DID and reveals the secret. Conceal again with
    /// [`Scheme::XWing`] to protect it.
    X25519,
    /// X-Wing, the hybrid of X25519 and ML-KEM-768, against the recipient's
    /// [`XWingPublicKey`](super::XWingPublicKey). Holds as long as either
    /// scheme does.
    XWing,
}

/// What a recipient needs to recover the AES-GCM key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Encapsulation {
    /// The sender's single-use X25519 public key.
    X25519([u8; EPHEMERAL_KEY_LEN]),
    /// The X-Wing ciphertext.
    XWing(Box<[u8; X_WING_CIPHERTEXT_LEN]>),
}

/// A secret concealed to one identity.
///
/// Produced by [`Seal::conceal`] and opened by [`Secret::reveal`]. Carries
/// what the recipient needs to recover the key, and the ciphertext is
/// authenticated, so tampering is detected rather than yielding garbage.
///
/// There are two encodings, one per [`Scheme`]:
///
/// - X25519: `ephemeral_public_key(32) || nonce(12) || ciphertext || tag(16)`,
///   92 bytes for a 32-byte secret.
/// - X-Wing: `zeros(32) || 0x01 || x_wing_ciphertext(1120) || nonce(12) ||
///   ciphertext || tag(16)`, 1213 bytes for a 32-byte secret.
///
/// [`Seal::conceal`]: super::Seal::conceal
/// [`Secret::reveal`]: super::Secret::reveal
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSecret {
    /// What the recipient needs to recover the key.
    pub(super) encapsulation: Encapsulation,
    /// The AES-GCM nonce.
    pub(super) nonce: [u8; NONCE_LEN],
    /// The ciphertext with its authentication tag appended.
    pub(super) ciphertext: Vec<u8>,
}

impl SealedSecret {
    /// The scheme this secret was concealed with.
    ///
    /// A secret concealed with [`Scheme::X25519`] is exposed to a quantum
    /// computer until it is revealed and concealed again with X-Wing.
    #[must_use]
    pub const fn scheme(&self) -> Scheme {
        match self.encapsulation {
            Encapsulation::X25519(_) => Scheme::X25519,
            Encapsulation::XWing(_) => Scheme::XWing,
        }
    }

    /// Encode to the wire format.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            VERSIONED.len() + 1 + X_WING_CIPHERTEXT_LEN + NONCE_LEN + self.ciphertext.len(),
        );
        match &self.encapsulation {
            Encapsulation::X25519(ephemeral) => bytes.extend_from_slice(ephemeral),
            Encapsulation::XWing(ciphertext) => {
                bytes.extend_from_slice(&VERSIONED);
                bytes.push(X_WING);
                bytes.extend_from_slice(ciphertext.as_slice());
            }
        }
        bytes.extend_from_slice(&self.nonce);
        bytes.extend_from_slice(&self.ciphertext);
        bytes
    }

    /// Decode from the wire format.
    ///
    /// # Errors
    ///
    /// Returns [`SecretError::Malformed`] if `bytes` is too short to hold a
    /// sealed secret, or names a version this release does not know.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SecretError> {
        let (encapsulation, rest) = match bytes.split_first_chunk::<EPHEMERAL_KEY_LEN>() {
            Some((prefix, rest)) if *prefix == VERSIONED => match rest.split_first() {
                Some((&X_WING, rest)) => {
                    let (ciphertext, rest) = rest
                        .split_first_chunk::<X_WING_CIPHERTEXT_LEN>()
                        .ok_or(SecretError::Malformed)?;
                    (Encapsulation::XWing(Box::new(*ciphertext)), rest)
                }
                _ => return Err(SecretError::Malformed),
            },
            Some((ephemeral, rest)) => (Encapsulation::X25519(*ephemeral), rest),
            None => return Err(SecretError::Malformed),
        };

        if rest.len() < NONCE_LEN + TAG_LEN {
            return Err(SecretError::Malformed);
        }
        let (nonce, ciphertext) = rest.split_at(NONCE_LEN);

        Ok(Self {
            encapsulation,
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
