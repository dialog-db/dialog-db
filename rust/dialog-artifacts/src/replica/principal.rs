use super::{Formatter, SignatureError, ToBase58, VerifyingKey};
use base58::FromBase58;
use dialog_capability::Did;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::OnceLock;

/// Cryptographic identifier like Ed25519 public key representing
/// a principal that produced a change.
///
/// The DID representation is memoized on first access.
pub struct Principal {
    /// The raw 32-byte Ed25519 public key.
    bytes: [u8; 32],
    /// Cached DID, computed lazily.
    did: OnceLock<Did>,
}

impl Principal {
    /// Creates a new Principal from raw public key bytes.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self {
            bytes,
            did: OnceLock::new(),
        }
    }

    /// Creates a Principal from a DID string.
    ///
    /// Parses `did:key:z...` format to extract the Ed25519 public key bytes.
    /// Returns `None` if the DID is not a valid Ed25519 did:key.
    pub fn from_did(did: &Did) -> Option<Self> {
        let did_str: &str = did.as_ref();

        // Must start with "did:key:z"
        let encoded = did_str.strip_prefix("did:key:z")?;

        // Base58 decode
        let decoded = encoded.from_base58().ok()?;

        // Must be exactly 34 bytes (2-byte multicodec + 32-byte key)
        if decoded.len() != 34 {
            return None;
        }

        // Verify Ed25519 multicodec prefix [0xed, 0x01]
        if decoded[0] != 0xed || decoded[1] != 0x01 {
            return None;
        }

        // Extract 32-byte public key
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&decoded[2..]);

        Some(Self::new(bytes))
    }

    /// Returns the raw public key bytes.
    pub fn bytes(&self) -> &[u8; 32] {
        &self.bytes
    }

    /// Formats principal as did:key using proper multicodec encoding.
    ///
    /// The did:key format for Ed25519 public keys is:
    /// 1. Prepend multicodec prefix `[0xed, 0x01]` to the 32-byte public key
    /// 2. Base58btc encode the 34-byte result
    /// 3. Prepend "z" (base58btc identifier) and wrap in "did:key:"
    ///
    /// The result is memoized after first computation.
    pub fn did(&self) -> &Did {
        self.did.get_or_init(|| {
            // Ed25519 public key multicodec prefix
            const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

            // Create 34-byte buffer: 2-byte prefix + 32-byte public key
            let mut multicodec_key = [0u8; 34];
            multicodec_key[..2].copy_from_slice(&ED25519_MULTICODEC);
            multicodec_key[2..].copy_from_slice(&self.bytes);

            // Base58btc encode (which starts with 'z' indicator in did:key format)
            let encoded = multicodec_key.to_base58();

            format!("did:key:z{}", encoded).into()
        })
    }
}

impl Clone for Principal {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes,
            did: OnceLock::new(),
        }
    }
}

impl PartialEq for Principal {
    fn eq(&self, other: &Self) -> bool {
        self.bytes == other.bytes
    }
}

impl Eq for Principal {}

impl PartialOrd for Principal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Principal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes.cmp(&other.bytes)
    }
}

impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.bytes.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes = <[u8; 32]>::deserialize(deserializer)?;
        Ok(Self::new(bytes))
    }
}

impl Debug for Principal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.did())
    }
}

impl TryFrom<Principal> for VerifyingKey {
    type Error = SignatureError;
    fn try_from(value: Principal) -> Result<Self, Self::Error> {
        VerifyingKey::from_bytes(&value.bytes)
    }
}

impl TryFrom<&Principal> for VerifyingKey {
    type Error = SignatureError;
    fn try_from(value: &Principal) -> Result<Self, Self::Error> {
        VerifyingKey::from_bytes(&value.bytes)
    }
}
