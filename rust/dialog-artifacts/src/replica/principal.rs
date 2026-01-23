use super::{Formatter, SignatureError, ToBase58, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Cryptographic identifier like Ed25519 public key representing
/// a principal that produced a change.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Principal(pub(super) [u8; 32]);
impl Principal {
    /// Formats principal as did:key using proper multicodec encoding.
    ///
    /// The did:key format for Ed25519 public keys is:
    /// 1. Prepend multicodec prefix `[0xed, 0x01]` to the 32-byte public key
    /// 2. Base58btc encode the 34-byte result
    /// 3. Prepend "z" (base58btc identifier) and wrap in "did:key:"
    pub fn did(&self) -> String {
        // Ed25519 public key multicodec prefix
        const ED25519_MULTICODEC: [u8; 2] = [0xed, 0x01];

        // Create 34-byte buffer: 2-byte prefix + 32-byte public key
        let mut multicodec_key = [0u8; 34];
        multicodec_key[..2].copy_from_slice(&ED25519_MULTICODEC);
        multicodec_key[2..].copy_from_slice(&self.0);

        // Base58btc encode (which starts with 'z' indicator in did:key format)
        let encoded = multicodec_key.to_base58();

        format!("did:key:z{}", encoded)
    }
}
impl Debug for Principal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.did())
    }
}

impl TryFrom<Principal> for VerifyingKey {
    type Error = SignatureError;
    fn try_from(value: Principal) -> Result<Self, Self::Error> {
        VerifyingKey::from_bytes(&value.0)
    }
}
