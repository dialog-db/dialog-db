use super::{Formatter, SignatureError, ToBase58, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Cryptographic identifier like Ed25519 public key representing
/// a principal that produced a change.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Principal(pub(super) [u8; 32]);
impl Principal {
    /// Formats principal as did:key
    pub fn did(&self) -> String {
        const PREFIX: &str = "z6Mk";
        let id = [PREFIX, self.0.as_ref().to_base58().as_str()].concat();

        format!("did:key:{id}")
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
