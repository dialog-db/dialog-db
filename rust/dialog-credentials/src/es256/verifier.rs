//! ES256 DID principal and verifier.

use super::{Es256VerifyingKey, error::Es256DidFromStrError};
use base58::ToBase58;
use dialog_varsig::{Did, Principal, Verifier, ecdsa::Es256Signature};
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;

/// The `p256-pub` multicodec code `0x1200`, encoded as an unsigned varint.
const P256_PUB_MULTICODEC: [u8; 2] = [0x80, 0x24];

/// An `ES256` (`P-256`) `did:key`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Es256Verifier(pub Es256VerifyingKey);

impl From<Es256VerifyingKey> for Es256Verifier {
    fn from(key: Es256VerifyingKey) -> Self {
        Es256Verifier(key)
    }
}

impl From<p256::ecdsa::VerifyingKey> for Es256Verifier {
    fn from(key: p256::ecdsa::VerifyingKey) -> Self {
        Es256Verifier(Es256VerifyingKey::Native(key))
    }
}

impl std::fmt::Display for Es256Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let compressed = self.0.to_compressed_bytes();
        let mut raw_bytes = Vec::with_capacity(P256_PUB_MULTICODEC.len() + compressed.len());
        raw_bytes.extend_from_slice(&P256_PUB_MULTICODEC);
        raw_bytes.extend_from_slice(&compressed);
        let b58 = ToBase58::to_base58(raw_bytes.as_slice());
        write!(f, "did:key:z{b58}")
    }
}

impl FromStr for Es256Verifier {
    type Err = Es256DidFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let did_tag = *parts
            .first()
            .ok_or(Es256DidFromStrError::InvalidDidHeader)?;
        let key_tag = *parts.get(1).ok_or(Es256DidFromStrError::InvalidDidHeader)?;

        if parts.len() != 3 || did_tag != "did" || key_tag != "key" {
            return Err(Es256DidFromStrError::InvalidDidHeader);
        }
        let b58 = parts
            .get(2)
            .ok_or(Es256DidFromStrError::InvalidDidHeader)?
            .strip_prefix('z')
            .ok_or(Es256DidFromStrError::MissingBase58Prefix)?;
        let raw = base58::FromBase58::from_base58(b58)
            .map_err(|_| Es256DidFromStrError::InvalidBase58)?;
        let raw_arr =
            <[u8; 35]>::try_from(raw.as_slice()).map_err(|_| Es256DidFromStrError::InvalidKey)?;
        if raw_arr[0..2] != P256_PUB_MULTICODEC {
            return Err(Es256DidFromStrError::InvalidKey);
        }
        let key = p256::ecdsa::VerifyingKey::from_sec1_bytes(&raw_arr[2..])
            .map_err(|_| Es256DidFromStrError::InvalidKey)?;
        Ok(Es256Verifier(Es256VerifyingKey::Native(key)))
    }
}

impl Verifier<Es256Signature> for Es256Verifier {
    async fn verify(&self, msg: &[u8], signature: &Es256Signature) -> Result<(), signature::Error> {
        self.0.verify_signature(msg, signature).await
    }
}

impl Principal for Es256Verifier {
    fn did(&self) -> Did {
        self.to_string().parse().expect("valid DID string")
    }
}

impl Serialize for Es256Verifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Es256Verifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DidKeyVisitor;

        impl serde::de::Visitor<'_> for DidKeyVisitor {
            type Value = Es256Verifier;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a did:key string containing a P-256 public key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse()
                    .map_err(|e| E::custom(format!("invalid es256 did:key: {e}")))
            }
        }

        deserializer.deserialize_str(DidKeyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_verifying_key(seed: u8) -> Es256VerifyingKey {
        let signing_key = p256::ecdsa::SigningKey::from_slice(&[seed; 32]).unwrap();
        Es256VerifyingKey::Native(*signing_key.verifying_key())
    }

    #[dialog_common::test]
    fn es256_did_display_roundtrip() {
        let vk = test_verifying_key(1);
        let principal = Es256Verifier(vk);
        let did_string = principal.to_string();
        assert!(did_string.starts_with("did:key:z"));
        let parsed: Es256Verifier = did_string.parse().unwrap();
        assert_eq!(parsed, principal);
    }

    #[dialog_common::test]
    fn es256_did_from_str_invalid_header() {
        let result: Result<Es256Verifier, _> = "not:a:did".parse();
        assert!(matches!(
            result,
            Err(Es256DidFromStrError::InvalidDidHeader)
        ));
    }

    #[dialog_common::test]
    fn es256_did_from_str_missing_prefix() {
        let result: Result<Es256Verifier, _> = "did:key:abc".parse();
        assert!(matches!(
            result,
            Err(Es256DidFromStrError::MissingBase58Prefix)
        ));
    }
}
