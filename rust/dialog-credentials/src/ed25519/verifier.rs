//! Ed25519 DID principal and verifier.

use super::{Ed25519Signature, Ed25519VerifyingKey, error::Ed25519DidFromStrError};
use base58::ToBase58;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;
use varsig::{Did, Principal, Verifier};

/// An `Ed25519` `did:key`.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_copy_implementations)] // Ed25519VerifyingKey is not Copy on WASM
pub struct Ed25519Verifier(pub Ed25519VerifyingKey);

impl From<Ed25519VerifyingKey> for Ed25519Verifier {
    fn from(key: Ed25519VerifyingKey) -> Self {
        Ed25519Verifier(key)
    }
}

impl From<ed25519_dalek::VerifyingKey> for Ed25519Verifier {
    fn from(key: ed25519_dalek::VerifyingKey) -> Self {
        Ed25519Verifier(Ed25519VerifyingKey::Native(key))
    }
}

impl From<ed25519_dalek::SigningKey> for Ed25519Verifier {
    fn from(key: ed25519_dalek::SigningKey) -> Self {
        let verifying_key = Ed25519VerifyingKey::Native(key.verifying_key());
        Ed25519Verifier(verifying_key)
    }
}

impl std::fmt::Display for Ed25519Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut raw_bytes = Vec::with_capacity(34);
        raw_bytes.push(0xed);
        raw_bytes.push(0x01);
        raw_bytes.extend_from_slice(&self.0.to_bytes());
        let b58 = ToBase58::to_base58(raw_bytes.as_slice());
        write!(f, "did:key:z{b58}")
    }
}

impl FromStr for Ed25519Verifier {
    type Err = Ed25519DidFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let did_tag = *parts
            .first()
            .ok_or(Ed25519DidFromStrError::InvalidDidHeader)?;
        let key_tag = *parts
            .get(1)
            .ok_or(Ed25519DidFromStrError::InvalidDidHeader)?;

        if parts.len() != 3 || did_tag != "did" || key_tag != "key" {
            return Err(Ed25519DidFromStrError::InvalidDidHeader);
        }
        let b58 = parts
            .get(2)
            .ok_or(Ed25519DidFromStrError::InvalidDidHeader)?
            .strip_prefix('z')
            .ok_or(Ed25519DidFromStrError::MissingBase58Prefix)?;
        let key_bytes =
            base58::FromBase58::from_base58(b58).map_err(|_| Ed25519DidFromStrError::InvalidKey)?;
        let raw_arr = <[u8; 34]>::try_from(key_bytes.as_slice())
            .map_err(|_| Ed25519DidFromStrError::InvalidKey)?;
        if raw_arr[0] != 0xed || raw_arr[1] != 0x01 {
            return Err(Ed25519DidFromStrError::InvalidKey);
        }
        let key_arr: [u8; 32] = raw_arr[2..]
            .try_into()
            .map_err(|_| Ed25519DidFromStrError::InvalidKey)?;
        let key = ed25519_dalek::VerifyingKey::from_bytes(&key_arr)
            .map_err(|_| Ed25519DidFromStrError::InvalidKey)?;
        Ok(Ed25519Verifier(Ed25519VerifyingKey::Native(key)))
    }
}

impl Verifier<Ed25519Signature> for Ed25519Verifier {
    async fn verify(
        &self,
        msg: &[u8],
        signature: &Ed25519Signature,
    ) -> Result<(), signature::Error> {
        self.0.verify_signature(msg, signature).await
    }
}

impl Principal for Ed25519Verifier {
    fn did(&self) -> Did {
        self.to_string().parse().expect("valid DID string")
    }
}

impl Serialize for Ed25519Verifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Ed25519Verifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DidKeyVisitor;

        impl serde::de::Visitor<'_> for DidKeyVisitor {
            type Value = Ed25519Verifier;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a did:key string containing an ed25519 public key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                const DID_PREFIX: &str = "did:key:z";
                const ED25519_PUB: [u8; 2] = [0xED, 0x01];

                if !v.starts_with(DID_PREFIX) {
                    return Err(E::custom("expected did:key with base58btc (did:key:z…)"));
                }

                let b58_payload = &v[DID_PREFIX.len()..];
                let decoded = base58::FromBase58::from_base58(b58_payload)
                    .map_err(|e| E::custom(format!("base58 decode failed: {e:?}")))?;

                if decoded.len() != 34 {
                    return Err(E::custom(format!(
                        "unexpected byte length: got {}, want 34 (2-byte header + 32-byte key)",
                        decoded.len()
                    )));
                }

                let leading = decoded.get(0..2).ok_or_else(|| {
                    E::custom("decoded did:key payload too short to contain multicodec header")
                })?;

                if leading != ED25519_PUB {
                    return Err(E::custom("not an ed25519-pub multicodec (0xED 0x01)"));
                }

                let remainder = decoded.get(2..).ok_or_else(|| {
                    E::custom("decoded did:key payload too short to contain ed25519 public key")
                })?;

                #[allow(clippy::expect_used)]
                let key_bytes: [u8; 32] =
                    remainder.try_into().expect("slice length verified above");

                let vk = ed25519_dalek::VerifyingKey::from_bytes(&key_bytes).map_err(|e| {
                    E::custom(format!(
                        "failed to construct ed25519 public key from bytes: {e:?}"
                    ))
                })?;

                Ok(Ed25519Verifier(Ed25519VerifyingKey::Native(vk)))
            }
        }

        deserializer.deserialize_str(DidKeyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a deterministic verifying key from a seed.
    fn test_verifying_key(seed: u8) -> Ed25519VerifyingKey {
        let signing_key = ed25519_dalek::SigningKey::from_bytes(&[seed; 32]);
        Ed25519VerifyingKey::Native(signing_key.verifying_key())
    }

    #[dialog_common::test]
    fn ed25519_did_display_roundtrip() {
        let vk = test_verifying_key(0);
        let principal = Ed25519Verifier(vk);
        let did_string = principal.to_string();
        let parsed: Ed25519Verifier = did_string.parse().unwrap();
        assert_eq!(parsed, principal);
    }

    #[dialog_common::test]
    fn ed25519_did_from_str_invalid_header() {
        let result: Result<Ed25519Verifier, _> = "not:a:did".parse();
        assert!(matches!(
            result,
            Err(Ed25519DidFromStrError::InvalidDidHeader)
        ));
    }

    #[dialog_common::test]
    fn ed25519_did_from_str_missing_prefix() {
        let result: Result<Ed25519Verifier, _> = "did:key:abc".parse();
        assert!(matches!(
            result,
            Err(Ed25519DidFromStrError::MissingBase58Prefix)
        ));
    }
}
