//! RSA DID principal and verifier.

use super::{RsaVerifyingKey, error::RsaDidFromStrError};
use dialog_varsig::{AlgorithmTag, AnySignature, Did, Principal, Verifier};
use multibase::Base;
use serde::{Deserialize, Deserializer, Serialize};
use std::str::FromStr;

/// The `rsa-pub` multicodec code `0x1205`, encoded as an unsigned varint.
const RSA_PUB_MULTICODEC: [u8; 2] = [0x85, 0x24];

/// The longest `did:key` multibase body this parser will decode.
///
/// The RSA arm is the only variable-length one, so unlike the curve arms it
/// would otherwise hand an attacker-chosen string straight to
/// `multibase::decode`. base58 decoding is quadratic in the input length, so
/// the cost would be the caller's to choose: measured in release, a
/// 100k-character `did:key` takes about 77ms, and 200k takes seconds. That is
/// reachable from unauthenticated input, because the authorizer resolves the
/// issuer DID of any submitted invocation and
/// [`Verifier::from_did_key`](crate::Verifier::from_did_key) falls through to
/// this arm after the curve arms bail.
///
/// The curve arms are immune only by accident: the `base58` crate's 132-byte
/// output buffer makes them refuse an over-long string in microseconds. This
/// is the deliberate equivalent. A 4096-bit RSA `did:key` is about 730
/// characters, so this leaves ample headroom for every real key.
const MAX_DID_KEY_MULTIBASE_LEN: usize = 1024;

/// An RSA `did:key`.
///
/// Unlike the fixed-width curve keys, the RSA public key is variable length: a
/// `did:key` carries the `rsa-pub` multicodec (`0x1205`) followed by the public
/// key in PKCS#1 DER (`RSAPublicKey`). The verifier stores the key size so it can
/// dispatch the correct signature width when verifying an [`AnySignature`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RsaVerifier(pub RsaVerifyingKey);

impl From<RsaVerifyingKey> for RsaVerifier {
    fn from(key: RsaVerifyingKey) -> Self {
        RsaVerifier(key)
    }
}

impl RsaVerifier {
    /// The agnostic algorithm tag for this key's size.
    #[must_use]
    pub const fn algorithm(&self) -> AlgorithmTag {
        self.0.size().algorithm_tag()
    }
}

impl std::fmt::Display for RsaVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let key_der = self.0.to_pkcs1_der();
        let mut raw_bytes = Vec::with_capacity(RSA_PUB_MULTICODEC.len() + key_der.len());
        raw_bytes.extend_from_slice(&RSA_PUB_MULTICODEC);
        raw_bytes.extend_from_slice(&key_der);
        // `multibase` base58btc encoding is the `z`-prefixed did:key form. Unlike
        // the `base58` 0.2 crate used for the fixed-width curve keys, it handles
        // the variable-length RSA modulus (whose base58 exceeds that crate's
        // fixed decode buffer).
        let encoded = multibase::encode(Base::Base58Btc, raw_bytes.as_slice());
        write!(f, "did:key:{encoded}")
    }
}

impl FromStr for RsaVerifier {
    type Err = RsaDidFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(':').collect();
        let did_tag = *parts.first().ok_or(RsaDidFromStrError::InvalidDidHeader)?;
        let key_tag = *parts.get(1).ok_or(RsaDidFromStrError::InvalidDidHeader)?;

        if parts.len() != 3 || did_tag != "did" || key_tag != "key" {
            return Err(RsaDidFromStrError::InvalidDidHeader);
        }
        let multibase_str = parts.get(2).ok_or(RsaDidFromStrError::InvalidDidHeader)?;
        if !multibase_str.starts_with('z') {
            return Err(RsaDidFromStrError::MissingBase58Prefix);
        }
        // Refuse on length before decoding: base58 is quadratic, so this is
        // what keeps the cost off the caller's choosing. See
        // `MAX_DID_KEY_MULTIBASE_LEN`.
        if multibase_str.len() > MAX_DID_KEY_MULTIBASE_LEN {
            return Err(RsaDidFromStrError::InvalidBase58);
        }
        let (base, raw) =
            multibase::decode(multibase_str).map_err(|_| RsaDidFromStrError::InvalidBase58)?;
        if base != Base::Base58Btc {
            return Err(RsaDidFromStrError::InvalidBase58);
        }
        // The RSA key body is variable length, so unlike the curve keys we take
        // a variable-length prefix (the multicodec) and treat all remaining
        // bytes as the PKCS#1 DER public key.
        let key_der = raw
            .strip_prefix(RSA_PUB_MULTICODEC.as_slice())
            .ok_or(RsaDidFromStrError::WrongMulticodec)?;
        let key =
            RsaVerifyingKey::from_pkcs1_der(key_der).map_err(|_| RsaDidFromStrError::InvalidKey)?;
        Ok(RsaVerifier(key))
    }
}

impl Verifier<AnySignature> for RsaVerifier {
    async fn verify(&self, msg: &[u8], signature: &AnySignature) -> Result<(), signature::Error> {
        if signature.algorithm() != self.algorithm() {
            return Err(signature::Error::new());
        }
        self.0.verify_bytes(msg, signature.to_bytes())
    }
}

impl Principal for RsaVerifier {
    fn did(&self) -> Did {
        self.to_string().parse().expect("valid DID string")
    }
}

impl Serialize for RsaVerifier {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RsaVerifier {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DidKeyVisitor;

        impl serde::de::Visitor<'_> for DidKeyVisitor {
            type Value = RsaVerifier;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a did:key string containing an RSA public key")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                v.parse()
                    .map_err(|e| E::custom(format!("invalid rsa did:key: {e}")))
            }
        }

        deserializer.deserialize_str(DidKeyVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rsa::RsaSigningKey;

    /// A cached 2048-bit RSA key. Generating one per test is far too slow.
    fn test_signing_key() -> RsaSigningKey {
        let der = include_bytes!("fixtures/test_2048.pkcs1.der");
        RsaSigningKey::from_pkcs1_der(der).unwrap()
    }

    #[dialog_common::test]
    fn rsa_did_display_roundtrip() {
        let vk = test_signing_key().verifying_key();
        let principal = RsaVerifier(vk);
        let did_string = principal.to_string();
        assert!(did_string.starts_with("did:key:z"));
        let parsed: RsaVerifier = did_string.parse().unwrap();
        assert_eq!(parsed, principal);
    }

    #[dialog_common::test]
    fn rsa_did_carries_rsa_pub_multicodec() {
        let vk = test_signing_key().verifying_key();
        let did = RsaVerifier(vk).to_string();
        let multibase_str = did.strip_prefix("did:key:").unwrap();
        let (base, raw) = multibase::decode(multibase_str).unwrap();
        assert_eq!(base, Base::Base58Btc);
        // 0x1205 as unsigned varint is [0x85, 0x24].
        assert_eq!(&raw[0..2], &RSA_PUB_MULTICODEC);
        // The key body is variable-length DER, much larger than a curve point.
        assert!(raw.len() > 200);
    }

    #[dialog_common::test]
    fn rsa_did_from_str_invalid_header() {
        let result: Result<RsaVerifier, _> = "not:a:did".parse();
        assert!(matches!(result, Err(RsaDidFromStrError::InvalidDidHeader)));
    }

    #[dialog_common::test]
    fn rsa_did_from_str_wrong_multicodec() {
        // A P-256 did:key (0x1200 multicodec) must be rejected by the RSA parser.
        let result: Result<RsaVerifier, _> =
            "did:key:zDnaerx9CtbPJ1q36T5Ln5wYt3MQYeGRG5ehnPAmxcf5mDZpv".parse();
        assert!(matches!(
            result,
            Err(RsaDidFromStrError::WrongMulticodec | RsaDidFromStrError::InvalidBase58)
        ));
    }

    /// An over-long `did:key` must be refused on length *before* it is decoded.
    ///
    /// The RSA arm is the only variable-length one, so unlike the curve arms it
    /// hands the whole string to `multibase::decode` with no bound. base58
    /// decoding is O(n^2), so the cost is set by an attacker-chosen string
    /// length. The fixed-width arms are immune only by accident: the `base58`
    /// crate's 132-byte output buffer makes them bail in microseconds.
    ///
    /// This is reachable from unauthenticated input. `Verifier::from_did_key`
    /// tries ed25519, WebAuthn, and es256 (each bailing immediately) and then
    /// falls through to this arm, and the authorizer resolves the issuer DID of
    /// any submitted invocation. A 4096-bit RSA `did:key` is about 730
    /// characters, so a cap well above that costs nothing legitimate.
    #[dialog_common::test]
    fn rsa_did_from_str_rejects_an_over_long_did_before_decoding() {
        // Far longer than any real RSA key, but still valid base58 characters.
        let oversized = format!("did:key:z{}", "1".repeat(100_000));

        let started = std::time::Instant::now();
        let result: Result<RsaVerifier, _> = oversized.parse();
        let elapsed = started.elapsed();

        assert!(result.is_err(), "an over-long did:key must not parse");
        assert!(
            elapsed < std::time::Duration::from_millis(50),
            "an over-long did:key must be refused on length before the O(n^2) \
             base58 decode; took {elapsed:?} for a 100k-character DID"
        );
    }
}
