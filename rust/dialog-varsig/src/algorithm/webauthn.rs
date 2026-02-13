//! WebAuthn signature algorithm wrapper.
//!
//! Wraps an inner signature algorithm (e.g., ECDSA P-256) with WebAuthn
//! authenticator context per the [varsig WebAuthn extension].
//!
//! [varsig WebAuthn extension]: https://github.com/ChainAgnostic/varsig/pull/11

use super::{SignatureAlgorithm, hash::Multihasher};
use crate::signature::Signature;
use signature::SignatureEncoding;
use std::marker::PhantomData;

/// Private-use multicodec tag identifying the WebAuthn wrapper.
pub const WEBAUTHN_MARKER: u64 = 0x300001;

/// Multicodec tag for the P-256 public key (p256-pub).
pub const P256_PUB_TAG: u64 = 0x1200;

/// WebAuthn signature algorithm wrapping an inner algorithm.
///
/// The varsig header encodes as:
/// `[inner_algo_prefix, curve_tag, 0x300001]`
///
/// The inner algorithm determines the actual cryptographic operation
/// (e.g., ECDSA P-256), while the WebAuthn wrapper indicates that the
/// signature carries `client_data_json` and `authenticator_data` context.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WebAuthn<C: WebAuthnCurve, H: Multihasher>(PhantomData<(C, H)>);

/// Curves usable within a WebAuthn context.
pub trait WebAuthnCurve: Sized {
    /// The inner algorithm prefix (e.g., `0xec` for ECDSA).
    const INNER_ALGO_PREFIX: u64;
    /// The curve multicodec tag (e.g., `0x1200` for P-256).
    const CURVE_TAG: u64;
}

#[cfg(feature = "secp256r1")]
impl WebAuthnCurve for super::curve::Secp256r1 {
    const INNER_ALGO_PREFIX: u64 = 0xec;
    const CURVE_TAG: u64 = P256_PUB_TAG;
}

/// WebAuthn with ECDSA P-256 and SHA-256 (the dominant passkey algorithm).
#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
pub type WebAuthnP256 = WebAuthn<super::curve::Secp256r1, super::hash::Sha2_256>;

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl SignatureAlgorithm for WebAuthnP256 {
    fn prefix(&self) -> u64 {
        <super::curve::Secp256r1 as WebAuthnCurve>::INNER_ALGO_PREFIX
    }

    fn config_tags(&self) -> Vec<u64> {
        vec![
            <super::curve::Secp256r1 as WebAuthnCurve>::CURVE_TAG,
            WEBAUTHN_MARKER,
        ]
    }

    fn try_from_tags(tags: &[u64]) -> Option<(Self, &[u64])> {
        if *tags.get(0..=2)? == [0xec, P256_PUB_TAG, WEBAUTHN_MARKER] {
            Some((Self::default(), tags.get(3..)?))
        } else {
            None
        }
    }
}

/// A WebAuthn signature carrying authenticator context alongside the inner
/// cryptographic signature.
///
/// On the wire this is encoded as:
/// ```text
/// varint(client_data_json.len) | client_data_json
/// | varint(authenticator_data.len) | authenticator_data
/// | signature_bytes
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebAuthnSignature {
    /// The `clientDataJSON` from the WebAuthn assertion, as raw bytes.
    pub client_data_json: Vec<u8>,
    /// The `authenticatorData` from the WebAuthn assertion, as raw bytes.
    pub authenticator_data: Vec<u8>,
    /// The raw inner signature bytes (e.g., DER-encoded ECDSA for P-256).
    pub signature: Vec<u8>,
}

impl WebAuthnSignature {
    /// Create a new `WebAuthnSignature`.
    #[must_use]
    pub fn new(client_data_json: Vec<u8>, authenticator_data: Vec<u8>, signature: Vec<u8>) -> Self {
        Self {
            client_data_json,
            authenticator_data,
            signature,
        }
    }

    /// Encode the signature to a byte vector using varint-length-prefixed fields.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        leb128::write::unsigned(&mut buf, self.client_data_json.len() as u64)
            .expect("write to Vec never fails");
        buf.extend_from_slice(&self.client_data_json);
        leb128::write::unsigned(&mut buf, self.authenticator_data.len() as u64)
            .expect("write to Vec never fails");
        buf.extend_from_slice(&self.authenticator_data);
        buf.extend_from_slice(&self.signature);
        buf
    }

    /// Decode from bytes (varint-length-prefixed client data, auth data, then remaining = signature).
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if the encoding is malformed.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let mut cursor = std::io::Cursor::new(bytes);
        let cd_len = leb128::read::unsigned(&mut cursor).map_err(|_| signature::Error::new())?;
        let pos = cursor.position() as usize;
        let cd_end = pos
            .checked_add(cd_len as usize)
            .ok_or_else(signature::Error::new)?;
        if cd_end > bytes.len() {
            return Err(signature::Error::new());
        }
        let client_data_json = bytes[pos..cd_end].to_vec();
        cursor.set_position(cd_end as u64);

        let ad_len = leb128::read::unsigned(&mut cursor).map_err(|_| signature::Error::new())?;
        let pos2 = cursor.position() as usize;
        let ad_end = pos2
            .checked_add(ad_len as usize)
            .ok_or_else(signature::Error::new)?;
        if ad_end > bytes.len() {
            return Err(signature::Error::new());
        }
        let authenticator_data = bytes[pos2..ad_end].to_vec();

        let signature = bytes[ad_end..].to_vec();
        if signature.is_empty() {
            return Err(signature::Error::new());
        }

        Ok(Self {
            client_data_json,
            authenticator_data,
            signature,
        })
    }
}

impl SignatureEncoding for WebAuthnSignature {
    type Repr = Box<[u8]>;
}

impl TryFrom<&[u8]> for WebAuthnSignature {
    type Error = signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(bytes)
    }
}

impl From<WebAuthnSignature> for Box<[u8]> {
    fn from(sig: WebAuthnSignature) -> Self {
        sig.to_vec().into_boxed_slice()
    }
}

impl serde::Serialize for WebAuthnSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde_bytes::serialize(&self.to_vec(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for WebAuthnSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: serde_bytes::ByteBuf = serde::Deserialize::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

#[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
impl Signature for WebAuthnSignature {
    type Algorithm = WebAuthnP256;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webauthn_signature_roundtrip() {
        let sig = WebAuthnSignature::new(
            b"client-data".to_vec(),
            b"auth-data".to_vec(),
            b"signature-bytes".to_vec(),
        );

        let encoded = sig.to_vec();
        let decoded = WebAuthnSignature::from_bytes(&encoded).unwrap();

        assert_eq!(decoded.client_data_json, b"client-data");
        assert_eq!(decoded.authenticator_data, b"auth-data");
        assert_eq!(decoded.signature, b"signature-bytes");
    }

    #[test]
    fn webauthn_signature_empty_signature_fails() {
        // Construct bytes with zero-length signature portion
        let mut buf = Vec::new();
        leb128::write::unsigned(&mut buf, 2).unwrap();
        buf.extend_from_slice(b"cd");
        leb128::write::unsigned(&mut buf, 2).unwrap();
        buf.extend_from_slice(b"ad");
        // no signature bytes after auth data

        assert!(WebAuthnSignature::from_bytes(&buf).is_err());
    }

    #[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
    #[test]
    fn webauthn_p256_algorithm_tags() {
        let algo = WebAuthnP256::default();
        assert_eq!(algo.prefix(), 0xec);
        assert_eq!(algo.config_tags(), vec![P256_PUB_TAG, WEBAUTHN_MARKER]);
    }

    #[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
    #[test]
    fn webauthn_p256_try_from_tags() {
        let tags = [0xec, P256_PUB_TAG, WEBAUTHN_MARKER, 0x71];
        let (algo, rest) = WebAuthnP256::try_from_tags(&tags).unwrap();
        assert_eq!(algo, WebAuthnP256::default());
        assert_eq!(rest, &[0x71]);
    }

    #[cfg(all(feature = "secp256r1", feature = "sha2_256"))]
    #[test]
    fn webauthn_p256_rejects_non_webauthn() {
        // Regular ES256 tags should not parse as WebAuthn
        let tags = [0xec, 0x1201, 0x15, 0x71];
        assert!(WebAuthnP256::try_from_tags(&tags).is_none());
    }
}
