//! WebAuthn signature algorithm.
//!
//! WebAuthn ("passkey") assertions are not a plain signature over the payload:
//! the authenticator signs `authenticatorData || SHA-256(clientDataJSON)`, and
//! the payload only appears indirectly as the challenge embedded in
//! `clientDataJSON`. A [`WebAuthnSignature`] therefore carries the
//! `clientDataJSON` and `authenticatorData` context alongside the inner
//! signature so a verifier can reconstruct and check the signed message.
//!
//! Per the [varsig WebAuthn extension], WebAuthn is a *wrapper* over an inner
//! signature algorithm, not a P-256-specific algorithm. The header is the inner
//! algorithm's tags followed by the WebAuthn marker `0x300001`:
//! `[inner_prefix, inner_config_tags..., 0x300001]`. Wrapping a different inner
//! algorithm (EdDSA, RSA, ...) is therefore adding a type parameter binding,
//! not restructuring: [`WebAuthn<Inner>`] composes with whatever `Inner`'s tags
//! are. The `0x300001` marker is a private-use placeholder; no official
//! multicodec has been allocated for the WebAuthn wrapper yet.
//!
//! [`WebAuthnP256`] is the concrete instance implemented today: WebAuthn
//! wrapping [`Es256`](super::ecdsa::Es256), the dominant passkey algorithm.
//! Its header `[0xec, 0x1201, 0x15, 0x300001]` extends the plain ES256 header
//! with the marker, so the two never collide.
//!
//! [varsig WebAuthn extension]: https://github.com/ChainAgnostic/varsig/pull/11

use super::SignatureAlgorithm;
use crate::signature::Signature;
use signature::SignatureEncoding;
use std::marker::PhantomData;

/// Private-use multicodec tag marking a varsig header as a WebAuthn wrapper.
///
/// It is appended to the inner algorithm's tags. No official multicodec has
/// been allocated for the WebAuthn wrapper, so this is a private-use value.
pub const WEBAUTHN_MARKER: u64 = 0x300001;

/// WebAuthn wrapper over an inner signature algorithm `Inner`.
///
/// The varsig header is the inner algorithm's `[prefix, config_tags...]`
/// followed by [`WEBAUTHN_MARKER`]. The inner algorithm determines the actual
/// cryptographic operation, while the marker indicates the signature carries
/// `clientDataJSON` and `authenticatorData` context.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WebAuthn<Inner: SignatureAlgorithm>(PhantomData<Inner>);

impl<Inner: SignatureAlgorithm> SignatureAlgorithm for WebAuthn<Inner> {
    fn prefix(&self) -> u64 {
        Inner::default().prefix()
    }

    fn config_tags(&self) -> Vec<u64> {
        // Inner algorithm's config tags, then the WebAuthn marker.
        let mut tags = Inner::default().config_tags();
        tags.push(WEBAUTHN_MARKER);
        tags
    }

    fn try_from_tags(tags: &[u64]) -> Option<(Self, &[u64])> {
        // Parse the inner algorithm's full header (prefix + config tags), then
        // require the WebAuthn marker immediately after. Only then is this a
        // WebAuthn wrapper; a bare inner header (no marker) is not.
        let (_, rest) = Inner::try_from_tags(tags)?;
        let (marker, rest) = rest.split_first()?;
        if *marker == WEBAUTHN_MARKER {
            Some((Self::default(), rest))
        } else {
            None
        }
    }
}

/// WebAuthn wrapping ES256 (ECDSA P-256 + SHA-256), the dominant passkey case.
pub type WebAuthnP256 = WebAuthn<super::ecdsa::Es256>;

/// A WebAuthn signature carrying authenticator context alongside the inner
/// ECDSA signature.
///
/// On the wire this is encoded as varint-length-prefixed fields:
///
/// ```text
/// varint(client_data_json.len) | client_data_json
/// | varint(authenticator_data.len) | authenticator_data
/// | signature_bytes
/// ```
///
/// The inner `signature` is DER-encoded ECDSA, exactly as a browser
/// authenticator emits it in a WebAuthn assertion (the verifier parses it with
/// `p256::ecdsa::DerSignature`). Unlike [`Es256Signature`](super::ecdsa::Es256Signature),
/// which is fixed-width raw `r || s`, a WebAuthn signature is variable length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebAuthnSignature {
    /// The `clientDataJSON` from the WebAuthn assertion, as raw bytes.
    pub client_data_json: Vec<u8>,
    /// The `authenticatorData` from the WebAuthn assertion, as raw bytes.
    pub authenticator_data: Vec<u8>,
    /// The inner DER-encoded ECDSA signature bytes.
    pub signature: Vec<u8>,
}

impl WebAuthnSignature {
    /// Create a new [`WebAuthnSignature`].
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

    /// Decode from bytes: varint-length-prefixed client data, then auth data,
    /// then the remaining bytes as the inner signature.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if the encoding is malformed or the inner
    /// signature portion is empty.
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
        let mut buf = Vec::new();
        leb128::write::unsigned(&mut buf, 2).unwrap();
        buf.extend_from_slice(b"cd");
        leb128::write::unsigned(&mut buf, 2).unwrap();
        buf.extend_from_slice(b"ad");
        // no signature bytes after auth data

        assert!(WebAuthnSignature::from_bytes(&buf).is_err());
    }

    #[test]
    fn webauthn_p256_header_is_inner_tags_plus_marker() {
        use crate::algorithm::ecdsa::Es256;

        let algo = WebAuthnP256::default();
        assert_eq!(algo.prefix(), Es256::default().prefix());

        // config_tags = Es256's config tags, then the WebAuthn marker.
        let mut expected = Es256::default().config_tags();
        expected.push(WEBAUTHN_MARKER);
        assert_eq!(algo.config_tags(), expected);
    }

    #[test]
    fn webauthn_p256_try_from_tags() {
        use crate::algorithm::ecdsa::Es256;

        // Full header: [prefix, inner config tags..., marker, trailing].
        let mut tags = vec![Es256::default().prefix()];
        tags.extend(Es256::default().config_tags());
        tags.push(WEBAUTHN_MARKER);
        tags.push(0x71); // trailing codec tag

        let (algo, rest) = WebAuthnP256::try_from_tags(&tags).unwrap();
        assert_eq!(algo, WebAuthnP256::default());
        assert_eq!(rest, &[0x71]);
    }

    #[test]
    fn webauthn_p256_rejects_bare_es256_header() {
        use crate::algorithm::ecdsa::Es256;

        // Plain ES256 header WITHOUT the marker must not parse as WebAuthn.
        let mut tags = vec![Es256::default().prefix()];
        tags.extend(Es256::default().config_tags());
        tags.push(0x71);
        assert!(WebAuthnP256::try_from_tags(&tags).is_none());
    }
}
