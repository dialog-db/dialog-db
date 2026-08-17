//! DKIM signature algorithm (did:mailto proofs).
//!
//! A did:mailto identity does not sign UCAN payloads directly. Instead, a
//! one-time DKIM-signed email whose subject reads `I am also known as {did:key}`
//! binds the email identity to a `did:key`, which then signs everything. A
//! [`DkimSignature`] is the captured proof of that email: the `DKIM-Signature`
//! header the provider emitted plus the signed header values, enough to
//! re-verify the header signature `b=` offline against the domain's DKIM key.
//! The body is never carried (see [`dialog_dkim`]).
//!
//! Unlike [`WebAuthn`](super::webauthn::WebAuthn), which wraps an inner
//! signature algorithm, DKIM is standalone: the inner algorithm (`rsa-sha256` or
//! `ed25519-sha256`) is named *inside* the captured `DKIM-Signature` header's
//! `a=` tag, not in the varsig header, because a verifier does not choose it. So
//! the varsig header is just a prefix plus the DKIM marker.
//!
//! The [`DKIM_MARKER`] `0x300002` is a **private-use placeholder**: no official
//! multicodec has been allocated for a DKIM / did:mailto varsig wrapper. It sits
//! next to the WebAuthn placeholder `0x300001` in the same private-use range.

use super::SignatureAlgorithm;
use crate::signature::Signature;
use dialog_dkim::{DkimSignatureHeader, Header, SignedEmail};
use signature::SignatureEncoding;

/// Private-use multicodec tag marking a varsig header as a DKIM did:mailto proof.
///
/// No official multicodec has been allocated, so this is a private-use value in
/// the same range as the WebAuthn marker (`0x300001`).
pub const DKIM_MARKER: u64 = 0x300002;

/// The DKIM signature algorithm descriptor.
///
/// The varsig header is `[DKIM_MARKER]`: DKIM is standalone (the inner algorithm
/// lives in the captured `DKIM-Signature` header, not the varsig header), so
/// there is no inner prefix to compose with.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dkim;

impl SignatureAlgorithm for Dkim {
    fn prefix(&self) -> u64 {
        DKIM_MARKER
    }

    fn config_tags(&self) -> Vec<u64> {
        // No configuration tags: everything DKIM needs travels in the signature
        // body (the captured DKIM-Signature header and signed headers).
        Vec::new()
    }

    fn try_from_tags(tags: &[u64]) -> Option<(Self, &[u64])> {
        let (marker, rest) = tags.split_first()?;
        if *marker == DKIM_MARKER {
            Some((Self, rest))
        } else {
            None
        }
    }
}

/// A captured DKIM proof carried as a varsig signature.
///
/// The wire form is a length-prefixed encoding of the captured proof: the raw
/// `DKIM-Signature` header value, then the count of signed headers, then each
/// signed header as a length-prefixed name and value. Every length is an
/// unsigned LEB128 varint, matching the [`WebAuthnSignature`] convention.
///
/// [`WebAuthnSignature`]: super::webauthn::WebAuthnSignature
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DkimSignature {
    /// The captured, portable proof: the parsed `DKIM-Signature` header and the
    /// signed header values, with no body.
    pub proof: SignedEmail,
}

impl DkimSignature {
    /// Wrap a captured proof as a varsig signature.
    #[must_use]
    pub fn new(proof: SignedEmail) -> Self {
        Self { proof }
    }

    /// Encode the captured proof to its length-prefixed byte form.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_bytes(&mut buf, self.proof.signature.raw_header_value.as_bytes());
        write_len(&mut buf, self.proof.signed_headers.len() as u64);
        for header in &self.proof.signed_headers {
            write_bytes(&mut buf, header.name.as_bytes());
            write_bytes(&mut buf, header.raw_value.as_bytes());
        }
        buf
    }

    /// Decode a captured proof from its length-prefixed byte form.
    ///
    /// # Errors
    ///
    /// Returns `signature::Error` if the encoding is malformed or the
    /// `DKIM-Signature` header does not parse.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, signature::Error> {
        let mut cursor = 0usize;
        let raw_header = read_bytes(bytes, &mut cursor)?;
        let raw_header = String::from_utf8(raw_header).map_err(|_| signature::Error::new())?;

        let count = read_len(bytes, &mut cursor)?;
        let mut signed_headers = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let name = read_bytes(bytes, &mut cursor)?;
            let value = read_bytes(bytes, &mut cursor)?;
            signed_headers.push(Header {
                name: String::from_utf8(name).map_err(|_| signature::Error::new())?,
                raw_value: String::from_utf8(value).map_err(|_| signature::Error::new())?,
            });
        }

        let signature =
            DkimSignatureHeader::parse(&raw_header).map_err(|_| signature::Error::new())?;
        Ok(Self {
            proof: SignedEmail {
                signature,
                signed_headers,
            },
        })
    }
}

fn write_len(buf: &mut Vec<u8>, len: u64) {
    leb128::write::unsigned(buf, len).expect("write to Vec never fails");
}

fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_len(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

fn read_len(bytes: &[u8], cursor: &mut usize) -> Result<u64, signature::Error> {
    let mut slice = &bytes[*cursor..];
    let start_len = slice.len();
    let value = leb128::read::unsigned(&mut slice).map_err(|_| signature::Error::new())?;
    *cursor += start_len - slice.len();
    Ok(value)
}

fn read_bytes(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, signature::Error> {
    let len = read_len(bytes, cursor)? as usize;
    let end = cursor.checked_add(len).ok_or_else(signature::Error::new)?;
    if end > bytes.len() {
        return Err(signature::Error::new());
    }
    let out = bytes[*cursor..end].to_vec();
    *cursor = end;
    Ok(out)
}

impl SignatureEncoding for DkimSignature {
    type Repr = Box<[u8]>;
}

impl TryFrom<&[u8]> for DkimSignature {
    type Error = signature::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(bytes)
    }
}

impl From<DkimSignature> for Box<[u8]> {
    fn from(sig: DkimSignature) -> Self {
        sig.to_vec().into_boxed_slice()
    }
}

impl serde::Serialize for DkimSignature {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde_bytes::serialize(&self.to_vec(), serializer)
    }
}

impl<'de> serde::Deserialize<'de> for DkimSignature {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let bytes: serde_bytes::ByteBuf = serde::Deserialize::deserialize(deserializer)?;
        Self::from_bytes(&bytes).map_err(serde::de::Error::custom)
    }
}

impl Signature for DkimSignature {
    type Algorithm = Dkim;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_proof() -> SignedEmail {
        let raw = " v=1; a=rsa-sha256; c=relaxed/simple; d=example.com; s=sel; \
                   h=from:subject; bh=AAAA; b=BBBB";
        let signature = DkimSignatureHeader::parse(raw).unwrap();
        SignedEmail {
            signature,
            signed_headers: vec![
                Header {
                    name: "From".into(),
                    raw_value: " alice@example.com".into(),
                },
                Header {
                    name: "Subject".into(),
                    raw_value: " I am also known as did:key:z6Mkabc".into(),
                },
            ],
        }
    }

    #[test]
    fn dkim_header_is_marker_only() {
        let algo = Dkim;
        assert_eq!(algo.prefix(), DKIM_MARKER);
        assert!(algo.config_tags().is_empty());
    }

    #[test]
    fn dkim_try_from_tags_consumes_marker() {
        let tags = [DKIM_MARKER, 0x71];
        let (algo, rest) = Dkim::try_from_tags(&tags).unwrap();
        assert_eq!(algo, Dkim);
        assert_eq!(rest, &[0x71]);
    }

    #[test]
    fn dkim_rejects_non_marker_header() {
        assert!(Dkim::try_from_tags(&[0x1234]).is_none());
        assert!(Dkim::try_from_tags(&[]).is_none());
    }

    #[test]
    fn dkim_signature_roundtrips_through_bytes() {
        let sig = DkimSignature::new(sample_proof());
        let encoded = sig.to_vec();
        let decoded = DkimSignature::from_bytes(&encoded).unwrap();
        assert_eq!(decoded, sig);
        assert_eq!(decoded.proof.signature.domain, "example.com");
        assert_eq!(decoded.proof.signed_headers.len(), 2);
    }

    #[test]
    fn dkim_signature_rejects_truncated_bytes() {
        let sig = DkimSignature::new(sample_proof());
        let encoded = sig.to_vec();
        assert!(DkimSignature::from_bytes(&encoded[..encoded.len() / 2]).is_err());
    }
}
