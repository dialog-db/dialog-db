//! Reconstruct the DKIM-signed header string (RFC 6376 section 3.7) and verify
//! the header signature `b=` with the domain public key.
//!
//! # What gets hashed
//!
//! For each header name in the `h=` list, in order, the signer canonicalized the
//! corresponding header (per the header half of `c=`) and appended it, each
//! terminated by CRLF. Finally the `DKIM-Signature` header itself is
//! canonicalized with its `b=` value emptied and appended **without** a trailing
//! CRLF. The signature `b=` is over that whole byte string, run through the
//! inner algorithm (`rsa-sha256` = RSA PKCS#1 v1.5 over SHA-256;
//! `ed25519-sha256` = Ed25519 over the SHA-256 of the byte string).
//!
//! # Body hash `bh=` is trusted, not recomputed
//!
//! The proof carries no body, so the body hash `bh=` cannot be recomputed here.
//! It does not need to be: `bh=` is a tag **inside** the `DKIM-Signature`
//! header, which is itself part of the signed string, so a valid `b=` proves the
//! signer committed to that `bh=`. Our security claim is about the signed
//! `From:`/`Subject:` headers, not the body, so verifying `b=` over the captured
//! headers is exactly the guarantee we want. This is the private-use, no-body
//! caveat: we intentionally do not fetch or re-hash a body.

use super::canonicalize::Canonicalization;
use super::error::DkimError;
use super::key::DkimPublicKey;
use super::message::{Header, Message};
use super::signature::{DkimSignatureHeader, SignatureAlgorithm};

/// The DKIM header name.
const DKIM_SIGNATURE: &str = "DKIM-Signature";

/// A captured, portable DKIM proof: the signed header values the signer emitted
/// plus the `DKIM-Signature` header, with **no body**.
///
/// This is what a did:mailto proof carries. It is deliberately the minimum
/// needed to re-verify `b=`: the `DKIM-Signature` header's raw value (which
/// includes `bh=`, so the body hash rides along for free) and, for each header
/// named in `h=`, the exact `(name, raw_value)` the signer signed over. The
/// body is never included.
///
/// # Structure for a real `.eml`
///
/// A raw Gmail `.eml` is parsed into this shape by [`SignedEmail::from_raw_eml`].
/// Once captured, verification ([`verify`]) needs only the domain public key and
/// no longer the full email, so the portable proof drops the body and the
/// unsigned headers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedEmail {
    /// The parsed `DKIM-Signature` header.
    pub signature: DkimSignatureHeader,
    /// The signed headers, as the signer emitted them, keyed by the `h=` list.
    /// Each entry is `(field-name, raw-field-value)` with folding preserved, so
    /// both `simple` and `relaxed` canonicalization can be reproduced.
    ///
    /// Bottom-up order per RFC 6376: index `i` is the header that satisfies the
    /// `i`-th name in `h=`, walking the email from the bottom for repeats.
    pub signed_headers: Vec<Header>,
}

impl SignedEmail {
    /// Extract the portable proof from a raw email, capturing only the
    /// `DKIM-Signature` header and the signed header values (no body).
    ///
    /// # Errors
    ///
    /// Returns [`DkimError`] if the email or its `DKIM-Signature` header is
    /// malformed, or a header listed in `h=` is absent.
    pub fn from_raw_eml(raw: &[u8]) -> Result<Self, DkimError> {
        let message = Message::parse(raw)?;
        let dkim_header = message
            .last_header(DKIM_SIGNATURE)
            .ok_or(DkimError::NoSignatureHeader)?;
        let signature = DkimSignatureHeader::parse(&dkim_header.raw_value)?;
        let signed_headers = select_signed_headers(&message, &signature)?;
        Ok(Self {
            signature,
            signed_headers,
        })
    }

    /// Reconstruct the exact byte string the signer's `b=` covers.
    #[must_use]
    fn signed_data(&self) -> Vec<u8> {
        build_signed_data(
            &self.signature.canonicalization,
            &self.signed_headers,
            &self.signature.raw_header_value,
        )
    }
}

/// Select, for each name in the `h=` list, the matching header from the email,
/// walking bottom-up so repeated names consume from the bottom (RFC 6376 section
/// 5.4.2).
fn select_signed_headers(
    message: &Message,
    signature: &DkimSignatureHeader,
) -> Result<Vec<Header>, DkimError> {
    // Track how many times each (lowercased) name has already been consumed, so
    // a repeated name in h= picks successively higher occurrences.
    let mut consumed: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    let mut selected = Vec::with_capacity(signature.signed_headers.len());

    for name in &signature.signed_headers {
        let lower = name.to_ascii_lowercase();
        let already = consumed.entry(lower.clone()).or_insert(0);

        // Walk the email bottom-up, skipping `already` matches, take the next.
        let found = message
            .headers
            .iter()
            .rev()
            .filter(|h| h.name_eq_ignore_case(name))
            .nth(*already)
            .cloned();

        match found {
            Some(header) => {
                *already += 1;
                selected.push(header);
            }
            // A name in h= with no (remaining) matching header: the standard
            // treats an over-count as signing a null header (non-existence). We
            // refuse rather than silently sign a phantom, because our proof must
            // carry a real captured header value.
            None => return Err(DkimError::MissingSignedHeader(name.clone())),
        }
    }

    Ok(selected)
}

/// Build the RFC 6376 section 3.7 signed byte string from the canonicalization,
/// the ordered signed headers, and the raw `DKIM-Signature` value.
fn build_signed_data(
    canon: &Canonicalization,
    signed_headers: &[Header],
    dkim_signature_raw_value: &str,
) -> Vec<u8> {
    let mut data = Vec::new();
    for header in signed_headers {
        data.extend_from_slice(&canon.canonicalize_header(header));
    }
    data.extend_from_slice(&canon.canonicalize_dkim_signature(dkim_signature_raw_value));
    data
}

/// Verify a raw email's DKIM header signature against the domain public key.
///
/// This is the whole-email entry point: parse, extract the `DKIM-Signature`
/// header, reconstruct the signed string, and verify `b=`. Use it for a raw
/// `.eml` fixture, or to mint a [`SignedEmail`] proof after checking it.
///
/// # Errors
///
/// Returns [`DkimError`] at the first failure: malformed input, an unsupported
/// algorithm, a key/algorithm mismatch, invalid base64, or a signature that does
/// not verify.
#[cfg(feature = "dkim")]
pub fn verify(raw_eml: &[u8], key: &DkimPublicKey) -> Result<DkimSignatureHeader, DkimError> {
    let email = SignedEmail::from_raw_eml(raw_eml)?;
    verify_with_key(&email, key)?;
    Ok(email.signature)
}

/// Verify a captured [`SignedEmail`] proof against the domain public key.
///
/// This is the offline entry point: it needs only the captured proof (no full
/// email, no body) and the resolved domain key.
///
/// # Errors
///
/// Returns [`DkimError`] on a key/algorithm mismatch, invalid base64 in `b=`, or
/// a signature that does not verify.
#[cfg(feature = "dkim")]
pub fn verify_with_key(email: &SignedEmail, key: &DkimPublicKey) -> Result<(), DkimError> {
    let algorithm = email.signature.algorithm;
    if !key.matches(algorithm) {
        return Err(DkimError::KeyAlgorithmMismatch);
    }

    let signed_data = email.signed_data();

    use base64::Engine;
    let signature_bytes = base64::engine::general_purpose::STANDARD
        .decode(email.signature.signature.as_bytes())
        .map_err(|_| DkimError::InvalidBase64("b=".into()))?;

    match algorithm {
        SignatureAlgorithm::RsaSha256 => verify_rsa_sha256(key, &signed_data, &signature_bytes),
        SignatureAlgorithm::Ed25519Sha256 => {
            verify_ed25519_sha256(key, &signed_data, &signature_bytes)
        }
    }
}

/// Verify an `rsa-sha256` DKIM signature: RSA PKCS#1 v1.5 over SHA-256 of the
/// signed byte string, with the SPKI public key from DNS.
#[cfg(feature = "dkim")]
fn verify_rsa_sha256(
    key: &DkimPublicKey,
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), DkimError> {
    use rsa::RsaPublicKey;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePublicKey;
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;

    let DkimPublicKey::Rsa { spki_der } = key else {
        return Err(DkimError::KeyAlgorithmMismatch);
    };

    let public_key = RsaPublicKey::from_public_key_der(spki_der)
        .map_err(|e| DkimError::InvalidPublicKey(e.to_string()))?;
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);
    let signature = Signature::try_from(signature)
        .map_err(|_| DkimError::MalformedSignature("b= is not a valid RSA signature".into()))?;

    // VerifyingKey<Sha256> hashes `signed_data` with SHA-256 internally, exactly
    // as DKIM rsa-sha256 requires.
    verifying_key
        .verify(signed_data, &signature)
        .map_err(|_| DkimError::VerificationFailed)
}

/// Verify an `ed25519-sha256` DKIM signature: Ed25519 over the SHA-256 hash of
/// the signed byte string (RFC 8463).
#[cfg(feature = "dkim")]
fn verify_ed25519_sha256(
    key: &DkimPublicKey,
    signed_data: &[u8],
    signature: &[u8],
) -> Result<(), DkimError> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    use sha2::{Digest, Sha256};

    let DkimPublicKey::Ed25519 { public_key } = key else {
        return Err(DkimError::KeyAlgorithmMismatch);
    };

    let verifying_key = VerifyingKey::from_bytes(public_key)
        .map_err(|e| DkimError::InvalidPublicKey(e.to_string()))?;
    let signature = Signature::from_slice(signature)
        .map_err(|_| DkimError::MalformedSignature("b= is not a valid ed25519 signature".into()))?;

    // RFC 8463: ed25519-sha256 signs the SHA-256 hash of the canonicalized
    // header string (PureEd25519 over that 32-byte digest).
    let digest = Sha256::digest(signed_data);
    verifying_key
        .verify(&digest, &signature)
        .map_err(|_| DkimError::VerificationFailed)
}
