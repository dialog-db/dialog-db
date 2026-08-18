//! Verifying a `did:mailto` proof and yielding the authorized `did:key`.
//!
//! A `did:mailto` identity is bound to a `did:key` by a DKIM-signed "I am also
//! known as {did:key}" email (a **powerline** delegation: the `did:key` may
//! then sign anything on the email's behalf). This module runs the full check:
//!
//! 1. Resolve the domain's DKIM key from the proof's `s=`/`d=` tags (DoH TXT).
//! 2. Verify the DKIM header signature `b=` over the captured signed headers.
//! 3. Extract the `did:key` from the signed `Subject:` template.
//! 4. Bind the proof to the identity: the signed `From:` address must match the
//!    `did:mailto`, and the DKIM `d=` domain must be the identity's domain.
//!
//! The result is a [`dialog_credentials::Verifier`] for the authorized
//! `did:key`. That verifier is what checks the actual per-UCAN signatures; the
//! `did:mailto` never signs a payload itself. Multiple proofs (multiple "I am
//! also known as" emails) yield multiple authorized keys, which compose into a
//! [`MultiVerifier`](crate::MultiVerifier) over the same `did:mailto`.

use dialog_credentials::Verifier;
use dialog_dkim::{DkimPublicKey, SignedEmail, verify_with_key};
use dialog_varsig::Did;

use super::did::MailtoDid;
use super::key_provider::DkimKeyProvider;
use super::subject::{extract_did_key, extract_from_address};
use crate::error::ResolveError;
use crate::fetch::Fetch;
use crate::verifier::MultiVerifier;

/// A verified binding from a `did:mailto` identity to an authorized `did:key`.
///
/// Produced by [`verify_mailto_proof`]. It carries the identity (for reporting)
/// and the resolved `did:key` verifier that signs on the identity's behalf.
#[derive(Debug, Clone)]
pub struct DidMailtoBinding {
    /// The `did:mailto` identity this proof authorizes for.
    pub identity: Did,
    /// The authorized `did:key` verifier extracted from the proof's subject.
    pub authorized_key: Verifier,
}

/// Verify a captured DKIM proof binds a `did:key` to the given `did:mailto`.
///
/// `identity` is the `did:mailto` DID string; `proof` is the captured DKIM
/// material; `key_provider` resolves the domain's DKIM key over DNS-over-HTTPS.
///
/// # Errors
///
/// Returns [`ResolveError`] if the DID is malformed, the DKIM key cannot be
/// resolved, the signature does not verify, the subject is not an "I am also
/// known as" binding, the embedded key is not a supported `did:key`, or the
/// signed `From:`/`d=` do not bind to the identity.
pub async fn verify_mailto_proof<F: Fetch>(
    identity: &Did,
    proof: &SignedEmail,
    key_provider: &DkimKeyProvider<F>,
) -> Result<DidMailtoBinding, ResolveError> {
    let mailto = MailtoDid::parse(identity.as_str())?;

    // 1. Resolve the domain DKIM key named by the proof's selector and domain.
    let selector = &proof.signature.selector;
    let domain = &proof.signature.domain;
    let key: DkimPublicKey = key_provider.resolve_key(selector, domain).await?;

    // 2. Verify the DKIM header signature over the captured signed headers.
    verify_with_key(proof, &key)
        .map_err(|e| ResolveError::UnsupportedKey(format!("DKIM verification failed: {e}")))?;

    // 3/4. Bind the (now-verified) signed headers to the identity, then extract
    // the authorized did:key from the signed subject.
    bind_and_extract(&mailto, proof)
}

/// With the DKIM signature already verified, check that the signed `From:` and
/// the DKIM `d=` bind to `mailto`, and extract the authorized `did:key`.
fn bind_and_extract(
    mailto: &MailtoDid,
    proof: &SignedEmail,
) -> Result<DidMailtoBinding, ResolveError> {
    // The DKIM d= domain must be the identity's domain (case-insensitively).
    if !proof.signature.domain.eq_ignore_ascii_case(&mailto.domain) {
        return Err(ResolveError::UnsupportedKey(format!(
            "DKIM d={} does not match did:mailto domain {}",
            proof.signature.domain, mailto.domain
        )));
    }

    // The signed From: address must be the identity's email. From: MUST be in
    // the signed set (RFC 6376 requires it), so a missing signed From: is a
    // malformed proof, and a doubled one is refused (see `signed_header`).
    let from = signed_header(proof, "from")?;
    let from_address = extract_from_address(&from.raw_value)?;
    if !mailto.matches_email(&from_address) {
        return Err(ResolveError::UnsupportedKey(format!(
            "signed From: {from_address} does not match did:mailto {}",
            mailto.email()
        )));
    }

    // Extract the authorized did:key from the signed Subject template.
    let subject = signed_header(proof, "subject")?;
    let did_key = extract_did_key(&subject.raw_value)?;
    let authorized_key = Verifier::from_did_key(&did_key)
        .map_err(|_| ResolveError::UnsupportedKey(format!("unsupported did:key: {did_key}")))?;

    let identity: Did = format!("did:mailto:{}:{}", mailto.domain, mailto.local)
        .parse()
        .map_err(|_| ResolveError::MalformedDid(mailto.email()))?;

    Ok(DidMailtoBinding {
        identity,
        authorized_key,
    })
}

/// The *only* signed header with the given (lowercased) name.
///
/// A binding proof must sign exactly one `From:` and exactly one `Subject:`,
/// and a duplicate is refused rather than resolved by picking one.
///
/// Picking one would be a divergence between what the verifier reads and what
/// the sender saw. RFC 6376 section 5.4.2 consumes repeated `h=` names
/// bottom-up, so the first entry for a doubled name is the *bottom-most*
/// header, while every mail client displays the *top-most*. A message signed
/// with `h=from:subject:subject` would therefore show the sender an innocuous
/// subject while authorizing a key from a different one. Oversigning like that
/// is standard anti-replay practice (RFC 6376 section 8.15), so it is a normal
/// configuration rather than an exotic one, and a binding proof has no
/// legitimate reason to sign two subjects.
fn signed_header<'a>(
    proof: &'a SignedEmail,
    name: &str,
) -> Result<&'a dialog_dkim::Header, ResolveError> {
    let mut matches = proof
        .signed_headers
        .iter()
        .filter(|h| h.name_eq_ignore_case(name));

    let first = matches.next().ok_or_else(|| {
        ResolveError::MalformedDocument(format!("proof does not sign a {name}: header"))
    })?;

    if matches.next().is_some() {
        return Err(ResolveError::MalformedDocument(format!(
            "proof signs more than one {name}: header; the header the verifier \
             reads would not be the one the sender saw"
        )));
    }

    Ok(first)
}

/// Build a [`MultiVerifier`] over a `did:mailto` from one or more verified
/// bindings, so multiple "I am also known as" proofs authorize a key *set*.
///
/// Every binding must be for the same `did:mailto` identity (the caller has
/// verified each proof against it). The resulting verifier accepts a UCAN
/// signature that any of the authorized keys produced, reusing the existing
/// multi-key verification path.
///
/// # Errors
///
/// Returns [`ResolveError::UnsupportedKey`] if `bindings` is empty (a
/// `did:mailto` with no verified key authorizes nothing).
pub fn multi_verifier_from_bindings(
    identity: &Did,
    bindings: Vec<DidMailtoBinding>,
) -> Result<MultiVerifier, ResolveError> {
    if bindings.is_empty() {
        return Err(ResolveError::UnsupportedKey(
            "did:mailto has no verified authorized keys".into(),
        ));
    }
    let keys = bindings.into_iter().map(|b| b.authorized_key).collect();
    Ok(MultiVerifier::new(identity.clone(), keys))
}
