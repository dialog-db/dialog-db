//! Parsing the human-readable subject and From header a did:mailto proof binds.
//!
//! The proof email's subject is literally `I am also known as {did:key}` (the
//! storacha model): human-readable so it does not look like spam, with the
//! authorized `did:key` embedded. Verification extracts the `did:key` from
//! inside that template rather than treating the whole subject as opaque.
//!
//! The `From:` header carries the sender's email, possibly as
//! `Display Name <local@domain>`; the address inside the angle brackets (or the
//! bare address) is what must match the `did:mailto` identity.

use crate::error::ResolveError;

/// The fixed prefix of the binding subject. The `did:key` follows it verbatim.
const SUBJECT_PREFIX: &str = "I am also known as ";

/// Extract the `did:key` from a proof subject of the form
/// `I am also known as {did:key}`.
///
/// Leading/trailing whitespace on the subject value is ignored (a signed header
/// value often begins with the space after the colon). The returned string is
/// the `did:key` substring, trimmed.
///
/// # Errors
///
/// Returns [`ResolveError::UnsupportedKey`] if the subject does not match the
/// template or the embedded value is not a `did:key`.
pub fn extract_did_key(subject_value: &str) -> Result<String, ResolveError> {
    let trimmed = subject_value.trim();
    let key = trimmed.strip_prefix(SUBJECT_PREFIX).ok_or_else(|| {
        ResolveError::UnsupportedKey(format!(
            "subject is not an 'I am also known as' binding: {subject_value:?}"
        ))
    })?;
    let key = key.trim();
    if !key.starts_with("did:key:") {
        return Err(ResolveError::UnsupportedKey(format!(
            "binding subject does not embed a did:key: {key:?}"
        )));
    }
    Ok(key.to_string())
}

/// Extract the bare email address from a `From:` header value.
///
/// Handles both `local@domain` and `Display Name <local@domain>`. The address
/// is returned without surrounding whitespace or angle brackets.
///
/// # Errors
///
/// Returns [`ResolveError::MalformedDocument`] if no address can be found.
pub fn extract_from_address(from_value: &str) -> Result<String, ResolveError> {
    let trimmed = from_value.trim();
    // Prefer the angle-bracketed address if present.
    if let Some(start) = trimmed.find('<')
        && let Some(end) = trimmed[start + 1..].find('>')
    {
        let addr = trimmed[start + 1..start + 1 + end].trim();
        if addr.contains('@') {
            return Ok(addr.to_string());
        }
    }
    // Otherwise the whole value should be a bare address.
    if trimmed.contains('@') && !trimmed.contains(' ') {
        return Ok(trimmed.to_string());
    }
    Err(ResolveError::MalformedDocument(format!(
        "From header has no parseable email address: {from_value:?}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_did_key_from_binding_subject() {
        let key = extract_did_key(" I am also known as did:key:z6Mkabc").unwrap();
        assert_eq!(key, "did:key:z6Mkabc");
    }

    #[test]
    fn rejects_non_binding_subject() {
        assert!(matches!(
            extract_did_key("Re: your account"),
            Err(ResolveError::UnsupportedKey(_))
        ));
    }

    #[test]
    fn rejects_binding_without_did_key() {
        assert!(matches!(
            extract_did_key("I am also known as bob@example.com"),
            Err(ResolveError::UnsupportedKey(_))
        ));
    }

    #[test]
    fn extracts_bracketed_from_address() {
        let addr = extract_from_address(" Alice <alice@example.com>").unwrap();
        assert_eq!(addr, "alice@example.com");
    }

    #[test]
    fn extracts_bare_from_address() {
        let addr = extract_from_address(" alice@example.com").unwrap();
        assert_eq!(addr, "alice@example.com");
    }

    #[test]
    fn rejects_from_without_address() {
        assert!(matches!(
            extract_from_address("Alice"),
            Err(ResolveError::MalformedDocument(_))
        ));
    }

    /// A display name that itself looks like an email must not be mistaken for
    /// the address: the angle-bracketed address is the real sender. Here the
    /// display name is `attacker@evil.example` but the real address is
    /// `victim@good.example`; the bracketed one must win, or an attacker could
    /// spoof any identity by putting it in the display name.
    #[test]
    fn display_name_email_does_not_spoof_bracketed_address() {
        let addr =
            extract_from_address(r#" "attacker@evil.example" <victim@good.example>"#).unwrap();
        assert_eq!(addr, "victim@good.example");
    }

    /// The reverse arrangement: a benign display name with the real (attacker)
    /// address in brackets extracts that bracketed address, so binding then
    /// compares against the true sender.
    #[test]
    fn bracketed_address_is_the_sender_not_the_display_name() {
        let addr = extract_from_address(" Alice Smith <alice@example.com>").unwrap();
        assert_eq!(addr, "alice@example.com");
    }

    /// A subject that embeds the binding did:key alongside extra tokens must not
    /// yield a multi-token string that some later relaxation could mis-split.
    /// The extracted value is fed to `Verifier::from_did_key`, which rejects the
    /// trailing tokens today, so the whole proof fails closed; this pins that a
    /// second did:key cannot ride along in the subject.
    #[test]
    fn subject_with_trailing_tokens_does_not_smuggle_a_second_key() {
        let extracted =
            extract_did_key("I am also known as did:key:zLEGIT and also did:key:zEVIL").unwrap();
        // The extractor returns the tail verbatim today; the guarantee we pin is
        // that it is NOT a bare, single, usable did:key token.
        assert!(
            extracted.contains(' ') || !extracted.starts_with("did:key:zEVIL"),
            "extraction must not yield a clean second key: {extracted:?}"
        );
        assert!(
            dialog_credentials::Verifier::from_did_key(&extracted).is_err(),
            "a multi-token subject must not parse to a usable key: {extracted:?}"
        );
    }
}
