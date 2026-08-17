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
}
