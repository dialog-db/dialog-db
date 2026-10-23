//! Parsing a `did:mailto` identity (the storacha did-mailto spec).
//!
//! The format is `did:mailto:{domain}:{percent-encoded-local}` in **plaintext**
//! (not hashed): the email address is `{local}@{domain}` once the local part is
//! percent-decoded. Percent-encoding is used because an email local part may
//! contain characters (`:`, `%`, and others) that are not safe in a DID's
//! method-specific id.
//!
//! Reference: <https://github.com/storacha/specs/blob/main/did-mailto.md>

use percent_encoding::percent_decode_str;

use crate::error::ResolveError;

/// A parsed `did:mailto` identity: a domain and an email local part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailtoDid {
    /// The email domain (the DKIM `d=` this proof must match).
    pub domain: String,
    /// The email local part, percent-decoded (the text before `@`).
    pub local: String,
}

impl MailtoDid {
    /// Parse a `did:mailto:{domain}:{percent-encoded-local}` string.
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::MalformedDid`] if the string is not a `did:mailto`
    /// with both a domain and a local part, or the local part is not valid
    /// percent-encoded UTF-8.
    pub fn parse(did: &str) -> Result<Self, ResolveError> {
        let rest = did
            .strip_prefix("did:mailto:")
            .ok_or_else(|| ResolveError::MalformedDid(format!("not a did:mailto DID: {did}")))?;

        // Exactly `{domain}:{local}`: the domain is the first segment, the local
        // part is everything after the first colon (a percent-encoded local part
        // never contains a raw colon, so a single split is unambiguous).
        let (domain, local_encoded) = rest.split_once(':').ok_or_else(|| {
            ResolveError::MalformedDid(format!("did:mailto has no local part: {did}"))
        })?;

        if domain.is_empty() {
            return Err(ResolveError::MalformedDid(format!(
                "did:mailto has an empty domain: {did}"
            )));
        }
        if local_encoded.is_empty() {
            return Err(ResolveError::MalformedDid(format!(
                "did:mailto has an empty local part: {did}"
            )));
        }

        let local = percent_decode_str(local_encoded)
            .decode_utf8()
            .map_err(|e| {
                ResolveError::MalformedDid(format!("invalid local-part encoding in {did}: {e}"))
            })?
            .into_owned();

        Ok(Self {
            domain: domain.to_string(),
            local,
        })
    }

    /// The full email address `{local}@{domain}`.
    #[must_use]
    pub fn email(&self) -> String {
        format!("{}@{}", self.local, self.domain)
    }

    /// Whether the given email address (as it appears in a signed `From:`)
    /// matches this identity, case-insensitively on the domain.
    ///
    /// The local part is compared exactly (email local parts are technically
    /// case-sensitive per RFC 5321), the domain case-insensitively (domains are
    /// not).
    #[must_use]
    pub fn matches_email(&self, email: &str) -> bool {
        match email.rsplit_once('@') {
            Some((local, domain)) => {
                local == self.local && domain.eq_ignore_ascii_case(&self.domain)
            }
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn parses_plain_identity() {
        let did = MailtoDid::parse("did:mailto:example.com:alice").unwrap();
        assert_eq!(did.domain, "example.com");
        assert_eq!(did.local, "alice");
        assert_eq!(did.email(), "alice@example.com");
    }

    #[dialog_common::test]
    fn percent_decodes_local_part() {
        // `alice+tag` where `+` is left literal but a percent-encoded colon is
        // decoded.
        let did = MailtoDid::parse("did:mailto:example.com:alice%2Btag").unwrap();
        assert_eq!(did.local, "alice+tag");
        assert_eq!(did.email(), "alice+tag@example.com");
    }

    #[dialog_common::test]
    fn matches_email_case_insensitive_domain() {
        let did = MailtoDid::parse("did:mailto:Example.com:Alice").unwrap();
        assert!(did.matches_email("Alice@EXAMPLE.COM"));
        // Local part is case-sensitive.
        assert!(!did.matches_email("alice@example.com"));
    }

    #[dialog_common::test]
    fn rejects_missing_local_part() {
        assert!(matches!(
            MailtoDid::parse("did:mailto:example.com"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    #[dialog_common::test]
    fn rejects_non_mailto() {
        assert!(matches!(
            MailtoDid::parse("did:web:example.com"),
            Err(ResolveError::MalformedDid(_))
        ));
    }
}
