//! DID to HTTPS URL derivation.
//!
//! `did:web`, per the did:web method spec:
//! - `did:web:example.com` becomes `https://example.com/.well-known/did.json`.
//! - `did:web:example.com:path:to` becomes `https://example.com/path/to/did.json`.
//! - a percent-encoded colon in the host segment (`did:web:host%3A3000`) decodes
//!   to a port (`host:3000`).
//!
//! `did:plc`, resolved against the PLC directory:
//! - `did:plc:xxxx` becomes `https://plc.directory/did:plc:xxxx`.
//!
//! # Validating the decoded output
//!
//! Percent-decoding happens before the URL is assembled, so decoding alone
//! would let a crafted DID choose the fetch target: `did:web:good.com%40evil.example`
//! decodes to a `good.com@evil.example` authority, whose userinfo makes the
//! request go to `evil.example` while the DID reads as `good.com`. A decoded
//! `/` or `?` smuggles an arbitrary path and query past the fixed
//! `/.well-known/did.json` suffix, `..` walks to another resource, and control
//! bytes splice into the URL.
//!
//! Because the authorizer resolves the issuer DID of any submitted invocation,
//! that is an unauthenticated server-side request-forgery primitive as well as
//! an identity-substitution vector. So each *decoded* segment is checked against
//! an allowlist: a host may hold only letters, digits, `-`, `.`, and a single
//! `:port` suffix, and a path segment only unreserved characters.
//!
//! `did:plc` needs no decoding at all: the identifier is base32 (`a-z2-7`) of a
//! fixed length, which [`did_plc_url`] checks before it reaches the URL.

use percent_encoding::percent_decode_str;

use crate::error::ResolveError;

/// Is this a character a decoded `did:web` path segment may contain?
///
/// The unreserved set of RFC 3986 plus `~`. Everything else (`/`, `?`, `#`,
/// `@`, `%`, control bytes, non-ASCII) would change the structure of the URL
/// rather than sit inside one segment.
fn is_path_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '.' | '_' | '~')
}

/// Validate a decoded `did:web` host, which may carry a `:port` suffix (the
/// only thing `%3A` legitimately decodes to).
fn validate_host(host: &str, did: &str) -> Result<(), ResolveError> {
    let malformed =
        |reason: &str| ResolveError::MalformedDid(format!("did:web host {reason}: {did}"));

    // At most one colon, and only as a port separator.
    let (name, port) = match host.split_once(':') {
        Some((name, port)) => (name, Some(port)),
        None => (host, None),
    };

    if name.is_empty() {
        return Err(malformed("is empty"));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '.'))
    {
        return Err(malformed(
            "may contain only letters, digits, '-' and '.' after decoding",
        ));
    }
    // A bare `..` or a leading/trailing dot is not a hostname.
    if name.split('.').any(str::is_empty) {
        return Err(malformed("has an empty label"));
    }

    match port {
        None => Ok(()),
        Some(port) if !port.is_empty() && port.chars().all(|c| c.is_ascii_digit()) => Ok(()),
        Some(_) => Err(malformed("has a non-numeric port")),
    }
}

/// Derive the `did.json` URL for a `did:web` DID string.
///
/// # Errors
///
/// Returns [`ResolveError::MalformedDid`] if the string is not a `did:web` DID
/// with a non-empty host, or if any segment decodes to something that would
/// change the shape of the URL rather than fill in a host or path segment.
pub fn did_web_url(did: &str) -> Result<String, ResolveError> {
    let rest = did
        .strip_prefix("did:web:")
        .ok_or_else(|| ResolveError::MalformedDid(format!("not a did:web DID: {did}")))?;

    if rest.is_empty() {
        return Err(ResolveError::MalformedDid(format!(
            "did:web has no host: {did}"
        )));
    }

    let mut segments = rest.split(':');

    let host_segment = segments
        .next()
        .ok_or_else(|| ResolveError::MalformedDid(format!("did:web has no host: {did}")))?;
    let host = percent_decode_str(host_segment)
        .decode_utf8()
        .map_err(|e| ResolveError::MalformedDid(format!("invalid host encoding in {did}: {e}")))?;
    if host.is_empty() {
        return Err(ResolveError::MalformedDid(format!(
            "did:web has an empty host: {did}"
        )));
    }
    validate_host(&host, did)?;

    let path_segments: Vec<String> = segments
        .map(|segment| {
            percent_decode_str(segment)
                .decode_utf8()
                .map(|s| s.into_owned())
                .map_err(|e| {
                    ResolveError::MalformedDid(format!("invalid path encoding in {did}: {e}"))
                })
        })
        .collect::<Result<_, _>>()?;

    if path_segments.iter().any(String::is_empty) {
        return Err(ResolveError::MalformedDid(format!(
            "did:web has an empty path segment: {did}"
        )));
    }

    for segment in &path_segments {
        if !segment.chars().all(is_path_char) {
            return Err(ResolveError::MalformedDid(format!(
                "did:web path segment contains a character that is not allowed \
                 after decoding: {did}"
            )));
        }
        // `.` and `..` are dot-segments: they would walk the fetch elsewhere.
        if segment == "." || segment == ".." {
            return Err(ResolveError::MalformedDid(format!(
                "did:web path segment is a dot-segment: {did}"
            )));
        }
    }

    let url = if path_segments.is_empty() {
        format!("https://{host}/.well-known/did.json")
    } else {
        format!("https://{host}/{}/did.json", path_segments.join("/"))
    };

    Ok(url)
}

/// The `plc.directory` origin every `did:plc` resolves against.
const PLC_DIRECTORY: &str = "https://plc.directory";

/// The fixed length of a `did:plc` identifier: 24 base32 characters.
const PLC_IDENTIFIER_LEN: usize = 24;

/// Derive the resolution URL for a `did:plc` DID string.
///
/// The whole DID (`did:plc:<identifier>`) is the last path segment:
/// `did:plc:xxxx` becomes `https://plc.directory/did:plc:xxxx`. A `did:plc`
/// identifier is 24 characters of base32 (`a-z2-7`) and contains no `:`, so the
/// DID has exactly three colon-separated segments and needs no percent-encoding
/// (a literal `:` is valid in a URL path segment).
///
/// Both the charset and the length are checked before the identifier reaches
/// the URL. The charset is what keeps a crafted DID from escaping the
/// `plc.directory` path (no `/`, `?`, `#`, `.` or `%` can appear), and the
/// length is what keeps a well-formed-looking but bogus identifier from
/// becoming a directory request at all.
///
/// # Errors
///
/// Returns [`ResolveError::MalformedDid`] if the string is not a `did:plc` DID
/// whose identifier is 24 base32 characters.
pub fn did_plc_url(did: &str) -> Result<String, ResolveError> {
    let identifier = did
        .strip_prefix("did:plc:")
        .ok_or_else(|| ResolveError::MalformedDid(format!("not a did:plc DID: {did}")))?;

    if identifier.is_empty() {
        return Err(ResolveError::MalformedDid(format!(
            "did:plc has no identifier: {did}"
        )));
    }

    // A did:plc identifier is base32 [a-z2-7]. Anything else (an embedded ':',
    // a '/', uppercase, a fragment) is not a plc identifier and must not be
    // spliced into the URL path.
    if !identifier
        .bytes()
        .all(|b| b.is_ascii_lowercase() || (b'2'..=b'7').contains(&b))
    {
        return Err(ResolveError::MalformedDid(format!(
            "did:plc identifier is not base32 [a-z2-7]: {did}"
        )));
    }

    if identifier.len() != PLC_IDENTIFIER_LEN {
        return Err(ResolveError::MalformedDid(format!(
            "did:plc identifier must be {PLC_IDENTIFIER_LEN} characters, got {}: {did}",
            identifier.len()
        )));
    }

    Ok(format!("{PLC_DIRECTORY}/did:plc:{identifier}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn plain_host() {
        assert_eq!(
            did_web_url("did:web:example.com").unwrap(),
            "https://example.com/.well-known/did.json"
        );
    }

    #[dialog_common::test]
    fn host_with_path() {
        assert_eq!(
            did_web_url("did:web:example.com:path:to").unwrap(),
            "https://example.com/path/to/did.json"
        );
    }

    /// The host allowlist must not reject the shapes did:web legitimately
    /// produces, so pin the accepted forms alongside the refused ones.
    #[dialog_common::test]
    fn accepts_ordinary_hosts_and_paths() {
        for did in [
            "did:web:example.com",
            "did:web:sub.domain.example.com",
            "did:web:my-host.example",
            "did:web:example.com:users:alice",
            "did:web:example.com:a-b_c~d.e",
            "did:web:localhost%3A3000:path",
        ] {
            assert!(did_web_url(did).is_ok(), "{did} should derive a URL");
        }
    }

    /// A second colon in the decoded host is not a second port.
    #[dialog_common::test]
    fn rejects_multiple_colons_in_host() {
        assert!(matches!(
            did_web_url("did:web:host.example%3A80%3A90"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    /// A port must be numeric: `host:evil` is not a host with a port.
    #[dialog_common::test]
    fn rejects_non_numeric_port() {
        assert!(matches!(
            did_web_url("did:web:host.example%3Anotaport"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    /// An empty label (`host..example`, a leading or trailing dot) is not a
    /// hostname, and `..` in the host position is a traversal attempt.
    #[dialog_common::test]
    fn rejects_empty_host_label() {
        for did in [
            "did:web:host..example",
            "did:web:.example.com",
            "did:web:example.com.",
            "did:web:..",
        ] {
            assert!(
                matches!(did_web_url(did), Err(ResolveError::MalformedDid(_))),
                "{did} should be refused"
            );
        }
    }

    /// A single-dot segment is a dot-segment too, and normalizes away.
    #[dialog_common::test]
    fn rejects_single_dot_path_segment() {
        assert!(matches!(
            did_web_url("did:web:host.example:."),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    /// A decoded `#` would make the rest of the derived URL a fragment,
    /// truncating the `/did.json` suffix.
    #[dialog_common::test]
    fn rejects_fragment_in_path() {
        assert!(matches!(
            did_web_url("did:web:host.example:a%23b"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    /// A decoded `%` could start a fresh escape once re-parsed by the client.
    #[dialog_common::test]
    fn rejects_percent_in_path() {
        assert!(matches!(
            did_web_url("did:web:host.example:a%25b"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    #[dialog_common::test]
    fn host_with_port() {
        assert_eq!(
            did_web_url("did:web:localhost%3A3000").unwrap(),
            "https://localhost:3000/.well-known/did.json"
        );
    }

    #[dialog_common::test]
    fn rejects_non_web() {
        assert!(matches!(
            did_web_url("did:key:z6Mk"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    #[dialog_common::test]
    fn rejects_empty_host() {
        assert!(matches!(
            did_web_url("did:web:"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    /// A percent-encoded `/` and `?` in the host segment must not smuggle an
    /// arbitrary path and query into the fetch URL. `did:web` permits decoding
    /// only `%3A` (a port colon) in the host; a decoded `/` or `?` here turns
    /// the fixed `/.well-known/did.json` suffix into a query-string tail,
    /// letting a crafted DID choose any path on the host. Because the S3
    /// authorizer resolves attacker-supplied issuer DIDs over the network, this
    /// is a server-side request-forgery primitive.
    #[dialog_common::test]
    fn rejects_path_and_query_smuggled_into_host() {
        let url = did_web_url("did:web:victim.example%2Fadmin%3Fx%3D");
        assert!(
            matches!(url, Err(ResolveError::MalformedDid(_))),
            "a host that decodes to contain '/' or '?' must be refused, got {url:?}"
        );
    }

    /// A percent-encoded `@` in the host segment must not introduce a userinfo
    /// component, which would make the fetch target a different host than the
    /// one the DID string appears to name (`good.com@evil.com` fetches
    /// `evil.com`).
    #[dialog_common::test]
    fn rejects_userinfo_in_host() {
        let url = did_web_url("did:web:good.com%40evil.example");
        assert!(
            matches!(url, Err(ResolveError::MalformedDid(_))),
            "a host that decodes to contain '@' (userinfo) must be refused, got {url:?}"
        );
    }

    /// Dot-segments in the path must not walk the fetch to a different resource
    /// on the host (`did:web:host:..:..` -> `https://host/../../did.json`),
    /// which combined with a missing document-`id` check lets any
    /// DID-document-shaped JSON on the host masquerade as this identity.
    #[dialog_common::test]
    fn rejects_dot_segment_path_traversal() {
        let url = did_web_url("did:web:host.example:..:..");
        assert!(
            matches!(url, Err(ResolveError::MalformedDid(_))),
            "path segments of '..' must be refused, got {url:?}"
        );
    }

    /// A raw control or delimiter byte introduced by percent-decoding a path
    /// segment (here a CR/LF) must be refused rather than spliced into the URL.
    #[dialog_common::test]
    fn rejects_control_bytes_in_path() {
        let url = did_web_url("did:web:host.example:a%0d%0ab");
        assert!(
            matches!(url, Err(ResolveError::MalformedDid(_))),
            "a path segment decoding to control bytes must be refused, got {url:?}"
        );
    }

    #[dialog_common::test]
    fn plc_identifier_in_path() {
        assert_eq!(
            did_plc_url("did:plc:ewvi7nxzyoun6zhxrhs64oiz").unwrap(),
            "https://plc.directory/did:plc:ewvi7nxzyoun6zhxrhs64oiz"
        );
    }

    #[dialog_common::test]
    fn plc_rejects_non_plc() {
        assert!(matches!(
            did_plc_url("did:web:example.com"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    #[dialog_common::test]
    fn plc_rejects_empty_identifier() {
        assert!(matches!(
            did_plc_url("did:plc:"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    #[dialog_common::test]
    fn plc_rejects_non_base32_identifier() {
        assert!(matches!(
            did_plc_url("did:plc:has/slash"),
            Err(ResolveError::MalformedDid(_))
        ));
        assert!(matches!(
            did_plc_url("did:plc:UPPERCASE"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    /// A did:plc identifier is exactly 24 base32 characters (per the PLC spec,
    /// a truncated 32-byte SHA-256 in base32). The doc comment already asserts
    /// this length; the code must enforce it, not just the charset, so a
    /// malformed short or long identifier is refused before it becomes a
    /// directory request URL.
    #[dialog_common::test]
    fn plc_rejects_wrong_length_identifier() {
        // One char: passes the charset test but is not a plc identifier.
        assert!(
            matches!(did_plc_url("did:plc:a"), Err(ResolveError::MalformedDid(_))),
            "a 1-char identifier must be refused"
        );
        // 25 base32 chars: one over the fixed length.
        assert!(
            matches!(
                did_plc_url("did:plc:aaaaaaaaaaaaaaaaaaaaaaaaa"),
                Err(ResolveError::MalformedDid(_))
            ),
            "an over-length identifier must be refused"
        );
    }
}
