//! `did:web` to HTTPS URL derivation, per the did:web method spec.
//!
//! - `did:web:example.com` becomes `https://example.com/.well-known/did.json`.
//! - `did:web:example.com:path:to` becomes `https://example.com/path/to/did.json`.
//! - a percent-encoded colon in the host segment (`did:web:host%3A3000`) decodes
//!   to a port (`host:3000`).

use percent_encoding::percent_decode_str;

use crate::error::ResolveError;

/// Derive the `did.json` URL for a `did:web` DID string.
///
/// # Errors
///
/// Returns [`ResolveError::MalformedDid`] if the string is not a `did:web` DID
/// with a non-empty host.
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

    let url = if path_segments.is_empty() {
        format!("https://{host}/.well-known/did.json")
    } else {
        format!("https://{host}/{}/did.json", path_segments.join("/"))
    };

    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_host() {
        assert_eq!(
            did_web_url("did:web:example.com").unwrap(),
            "https://example.com/.well-known/did.json"
        );
    }

    #[test]
    fn host_with_path() {
        assert_eq!(
            did_web_url("did:web:example.com:path:to").unwrap(),
            "https://example.com/path/to/did.json"
        );
    }

    #[test]
    fn host_with_port() {
        assert_eq!(
            did_web_url("did:web:localhost%3A3000").unwrap(),
            "https://localhost:3000/.well-known/did.json"
        );
    }

    #[test]
    fn rejects_non_web() {
        assert!(matches!(
            did_web_url("did:key:z6Mk"),
            Err(ResolveError::MalformedDid(_))
        ));
    }

    #[test]
    fn rejects_empty_host() {
        assert!(matches!(
            did_web_url("did:web:"),
            Err(ResolveError::MalformedDid(_))
        ));
    }
}
