//! Parsing the `DKIM-Signature` header into its tags (RFC 6376 section 3.5).
//!
//! The header value is a `;`-separated list of `tag=value` pairs. This parses
//! the ones verification needs: `v` (version), `a` (algorithm), `c`
//! (canonicalization), `d` (domain), `s` (selector), `h` (signed header list),
//! `bh` (body hash), `b` (the signature), and optionally `i` (agent/identity),
//! `t`, `l`, `q`. Only `v a d s h bh b` are required; the rest are captured when
//! present and otherwise defaulted per the RFC.

use super::canonicalize::Canonicalization;
use super::error::DkimError;

/// The inner signing algorithm named by the `a=` tag.
///
/// DKIM couples a public-key algorithm with a hash. The two registered pairs are
/// `rsa-sha256` (overwhelmingly the common case, what Gmail/Google use) and
/// `ed25519-sha256`. The legacy `rsa-sha1` is deliberately unsupported: SHA-1 is
/// broken and no verifier should accept it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureAlgorithm {
    /// `rsa-sha256`: RSA PKCS#1 v1.5 signature over a SHA-256 hash.
    RsaSha256,
    /// `ed25519-sha256`: Ed25519 signature over the SHA-256-hashed header input.
    Ed25519Sha256,
}

impl SignatureAlgorithm {
    fn parse(value: &str) -> Result<Self, DkimError> {
        match value {
            "rsa-sha256" => Ok(Self::RsaSha256),
            "ed25519-sha256" => Ok(Self::Ed25519Sha256),
            other => Err(DkimError::UnsupportedAlgorithm(other.to_string())),
        }
    }
}

/// A parsed `DKIM-Signature` header.
///
/// Field names mirror the RFC tag letters. The `raw_value` of the whole header
/// is kept so the signature reconstruction can rebuild the `DKIM-Signature`
/// header with the `b=` value emptied (RFC 6376 section 3.7 step 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DkimSignatureHeader {
    /// `a=`: the inner algorithm.
    pub algorithm: SignatureAlgorithm,
    /// `c=`: header and body canonicalization.
    pub canonicalization: Canonicalization,
    /// `d=`: the signing domain.
    pub domain: String,
    /// `s=`: the selector (names the DNS record under `_domainkey`).
    pub selector: String,
    /// `h=`: the ordered list of signed header field names.
    pub signed_headers: Vec<String>,
    /// `bh=`: the base64 body hash, kept verbatim (its bytes are trusted, not
    /// recomputed, because the body is never carried).
    pub body_hash: String,
    /// `b=`: the base64 signature over the reconstructed signed string.
    pub signature: String,
    /// `i=`: the signing identity / AUID, if present (e.g. `@example.com`).
    pub identity: Option<String>,
    /// The raw field value of the whole `DKIM-Signature` header (no name, no
    /// trailing CRLF), verbatim including folding, as needed to reconstruct the
    /// header with `b=` emptied.
    pub raw_header_value: String,
}

impl DkimSignatureHeader {
    /// Parse a `DKIM-Signature` header's raw field value into tags.
    ///
    /// `raw_value` is the text after `DKIM-Signature:` (leading space and any
    /// folding included), exactly as [`Header::raw_value`](super::Header).
    ///
    /// # Errors
    ///
    /// Returns [`DkimError::MalformedSignature`] if a required tag is absent, or
    /// a more specific error for an unsupported algorithm/canonicalization.
    pub fn parse(raw_value: &str) -> Result<Self, DkimError> {
        let tags = parse_tags(raw_value);

        let get = |k: &str| {
            tags.iter()
                .find(|(tag, _)| tag == k)
                .map(|(_, v)| v.clone())
        };

        let algorithm = SignatureAlgorithm::parse(
            &get("a").ok_or_else(|| DkimError::MalformedSignature("missing a= tag".into()))?,
        )?;

        // c= defaults to `simple/simple` when absent (RFC 6376 section 3.5).
        let canonicalization = match get("c") {
            Some(c) => Canonicalization::parse(&c)?,
            None => Canonicalization::default(),
        };

        let domain =
            get("d").ok_or_else(|| DkimError::MalformedSignature("missing d= tag".into()))?;
        let selector =
            get("s").ok_or_else(|| DkimError::MalformedSignature("missing s= tag".into()))?;

        let signed_headers_raw =
            get("h").ok_or_else(|| DkimError::MalformedSignature("missing h= tag".into()))?;
        // h= is a colon-separated list; whitespace around names is insignificant.
        let signed_headers: Vec<String> = signed_headers_raw
            .split(':')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if signed_headers.is_empty() {
            return Err(DkimError::MalformedSignature("empty h= tag".into()));
        }

        let body_hash = strip_ws(
            &get("bh").ok_or_else(|| DkimError::MalformedSignature("missing bh= tag".into()))?,
        );
        let signature = strip_ws(
            &get("b").ok_or_else(|| DkimError::MalformedSignature("missing b= tag".into()))?,
        );

        Ok(Self {
            algorithm,
            canonicalization,
            domain,
            selector,
            signed_headers,
            body_hash,
            signature,
            identity: get("i"),
            raw_header_value: raw_value.to_string(),
        })
    }
}

/// Split a `DKIM-Signature` value into `(tag, value)` pairs.
///
/// Tags are separated by `;`. Within a tag, everything up to the first `=` is
/// the tag name (trimmed); the rest is the value. Folding whitespace inside a
/// value is left in place here; callers strip it where it matters (`b`, `bh`).
fn parse_tags(raw_value: &str) -> Vec<(String, String)> {
    raw_value
        .split(';')
        .filter_map(|chunk| {
            let chunk = chunk.trim();
            if chunk.is_empty() {
                return None;
            }
            let (name, value) = chunk.split_once('=')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

/// Remove all whitespace (including CRLF folding) from a base64 tag value.
///
/// RFC 6376 permits folding whitespace inside the `b=` and `bh=` values; it is
/// not part of the base64 and must be removed before decoding.
fn strip_ws(value: &str) -> String {
    value.chars().filter(|c| !c.is_whitespace()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_all_required_tags() {
        let raw = " v=1; a=rsa-sha256; c=relaxed/relaxed; d=example.com; s=sel;\r\n \
                   h=from:subject; bh=AAAA; b=BBBB==";
        let sig = DkimSignatureHeader::parse(raw).unwrap();
        assert_eq!(sig.algorithm, SignatureAlgorithm::RsaSha256);
        assert_eq!(sig.domain, "example.com");
        assert_eq!(sig.selector, "sel");
        assert_eq!(sig.signed_headers, vec!["from", "subject"]);
        assert_eq!(sig.body_hash, "AAAA");
        assert_eq!(sig.signature, "BBBB==");
    }

    #[test]
    fn strips_folding_from_signature_value() {
        let raw = " v=1; a=ed25519-sha256; d=d.com; s=s; h=from; bh=x;\r\n \
                   b=abc\r\n def";
        let sig = DkimSignatureHeader::parse(raw).unwrap();
        assert_eq!(sig.algorithm, SignatureAlgorithm::Ed25519Sha256);
        // The CRLF and continuation whitespace inside b= are removed.
        assert_eq!(sig.signature, "abcdef");
    }

    #[test]
    fn canonicalization_defaults_to_simple_simple() {
        let raw = " v=1; a=rsa-sha256; d=d.com; s=s; h=from; bh=x; b=y";
        let sig = DkimSignatureHeader::parse(raw).unwrap();
        assert_eq!(sig.canonicalization, Canonicalization::default());
    }

    #[test]
    fn missing_required_tag_is_rejected() {
        // No b= tag.
        let raw = " v=1; a=rsa-sha256; d=d.com; s=s; h=from; bh=x";
        assert!(matches!(
            DkimSignatureHeader::parse(raw),
            Err(DkimError::MalformedSignature(_))
        ));
    }

    #[test]
    fn rejects_unsupported_algorithm() {
        let raw = " v=1; a=rsa-sha1; d=d.com; s=s; h=from; bh=x; b=y";
        assert!(matches!(
            DkimSignatureHeader::parse(raw),
            Err(DkimError::UnsupportedAlgorithm(_))
        ));
    }
}
