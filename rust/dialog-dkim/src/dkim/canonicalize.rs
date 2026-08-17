//! DKIM header canonicalization (RFC 6376 section 3.4).
//!
//! A DKIM signer canonicalizes the signed headers before hashing, in one of two
//! ways, named by the `c=` tag as `header/body`. This crate only canonicalizes
//! headers (the body is never carried, so body canonicalization is out of
//! scope), but it must implement **both** header algorithms: a verifier does not
//! choose `c=`, the signer does, so we must reproduce whichever the signer used.
//!
//! - `simple` (section 3.4.1): the header is used verbatim, exactly as it
//!   appeared, with its original folding, terminated by a single CRLF.
//! - `relaxed` (section 3.4.2): the field name is lowercased; the colon has no
//!   surrounding space; internal runs of whitespace (including folding CRLFs)
//!   collapse to a single space; leading/trailing whitespace of the value is
//!   removed; the result is terminated by a single CRLF.

use super::error::DkimError;
use super::message::Header;

/// The `c=` canonicalization pair. Only the header half is used here; the body
/// half is parsed and retained for completeness but never applied (no body).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Canonicalization {
    /// The header canonicalization algorithm.
    pub header: HeaderCanon,
    /// The body canonicalization algorithm, parsed but unused (body not carried).
    pub body: BodyCanon,
}

/// The header canonicalization algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderCanon {
    /// `simple`: verbatim header, original folding preserved.
    Simple,
    /// `relaxed`: lowercased name, collapsed whitespace, trimmed value.
    Relaxed,
}

/// The body canonicalization algorithm. Retained from `c=` but not applied,
/// because the proof never carries the body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyCanon {
    /// `simple` body canonicalization.
    Simple,
    /// `relaxed` body canonicalization.
    Relaxed,
}

impl Default for Canonicalization {
    /// RFC 6376 section 3.5: when `c=` is absent the default is `simple/simple`.
    fn default() -> Self {
        Self {
            header: HeaderCanon::Simple,
            body: BodyCanon::Simple,
        }
    }
}

impl Canonicalization {
    /// Parse a `c=` value like `relaxed/relaxed`, `simple`, or `relaxed/simple`.
    ///
    /// A missing body half defaults to `simple` (RFC 6376 section 3.5).
    ///
    /// # Errors
    ///
    /// Returns [`DkimError::UnsupportedCanonicalization`] for any name other than
    /// `simple` or `relaxed`.
    pub fn parse(value: &str) -> Result<Self, DkimError> {
        let mut parts = value.split('/');
        let header_str = parts.next().unwrap_or("simple").trim();
        let body_str = parts.next().unwrap_or("simple").trim();

        let header = match header_str {
            "simple" => HeaderCanon::Simple,
            "relaxed" => HeaderCanon::Relaxed,
            other => return Err(DkimError::UnsupportedCanonicalization(other.to_string())),
        };
        let body = match body_str {
            "simple" => BodyCanon::Simple,
            "relaxed" => BodyCanon::Relaxed,
            other => return Err(DkimError::UnsupportedCanonicalization(other.to_string())),
        };

        Ok(Self { header, body })
    }

    /// Canonicalize a signed header into the bytes DKIM hashes for it.
    ///
    /// The result is terminated by a single CRLF, per RFC 6376: each signed
    /// header contributes `canonical(header) CRLF` to the hash input.
    #[must_use]
    pub fn canonicalize_header(&self, header: &Header) -> Vec<u8> {
        match self.header {
            HeaderCanon::Simple => canonicalize_header_simple(header),
            HeaderCanon::Relaxed => canonicalize_header_relaxed(header),
        }
    }

    /// Canonicalize the `DKIM-Signature` header itself for the final hash step,
    /// with its `b=` value emptied and **no** trailing CRLF (RFC 6376 section
    /// 3.7).
    ///
    /// `raw_header_value` is the raw field value of the `DKIM-Signature` header
    /// (as [`DkimSignatureHeader::raw_header_value`](super::DkimSignatureHeader)).
    #[must_use]
    pub fn canonicalize_dkim_signature(&self, raw_header_value: &str) -> Vec<u8> {
        let emptied = empty_b_tag(raw_header_value);
        let header = Header {
            name: "DKIM-Signature".to_string(),
            raw_value: emptied,
        };
        let mut bytes = match self.header {
            HeaderCanon::Simple => canonicalize_header_simple(&header),
            HeaderCanon::Relaxed => canonicalize_header_relaxed(&header),
        };
        // The DKIM-Signature header is NOT followed by a CRLF in the hash input.
        if bytes.ends_with(b"\r\n") {
            bytes.truncate(bytes.len() - 2);
        }
        bytes
    }
}

/// `simple` header canonicalization (RFC 6376 section 3.4.1): the header,
/// verbatim, as `Name:raw_value` terminated by CRLF.
fn canonicalize_header_simple(header: &Header) -> Vec<u8> {
    let mut out = String::with_capacity(header.name.len() + header.raw_value.len() + 3);
    out.push_str(&header.name);
    out.push(':');
    out.push_str(&header.raw_value);
    out.push_str("\r\n");
    out.into_bytes()
}

/// `relaxed` header canonicalization (RFC 6376 section 3.4.2).
fn canonicalize_header_relaxed(header: &Header) -> Vec<u8> {
    // 1. Lowercase the field name.
    let name = header.name.to_ascii_lowercase();
    // 2/3/4. Unfold, collapse internal whitespace runs to one SP, and trim the
    // value's leading and trailing whitespace.
    let value = relax_value(&header.raw_value);

    let mut out = String::with_capacity(name.len() + value.len() + 3);
    out.push_str(&name);
    out.push(':');
    out.push_str(&value);
    out.push_str("\r\n");
    out.into_bytes()
}

/// Apply the value-normalization half of relaxed canonicalization: unfold
/// (remove CRLFs), collapse every run of WSP (SP/HTAB) into a single SP, and
/// trim leading/trailing whitespace.
fn relax_value(raw_value: &str) -> String {
    // Remove CRLF folding first (a CRLF in a header value is always folding).
    let unfolded = raw_value.replace("\r\n", "");
    // Collapse runs of SP/HTAB into a single SP.
    let mut collapsed = String::with_capacity(unfolded.len());
    let mut in_ws = false;
    for ch in unfolded.chars() {
        if ch == ' ' || ch == '\t' {
            if !in_ws {
                collapsed.push(' ');
                in_ws = true;
            }
        } else {
            collapsed.push(ch);
            in_ws = false;
        }
    }
    collapsed.trim().to_string()
}

/// Return `raw_header_value` with the contents of its `b=` tag removed (the tag
/// and its `=` are kept; only the value between `b=` and the next `;` or the end
/// is deleted). RFC 6376 section 3.7 step 4.
fn empty_b_tag(raw_header_value: &str) -> String {
    // Find the `b=` tag. It is a tag boundary: preceded by start-or-`;`-or-WSP
    // and the letter `b` then `=`. Scanning for the exact `b=` that is a tag
    // (not the `b` inside `bh=`) is done by requiring the char before `b` to be
    // a tag separator.
    let bytes = raw_header_value.as_bytes();
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'b' && idx + 1 < bytes.len() && bytes[idx + 1] == b'=' {
            let prev_is_boundary = idx == 0 || {
                let p = bytes[idx - 1];
                p == b';' || p == b' ' || p == b'\t' || p == b'\n' || p == b'\r'
            };
            if prev_is_boundary {
                // Keep up to and including `b=`, then drop until the next `;`.
                let keep = &raw_header_value[..=idx + 1];
                let after = &raw_header_value[idx + 2..];
                let tail = match after.find(';') {
                    Some(pos) => &after[pos..],
                    None => "",
                };
                return format!("{keep}{tail}");
            }
        }
        idx += 1;
    }
    // No b= tag found; return unchanged (the caller validated its presence).
    raw_header_value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(name: &str, raw_value: &str) -> Header {
        Header {
            name: name.to_string(),
            raw_value: raw_value.to_string(),
        }
    }

    #[test]
    fn parses_pairs_and_defaults() {
        assert_eq!(
            Canonicalization::parse("relaxed/relaxed").unwrap(),
            Canonicalization {
                header: HeaderCanon::Relaxed,
                body: BodyCanon::Relaxed
            }
        );
        // A lone header algorithm defaults the body half to simple.
        assert_eq!(
            Canonicalization::parse("relaxed").unwrap(),
            Canonicalization {
                header: HeaderCanon::Relaxed,
                body: BodyCanon::Simple
            }
        );
    }

    #[test]
    fn simple_header_is_verbatim() {
        let c = Canonicalization {
            header: HeaderCanon::Simple,
            body: BodyCanon::Simple,
        };
        // Simple keeps the leading space and case exactly.
        let out = c.canonicalize_header(&header("From", " Alice <a@b.c>"));
        assert_eq!(out, b"From: Alice <a@b.c>\r\n");
    }

    #[test]
    fn relaxed_header_lowercases_name_and_collapses_ws() {
        let c = Canonicalization {
            header: HeaderCanon::Relaxed,
            body: BodyCanon::Simple,
        };
        // RFC 6376 example: "A: X" -> "a:X"; whitespace around value trimmed.
        assert_eq!(c.canonicalize_header(&header("A", " X ")), b"a:X\r\n");
        // Folded value with tabs collapses to single spaces.
        let out = c.canonicalize_header(&header("Subject", " hello\r\n\tthere   world"));
        assert_eq!(out, b"subject:hello there world\r\n");
    }

    #[test]
    fn dkim_signature_b_tag_is_emptied_no_trailing_crlf() {
        let c = Canonicalization {
            header: HeaderCanon::Relaxed,
            body: BodyCanon::Relaxed,
        };
        let raw = " v=1; a=rsa-sha256; bh=ZZZZ; h=from; b=SIGNATUREHERE";
        let out = c.canonicalize_dkim_signature(raw);
        let s = String::from_utf8(out).unwrap();
        // b= is emptied, bh= is untouched, no trailing CRLF, name lowercased.
        assert!(s.starts_with("dkim-signature:"));
        assert!(s.contains("bh=ZZZZ"));
        assert!(s.ends_with("b="));
        assert!(!s.ends_with("\r\n"));
    }

    #[test]
    fn empty_b_does_not_touch_bh() {
        // The `b` inside `bh=` must not be mistaken for the `b=` tag.
        let raw = "a=rsa-sha256; bh=BODYHASH; b=SIG";
        let out = empty_b_tag(raw);
        assert_eq!(out, "a=rsa-sha256; bh=BODYHASH; b=");
    }

    #[test]
    fn empty_b_when_b_is_not_last_tag() {
        let raw = "b=SIG; h=from";
        let out = empty_b_tag(raw);
        assert_eq!(out, "b=; h=from");
    }
}
