//! Minimal RFC 5322 email parsing: split a raw message into its header block and
//! body, and expose each header as a (name, raw-value) pair.
//!
//! DKIM canonicalization needs the header *exactly as it appeared on the wire*
//! for `simple` canonicalization, and a whitespace-normalized form for
//! `relaxed`. So a [`Header`] stores the raw value including any internal
//! folding (continuation lines and their CRLFs), not an unfolded value. The
//! caller (canonicalization) decides how to normalize.

use super::error::DkimError;

/// A single email header: its field name and its raw field value.
///
/// The `name` is the text before the first colon, trimmed of surrounding
/// whitespace (header field names never contain whitespace). The `raw_value` is
/// everything after the colon up to (but not including) the CRLF that ends the
/// header, with continuation lines preserved verbatim (their leading CRLF and
/// whitespace are part of `raw_value`). This verbatim form is what `simple`
/// canonicalization requires.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    /// The field name, as it appeared, trimmed of whitespace (for example
    /// `"From"` or `"DKIM-Signature"`).
    pub name: String,
    /// The raw field value, continuation lines included, no trailing CRLF.
    pub raw_value: String,
}

impl Header {
    /// Whether this header's name equals `other` case-insensitively (header
    /// field names are case-insensitive per RFC 5322).
    #[must_use]
    pub fn name_eq_ignore_case(&self, other: &str) -> bool {
        self.name.eq_ignore_ascii_case(other)
    }
}

/// A parsed email: an ordered list of headers plus the raw body.
///
/// Headers are kept in the order they appeared, which matters: DKIM's `h=` tag
/// selects headers bottom-up when a name appears more than once, and the
/// signature header reconstruction depends on that order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// The headers, in the order they appeared in the message.
    pub headers: Vec<Header>,
    /// The raw body (everything after the blank line separating headers from
    /// body). Empty if the message had no body.
    pub body: Vec<u8>,
}

impl Message {
    /// Parse a raw email into headers and body.
    ///
    /// Accepts both CRLF (`\r\n`, the wire form) and bare LF (`\n`, common in
    /// files) line endings: the header/body split and folding detection treat a
    /// bare LF as a line ending. The stored `raw_value` normalizes every line
    /// ending to CRLF so downstream canonicalization sees consistent bytes
    /// regardless of how the fixture was saved.
    ///
    /// # Errors
    ///
    /// Returns [`DkimError::MalformedEmail`] if there is a header line with no
    /// colon, or the input is empty.
    pub fn parse(raw: &[u8]) -> Result<Self, DkimError> {
        // Normalize to a String with CRLF endings. DKIM operates on ASCII header
        // text; non-UTF-8 header bytes are not something we sign over here.
        let text = String::from_utf8_lossy(raw);
        let normalized = normalize_crlf(&text);

        // The header block ends at the first blank line (CRLF CRLF).
        let (header_block, body) = match normalized.split_once("\r\n\r\n") {
            Some((h, b)) => (h, b.as_bytes().to_vec()),
            // No blank line: the whole thing is headers, no body. Strip a
            // trailing CRLF if present so the last header has no dangling break.
            None => (
                normalized
                    .strip_suffix("\r\n")
                    .unwrap_or(normalized.as_str()),
                Vec::new(),
            ),
        };

        if header_block.is_empty() {
            return Err(DkimError::MalformedEmail("no headers".into()));
        }

        let headers = parse_headers(header_block)?;
        Ok(Self { headers, body })
    }

    /// The last header (bottom-most) whose name matches `name` case-insensitively.
    ///
    /// DKIM's `DKIM-Signature` header is found this way, and `h=` selection is
    /// bottom-up, so "last" is the DKIM-relevant choice when a name repeats.
    #[must_use]
    pub fn last_header(&self, name: &str) -> Option<&Header> {
        self.headers
            .iter()
            .rev()
            .find(|h| h.name_eq_ignore_case(name))
    }
}

/// Normalize all line endings (CRLF or bare LF) to CRLF.
fn normalize_crlf(text: &str) -> String {
    // Replace CRLF first would double-count, so strip any CR then re-add before
    // each LF. This maps CRLF, CR, and LF all to a single CRLF.
    let without_cr = text.replace('\r', "");
    without_cr.replace('\n', "\r\n")
}

/// Parse a CRLF-normalized header block into ordered headers, joining
/// continuation (folded) lines into their parent header's raw value.
fn parse_headers(block: &str) -> Result<Vec<Header>, DkimError> {
    let mut headers: Vec<Header> = Vec::new();

    for line in split_keep_crlf(block) {
        // A continuation line begins with whitespace (SP or HTAB). It belongs to
        // the previous header's raw value, folding and all.
        let starts_folded = line.starts_with(' ') || line.starts_with('\t');
        if starts_folded {
            let current = headers.last_mut().ok_or_else(|| {
                DkimError::MalformedEmail("header starts with folding whitespace".into())
            })?;
            current.raw_value.push_str("\r\n");
            current.raw_value.push_str(line);
            continue;
        }

        let (name, value) = line.split_once(':').ok_or_else(|| {
            DkimError::MalformedEmail(format!("header line has no colon: {line:?}"))
        })?;
        // The field name is the text before the colon with surrounding
        // whitespace removed; the value keeps its leading space for now (relaxed
        // canonicalization trims it, simple keeps it).
        headers.push(Header {
            name: name.trim().to_string(),
            raw_value: value.to_string(),
        });
    }

    if headers.is_empty() {
        return Err(DkimError::MalformedEmail("no headers".into()));
    }
    Ok(headers)
}

/// Split a CRLF-joined block into its logical lines, dropping the CRLFs.
fn split_keep_crlf(block: &str) -> impl Iterator<Item = &str> {
    block.split("\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_headers_and_body() {
        let raw = b"From: alice@example.com\r\nSubject: hello\r\n\r\nbody here";
        let msg = Message::parse(raw).unwrap();
        assert_eq!(msg.headers.len(), 2);
        assert_eq!(msg.headers[0].name, "From");
        assert_eq!(msg.headers[0].raw_value, " alice@example.com");
        assert_eq!(msg.headers[1].name, "Subject");
        assert_eq!(msg.body, b"body here");
    }

    #[test]
    fn joins_folded_continuation_lines() {
        let raw =
            b"DKIM-Signature: v=1; a=rsa-sha256;\r\n d=example.com; s=sel\r\nFrom: a@b.c\r\n\r\n";
        let msg = Message::parse(raw).unwrap();
        let dkim = msg.last_header("dkim-signature").unwrap();
        // The folded line and its CRLF are preserved verbatim in the raw value.
        assert_eq!(
            dkim.raw_value,
            " v=1; a=rsa-sha256;\r\n d=example.com; s=sel"
        );
    }

    #[test]
    fn accepts_bare_lf_line_endings() {
        let raw = b"From: a@b.c\nSubject: hi\n\nbody";
        let msg = Message::parse(raw).unwrap();
        assert_eq!(msg.headers.len(), 2);
        assert_eq!(msg.body, b"body");
    }

    #[test]
    fn last_header_is_bottom_most() {
        let raw = b"Received: one\r\nReceived: two\r\nFrom: a@b.c\r\n\r\n";
        let msg = Message::parse(raw).unwrap();
        assert_eq!(msg.last_header("received").unwrap().raw_value, " two");
    }

    #[test]
    fn header_with_no_colon_is_rejected() {
        let raw = b"not a header\r\n\r\n";
        assert!(matches!(
            Message::parse(raw),
            Err(DkimError::MalformedEmail(_))
        ));
    }
}
