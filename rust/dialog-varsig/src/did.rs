//! DID (Decentralized Identifier) types.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{fmt, str::FromStr};

/// A [Decentralized Identifier][spec] string.
///
/// Wraps a raw DID string like `did:key:z6Mk...` or `did:web:example.com`.
/// Use [`method()`][Did::method] to inspect the DID method at runtime.
///
/// A DID is immutable once parsed, and handles to one (subjects,
/// capabilities, catalogs) are cloned on every read that names it, so
/// the string is shared rather than copied by each clone.
///
/// [spec]: https://www.w3.org/TR/did-core/
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Did(Arc<str>);

impl Did {
    /// Get the raw DID string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the DID method name (e.g. `"key"` for `did:key:...`,
    /// `"web"` for `did:web:...`).
    ///
    /// # Panics
    ///
    /// Panics if the DID string is malformed (no second `:`). This
    /// cannot happen for values created via [`FromStr`].
    #[must_use]
    #[allow(clippy::expect_used)]
    pub fn method(&self) -> &str {
        let after_did = &self.0["did:".len()..];
        after_did
            .split(':')
            .next()
            .expect("DID has no method segment")
    }
}

impl AsRef<str> for Did {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<&Did> for Did {
    fn from(did: &Did) -> Self {
        did.clone()
    }
}

impl fmt::Debug for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for Did {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Error when parsing a DID string.
#[derive(Debug, Clone, thiserror::Error)]
#[error("invalid DID: {0}")]
pub struct DidParseError(pub String);

impl FromStr for Did {
    type Err = DidParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if !is_did(s) {
            return Err(DidParseError(format!(
                "expected did:method:identifier in DID syntax, got: {s}"
            )));
        }
        Ok(Did(Arc::from(s)))
    }
}

/// Whether `s` is a DID, or a DID URL, by the [DID syntax][syntax]:
///
/// ```text
/// did-url            = did path-abempty [ "?" query ] [ "#" fragment ]
/// did                = "did:" method-name ":" method-specific-id
/// method-name        = 1*( %x61-7A / DIGIT )
/// method-specific-id = *( *idchar ":" ) 1*idchar
/// idchar             = ALPHA / DIGIT / "." / "-" / "_" / pct-encoded
/// pct-encoded        = "%" HEXDIG HEXDIG
/// ```
///
/// with `path-abempty`, `query` and `fragment` as in RFC 3986. A DID URL
/// selects within what a DID resolves to (`did:web:example.com#key-1`, a
/// key of its document), so it is a `Did` too.
///
/// A DID names a subject, and a subject is an entity, so every DID must
/// also be an entity URI. This syntax guarantees it: no whitespace, no
/// stray punctuation, nothing a URI parser would reject or rewrite.
///
/// One method outside the syntax is accepted: `_`, the method of
/// `did:_:_`, which stands for any subject in a delegation's scope.
///
/// [syntax]: https://www.w3.org/TR/did-core/#did-syntax
#[must_use]
pub const fn is_did(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 4 || b[0] != b'd' || b[1] != b'i' || b[2] != b'd' || b[3] != b':' {
        return false;
    }
    let mut i = 4;
    let method = i;
    if i < b.len() && b[i] == b'_' {
        i += 1;
    } else {
        while i < b.len() && (b[i].is_ascii_lowercase() || b[i].is_ascii_digit()) {
            i += 1;
        }
    }
    if i == method || i == b.len() || b[i] != b':' {
        return false;
    }
    i += 1;

    // The identifier: segments of idchars between colons, the last one
    // not empty.
    let mut segment = 0;
    while i < b.len() {
        let c = b[i];
        if c == b':' {
            segment = 0;
            i += 1;
        } else if c == b'%' {
            if !pct_encoded(b, i) {
                return false;
            }
            segment += 1;
            i += 3;
        } else if c.is_ascii_alphanumeric() || c == b'.' || c == b'-' || c == b'_' {
            segment += 1;
            i += 1;
        } else {
            break;
        }
    }
    if segment == 0 {
        return false;
    }
    if i < b.len() && b[i] != b'/' && b[i] != b'?' && b[i] != b'#' {
        return false;
    }

    // What a DID URL adds: a path, a query, a fragment, each optional and
    // in that order.
    let mut fragment = false;
    while i < b.len() {
        let c = b[i];
        if c == b'%' {
            if !pct_encoded(b, i) {
                return false;
            }
            i += 3;
            continue;
        }
        let pchar = c.is_ascii_alphanumeric()
            || matches!(
                c,
                b'-' | b'.'
                    | b'_'
                    | b'~'
                    | b'!'
                    | b'$'
                    | b'&'
                    | b'\''
                    | b'('
                    | b')'
                    | b'*'
                    | b'+'
                    | b','
                    | b';'
                    | b'='
                    | b':'
                    | b'@'
            );
        if c == b'#' {
            if fragment {
                return false;
            }
            fragment = true;
        } else if !pchar && c != b'/' && c != b'?' {
            return false;
        }
        i += 1;
    }
    true
}

/// Whether `b[i]` starts a `%XX` escape.
const fn pct_encoded(b: &[u8], i: usize) -> bool {
    i + 2 < b.len() && b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit()
}

impl TryFrom<String> for Did {
    type Error = DidParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl Serialize for Did {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Did {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Creates a [`Did`] from a string literal, validated at compile time.
///
/// The `"did:"` prefix is added automatically — pass `"method:identifier"`.
///
/// ```
/// use dialog_varsig::did;
///
/// let d = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
/// assert_eq!(d.method(), "key");
///
/// let w = did!("web:example.com");
/// assert_eq!(w.method(), "web");
/// ```
///
/// Invalid literals fail at compile time:
/// ```compile_fail
/// use dialog_varsig::did;
/// let _bad = did!("nocolon");
/// ```
#[macro_export]
macro_rules! did {
    ($s:literal) => {{
        const _: () = assert!(
            $crate::did::is_did(concat!("did:", $s)),
            "expected \"method:identifier\" in DID syntax"
        );
        #[allow(clippy::expect_used)]
        format!("did:{}", $s)
            .parse::<$crate::did::Did>()
            // The const assertion above validated it
            .expect("Invalid did 'did:{$s}'")
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_dids_in_did_syntax() {
        for did in [
            "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK",
            "did:web:example.com",
            "did:web:example.com%3A8080:user:alice",
            "did:example:a::b",
            "did:_:_",
            "did:web:pick.example#key-2",
            "did:web:example.com/path",
            "did:key:z6Mk/index/abc",
            "did:key:abc?query#fragment",
        ] {
            assert!(did.parse::<Did>().is_ok(), "{did}");
        }
    }

    #[test]
    fn it_refuses_what_is_not_did_syntax() {
        for not in [
            "",
            "did:",
            "did:key",
            "did:key:",
            "did::abc",
            "did:Key:abc",
            "did:_key:abc",
            "did:key:a:",
            "did:key:has space",
            "did:key:abc, ",
            "did:key:abc,",
            "did:key:abc#a#b",
            "did:key:abc#frag ment",
            "did:key:abc/pa th",
            "did:key:line\nbreak",
            "did:key:abc%2",
            "did:key:abc%zz",
            "key:abc",
        ] {
            assert!(
                matches!(not.parse::<Did>(), Err(DidParseError(_))),
                "{not:?} is refused"
            );
        }
    }
}
