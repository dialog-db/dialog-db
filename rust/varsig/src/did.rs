//! DID (Decentralized Identifier) types.

use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

/// A [Decentralized Identifier][spec] string.
///
/// Wraps a raw DID string like `did:key:z6Mk...` or `did:web:example.com`.
/// Use [`method()`][Did::method] to inspect the DID method at runtime.
///
/// [spec]: https://www.w3.org/TR/did-core/
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Did(String);

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
        if !s.starts_with("did:") {
            return Err(DidParseError(format!("expected did: prefix, got: {s}")));
        }
        // Must have at least did:method:identifier
        let rest = &s["did:".len()..];
        if !rest.contains(':') {
            return Err(DidParseError(format!(
                "expected did:method:identifier, got: {s}"
            )));
        }
        Ok(Did(s.to_string()))
    }
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
/// use varsig::did;
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
/// use varsig::did;
/// let _bad = did!("nocolon");
/// ```
#[macro_export]
macro_rules! did {
    ($s:literal) => {{
        const _: () = {
            let b = $s.as_bytes();
            let mut i = 0;
            let mut found_colon = false;
            while i < b.len() {
                if b[i] == b':' {
                    assert!(i > 0, "DID method must not be empty");
                    assert!(i + 1 < b.len(), "DID identifier must not be empty");
                    found_colon = true;
                    break;
                }
                i += 1;
            }
            assert!(found_colon, "expected \"method:identifier\"");
        };
        #[allow(clippy::expect_used)]
        format!("did:{}", $s)
            .parse::<$crate::did::Did>()
            // The cons block above validated the format ensuring this
            // never happens
            .expect("Invalid did 'did:{$s}'")
    }};
}
