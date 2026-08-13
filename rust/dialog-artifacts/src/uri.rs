use std::{
    fmt::{Display, Formatter, Result as FmtResult},
    hash::{Hash, Hasher},
    io,
    io::Write,
    ops::Deref,
    str::FromStr,
};

use serde::{Deserialize, Serialize, de::Error as _};
use url::Url;

use base58::ToBase58;
use ed25519_dalek::SigningKey;

use crate::{DialogArtifactsError, ENTITY_LENGTH, make_reference, mutable_slice};

/// A [`Uri`] is a helper type that helps validate and reliably convert between
/// plain string URIs (which typically represent an [`Entity`]) and their other
/// representations such as their byte representation when used as a component
/// of an index key.
///
/// Internally this holds the NORMALIZED string form: parsing goes through
/// [`url::Url`] once at every ingest boundary ([`FromStr`], deserialization),
/// and what is stored is the parser's normalized rendering. Strings read back
/// out of the index are exactly these normalized renderings, so the read path
/// admits them via [`from_stored`](Uri::from_stored), which verifies they are
/// canonical (parse + equality with the parse's own rendering) and adopts the
/// stored string without re-rendering it. A stored string that fails that
/// check is a corrupt or foreign-written entry; it is rejected as
/// [`CorruptEntry`](DialogArtifactsError::CorruptEntry) so readers can ignore
/// the row. Either way a [`Uri`] holds a valid canonical URI by construction.
#[derive(Serialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(transparent)]
pub struct Uri(Box<str>);

impl Hash for Uri {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<'de> Deserialize<'de> for Uri {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Deserialization is an ingest boundary: the bytes may come from
        // anywhere, so they get the full parse-and-normalize treatment.
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(D::Error::custom)
    }
}

impl Uri {
    /// Generate a globally unique URI. The raw format will be an ed25519 DID
    /// Key.
    pub fn unique() -> Result<Self, DialogArtifactsError> {
        const PREFIX: &str = "z6Mk";

        let key = [
            PREFIX,
            SigningKey::generate(&mut rand::thread_rng())
                .verifying_key()
                .as_bytes()
                .as_ref()
                .to_base58()
                .as_str(),
        ]
        .concat();

        format!("did:key:{key}")
            .parse()
            .map_err(|error| DialogArtifactsError::InvalidEntity(format!("{error}")))
    }

    /// Builds a [`Uri`] from a string read back out of the index, validating
    /// that it is a URI in CANONICAL form: it parses as a [`url::Url`] and it
    /// is exactly the parser's own rendering of itself.
    ///
    /// Every entity string this crate writes into the index is the normalized
    /// rendering of a parsed [`url::Url`] ([`FromStr`] or deserialization
    /// stores nothing else), so for our own data the canonicality check always
    /// passes and the stored string is adopted as-is — no re-render, no second
    /// allocation. A string that fails either check did not come from this
    /// writer: the entry is corrupt or foreign, and the error is
    /// [`CorruptEntry`](DialogArtifactsError::CorruptEntry) so scan paths can
    /// ignore the row rather than fail the query. An [`Entity`](crate::Entity)
    /// therefore holds a valid canonical URI by construction on every path,
    /// stored reads included.
    pub(crate) fn from_stored(s: &str) -> Result<Self, DialogArtifactsError> {
        let url: Url = s.parse().map_err(|error| {
            DialogArtifactsError::CorruptEntry(format!("stored entity is not a URI: {error}"))
        })?;
        // Canonicality subsumes the whitespace/control guard in `from_str`:
        // `url::Url::parse` strips those characters, so a string containing
        // them can never equal its own parse's rendering.
        if url.as_str() != s {
            return Err(DialogArtifactsError::CorruptEntry(format!(
                "stored entity is not a canonical URI rendering: {s:?}"
            )));
        }
        Ok(Self(s.into()))
    }

    /// The URI as its normalized string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert this [`Uri`] to the byte representation expected for use as part
    /// of an index key.
    ///
    /// The layout is 64 bytes wide. The first 32 bytes contain the first 32 bytes
    /// of the UTF-8-encoded URI string; the last 32 bytes are the hash of any
    /// remaining bytes in the URI string (or else all zeroes).
    pub fn key_bytes(&self) -> Result<[u8; ENTITY_LENGTH], DialogArtifactsError> {
        let format = |bytes: &[u8]| {
            let mut key_bytes = [0u8; 64];

            if let Some((l, r)) = bytes.split_at_checked(32) {
                let rest = make_reference(r);

                mutable_slice!(key_bytes, 0, 32).write_all(l)?;
                mutable_slice!(key_bytes, 32, 32).write_all(rest.as_ref())?;
            } else {
                mutable_slice!(key_bytes, 0, 32).write_all(bytes)?;
            }

            Ok(key_bytes) as Result<[u8; 64], io::Error>
        };

        format(self.0.as_bytes()).map_err(|error| {
            DialogArtifactsError::InvalidEntity(format!("Could not format as key bytes: {error}"))
        })
    }
}

impl Display for Uri {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str(&self.0)
    }
}

impl From<Uri> for String {
    fn from(value: Uri) -> Self {
        value.0.into()
    }
}

impl Deref for Uri {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl FromStr for Uri {
    type Err = DialogArtifactsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Reject strings that only *look* URI-ish because they carry
        // whitespace. `url::Url::parse` is lenient: it silently strips
        // ASCII whitespace — spaces, tabs, and crucially `\r`/`\n` — and
        // lowercases the scheme, so an arbitrary text value like
        // `"Tonk-Prose-Version: 1\r\n…"` parses "successfully" into a
        // mangled URL (newlines gone, first token lowercased). Because
        // `Value` deserializes untagged and tries `Entity` before
        // `String`, that mangled parse would win and a plain multi-line
        // text value stored in an entity-less field would be corrupted.
        // A canonical URI never contains raw whitespace or control
        // characters, so their presence marks the value as text, not a
        // URI. (We deliberately don't require full round-trip stability:
        // `url` legitimately normalizes some real URLs, e.g. adding a
        // trailing slash to `https://host`, and those are valid entities.)
        if s.chars().any(|c| c.is_ascii_whitespace() || c.is_control()) {
            return Err(DialogArtifactsError::InvalidUri(format!(
                "URI must not contain whitespace or control characters: {s:?}"
            )));
        }
        // Parse plainly, with no memo. A memo here looks attractive (joins
        // re-parse the same entity per outer binding) but any bounded,
        // admit-on-miss cache inverts into pure overhead the moment a scan
        // materializes more DISTINCT entities than it can hold: every row
        // then pays the parse PLUS the memo's bookkeeping. Measured on the
        // query benches, a 4096-entry memo regressed 10k-row
        // scan-and-materialize by ~31% while buying joins only ~8% at
        // 1000 rows (their gains come from the engine, not this parse) —
        // and sieve eviction or second-sight admission only shrank, never
        // removed, the loss. (Strings coming back OUT of the index go
        // through `from_stored` instead, which validates canonicality
        // without re-rendering.)
        //
        // What is stored is the parser's normalized rendering, not the
        // input string — that is what makes `from_stored`'s canonicality
        // check exact: our own writes always pass it.
        let url: Url = s
            .parse()
            .map_err(|error| DialogArtifactsError::InvalidUri(format!("{error}")))?;
        Ok(Uri(String::from(url).into()))
    }
}

impl TryFrom<String> for Uri {
    type Error = DialogArtifactsError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

#[cfg(test)]
mod tests {
    use crate::Entity;
    use anyhow::Result;

    use super::Uri;
    use crate::DialogArtifactsError;

    /// `from_stored` admits exactly the canonical renderings `from_str`
    /// stores, and rejects both non-URIs and valid-but-non-canonical
    /// strings as `CorruptEntry`.
    #[test]
    fn it_validates_stored_strings_as_canonical_uris() {
        assert!(Uri::from_stored("https://google.com/").is_ok());
        assert!(Uri::from_stored("did:key:z6MkExample").is_ok());
        assert!(matches!(
            Uri::from_stored("not a uri"),
            Err(DialogArtifactsError::CorruptEntry(_))
        ));
        // Parses as a URL, but is not the parser's own rendering of itself.
        assert!(matches!(
            Uri::from_stored("HTTPS://Google.com"),
            Err(DialogArtifactsError::CorruptEntry(_))
        ));

        // Whatever `from_str` normalizes and stores, `from_stored` admits:
        // the canonicality check is exact for this writer's own output.
        for raw in ["HTTPS://Google.com", "https://example.com", "user:alice"] {
            let parsed: Uri = raw.parse().expect("parses");
            assert!(Uri::from_stored(parsed.as_str()).is_ok(), "{raw}");
        }
    }

    #[test]
    fn it_can_convert_to_key_bytes() -> Result<()> {
        let entity: Entity = "https://google.com".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                104, 116, 116, 112, 115, 58, 47, 47, 103, 111, 111, 103, 108, 101, 46, 99, 111,
                109, 47, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let entity: Entity = "https://www.reddit.com/r/Wellington/".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                104, 116, 116, 112, 115, 58, 47, 47, 119, 119, 119, 46, 114, 101, 100, 100, 105,
                116, 46, 99, 111, 109, 47, 114, 47, 87, 101, 108, 108, 105, 110, 103, 174, 109, 72,
                16, 228, 74, 156, 26, 71, 116, 75, 44, 178, 112, 196, 124, 85, 229, 151, 72, 94,
                42, 78, 114, 123, 226, 181, 252, 47, 68, 96, 188
            ]
        );

        let entity: Entity = "did:web:cdata.earth".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                100, 105, 100, 58, 119, 101, 98, 58, 99, 100, 97, 116, 97, 46, 101, 97, 114, 116,
                104, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let entity: Entity = "did:web:anubiarts.neocities.org".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                100, 105, 100, 58, 119, 101, 98, 58, 97, 110, 117, 98, 105, 97, 114, 116, 115, 46,
                110, 101, 111, 99, 105, 116, 105, 101, 115, 46, 111, 114, 103, 0, 0, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        );

        let entity: Entity = "did:key:z6Mk2WiNvjBbuWZ8jYNmFzh4uFyt8iqwpDND6ymg6KnKzchw".parse()?;

        println!("\n{entity}");
        println!("{:?}", entity.key_bytes());
        assert_eq!(
            entity.key_bytes(),
            &[
                100, 105, 100, 58, 107, 101, 121, 58, 122, 54, 77, 107, 50, 87, 105, 78, 118, 106,
                66, 98, 117, 87, 90, 56, 106, 89, 78, 109, 70, 122, 104, 52, 145, 178, 200, 111,
                186, 28, 163, 145, 181, 81, 20, 47, 75, 48, 26, 200, 30, 45, 131, 111, 84, 186,
                185, 89, 166, 62, 252, 15, 108, 30, 140, 116
            ]
        );

        Ok(())
    }
}
