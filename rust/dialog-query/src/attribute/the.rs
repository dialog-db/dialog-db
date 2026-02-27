use crate::artifact::{Attribute as ArtifactsAttribute, DialogArtifactsError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum length in bytes for an attribute selector (`"domain/name"`).
pub const MAX_SELECTOR_LENGTH: usize = 64;

/// Nominal relation identifier comprised of the domain and name
/// components. It denotes the kind of relation entity and value
/// form. Validates on construction.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct The {
    /// The attribute domain.
    domain: String,
    /// The attribute name.
    name: String,
}

impl The {
    /// Returns the attribute domain.
    pub fn domain(&self) -> &str {
        &self.domain
    }

    /// Returns the attribute name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for The {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.domain, self.name)
    }
}

impl std::str::FromStr for The {
    type Err = DialogArtifactsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (domain, name) = s.split_once('/').ok_or_else(|| {
            DialogArtifactsError::InvalidAttribute(format!(
                "Attribute format is \"domain/predicate\", but got \"{s}\""
            ))
        })?;
        // Validate via ArtifactsAttribute to enforce length limit
        let _: ArtifactsAttribute = s.parse()?;
        Ok(Self {
            domain: domain.to_owned(),
            name: name.to_owned(),
        })
    }
}

impl From<The> for ArtifactsAttribute {
    fn from(the: The) -> Self {
        the.to_string()
            .parse()
            .expect("The is always a valid ArtifactsAttribute")
    }
}

impl From<&The> for ArtifactsAttribute {
    fn from(the: &The) -> Self {
        the.to_string()
            .parse()
            .expect("The is always a valid ArtifactsAttribute")
    }
}

impl From<ArtifactsAttribute> for The {
    fn from(attr: ArtifactsAttribute) -> Self {
        let s = attr.to_string();
        let (domain, name) = s
            .split_once('/')
            .expect("ArtifactsAttribute always contains '/'");
        Self {
            domain: domain.to_owned(),
            name: name.to_owned(),
        }
    }
}

impl Serialize for The {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for The {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Compile-time validated attribute selector.
///
/// Verifies at compile time that the literal:
/// - does not exceed [`MAX_SELECTOR_LENGTH`] bytes
/// - contains a `'/'` separator
///
/// # Examples
///
/// ```
/// use dialog_query::the;
/// let selector = the!("person/name");
/// assert_eq!(selector.to_string(), "person/name");
/// ```
#[macro_export]
macro_rules! the {
    ($selector:literal) => {{
        const _: () = {
            assert!(
                $selector.len() <= $crate::attribute::MAX_SELECTOR_LENGTH,
                "attribute selector exceeds maximum length of 64 bytes"
            );
            let bytes = $selector.as_bytes();
            let mut found = false;
            let mut i = 0;
            while i < bytes.len() {
                if bytes[i] == b'/' {
                    found = true;
                    break;
                }
                i += 1;
            }
            assert!(found, "attribute selector must contain '/' separator");
        };
        // SAFETY: compile-time checks above guarantee the literal is valid.
        <$crate::attribute::The as ::std::str::FromStr>::from_str($selector).unwrap()
    }};
}
