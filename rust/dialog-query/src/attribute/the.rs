use crate::artifact::{Attribute as ArtifactsAttribute, DialogArtifactsError};

/// Maximum length in bytes for an attribute selector (`"namespace/name"`).
pub const MAX_SELECTOR_LENGTH: usize = 64;

/// A validated attribute selector (`"namespace/name"`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct The {
    /// The domain namespace.
    pub namespace: String,
    /// The attribute name.
    pub name: String,
}

impl std::fmt::Display for The {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.namespace, self.name)
    }
}

impl std::str::FromStr for The {
    type Err = DialogArtifactsError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (namespace, name) = s.split_once('/').ok_or_else(|| {
            DialogArtifactsError::InvalidAttribute(format!(
                "Attribute format is \"namespace/predicate\", but got \"{s}\""
            ))
        })?;
        // Validate via ArtifactsAttribute to enforce length limit
        let _: ArtifactsAttribute = s.parse()?;
        Ok(Self {
            namespace: namespace.to_owned(),
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
        let (namespace, name) = s
            .split_once('/')
            .expect("ArtifactsAttribute always contains '/'");
        Self {
            namespace: namespace.to_owned(),
            name: name.to_owned(),
        }
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
