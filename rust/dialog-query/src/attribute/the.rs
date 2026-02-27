use crate::artifact::Attribute as ArtifactsAttribute;
use crate::error::{InvalidIdentifier, OwnedInvalidIdentifier};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Maximum length in bytes for a relation identifier (`"domain/name"`).
pub const MAX_RELATION_LENGTH: usize = 64;

/// Nominal relation identifier in `domain/name` format. Represents the
/// `the` component of a claim, categorizing it by the kind of association
/// being established. The domain scopes the relation to a specific problem
/// area; the name identifies the specific association within that domain.
///
/// This is a transparent wrapper over [`ArtifactsAttribute`] that adds
/// stricter validation (lowercase, kebab-case, etc.) on construction.
///
/// Validates on construction:
///
/// - Total `domain/name` must not exceed [`MAX_RELATION_LENGTH`] (64) bytes.
/// - **Domain**: lowercase letters, digits, hyphens, and dots.
///   Must start with a letter. No trailing dots or hyphens.
///   At least one character.
/// - **Name**: lowercase kebab-case (letters, digits, hyphens; no dots).
///   Must start with a letter. No trailing hyphen.
///   At least one character.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct The(ArtifactsAttribute);

impl The {
    /// Validates a relation literal as raw bytes.
    ///
    /// This is a `const fn` returning `Result` so it can be used both at
    /// compile time (from the [`the!`] macro) and at runtime (from
    /// [`FromStr`]).
    pub const fn validate(input: &str) -> Result<(), InvalidIdentifier<'_>> {
        let bytes = input.as_bytes();
        macro_rules! fail {
            ($reason:literal) => {
                Err(InvalidIdentifier {
                    input: bytes,
                    reason: $reason,
                })
            };
        }

        if bytes.len() > MAX_RELATION_LENGTH {
            return fail!("exceeds maximum length of 64 bytes");
        }

        // Find the '/' separator
        let mut slash = 0;
        let mut found = false;
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'/' {
                if found {
                    return fail!("must contain exactly one '/'");
                }
                found = true;
                slash = i;
            }
            i += 1;
        }
        if !found {
            return fail!("must contain '/' separator");
        }
        if slash == 0 {
            return fail!("domain must not be empty");
        }
        if slash + 1 >= bytes.len() {
            return fail!("name must not be empty");
        }

        // Validate domain (bytes[0..slash])
        if !(bytes[0] >= b'a' && bytes[0] <= b'z') {
            return fail!("domain must start with a lowercase letter");
        }
        let last_domain = bytes[slash - 1];
        if last_domain == b'.' || last_domain == b'-' {
            return fail!("domain must not end with a dot or hyphen");
        }

        let mut j = 0;
        while j < slash {
            let b = bytes[j];
            if !((b >= b'a' && b <= b'z') || (b >= b'0' && b <= b'9') || b == b'-' || b == b'.') {
                return fail!(
                    "domain must contain only lowercase letters, digits, hyphens, or dots"
                );
            }
            j += 1;
        }

        // Validate name (bytes[slash+1..])
        let name_start = slash + 1;
        if !(bytes[name_start] >= b'a' && bytes[name_start] <= b'z') {
            return fail!("name must start with a lowercase letter");
        }
        if bytes[bytes.len() - 1] == b'-' {
            return fail!("name must not end with a hyphen");
        }

        let mut m = name_start;
        while m < bytes.len() {
            let b = bytes[m];
            if !((b >= b'a' && b <= b'z') || (b >= b'0' && b <= b'9') || b == b'-') {
                return fail!("name must contain only lowercase letters, digits, or hyphens");
            }
            m += 1;
        }

        Ok(())
    }

    /// Returns the relation domain.
    pub fn domain(&self) -> &str {
        let bytes = self.0.key_bytes();
        let slash = bytes
            .iter()
            .position(|&b| b == b'/')
            .expect("The always contains '/'");
        std::str::from_utf8(&bytes[..slash]).expect("domain is valid UTF-8")
    }

    /// Returns the relation name.
    pub fn name(&self) -> &str {
        let bytes = self.0.key_bytes();
        let slash = bytes
            .iter()
            .position(|&b| b == b'/')
            .expect("The always contains '/'");
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        std::str::from_utf8(&bytes[slash + 1..end]).expect("name is valid UTF-8")
    }
}

impl std::fmt::Display for The {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for The {
    type Err = OwnedInvalidIdentifier;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::validate(s).map_err(|e| OwnedInvalidIdentifier {
            input: s.to_owned(),
            reason: e.reason,
        })?;

        let attr: ArtifactsAttribute = s.parse().expect("already validated format and length");

        Ok(Self(attr))
    }
}

impl From<The> for ArtifactsAttribute {
    fn from(the: The) -> Self {
        the.0
    }
}

impl From<&The> for ArtifactsAttribute {
    fn from(the: &The) -> Self {
        the.0.clone()
    }
}

impl From<ArtifactsAttribute> for The {
    fn from(attr: ArtifactsAttribute) -> Self {
        Self(attr)
    }
}

impl TryFrom<crate::artifact::Value> for The {
    type Error = crate::artifact::TypeError;

    fn try_from(value: crate::artifact::Value) -> Result<Self, Self::Error> {
        let attr = ArtifactsAttribute::try_from(value)?;
        Ok(Self(attr))
    }
}

impl Serialize for The {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
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

/// Compile-time validated relation.
///
/// Validates the literal against the formal notation rules:
/// - Total length does not exceed [`MAX_RELATION_LENGTH`] bytes
/// - Contains exactly one `'/'` separator
/// - Domain: lowercase letters, digits, hyphens, dots; starts with a
///   letter; no trailing dot or hyphen; at least one character
/// - Name: lowercase kebab-case (letters, digits, hyphens); starts with
///   a letter; no trailing hyphen; at least one character
///
/// # Examples
///
/// ```
/// use dialog_query::the;
/// let relation = the!("io.example/name");
/// assert_eq!(relation.to_string(), "io.example/name");
/// ```
#[macro_export]
macro_rules! the {
    ($source:literal) => {{
        const _: () = {
            match $crate::attribute::The::validate($source) {
                Ok(()) => {}
                Err(e) => panic!("{}", e.reason)
            }
        };
        // SAFETY: compile-time validation above guarantees the literal is valid.
        <$crate::attribute::The as ::std::convert::From<$crate::attribute::ArtifactsAttribute>>::from(
            <$crate::attribute::ArtifactsAttribute as ::std::str::FromStr>::from_str($source)
                .unwrap(),
        )
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<The, OwnedInvalidIdentifier> {
        s.parse()
    }

    // Valid relations

    #[dialog_common::test]
    fn it_parses_simple_relation() {
        let the = parse("person/name").unwrap();
        assert_eq!(the.domain(), "person");
        assert_eq!(the.name(), "name");
    }

    #[dialog_common::test]
    fn it_parses_dotted_domain() {
        let the = parse("io.gozala.person/name").unwrap();
        assert_eq!(the.domain(), "io.gozala.person");
        assert_eq!(the.name(), "name");
    }

    #[dialog_common::test]
    fn it_parses_kebab_case_name() {
        let the = parse("diy.cook/ingredient-name").unwrap();
        assert_eq!(the.name(), "ingredient-name");
    }

    #[dialog_common::test]
    fn it_parses_single_char_components() {
        let the = parse("a/b").unwrap();
        assert_eq!(the.domain(), "a");
        assert_eq!(the.name(), "b");
    }

    #[dialog_common::test]
    fn it_parses_digits_in_domain() {
        let the = parse("web3/token").unwrap();
        assert_eq!(the.domain(), "web3");
    }

    #[dialog_common::test]
    fn it_parses_digits_in_name() {
        let the = parse("person/address2").unwrap();
        assert_eq!(the.name(), "address2");
    }

    #[dialog_common::test]
    fn it_parses_hyphen_in_domain() {
        let the = parse("my-app/name").unwrap();
        assert_eq!(the.domain(), "my-app");
    }

    #[dialog_common::test]
    fn it_displays_as_domain_slash_name() {
        let the = parse("io.example/name").unwrap();
        assert_eq!(the.to_string(), "io.example/name");
    }

    #[dialog_common::test]
    fn it_round_trips_through_serde() {
        let original = parse("diy.cook/quantity").unwrap();
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, "\"diy.cook/quantity\"");
        let restored: The = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[dialog_common::test]
    fn it_accepts_max_length_relation() {
        // 64 bytes exactly: 31-char domain + '/' + 32-char name
        let domain = "a".repeat(31);
        let name = "b".repeat(32);
        let relation = format!("{domain}/{name}");
        assert_eq!(relation.len(), 64);
        assert!(parse(&relation).is_ok());
    }

    #[dialog_common::test]
    fn it_converts_to_artifacts_attribute() {
        let the = parse("person/name").unwrap();
        let attr: ArtifactsAttribute = the.into();
        assert_eq!(attr.to_string(), "person/name");
    }

    #[dialog_common::test]
    fn it_converts_from_artifacts_attribute() {
        let attr: ArtifactsAttribute = "person/name".parse().unwrap();
        let the = The::from(attr);
        assert_eq!(the.to_string(), "person/name");
    }

    // Invalid relations

    #[dialog_common::test]
    fn it_rejects_missing_slash() {
        assert!(parse("personname").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_empty_string() {
        assert!(parse("").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_empty_domain() {
        assert!(parse("/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_empty_name() {
        assert!(parse("person/").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_exceeding_max_length() {
        let relation = format!("{}/{}", "a".repeat(32), "b".repeat(32));
        assert_eq!(relation.len(), 65);
        assert!(parse(&relation).is_err());
    }

    #[dialog_common::test]
    fn it_rejects_uppercase_in_domain() {
        assert!(parse("Person/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_uppercase_in_name() {
        assert!(parse("person/Name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_domain_starting_with_digit() {
        assert!(parse("3person/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_domain_starting_with_hyphen() {
        assert!(parse("-person/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_domain_starting_with_dot() {
        assert!(parse(".person/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_domain_ending_with_dot() {
        assert!(parse("person./name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_domain_ending_with_hyphen() {
        assert!(parse("person-/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_name_starting_with_digit() {
        assert!(parse("person/1name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_name_starting_with_hyphen() {
        assert!(parse("person/-name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_name_ending_with_hyphen() {
        assert!(parse("person/name-").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_dot_in_name() {
        assert!(parse("person/first.name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_underscore_in_domain() {
        assert!(parse("my_app/name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_underscore_in_name() {
        assert!(parse("person/first_name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_space_in_relation() {
        assert!(parse("person/my name").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_multiple_slashes() {
        assert!(parse("a/b/c").is_err());
    }

    #[dialog_common::test]
    fn it_rejects_serde_invalid_relation() {
        let result = serde_json::from_str::<The>("\"Person/Name\"");
        assert!(result.is_err());
    }
}
