//! Command helpers.
//!
//! Commands MUST be lowercase, and begin with a slash (`/`).
//! Segments MUST be separated by a slash.
//! A trailing slash MUST NOT be present.

use serde::{Deserialize, Serialize, Serializer};
use thiserror::Error;

/// Errors that can occur when parsing a Command.
#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
pub enum CommandParseError {
    /// Command must begin with a slash (`/`).
    #[error("command must begin with a slash")]
    MissingLeadingSlash,

    /// Command must not have a trailing slash.
    #[error("command must not have a trailing slash")]
    TrailingSlash,

    /// Command must be lowercase.
    #[error("command must be lowercase")]
    NotLowercase,

    /// Command segments must not be empty (e.g., `/crud//create` is invalid).
    #[error("command segments must not be empty")]
    EmptySegment,
}

/// Command type representing a sequence of command segments.
///
/// Commands are `/`-delimited paths that describe a set of commands.
/// For example: `/`, `/crud`, `/crud/create`, `/msg/send`.
///
/// Valid commands:
/// - `/` (root command - all commands)
/// - `/crud`
/// - `/crud/create`
/// - `/msg/send`
/// - `/foo/bar/baz`
///
/// Invalid commands:
/// - `crud` (missing leading slash)
/// - `/crud/` (trailing slash)
/// - `/CRUD` (not lowercase)
/// - `/crud//create` (empty segment)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct Command(pub Vec<String>);

impl Command {
    /// Parse a command string into a Command.
    ///
    /// # Errors
    ///
    /// Returns an error if the command string is not valid:
    /// - Missing leading slash
    /// - Has trailing slash (except for root `/`)
    /// - Contains uppercase characters
    /// - Contains empty segments
    pub fn parse(s: &str) -> Result<Self, CommandParseError> {
        // Must begin with a slash
        if !s.starts_with('/') {
            return Err(CommandParseError::MissingLeadingSlash);
        }

        // Root command "/" is valid
        if s == "/" {
            return Ok(Command(vec![]));
        }

        // Must not have trailing slash (except root)
        if s.ends_with('/') {
            return Err(CommandParseError::TrailingSlash);
        }

        // Must be lowercase
        if s.chars().any(char::is_uppercase) {
            return Err(CommandParseError::NotLowercase);
        }

        // Parse segments (skip first empty segment from leading slash)
        let segments: Vec<String> = s[1..].split('/').map(String::from).collect();

        // Check for empty segments (e.g., "/crud//create")
        if segments.iter().any(String::is_empty) {
            return Err(CommandParseError::EmptySegment);
        }

        Ok(Command(segments))
    }

    /// Create a new Command from a vector of strings.
    ///
    /// This does not validate the segments. Use `parse` for validated construction.
    #[must_use]
    pub const fn new(segments: Vec<String>) -> Self {
        Command(segments)
    }

    /// Get the segments of the command.
    #[must_use]
    pub const fn segments(&self) -> &Vec<String> {
        &self.0
    }

    /// Check if the command starts with the given prefix.
    #[must_use]
    pub fn starts_with(&self, prefix: &Command) -> bool {
        if prefix.0.len() > self.0.len() {
            return false;
        }
        self.0.iter().zip(&prefix.0).all(|(a, b)| a == b)
    }
}

impl From<Vec<String>> for Command {
    fn from(segments: Vec<String>) -> Self {
        Command::new(segments)
    }
}

impl From<Command> for Vec<String> {
    fn from(command: Command) -> Self {
        command.0
    }
}

impl std::fmt::Display for Command {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            f.write_str("/")
        } else {
            write!(f, "/{}", self.0.join("/"))
        }
    }
}

impl Serialize for Command {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Command {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Command::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl std::str::FromStr for Command {
    type Err = CommandParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Command::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Valid command examples from the spec
    #[test]
    fn test_valid_root_command() {
        let cmd = Command::parse("/").unwrap();
        assert_eq!(cmd.segments().len(), 0);
        assert_eq!(cmd.to_string(), "/");
    }

    #[test]
    fn test_valid_single_segment() {
        let cmd = Command::parse("/crud").unwrap();
        assert_eq!(cmd.segments(), &["crud"]);
        assert_eq!(cmd.to_string(), "/crud");
    }

    #[test]
    fn test_valid_two_segments() {
        let cmd = Command::parse("/crud/create").unwrap();
        assert_eq!(cmd.segments(), &["crud", "create"]);
        assert_eq!(cmd.to_string(), "/crud/create");
    }

    #[test]
    fn test_valid_many_segments() {
        let cmd = Command::parse("/foo/bar/baz/qux/quux").unwrap();
        assert_eq!(cmd.segments(), &["foo", "bar", "baz", "qux", "quux"]);
        assert_eq!(cmd.to_string(), "/foo/bar/baz/qux/quux");
    }

    #[test]
    fn test_valid_unicode() {
        // From spec: /ほげ/ふが
        let cmd = Command::parse("/ほげ/ふが").unwrap();
        assert_eq!(cmd.segments(), &["ほげ", "ふが"]);
        assert_eq!(cmd.to_string(), "/ほげ/ふが");
    }

    // Invalid command examples
    #[test]
    fn test_invalid_missing_leading_slash() {
        let err = Command::parse("crud").unwrap_err();
        assert_eq!(err, CommandParseError::MissingLeadingSlash);
    }

    #[test]
    fn test_invalid_trailing_slash() {
        let err = Command::parse("/crud/").unwrap_err();
        assert_eq!(err, CommandParseError::TrailingSlash);
    }

    #[test]
    fn test_invalid_trailing_slash_nested() {
        let err = Command::parse("/crud/create/").unwrap_err();
        assert_eq!(err, CommandParseError::TrailingSlash);
    }

    #[test]
    fn test_invalid_uppercase() {
        let err = Command::parse("/CRUD").unwrap_err();
        assert_eq!(err, CommandParseError::NotLowercase);
    }

    #[test]
    fn test_invalid_mixed_case() {
        let err = Command::parse("/Crud/Create").unwrap_err();
        assert_eq!(err, CommandParseError::NotLowercase);
    }

    #[test]
    fn test_invalid_empty_segment() {
        let err = Command::parse("/crud//create").unwrap_err();
        assert_eq!(err, CommandParseError::EmptySegment);
    }

    // Roundtrip tests
    #[test]
    fn test_json_roundtrip() {
        let original = "\"/msg/send\"";
        let cmd: Command = serde_json::from_str(original).unwrap();
        let serialized = serde_json::to_string(&cmd).unwrap();
        assert_eq!(serialized, original);

        let cmd2: Command = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, cmd2);
    }

    #[test]
    fn test_json_roundtrip_root() {
        let original = "\"/\"";
        let cmd: Command = serde_json::from_str(original).unwrap();
        let serialized = serde_json::to_string(&cmd).unwrap();
        assert_eq!(serialized, original);

        let cmd2: Command = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, cmd2);
    }

    #[test]
    fn test_cbor_roundtrip() {
        let cmd: Command = Command::parse("/store/put").unwrap();

        let cbor = serde_ipld_dagcbor::to_vec(&cmd).unwrap();
        let cmd2: Command = serde_ipld_dagcbor::from_slice(&cbor).unwrap();
        assert_eq!(cmd, cmd2);

        let cbor2 = serde_ipld_dagcbor::to_vec(&cmd2).unwrap();
        assert_eq!(cbor, cbor2);
    }

    #[test]
    fn test_cbor_roundtrip_root() {
        let cmd: Command = Command::parse("/").unwrap();

        let cbor = serde_ipld_dagcbor::to_vec(&cmd).unwrap();
        let cmd2: Command = serde_ipld_dagcbor::from_slice(&cbor).unwrap();
        assert_eq!(cmd, cmd2);

        let cbor2 = serde_ipld_dagcbor::to_vec(&cmd2).unwrap();
        assert_eq!(cbor, cbor2);
    }

    // Deserialization should reject invalid commands
    #[test]
    fn test_deserialize_rejects_missing_leading_slash() {
        let result: Result<Command, _> = serde_json::from_str("\"crud\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_rejects_trailing_slash() {
        let result: Result<Command, _> = serde_json::from_str("\"/crud/\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_rejects_uppercase() {
        let result: Result<Command, _> = serde_json::from_str("\"/CRUD\"");
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_rejects_empty_segment() {
        let result: Result<Command, _> = serde_json::from_str("\"/crud//create\"");
        assert!(result.is_err());
    }

    // starts_with tests (for delegation hierarchy)
    #[test]
    fn test_starts_with_root_matches_all() {
        let root = Command::parse("/").unwrap();
        let cmd = Command::parse("/crypto/sign").unwrap();
        assert!(cmd.starts_with(&root));
    }

    #[test]
    fn test_starts_with_prefix_matches() {
        let prefix = Command::parse("/crypto").unwrap();
        let cmd = Command::parse("/crypto/sign").unwrap();
        assert!(cmd.starts_with(&prefix));
    }

    #[test]
    fn test_starts_with_different_prefix_no_match() {
        let prefix = Command::parse("/crypto").unwrap();
        let cmd = Command::parse("/stack/pop").unwrap();
        assert!(!cmd.starts_with(&prefix));
    }

    #[test]
    fn test_starts_with_similar_prefix_no_match() {
        // /crypto cannot prove /cryptocurrency
        let prefix = Command::parse("/crypto").unwrap();
        let cmd = Command::parse("/cryptocurrency").unwrap();
        assert!(!cmd.starts_with(&prefix));
    }
}
