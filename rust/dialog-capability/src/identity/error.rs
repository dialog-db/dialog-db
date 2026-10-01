use thiserror::Error;

/// What can go wrong turning bytes or strings into a name.
///
/// Narrower than the artifact-layer error it used to be a part of:
/// naming fails for three reasons, none of which involve storage or
/// trees, so the identity vocabulary carries its own.
#[derive(Debug, Error, PartialEq)]
pub enum IdentityError {
    /// Could not interpret some string as a URI.
    #[error("Could not parse as URI: {0}")]
    InvalidUri(String),

    /// Raw bytes could not be interpreted as an entity.
    #[error("Could not convert bytes into entity: {0}")]
    InvalidEntity(String),

    /// A stored index entry failed read-side validation: its entity is
    /// not a canonical URI. Surfaced so a scan can skip the entry
    /// rather than failing the whole query.
    #[error("Corrupt stored entry: {0}")]
    CorruptEntry(String),
}
