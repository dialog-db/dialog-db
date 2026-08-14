use dialog_capability::access::AuthorizeError;
use dialog_effects::Rejection;
use dialog_effects::archive::ArchiveError;
use dialog_effects::memory::MemoryError;
use dialog_search_tree::DialogSearchTreeError;
use dialog_storage::DialogStorageError;
use thiserror::Error;

use crate::ValueDataType;

/// The common error type used by this crate
#[derive(Error, Debug, PartialEq)]
pub enum DialogArtifactsError {
    /// An error occured in storage-related code
    #[error("Storage operation failed: {0}")]
    Storage(String),

    /// An error occurred in prolly tree-related code
    #[error("Tree operation failed: {0}")]
    Tree(String),

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// A database index was not shaped as expected
    #[error("Malformed database index: {0}")]
    MalformedIndex(String),

    /// Raw bytes could not be interpreted as a version
    #[error("Could not convert bytes into version: {0}")]
    InvalidRevision(String),

    /// Raw bytes could not be interpreted as a database index key
    #[error("Could not convert bytes into key: {0}")]
    InvalidKey(String),

    /// Could not interpret some string as a URI
    #[error("Could not parse as URI: {0}")]
    InvalidUri(String),

    /// Raw bytes could not be interpreted as a typed value
    #[error("Could not convert bytes into value: {0}")]
    InvalidValue(String),

    /// A causal reference was invalid
    #[error("Could not convert bytes into reference: {0}")]
    InvalidReference(String),

    /// Raw bytes could not be interpreted as an attribute
    #[error("Invalid attribute: {0}")]
    InvalidAttribute(String),

    /// The attribute belongs to the reserved `dialog.` namespace, which
    /// only version-control machinery may write
    #[error("Reserved attribute (the dialog. namespace is reserved): {0}")]
    ReservedAttribute(String),

    /// Raw bytes could not be interpreted as an entity
    #[error("Could not convert bytes into entity: {0}")]
    InvalidEntity(String),

    /// An attempt to export the database failed
    #[error("Could not export data: {0}")]
    Export(String),

    /// Attempted to query with an unconstrained [`ArtifactSelector`]
    #[error("An artifact selector must specify at least one field")]
    EmptySelector,

    /// A revision signature or structural integrity check failed
    #[error("Invalid revision signature: {0}")]
    InvalidSignature(String),

    /// Causal ordering could not be determined because claims for the given
    /// version have not been replicated yet
    #[error("Incomplete history: missing claims for version {0}")]
    IncompleteHistory(String),
}

impl From<DialogStorageError> for DialogArtifactsError {
    fn from(error: DialogStorageError) -> Self {
        match error {
            DialogStorageError::Authorization(error) => Self::Authorization(error),
            DialogStorageError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<ArchiveError> for DialogArtifactsError {
    fn from(error: ArchiveError) -> Self {
        match error {
            ArchiveError::Authorization(error) => Self::Authorization(error),
            ArchiveError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<MemoryError> for DialogArtifactsError {
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::Authorization(error) => Self::Authorization(error),
            MemoryError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<DialogSearchTreeError> for DialogArtifactsError {
    fn from(error: DialogSearchTreeError) -> Self {
        match error {
            DialogSearchTreeError::Storage(DialogStorageError::Authorization(error)) => {
                Self::Authorization(error)
            }
            DialogSearchTreeError::Storage(DialogStorageError::Rejected(error)) => {
                Self::Rejected(error)
            }
            error => Self::Tree(error.to_string()),
        }
    }
}

/// Errors created when types are used inconsistently with value.
#[derive(Error, Debug, PartialEq)]
pub enum TypeError {
    /// Expected type and actual type mismatch.
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(ValueDataType, ValueDataType),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unused_async)]

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_capability::access::AuthorizeError;
    use dialog_effects::archive::ArchiveError;
    use dialog_storage::DialogStorageError;

    use super::*;

    fn revoked() -> AuthorizeError {
        AuthorizeError::Revoked {
            subject: dialog_capability::did!(
                "did:key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"
            ),
        }
    }

    // This is the hop that used to flatten everything: the conversion
    // rescued one variant by name and sent the rest through
    // `to_string()`, so a reason built two crates down died here and
    // callers were left parsing messages. The fallback arm still exists
    // for genuinely unstructured failures, which is why this needs a
    // test rather than the compiler -- adding a variant and forgetting
    // an arm degrades silently instead of failing the build.
    #[dialog_common::test]
    async fn it_keeps_the_reason_a_conversion_used_to_flatten() {
        let error = DialogArtifactsError::from(ArchiveError::Authorization(revoked()));
        assert!(
            matches!(
                error,
                DialogArtifactsError::Authorization(AuthorizeError::Revoked { .. })
            ),
            "the reason survives the hop, got {error:?}"
        );

        let through_storage =
            DialogArtifactsError::from(DialogStorageError::Authorization(revoked()));
        assert!(
            matches!(
                through_storage,
                DialogArtifactsError::Authorization(AuthorizeError::Revoked { .. })
            ),
            "and survives arriving via storage, got {through_storage:?}"
        );
    }
}
