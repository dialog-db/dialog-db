use dialog_capability::access::{AuthorizationFailure, AuthorizeError};
use dialog_capability::identity::IdentityError;
use dialog_effects::Rejection;
use dialog_effects::archive::ArchiveError;
use dialog_effects::blob::BlobError;
use dialog_effects::memory::MemoryError;
use dialog_search_tree::DialogSearchTreeError;
use thiserror::Error;

use crate::ValueDataType;

/// The common error type used by this crate
#[derive(Error, Debug, PartialEq)]
pub enum DialogCorpusError {
    /// An error occured in storage-related code
    #[error("Storage operation failed: {0}")]
    Storage(String),

    /// An error occurred in probabilistic search tree-related code
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
    InvalidRelation(String),

    /// The attribute belongs to the reserved `dialog.` namespace, which
    /// only version-control machinery may write
    #[error("Reserved attribute (the dialog. namespace is reserved): {0}")]
    ReservedRelation(String),

    /// A batch that changes assets was handed to a target that holds facts
    /// only, such as an ephemeral line or the replica registry. Assets are
    /// stored by a transaction's commit; anywhere else their changes would
    /// be dropped, so the batch is refused instead. Names the target.
    #[error("Assets can only be changed by a transaction's commit, not by {0}")]
    AssetsUnsupported(String),

    /// Raw bytes could not be interpreted as an entity
    #[error("Could not convert bytes into entity: {0}")]
    InvalidEntity(String),

    /// A stored index entry failed read-side validation: its entity is not a
    /// canonical URI, its attribute breaks the attribute invariants, or a
    /// column is not UTF-8. The bytes came out of the tree, so this marks a
    /// corrupt or foreign-written entry rather than caller error. Scan paths
    /// treat rows carrying this error as ignorable — they are skipped (with a
    /// warning) instead of failing the whole query — so an invalid entry in a
    /// tree cannot poison every query that ranges over it.
    #[error("Corrupt stored entry: {0}")]
    CorruptEntry(String),

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

impl AuthorizationFailure for DialogCorpusError {
    fn authorization_failure(&self) -> Option<&AuthorizeError> {
        match self {
            Self::Authorization(reason) => Some(reason),
            _ => None,
        }
    }
}

impl From<ArchiveError> for DialogCorpusError {
    fn from(error: ArchiveError) -> Self {
        match error {
            ArchiveError::Authorization(error) => Self::Authorization(error),
            ArchiveError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<BlobError> for DialogCorpusError {
    fn from(error: BlobError) -> Self {
        match error {
            BlobError::Authorization(error) => Self::Authorization(error),
            BlobError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<MemoryError> for DialogCorpusError {
    fn from(error: MemoryError) -> Self {
        match error {
            MemoryError::Authorization(error) => Self::Authorization(error),
            MemoryError::Rejected(error) => Self::Rejected(error),
            error => Self::Storage(error.to_string()),
        }
    }
}

impl From<DialogSearchTreeError> for DialogCorpusError {
    fn from(error: DialogSearchTreeError) -> Self {
        if let Some(reason) = error.authorization_failure() {
            return Self::Authorization(reason.clone());
        }
        if let Some(rejection) = error.rejection() {
            return Self::Rejected(rejection.clone());
        }
        Self::Tree(error.to_string())
    }
}

/// Errors created when types are used inconsistently with value.
#[derive(Error, Debug, PartialEq)]
pub enum TypeError {
    /// Expected type and actual type mismatch.
    #[error("Type mismatch: expected {0}, got {1}")]
    TypeMismatch(ValueDataType, ValueDataType),
}

/// Flattened, not nested: the corrupt-entry path matches on
/// [`DialogCorpusError::CorruptEntry`] to decide whether to skip a
/// row rather than fail the scan, so an identity failure has to arrive
/// as that same variant.
impl From<IdentityError> for DialogCorpusError {
    fn from(error: IdentityError) -> Self {
        match error {
            IdentityError::InvalidUri(reason) => Self::InvalidUri(reason),
            IdentityError::InvalidEntity(reason) => Self::InvalidEntity(reason),
            IdentityError::CorruptEntry(reason) => Self::CorruptEntry(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unused_async)]

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use dialog_capability::access::AuthorizeError;
    use dialog_effects::archive::ArchiveError;
    use dialog_search_tree::helpers::test_catalog;
    use dialog_search_tree::nodes;

    use super::*;
    use crate::tree::CorpusNode;

    fn revoked() -> AuthorizeError {
        AuthorizeError::Revoked {
            subject: dialog_capability::did!(
                "did:key:z6MkrCD1csqtgdj8sRHYRPGLYcMFXAoDhkgvHNq2FML2xqCX"
            ),
            certificate: None,
            revocation: None,
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
        let error = DialogCorpusError::from(ArchiveError::Authorization(revoked()));
        assert!(
            matches!(
                error,
                DialogCorpusError::Authorization(AuthorizeError::Revoked { .. })
            ),
            "the reason survives the hop, got {error:?}"
        );

        // A refusal the block layer makes while a node is read arrives as
        // the tree's error, and has to come out the same way.
        let read = nodes::Read::<CorpusNode>::new(
            test_catalog(),
            dialog_common::Blake3Hash::from([0u8; 32]),
        );
        let through_tree = read
            .through_loader(
                |_| async { Err(ArchiveError::Authorization(revoked())) },
                None,
            )
            .await
            .expect_err("the load refused");
        let through_tree = DialogCorpusError::from(through_tree);
        assert!(
            matches!(
                through_tree,
                DialogCorpusError::Authorization(AuthorizeError::Revoked { .. })
            ),
            "and survives arriving through a node read, got {through_tree:?}"
        );
    }
}
