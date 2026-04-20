use dialog_artifacts::DialogArtifactsError;
use dialog_credentials::Ed25519SignerError;
use dialog_effects::archive::ArchiveError;
use dialog_effects::authority::AuthorityError;
use dialog_effects::memory::MemoryError;
use dialog_effects::storage::StorageError;
use dialog_prolly_tree::DialogProllyTreeError;
use dialog_storage::DialogStorageError;
use thiserror::Error;

/// The common error type used by repository operations.
#[derive(Error, Debug)]
pub enum RepositoryError {
    /// Branch with the given name was not found.
    #[error("Branch {name} not found")]
    BranchNotFound {
        /// The name of the branch that was not found.
        name: String,
    },

    /// Branch has no configured upstream.
    #[error("Branch {name} has no upstream")]
    BranchHasNoUpstream {
        /// The name of the branch that has no upstream.
        name: String,
    },

    /// Branch upstream is set to itself.
    #[error("Upstream of local {name} is set to itself")]
    BranchUpstreamIsItself {
        /// Branch name.
        name: String,
    },

    /// Remote repository not found.
    #[error("Remote {remote} not found")]
    RemoteNotFound {
        /// Remote site name.
        remote: String,
    },

    /// Remote repository already exists.
    #[error("Remote {remote} already exists")]
    RemoteAlreadyExists {
        /// Remote site name.
        remote: String,
    },

    /// Pushing a revision failed.
    #[error("Pushing revision failed: {cause}")]
    PushFailed {
        /// The underlying error message.
        cause: String,
    },

    /// Identifying the current authority failed.
    #[error("Identify failed: {0}")]
    Identify(#[from] AuthorityError),

    /// A memory effect (publish/resolve) failed.
    #[error(transparent)]
    Memory(#[from] MemoryError),

    /// An archive effect (get/put) failed.
    #[error(transparent)]
    Archive(#[from] ArchiveError),

    /// A storage-backed operation failed.
    #[error(transparent)]
    Storage(#[from] DialogStorageError),

    /// An operator-space storage effect failed.
    #[error(transparent)]
    OperatorStorage(#[from] StorageError),

    /// Key generation or signing failed.
    #[error(transparent)]
    Signer(#[from] Ed25519SignerError),

    /// An artifact operation (tree manipulation, keyview, etc.) failed.
    #[error(transparent)]
    Artifact(#[from] DialogArtifactsError),

    /// A prolly tree operation failed.
    #[error(transparent)]
    Tree(#[from] DialogProllyTreeError),

    /// Repository not found.
    #[error("Repository '{0}' not found")]
    NotFound(String),

    /// Repository already exists.
    #[error("Repository '{0}' already exists")]
    AlreadyExists(String),

    /// Attempted to use a verifier-only credential where a signer is required.
    #[error("Expected signer credential, got verifier-only")]
    SignerRequired,

    /// Invalid internal state (should never happen in normal operation).
    #[error("Invalid state: {message}")]
    InvalidState {
        /// Description of the invalid state.
        message: String,
    },
}
