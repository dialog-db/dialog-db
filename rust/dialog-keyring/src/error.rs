//! Errors from keyring resolution and sealing.

use crate::EpochId;

/// What can go wrong resolving an epoch or opening a sealed blob.
#[derive(Debug, thiserror::Error)]
pub enum KeyringError {
    /// The epoch a sealed blob names is not in this keyring's log.
    ///
    /// Not a corruption: it means the epoch record has not replicated yet.
    /// Merging the writer's log makes the blob readable.
    #[error("epoch {0} is not in this keyring")]
    UnknownEpoch(EpochId),

    /// The encoded blob is too short to hold a header.
    #[error("malformed sealed blob")]
    Malformed,

    /// The blob names a header version this build does not know.
    #[error("unsupported sealed blob version {0}")]
    UnsupportedVersion(u8),

    /// The blob could not be opened.
    ///
    /// Wrong key, or tampering. The two are deliberately not distinguished,
    /// so nothing can be probed by watching which error comes back.
    #[error("could not open sealed blob")]
    Failed,

    /// A platform crypto operation failed.
    #[error("crypto operation failed: {0}")]
    Crypto(String),

    /// No secret is held for the level generation a node was sealed under.
    ///
    /// Either this party was never granted that level, or it was removed
    /// before the generation was minted.
    #[error("no secret for generation {0}")]
    MissingGeneration(EpochId),

    /// A fetched block does not hash to the address it was fetched by.
    ///
    /// Detectable without any key, which is what lets a replicator refuse
    /// corrupt data it cannot read.
    #[error("block {0} does not match its address")]
    Corrupt(dialog_common::Blake3Hash),

    /// No block is known for a node: its address was never learned from a
    /// parent, or the store does not hold it.
    #[error("no block for node {0}")]
    UnknownNode(dialog_common::Blake3Hash),

    /// A node's plaintext could not be read as a search-tree node.
    #[error("not a search-tree node: {0}")]
    Node(String),

    /// Reading or writing the underlying store failed.
    #[error("storage failed: {0}")]
    Storage(String),

    /// The archive refused or failed a read or write of envelopes.
    ///
    /// Keeps the archive's own error, so an authorization decision stays
    /// distinguishable from a storage failure.
    #[error(transparent)]
    Archive(#[from] dialog_effects::archive::ArchiveError),

    /// The platform would not supply entropy for a new epoch.
    #[error("entropy unavailable: {0}")]
    Entropy(String),
}

impl From<dialog_credentials::secret::SecretError> for KeyringError {
    fn from(error: dialog_credentials::secret::SecretError) -> Self {
        match error {
            dialog_credentials::secret::SecretError::Failed => Self::Failed,
            other => Self::Crypto(other.to_string()),
        }
    }
}
