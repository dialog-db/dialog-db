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
