//! Resolving an epoch to a key, and minting new ones.

use dialog_common::ConditionalSync;

use crate::{Epoch, EpochId, EpochLog, KeyringError, Sealed};

/// Domain separator for epoch key derivation.
const KEY_DOMAIN: &[u8] = b"dialog/keyring/epoch-key/v1";

/// What the sealing layer needs from key agreement, and nothing more.
///
/// This is the seam BeeKEM eventually sits behind. Everything above it — the
/// wire format, the derived nonce, the addressing, the tests — is written
/// against these three operations and does not change when the implementation
/// underneath grows a tree.
#[allow(async_fn_in_trait)]
pub trait Keyring: ConditionalSync {
    /// The epoch new content should be sealed under.
    fn current(&self) -> EpochId;

    /// Resolve an epoch named by a sealed blob, however old.
    ///
    /// This is why the log is permanent: a blob written three rotations ago
    /// is still readable, and nothing was ever re-encrypted to keep it that
    /// way.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownEpoch`] if the epoch has not replicated.
    async fn key(&self, epoch: &EpochId) -> Result<[u8; 32], KeyringError>;

    /// Mint a new epoch with fresh entropy.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Entropy`] if the platform will not supply
    /// randomness.
    async fn rotate(&mut self) -> Result<EpochId, KeyringError>;
}

/// Seal and open, for any [`Keyring`].
#[allow(async_fn_in_trait)]
pub trait KeyringExt: Keyring {
    /// Seal `plain` under the current epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if the current epoch cannot be resolved or the
    /// platform's cipher fails.
    async fn seal(&self, plain: &[u8]) -> Result<Sealed, KeyringError> {
        let epoch = self.current();
        let key = self.key(&epoch).await?;
        Sealed::seal(&key, &epoch, plain).await
    }

    /// Open a blob sealed under any epoch this keyring can resolve.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownEpoch`] if the blob's epoch has not
    /// replicated, or [`KeyringError::Failed`] if it does not open.
    async fn open(&self, sealed: &Sealed) -> Result<Vec<u8>, KeyringError> {
        let key = self.key(sealed.epoch()).await?;
        sealed.open(&key).await
    }
}

impl<K: Keyring> KeyringExt for K {}

/// A keyring with one shared secret and no key agreement.
///
/// Every epoch's key is `HKDF(space_secret, epoch_id)`, so anyone holding the
/// space secret resolves every epoch whose record they have replicated. It is
/// a *degenerate* BeeKEM, not a mock: the same epoch log, the same header, the
/// same resolution path, with the tree collapsed to a single shared secret.
///
/// # What it deliberately does not provide
///
/// **Post-compromise security.** Rotation here changes which key new content
/// is sealed under, but anyone holding the space secret derives every epoch,
/// past and future. That is not a bug to fix at this layer — it is the reason
/// BeeKEM exists. Swapping this for a CGKA-backed implementation turns `key`
/// from a KDF into a walk up a tree, and rotation starts to mean something.
///
/// Useful today for a single profile's own devices, where there is nobody to
/// lock out: the space secret reaches each device through
/// [`dialog_credentials::secret`], and an untrusted blob store sees nothing.
#[derive(Clone, Debug)]
pub struct LocalKeyring {
    /// The secret every epoch key is derived from.
    secret: [u8; 32],
    /// Every epoch this replica knows.
    log: EpochLog,
    /// The epoch this replica seals new content under.
    current: EpochId,
}

impl LocalKeyring {
    /// Create a keyring with a genesis epoch built from fresh entropy.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Entropy`] if the platform will not supply
    /// randomness.
    pub fn create(secret: [u8; 32]) -> Result<Self, KeyringError> {
        Ok(Self::genesis(secret, entropy()?))
    }

    /// Create a keyring whose genesis epoch uses the given entropy.
    ///
    /// Deterministic, so a test can build the same keyring twice and check
    /// that two replicas seal identical content identically.
    #[must_use]
    pub fn genesis(secret: [u8; 32], entropy: [u8; 32]) -> Self {
        let mut log = EpochLog::new();
        let current = log.insert(Epoch::new([], entropy));
        Self {
            secret,
            log,
            current,
        }
    }

    /// The epoch log, as it would be replicated.
    #[must_use]
    pub fn log(&self) -> &EpochLog {
        &self.log
    }

    /// Mint an epoch with the given entropy rather than sampling it.
    ///
    /// The deterministic counterpart of [`Keyring::rotate`], for tests that
    /// need a reproducible epoch name.
    pub fn rotate_with(&mut self, entropy: [u8; 32]) -> EpochId {
        let epoch = Epoch::new(self.log.heads(), entropy);
        self.current = self.log.insert(epoch);
        self.current.clone()
    }

    /// Take in another replica's epoch log and settle on a shared epoch.
    ///
    /// After a partition in which both sides rotated, this leaves both
    /// replicas holding every epoch — so each can open what the other wrote —
    /// and pointing at the same current epoch, so their subsequent writes
    /// converge again without either of them rotating.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Malformed`] if the merged log is empty.
    pub fn merge(&mut self, other: &EpochLog) -> Result<(), KeyringError> {
        self.log.merge(other);
        self.current = self.log.settled_head()?;
        Ok(())
    }
}

impl Keyring for LocalKeyring {
    fn current(&self) -> EpochId {
        self.current.clone()
    }

    async fn key(&self, epoch: &EpochId) -> Result<[u8; 32], KeyringError> {
        if !self.log.contains(epoch) {
            return Err(KeyringError::UnknownEpoch(epoch.clone()));
        }
        let mut info = Vec::with_capacity(KEY_DOMAIN.len() + 32);
        info.extend_from_slice(KEY_DOMAIN);
        info.extend_from_slice(epoch.as_bytes());
        Ok(dialog_credentials::symmetric::derive_key(&self.secret, &info).await?)
    }

    async fn rotate(&mut self) -> Result<EpochId, KeyringError> {
        Ok(self.rotate_with(entropy()?))
    }
}

/// Sample 32 bytes of platform entropy.
fn entropy() -> Result<[u8; 32], KeyringError> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|e| KeyringError::Entropy(e.to_string()))?;
    Ok(bytes)
}
