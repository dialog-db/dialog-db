//! The sealed blob: what a content-addressed store actually holds.
//!
//! # Wire format
//!
//! ```text
//! version(1) || epoch(32) || nonce(12) || ciphertext || tag(16)
//! ```
//!
//! The header is plaintext because it has to be: a reader needs to know which
//! epoch to resolve *before* it can decrypt anything. It is authenticated —
//! version and epoch are the AEAD's additional data — so it can be read
//! without being trusted.
//!
//! # Why the nonce is derived from the plaintext
//!
//! A prolly tree is content-addressed, and the whole point of that is that two
//! replicas which independently compute the same node compute the same hash,
//! so a diff can prune matching subtrees without reading them. A random nonce
//! would destroy that: identical nodes would seal to different bytes, hash
//! differently, and every diff would see changes that are not there.
//!
//! So the nonce is `blake3::keyed_hash(key, domain || epoch || plaintext)`,
//! truncated to 12 bytes. Same plaintext under the same epoch seals to
//! identical bytes; different plaintexts get different nonces, which is what
//! AES-GCM requires. The hash is *keyed*, so someone without the key cannot
//! confirm a guess at the plaintext by recomputing the nonce.
//!
//! Two costs come with that, and both are deliberate:
//!
//! - Identical plaintext under one epoch is visibly identical to a store that
//!   holds both. Within a group whose members can decrypt both anyway, this is
//!   narrow — but it is real, and it is the price of convergence.
//! - A 96-bit derived nonce collides at the birthday bound. A collision
//!   between *different* plaintexts under one key needs on the order of 2^48
//!   sealed blobs in a single epoch.

use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use dialog_common::Blake3Hash;

use crate::{EpochId, KeyringError};

/// Domain separator for the derived nonce.
const NONCE_DOMAIN: &[u8] = b"dialog/keyring/nonce/v1";

/// The only header version this build writes or reads.
const VERSION: u8 = 1;

/// Length of the version field.
const VERSION_LEN: usize = 1;
/// Length of the epoch name.
const EPOCH_LEN: usize = 32;
/// Length of the AES-GCM nonce.
const NONCE_LEN: usize = 12;
/// Length of the AES-GCM authentication tag.
const TAG_LEN: usize = 16;
/// Smallest possible encoding: a header plus an empty authenticated payload.
const MIN_LEN: usize = VERSION_LEN + EPOCH_LEN + NONCE_LEN + TAG_LEN;

/// Content sealed under one epoch's key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sealed {
    /// The epoch whose key opens this blob.
    epoch: EpochId,
    /// The nonce, derived from the plaintext.
    nonce: [u8; NONCE_LEN],
    /// Ciphertext with its authentication tag appended.
    ciphertext: Vec<u8>,
}

impl Sealed {
    /// Seal `plain` under `key`, labelled with `epoch`.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Crypto`] if the platform's cipher fails.
    pub async fn seal(key: &[u8; 32], epoch: &EpochId, plain: &[u8]) -> Result<Self, KeyringError> {
        let nonce = derive_nonce(key, epoch, plain);
        let ciphertext =
            dialog_credentials::symmetric::encrypt(key, &nonce, plain, &aad(epoch)).await?;
        Ok(Self {
            epoch: epoch.clone(),
            nonce,
            ciphertext,
        })
    }

    /// Seal `plain` without awaiting.
    ///
    /// The tree persists nodes in a synchronous call, so the write path needs
    /// this rather than [`seal`](Self::seal). Same algorithm, same derived
    /// nonce, same additional data — the bytes are identical, which
    /// `sync_and_async_sealing_agree` pins.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Crypto`] if the cipher fails.
    pub fn seal_now(key: &[u8; 32], epoch: &EpochId, plain: &[u8]) -> Result<Self, KeyringError> {
        let nonce = derive_nonce(key, epoch, plain);
        let ciphertext = Aes256Gcm::new(key.into())
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: plain,
                    aad: &aad(epoch),
                },
            )
            .map_err(|e| KeyringError::Crypto(e.to_string()))?;
        Ok(Self {
            epoch: epoch.clone(),
            nonce,
            ciphertext,
        })
    }

    /// Open the blob without awaiting.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Failed`] if the key is wrong or the blob has
    /// been tampered with.
    pub fn open_now(&self, key: &[u8; 32]) -> Result<Vec<u8>, KeyringError> {
        Aes256Gcm::new(key.into())
            .decrypt(
                Nonce::from_slice(&self.nonce),
                Payload {
                    msg: &self.ciphertext,
                    aad: &aad(&self.epoch),
                },
            )
            .map_err(|_| KeyringError::Failed)
    }

    /// Open the blob with `key`.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Failed`] if the key is wrong or the blob has
    /// been tampered with. The two are not distinguished.
    pub async fn open(&self, key: &[u8; 32]) -> Result<Vec<u8>, KeyringError> {
        dialog_credentials::symmetric::decrypt(
            key,
            &self.nonce,
            &self.ciphertext,
            &aad(&self.epoch),
        )
        .await
        .map_err(Into::into)
    }

    /// The epoch whose key opens this blob.
    #[must_use]
    pub fn epoch(&self) -> &EpochId {
        &self.epoch
    }

    /// The address this blob is stored under: the hash of its encoding.
    ///
    /// The hash covers the ciphertext, never the plaintext. Addressing by a
    /// plaintext hash would hand anyone who could guess the content a way to
    /// confirm the guess against the store, which is worse than the leak the
    /// derived nonce already accepts.
    #[must_use]
    pub fn address(&self) -> Blake3Hash {
        Blake3Hash::hash(&self.to_bytes())
    }

    /// Encode to the wire format.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(MIN_LEN - TAG_LEN + self.ciphertext.len());
        bytes.push(VERSION);
        bytes.extend_from_slice(self.epoch.as_bytes());
        bytes.extend_from_slice(&self.nonce);
        bytes.extend_from_slice(&self.ciphertext);
        bytes
    }

    /// Decode from the wire format.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Malformed`] if `bytes` is too short, or
    /// [`KeyringError::UnsupportedVersion`] if it names a version this build
    /// does not know.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, KeyringError> {
        if bytes.len() < MIN_LEN {
            return Err(KeyringError::Malformed);
        }
        let (version, rest) = bytes.split_at(VERSION_LEN);
        if version[0] != VERSION {
            return Err(KeyringError::UnsupportedVersion(version[0]));
        }
        let (epoch, rest) = rest.split_at(EPOCH_LEN);
        let (nonce, ciphertext) = rest.split_at(NONCE_LEN);

        let epoch: [u8; EPOCH_LEN] = epoch.try_into().map_err(|_| KeyringError::Malformed)?;
        Ok(Self {
            epoch: EpochId::from(epoch),
            nonce: nonce.try_into().map_err(|_| KeyringError::Malformed)?,
            ciphertext: ciphertext.to_vec(),
        })
    }
}

/// The additional authenticated data: the header a reader acts on before it
/// can verify anything else.
fn aad(epoch: &EpochId) -> Vec<u8> {
    let mut aad = Vec::with_capacity(VERSION_LEN + EPOCH_LEN);
    aad.push(VERSION);
    aad.extend_from_slice(epoch.as_bytes());
    aad
}

/// Derive the nonce from the plaintext, keyed so it cannot be recomputed
/// without the key.
fn derive_nonce(key: &[u8; 32], epoch: &EpochId, plain: &[u8]) -> [u8; NONCE_LEN] {
    let mut hasher = blake3::Hasher::new_keyed(key);
    hasher.update(NONCE_DOMAIN);
    hasher.update(epoch.as_bytes());
    hasher.update(plain);
    let digest = hasher.finalize();
    let mut nonce = [0u8; NONCE_LEN];
    nonce.copy_from_slice(&digest.as_bytes()[..NONCE_LEN]);
    nonce
}
