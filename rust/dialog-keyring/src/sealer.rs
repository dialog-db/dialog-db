//! Sealing nodes on the tree's write path.
//!
//! The keyring's own operations are async, because resolving an epoch may go
//! through the platform's crypto. Sealing a node cannot be: the search tree
//! persists nodes in a synchronous call. [`NodeSealer`] bridges the two by
//! resolving every epoch once, up front, and then sealing without awaiting.
//!
//! The consequence is worth stating plainly, because it constrains the design
//! rather than following from it: **content sealing cannot use `WebCrypto`**.
//! A software cipher is the only kind that can run inside a synchronous
//! persist. The bytes are identical either way — same algorithm, same derived
//! nonce — so a blob sealed by one path opens under the other.

use std::collections::BTreeMap;

use dialog_common::Blake3Hash;
use dialog_search_tree::NodeCipher;
use dialog_storage::DialogStorageError;

use crate::{EpochId, Keyring, KeyringError, Sealed};

/// Domain separator for blinded storage addresses.
const ADDRESS_DOMAIN: &[u8] = b"dialog/keyring/address/v1";

/// A keyring resolved to concrete keys, so nodes can be sealed synchronously.
///
/// Built once per session with [`resolve`](Self::resolve). Rotating the
/// keyring afterwards means resolving again — the sealer is a snapshot, and
/// deliberately so: the write path should not be able to change which epoch it
/// is writing under halfway through a commit.
#[derive(Clone)]
pub struct NodeSealer {
    /// The epoch new nodes are sealed under.
    current: EpochId,
    /// Every epoch this sealer can open.
    keys: BTreeMap<EpochId, [u8; 32]>,
    /// The stable key that blinds storage addresses.
    blinding: [u8; 32],
}

impl std::fmt::Debug for NodeSealer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the key material.
        f.debug_struct("NodeSealer")
            .field("current", &self.current)
            .field("epochs", &self.keys.len())
            .finish()
    }
}

impl NodeSealer {
    /// Resolve every epoch a keyring knows into a sealer.
    ///
    /// # Errors
    ///
    /// Returns whatever the keyring's own resolution returns.
    pub async fn resolve<K: Keyring>(keyring: &K) -> Result<Self, KeyringError> {
        let mut keys = BTreeMap::new();
        for epoch in keyring.epochs() {
            let key = keyring.key(&epoch).await?;
            keys.insert(epoch, key);
        }
        Ok(Self {
            current: keyring.current(),
            keys,
            blinding: keyring.blinding_key(),
        })
    }

    /// The epoch new nodes are sealed under.
    #[must_use]
    pub fn current(&self) -> &EpochId {
        &self.current
    }

    /// How many epochs this sealer can open.
    #[must_use]
    pub fn epochs(&self) -> usize {
        self.keys.len()
    }
}

impl NodeCipher for NodeSealer {
    fn address(&self, identity: &Blake3Hash) -> Blake3Hash {
        // Blinded with a key that never rotates. It has to be stable: a link
        // records a node's identity, and if rotating moved where that node
        // lived, every link written before the rotation would dangle.
        //
        // A key that never rotates is a weaker thing to hold than a key that
        // decrypts. Someone who kept it after being removed can confirm
        // guesses about nodes they could already read, and learn that a node
        // exists. They cannot read anything written since.
        let mut hasher = blake3::Hasher::new_keyed(&self.blinding);
        hasher.update(ADDRESS_DOMAIN);
        hasher.update(identity.as_bytes());
        Blake3Hash::from(*hasher.finalize().as_bytes())
    }

    fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, DialogStorageError> {
        let key = self
            .keys
            .get(&self.current)
            .ok_or_else(|| storage_error(&KeyringError::UnknownEpoch(self.current.clone())))?;
        Sealed::seal_now(key, &self.current, plain)
            .map(|sealed| sealed.to_bytes())
            .map_err(|error| storage_error(&error))
    }

    fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, DialogStorageError> {
        let sealed = Sealed::from_bytes(sealed).map_err(|error| storage_error(&error))?;
        let key = self
            .keys
            .get(sealed.epoch())
            .ok_or_else(|| storage_error(&KeyringError::UnknownEpoch(sealed.epoch().clone())))?;
        sealed.open_now(key).map_err(|error| storage_error(&error))
    }
}

/// Carry a keyring failure across the storage trait's error type.
fn storage_error(error: &KeyringError) -> DialogStorageError {
    DialogStorageError::Verification(error.to_string())
}
