//! Sealing the tree's blocks for an untrusted store.
//!
//! The keyring's own operations are async, because resolving an epoch may go
//! through the platform's crypto. [`NodeSealer`] resolves every epoch once, up
//! front, and then seals and opens without awaiting, with a software cipher
//! on both targets. The bytes are identical to the platform path's — same
//! algorithm, same derived nonce — so a blob sealed by one opens under the
//! other.
//!
//! [`SealedBlocks`] is where it meets the tree. The tree loads every node
//! through [`LoadBlock`], by the node's identity, and stages what it writes
//! in a [`Delta`]; a sealed store files each staged block as ciphertext under
//! a blinded address, and answers a load by blinding the identity and opening
//! what it finds there. The tree above it is untouched.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{Delta, DialogSearchTreeError, LoadBlock};
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

impl NodeSealer {
    /// The address the block with this content identity is stored under.
    ///
    /// Blinded with a key that never rotates. It has to be stable: a link
    /// records a node's identity, and if rotating moved where that node
    /// lived, every link written before the rotation would dangle.
    ///
    /// A key that never rotates is a weaker thing to hold than a key that
    /// decrypts. Someone who kept it after being removed can confirm guesses
    /// about nodes they could already read, and learn that a node exists.
    /// They cannot read anything written since.
    #[must_use]
    pub fn address(&self, identity: &Blake3Hash) -> Blake3Hash {
        let mut hasher = blake3::Hasher::new_keyed(&self.blinding);
        hasher.update(ADDRESS_DOMAIN);
        hasher.update(identity.as_bytes());
        Blake3Hash::from(*hasher.finalize().as_bytes())
    }

    /// Seal a block's bytes under the current epoch.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError`] if the current epoch's key is not held or
    /// the cipher fails.
    pub fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, KeyringError> {
        let key = self
            .keys
            .get(&self.current)
            .ok_or_else(|| KeyringError::UnknownEpoch(self.current.clone()))?;
        Sealed::seal_now(key, &self.current, plain).map(|sealed| sealed.to_bytes())
    }

    /// Open what [`seal`](Self::seal) produced, under whichever epoch it
    /// names.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError`] if the bytes do not open: a wrong key, an
    /// epoch this sealer cannot resolve, or tampering.
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, KeyringError> {
        let sealed = Sealed::from_bytes(sealed)?;
        let key = self
            .keys
            .get(sealed.epoch())
            .ok_or_else(|| KeyringError::UnknownEpoch(sealed.epoch().clone()))?;
        sealed.open_now(key)
    }
}

/// Tree blocks held sealed, each under its blinded address: what an
/// untrusted store would hold, and a [`LoadBlock`] provider reading through
/// it.
///
/// The in-memory counterpart of [`MemoryBlocks`](dialog_search_tree::MemoryBlocks)
/// for a sealed space. Clones share the same blocks, and
/// [`reading_with`](Self::reading_with) reads them through another sealer,
/// standing in for a second party pointed at the same store.
#[derive(Clone)]
pub struct SealedBlocks {
    /// The keys this store seals and opens with.
    sealer: Arc<NodeSealer>,
    /// Ciphertext by blinded address.
    stored: Arc<RwLock<HashMap<Blake3Hash, Vec<u8>>>>,
}

impl std::fmt::Debug for SealedBlocks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SealedBlocks")
            .field("sealer", &self.sealer)
            .field("blocks", &self.len())
            .finish()
    }
}

impl SealedBlocks {
    /// An empty store sealing with `sealer`.
    #[must_use]
    pub fn new(sealer: Arc<NodeSealer>) -> Self {
        Self {
            sealer,
            stored: Arc::default(),
        }
    }

    /// The same stored blocks, read and written through `sealer`.
    #[must_use]
    pub fn reading_with(&self, sealer: Arc<NodeSealer>) -> Self {
        Self {
            sealer,
            stored: self.stored.clone(),
        }
    }

    /// Seal `block` and keep it under its blinded address.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError`] if sealing fails.
    pub fn store(&self, block: &Buffer) -> Result<(), KeyringError> {
        let address = self.sealer.address(block.blake3_hash());
        let sealed = self.sealer.seal(block.as_ref())?;
        self.write().insert(address, sealed);
        Ok(())
    }

    /// Seal and keep every block `delta` stages, emptying it.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError`] if sealing any block fails.
    pub fn flush(&self, delta: &mut Delta<Blake3Hash, Buffer>) -> Result<(), KeyringError> {
        for (_, block) in delta.flush() {
            self.store(&block)?;
        }
        Ok(())
    }

    /// Everything the store holds: blinded addresses and ciphertext.
    #[must_use]
    pub fn stored(&self) -> Vec<(Blake3Hash, Vec<u8>)> {
        self.read()
            .iter()
            .map(|(address, sealed)| (address.clone(), sealed.clone()))
            .collect()
    }

    /// The ciphertext stored under `address`, if any. Addressed as the store
    /// sees it, not by content identity.
    #[must_use]
    pub fn raw(&self, address: &Blake3Hash) -> Option<Vec<u8>> {
        self.read().get(address).cloned()
    }

    /// How many blocks are stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether no block is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Blake3Hash, Vec<u8>>> {
        self.stored
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Blake3Hash, Vec<u8>>> {
        self.stored
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Provider<LoadBlock> for SealedBlocks {
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        // A blinded address that does not resolve is an absent block: under
        // another space's blinding key, every address misses.
        let Some(sealed) = self.raw(&self.sealer.address(&hash)) else {
            return Ok(None);
        };
        // The caller's `LoadBlock::perform` checks the opened bytes against
        // `hash`, so a block that opens to anything else is refused there.
        let plain = self.sealer.open(&sealed).map_err(|error| {
            DialogSearchTreeError::Storage(DialogStorageError::Verification(error.to_string()))
        })?;
        Ok(Some(Buffer::from(plain)))
    }
}
