//! Layered envelopes held in memory, and the tree reading through them.

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{Delta, DialogSearchTreeError, Key, LoadBlock, Value};
use dialog_storage::DialogStorageError;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use super::envelope::Envelope;
use super::keys::{Access, StructureKey, Writer};
use super::party::Party;
use crate::KeyringError;

/// Where a layered tree starts: its root envelope's address, and the
/// structure key that opens it. Everything else is reached from these, so
/// they are what a party is handed to read a tree at any level.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayeredRoot {
    /// The root envelope's address.
    pub address: Blake3Hash,
    /// The root's structure key.
    pub structure: StructureKey,
}

/// Envelopes by address, shared by every party pointed at one store.
type Envelopes = Arc<RwLock<HashMap<Blake3Hash, Vec<u8>>>>;

/// Layered envelopes held in memory, read by one party at one level of
/// [`Access`].
///
/// Clones share the envelopes and what this party has learned;
/// [`as_party`](Self::as_party) points another party, with its own access,
/// at the same envelopes. A party with content access reads the tree through
/// this as a [`LoadBlock`] provider: [`open_root`](Self::open_root) gives the
/// root's identity, and each node it opens tells it where its children live.
pub struct LayeredBlocks<K, V> {
    /// Envelopes by address.
    envelopes: Envelopes,
    /// What this party can open, and where each node it reached lives.
    party: Party,
    /// The tree's key and value types, which reading a node's links needs.
    types: PhantomData<fn() -> (K, V)>,
}

impl<K, V> Clone for LayeredBlocks<K, V> {
    fn clone(&self) -> Self {
        Self {
            envelopes: self.envelopes.clone(),
            party: self.party.clone(),
            types: PhantomData,
        }
    }
}

impl<K, V> std::fmt::Debug for LayeredBlocks<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredBlocks")
            .field("envelopes", &self.len())
            .finish_non_exhaustive()
    }
}

impl<K, V> LayeredBlocks<K, V> {
    /// An empty store, read by a party with `access`.
    #[must_use]
    pub fn new(access: Access) -> Self {
        Self {
            envelopes: Arc::default(),
            party: Party::new(access),
            types: PhantomData,
        }
    }

    /// The same envelopes, read by another party with `access`. It knows
    /// nothing yet: it reaches nodes from a root, like anyone else.
    #[must_use]
    pub fn as_party(&self, access: Access) -> Self {
        Self {
            envelopes: self.envelopes.clone(),
            party: Party::new(access),
            types: PhantomData,
        }
    }

    /// Everything the store holds: addresses and envelope bytes.
    #[must_use]
    pub fn stored(&self) -> Vec<(Blake3Hash, Vec<u8>)> {
        self.envelopes()
            .iter()
            .map(|(address, bytes)| (address.clone(), bytes.clone()))
            .collect()
    }

    /// The bytes stored under `address`, if any.
    #[must_use]
    pub fn raw(&self, address: &Blake3Hash) -> Option<Vec<u8>> {
        self.envelopes().get(address).cloned()
    }

    /// Keep `bytes` under `address` as given, without checking them: what a
    /// store receiving blocks from elsewhere, or a faulty one, would hold.
    pub fn put_raw(&self, address: Blake3Hash, bytes: Vec<u8>) {
        self.envelopes_mut().insert(address, bytes);
    }

    /// How many envelopes are stored.
    #[must_use]
    pub fn len(&self) -> usize {
        self.envelopes().len()
    }

    /// Whether no envelope is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The envelope stored under `address`, checked against it.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownNode`] if nothing is stored there,
    /// [`KeyringError::Corrupt`] if the bytes do not hash to `address` —
    /// checkable with no key at all — and [`KeyringError::Malformed`] if they
    /// do not decode.
    pub fn fetch(&self, address: &Blake3Hash) -> Result<Envelope, KeyringError> {
        let bytes = self
            .raw(address)
            .ok_or_else(|| KeyringError::UnknownNode(address.clone()))?;
        if &Blake3Hash::hash(&bytes) != address {
            return Err(KeyringError::Corrupt(address.clone()));
        }
        Envelope::from_bytes(&bytes)
    }

    /// Every envelope reachable from `root`, each checked against its
    /// address, in the order a depth-first walk reaches them. Needs only the
    /// root's structure key: this is what a replicator can do.
    ///
    /// # Errors
    ///
    /// Returns what [`fetch`](Self::fetch) returns for any node, and
    /// [`KeyringError::Failed`] if a structure region does not open.
    pub fn walk(&self, root: &LayeredRoot) -> Result<Vec<Blake3Hash>, KeyringError> {
        let mut seen = HashSet::new();
        let mut order = Vec::new();
        let mut pending = vec![(root.address.clone(), root.structure)];
        while let Some((address, key)) = pending.pop() {
            if !seen.insert(address.clone()) {
                continue;
            }
            let envelope = self.fetch(&address)?;
            let children = envelope.children(&key)?;
            order.push(address);
            pending.extend(children.into_iter().rev());
        }
        Ok(order)
    }

    /// Copy every envelope reachable from `root` out of `source` into this
    /// store, refusing any block that does not match its address. Needs only
    /// structure access: a replicator moves a tree it cannot read.
    ///
    /// Returns how many envelopes were copied.
    ///
    /// # Errors
    ///
    /// Returns what [`walk`](Self::walk) returns over `source`.
    pub fn replicate_from(
        &self,
        source: &LayeredBlocks<K, V>,
        root: &LayeredRoot,
    ) -> Result<usize, KeyringError> {
        let addresses = source.walk(root)?;
        let mut envelopes = self.envelopes_mut();
        for address in &addresses {
            let bytes = source
                .raw(address)
                .ok_or_else(|| KeyringError::UnknownNode(address.clone()))?;
            envelopes.insert(address.clone(), bytes);
        }
        Ok(addresses.len())
    }

    /// The address of the leaf that would hold `key`, found from the
    /// separators alone. Needs range access, and opens no node's content.
    ///
    /// At each index, the child taken is the last whose separator is at or
    /// below `key`, as the tree itself routes.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::MissingGeneration`] without the range
    /// generation a node was sealed under, and what [`fetch`](Self::fetch)
    /// returns for any node on the way.
    pub fn route(&self, root: &LayeredRoot, key: &[u8]) -> Result<Blake3Hash, KeyringError> {
        let (mut address, mut structure) = (root.address.clone(), root.structure);
        loop {
            let envelope = self.fetch(&address)?;
            let children = envelope.children(&structure)?;
            if children.is_empty() {
                return Ok(address);
            }
            let separators = envelope.separators(self.party.access(), &structure)?;
            let at = separators
                .iter()
                .rposition(|separator| separator.as_slice() <= key)
                .unwrap_or(0);
            let (child, child_key) = children
                .into_iter()
                .nth(at)
                .ok_or_else(|| KeyringError::Node("a separator without a child".into()))?;
            address = child;
            structure = child_key;
        }
    }

    fn envelopes(&self) -> RwLockReadGuard<'_, HashMap<Blake3Hash, Vec<u8>>> {
        self.envelopes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn envelopes_mut(&self) -> RwLockWriteGuard<'_, HashMap<Blake3Hash, Vec<u8>>> {
        self.envelopes
            .write()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl<K, V> LayeredBlocks<K, V>
where
    K: Key,
    V: Value,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Seal what a persist staged in `delta` under `writer`'s generations,
    /// emptying it once the envelopes are kept, and return where the tree
    /// rooted at `root` now starts. A refused write keeps nothing and leaves
    /// `delta` as it was.
    ///
    /// Sealed bottom-up from `root`: a parent records its children's
    /// addresses, so they are sealed first. A child that was not staged must
    /// already be known to this party — read through it while editing, or
    /// written by it before — and is linked where it already lives.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownNode`] for a child neither staged nor
    /// known, and [`KeyringError::Node`] if a staged block is not a node.
    pub fn write(
        &self,
        writer: &Writer,
        delta: &mut Delta<Blake3Hash, Buffer>,
        root: &Blake3Hash,
    ) -> Result<LayeredRoot, KeyringError> {
        let sealing = self.party.seal::<K, V>(writer, delta, root)?;
        self.envelopes_mut()
            .extend(sealing.envelopes.iter().cloned());
        delta.flush().for_each(drop);
        Ok(self.party.settle(sealing))
    }

    /// Open the root's content and return the tree's root identity, to read
    /// the tree from (`PersistentTree::from_hash`). Needs content access.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::MissingGeneration`] without both generations
    /// the root was sealed under, and what [`fetch`](Self::fetch) returns.
    pub fn open_root(&self, root: &LayeredRoot) -> Result<Blake3Hash, KeyringError> {
        let envelope = self.fetch(&root.address)?;
        self.party.open_root(root, &envelope)
    }

    /// The node `identity` names, opened, with where its children live
    /// learned from it.
    fn open_node(&self, identity: &Blake3Hash) -> Result<Option<Buffer>, KeyringError> {
        let Some((address, structure)) = self.party.known(identity) else {
            return Ok(None);
        };
        let envelope = self.fetch(&address)?;
        self.party.open::<K, V>(&envelope, &structure).map(Some)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<K, V> Provider<LoadBlock> for LayeredBlocks<K, V>
where
    K: Key,
    V: Value,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        // A node this party has not reached from a root reads as absent: it
        // has no way to know where it lives. `LoadBlock::perform` checks
        // whatever opens against `hash`.
        self.open_node(&hash).map_err(|error| {
            DialogSearchTreeError::Storage(DialogStorageError::Verification(error.to_string()))
        })
    }
}
