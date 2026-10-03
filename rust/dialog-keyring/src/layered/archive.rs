//! Layered envelopes kept in an archive catalog, and the tree reading
//! through them.
//!
//! An envelope's address is the hash of its bytes, which is exactly how the
//! archive addresses a block: `Import` derives each block's digest from its
//! content and `Get` returns the block filed under one. So envelopes go into
//! a catalog as ordinary blocks, and any replica or remote that moves blocks
//! moves them unchanged, checking each against its address with no key.

use std::marker::PhantomData;

use async_trait::async_trait;
use dialog_capability::Provider;
use dialog_common::{Blake3Hash, Buffer, ConditionalSync};
use dialog_effects::archive::prelude::CatalogScope;
use dialog_effects::archive::{Get, Import};
use dialog_search_tree::{Delta, DialogSearchTreeError, Key, LoadBlock, Value};
use dialog_storage::DialogStorageError;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use super::envelope::Envelope;
use super::keys::{Access, Writer};
use super::party::{Party, Staged};
use super::projection::NoAttachments;
use super::store::LayeredRoot;
use crate::KeyringError;

/// Layered envelopes in one archive catalog, read by one party at one level
/// of [`Access`].
///
/// The archive counterpart of [`LayeredBlocks`](super::LayeredBlocks):
/// [`write`](Self::write) seals what a persist staged and imports the
/// envelopes, and a party with content access reads the tree through this as
/// a [`LoadBlock`] provider, from [`open_root`](Self::open_root) down.
///
/// Clones share what this party has learned;
/// [`as_party`](Self::as_party) points another party at the same catalog.
pub struct LayeredArchive<'a, K, V, Env> {
    /// Where the archive's effects are performed.
    env: &'a Env,
    /// The catalog the envelopes are kept in.
    catalog: CatalogScope,
    /// What this party can open, and where each node it reached lives.
    party: Party,
    /// The tree's key and value types, which reading a node's links needs.
    types: PhantomData<fn() -> (K, V)>,
}

impl<K, V, Env> Clone for LayeredArchive<'_, K, V, Env> {
    fn clone(&self) -> Self {
        Self {
            env: self.env,
            catalog: self.catalog.clone(),
            party: self.party.clone(),
            types: PhantomData,
        }
    }
}

impl<K, V, Env> std::fmt::Debug for LayeredArchive<'_, K, V, Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredArchive")
            .field("catalog", &self.catalog)
            .finish_non_exhaustive()
    }
}

impl<'a, K, V, Env> LayeredArchive<'a, K, V, Env> {
    /// The envelopes in `catalog`, read by a party with `access` that has
    /// reached nothing yet.
    #[must_use]
    pub fn new(env: &'a Env, catalog: CatalogScope, access: Access) -> Self {
        Self {
            env,
            catalog,
            party: Party::new(access),
            types: PhantomData,
        }
    }

    /// The same catalog, read by another party with `access`. It knows
    /// nothing yet: it reaches nodes from a root, like anyone else.
    #[must_use]
    pub fn as_party(&self, access: Access) -> Self {
        Self::new(self.env, self.catalog.clone(), access)
    }

    /// The catalog the envelopes are kept in.
    #[must_use]
    pub fn catalog(&self) -> &CatalogScope {
        &self.catalog
    }
}

impl<K, V, Env> LayeredArchive<'_, K, V, Env>
where
    Env: Provider<Get> + ConditionalSync + 'static,
{
    /// The envelope filed under `address`, checked against it.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownNode`] if the catalog holds nothing
    /// there, [`KeyringError::Corrupt`] if the bytes do not hash to
    /// `address` — checkable with no key at all —,
    /// [`KeyringError::Malformed`] if they do not decode, and
    /// [`KeyringError::Archive`] if the archive refuses the read.
    pub async fn fetch(&self, address: &Blake3Hash) -> Result<Envelope, KeyringError> {
        let bytes = self
            .catalog
            .get(address.clone())
            .perform(self.env)
            .await?
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
    pub async fn walk(&self, root: &LayeredRoot) -> Result<Vec<Blake3Hash>, KeyringError> {
        let mut seen = std::collections::HashSet::new();
        let mut order = Vec::new();
        let mut pending = vec![(root.address.clone(), root.structure)];
        while let Some((address, key)) = pending.pop() {
            if !seen.insert(address.clone()) {
                continue;
            }
            let envelope = self.fetch(&address).await?;
            let children = envelope.children(&key)?;
            order.push(address);
            pending.extend(children.into_iter().rev());
        }
        Ok(order)
    }

    /// Open the root's content and return the tree's root identity, to read
    /// the tree from (`PersistentTree::from_hash`). Needs content access.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::MissingGeneration`] without both generations
    /// the root was sealed under, and what [`fetch`](Self::fetch) returns.
    pub async fn open_root(&self, root: &LayeredRoot) -> Result<Blake3Hash, KeyringError> {
        let envelope = self.fetch(&root.address).await?;
        self.party.open_root(root, &envelope)
    }
}

impl<K, V, Env> LayeredArchive<'_, K, V, Env>
where
    K: Key,
    V: Value,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<Import> + ConditionalSync + 'static,
{
    /// Seal what a persist staged in `delta` under `writer`'s generations,
    /// import the envelopes into the catalog, empty `delta`, and return
    /// where the tree rooted at `root` now starts.
    ///
    /// Every envelope is sealed before any is imported, so a write refused
    /// while sealing imports nothing and leaves `delta` as it was. A child
    /// that was not staged must already be known to this party (read through
    /// it while editing, or written by it before) and is linked where it
    /// already lives.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownNode`] for a child neither staged nor
    /// known, [`KeyringError::Node`] if a staged block is not a node, and
    /// [`KeyringError::Archive`] if the archive refuses the import.
    pub async fn write(
        &self,
        writer: &Writer,
        delta: &mut Delta<Blake3Hash, Buffer>,
        root: &Blake3Hash,
    ) -> Result<LayeredRoot, KeyringError> {
        let staged = Staged {
            blocks: delta,
            values: None,
        };
        let sealing = self
            .party
            .seal::<K, V, NoAttachments>(writer, &staged, root)?;
        self.catalog
            .import(
                sealing
                    .envelopes()
                    .map(|bytes| Buffer::from(bytes.to_vec())),
            )
            .perform(self.env)
            .await?;
        delta.flush().for_each(drop);
        Ok(self.party.settle(sealing))
    }
}

impl<K, V, Env> LayeredArchive<'_, K, V, Env>
where
    K: Key,
    V: Value,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<Get> + ConditionalSync + 'static,
{
    /// The node `identity` names, opened, with where its children live
    /// learned from it.
    async fn open_node(&self, identity: &Blake3Hash) -> Result<Option<Buffer>, KeyringError> {
        let Some((address, structure)) = self.party.known(identity) else {
            return Ok(None);
        };
        let envelope = self.fetch(&address).await?;
        self.party
            .open::<K, V, NoAttachments>(&envelope, &structure)
            .map(Some)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<K, V, Env> Provider<LoadBlock> for LayeredArchive<'_, K, V, Env>
where
    K: Key,
    V: Value,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
    Env: Provider<Get> + ConditionalSync + 'static,
{
    async fn execute(
        &self,
        LoadBlock { hash }: LoadBlock,
    ) -> Result<Option<Buffer>, DialogSearchTreeError> {
        // As with the in-memory store: a node this party has not reached from
        // a root reads as absent, and `LoadBlock::perform` checks whatever
        // opens against `hash`.
        self.open_node(&hash).await.map_err(|error| {
            DialogSearchTreeError::Storage(DialogStorageError::Verification(error.to_string()))
        })
    }
}
