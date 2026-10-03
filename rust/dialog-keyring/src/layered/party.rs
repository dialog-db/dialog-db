//! One party's view of a layered tree: what it can open, and where each node
//! it has reached lives.
//!
//! Sealing and opening are the same wherever the envelopes are kept, so they
//! live here. [`LayeredBlocks`](super::LayeredBlocks) keeps envelopes in
//! memory; [`LayeredArchive`](super::LayeredArchive) keeps them in an archive
//! catalog.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{Delta, Key, Value};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use super::envelope::Envelope;
use super::keys::{Access, StructureKey, Writer};
use super::projection::project;
use super::store::LayeredRoot;
use crate::KeyringError;

/// Where each node a party has reached lives: its content identity, mapped
/// to its envelope's address and its structure key.
type Known = Arc<RwLock<HashMap<Blake3Hash, (Blake3Hash, StructureKey)>>>;

/// What a party can open, and where each node it has reached lives.
///
/// Clones share what has been learned.
#[derive(Clone)]
pub(crate) struct Party {
    /// What this party can open.
    access: Access,
    /// Where each node this party has reached lives.
    known: Known,
}

/// A tree sealed but not yet stored: the envelopes to keep, and where each
/// node will live once they are kept.
pub(crate) struct Sealing {
    /// Where the tree starts.
    pub(crate) root: LayeredRoot,
    /// Envelopes by address, children before their parents.
    pub(crate) envelopes: Vec<(Blake3Hash, Vec<u8>)>,
    /// Each newly sealed node: identity, address, structure key.
    learned: Vec<(Blake3Hash, Blake3Hash, StructureKey)>,
}

/// What a sealing in progress has produced so far.
#[derive(Default)]
struct Pending {
    /// Nodes sealed in this pass, so a node staged twice is sealed once.
    sealed: HashMap<Blake3Hash, (Blake3Hash, StructureKey)>,
    /// Envelopes by address, children before their parents.
    envelopes: Vec<(Blake3Hash, Vec<u8>)>,
    /// Each sealed node: identity, address, structure key.
    learned: Vec<(Blake3Hash, Blake3Hash, StructureKey)>,
}

impl Party {
    /// A party that has reached nothing yet.
    pub(crate) fn new(access: Access) -> Self {
        Self {
            access,
            known: Arc::default(),
        }
    }

    /// What this party can open.
    pub(crate) fn access(&self) -> &Access {
        &self.access
    }

    /// Where the node `identity` lives, if this party has reached it.
    pub(crate) fn known(&self, identity: &Blake3Hash) -> Option<(Blake3Hash, StructureKey)> {
        self.known
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(identity)
            .cloned()
    }

    /// Remember that the node `identity` lives at `address`, opened by
    /// `structure`.
    pub(crate) fn learn(&self, identity: Blake3Hash, address: Blake3Hash, structure: StructureKey) {
        self.known
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(identity, (address, structure));
    }

    /// Remember where each node of a sealing lives, once its envelopes are
    /// kept. Learning before they are kept would let a failed write leave
    /// this party pointing at envelopes nobody holds.
    pub(crate) fn settle(&self, sealing: Sealing) -> LayeredRoot {
        for (identity, address, structure) in sealing.learned {
            self.learn(identity, address, structure);
        }
        sealing.root
    }

    /// Open the root's content and return the tree's root identity,
    /// learning where the root lives.
    pub(crate) fn open_root(
        &self,
        root: &LayeredRoot,
        envelope: &Envelope,
    ) -> Result<Blake3Hash, KeyringError> {
        let plain = envelope.content(&self.access, &root.structure)?;
        let identity = Blake3Hash::hash(&plain);
        self.learn(identity.clone(), root.address.clone(), root.structure);
        Ok(identity)
    }
}

impl Party {
    /// Seal the tree rooted at `root` from the blocks a persist staged in
    /// `delta`, under `writer`'s generations. Reads `delta` without emptying
    /// it and learns nothing: the caller keeps the envelopes, then
    /// [`settle`](Self::settle)s.
    ///
    /// Sealed bottom-up from `root`: a parent records its children's
    /// addresses, so they are sealed first. A child that was not staged must
    /// already be known to this party — read through it while editing, or
    /// written by it before — and is linked where it already lives.
    pub(crate) fn seal<K, V>(
        &self,
        writer: &Writer,
        delta: &Delta<Blake3Hash, Buffer>,
        root: &Blake3Hash,
    ) -> Result<Sealing, KeyringError>
    where
        K: Key,
        V: Value,
        V::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        let mut pending = Pending::default();
        let (address, structure) = self.seal_node::<K, V>(writer, delta, root, &mut pending)?;
        Ok(Sealing {
            root: LayeredRoot { address, structure },
            envelopes: pending.envelopes,
            learned: pending.learned,
        })
    }

    fn seal_node<K, V>(
        &self,
        writer: &Writer,
        delta: &Delta<Blake3Hash, Buffer>,
        identity: &Blake3Hash,
        pending: &mut Pending,
    ) -> Result<(Blake3Hash, StructureKey), KeyringError>
    where
        K: Key,
        V: Value,
        V::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        if let Some(known) = pending
            .sealed
            .get(identity)
            .cloned()
            .or_else(|| self.known(identity))
        {
            return Ok(known);
        }
        let block = delta
            .get(identity)
            .ok_or_else(|| KeyringError::UnknownNode(identity.clone()))?;
        let projection = project::<K, V>(&block)?;
        let children = projection
            .children
            .iter()
            .map(|child| self.seal_node::<K, V>(writer, delta, child, pending))
            .collect::<Result<Vec<_>, _>>()?;

        let structure = writer.structure_key(block.as_ref());
        let envelope = Envelope::seal(
            writer,
            &structure,
            &children,
            &projection.separators,
            block.as_ref(),
        )?;
        let address = envelope.address();
        pending
            .envelopes
            .push((address.clone(), envelope.to_bytes()));
        pending
            .learned
            .push((identity.clone(), address.clone(), structure));
        pending
            .sealed
            .insert(identity.clone(), (address.clone(), structure));
        Ok((address, structure))
    }

    /// Open the content of `envelope`, which holds the node reached at
    /// `structure`, and learn where its children live from it.
    pub(crate) fn open<K, V>(
        &self,
        envelope: &Envelope,
        structure: &StructureKey,
    ) -> Result<Buffer, KeyringError>
    where
        K: Key,
        V: Value,
        V::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        let plain = Buffer::from(envelope.content(&self.access, structure)?);
        let projection = project::<K, V>(&plain)?;
        let children = envelope.children(structure)?;
        if children.len() != projection.children.len() {
            return Err(KeyringError::Node(
                "a node's structure and content name different children".into(),
            ));
        }
        for (child, (child_address, child_structure)) in
            projection.children.into_iter().zip(children)
        {
            self.learn(child, child_address, child_structure);
        }
        Ok(plain)
    }
}
