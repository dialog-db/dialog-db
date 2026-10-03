//! One party's view of a layered tree: what it can open, and where each node
//! and value it has reached lives.
//!
//! Sealing and opening are the same wherever the envelopes are kept, so they
//! live here. [`LayeredBlocks`](super::LayeredBlocks) keeps envelopes in
//! memory, [`LayeredArchive`](super::LayeredArchive) in an archive catalog,
//! and [`Space`](super::Space) leaves keeping them to its caller.

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
use super::projection::{Attach, project};
use super::store::LayeredRoot;
use super::value;
use crate::KeyringError;

/// Where each node a party has reached lives: its content identity, mapped
/// to its envelope's address and its structure key.
type Known = Arc<RwLock<HashMap<Blake3Hash, (Blake3Hash, StructureKey)>>>;

/// Where each value a party has reached lives: its plaintext hash, mapped to
/// the sealed value's address.
type Values = Arc<RwLock<HashMap<Blake3Hash, Blake3Hash>>>;

/// What a party can open, and where each node and value it has reached
/// lives.
///
/// Clones share what has been learned.
#[derive(Clone)]
pub(crate) struct Party {
    /// What this party can open.
    access: Access,
    /// Where each node this party has reached lives.
    known: Known,
    /// Where each value this party has reached lives.
    values: Values,
}

/// A tree sealed but not yet stored: the envelopes and sealed values to keep,
/// and where each node and value will live once they are kept.
///
/// Keep every block it holds, then hand it back to be settled; until then the
/// party that sealed it has learned nothing from it.
#[derive(Debug)]
pub struct Sealing {
    /// Where the tree starts.
    root: LayeredRoot,
    /// Envelopes by address, children before their parents.
    envelopes: Vec<(Blake3Hash, Vec<u8>)>,
    /// Sealed values by address.
    values: Vec<(Blake3Hash, Vec<u8>)>,
    /// Each newly sealed node: identity, address, structure key.
    learned: Vec<(Blake3Hash, Blake3Hash, StructureKey)>,
    /// Each newly sealed value: plaintext hash, address.
    learned_values: Vec<(Blake3Hash, Blake3Hash)>,
}

impl Sealing {
    /// Where the tree will start once its blocks are kept.
    #[must_use]
    pub fn root(&self) -> &LayeredRoot {
        &self.root
    }

    /// The new envelopes, children before their parents, each stored under
    /// the hash of its bytes.
    pub fn envelopes(&self) -> impl Iterator<Item = &[u8]> {
        self.envelopes.iter().map(|(_, bytes)| bytes.as_slice())
    }

    /// The new sealed values, each stored under the hash of its bytes.
    pub fn values(&self) -> impl Iterator<Item = &[u8]> {
        self.values.iter().map(|(_, bytes)| bytes.as_slice())
    }

    /// The new envelopes by address.
    pub(crate) fn addressed_envelopes(&self) -> &[(Blake3Hash, Vec<u8>)] {
        &self.envelopes
    }
}

/// What a sealing in progress has produced so far.
#[derive(Default)]
struct Pending {
    /// Nodes sealed in this pass, so a node staged twice is sealed once.
    sealed: HashMap<Blake3Hash, (Blake3Hash, StructureKey)>,
    /// Values sealed in this pass.
    sealed_values: HashMap<Blake3Hash, Blake3Hash>,
    /// Envelopes by address, children before their parents.
    envelopes: Vec<(Blake3Hash, Vec<u8>)>,
    /// Sealed values by address.
    values: Vec<(Blake3Hash, Vec<u8>)>,
    /// Each sealed node: identity, address, structure key.
    learned: Vec<(Blake3Hash, Blake3Hash, StructureKey)>,
}

/// What a sealing reads: the blocks and values a persist staged, by
/// identity and plaintext hash.
pub(crate) struct Staged<'a> {
    /// Staged nodes, by content identity.
    pub(crate) blocks: &'a Delta<Blake3Hash, Buffer>,
    /// Staged values, by plaintext hash. `None` when nothing spills.
    pub(crate) values: Option<&'a Delta<Blake3Hash, Buffer>>,
}

impl Party {
    /// A party that has reached nothing yet.
    pub(crate) fn new(access: Access) -> Self {
        Self {
            access,
            known: Arc::default(),
            values: Arc::default(),
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

    /// Where the value hashing to `reference` lives, if this party has
    /// reached it.
    pub(crate) fn value(&self, reference: &Blake3Hash) -> Option<Blake3Hash> {
        self.values
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(reference)
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

    fn learn_value(&self, reference: Blake3Hash, address: Blake3Hash) {
        self.values
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(reference, address);
    }

    /// Remember where each node and value of a sealing lives, once its
    /// blocks are kept. Learning before they are kept would let a failed
    /// write leave this party pointing at blocks nobody holds.
    pub(crate) fn settle(&self, sealing: Sealing) -> LayeredRoot {
        for (identity, address, structure) in sealing.learned {
            self.learn(identity, address, structure);
        }
        for (reference, address) in sealing.learned_values {
            self.learn_value(reference, address);
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

    /// Open a sealed value hashing to `reference`.
    pub(crate) fn open_value(
        &self,
        reference: &Blake3Hash,
        bytes: &[u8],
    ) -> Result<Vec<u8>, KeyringError> {
        value::open(&self.access, reference, bytes)
    }
}

impl Party {
    /// Seal the tree rooted at `root` from what a persist staged, under
    /// `writer`'s generations. Reads `staged` without emptying it and learns
    /// nothing: the caller keeps the blocks, then [`settle`](Self::settle)s.
    ///
    /// Sealed bottom-up from `root`: a parent records its children's
    /// addresses, so they are sealed first. A child that was not staged must
    /// already be known to this party (read through it while editing, or
    /// written by it before) and is linked where it already lives; so must a
    /// value a node names that was not staged.
    pub(crate) fn seal<K, V, A>(
        &self,
        writer: &Writer,
        staged: &Staged<'_>,
        root: &Blake3Hash,
    ) -> Result<Sealing, KeyringError>
    where
        K: Key,
        V: Value,
        A: Attach<K, V>,
        V::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        let mut pending = Pending::default();
        let (address, structure) = self.seal_node::<K, V, A>(writer, staged, root, &mut pending)?;
        Ok(Sealing {
            root: LayeredRoot { address, structure },
            envelopes: pending.envelopes,
            values: pending.values,
            learned: pending.learned,
            learned_values: pending.sealed_values.into_iter().collect(),
        })
    }

    fn seal_node<K, V, A>(
        &self,
        writer: &Writer,
        staged: &Staged<'_>,
        identity: &Blake3Hash,
        pending: &mut Pending,
    ) -> Result<(Blake3Hash, StructureKey), KeyringError>
    where
        K: Key,
        V: Value,
        A: Attach<K, V>,
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
        let block = staged
            .blocks
            .get(identity)
            .ok_or_else(|| KeyringError::UnknownNode(identity.clone()))?;
        let projection = project::<K, V, A>(&block)?;
        let children = projection
            .children
            .iter()
            .map(|child| self.seal_node::<K, V, A>(writer, staged, child, pending))
            .collect::<Result<Vec<_>, _>>()?;
        let attachments = projection
            .attachments
            .iter()
            .map(|reference| self.seal_value(writer, staged, reference, pending))
            .collect::<Result<Vec<_>, _>>()?;

        let structure = writer.structure_key(block.as_ref());
        let envelope = Envelope::seal(
            writer,
            &structure,
            &children,
            &attachments,
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

    /// Where the value hashing to `reference` lives once sealed: where it
    /// already lives if this party knows, or sealed now from what was
    /// staged.
    fn seal_value(
        &self,
        writer: &Writer,
        staged: &Staged<'_>,
        reference: &Blake3Hash,
        pending: &mut Pending,
    ) -> Result<Blake3Hash, KeyringError> {
        if let Some(address) = pending
            .sealed_values
            .get(reference)
            .cloned()
            .or_else(|| self.value(reference))
        {
            return Ok(address);
        }
        let plain = staged
            .values
            .and_then(|values| values.get(reference))
            .ok_or_else(|| KeyringError::UnknownValue(reference.clone()))?;
        let sealed = value::seal(writer, plain.as_ref())?;
        let address = Blake3Hash::hash(&sealed);
        pending.values.push((address.clone(), sealed));
        pending
            .sealed_values
            .insert(reference.clone(), address.clone());
        Ok(address)
    }

    /// Open the content of `envelope`, which holds the node reached at
    /// `structure`, and learn where its children and values live from it.
    pub(crate) fn open<K, V, A>(
        &self,
        envelope: &Envelope,
        structure: &StructureKey,
    ) -> Result<Buffer, KeyringError>
    where
        K: Key,
        V: Value,
        A: Attach<K, V>,
        V::Archived: for<'a> CheckBytes<
            Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
        >,
    {
        let plain = Buffer::from(envelope.content(&self.access, structure)?);
        let projection = project::<K, V, A>(&plain)?;
        let (children, attachments) = envelope.structure(structure)?;
        if children.len() != projection.children.len()
            || attachments.len() != projection.attachments.len()
        {
            return Err(KeyringError::Node(
                "a node's structure and content name different children or values".into(),
            ));
        }
        for (child, (child_address, child_structure)) in
            projection.children.into_iter().zip(children)
        {
            self.learn(child, child_address, child_structure);
        }
        for (reference, address) in projection.attachments.into_iter().zip(attachments) {
            self.learn_value(reference, address);
        }
        Ok(plain)
    }
}
