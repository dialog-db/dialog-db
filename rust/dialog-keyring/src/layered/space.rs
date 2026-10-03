//! One party's keys for a space's layered trees, for a caller that keeps
//! the blocks itself.

use std::marker::PhantomData;

use dialog_common::{Blake3Hash, Buffer};
use dialog_search_tree::{Delta, Key, Value};
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::Strategy;
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;

use super::envelope::Envelope;
use super::keys::{Access, Writer};
use super::party::{Party, Sealing, Staged};
use super::projection::{Attach, NoAttachments};
use super::store::LayeredRoot;
use crate::KeyringError;

/// The types a [`Space`] is for, held without owning any.
type Types<K, V, A> = PhantomData<fn() -> (K, V, A)>;

/// One party's keys for the layered trees of a space, and where each node
/// and value it has reached lives.
///
/// [`LayeredBlocks`](super::LayeredBlocks) and
/// [`LayeredArchive`](super::LayeredArchive) keep envelopes themselves. A
/// `Space` leaves that to its caller, which fetches and stores blocks however
/// it already does, by address:
///
/// - To write, [`seal`](Self::seal) what a persist staged, keep every block
///   the [`Sealing`] holds, then [`settle`](Self::settle) it.
/// - To read, [`admit`](Self::admit) a tree's root, then for each node the
///   tree asks for, [`locate`](Self::locate) its envelope, fetch it, and
///   [`open`](Self::open) it. Opening a node is what locates its children and
///   values, so a party reaches exactly what descends from roots it was
///   handed.
///
/// Clones share what has been learned.
///
/// `A` names the values a node refers to without holding them, which are
/// sealed beside it.
pub struct Space<K, V, A = NoAttachments> {
    /// The generations this party seals under, if it writes.
    writer: Option<Writer>,
    /// What this party can open, and what it has reached.
    party: Party,
    /// The tree's key and value types, and how its nodes name values.
    types: Types<K, V, A>,
}

impl<K, V, A> Clone for Space<K, V, A> {
    fn clone(&self) -> Self {
        Self {
            writer: self.writer.clone(),
            party: self.party.clone(),
            types: PhantomData,
        }
    }
}

impl<K, V, A> std::fmt::Debug for Space<K, V, A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the key material.
        f.debug_struct("Space")
            .field("writes", &self.writer.is_some())
            .finish_non_exhaustive()
    }
}

impl<K, V, A> Space<K, V, A> {
    /// A party that reads with `access` and cannot seal.
    #[must_use]
    pub fn reader(access: Access) -> Self {
        Self {
            writer: None,
            party: Party::new(access),
            types: PhantomData,
        }
    }

    /// A party that reads with `access` and seals under `writer`'s
    /// generations.
    #[must_use]
    pub fn writer(writer: Writer, access: Access) -> Self {
        Self {
            writer: Some(writer),
            party: Party::new(access),
            types: PhantomData,
        }
    }

    /// Whether this party can seal.
    #[must_use]
    pub fn can_write(&self) -> bool {
        self.writer.is_some()
    }

    /// Learn that the tree whose root has content identity `identity` starts
    /// at `root`. Unchecked here: the root is checked when it is opened,
    /// against `identity` by whoever asked for it.
    pub fn admit(&self, identity: Blake3Hash, root: &LayeredRoot) {
        self.party
            .learn(identity, root.address.clone(), root.structure);
    }

    /// Where the node `identity` lives, if this party has reached it.
    #[must_use]
    pub fn locate(&self, identity: &Blake3Hash) -> Option<LayeredRoot> {
        self.party
            .known(identity)
            .map(|(address, structure)| LayeredRoot { address, structure })
    }

    /// Where the sealed value hashing to `reference` lives, if this party
    /// has reached a node that names it.
    #[must_use]
    pub fn locate_value(&self, reference: &Blake3Hash) -> Option<Blake3Hash> {
        self.party.value(reference)
    }

    /// Open the sealed value hashing to `reference`, from the bytes stored
    /// where [`locate_value`](Self::locate_value) says it lives.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownValue`] if this party has not located
    /// it, [`KeyringError::Corrupt`] if `bytes` are not what is stored there,
    /// [`KeyringError::MissingGeneration`] without its content generation,
    /// and [`KeyringError::Failed`] if it does not open.
    pub fn open_value(&self, reference: &Blake3Hash, bytes: &[u8]) -> Result<Buffer, KeyringError> {
        let address = self
            .locate_value(reference)
            .ok_or_else(|| KeyringError::UnknownValue(reference.clone()))?;
        if Blake3Hash::hash(bytes) != address {
            return Err(KeyringError::Corrupt(address));
        }
        self.party.open_value(reference, bytes).map(Buffer::from)
    }

    /// Remember where each node and value of `sealing` lives, once every
    /// block it holds is kept, and return where its tree starts.
    pub fn settle(&self, sealing: Sealing) -> LayeredRoot {
        self.party.settle(sealing)
    }
}

impl<K, V, A> Space<K, V, A>
where
    K: Key,
    V: Value,
    A: Attach<K, V>,
    V::Archived: for<'a> CheckBytes<
        Strategy<Validator<ArchiveValidator<'a>, SharedValidator>, rkyv::rancor::Error>,
    >,
{
    /// Open the node `identity` from the bytes stored where
    /// [`locate`](Self::locate) says it lives, learning where its children
    /// and values live.
    ///
    /// The plaintext is not checked against `identity` here; the tree's
    /// `LoadBlock` does that for every node it loads.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::UnknownNode`] if this party has not located
    /// it, [`KeyringError::Corrupt`] if `bytes` are not what is stored there,
    /// [`KeyringError::MissingGeneration`] without both generations it was
    /// sealed under, and [`KeyringError::Failed`] or
    /// [`KeyringError::Node`] if it does not open as a node.
    pub fn open(&self, identity: &Blake3Hash, bytes: &[u8]) -> Result<Buffer, KeyringError> {
        let (address, structure) = self
            .party
            .known(identity)
            .ok_or_else(|| KeyringError::UnknownNode(identity.clone()))?;
        if Blake3Hash::hash(bytes) != address {
            return Err(KeyringError::Corrupt(address));
        }
        let envelope = Envelope::from_bytes(bytes)?;
        self.party.open::<K, V, A>(&envelope, &structure)
    }

    /// Seal the tree rooted at `root` from the nodes and values a persist
    /// staged, without emptying either. Nothing is learned until the
    /// returned [`Sealing`] is [`settle`](Self::settle)d.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::ReadOnly`] if this party holds no writer and
    /// the tree is not sealed already,
    /// [`KeyringError::UnknownNode`] for a node neither staged nor located,
    /// [`KeyringError::UnknownValue`] for a value neither staged nor
    /// located, and [`KeyringError::Node`] if a staged block is not a node.
    pub fn seal(
        &self,
        blocks: &Delta<Blake3Hash, Buffer>,
        values: Option<&Delta<Blake3Hash, Buffer>>,
        root: &Blake3Hash,
    ) -> Result<Sealing, KeyringError> {
        // A tree already sealed needs no writer: nothing new is sealed.
        if let Some(sealing) = self.party.already(root) {
            return Ok(sealing);
        }
        let writer = self.writer.as_ref().ok_or(KeyringError::ReadOnly)?;
        self.party
            .seal::<K, V, A>(writer, &Staged { blocks, values }, root)
    }
}
