//! Epochs: named key generations, and the log that records them.
//!
//! An epoch is *named*, not counted. Its identifier is the hash of its own
//! record, so two writers who rotate concurrently mint two distinct epochs
//! without needing to agree on an order — the same way two concurrent updates
//! in BeeKEM leave a conflict node holding both versions rather than one
//! winning.
//!
//! This is what makes rotation work without coordination, and it is not
//! optional: a key derived deterministically from public state would be known
//! to whoever knew the previous key, which is exactly the party rotation
//! exists to lock out. Fresh entropy is required, and fresh entropy cannot be
//! agreed on without communication.

use std::collections::{BTreeMap, BTreeSet};

use dialog_common::Blake3Hash;
use serde::{Deserialize, Serialize};

use crate::KeyringError;

/// Domain separator for epoch identifiers.
const EPOCH_DOMAIN: &[u8] = b"dialog/keyring/epoch/v1";

/// The name of one key generation: the hash of its [`Epoch`] record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EpochId(Blake3Hash);

impl EpochId {
    /// The identifier's raw bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl From<[u8; 32]> for EpochId {
    fn from(bytes: [u8; 32]) -> Self {
        Self(Blake3Hash::from(bytes))
    }
}

impl std::fmt::Display for EpochId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// One entry in the epoch log.
///
/// The record is public — it travels in the plaintext keyring, and every
/// sealed blob names one. Secrecy lives entirely in what a member can *derive*
/// from an epoch, never in the record itself.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Epoch {
    /// The epochs this one supersedes. Empty for the genesis epoch; more than
    /// one when a rotation follows a merge of concurrent rotations.
    predecessors: Vec<EpochId>,
    /// Fresh entropy, sampled when the epoch was minted.
    ///
    /// Public, and load-bearing anyway: it is what makes this epoch's
    /// identifier — and so its derived key — unpredictable from the previous
    /// generation's public record.
    entropy: [u8; 32],
}

impl Epoch {
    /// Mint an epoch superseding `predecessors`, with the given entropy.
    ///
    /// Predecessors are sorted, so the identifier does not depend on the order
    /// a caller happened to collect them in.
    #[must_use]
    pub fn new(predecessors: impl IntoIterator<Item = EpochId>, entropy: [u8; 32]) -> Self {
        let mut predecessors: Vec<_> = predecessors.into_iter().collect();
        predecessors.sort();
        predecessors.dedup();
        Self {
            predecessors,
            entropy,
        }
    }

    /// The epochs this one supersedes.
    #[must_use]
    pub fn predecessors(&self) -> &[EpochId] {
        &self.predecessors
    }

    /// This epoch's name, derived from its content.
    #[must_use]
    pub fn id(&self) -> EpochId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(EPOCH_DOMAIN);
        hasher.update(&(self.predecessors.len() as u32).to_be_bytes());
        for predecessor in &self.predecessors {
            hasher.update(predecessor.as_bytes());
        }
        hasher.update(&self.entropy);
        EpochId(Blake3Hash::from(*hasher.finalize().as_bytes()))
    }
}

/// The append-only record of every epoch a keyring knows.
///
/// This is the stand-in for the CGKA operation log, and it is the same shape:
/// signed records naming their predecessors, replicated in the clear, replayed
/// to resolve a key. What it lacks is the tree — every member here derives
/// keys from one shared secret rather than from a path of their own.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EpochLog {
    /// Every epoch seen, keyed by name.
    epochs: BTreeMap<EpochId, Epoch>,
}

impl EpochLog {
    /// An empty log.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an epoch, returning its name. Recording twice is a no-op.
    pub fn insert(&mut self, epoch: Epoch) -> EpochId {
        let id = epoch.id();
        self.epochs.insert(id.clone(), epoch);
        id
    }

    /// Whether this log holds the named epoch.
    #[must_use]
    pub fn contains(&self, id: &EpochId) -> bool {
        self.epochs.contains_key(id)
    }

    /// The named epoch's record.
    #[must_use]
    pub fn get(&self, id: &EpochId) -> Option<&Epoch> {
        self.epochs.get(id)
    }

    /// How many epochs this log holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.epochs.len()
    }

    /// Whether the log is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.epochs.is_empty()
    }

    /// The epochs nothing supersedes.
    ///
    /// One head is the settled case. More than one means rotations happened
    /// concurrently and both survive — every blob under either stays readable,
    /// and the next rotation names both as predecessors, collapsing them.
    #[must_use]
    pub fn heads(&self) -> BTreeSet<EpochId> {
        let superseded: BTreeSet<_> = self
            .epochs
            .values()
            .flat_map(|epoch| epoch.predecessors.iter().cloned())
            .collect();
        self.epochs
            .keys()
            .filter(|id| !superseded.contains(id))
            .cloned()
            .collect()
    }

    /// Take in everything another replica knows.
    ///
    /// A union, because the log is append-only and every record is
    /// self-naming: there is nothing to reconcile.
    pub fn merge(&mut self, other: &Self) {
        for (id, epoch) in &other.epochs {
            self.epochs.insert(id.clone(), epoch.clone());
        }
    }

    /// The epoch a replica should write under, given what it knows.
    ///
    /// With one head this is that head. With several — a merge of concurrent
    /// rotations — it is the lowest by name, so every replica that has seen
    /// the same records picks the same one and their writes converge again
    /// without anybody rotating.
    ///
    /// # Errors
    ///
    /// Returns [`KeyringError::Malformed`] if the log is empty.
    pub fn settled_head(&self) -> Result<EpochId, KeyringError> {
        self.heads()
            .into_iter()
            .next()
            .ok_or(KeyringError::Malformed)
    }
}
