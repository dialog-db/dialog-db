//! Core types for the replica system.
//!
//! This module contains fundamental types like Principal, Revision, BranchId,
//! and BranchState that are used throughout the replica system.

use base58::ToBase58;
use dialog_prolly_tree::KeyType;
use dialog_storage::Blake3Hash;
use ed25519_dalek::{SignatureError, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;

use super::error::ReplicaError;
use super::upstream::UpstreamState;

/// Empty tree hash constant from prolly tree.
pub use dialog_prolly_tree::EMPT_TREE_HASH;

/// Cryptographic identifier like Ed25519 public key representing
/// a principal that produced a change.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Principal([u8; 32]);

impl Principal {
    /// Creates a new Principal from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the raw bytes of this principal.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Formats principal as did:key
    pub fn did(&self) -> String {
        const PREFIX: &str = "z6Mk";
        let id = [PREFIX, self.0.as_ref().to_base58().as_str()].concat();
        format!("did:key:{id}")
    }
}

impl Debug for Principal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.did())
    }
}

impl TryFrom<Principal> for VerifyingKey {
    type Error = SignatureError;
    fn try_from(value: Principal) -> Result<Self, Self::Error> {
        VerifyingKey::from_bytes(&value.0)
    }
}

/// We reference a tree by the root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeReference(Blake3Hash);

impl NodeReference {
    /// Creates a new NodeReference from a hash.
    pub fn new(hash: Blake3Hash) -> Self {
        Self(hash)
    }

    /// Returns the hash of this node reference.
    pub fn hash(&self) -> &Blake3Hash {
        &self.0
    }
}

impl Default for NodeReference {
    /// By default, a [`NodeReference`] is created to empty search tree.
    fn default() -> Self {
        Self(EMPT_TREE_HASH)
    }
}

impl Display for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}

impl Debug for NodeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl From<NodeReference> for Blake3Hash {
    fn from(value: NodeReference) -> Self {
        let NodeReference(hash) = value;
        hash
    }
}

/// Site identifier used to reference remotes.
pub type Site = String;

/// An edition is a content-addressed reference to a value.
/// It's like a hash pointer that can be used to detect changes.
/// The type parameter `T` is phantom - only the hash bytes matter for equality/hashing.
#[derive(Serialize, Deserialize)]
pub struct Edition<T>([u8; 32], PhantomData<fn() -> T>);

impl<T> Edition<T> {
    /// Creates a new Edition from a hash.
    pub fn new(hash: [u8; 32]) -> Self {
        Self(hash, PhantomData)
    }

    /// Returns the hash bytes of this edition.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl<T> Clone for Edition<T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T> Debug for Edition<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "#<{}>{}",
            std::any::type_name::<T>(),
            ToBase58::to_base58(self.0.as_slice())
        )
    }
}

impl<T> Hash for Edition<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl<T> PartialEq for Edition<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<T> Eq for Edition<T> {}

/// Represents when and where something occurred, used for ordering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Occurence {
    /// Site of this occurence.
    pub site: Principal,

    /// Logical coordinated time component denoting a last synchronization
    /// cycle.
    pub period: usize,

    /// Local uncoordinated time component denoting a moment within a
    /// period at which occurrence happened.
    pub moment: usize,
}

/// A [`Revision`] represents a concrete state of the dialog instance.
/// It is similar to a git commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    /// Site where this revision was created. Expected to be a signing
    /// principal representing a tool acting on author's behalf.
    pub issuer: Principal,

    /// Reference the root of the search tree.
    pub tree: NodeReference,

    /// Set of revisions this is based on. Empty for first revision,
    /// otherwise points to previous revision(s).
    pub cause: HashSet<Edition<Revision>>,

    /// Period indicating when this revision was created.
    pub period: usize,

    /// Moment at which this revision was created (transaction count within period).
    pub moment: usize,
}

impl Revision {
    /// Creates new revision with an empty tree.
    pub fn new(issuer: Principal) -> Self {
        Self {
            issuer,
            tree: NodeReference::default(),
            period: 0,
            moment: 0,
            cause: HashSet::new(),
        }
    }

    /// Issuer of this revision.
    pub fn issuer(&self) -> &Principal {
        &self.issuer
    }

    /// The root of the search index.
    pub fn tree(&self) -> &NodeReference {
        &self.tree
    }

    /// Period when changes were made.
    pub fn period(&self) -> &usize {
        &self.period
    }

    /// Number of transactions made by this issuer since the beginning of this epoch.
    pub fn moment(&self) -> &usize {
        &self.moment
    }

    /// Previous revision(s) this is based on.
    pub fn cause(&self) -> &HashSet<Edition<Revision>> {
        &self.cause
    }

    /// Creates an [`Edition`] of this revision by hashing it.
    pub fn edition(&self) -> Result<Edition<Revision>, ReplicaError> {
        let revision_bytes = serde_ipld_dagcbor::to_vec(self).map_err(|e| {
            ReplicaError::StorageError(format!("Failed to serialize revision: {}", e))
        })?;
        let revision_hash: [u8; 32] = *blake3::hash(&revision_bytes).as_bytes();
        Ok(Edition::new(revision_hash))
    }
}

impl From<Revision> for Occurence {
    fn from(revision: Revision) -> Self {
        Occurence {
            site: revision.issuer,
            period: revision.period,
            moment: revision.moment,
        }
    }
}

/// Unique name for a branch.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchId(String);

impl BranchId {
    /// Creates a new branch identifier from a string.
    pub fn new(id: String) -> Self {
        BranchId(id)
    }

    /// Returns a reference to the branch identifier string.
    pub fn id(&self) -> &String {
        &self.0
    }
}

impl KeyType for BranchId {
    fn bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl TryFrom<Vec<u8>> for BranchId {
    type Error = std::string::FromUtf8Error;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(BranchId(String::from_utf8(bytes)?))
    }
}

impl Display for BranchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Branch state represents a named state of work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchState {
    /// Unique identifier of this branch.
    pub id: BranchId,

    /// Free-form human-readable description of this branch.
    pub description: String,

    /// Current revision associated with this branch.
    pub revision: Revision,

    /// Root of the search tree this revision is based on.
    pub base: NodeReference,

    /// An upstream through which updates get propagated.
    pub upstream: Option<UpstreamState>,
}

impl BranchState {
    /// Create a new branch from the given revision.
    pub fn new(id: BranchId, revision: Revision, description: Option<String>) -> Self {
        Self {
            description: description.unwrap_or_else(|| id.0.clone()),
            base: revision.tree.clone(),
            revision,
            upstream: None,
            id,
        }
    }

    /// Unique identifier of this branch.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Current revision of this branch.
    pub fn revision(&self) -> &Revision {
        &self.revision
    }

    /// Description of this branch.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Upstream branch of this branch.
    pub fn upstream(&self) -> Option<&UpstreamState> {
        self.upstream.as_ref()
    }
}
