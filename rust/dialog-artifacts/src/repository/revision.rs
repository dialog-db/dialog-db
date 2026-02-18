use dialog_capability::Did;
use dialog_prolly_tree::KeyType;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

use base58::ToBase58;

use super::node_reference::NodeReference;
use super::RepositoryError;

/// A [`Revision`] represents a concrete state of the dialog instance. It is
/// kind of like git commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    /// DID of the site where this revision was created. It is expected to be
    /// the DID of a signing principal representing a tool acting on the
    /// author's behalf. In the future we expect to have a signed delegation
    /// chain from user to this site.
    pub issuer: Did,

    /// Reference the root of the search tree.
    pub tree: NodeReference,

    /// Set of revisions this is based of. It can be an empty set if this is
    /// a first revision, but more commonly it will point to a previous revision
    /// it is based on. If branch tracks multiple concurrent upstreams it will
    /// contain a set of revisions.
    ///
    /// It is effectively equivalent of of `parents` in git commit objects.
    pub cause: HashSet<Edition<Revision>>,

    /// Period indicating when this revision was created. This MUST be derived
    /// from the `cause`al revisions and it must be greater by one than the
    /// maximum period of the `cause`al revisions that have different `by` from
    /// this revision. More simply we create a new period whenever we
    /// synchronize.
    pub period: usize,

    /// Moment at which this revision was created. It represents a number of
    /// transactions that have being made in this period. If `cause`al revisions
    /// have a revision from same `by` this MUST be value greater by one,
    /// otherwise it should be `0`. This implies that when we sync we increment
    /// `period` and reset `moment` to `0`. And when we create a transaction we
    /// increment `moment` by one and keep the same `period`.
    pub moment: usize,
}

impl Revision {
    /// Creates new revision with an empty tree
    pub fn new(issuer: Did) -> Self {
        Self {
            issuer,
            tree: NodeReference::default(),
            period: 0,
            moment: 0,
            cause: HashSet::new(),
        }
    }

    /// DID of the issuer of this revision.
    pub fn issuer(&self) -> &Did {
        &self.issuer
    }

    /// The component of the [`Revision`] that corresponds to the root of the
    /// search index.
    pub fn tree(&self) -> &NodeReference {
        &self.tree
    }

    /// Period when changes have being made
    pub fn period(&self) -> &usize {
        &self.period
    }

    /// Number of transactions made by this issuer since the beginning of
    /// this epoch
    pub fn moment(&self) -> &usize {
        &self.moment
    }

    /// Previous revision this replaced.
    pub fn cause(&self) -> &HashSet<Edition<Revision>> {
        &self.cause
    }

    /// Creates an [`Edition`] of this revision by hashing it.
    ///
    /// This is used to reference this revision as a causal ancestor in subsequent revisions.
    pub fn edition(&self) -> Result<Edition<Revision>, RepositoryError> {
        let revision_bytes = serde_ipld_dagcbor::to_vec(self).map_err(|e| {
            RepositoryError::StorageError(format!("Failed to serialize revision: {}", e))
        })?;
        let revision_hash: [u8; 32] = *blake3::hash(&revision_bytes).as_bytes();
        Ok(Edition::new(revision_hash))
    }
}

/// Blake3 hash of the branch state.
#[derive(Serialize, Deserialize)]
pub struct Edition<T>([u8; 32], PhantomData<fn() -> T>);

impl<T> Edition<T> {
    /// Creates a new edition from a hash.
    pub fn new(hash: [u8; 32]) -> Self {
        Self(hash, PhantomData)
    }
}

impl<T> Clone for Edition<T> {
    fn clone(&self) -> Self {
        Self(self.0, PhantomData)
    }
}

impl<T> Debug for Edition<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!(
            "#<{}>{}",
            std::any::type_name::<T>(),
            self.0.to_base58().as_str()
        ))
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

impl<T> PartialOrd for Edition<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Edition<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.cmp(&other.0)
    }
}

impl<T> KeyType for Edition<T> {
    fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl<T> TryFrom<Vec<u8>> for Edition<T> {
    type Error = crate::DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self(
            value.try_into().map_err(|value: Vec<u8>| {
                crate::DialogArtifactsError::InvalidReference(format!(
                    "Incorrect length (expected {}, got {})",
                    32,
                    value.len()
                ))
            })?,
            PhantomData,
        ))
    }
}
