//! Repository revision types: a logical clock plus the search-tree
//! root that together name a repository's state at a point in time.
//!
//! The identity half of a revision lives here, with the
//! [`history`](crate::history) types its fields are built from. The half
//! that needs an `Entity`, the repository schema, or the in-tree
//! `RevisionRecord` lives in `dialog-repository` as `RevisionExt`, because
//! those types sit above this crate.

use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use crate::Did;
use crate::history::{Context, Edition, HistoryError, Origin, Version, verify_issuer_signature};

/// The raw 32-byte Blake3 hash a [`TreeReference`] wraps. Kept as a
/// bare array (not a wrapper type) so the wire form is a plain byte
/// array, matching `dialog_storage::Blake3Hash`.
type TreeHash = [u8; 32];

/// A hash representing an empty (usually newly created) search tree.
///
/// Matches the search tree's null root sentinel
/// (`dialog_common::NULL_BLAKE3_HASH`) byte for byte.
pub const EMPTY_TREE_HASH: TreeHash = [0; 32];

/// Reference to a search tree by its root hash.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TreeReference(TreeHash);

impl TreeReference {
    /// Returns a reference to the underlying hash.
    pub fn hash(&self) -> &TreeHash {
        &self.0
    }
}

impl Default for TreeReference {
    /// By default, a [`TreeReference`] points at the empty search tree.
    fn default() -> Self {
        Self(EMPTY_TREE_HASH)
    }
}

impl Display for TreeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        let bytes: &[u8] = self.hash();
        write!(f, "#{}", ToBase58::to_base58(bytes))
    }
}

impl Debug for TreeReference {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self, f)
    }
}

impl From<TreeHash> for TreeReference {
    fn from(hash: TreeHash) -> Self {
        Self(hash)
    }
}

impl From<TreeReference> for TreeHash {
    fn from(value: TreeReference) -> Self {
        let TreeReference(hash) = value;
        hash
    }
}

/// A revision represents a concrete state of the repository at a point in time.
///
/// Causal position is derived from the revision DAG per
/// `notes/version-control.md`: the [`Edition`] is a Lamport timestamp
/// (`max(edition of every revision this one builds on) + 1`), so a higher
/// edition has seen at least as much causal history as any lower one,
/// regardless of which replica produced it — including across repository
/// boundaries. Paired with the [`Origin`] derived from `(issuer, subject)`,
/// it forms a globally unique [`Version`]: two revisions with the same
/// edition but different origins are concurrent by inspection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revision {
    /// DID of the repository this revision belongs to.
    pub subject: Did,

    /// DID of the operator (ephemeral session key) that created this revision.
    pub issuer: Did,

    /// DID of the profile (long-lived key) that authorized this revision.
    pub authority: Did,

    /// Name of the branch this revision was minted on. Part of the
    /// revision's [`Origin`] scope: a branch head is an independent
    /// sequential actor, so two branches advanced by the same issuer must
    /// not share an origin — otherwise they could mint colliding versions.
    #[serde(default)]
    pub branch: String,

    /// Root of the search tree at this revision.
    pub tree: TreeReference,

    /// Parent tree roots this revision is based on. Empty for the first revision.
    pub cause: HashSet<TreeReference>,

    /// Causal depth of this revision: `max(cause editions) + 1`, or zero for
    /// the first revision. Isomorphic to a Lamport timestamp.
    pub edition: Edition,

    /// The causal context (per-origin watermark) of this revision's
    /// ancestry, itself included. Publishing it with the head is what
    /// lets a peer read a replica's knowledge without walking its log:
    /// pull seeds its context memo from it, skips upstreams whose
    /// context is included in ours (nothing new), and adopts an
    /// upstream's tree wholesale when its context includes ours and we
    /// have no local novelty. Covered by the head signature (see
    /// [`Revision::payload`]), so adopting it is as trustworthy as
    /// adopting the head. `None` on heads minted before the field
    /// existed; readers fall back to deriving the context by the
    /// ancestry walk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// The issuer's Ed25519 signature over [`Revision::payload`], with the
    /// key the issuer DID names (`did:key`). This is what binds the tree
    /// root to the issuer: the in-tree [`RevisionRecord`] signs everything
    /// else a revision states about itself, but cannot contain the root of
    /// the tree it lives in. Empty until the revision is published (the
    /// root is only final at publish time) — see [`Revision::verify`].
    #[serde(default, with = "serde_bytes")]
    pub signature: Vec<u8>,
}

impl Revision {
    /// Build the first revision of a branch, with no causal ancestor and
    /// the genesis edition.
    pub fn new(
        tree: TreeReference,
        subject: Did,
        branch: impl Into<String>,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject,
            issuer,
            authority,
            branch: branch.into(),
            tree,
            cause: HashSet::new(),
            edition: Edition::GENESIS,
            context: None,
            signature: Vec::new(),
        }
    }

    /// Build the revision that follows `self`, advancing the edition.
    ///
    /// Whoever advances a revision has, by construction, seen it, so the new
    /// edition is `self + 1` no matter which issuer commits. The previous
    /// revision's tree root is recorded in `cause`.
    ///
    /// The subject is the minting branch's repository, passed explicitly
    /// rather than inherited from `self`: a branch may have adopted a head
    /// minted in a *different* repository (a fast-forward pull from a
    /// foreign subject), and commits on top of it belong to this branch's
    /// lineage scope, not the foreign one — otherwise the minted version
    /// would disagree with the version its own data was tagged with.
    pub fn advance(
        &self,
        tree: TreeReference,
        subject: Did,
        branch: impl Into<String>,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject,
            issuer,
            authority,
            branch: branch.into(),
            tree,
            cause: HashSet::from([self.tree.clone()]),
            edition: self.edition.successor(),
            context: None,
            signature: Vec::new(),
        }
    }

    /// Build a merge revision combining `self` with `upstream`.
    ///
    /// The merge has seen both lineages, so its edition advances past both:
    /// `max(self, upstream) + 1`. The upstream tree is recorded as the
    /// causal ancestor; the branch's own prior tree is dropped from `cause`
    /// because it is now subsumed by the merged tree.
    ///
    /// As with [`Revision::advance`], the subject is the minting branch's
    /// repository, passed explicitly: either side of the merge may carry a
    /// foreign subject adopted through an earlier pull.
    pub fn merge(
        &self,
        upstream: &Revision,
        tree: TreeReference,
        subject: Did,
        branch: impl Into<String>,
        issuer: Did,
        authority: Did,
    ) -> Self {
        Self {
            subject,
            issuer,
            authority,
            branch: branch.into(),
            tree,
            cause: HashSet::from([upstream.tree.clone()]),
            edition: self.edition.max(upstream.edition).successor(),
            context: None,
            signature: Vec::new(),
        }
    }

    /// The [`Version`] identifying this revision: its origin paired with its
    /// edition. Versions sort by causal depth, and two versions with the
    /// same edition but different origins identify concurrent revisions.
    /// The [`Version`] identifying this revision under `origin`: the origin
    /// paired with this revision's edition. Versions sort by causal depth,
    /// and two versions with the same edition but different origins identify
    /// concurrent revisions.
    ///
    /// The origin is passed in rather than derived here: deriving it needs
    /// the repository schema's branch entity, which lives above this crate
    /// (see `RevisionExt::origin` in `dialog-repository`).
    pub fn version_with(&self, origin: Origin) -> Version {
        Version::new(origin, self.edition)
    }

    /// The canonical signing payload of this revision: every field except
    /// the signature, deterministically encoded. Variable-width fields are
    /// length-prefixed to keep the encoding injective; the unordered
    /// `cause` set is sorted.
    ///
    /// ```text
    /// (length (8, big-endian) ++ UTF-8) for subject, issuer, authority, branch
    /// tree (32)
    /// cause count (8, big-endian) ++ roots (32 each, sorted)
    /// edition (8, big-endian)
    /// context, when present:
    ///     0x01 ++ entry count (8, big-endian)
    ///          ++ entries (origin (32) ++ edition (8, big-endian), sorted)
    /// ```
    ///
    /// A head without a context appends nothing after the edition (the
    /// pre-context payload shape), and a head with one appends the `0x01`
    /// marker plus the sorted watermark entries. The two shapes differ in
    /// length for any fixed prefix, so the encoding stays injective: a
    /// signature over one can never validate the other.
    pub fn payload(&self) -> Vec<u8> {
        let mut cause: Vec<&TreeReference> = self.cause.iter().collect();
        cause.sort_by(|left, right| left.hash().cmp(right.hash()));

        let mut bytes = Vec::new();
        for field in [
            self.subject.as_str(),
            self.issuer.as_str(),
            self.authority.as_str(),
            self.branch.as_str(),
        ] {
            bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes.extend_from_slice(self.tree.hash());
        bytes.extend_from_slice(&(cause.len() as u64).to_be_bytes());
        for tree in cause {
            bytes.extend_from_slice(tree.hash());
        }
        bytes.extend_from_slice(&self.edition.key_bytes());
        if let Some(context) = &self.context {
            bytes.push(0x01);
            bytes.extend_from_slice(&(context.len() as u64).to_be_bytes());
            for (origin, edition) in context.iter() {
                bytes.extend_from_slice(&origin.0);
                bytes.extend_from_slice(&edition.key_bytes());
            }
        }
        bytes
    }

    /// Verify that the signature is the issuer's Ed25519 signature over
    /// [`Revision::payload`], resolving the key from the issuer's
    /// `did:key`. This is the check a replica runs before adopting a head
    /// it did not mint (e.g. on pull): a forged or tampered head — wrong
    /// tree root, reattributed issuer, adjusted edition — fails here.
    pub fn verify(&self) -> Result<(), HistoryError> {
        verify_issuer_signature(self.issuer.as_str(), &self.payload(), &self.signature)
    }
}
