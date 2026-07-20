//! Repository revision types: a logical clock plus the search-tree
//! root that together name a repository's state at a point in time.
//!
//! The identity half of a revision lives here, with the
//! [`history`](crate::history) types its fields are built from. The half
//! that needs an `Entity`, the repository schema, or the in-tree
//! `RevisionRecord` lives in `dialog-repository` as `RevisionExt`, because
//! those types sit above this crate.

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
    /// The content-derived identifier of the branch this revision was
    /// minted on: the repository schema's branch entity URI, folding the
    /// profile, the repository subject, and the branch name into one
    /// opaque hash. This and the issuer are the head's whole identity —
    /// the origin derives from exactly the two — so the head carries
    /// neither the repository DID nor the profile DID (the branch
    /// identifier already commits to both, and the in-tree revision
    /// record carries the attribution readably).
    ///
    /// Opaque by design, never the branch name: the name is whatever
    /// someone privately called their branch and no peer needs it, so it
    /// does not travel on published heads. (A guessable name is still
    /// enumerable from the hash by anyone holding the public DIDs —
    /// opacity hides casual exposure, not a determined probe.)
    #[serde(default)]
    pub branch: String,

    /// DID of the operator (ephemeral session key) that created this
    /// revision. A branch identifier is shared by every session advancing
    /// the branch, but an origin must identify a single sequential actor,
    /// so the issuer disambiguates — and folding it into the origin is
    /// also what stops a hostile issuer from minting into anyone else's
    /// origin: an origin is never carried, always recomputed from these
    /// signed fields.
    pub issuer: Did,

    /// Root of the search tree at this revision.
    pub tree: TreeReference,

    /// Causal depth of this revision: `max(edition of every revision this
    /// one builds on) + 1`, or zero for the first revision. Isomorphic to
    /// a Lamport timestamp. The DAG edges themselves live in the in-tree
    /// revision record (`RevisionRecord::parents`), not on the head.
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
    pub fn new(tree: TreeReference, branch: impl Into<String>, issuer: Did) -> Self {
        Self {
            branch: branch.into(),
            issuer,
            tree,
            edition: Edition::GENESIS,
            context: None,
            signature: Vec::new(),
        }
    }

    /// Build the revision that follows `self`, advancing the edition.
    ///
    /// Whoever advances a revision has, by construction, seen it, so the new
    /// edition is `self + 1` no matter which issuer commits. The DAG edge to
    /// the previous revision is recorded in the in-tree revision record, not
    /// on the head.
    ///
    /// The branch identifier is the minting branch's, passed explicitly
    /// rather than inherited from `self`: a branch may have adopted a head
    /// minted on a *different* branch or repository (a fast-forward pull),
    /// and commits on top of it belong to this branch's scope, not the
    /// foreign one — otherwise the minted version would disagree with the
    /// version its own data was tagged with.
    pub fn advance(&self, tree: TreeReference, branch: impl Into<String>, issuer: Did) -> Self {
        Self {
            branch: branch.into(),
            issuer,
            tree,
            edition: self.edition.successor(),
            context: None,
            signature: Vec::new(),
        }
    }

    /// Build a merge revision combining `self` with `upstream`.
    ///
    /// The merge has seen both lineages, so its edition advances past both:
    /// `max(self, upstream) + 1`. Both DAG edges are recorded in the
    /// in-tree revision record, not on the head.
    ///
    /// As with [`Revision::advance`], the branch identifier is the minting
    /// branch's, passed explicitly: either side of the merge may carry a
    /// foreign scope adopted through an earlier pull.
    pub fn merge(
        &self,
        upstream: &Revision,
        tree: TreeReference,
        branch: impl Into<String>,
        issuer: Did,
    ) -> Self {
        Self {
            branch: branch.into(),
            issuer,
            tree,
            edition: self.edition.max(upstream.edition).successor(),
            context: None,
            signature: Vec::new(),
        }
    }

    /// The [`Origin`] of this revision: the branch-scoped identity of its
    /// issuer, derived from the head's own two identity fields.
    pub fn origin(&self) -> Origin {
        Origin::derive_from_identifiers([self.branch.as_str(), self.issuer.as_str()])
    }

    /// The [`Version`] identifying this revision: its origin paired with
    /// its edition. Versions sort by causal depth, and two versions with
    /// the same edition but different origins identify concurrent
    /// revisions.
    pub fn version(&self) -> Version {
        Version::new(self.origin(), self.edition)
    }

    /// The canonical signing payload of this revision: every field except
    /// the signature, deterministically encoded. Variable-width fields are
    /// length-prefixed to keep the encoding injective.
    ///
    /// ```text
    /// (length (8, big-endian) ++ UTF-8) for branch, issuer
    /// tree (32)
    /// edition (8, big-endian)
    /// context, when present:
    ///     0x01 ++ entry count (8, big-endian)
    ///          ++ entries (origin (32) ++ edition (8, big-endian)
    ///                      ++ revision count (8, big-endian), sorted)
    /// ```
    ///
    /// A head without a context appends nothing after the edition (the
    /// pre-context payload shape), and a head with one appends the `0x01`
    /// marker plus the sorted watermark entries. The two shapes differ in
    /// length for any fixed prefix, so the encoding stays injective: a
    /// signature over one can never validate the other.
    pub fn payload(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        for field in [self.branch.as_str(), self.issuer.as_str()] {
            bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes.extend_from_slice(self.tree.hash());
        bytes.extend_from_slice(&self.edition.key_bytes());
        if let Some(context) = &self.context {
            bytes.push(0x01);
            bytes.extend_from_slice(&(context.len() as u64).to_be_bytes());
            for (origin, watermark) in context.iter() {
                bytes.extend_from_slice(&origin.0);
                bytes.extend_from_slice(&watermark.edition.key_bytes());
                bytes.extend_from_slice(&watermark.count.to_be_bytes());
            }
        }
        bytes
    }

    /// Verify that the signature is the issuer's Ed25519 signature over
    /// [`Revision::payload`], resolving the key from the issuer's
    /// `did:key`. This is the check a replica runs before adopting a head
    /// it did not mint (e.g. on pull): a forged or tampered head — wrong
    /// tree root, reattributed issuer, adjusted edition — fails here.
    ///
    /// Beyond the signature, the head's editions must sit below
    /// [`EDITION_CEILING`]: a validly *signed* head can still carry a
    /// hostile edition, and accepting one near `u64::MAX` would make the
    /// saturating successor mint the same version twice (breaking the
    /// one-origin-one-sequence rule) while pinning the watermark at a
    /// value that silently drops every future write from that origin.
    pub fn verify(&self) -> Result<(), HistoryError> {
        verify_issuer_signature(self.issuer.as_str(), &self.payload(), &self.signature)?;
        let ceiling = |edition: &Edition, what: &str| {
            if edition.value() >= EDITION_CEILING {
                return Err(HistoryError::InvalidReference(format!(
                    "{what} edition {edition} exceeds the protocol ceiling; \
                     no legitimate chain reaches it"
                )));
            }
            Ok(())
        };
        ceiling(&self.edition, "head")?;
        if let Some(context) = &self.context {
            for (_, watermark) in context.iter() {
                ceiling(&watermark.edition, "watermark")?;
                // A count can never exceed the observed prefix's depth:
                // an origin's chain has strictly increasing editions, so
                // at most `edition + 1` revisions fit below the
                // watermark. A hostile inflated count would misroute
                // every peer's merge direction.
                if watermark.count > watermark.edition.value().saturating_add(1) {
                    return Err(HistoryError::InvalidReference(format!(
                        "watermark count {} exceeds its edition {} — more revisions \
                         than the chain can hold",
                        watermark.count, watermark.edition
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The highest edition a verified head (or any watermark entry it
/// publishes) may carry. Editions grow by one per commit or merge, so no
/// legitimate chain approaches 2^62 sequential operations; anything at or
/// above it is protocol corruption, refused at the trust boundary before
/// the saturating successor arithmetic could ever be reached.
pub const EDITION_CEILING: u64 = 1 << 62;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use base58::ToBase58;
    use ed25519_dalek::Signer as _;

    use super::*;
    use crate::history::{Origin, Version};

    fn key(seed: u8) -> ed25519_dalek::SigningKey {
        ed25519_dalek::SigningKey::from_bytes(&[seed; 32])
    }

    fn did_of(key: &ed25519_dalek::SigningKey) -> Did {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(key.verifying_key().as_bytes());
        format!("did:key:z{}", bytes.to_base58()).parse().unwrap()
    }

    fn signed_head(
        issuer: &ed25519_dalek::SigningKey,
        edit: impl FnOnce(&mut Revision),
    ) -> Revision {
        let did = did_of(issuer);
        let mut revision = Revision::new(TreeReference::from([7u8; 32]), "branch:opaque", did);
        let mut context = Context::new();
        context.record(Version::new(Origin::from([1u8; 32]), Edition::new(4)));
        revision.context = Some(context);
        edit(&mut revision);
        revision.signature = issuer.sign(&revision.payload()).to_bytes().to_vec();
        revision
    }

    /// A signed head with a published watermark verifies, and it survives
    /// the dag-cbor wire byte-for-byte: the context encodes as an ordered
    /// array of (byte-string origin, edition) pairs — a shape the IPLD
    /// spec permits — and the decoded head still carries a valid
    /// signature. This pins the wire format before heads proliferate.
    #[test]
    fn it_roundtrips_a_signed_head_through_dagcbor() {
        let head = signed_head(&key(1), |_| {});
        head.verify().expect("a signed head verifies");

        let bytes = serde_ipld_dagcbor::to_vec(&head).expect("head encodes");
        let decoded: Revision = serde_ipld_dagcbor::from_slice(&bytes).expect("head decodes");
        assert_eq!(decoded, head, "the wire round-trip is lossless");
        decoded
            .verify()
            .expect("the decoded head still carries a valid signature");
    }

    /// A validly SIGNED head whose edition sits at the protocol ceiling is
    /// refused at verification: nothing legitimate reaches 2^62 sequential
    /// operations, and accepting it would let the saturating successor
    /// mint one version twice while pinning the watermark so high that
    /// every future write from the origin is silently dropped as seen.
    #[test]
    fn it_refuses_a_head_edition_at_the_ceiling() {
        let sane = signed_head(&key(1), |head| {
            head.edition = Edition::new(EDITION_CEILING - 1);
        });
        sane.verify()
            .expect("editions below the ceiling verify, however deep");

        for hostile in [EDITION_CEILING, u64::MAX] {
            let head = signed_head(&key(1), |head| {
                head.edition = Edition::new(hostile);
            });
            assert!(
                head.verify().is_err(),
                "edition {hostile} must be refused despite the valid signature"
            );
        }
    }

    /// The ceiling guards the published watermark too: a hostile entry at
    /// `u64::MAX` would pin that origin as fully seen forever on every
    /// replica that merges the context.
    #[test]
    fn it_refuses_a_watermark_entry_at_the_ceiling() {
        let head = signed_head(&key(1), |head| {
            let mut context = Context::new();
            context.record(Version::new(
                Origin::from([2u8; 32]),
                Edition::new(u64::MAX),
            ));
            head.context = Some(context);
        });
        assert!(
            head.verify().is_err(),
            "a hostile watermark entry must be refused despite the valid signature"
        );
    }
}
