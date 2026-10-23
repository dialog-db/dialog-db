//! Repository revision types: a logical clock plus the search-tree
//! root that together name a repository's state at a point in time.
//!
//! The identity half of a revision lives here, with the
//! [`history`](crate::history) types its fields are built from. The half
//! that needs an `Entity`, the repository schema, or the in-tree
//! `RevisionRecord` lives in `dialog-repository` as `RevisionExt`, because
//! those types sit above this crate.

use std::cmp::Ordering;
use std::fmt::{Debug, Display, Formatter, Result as FmtResult};

use base58::ToBase58;
use serde::{Deserialize, Serialize};

use crate::Did;

use super::Entity;
use crate::history::{
    Context, Edition, HistoryError, Origin, Version, Watermark, verify_issuer_signature,
};

/// The raw 32-byte Blake3 hash a [`TreeReference`] wraps. Kept as a
/// bare array (not a wrapper type) so the wire form is a plain byte
/// array, matching `dialog_storage::Blake3Hash`.
type TreeHash = [u8; 32];

/// Reference to a search tree by its root hash.
///
/// Always names a real, persisted tree — the empty tree included, whose
/// root is the zero-entry manifest-carrying node the tree layer derives
/// for its format (see `PersistentTree::empty_root`). "No tree" is not a
/// reference; it is the absence of one (`Option<TreeReference>`, or an
/// absent [`Revision`]).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TreeReference(TreeHash);

impl TreeReference {
    /// Returns a reference to the underlying hash.
    pub fn hash(&self) -> &TreeHash {
        &self.0
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
    pub branch: Entity,

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

    /// The whole causal context (per-origin watermark) of this revision's
    /// ancestry, itself included, on a head minted before the context
    /// moved into the tree. Heads minted since carry none, and say so with
    /// [`region`](Revision::region); a head minted before heads carried a
    /// context carries neither, and readers derive its context by the
    /// ancestry walk. Covered by the head signature (see
    /// [`Revision::payload`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// Whether this head's causal context is in its tree's context region,
    /// which is how every head minted since the region existed carries it.
    /// The region holds every origin's watermark exactly except this
    /// head's own, which [`Revision::context_from_region`] derives from the
    /// head's edition. Covered by the head signature, so a head cannot be
    /// read as one of the other two shapes.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub region: bool,

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
    pub fn new(tree: TreeReference, branch: impl Into<Entity>, issuer: Did) -> Self {
        Self {
            branch: branch.into(),
            issuer,
            tree,
            edition: Edition::GENESIS,
            context: None,
            region: false,
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
    pub fn advance(&self, tree: TreeReference, branch: impl Into<Entity>, issuer: Did) -> Self {
        Self {
            branch: branch.into(),
            issuer,
            tree,
            edition: self.edition.successor(),
            context: None,
            region: false,
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
        branch: impl Into<Entity>,
        issuer: Did,
    ) -> Self {
        Self {
            branch: branch.into(),
            issuer,
            tree,
            edition: self.edition.max(upstream.edition).successor(),
            context: None,
            region: false,
            signature: Vec::new(),
        }
    }

    /// Mark this head as carrying its causal context in its tree's context
    /// region, the shape every head is minted in. The caller writes the
    /// region entries the head needs (see `history::region_writes` in
    /// `dialog-artifacts`).
    pub fn publish_region(&mut self) {
        self.context = None;
        self.region = true;
    }

    /// The head's whole causal context, when the head carries it inline:
    /// a head minted before the context moved into the tree. `None` for a
    /// head whose context is in its tree's region, and for one minted
    /// before heads carried a context.
    pub fn inline_context(&self) -> Option<&Context> {
        match self.region {
            true => None,
            false => self.context.as_ref(),
        }
    }

    /// This head's full causal context, from `region`, the entries its
    /// tree's context region holds.
    ///
    /// The region holds every origin's watermark exactly except this
    /// head's own. Along one origin's own chain of commits the edition and
    /// the revision count each rise by one per commit, so the region keeps
    /// the entry from where the chain began and the reader brings it up to
    /// the head: `(head edition, count + head edition - stored edition)`.
    /// A commit whose parent head has the same origin then writes nothing
    /// to the region, and every head that breaks the chain (a merge, or a
    /// commit on another origin's head) writes the entries it changes.
    ///
    /// A region without an entry for this head's origin, or with one above
    /// the head's edition, is not one any head minted, and is refused; so
    /// is a derived entry outside [`Watermark::check`]'s bounds.
    pub fn context_from_region(&self, mut region: Context) -> Result<Context, HistoryError> {
        let version = self.version();
        let refuse = |why: String| Err(HistoryError::InvalidReference(why));
        let Some(stored) = region.get(&version.origin).copied() else {
            return refuse(format!(
                "the context region of head {version} holds no entry for its own origin"
            ));
        };
        let Some(run) = self.edition.value().checked_sub(stored.edition.value()) else {
            return refuse(format!(
                "the context region of head {version} holds its own origin at edition {}, \
                 above the head",
                stored.edition
            ));
        };
        let derived = Watermark {
            edition: self.edition,
            count: stored.count.saturating_add(run),
        };
        derived.check()?;
        region.set(version.origin, derived);
        Ok(region)
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
    /// The payload opens with [`HEAD_SIGNING_DOMAIN`]: the same session
    /// key signs both heads and in-tree revision records (through the
    /// same attest effect), and the domain tag is what makes the two
    /// payload spaces disjoint by construction — a signature over one
    /// kind can never verify as the other, whatever the field contents.
    ///
    /// ```text
    /// domain tag ("dialog/head@1\n")
    /// (length (8, big-endian) ++ UTF-8) for branch, issuer
    /// tree (32)
    /// edition (8, big-endian)
    /// whole context inline (a head minted before the region):
    ///     0x01 ++ entry count (8, big-endian)
    ///          ++ entries (origin (32) ++ edition (8, big-endian)
    ///                      ++ revision count (8, big-endian), sorted)
    /// context in the tree's region:
    ///     0x02
    /// ```
    ///
    /// A head minted before heads carried a context appends nothing after
    /// the edition, one carrying its whole context inline appends `0x01`
    /// and its entries, and one whose context is in its tree's region
    /// appends the lone `0x02`. The marker byte sits at the same offset in
    /// the two shapes that have one, and the pre-context shape ends where
    /// they begin, so the encoding stays injective: a signature over one
    /// shape never validates another. A head claiming both a region and an
    /// inline context is refused by [`Revision::verify`]; it signs as the
    /// `0x02` marker followed by the entries, so the payload stays total
    /// and injective.
    pub fn payload(&self) -> Vec<u8> {
        let mut bytes = HEAD_SIGNING_DOMAIN.to_vec();
        for field in [self.branch.as_str(), self.issuer.as_str()] {
            bytes.extend_from_slice(&(field.len() as u64).to_be_bytes());
            bytes.extend_from_slice(field.as_bytes());
        }
        bytes.extend_from_slice(self.tree.hash());
        bytes.extend_from_slice(&self.edition.key_bytes());
        let entries = |bytes: &mut Vec<u8>, context: &Context| {
            bytes.extend_from_slice(&(context.len() as u64).to_be_bytes());
            for (origin, watermark) in context.iter() {
                bytes.extend_from_slice(&origin.0);
                bytes.extend_from_slice(&watermark.edition.key_bytes());
                bytes.extend_from_slice(&watermark.count.to_be_bytes());
            }
        };
        match (self.region, &self.context) {
            (false, None) => {}
            (false, Some(context)) => {
                bytes.push(0x01);
                entries(&mut bytes, context);
            }
            (true, context) => {
                bytes.push(0x02);
                if let Some(context) = context {
                    entries(&mut bytes, context);
                }
            }
        }
        bytes
    }

    /// The content-derived entity identifying this revision — the entity
    /// onto which commit metadata can be associated, like on any other
    /// entity.
    pub fn entity(&self) -> Entity {
        self.version().entity()
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
                watermark.check()?;
            }
        }
        if self.region && self.context.is_some() {
            return Err(HistoryError::InvalidReference(
                "a head carries both an inline context and a context region".into(),
            ));
        }
        Ok(())
    }

    /// Where this head stands relative to `other`, another head of the
    /// same branch.
    ///
    /// A branch's head moves only by a fast-forward publish, so every
    /// head it takes was built on the one it replaced: each head's
    /// context includes the one before, and its edition is higher.
    /// Comparing contexts orders two heads without trusting whoever
    /// delivered them, which is what lets a head observed out of order
    /// be recognized as stale.
    ///
    /// [`Precedence::Concurrent`] means neither head built on the other:
    /// the branch was reset, or recreated, or written by something other
    /// than a fast-forward publish, and neither head says which is newer.
    ///
    /// Two heads that carry their whole contexts inline are compared by
    /// them. Two heads of one origin are ordered exactly by edition, since
    /// an origin's revisions form one chain. Any other pair, which includes
    /// every pair where a head keeps its context in its tree's region, is
    /// compared without reading a block of either tree, as the cell watcher
    /// needs, and so is answered only as far as the heads alone allow: a
    /// head at a higher edition may have built on the other, and is taken
    /// to be [`After`](Precedence::After); one at or below the other's
    /// edition cannot be known to be older, and is
    /// [`Concurrent`](Precedence::Concurrent). [`Before`](Precedence::Before)
    /// is never answered across origins without contexts, so a head a
    /// branch was reset to is never mistaken for a stale one.
    pub fn precedence(&self, other: &Revision) -> Precedence {
        let same = self.tree == other.tree && self.version() == other.version();
        if let (Some(ours), Some(theirs)) = (self.inline_context(), other.inline_context()) {
            return match (ours.includes(theirs), theirs.includes(ours)) {
                (true, true) if same => Precedence::Same,
                (true, false) => Precedence::After,
                (false, true) => Precedence::Before,
                _ => Precedence::Concurrent,
            };
        }
        let one_chain = self.origin() == other.origin();
        match self.edition.cmp(&other.edition) {
            Ordering::Greater => Precedence::After,
            Ordering::Less if one_chain => Precedence::Before,
            Ordering::Equal if same => Precedence::Same,
            Ordering::Less | Ordering::Equal => Precedence::Concurrent,
        }
    }
}

/// Where one head of a branch stands relative to another, as
/// [`Revision::precedence`] answers it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Precedence {
    /// The same head.
    Same,
    /// An earlier head: the other one built on it.
    Before,
    /// A later head: it built on the other one.
    After,
    /// Neither built on the other.
    Concurrent,
}

/// The domain tag opening every head signing payload. Signing payload
/// kinds sharing one key (heads here, in-tree revision records in
/// `dialog-artifacts`) each open with their own tag, so the payload
/// spaces are disjoint by construction and a signature can never be
/// replayed across kinds. The trailing newline keeps any future tag
/// from being a prefix of another.
pub const HEAD_SIGNING_DOMAIN: &[u8] = b"dialog/head@1\n";

pub use crate::history::EDITION_CEILING;

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
        let mut revision = Revision::new(
            TreeReference::from([7u8; 32]),
            "branch:opaque".parse::<Entity>().unwrap(),
            did,
        );
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

    /// Every head signing payload opens with the head domain tag, so the
    /// head and record payload spaces are disjoint byte spaces: one
    /// session key signs both kinds, and a signature over one must never
    /// verify as the other whatever the field contents.
    #[test]
    fn it_domain_separates_the_head_signing_payload() {
        let head = signed_head(&key(1), |_| {});
        assert!(
            head.payload().starts_with(HEAD_SIGNING_DOMAIN),
            "the head payload opens with its domain tag"
        );
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

    /// A head minted on `previous` by `issuer`, carrying the context a
    /// commit publishes: everything `previous` saw, and itself.
    fn successor(previous: &Revision, issuer: &ed25519_dalek::SigningKey, tree: u8) -> Revision {
        let mut next = previous.advance(
            TreeReference::from([tree; 32]),
            previous.branch.clone(),
            did_of(issuer),
        );
        let mut context = previous.context.clone().unwrap_or_default();
        context.record(next.version());
        next.context = Some(context);
        next
    }

    fn genesis(issuer: &ed25519_dalek::SigningKey) -> Revision {
        let mut head = Revision::new(
            TreeReference::from([1u8; 32]),
            "branch:opaque".parse::<Entity>().unwrap(),
            did_of(issuer),
        );
        let mut context = Context::new();
        context.record(head.version());
        head.context = Some(context);
        head
    }

    /// A head built on another is after it, that one is before it, and a
    /// head is the same as itself, whichever issuer minted each step.
    #[dialog_common::test]
    fn it_orders_a_head_after_the_one_it_built_on() {
        let first = genesis(&key(1));
        let second = successor(&first, &key(2), 2);
        let third = successor(&second, &key(1), 3);

        assert_eq!(second.precedence(&first), Precedence::After);
        assert_eq!(first.precedence(&second), Precedence::Before);
        assert_eq!(third.precedence(&first), Precedence::After);
        assert_eq!(first.precedence(&third), Precedence::Before);
        assert_eq!(second.precedence(&second.clone()), Precedence::Same);
    }

    /// Two heads built on one head, neither on the other, are concurrent:
    /// nothing either carries says which is newer.
    #[dialog_common::test]
    fn it_finds_heads_built_apart_concurrent() {
        let base = genesis(&key(1));
        let ours = successor(&base, &key(1), 2);
        let theirs = successor(&base, &key(2), 3);

        assert_eq!(ours.precedence(&theirs), Precedence::Concurrent);
        assert_eq!(theirs.precedence(&ours), Precedence::Concurrent);
    }

    /// Heads minted before contexts were published are ordered by their
    /// editions, and two different heads at one edition are concurrent.
    #[dialog_common::test]
    fn it_orders_heads_without_a_context_by_edition() {
        let mut first = genesis(&key(1));
        first.context = None;
        let mut second = successor(&first, &key(1), 2);
        second.context = None;
        let mut other = successor(&first, &key(2), 3);
        other.context = None;

        assert_eq!(second.precedence(&first), Precedence::After);
        assert_eq!(first.precedence(&second), Precedence::Before);
        assert_eq!(first.precedence(&first.clone()), Precedence::Same);
        assert_eq!(second.precedence(&other), Precedence::Concurrent);
    }

    /// A signed head at edition 1 000 whose context is in its tree's region.
    fn regional(issuer: &ed25519_dalek::SigningKey) -> Revision {
        signed_head(issuer, |head| {
            head.edition = Edition::new(1_000);
            head.publish_region();
        })
    }

    /// A head whose context is in its tree's region verifies, survives the
    /// dag-cbor wire byte for byte, and carries no watermark at all: its
    /// size does not depend on how many origins its history holds.
    #[test]
    fn it_roundtrips_a_region_head_through_dagcbor() {
        let head = regional(&key(1));
        head.verify().expect("a region head verifies");
        assert!(head.context.is_none() && head.region);

        let bytes = serde_ipld_dagcbor::to_vec(&head).expect("head encodes");
        let decoded: Revision = serde_ipld_dagcbor::from_slice(&bytes).expect("head decodes");
        assert_eq!(decoded, head, "the wire round-trip is lossless");
        decoded
            .verify()
            .expect("the decoded head still carries a valid signature");
        assert!(
            bytes.len() < 400 && head.payload().len() < 400,
            "a head is a few hundred bytes: {} encoded, {} signed",
            bytes.len(),
            head.payload().len()
        );
    }

    /// The three payload shapes (no context, the whole context inline, the
    /// context in the region) sign apart: a signature over any one of them
    /// never verifies the same head reshaped into another.
    #[test]
    fn it_signs_each_context_shape_apart() {
        let mut inline = Context::new();
        inline.record(Version::new(Origin::from([1u8; 32]), Edition::new(4)));
        let shapes: [(&str, Option<Context>, bool); 3] = [
            ("pre-context", None, false),
            ("inline", Some(inline), false),
            ("region", None, true),
        ];
        for (signed_as, context, region) in &shapes {
            let head = signed_head(&key(1), |head| {
                head.context = context.clone();
                head.region = *region;
            });
            head.verify()
                .unwrap_or_else(|error| panic!("the {signed_as} shape verifies: {error}"));
            for (read_as, context, region) in &shapes {
                if read_as == signed_as {
                    continue;
                }
                let mut reshaped = head.clone();
                reshaped.context = context.clone();
                reshaped.region = *region;
                assert!(
                    matches!(reshaped.verify(), Err(HistoryError::InvalidSignature(_))),
                    "a signature over the {signed_as} shape must not verify the {read_as} shape"
                );
            }
        }
    }

    /// A head claiming both an inline context and a context region is
    /// refused even when validly signed: no minter produces one, and a
    /// reader could not tell which of the two to believe.
    #[test]
    fn it_refuses_a_head_with_both_an_inline_context_and_a_region() {
        let head = signed_head(&key(1), |head| head.region = true);
        assert!(head.context.is_some());
        assert!(matches!(
            head.verify(),
            Err(HistoryError::InvalidReference(_))
        ));
    }

    /// The region holds every origin exactly except the head's own, whose
    /// entry is where the head's chain of commits began: the reader brings
    /// it up to the head, one edition and one revision per commit, and
    /// leaves every other entry as stored.
    #[test]
    fn it_derives_the_heads_own_entry_from_the_region() {
        let head = regional(&key(1));
        let own = head.origin();
        let other = Origin::from([9u8; 32]);
        let mut region = Context::new();
        region.set(
            own,
            Watermark {
                edition: Edition::new(990),
                count: 40,
            },
        );
        region.set(
            other,
            Watermark {
                edition: Edition::new(500),
                count: 3,
            },
        );

        let context = head
            .context_from_region(region.clone())
            .expect("the region is one a head minted");
        assert_eq!(
            context.get(&own),
            Some(&Watermark {
                edition: Edition::new(1_000),
                count: 50,
            }),
            "ten commits since the chain began"
        );
        assert_eq!(context.get(&other), region.get(&other));
        assert_eq!(context.len(), 2);

        // A chain that begins at the head itself derives nothing new.
        let mut exact = region.clone();
        exact.set(
            own,
            Watermark {
                edition: Edition::new(1_000),
                count: 50,
            },
        );
        assert_eq!(head.context_from_region(exact.clone()).unwrap(), exact);
    }

    /// A region no head could have minted is refused, with nothing derived:
    /// one without the head's own origin, one holding it above the head,
    /// and one whose derived entry counts more revisions than its edition
    /// leaves room for.
    #[test]
    fn it_refuses_a_region_no_head_minted() {
        let head = regional(&key(1));
        let own = head.origin();
        let at = |edition: u64, count: u64| {
            let mut region = Context::new();
            region.set(
                own,
                Watermark {
                    edition: Edition::new(edition),
                    count,
                },
            );
            region
        };
        let hostile = [
            ("no entry for the head's origin", Context::new()),
            ("its own origin above the head", at(1_001, 1)),
            ("more revisions than editions", at(999, 1_001)),
        ];
        for (why, region) in hostile {
            assert!(
                matches!(
                    head.context_from_region(region),
                    Err(HistoryError::InvalidReference(_))
                ),
                "{why} must be refused"
            );
        }
    }

    /// Heads whose contexts are in their trees' regions are ordered without
    /// reading either tree: exactly by edition along one origin's chain,
    /// and across origins only as far as editions allow, so a head at or
    /// below another origin's edition is concurrent with it, never older.
    #[dialog_common::test]
    fn it_orders_region_heads_by_edition() {
        let mut first = genesis(&key(1));
        first.publish_region();
        let mut second = successor(&first, &key(1), 2);
        second.publish_region();
        let mut other = successor(&first, &key(2), 3);
        other.publish_region();

        assert_eq!(second.precedence(&first), Precedence::After);
        assert_eq!(first.precedence(&second), Precedence::Before);
        assert_eq!(second.precedence(&second.clone()), Precedence::Same);
        assert_eq!(second.precedence(&other), Precedence::Concurrent);

        let mut later = successor(&other, &key(2), 4);
        later.publish_region();
        assert_eq!(later.precedence(&first), Precedence::After);
        assert_eq!(
            first.precedence(&later),
            Precedence::Concurrent,
            "an earlier head of another origin may be one the branch was reset to"
        );
    }
}
