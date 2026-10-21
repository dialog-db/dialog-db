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
    ACTIVE_CONTEXT, Context, Edition, HistoryError, Origin, Spill, Version, verify_issuer_signature,
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

    /// The causal context (per-origin watermark) of this revision's
    /// ancestry, itself included, as far as the head carries it.
    /// Publishing it with the head is what lets a peer read a replica's
    /// knowledge without walking its log: pull seeds its context memo
    /// from it, skips upstreams whose context is included in ours
    /// (nothing new), and adopts an upstream's tree wholesale when its
    /// context includes ours and we have no local novelty. Covered by the
    /// head signature (see [`Revision::payload`]), so adopting it is as
    /// trustworthy as adopting the head.
    ///
    /// On a head that carries a [`spill`](Revision::spill), this is the
    /// **active set**: the [`ACTIVE_CONTEXT`] watermarks with the highest
    /// editions. The full context is in the tree's context region, and
    /// the spill summarizes what the active set leaves out. On a head
    /// minted before the region existed it is the whole context, and on
    /// one minted before heads carried a context it is `None`; readers
    /// fall back to deriving the context by the ancestry walk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context: Option<Context>,

    /// The summary of the watermarks [`context`](Revision::context) leaves
    /// out: a digest of them, how many there are, and the highest edition
    /// among them. Present on every head minted since the full context
    /// moved into the tree's context region, so its presence is also what
    /// says the head's tree carries that region; `None` on heads minted
    /// before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spill: Option<Spill>,

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
            spill: None,
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
            spill: None,
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
            spill: None,
            signature: Vec::new(),
        }
    }

    /// Publish `context`, the full causal context of this revision, on
    /// the head: its active set as [`context`](Revision::context) and the
    /// summary of the rest as [`spill`](Revision::spill). The caller
    /// writes the full context into the tree's context region.
    pub fn publish_context(&mut self, context: &Context) {
        let (active, spill) = context.project(ACTIVE_CONTEXT);
        self.publish_projection(active, spill);
    }

    /// Publish an already computed projection of this revision's full
    /// context (see [`Context::project`] and [`Context::project_from`]).
    pub fn publish_projection(&mut self, active: Context, spill: Spill) {
        self.context = Some(active);
        self.spill = Some(spill);
    }

    /// The head's whole causal context, when the head carries all of it:
    /// a head minted before the context region existed, or one whose
    /// active set holds every watermark (nothing spilled). `None` when the
    /// whole context is only in the tree's context region, or when the
    /// head carries no context at all.
    pub fn inline_context(&self) -> Option<&Context> {
        match (&self.context, &self.spill) {
            (Some(context), None) => Some(context),
            (Some(context), Some(spill)) if spill.is_empty() => Some(context),
            _ => None,
        }
    }

    /// Check that `context`, read from this head's tree, is the context the
    /// head publishes: it projects to exactly the head's active set and
    /// spill. A head that publishes its whole context inline must equal it.
    ///
    /// This is what binds a tree's context region to the head that names
    /// it. The signature covers the region through the tree root, but only
    /// this check says the region agrees with what the head itself claims;
    /// a context that does not is refused before any gate acts on it.
    pub fn check_context(&self, context: &Context) -> Result<(), HistoryError> {
        let agrees = match (&self.context, &self.spill) {
            (Some(active), Some(spill)) => {
                let (projected, projected_spill) = context.project(ACTIVE_CONTEXT);
                &projected == active && &projected_spill == spill
            }
            (Some(full), None) => full == context,
            (None, _) => false,
        };
        if agrees {
            Ok(())
        } else {
            Err(HistoryError::InvalidReference(format!(
                "the context of head {} does not project to what the head publishes",
                self.version()
            )))
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
    /// context, when present without a spill (the whole context inline):
    ///     0x01 ++ entry count (8, big-endian)
    ///          ++ entries (origin (32) ++ edition (8, big-endian)
    ///                      ++ revision count (8, big-endian), sorted)
    /// active set and spill, when a spill is present:
    ///     0x02 ++ entry count (8, big-endian) ++ entries (as above)
    ///          ++ spill digest (32) ++ spilled count (8, big-endian)
    ///          ++ highest spilled edition (8, big-endian)
    /// ```
    ///
    /// A head without a context appends nothing after the edition (the
    /// pre-context payload shape), a head carrying its whole context
    /// appends the `0x01` marker plus the sorted watermark entries, and a
    /// head carrying its active set appends the `0x02` marker, the entries,
    /// and the spill. The marker byte sits at the same offset in both
    /// shapes that have one, and the pre-context shape ends where they
    /// begin, so the encoding stays injective: a signature over one shape
    /// can never validate another.
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
        match (&self.context, &self.spill) {
            (None, None) => {}
            (Some(context), None) => {
                bytes.push(0x01);
                entries(&mut bytes, context);
            }
            // A spill without an active set is refused by `verify`; it
            // signs as an empty active set so the payload stays total.
            (context, Some(spill)) => {
                bytes.push(0x02);
                entries(&mut bytes, context.as_ref().unwrap_or(&Context::new()));
                bytes.extend_from_slice(&spill.digest);
                bytes.extend_from_slice(&spill.count.to_be_bytes());
                bytes.extend_from_slice(&spill.highest.key_bytes());
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
        if let Some(spill) = &self.spill {
            self.verify_projection(spill)?;
        }
        Ok(())
    }

    /// The checks a head carrying an active set and a spill passes from the
    /// head alone: the active set is bounded by [`ACTIVE_CONTEXT`], and it
    /// is the top of the map. Whether the spill is what the tree's region
    /// actually holds is checked when the region is read
    /// ([`Revision::check_context`]).
    fn verify_projection(&self, spill: &Spill) -> Result<(), HistoryError> {
        let refuse = |why: String| Err(HistoryError::InvalidReference(why));
        let Some(active) = &self.context else {
            return refuse("a head carries a spill without an active set".into());
        };
        if active.len() > ACTIVE_CONTEXT {
            return refuse(format!(
                "a head carries {} active watermarks, more than the {ACTIVE_CONTEXT} allowed",
                active.len()
            ));
        }
        if spill.is_empty() {
            if *spill != Spill::empty() {
                return refuse("an empty spill carries a digest or edition".into());
            }
            return Ok(());
        }
        // Something spilled, so the active set must be full: the
        // projection only spills what does not fit.
        if active.len() < ACTIVE_CONTEXT {
            return refuse(format!(
                "a head spills {} watermarks beside an active set of only {}",
                spill.count,
                active.len()
            ));
        }
        if spill.highest.value() >= EDITION_CEILING {
            return refuse(format!(
                "spilled edition {} exceeds the protocol ceiling",
                spill.highest
            ));
        }
        // The active set is the top of the map by edition.
        if let Some((_, low)) = active
            .iter()
            .find(|(_, watermark)| watermark.edition < spill.highest)
        {
            return refuse(format!(
                "active watermark at edition {} sits below a spilled one at {}",
                low.edition, spill.highest
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
    /// be recognized as stale. Heads minted before contexts were
    /// published are ordered by edition alone.
    ///
    /// [`Precedence::Concurrent`] means neither head built on the other:
    /// the branch was reset, or recreated, or written by something other
    /// than a fast-forward publish, and neither head says which is newer.
    ///
    /// Heads that carry only their active sets are compared exactly when
    /// their spills are equal (the spilled watermarks are then the same on
    /// both sides). Otherwise each side is asked only what its head can
    /// rule out: a head that cannot include the other is told apart for
    /// certain, and one that may include it is taken to, which is the
    /// order a head minted before contexts were published gets by edition.
    pub fn precedence(&self, other: &Revision) -> Precedence {
        let same = self.tree == other.tree && self.version() == other.version();
        match (self.knowledge(), other.knowledge()) {
            (Some(ours), Some(theirs)) => match (ours.includes(&theirs), theirs.includes(&ours)) {
                (true, true) if same => Precedence::Same,
                (true, false) => Precedence::After,
                (false, true) => Precedence::Before,
                _ => Precedence::Concurrent,
            },
            _ => match self.edition.cmp(&other.edition) {
                Ordering::Greater => Precedence::After,
                Ordering::Less => Precedence::Before,
                Ordering::Equal if same => Precedence::Same,
                Ordering::Equal => Precedence::Concurrent,
            },
        }
    }
}

impl Revision {
    /// What this head says about its context, for [`Revision::precedence`]:
    /// a head carrying its whole context inline reads as an active set
    /// with nothing spilled.
    fn knowledge(&self) -> Option<Knowledge<'_>> {
        let active = self.context.as_ref()?;
        Some(Knowledge {
            edition: self.edition,
            active,
            spill: self.spill.unwrap_or_else(Spill::empty),
        })
    }
}

/// One head's published knowledge, as [`Revision::precedence`] compares it.
struct Knowledge<'a> {
    edition: Edition,
    active: &'a Context,
    spill: Spill,
}

impl Knowledge<'_> {
    /// Whether the full context behind this head may include the one behind
    /// `other`. Exact when the spills are equal; otherwise `false` only when
    /// the heads alone rule inclusion out.
    fn includes(&self, other: &Knowledge<'_>) -> bool {
        if self.spill == other.spill {
            return self.active.includes(other.active);
        }
        // Including the other head means having seen its own version, the
        // highest edition in its context; a head's edition is the highest in
        // its own. And an included map has no more origins than the one
        // including it.
        if self.edition < other.edition
            || (other.active.len() as u64).saturating_add(other.spill.count)
                > (self.active.len() as u64).saturating_add(self.spill.count)
        {
            return false;
        }
        other
            .active
            .iter()
            .all(|(origin, theirs)| match self.active.get(origin) {
                Some(ours) => ours.edition >= theirs.edition,
                // Not active here, so it can only be among what we spill, all
                // of which sits at or below the highest spilled edition.
                None => !self.spill.is_empty() && theirs.edition <= self.spill.highest,
            })
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

    use std::iter::once;

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

    /// One way to reshape a head before it is signed.
    type Edit<'a> = Box<dyn Fn(&mut Revision) + 'a>;

    /// A context of `origins` passing origins plus the head's own, the head
    /// at the top.
    fn crowded(head: &Revision, origins: u8) -> Context {
        let mut context = Context::new();
        for seed in 0..origins {
            context.record(Version::new(
                Origin::from([seed; 32]),
                Edition::new(u64::from(seed)),
            ));
        }
        context.record(head.version());
        context
    }

    /// A signed head at edition 1 000 publishing the projection of a
    /// context of `origins` passing origins plus its own.
    fn projected(issuer: &ed25519_dalek::SigningKey, origins: u8) -> (Revision, Context) {
        let mut full = None;
        let head = signed_head(issuer, |head| {
            head.edition = Edition::new(1_000);
            let context = crowded(head, origins);
            head.publish_context(&context);
            full = Some(context);
        });
        (head, full.expect("the edit ran"))
    }

    /// A head carrying its active set and spill verifies and survives the
    /// dag-cbor wire byte for byte, and carries no more than the active
    /// set inline however many origins its context holds.
    #[test]
    fn it_roundtrips_a_projected_head_through_dagcbor() {
        let (head, full) = projected(&key(1), 200);
        head.verify().expect("a projected head verifies");
        assert_eq!(
            head.context.as_ref().map(Context::len),
            Some(ACTIVE_CONTEXT)
        );
        assert_eq!(
            head.spill.map(|spill| spill.count),
            Some((full.len() - ACTIVE_CONTEXT) as u64)
        );

        let bytes = serde_ipld_dagcbor::to_vec(&head).expect("head encodes");
        let decoded: Revision = serde_ipld_dagcbor::from_slice(&bytes).expect("head decodes");
        assert_eq!(decoded, head, "the wire round-trip is lossless");
        decoded
            .verify()
            .expect("the decoded head still carries a valid signature");
    }

    /// The three payload shapes (no context, the whole context inline, an
    /// active set and spill) sign apart: a signature over one never
    /// verifies a head reshaped into another.
    #[test]
    fn it_signs_each_context_shape_apart() {
        let (projected, full) = projected(&key(1), 200);

        let mut inline = projected.clone();
        inline.context = Some(full.clone());
        inline.spill = None;
        assert!(
            matches!(inline.verify(), Err(HistoryError::InvalidSignature(_))),
            "a projected head's signature does not cover the inline shape"
        );

        let mut bare = projected.clone();
        bare.context = None;
        bare.spill = None;
        assert!(
            matches!(bare.verify(), Err(HistoryError::InvalidSignature(_))),
            "nor the pre-context shape"
        );

        let legacy = signed_head(&key(1), |head| {
            head.edition = projected.edition;
            head.context = Some(full.clone());
        });
        legacy.verify().expect("the inline shape still verifies");
        let mut reshaped = legacy.clone();
        reshaped.context = projected.context.clone();
        reshaped.spill = projected.spill;
        assert!(
            matches!(reshaped.verify(), Err(HistoryError::InvalidSignature(_))),
            "an inline head's signature does not cover the projected shape"
        );
        assert_ne!(legacy.payload(), projected.payload());
    }

    /// Projections no map can produce are refused at verification even
    /// when validly signed: more active watermarks than allowed, a spill
    /// beside an active set that is not full, an active watermark below a
    /// spilled one, a spilled edition at the ceiling, a spill without an
    /// active set, and an empty spill that names a digest.
    #[test]
    fn it_refuses_projections_no_map_can_produce() {
        let (template, full) = projected(&key(1), 200);
        let active = template.context.clone().expect("projected");
        let spill = template.spill.expect("projected");

        let mut short = Context::new();
        for (origin, watermark) in active.iter().take(10) {
            short.merge(&once((*origin, *watermark)).collect());
        }

        let hostile: Vec<(&str, Edit<'_>)> = vec![
            (
                "too many active watermarks",
                Box::new(|head: &mut Revision| {
                    head.context = Some(full.clone());
                    head.spill = Some(Spill::empty());
                }),
            ),
            (
                "a spill beside a short active set",
                Box::new(|head: &mut Revision| {
                    head.context = Some(short.clone());
                    head.spill = Some(spill);
                }),
            ),
            (
                "an active watermark below a spilled one",
                Box::new(|head: &mut Revision| {
                    head.context = Some(active.clone());
                    head.spill = Some(Spill {
                        highest: Edition::new(999),
                        ..spill
                    });
                }),
            ),
            (
                "a spilled edition at the ceiling",
                Box::new(|head: &mut Revision| {
                    head.edition = Edition::new(EDITION_CEILING - 1);
                    head.context = Some(active.clone());
                    head.spill = Some(Spill {
                        highest: Edition::new(EDITION_CEILING),
                        ..spill
                    });
                }),
            ),
            (
                "a spill without an active set",
                Box::new(|head: &mut Revision| {
                    head.context = None;
                    head.spill = Some(spill);
                }),
            ),
            (
                "an empty spill naming a digest",
                Box::new(|head: &mut Revision| {
                    head.context = Some(short.clone());
                    head.spill = Some(Spill { count: 0, ..spill });
                }),
            ),
        ];
        for (why, edit) in hostile {
            let head = signed_head(&key(1), |head| {
                head.edition = template.edition;
                edit(head);
            });
            assert!(
                matches!(head.verify(), Err(HistoryError::InvalidReference(_))),
                "{why} must be refused despite the valid signature"
            );
        }
    }

    /// A context read back from a head's tree is accepted only when it is
    /// the one the head publishes: for a projected head, one that projects
    /// to its active set and spill; for an inline head, its context.
    #[test]
    fn it_checks_a_region_against_the_head_that_names_it() {
        let (head, full) = projected(&key(1), 200);
        head.check_context(&full)
            .expect("the full context projects to the head");

        let mut spilled_raised = full.clone();
        spilled_raised.record(Version::new(Origin::from([1u8; 32]), Edition::new(2)));
        assert!(matches!(
            head.check_context(&spilled_raised),
            Err(HistoryError::InvalidReference(_))
        ));

        let mut extra = full.clone();
        extra.record(Version::new(Origin::from([250u8; 32]), Edition::new(0)));
        assert!(matches!(
            head.check_context(&extra),
            Err(HistoryError::InvalidReference(_))
        ));

        let inline = signed_head(&key(1), |head| head.context = Some(full.clone()));
        inline
            .check_context(&full)
            .expect("an inline head names its context");
        assert!(matches!(
            inline.check_context(&extra),
            Err(HistoryError::InvalidReference(_))
        ));
    }

    /// Heads that carry only their active sets are ordered exactly while
    /// their spills agree, and a head that cannot include another, by
    /// edition or by an active watermark above everything it spills, is
    /// never ordered after it.
    #[dialog_common::test]
    fn it_orders_projected_heads() {
        let mut full = Context::new();
        let mut first = genesis(&key(1));
        for seed in 0..150u8 {
            full.record(Version::new(
                Origin::from([seed; 32]),
                Edition::new(u64::from(seed)),
            ));
        }
        first.edition = Edition::new(150);
        full.record(first.version());
        first.publish_context(&full);

        let step =
            |previous: &Revision, full: &Context, issuer: &ed25519_dalek::SigningKey, tree: u8| {
                let mut next = previous.advance(
                    TreeReference::from([tree; 32]),
                    previous.branch.clone(),
                    did_of(issuer),
                );
                let mut context = full.clone();
                context.record(next.version());
                next.publish_context(&context);
                (next, context)
            };

        let (second, second_full) = step(&first, &full, &key(1), 2);
        let (third, _) = step(&second, &second_full, &key(1), 3);
        assert_eq!(
            first.spill, second.spill,
            "an active writer keeps the spill"
        );
        assert_eq!(second.precedence(&first), Precedence::After);
        assert_eq!(first.precedence(&second), Precedence::Before);
        assert_eq!(third.precedence(&first), Precedence::After);
        assert_eq!(second.precedence(&second.clone()), Precedence::Same);

        // Two heads built apart on the same head, by writers that keep the
        // spill, are concurrent.
        let (ours, _) = step(&second, &second_full, &key(1), 4);
        let mut theirs = second.advance(
            TreeReference::from([5u8; 32]),
            second.branch.clone(),
            did_of(&key(1)),
        );
        // Same writer, same edition, different tree: neither built on the
        // other.
        let mut context = second_full.clone();
        context.record(theirs.version());
        theirs.publish_context(&context);
        assert_eq!(ours.precedence(&theirs), Precedence::Concurrent);

        // A new writer changes the spill; the older head still cannot be
        // ordered after the newer one.
        let (fresh, _) = step(&second, &second_full, &key(9), 6);
        assert_ne!(fresh.spill, second.spill, "a new writer pushes one out");
        assert_eq!(fresh.precedence(&second), Precedence::After);
        assert_eq!(second.precedence(&fresh), Precedence::Before);
    }
}
