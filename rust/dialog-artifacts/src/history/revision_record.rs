use serde::{Deserialize, Serialize};

use crate::history::VersionExt as _;
use crate::{
    Artifact, Attribute, Datum, DialogArtifactsError, Entity, Key, State, Value,
    key::artifact_index_keys,
};
use dialog_search_tree::Manifest;

use super::{Edition, Origin, REVISION_ATTRIBUTE, Version, verify_issuer_signature};

/// Everything a revision states about itself, as one atomic record.
///
/// Stored as a single fact in the ordinary EAV/AEV/VAE indexes — entity =
/// [`Version::entity`], attribute = [`REVISION_ATTRIBUTE`], value =
/// [`Value::Record`] of this struct's dag-cbor encoding. One record per
/// revision keeps the metadata atomic (a revision is never partially
/// described: the record is present or it is not), makes each step of
/// ancestor traversal a single exact lookup, and is the unit the issuer
/// signs. Individual fields are exposed to queries through formulas over
/// the record rather than stored as separate facts.
///
/// The attribute lives in the reserved `dialog.` namespace: user
/// instructions cannot write it (see
/// [`ReservedAttribute`](DialogArtifactsError::ReservedAttribute)), so at
/// the library level lineage cannot be corrupted through the ordinary
/// write path. Against a hostile peer crafting records on the wire, the
/// record carries the issuer's signature over every other field, and it
/// binds itself to its slot: the [`Version`] it was recorded under is
/// derivable from the record's own contents ([`RevisionRecord::version`]),
/// so a valid record replayed at a different revision entity is detected
/// just like a tampered one (see [`RevisionRecord::verify`]).
///
/// The revision's tree root is deliberately absent: the record lives
/// inside that tree, so the root cannot appear inside itself. The head
/// `Revision` published to the branch cell carries the root — and its own
/// signature binding the root to the issuer.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RevisionRecord {
    /// Encoding version of this record, for forward evolution
    pub format: u8,
    /// The branch entity this revision was minted on (the opaque
    /// content-derived identifier folding replica and name — the same
    /// one the head carries)
    pub branch: Entity,
    /// DID of the operator (session key) that minted the revision — the
    /// key whose signature binds this record
    pub issuer: String,
    /// DID of the profile (long-lived key) the issuer *claims* authorized
    /// it. Attribution metadata vouched for by the issuer's signature, not
    /// an authorization proof: binding it cryptographically needs
    /// delegation proofs plus a time-anchoring story (see
    /// `notes/version-control.md`).
    pub authority: String,
    /// Parent revision versions — the revision DAG edge. Empty for
    /// genesis; two entries for a merge.
    pub parents: Vec<Version>,
    /// Skip links: the distinct 2-adic anchors of this revision's
    /// first-parent run, in strictly decreasing edition order — the
    /// level-`k` anchor (most recent ancestor whose edition is divisible
    /// by `2^(k+1)`) is the first entry with that divisibility (see
    /// [`carry_skips`](super::carry_skips)). Empty for genesis and merge
    /// revisions.
    pub skips: Vec<Version>,
    /// The issuer's Ed25519 signature over [`RevisionRecord::payload`] —
    /// this record encoded with an empty signature field. The key is the
    /// one the issuer DID names (`did:key`).
    #[serde(with = "serde_bytes")]
    pub signature: Vec<u8>,
}

/// The current [`RevisionRecord::format`]
pub const REVISION_RECORD_FORMAT: u8 = 0;

/// The domain tag opening every revision-record signing payload; the
/// counterpart of `crate::HEAD_SIGNING_DOMAIN` for the other
/// payload kind the same session key signs.
pub const RECORD_SIGNING_DOMAIN: &[u8] = b"dialog/revision-record@1\n";

impl RevisionRecord {
    /// Encode this record into the bytes carried by its [`Value::Record`]
    pub fn to_bytes(&self) -> Result<Vec<u8>, DialogArtifactsError> {
        serde_ipld_dagcbor::to_vec(self)
            .map_err(|error| DialogArtifactsError::InvalidValue(format!("{error}")))
    }

    /// Decode a record from the bytes of its [`Value::Record`]
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, DialogArtifactsError> {
        serde_ipld_dagcbor::from_slice(bytes)
            .map_err(|error| DialogArtifactsError::InvalidValue(format!("{error}")))
    }

    /// The canonical signing payload: the record signing domain tag
    /// followed by this record dag-cbor encoded with an empty signature
    /// field. The tag makes the record and head payload spaces disjoint
    /// by construction — the same session key signs both, and a
    /// signature over one kind must never verify as the other (see
    /// `crate::HEAD_SIGNING_DOMAIN`).
    pub fn payload(&self) -> Result<Vec<u8>, DialogArtifactsError> {
        let mut unsigned = self.clone();
        unsigned.signature = Vec::new();
        let mut payload = RECORD_SIGNING_DOMAIN.to_vec();
        payload.extend_from_slice(&unsigned.to_bytes()?);
        Ok(payload)
    }

    /// The [`Origin`] of this record's revision, derived from the branch
    /// entity and issuer the record itself names — the same derivation the
    /// minting replica used
    pub fn origin(&self) -> Origin {
        Origin::derive_from_identifiers([self.branch.as_str(), self.issuer.as_str()])
    }

    /// The [`Edition`] of this record's revision, derived from its parents:
    /// `max(parent editions) + 1`, or the genesis edition when there are
    /// none
    pub fn edition(&self) -> Edition {
        self.parents
            .iter()
            .map(|parent| parent.edition)
            .max()
            .map(|edition| edition.successor())
            .unwrap_or(Edition::GENESIS)
    }

    /// The [`Version`] this record describes, derived entirely from the
    /// record's own contents — so a reader can bind a record to the slot it
    /// was found at
    pub fn version(&self) -> Version {
        Version::new(self.origin(), self.edition())
    }

    /// Verify this record against the version slot it was found at: the
    /// version derived from the record's own contents must be that slot
    /// (a valid record cannot be replayed at another revision entity), and
    /// the signature must verify under the key the issuer DID names.
    pub fn verify(&self, version: &Version) -> Result<(), DialogArtifactsError> {
        let derived = self.version();
        if derived != *version {
            return Err(DialogArtifactsError::InvalidSignature(format!(
                "Revision record derives version {derived} but was recorded at {version}"
            )));
        }
        verify_issuer_signature(&self.issuer, &self.payload()?, &self.signature)?;
        Ok(())
    }

    /// The fact carrying this record: an [`Artifact`] on the revision
    /// entity under [`REVISION_ATTRIBUTE`], valued with the encoded record
    pub fn to_artifact(&self, version: &Version) -> Result<Artifact, DialogArtifactsError> {
        Ok(Artifact {
            the: Attribute::try_from(REVISION_ATTRIBUTE.to_string())?,
            of: version.entity(),
            is: Value::Record(self.to_bytes()?),
            cause: None,
        })
    }

    /// The tree entries carrying this record: one fact on the revision
    /// entity under the reserved revision attribute, keyed into the
    /// entity- and attribute-ordered indexes. Those are the two shapes
    /// queries take (entity bound: the planner scans EAV; entity free:
    /// AEV), and the durable history reader uses the attribute
    /// ordering. The value ordering is deliberately skipped: a record
    /// blob is unique to its revision and nothing ever looks one up by
    /// value, and skipping it cuts a commit's record write from three
    /// large-leaf rebuilds to two. (Collapsing to one ordering needs
    /// the query planner to learn per-attribute index availability;
    /// until then both query shapes must be served.)
    ///
    /// `manifest` is the target tree's format: the record's value rides its key
    /// through the inline-vs-spill decision, so a read must use the same
    /// manifest to find it.
    pub fn entries(
        &self,
        manifest: &Manifest,
    ) -> Result<Vec<(Key, State<Datum>)>, DialogArtifactsError> {
        let version = self.version();
        let artifact = self.to_artifact(&version)?;

        // Only the EAV and AEV orderings are written (see above), so the VAE
        // key this builds is dropped.
        let (entity_key, attribute_key, _) = artifact_index_keys(&artifact, manifest);
        let mut datum = Datum::for_artifact(&artifact);
        datum.version = Some(version);
        let added = State::Added(datum);

        Ok(vec![(entity_key, added.clone()), (attribute_key, added)])
    }
}
