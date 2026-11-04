use dialog_capability::Did;
use dialog_ucan_core::{Delegation, subject::Subject};
use dialog_varsig::algorithm::eddsa::Ed25519Signature;
use serde::{Deserialize, Serialize};

use crate::Revision;
use crate::{
    Artifact, Datum, DialogArtifactsError, Entity, Key, Relation, State, Value,
    key::{EncodedValue, artifact_index_keys_with},
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
/// [`ReservedRelation`](DialogArtifactsError::ReservedAttribute)), so at
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
    /// DID of the profile (long-lived key) the issuer acted for. On its
    /// own this is only the issuer's claim; the profile's delegation to
    /// the issuer, stored beside the record as a
    /// [`DelegationRecord`](super::DelegationRecord), is
    /// what proves it (see [`RevisionRecord::attests`]).
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

    /// Check that `delegation` binds the record's
    /// [`authority`](Self::authority): issued by the authority, to the
    /// issuer, for any subject and the root command, and signed by the
    /// authority. When it does, the record proves the authority
    /// responsible for its revision; its issuer it proves by its own
    /// signature (see [`verify`](Self::verify)).
    ///
    /// Attribution, not authorization: the delegation's time bounds are
    /// not checked, because a profile that delegated to the issuer stays
    /// the one it acted for after the delegation lapses, while whether
    /// the write was allowed is decided by whoever accepted it.
    pub fn attests(&self, delegation: &[u8]) -> Result<(), DialogArtifactsError> {
        let refused = |reason: &str| {
            DialogArtifactsError::InvalidSignature(format!(
                "Revision record delegation does not bind {}: {reason}",
                self.authority
            ))
        };
        let delegation: Delegation<Ed25519Signature> =
            serde_ipld_dagcbor::from_slice(delegation)
                .map_err(|error| refused(&format!("not an Ed25519 delegation ({error})")))?;
        if delegation.issuer().to_string() != self.authority {
            return Err(refused("issued by another principal"));
        }
        if delegation.audience().to_string() != self.issuer {
            return Err(refused("issued to another principal"));
        }
        if *delegation.subject() != Subject::Any || !delegation.command().segments().is_empty() {
            return Err(refused("narrower than the authority's whole authority"));
        }
        let payload = delegation
            .signed_payload()
            .map_err(|error| refused(&format!("unencodable ({error})")))?;
        verify_issuer_signature(
            &self.authority,
            &payload,
            &delegation.signature().to_bytes(),
        )
        .map_err(|error| refused(&format!("{error}")))
    }

    /// The fact carrying this record: an [`Artifact`] on the revision
    /// entity under [`REVISION_ATTRIBUTE`], valued with the encoded record
    pub fn to_artifact(&self, version: &Version) -> Result<Artifact, DialogArtifactsError> {
        Ok(Artifact {
            the: Relation::try_from(REVISION_ATTRIBUTE.to_string())?,
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
    /// manifest to find it. Under a manifest whose `inline_n` is below the
    /// record's size the value spills, and the result carries the block to
    /// store beside the tree.
    pub fn entries(&self, manifest: &Manifest) -> Result<RecordEntries, DialogArtifactsError> {
        let version = self.version();
        let artifact = self.to_artifact(&version)?;

        // Only the EAV and AEV orderings are written (see above), so the VAE
        // key this builds is dropped.
        let encoded = EncodedValue::new(&artifact.is, manifest);
        let (entity_key, attribute_key, _) =
            artifact_index_keys_with(&artifact, encoded.value_type, encoded.payload);
        let mut datum = Datum::for_artifact(&artifact);
        datum.version = Some(version);
        let added = State::Added(datum);

        Ok(RecordEntries {
            entries: vec![(entity_key, added.clone()), (attribute_key, added)],
            spills: encoded.spill.into_iter().collect(),
        })
    }

    /// The record for `revision` —
    /// everything the revision states about itself as one atomic fact,
    /// ready to be signed and written into the tree.
    ///
    /// The `authority` (the profile the issuer acts for) is passed in: the
    /// head does not carry it — its identity is the branch entity plus the
    /// issuer — but the record keeps the attribution readable. The
    /// revision's tree root is deliberately not in the record: the record
    /// lives in that tree, so the root cannot appear inside itself.
    pub fn create(
        revision: &Revision,
        authority: &Did,
        parents: Vec<Version>,
        skips: Vec<Version>,
    ) -> Self {
        Self {
            format: REVISION_RECORD_FORMAT,
            branch: revision.branch.clone(),
            issuer: revision.issuer.to_string(),
            authority: authority.to_string(),
            parents,
            skips,
            signature: Vec::new(),
        }
    }
}

/// Pre-built tree entries to write outside the instruction path (revision
/// records, blob-index machinery), with the value block they need when a
/// value spilled out of its key.
///
/// The write paths ([`ArtifactTreeExt::record`](crate::tree::ArtifactTreeExt::record),
/// [`BufferedBatch::record`](crate::BufferedBatch::record)) store the block
/// with the entries, so a spilled value is never written without its bytes.
#[derive(Debug, Clone, Default)]
pub struct RecordEntries {
    /// The tree entries.
    pub entries: Vec<(Key, State<Datum>)>,
    /// The blocks of spilled values: the reference each one's keys carry
    /// and the raw value bytes stored under it. Empty when every value
    /// stays inline.
    pub spills: Vec<(dialog_storage::Blake3Hash, Vec<u8>)>,
}

impl RecordEntries {
    /// These entries followed by `other`'s, to be written together.
    pub fn and(mut self, other: RecordEntries) -> Self {
        self.entries.extend(other.entries);
        self.spills.extend(other.spills);
        self
    }
}

impl From<Vec<(Key, State<Datum>)>> for RecordEntries {
    fn from(entries: Vec<(Key, State<Datum>)>) -> Self {
        Self {
            entries,
            spills: Vec::new(),
        }
    }
}
