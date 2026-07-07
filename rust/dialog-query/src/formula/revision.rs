//! Formulas projecting the fields of version-control revision records.
//!
//! A revision describes itself with one atomic `dialog.db/revision` fact
//! — a [`Value::Record`](crate::Value) of the dag-cbor
//! [`RevisionRecord`] — rather than per-field facts (see
//! `dialog_artifacts::history`). These formulas expose the record's
//! individual fields to queries: bind the record with an attribute scan,
//! then apply [`Revision`] (scalar fields, one row) or [`RevisionParent`]
//! (the DAG edge, one row per parent).
//!
//! # Forgery does not project
//!
//! Both formulas re-derive the revision's identity from the record's own
//! contents and refuse records that don't verify:
//!
//! - The issuer's signature must verify under the key its `did:key`
//!   names — a tampered or fabricated record yields no rows.
//! - The `this` output is the revision entity *derived* from the record
//!   (origin from `(lineage, issuer)`, edition from the parents), not
//!   the entity the record was found at. Sharing the `this` variable
//!   with the scan's `of` makes the join itself reject a valid record
//!   replayed at another revision entity.

use dialog_artifacts::history::RevisionRecord;

use crate::formula::Input;
use crate::types::RecordBytes;
use crate::{Entity, Formula};

/// Decode and verify the [`RevisionRecord`] carried by `of`, or `None`
/// if the record is malformed or does not vouch for itself.
fn verified(of: &RecordBytes) -> Option<RevisionRecord> {
    let record = RevisionRecord::try_from_bytes(&of.0).ok()?;
    record.verify(&record.version()).ok()?;
    Some(record)
}

/// Projects the scalar fields of a revision record: attribution
/// (issuer, authority), the branch lineage, the causal depth, and the
/// revision entity derived from the record itself.
#[derive(Debug, Clone, Formula)]
pub struct Revision {
    /// The revision record bytes — the value of a `dialog.db/revision`
    /// fact.
    pub of: RecordBytes,
    /// The revision entity derived from the record's own contents.
    /// Join it against the scanned fact's entity to reject replays.
    #[output]
    pub this: Entity,
    /// The branch lineage entity the revision was minted on.
    #[output]
    pub lineage: Entity,
    /// DID (as entity) of the operator that minted the revision.
    #[output]
    pub issuer: Entity,
    /// DID (as entity) of the profile that authorized the revision.
    #[output]
    pub authority: Entity,
    /// Causal depth of the revision (its edition — a Lamport timestamp).
    #[output]
    pub edition: u64,
}

impl Revision {
    /// Project the record's scalar fields; a record that does not
    /// verify projects nothing.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let Some(record) = verified(&input.of) else {
            return Vec::new();
        };
        let (Ok(issuer), Ok(authority)) = (record.issuer.parse(), record.authority.parse()) else {
            return Vec::new();
        };
        let version = record.version();
        vec![Revision {
            of: input.of.clone(),
            this: version.entity(),
            lineage: record.lineage,
            issuer,
            authority,
            edition: version.edition.value(),
        }]
    }
}

/// Projects a revision record's DAG edge: one row per parent revision,
/// each parent named by its content-derived revision entity. A genesis
/// revision (no parents) projects nothing.
#[derive(Debug, Clone, Formula)]
pub struct RevisionParent {
    /// The revision record bytes — the value of a `dialog.db/revision`
    /// fact.
    pub of: RecordBytes,
    /// The revision entity derived from the record's own contents.
    #[output]
    pub this: Entity,
    /// A parent revision's entity — one row per parent; two for a
    /// merge.
    #[output]
    pub parent: Entity,
}

impl RevisionParent {
    /// Project one row per parent; a record that does not verify
    /// projects nothing.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let Some(record) = verified(&input.of) else {
            return Vec::new();
        };
        let this = record.version().entity();
        record
            .parents
            .iter()
            .map(|parent| RevisionParent {
                of: input.of.clone(),
                this: this.clone(),
                parent: parent.entity(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base58::ToBase58 as _;
    use dialog_artifacts::history::{Edition, Origin, REVISION_RECORD_FORMAT, Version};
    use ed25519_dalek::Signer as _;

    fn did_key_of(key: &ed25519_dalek::SigningKey) -> String {
        let mut bytes = vec![0xed, 0x01];
        bytes.extend_from_slice(key.verifying_key().as_bytes());
        format!("did:key:z{}", bytes.to_base58())
    }

    fn signed_record(parents: Vec<Version>) -> (RevisionRecord, ed25519_dalek::SigningKey) {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7u8; 32]);
        let mut record = RevisionRecord {
            format: REVISION_RECORD_FORMAT,
            lineage: "test:lineage".parse().expect("valid entity"),
            issuer: did_key_of(&key),
            authority: did_key_of(&key),
            parents,
            skips: Vec::new(),
            signature: Vec::new(),
        };
        record.signature = key
            .sign(&record.payload().expect("record encodes"))
            .to_bytes()
            .to_vec();
        (record, key)
    }

    #[test]
    fn it_projects_a_verified_record() {
        let parent = Version::new(Origin::from([3u8; 32]), Edition::new(4));
        let (record, _) = signed_record(vec![parent]);
        let of = RecordBytes(record.to_bytes().expect("record encodes"));

        let rows = Revision::compute(RevisionInput { of: of.clone() });
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].this, record.version().entity());
        assert_eq!(rows[0].lineage, record.lineage);
        assert_eq!(rows[0].issuer.to_string(), record.issuer);
        assert_eq!(rows[0].edition, 5);

        let edges = RevisionParent::compute(RevisionParentInput { of });
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].parent, parent.entity());
    }

    #[test]
    fn it_projects_nothing_for_a_forged_record() {
        let (record, _) = signed_record(Vec::new());

        // Tamper with a signed field after signing.
        let mut forged = record.clone();
        forged.lineage = "test:elsewhere".parse().expect("valid entity");
        let of = RecordBytes(forged.to_bytes().expect("record encodes"));
        assert!(Revision::compute(RevisionInput { of: of.clone() }).is_empty());
        assert!(RevisionParent::compute(RevisionParentInput { of }).is_empty());

        // Garbage bytes project nothing rather than erroring.
        let garbage = RecordBytes(vec![0xff, 0x00, 0x13]);
        assert!(Revision::compute(RevisionInput { of: garbage }).is_empty());
    }
}
