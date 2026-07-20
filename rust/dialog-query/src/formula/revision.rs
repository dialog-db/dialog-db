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

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use dialog_artifacts::history::VersionExt as _;
use dialog_artifacts::history::{RevisionRecord, verify_issuer_signature};
use dialog_common::Blake3Hash;

use crate::formula::Input;
use crate::types::RecordBytes;
use crate::{Entity, Formula};

/// Entries the verified-record memo holds before it resets. Entries are
/// re-verifiable, so an occasional refill only costs the verification it
/// would have paid anyway.
const VERIFIED_MEMO_BOUND: usize = 4096;

/// Decode and verify the [`RevisionRecord`] carried by `of`, or `None`
/// if the record is malformed or does not vouch for itself.
///
/// Memoized by the blake3 of the record bytes: Ed25519 verification
/// dominates projecting a record, and the fixpoint evaluator re-applies
/// these formulas to the same records across delta rounds — an ancestry
/// closure over n revisions otherwise pays O(n · rounds) verifications.
/// Record bytes are immutable, so an entry (positive or negative) never
/// invalidates.
///
/// Only the signature is checked here: the slot binding
/// [`RevisionRecord::verify`] adds compares the derived version against
/// the slot a record was FOUND at, and a formula holds only the bytes —
/// replay rejection happens in the join on `this` instead (see the
/// module docs).
fn verified(of: &RecordBytes) -> Option<RevisionRecord> {
    static MEMO: OnceLock<Mutex<HashMap<[u8; 32], Option<RevisionRecord>>>> = OnceLock::new();
    let memo = MEMO.get_or_init(|| Mutex::new(HashMap::new()));
    let key = *Blake3Hash::hash(&of.0).as_bytes();
    if let Some(hit) = memo.lock().expect("verified-record memo lock").get(&key) {
        return hit.clone();
    }
    let record = (|| {
        let record = RevisionRecord::try_from_bytes(&of.0).ok()?;
        verify_issuer_signature(&record.issuer, &record.payload().ok()?, &record.signature)
            .ok()?;
        Some(record)
    })();
    let mut memo = memo.lock().expect("verified-record memo lock");
    if memo.len() >= VERIFIED_MEMO_BOUND {
        memo.clear();
    }
    memo.insert(key, record.clone());
    record
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
    /// projects nothing. Parents are deduplicated: a duplicate entry in
    /// a (self-signed) record is one DAG edge, not two rows — and a
    /// hostile record repeating one parent must not multiply output.
    pub fn compute(input: Input<Self>) -> Vec<Self> {
        let Some(record) = verified(&input.of) else {
            return Vec::new();
        };
        let this = record.version().entity();
        let mut seen = std::collections::HashSet::new();
        record
            .parents
            .iter()
            .filter(|parent| seen.insert(**parent))
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

    /// A multi-row formula evaluated with an output already bound is a
    /// membership test: the row whose output agrees must survive even
    /// when sibling rows conflict. Regression for the default
    /// `Formula::resolve`, which used to abort the whole batch on the
    /// first conflicting write — a merge record (two parents) queried
    /// with `parent` bound projected nothing instead of the matching
    /// edge, which broke the recursive ancestor closure at every merge.
    #[test]
    fn it_keeps_the_matching_row_when_a_sibling_conflicts() {
        use crate::Value;
        use crate::formula::query::FormulaQuery;
        use crate::selection::Match;
        use crate::term::Term;
        use crate::types::Any;

        let first = Version::new(Origin::from([3u8; 32]), Edition::new(4));
        let second = Version::new(Origin::from([5u8; 32]), Edition::new(4));
        let (record, _) = signed_record(vec![first, second]);
        let of = RecordBytes(record.to_bytes().expect("record encodes"));

        let query: FormulaQuery = RevisionParentQuery {
            of: Term::var("record"),
            this: Term::var("this"),
            parent: Term::var("parent"),
        }
        .into();

        let mut matched = Match::new();
        matched
            .bind(&Term::<Any>::var("record"), Value::Record(of.0.clone()))
            .expect("record binds");
        matched
            .bind(&Term::<Any>::var("parent"), Value::Entity(first.entity()))
            .expect("parent binds");

        let rows = query.expand(matched).expect("expansion succeeds");
        assert_eq!(
            rows.len(),
            1,
            "the row agreeing with the bound parent survives its conflicting sibling"
        );
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
