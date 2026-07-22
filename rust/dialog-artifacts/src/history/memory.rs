use std::collections::BTreeMap;

use crate::{Attribute, DialogArtifactsError, Entity};

use super::{
    Claim, History, HistoryKey, REVISION_RECORD_FORMAT, Revision, RevisionRecord, Version,
};

/// An in-memory [`History`] index, mapping
/// `/edition/origin/entity/attribute/value_hash` keys to [`Claim`]s.
///
/// This is a reference implementation used to exercise the version control
/// machinery in unit tests; [`TreeHistory`](super::TreeHistory) is the
/// durable implementation, reading the history region of the artifact tree.
#[derive(Clone, Debug, Default)]
pub struct MemoryHistory {
    claims: BTreeMap<HistoryKey, Claim>,
    records: BTreeMap<Version, RevisionRecord>,
}

impl MemoryHistory {
    /// Record a claim produced by the revision identified by `version`
    pub fn record(&mut self, version: &Version, claim: Claim) {
        self.claims.insert(HistoryKey::new(version, &claim), claim);
    }

    /// Record the [`RevisionRecord`] for the given revision, deriving the
    /// attribution and parents from the signed revision itself
    pub fn record_revision(&mut self, revision: &Revision) -> Result<(), DialogArtifactsError> {
        let record = RevisionRecord {
            format: REVISION_RECORD_FORMAT,
            branch: revision.subject().clone(),
            issuer: revision.issuer().to_string(),
            authority: revision.authority().to_string(),
            parents: revision.cause().versions().to_vec(),
            skips: Vec::new(),
            // A test double stores records as-is and never verifies them,
            // so the signature stays empty.
            signature: Vec::new(),
        };
        self.records.insert(revision.version(), record);
        Ok(())
    }

    /// Attach a skip table to an already-recorded revision
    pub fn record_skips(&mut self, version: &Version, skips: Vec<Version>) {
        if let Some(record) = self.records.get_mut(version) {
            record.skips = skips;
        }
    }

    /// The number of recorded claims
    pub fn len(&self) -> usize {
        self.claims.len()
    }

    /// Whether the index is empty
    pub fn is_empty(&self) -> bool {
        self.claims.is_empty()
    }
}

impl History for MemoryHistory {
    async fn claims_at(
        &self,
        version: &Version,
        of: &Entity,
        the: &Attribute,
    ) -> Result<Vec<Claim>, DialogArtifactsError> {
        let (min, max) = HistoryKey::claim_range(version, of, the);
        Ok(self
            .claims
            .range(min..=max)
            .map(|(_, claim)| claim.clone())
            .collect())
    }

    async fn revision_record(
        &self,
        version: &Version,
    ) -> Result<Option<RevisionRecord>, DialogArtifactsError> {
        Ok(self.records.get(version).cloned())
    }
}
