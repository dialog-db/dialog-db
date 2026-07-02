use std::collections::BTreeMap;
use std::str::FromStr;

use async_trait::async_trait;

use crate::{Attribute, DialogArtifactsError, Entity, Value};

use super::{Claim, History, HistoryKey, REVISION_ATTRIBUTE, Revision, Version};

/// An in-memory [`History`] index, mapping
/// `/edition/origin/entity/attribute/value_hash` keys to [`Claim`]s.
///
/// This is a reference implementation used to exercise the version control
/// machinery; a durable implementation would be backed by the same prolly
/// tree infrastructure as the EAV indexes.
#[derive(Clone, Debug, Default)]
pub struct MemoryHistory {
    claims: BTreeMap<HistoryKey, Claim>,
}

impl MemoryHistory {
    /// Record a claim produced by the revision identified by `version`
    pub fn record(&mut self, version: &Version, claim: Claim) {
        self.claims.insert(HistoryKey::new(version, &claim), claim);
    }

    /// Record the lineage claim for the given revision: a claim under the
    /// repository DID whose value is the revision's content-addressed entity
    /// and whose cause lists the parent revision versions
    pub fn record_revision(&mut self, revision: &Revision) -> Result<(), DialogArtifactsError> {
        let claim = Claim {
            the: Attribute::from_str(REVISION_ATTRIBUTE)?,
            of: revision.subject().clone(),
            is: Value::Entity(revision.entity()?),
            cause: revision.cause().clone(),
        };
        self.record(&revision.version(), claim);
        Ok(())
    }

    /// The recorded revision lineage claims for the repository identified by
    /// `subject`, in a total order consistent with causality (ascending by
    /// version; no revision appears before one of its ancestors)
    pub fn revisions(&self, subject: &Entity) -> Vec<(Version, Claim)> {
        let subject = subject.key_bytes();
        let attribute = Attribute::from_str(REVISION_ATTRIBUTE)
            .expect("the revision attribute is well-formed");
        self.claims
            .iter()
            .filter(|(key, _)| {
                key.entity_bytes() == subject.as_slice()
                    && key.attribute_bytes() == attribute.key_bytes().as_slice()
            })
            .map(|(key, claim)| (key.version(), claim.clone()))
            .collect()
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

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
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

    async fn revision_at(&self, version: &Version) -> Result<Vec<Claim>, DialogArtifactsError> {
        let attribute = Attribute::from_str(REVISION_ATTRIBUTE)?;
        let (min, max) = HistoryKey::version_range(version);
        Ok(self
            .claims
            .range(min..=max)
            .filter(|(key, _)| key.attribute_bytes() == attribute.key_bytes().as_slice())
            .map(|(_, claim)| claim.clone())
            .collect())
    }
}
