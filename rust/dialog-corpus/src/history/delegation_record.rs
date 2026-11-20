use crate::reads::*;
use crate::tree::{Corpus, CorpusExt as _};
use crate::{
    ArchiveDelta, Datum, DeltaOverlay, DialogCorpusError, Entity, Fact, Relation, State, Value,
    key::{EncodedValue, fact_index_keys_with},
};
use dialog_search_tree::Manifest;

use super::RecordEntries;

/// The attribute a [`DelegationRecord`] is stored under. In the reserved
/// `dialog.` namespace, so only the internal record path writes it.
pub const DELEGATION_ATTRIBUTE: &str = "dialog.db/delegation";

/// A profile's delegation to a session key, stored on the session key so
/// the revision records that key signs can be attributed to the profile.
///
/// A session signs every revision it mints under one delegation, so the
/// delegation is written once per tree rather than with every record: one
/// fact on the session key's own entity (its DID), under
/// [`DELEGATION_ATTRIBUTE`], valued with the encoded UCAN. A record needs
/// no reference to it: the delegation names its audience and is signed by
/// its issuer, so finding one on the record's issuer that binds the
/// record's authority (see
/// [`RevisionRecord::attests`](super::RevisionRecord::attests)) proves the
/// attribution whatever put it there. The fact is written without a
/// version, so every branch that records it writes the identical entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelegationRecord {
    of: Entity,
    bytes: Vec<u8>,
}

impl DelegationRecord {
    /// The delegation encoded as `bytes` (DAG-CBOR), to the session key
    /// `of` (its DID, as an entity).
    pub fn new(of: Entity, bytes: Vec<u8>) -> Self {
        Self { of, bytes }
    }

    /// The session key the delegation is stored on.
    pub fn of(&self) -> &Entity {
        &self.of
    }

    /// The encoded delegation.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Whether `tree` already holds this delegation, reading through
    /// `delta` (where a tree's newest nodes may still be staged) before
    /// `store`. A tree that holds it is not written again.
    pub async fn is_in<S>(
        &self,
        tree: &Corpus,
        store: &S,
        delta: &ArchiveDelta,
    ) -> Result<bool, DialogCorpusError>
    where
        S: Provider<nodes::Read<CorpusNode>>
            + Provider<nodes::Preload<CorpusNode>>
            + Provider<blob::Load>
            + ConditionalSend
            + ConditionalSync
            + Clone,
    {
        let the = Relation::try_from(DELEGATION_ATTRIBUTE.to_string())?;
        let staged = DeltaOverlay::new(delta, store);
        Ok(tree
            .select_record(staged, &self.of, &the)
            .await?
            .iter()
            .any(|artifact| matches!(&artifact.is, Value::Record(bytes) if *bytes == self.bytes)))
    }

    /// The fact carrying this delegation.
    pub fn to_fact(&self) -> Result<Fact, DialogCorpusError> {
        Ok(Fact {
            the: Relation::try_from(DELEGATION_ATTRIBUTE.to_string())?,
            of: self.of.clone(),
            is: Value::Record(self.bytes.clone()),
            cause: None,
        })
    }

    /// The tree entries carrying this delegation: the entity- and
    /// attribute-ordered keys, as for a revision record, built under the
    /// target tree's `manifest`.
    pub fn entries(&self, manifest: &Manifest) -> Result<RecordEntries, DialogCorpusError> {
        let artifact = self.to_fact()?;
        let encoded = EncodedValue::new(&artifact.is, manifest);
        let (entity_key, attribute_key, _) =
            fact_index_keys_with(&artifact, encoded.value_type, encoded.payload);
        let added = State::Added(Datum::for_fact(&artifact));
        Ok(RecordEntries {
            entries: vec![(entity_key, added.clone()), (attribute_key, added)],
            spills: encoded.spill.into_iter().collect(),
        })
    }
}
