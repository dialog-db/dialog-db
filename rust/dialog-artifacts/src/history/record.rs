use std::str::FromStr;
use std::str::from_utf8;

use serde::{Deserialize, Serialize};

use crate::artifacts::decode_value;
use crate::key::varkey::{self, ValuePayload};
use crate::{
    Attribute, Datum, DialogArtifactsError, Entity, Key, State, coverage_key, history_key,
};

use super::{Cause, Claim, Version};

/// A [`Claim`] paired with its polarity, as stored in the history index.
///
/// Retractions are claims like any other and participate in the same cause
/// lineage — a retraction's cause identifies the claim(s) whose assertion it
/// withdraws — but the history index must remember which of the two a claim
/// was in order to reconstruct state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Record {
    /// The claim asserts its value
    Assert(Claim),
    /// The claim withdraws a previous assertion of its value
    Retract(Claim),
}

impl Record {
    /// The [`Claim`] carried by this record, regardless of polarity
    pub fn claim(&self) -> &Claim {
        match self {
            Record::Assert(claim) => claim,
            Record::Retract(claim) => claim,
        }
    }

    /// Whether this record asserts (rather than retracts) its claim
    pub fn is_assertion(&self) -> bool {
        matches!(self, Record::Assert(_))
    }

    /// The tree entry storing this record in the history region of the
    /// artifact tree: the [`history_key`] for the claim at `version`, and
    /// the claim in [`Datum`] form with the supersedes/retraction fields
    /// carrying what [`Claim::cause`] and the record polarity express.
    pub fn into_entry(self, version: &Version) -> (Key, State<Datum>) {
        let retraction = !self.is_assertion();
        let claim = match self {
            Record::Assert(claim) | Record::Retract(claim) => claim,
        };
        let key = history_key(version, &claim.of, &claim.the, &claim.is);
        let datum = Datum {
            cause: None,
            blob: None,
            version: Some(*version),
            collapsed: Vec::new(),
            supersedes: claim.cause.versions().to_vec(),
            retraction,
        };
        (key, State::Added(datum))
    }

    /// The compact coverage entry mirroring this record, when it covers
    /// anything: the [`coverage_key`] for the claim at `version`, and a
    /// [`Datum`] carrying only what a repair pass needs — the entity, the
    /// attribute, the superseded versions, and the polarity. No value
    /// bytes: coverage matches claims by version, never by content, and
    /// keeping the region value-free is what makes "every deletion or
    /// replacement since the sync base" a cheap scoped diff. `None` for
    /// records that cover nothing (plain assertions, genesis
    /// retractions).
    pub fn coverage_entry(&self, version: &Version) -> Option<(Key, State<Datum>)> {
        let claim = self.claim();
        if claim.cause.versions().is_empty() {
            return None;
        }
        let key = coverage_key(version, &claim.of, &claim.the, &claim.is);
        let datum = Datum {
            cause: None,
            blob: None,
            version: Some(*version),
            collapsed: Vec::new(),
            supersedes: claim.cause.versions().to_vec(),
            retraction: !self.is_assertion(),
        };
        Some((key, State::Added(datum)))
    }

    /// Reconstruct a record from its stored key and [`Datum`], for a key
    /// whose value is stored INLINE.
    ///
    /// A convenience over [`Record::try_from_key_datum_with_value`] with no
    /// spilled bytes; a spilled key errors, because the raw value bytes live
    /// in a separate archive block the caller must fetch.
    pub fn try_from_key_datum(key: &Key, datum: Datum) -> Result<Record, DialogArtifactsError> {
        Self::try_from_key_datum_with_value(key, datum, None)
    }

    /// Reconstruct a record from its stored key, its [`Datum`], and, for a
    /// *spilled* key, the raw value bytes fetched from the archive block
    /// store.
    ///
    /// The key is lossless (see `key::history`), so entity, attribute and
    /// value all come from it; the payload carries only the lineage the key
    /// does not encode (the superseded versions and the record's polarity).
    /// This mirrors how a fact reconstructs through
    /// [`Artifact::from_key_datum_with_value`](crate::Artifact::from_key_datum_with_value):
    /// pass `spilled = None` for inline keys and `Some(bytes)` (the block
    /// fetched by the key's 32-byte reference) for spilled ones.
    pub fn try_from_key_datum_with_value(
        key: &Key,
        datum: Datum,
        spilled: Option<Vec<u8>>,
    ) -> Result<Record, DialogArtifactsError> {
        let parts = varkey::parse_key(key.as_ref()).ok_or_else(|| {
            DialogArtifactsError::InvalidKey("history key did not parse".to_string())
        })?;
        let of = Entity::from_str(from_utf8(&parts.entity).map_err(|error| {
            DialogArtifactsError::InvalidEntity(format!("entity key is not UTF-8: {error}"))
        })?)?;
        let the = Attribute::from_str(from_utf8(&parts.attribute).map_err(|error| {
            DialogArtifactsError::InvalidAttribute(format!("attribute key is not UTF-8: {error}"))
        })?)?;
        let is = match &parts.value {
            ValuePayload::Spilled { .. } => {
                let bytes = spilled.ok_or_else(|| {
                    DialogArtifactsError::InvalidKey(
                        "history record value spilled; resolve it through the archive".to_string(),
                    )
                })?;
                crate::Value::try_from((parts.value_type, bytes))?
            }
            ValuePayload::Inline(payload) => {
                decode_value(parts.value_type, payload)
                    .ok_or_else(|| {
                        DialogArtifactsError::InvalidKey(
                            "history key value payload did not decode".to_string(),
                        )
                    })?
                    .0
            }
        };
        let claim = Claim {
            the,
            of,
            is,
            cause: Cause::new(datum.supersedes),
        };
        Ok(if datum.retraction {
            Record::Retract(claim)
        } else {
            Record::Assert(claim)
        })
    }
}
