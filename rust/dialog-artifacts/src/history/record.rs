use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{
    Attribute, Datum, DialogArtifactsError, Entity, Key, State, Value, ValueDataType, history_key,
    make_reference,
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
        let value_type = claim.is.data_type();
        let value = claim.is.to_bytes();
        let key = history_key(
            version,
            &claim.of,
            &claim.the,
            value_type,
            &make_reference(&value),
        );
        let datum = Datum {
            entity: claim.of.to_string(),
            attribute: claim.the.to_string(),
            value_type: value_type.into(),
            value,
            cause: None,
            version: Some(*version),
            supersedes: claim.cause.versions().to_vec(),
            retraction,
        };
        (key, State::Added(datum))
    }

    /// Reconstruct a record from its stored [`Datum`] form
    pub fn try_from_datum(datum: Datum) -> Result<Record, DialogArtifactsError> {
        let claim = Claim {
            the: Attribute::from_str(&datum.attribute)?,
            of: Entity::from_str(&datum.entity)?,
            is: Value::try_from((ValueDataType::from(datum.value_type), datum.value))?,
            cause: Cause::new(datum.supersedes),
        };
        Ok(if datum.retraction {
            Record::Retract(claim)
        } else {
            Record::Assert(claim)
        })
    }
}
