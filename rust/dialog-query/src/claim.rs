//! Read-side claim type representing a stored EAV datum.

use crate::artifact::Type;
pub use crate::artifact::{Artifact, ArtifactsAttribute, Cause, Entity, Value};
use crate::attribute::The;
use crate::concept::Conclusion;
use dialog_artifacts::{RecordError, RecordFormat};
use serde::{Deserialize, Serialize};

/// A claim represents a stored EAV datum with full metadata.
///
/// This is the result type for relation queries. It carries the attribute
/// identifier alongside the entity-value-cause data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Claim {
    /// The claim identifier (e.g., "user/name")
    pub the: The,
    /// The entity (subject)
    pub of: Entity,
    /// The value (object)
    pub is: Value,
    /// The cause (provenance hash) of this claim
    pub cause: Cause,
}

impl Claim {
    /// Get the attribute for this claim
    pub fn the(&self) -> &The {
        &self.the
    }

    /// Get the domain of this claim's attribute
    pub fn domain(&self) -> &str {
        self.the.domain()
    }

    /// Get the name of this claim's attribute
    pub fn name(&self) -> &str {
        self.the.name()
    }

    /// Get the entity of this claim
    pub fn of(&self) -> &Entity {
        &self.of
    }

    /// Get the value of this claim
    pub fn is(&self) -> &Value {
        &self.is
    }

    /// Get the cause (provenance hash) of this claim
    pub fn cause(&self) -> &Cause {
        &self.cause
    }
}

impl Conclusion for Claim {
    /// The claim's subject entity. Lets claim-valued query results
    /// participate in subject-keyed machinery (e.g. per-entity
    /// incremental maintenance) uniformly with concept conclusions.
    fn this(&self) -> &Entity {
        &self.of
    }
}

impl From<&Artifact> for Claim {
    fn from(artifact: &Artifact) -> Self {
        Claim {
            the: The::from(artifact.the.clone()),
            of: artifact.of.clone(),
            is: artifact.is.clone(),
            cause: artifact.cause.clone().unwrap_or(Cause([0; 32])),
        }
    }
}

/// The byte encoding of a [`Claim`] as a [`RecordFormat`]: a DAG-CBOR tuple
/// of `(the, of, value type, value bytes, cause)`.
///
/// The value slot cannot ride [`Value`]'s own serde: that representation is
/// untagged, so byte-shaped variants collapse on the way back in (`Record`
/// bytes decode as `Bytes`, a `Symbol` string as `String`). Carrying the
/// [`Type`] tag beside the raw value bytes reuses the storage layer's
/// `(ValueDataType, bytes)` convention, which round-trips every variant.
type ClaimEnvelope = (The, Entity, Type, Vec<u8>, Cause);

impl RecordFormat for Claim {
    fn decode(bytes: &[u8]) -> Result<Self, RecordError> {
        let (the, of, is_type, is_bytes, cause): ClaimEnvelope =
            serde_ipld_dagcbor::from_slice(bytes)
                .map_err(|error| RecordError::Decode(error.to_string()))?;
        let is = Value::try_from((is_type, is_bytes))
            .map_err(|error| RecordError::Decode(error.to_string()))?;
        Ok(Claim { the, of, is, cause })
    }

    fn encode(&self) -> Result<Vec<u8>, RecordError> {
        let envelope = (
            &self.the,
            &self.of,
            self.is.data_type(),
            self.is.to_bytes(),
            &self.cause,
        );
        serde_ipld_dagcbor::to_vec(&envelope)
            .map_err(|error| RecordError::Encode(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dialog_artifacts::Record;
    use std::str::FromStr;

    fn claim_with(is: Value) -> Claim {
        Claim {
            the: "person/name".parse().unwrap(),
            of: Entity::new().unwrap(),
            is,
            cause: Cause([7; 32]),
        }
    }

    #[dialog_common::test]
    fn it_round_trips_through_record_format() {
        for is in [
            Value::String("Alice".into()),
            Value::Bytes(vec![1, 2, 3]),
            Value::Entity(Entity::new().unwrap()),
            Value::Boolean(true),
            Value::UnsignedInt(42),
            Value::SignedInt(-42),
            Value::Float(1.5),
        ] {
            let claim = claim_with(is);
            let decoded = Claim::decode(&claim.encode().unwrap()).unwrap();
            assert_eq!(decoded, claim);
        }
    }

    /// `Record` and `Symbol` values are the reason the envelope carries a
    /// type tag: their bare bytes are indistinguishable from `Bytes` and
    /// `String` under `Value`'s untagged serde.
    #[dialog_common::test]
    fn it_round_trips_byte_shaped_value_types() {
        let record = claim_with(Value::Record(Record::from(vec![9, 9, 9])));
        let decoded = Claim::decode(&record.encode().unwrap()).unwrap();
        assert!(matches!(decoded.is, Value::Record(_)));
        assert_eq!(decoded, record);

        let symbol = claim_with(Value::Symbol(
            ArtifactsAttribute::from_str("person/name").unwrap(),
        ));
        let decoded = Claim::decode(&symbol.encode().unwrap()).unwrap();
        assert!(matches!(decoded.is, Value::Symbol(_)));
        assert_eq!(decoded, symbol);
    }

    #[dialog_common::test]
    fn it_realizes_from_hydrated_record_bytes() {
        let claim = claim_with(Value::String("Alice".into()));
        let record = Record::from_format(claim.clone()).unwrap();

        // A record hydrated from bare storage bytes decodes the same claim.
        let hydrated = Record::from(record.as_bytes().to_vec());
        assert_eq!(*hydrated.realize::<Claim>().unwrap(), claim);
    }

    #[dialog_common::test]
    fn it_rejects_undecodable_bytes() {
        assert!(matches!(
            Claim::decode(&[0xff, 0x00, 0x13]),
            Err(RecordError::Decode(_))
        ));
    }
}
