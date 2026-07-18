use serde::{Deserialize, Serialize};

use crate::{Attribute, Entity, Value};

use super::Cause;

/// Serialize a [`Value`] as a `(value_type, bytes)` pair — the same
/// type-tagged shape [`Datum`](crate::Datum) uses — rather than serde's
/// untagged representation, which cannot round-trip every variant
/// faithfully (an unsigned integer resurfaces as a float).
mod value_codec {
    use crate::{Value, ValueDataType};
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(value: &Value, serializer: S) -> Result<S::Ok, S::Error> {
        let data_type: u8 = value.data_type().into();
        (data_type, serde_bytes::Bytes::new(&value.to_bytes())).serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
        let (data_type, bytes): (u8, serde_bytes::ByteBuf) =
            Deserialize::deserialize(deserializer)?;
        Value::try_from((ValueDataType::from(data_type), bytes.into_vec()))
            .map_err(D::Error::custom)
    }
}

/// A claim as recorded in the history index.
///
/// Structurally this is an [`Artifact`](crate::Artifact) whose `cause` is a
/// set of [`Version`](super::Version)s rather than a single content hash:
/// the cause identifies the prior claims on the same `(entity, attribute)`
/// that this claim supersedes, analogous to how a git commit records which
/// commits it builds on, but scoped to individual fact lineages.
///
/// Retractions are claims like any other and participate in the same lineage:
/// a retraction's cause identifies the claim(s) whose assertion it withdraws.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    /// The attribute (predicate) of the claim
    pub the: Attribute,
    /// The entity (subject) of the claim
    pub of: Entity,
    /// The value (object) of the claim
    #[serde(with = "value_codec")]
    pub is: Value,
    /// The versions of the prior claims on the same `(of, the)` superseded by
    /// this claim; empty on first write
    pub cause: Cause,
}
