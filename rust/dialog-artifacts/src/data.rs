use std::io::{Cursor, Read};

use dialog_prolly_tree::ValueType;
use dialog_storage::Blake3Hash;
use serde::{Deserialize, Serialize};

use crate::{Cause, DialogArtifactsError, HASH_SIZE, Value, ValueDataType, make_reference};

#[cfg(doc)]
use crate::{Artifacts, Attribute, Entity};

/// The primitive representation of an [`Entity`]: 32 bytes
pub type RawEntity = [u8; 32];
/// The primitive representation of a [`Value`]: a buffer of bytes
pub type RawValue = Vec<u8>;
/// The primitive representation of [`Attribute`]: a UTF-8 string
pub type RawAttribute = String;

/// An [`EntityDatum`] is the layout of data stored in the value index of
/// [`Artifacts`]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EntityDatum {}

impl ValueType for EntityDatum {}

/// A [`ValueDatum`] is the layout of data stored in the entity and attribute
/// indexes of [`Artifacts`]
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "Vec<u8>", into = "Vec<u8>")]
pub struct ValueDatum {
    /// The raw representation of the [`Value`] asscoiated with this [`ValueDatum`]
    raw_value: RawValue,
    cause: Option<Cause>,

    // TODO: We automatically hash values when the `ValueDatum` is created. Ideally
    // we would only compute the hash lazily if it is requested (and then memoize it).
    reference: Blake3Hash,
}

impl ValueDatum {
    /// Initialize a new [`ValueDatum`] from the [`Value`] and an optional
    /// [`Cause`] (causal antecedent)
    pub fn new(value: Value, cause: Option<Cause>) -> Self {
        let value = value.to_bytes();
        Self {
            reference: make_reference(&value),
            raw_value: value,
            cause,
        }
    }

    /// Get the [`RawValue`] of this [`ValueDatum`]
    pub fn raw_value(&self) -> &RawValue {
        &self.raw_value
    }

    /// Get the [`Cuase`] of this [`ValueDatum`], if any
    pub fn cause(&self) -> Option<&Cause> {
        self.cause.as_ref()
    }

    /// The hash reference that corresponds to this [`Value`]
    pub fn reference(&self) -> &Blake3Hash {
        &self.reference
    }

    /// Decompose the [`ValueDatum`] into a [`Value`] and [`Cause`] (if one is
    /// assigned)
    pub fn into_value_and_cause(
        self,
        data_type: ValueDataType,
    ) -> Result<(Value, Option<Cause>), DialogArtifactsError> {
        Ok((Value::try_from((data_type, self.raw_value))?, self.cause))
    }
}

impl ValueType for ValueDatum {}

impl From<ValueDatum> for Vec<u8> {
    fn from(value: ValueDatum) -> Self {
        let mut value_length = Vec::new();
        let value_bytes = value.raw_value;

        leb128::write::unsigned(&mut value_length, value_bytes.len() as u64)
            .map_err(|error| {
                DialogArtifactsError::InvalidValue(format!(
                    "Failed to write serialized length: {error}"
                ))
            })
            .unwrap();

        [
            value_length,
            value_bytes,
            value
                .cause
                .as_ref()
                .map(|cause| (*cause).to_vec())
                .unwrap_or_default(),
        ]
        .concat()
    }
}

impl TryFrom<Vec<u8>> for ValueDatum {
    type Error = DialogArtifactsError;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        let mut cursor = Cursor::new(&bytes);
        let value_length = leb128::read::unsigned(&mut cursor).map_err(|error| {
            DialogArtifactsError::InvalidValue(format!("Unable to read value length: {error}"))
        })?;
        let mut value = vec![0; value_length as usize];

        cursor
            .read_exact(&mut value[0..value_length as usize])
            .map_err(|error| {
                DialogArtifactsError::InvalidValue(format!("Unable to read value: {error}"))
            })?;

        let mut cause = Vec::new();

        let cause = match cursor.read_to_end(&mut cause) {
            Ok(0) => None,
            Ok(length_read) if length_read == HASH_SIZE => {
                let hash: Blake3Hash = cause.try_into().map_err(|_| {
                    DialogArtifactsError::InvalidValue(
                        "Unexpected cause conversion failure".to_string(),
                    )
                })?;

                Some(Cause::from(hash))
            }
            Ok(unexpected_length) => {
                return Err(DialogArtifactsError::InvalidValue(format!(
                    "Cause had unexpected length (expected {}, got {})",
                    HASH_SIZE, unexpected_length
                )));
            }
            Err(error) => {
                return Err(DialogArtifactsError::InvalidValue(format!(
                    "Failed to read value cause: {error}"
                )));
            }
        };

        Ok(Self {
            // TODO: Should a value reference capture the cause also?
            reference: make_reference(&value),
            raw_value: value,
            cause,
        })
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{Cause, Value, make_reference};

    use super::ValueDatum;

    #[test]
    fn it_can_serialize_and_deserialize() -> Result<()> {
        let value = Value::String("Foo Bar FOO BAR".into());
        let cause = None;
        let datum = ValueDatum::new(value.clone(), cause);
        let bytes = Vec::from(datum.clone());
        let deserialized_datum = ValueDatum::try_from(bytes.clone())?;

        assert_eq!(datum, deserialized_datum);

        let cause = Some(Cause::from(make_reference(&bytes)));
        let datum_with_cause = ValueDatum::new(value, cause);
        let bytes = Vec::from(datum_with_cause.clone());
        let deserialized_datum = ValueDatum::try_from(bytes.clone())?;

        assert_eq!(datum_with_cause, deserialized_datum);

        Ok(())
    }
}
