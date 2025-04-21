use std::{
    io::{Cursor, Read},
    ops::Deref,
};

use dialog_prolly_tree::ValueType;

use crate::{
    Blake3Hash, Cause, DialogArtifactsError, HASH_SIZE, Value, ValueDataType, make_reference,
};

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
#[derive(Clone, Debug)]
pub struct EntityDatum {
    /// The raw representation of the [`Entity`] associated with this
    /// [`EntityDatum`]
    pub entity: RawEntity,
}

impl Deref for EntityDatum {
    type Target = RawEntity;

    fn deref(&self) -> &Self::Target {
        &self.entity
    }
}

impl ValueType for EntityDatum {
    fn serialize(&self) -> Vec<u8> {
        self.entity.to_vec()
    }
}

impl TryFrom<Vec<u8>> for EntityDatum {
    type Error = DialogArtifactsError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Ok(Self {
            entity: value.try_into().map_err(|value: Vec<u8>| {
                DialogArtifactsError::InvalidValue(format!(
                    "Wrong byte length for entity; expected {HASH_SIZE}, got {}",
                    value.len()
                ))
            })?,
        })
    }
}

/// A [`ValueDatum`] is the layout of data stored in the entity and attribute
/// indexes of [`Artifacts`]
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ValueDatum {
    /// The raw representation of the [`Value`] asscoiated with this [`ValueDatum`]
    raw_value: RawValue,
    cause: Option<Cause>,

    // TODO: We automatically hash values when the `ValueDatum` is created. Ideally
    // we would only compute the hash lazily if it is requested (and then memoize it).
    reference: Blake3Hash,
}

impl ValueDatum {
    pub fn new(value: Value, cause: Option<Cause>) -> Self {
        let value = value.to_bytes();
        Self {
            reference: make_reference(&value),
            raw_value: value,
            cause,
        }
    }

    pub fn raw_value(&self) -> &RawValue {
        &self.raw_value
    }

    pub fn cause(&self) -> Option<&Cause> {
        self.cause.as_ref()
    }

    /// The hash reference that corresponds to this [`Value`]
    pub fn reference(&self) -> &Blake3Hash {
        &self.reference
    }

    pub fn into_value_and_cause(
        self,
        data_type: ValueDataType,
    ) -> Result<(Value, Option<Cause>), DialogArtifactsError> {
        Ok((
            Value::try_from((data_type, self.raw_value))?,
            self.cause.map(|hash| Cause::from(hash)),
        ))
    }
}

impl ValueType for ValueDatum {
    fn serialize(&self) -> Vec<u8> {
        let mut value_length = Vec::new();
        let value_bytes = self.raw_value.serialize();

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
            self.cause
                .as_ref()
                .and_then(|cause| Some((*cause).to_vec()))
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
            Ok(length_read) if length_read == 0 => None,
            Ok(length_read) if length_read == HASH_SIZE => {
                let hash: Blake3Hash = cause.try_into().map_err(|_| {
                    DialogArtifactsError::InvalidValue(format!(
                        "Unexpected cause conversion failure"
                    ))
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
    use dialog_prolly_tree::ValueType;

    use crate::{Cause, Value, make_reference};

    use super::ValueDatum;

    #[test]
    fn it_can_serialize_and_deserialize() -> Result<()> {
        let value = Value::String("Foo Bar FOO BAR".into());
        let cause = None;
        let datum = ValueDatum::new(value.clone(), cause);
        let bytes = datum.serialize();
        let deserialized_datum = ValueDatum::try_from(bytes.clone())?;

        assert_eq!(datum, deserialized_datum);

        let cause = Some(Cause::from(make_reference(&bytes)));
        let datum_with_cause = ValueDatum::new(value, cause);
        let bytes = datum_with_cause.serialize();
        let deserialized_datum = ValueDatum::try_from(bytes.clone())?;

        assert_eq!(datum_with_cause, deserialized_datum);

        Ok(())
    }
}
