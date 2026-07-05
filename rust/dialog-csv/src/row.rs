use base58::{FromBase58, ToBase58};
use dialog_artifacts::{Artifact, Attribute, Cause, Entity, Value};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use dialog_artifacts::DialogArtifactsError;

/// Intermediate CSV row representation.
///
/// Converts between `Artifact` and flat text fields suitable for CSV.
/// The value is split into a type column (`as`) and the raw text
/// payload (`is`), rather than using a tagged `type:value` encoding.
#[derive(Serialize, Deserialize)]
pub(crate) struct CsvRow {
    pub the: String,
    pub of: String,
    #[serde(rename = "as")]
    pub value_type: String,
    pub is: String,
    pub cause: Option<String>,
}

fn value_to_parts(value: &Value) -> (&'static str, String) {
    match value {
        Value::Bytes(bytes) => ("bytes", bytes.to_base58()),
        Value::Entity(entity) => ("entity", entity.to_string()),
        Value::Boolean(v) => ("boolean", v.to_string()),
        Value::String(s) => ("text", s.clone()),
        Value::UnsignedInt(n) => ("natural", n.to_string()),
        Value::SignedInt(n) => ("integer", n.to_string()),
        Value::Float(n) => ("float", n.to_string()),
        Value::Record(record) => ("record", record.to_base58()),
        Value::Symbol(attr) => ("attribute", attr.to_string()),
    }
}

fn parts_to_value(value_type: &str, is: &str) -> Result<Value, DialogArtifactsError> {
    fn parse_err<E: std::fmt::Debug>(e: E) -> DialogArtifactsError {
        DialogArtifactsError::InvalidValue(format!("{:?}", e))
    }

    match value_type {
        "bytes" => Ok(Value::Bytes(is.from_base58().map_err(parse_err)?)),
        "entity" => Ok(Value::Entity(Entity::from_str(is)?)),
        "boolean" => {
            Ok(Value::Boolean(bool::from_str(is).map_err(|e| {
                DialogArtifactsError::InvalidValue(e.to_string())
            })?))
        }
        "text" => Ok(Value::String(is.to_owned())),
        "natural" => {
            Ok(Value::UnsignedInt(is.parse().map_err(|e| {
                DialogArtifactsError::InvalidValue(format!("{e}"))
            })?))
        }
        "integer" => {
            Ok(Value::SignedInt(is.parse().map_err(|e| {
                DialogArtifactsError::InvalidValue(format!("{e}"))
            })?))
        }
        "float" => {
            Ok(Value::Float(is.parse().map_err(|e| {
                DialogArtifactsError::InvalidValue(format!("{e}"))
            })?))
        }
        "record" => Ok(Value::Record(is.from_base58().map_err(parse_err)?)),
        "attribute" => Ok(Value::Symbol(Attribute::from_str(is)?)),
        _ => Err(DialogArtifactsError::InvalidValue(format!(
            "unknown value type: {value_type}"
        ))),
    }
}

impl From<&Artifact> for CsvRow {
    fn from(artifact: &Artifact) -> Self {
        let (value_type, is) = value_to_parts(&artifact.is);
        Self {
            the: artifact.the.to_string(),
            of: artifact.of.to_string(),
            value_type: value_type.to_string(),
            is,
            cause: artifact.cause.as_ref().map(|c| c.to_base58()),
        }
    }
}

impl TryFrom<CsvRow> for Artifact {
    type Error = DialogArtifactsError;

    fn try_from(row: CsvRow) -> Result<Self, Self::Error> {
        let the = Attribute::from_str(&row.the)?;
        let of = Entity::from_str(&row.of)?;
        let is = parts_to_value(&row.value_type, &row.is)?;
        let cause = row
            .cause
            .filter(|s| !s.is_empty())
            .map(|s| {
                let bytes = s.from_base58().map_err(|e| {
                    DialogArtifactsError::InvalidReference(format!("invalid base58: {:?}", e))
                })?;
                let mut hash = [0u8; 32];
                let len = bytes.len().min(32);
                hash[..len].copy_from_slice(&bytes[..len]);
                Ok::<Cause, DialogArtifactsError>(Cause::from(hash))
            })
            .transpose()?;

        Ok(Artifact { the, of, is, cause })
    }
}
