use crate::{DataType, Entity, Reference, XQueryError, make_reference};

use super::Attribute;

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Value {
    Null,
    Bytes(Vec<u8>),
    Entity(Entity),
    Boolean(bool),
    String(String),
    UnsignedInt(u128),
    SignedInt(i128),
    Float(f64),
    Structured(Vec<u8>),
    Symbol(String),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Null,
            Value::Bytes(_) => DataType::Bytes,
            Value::Entity(_) => DataType::Entity,
            Value::Boolean(_) => DataType::Boolean,
            Value::String(_) => DataType::String,
            Value::UnsignedInt(_) => DataType::UnsignedInt,
            Value::SignedInt(_) => DataType::SignedInt,
            Value::Float(_) => DataType::Float,
            Value::Structured(_) => DataType::Structured,
            Value::Symbol(_) => DataType::Symbol,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Value::Null => vec![],
            Value::Bytes(bytes) => bytes.to_owned(),
            Value::Entity(entity) => entity.as_ref().to_vec(),
            Value::Boolean(value) => vec![u8::from(*value)],
            Value::String(string) => string.as_bytes().to_vec(),
            Value::UnsignedInt(value) => value.to_le_bytes().to_vec(),
            Value::SignedInt(value) => value.to_le_bytes().to_vec(),
            Value::Float(value) => value.to_le_bytes().to_vec(),
            Value::Structured(value) => value.to_owned(),
            Value::Symbol(value) => value.as_bytes().to_vec(),
        }
    }

    pub fn to_tagged_bytes(&self) -> Vec<u8> {
        [vec![u8::from(self.data_type())], self.to_bytes()].concat()
    }

    pub fn as_unsigned_int(&self) -> Option<u128> {
        match self {
            Value::UnsignedInt(unsigned_int) => Some(*unsigned_int),
            _ => None,
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Bytes(value)
    }
}

impl From<Entity> for Value {
    fn from(value: Entity) -> Self {
        Value::Entity(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

impl From<u128> for Value {
    fn from(value: u128) -> Self {
        Value::UnsignedInt(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Value::UnsignedInt(value.into())
    }
}

impl From<i128> for Value {
    fn from(value: i128) -> Self {
        Value::SignedInt(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Value::SignedInt(value.into())
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(value.into())
    }
}

impl From<Attribute> for Value {
    fn from(value: Attribute) -> Self {
        Value::Symbol(format!("{}/{}", value.namespace, value.predicate))
    }
}

impl From<&Value> for DataType {
    fn from(value: &Value) -> Self {
        value.data_type()
    }
}

impl From<Value> for DataType {
    fn from(value: Value) -> Self {
        Self::from(&value)
    }
}

impl From<Value> for Reference {
    fn from(value: Value) -> Self {
        Reference::from(&value)
    }
}

impl From<&Value> for Reference {
    fn from(value: &Value) -> Self {
        match value {
            Value::Entity(entity) => entity.clone().into(),
            _ => make_reference(value.to_bytes()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct TaggedBytes(Vec<u8>);

impl TaggedBytes {
    pub fn new(tag: DataType, bytes: Vec<u8>) -> Self {
        Self([vec![tag.into()], bytes].concat())
    }
    pub fn data_type(&self) -> DataType {
        self.0.first().map(DataType::from).unwrap_or_default()
    }

    pub fn untagged_bytes(&self) -> &[u8] {
        if !self.0.is_empty() {
            &self.0[1..]
        } else {
            &[]
        }
    }
}

impl TryFrom<TaggedBytes> for Value {
    type Error = XQueryError;

    fn try_from(value: TaggedBytes) -> Result<Self, Self::Error> {
        Ok(match value.data_type() {
            DataType::Null => Value::Null,
            DataType::Bytes => Value::Bytes(value.untagged_bytes().to_vec()),
            DataType::Entity => {
                Value::Entity(Entity::from(Reference::try_from(value.untagged_bytes())?))
            }
            DataType::Boolean => Value::Boolean(
                value
                    .untagged_bytes()
                    .first()
                    .map(|value| *value != 0)
                    .unwrap_or_default(),
            ),
            DataType::String => Value::String(
                String::from_utf8(value.untagged_bytes().to_vec())
                    .map_err(|error| XQueryError::InvalidRawValue(format!("{error}")))?,
            ),
            DataType::UnsignedInt => Value::UnsignedInt(u128::from_le_bytes(
                value
                    .untagged_bytes()
                    .try_into()
                    .map_err(|error| XQueryError::InvalidRawValue(format!("{error}")))?,
            )),
            DataType::SignedInt => Value::SignedInt(i128::from_le_bytes(
                value
                    .untagged_bytes()
                    .try_into()
                    .map_err(|error| XQueryError::InvalidRawValue(format!("{error}")))?,
            )),
            DataType::Float => Value::Float(f64::from_le_bytes(
                value
                    .untagged_bytes()
                    .try_into()
                    .map_err(|error| XQueryError::InvalidRawValue(format!("{error}")))?,
            )),
            DataType::Structured => Value::Bytes(value.untagged_bytes().to_vec()),
            DataType::Symbol => Value::Symbol(
                String::from_utf8(value.untagged_bytes().to_vec())
                    .map_err(|error| XQueryError::InvalidRawValue(format!("{error}")))?,
            ),
        })
    }
}

impl From<Value> for TaggedBytes {
    fn from(value: Value) -> Self {
        TaggedBytes::new(value.data_type(), value.to_bytes())
    }
}
