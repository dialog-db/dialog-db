//! Generic serde helper for types that can be represented as bytes.
//!
//! Use with `#[serde(with = "dialog_common::as_bytes")]` for types that implement
//! `AsRef<[u8]>` and `TryFrom<Vec<u8>>`.

use serde::{Deserialize, Deserializer, Serializer};
use std::fmt::Display;

/// Serialize a value as raw bytes.
pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
where
    T: AsRef<[u8]>,
    S: Serializer,
{
    serializer.serialize_bytes(value.as_ref())
}

/// Deserialize raw bytes into a value.
pub fn deserialize<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: TryFrom<Vec<u8>>,
    <T as TryFrom<Vec<u8>>>::Error: Display,
    D: Deserializer<'de>,
{
    let buf: serde_bytes::ByteBuf = serde_bytes::ByteBuf::deserialize(deserializer)?;
    T::try_from(buf.into_vec()).map_err(serde::de::Error::custom)
}
