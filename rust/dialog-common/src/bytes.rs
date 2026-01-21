//! Bytes newtype for IPLD-compatible serialization.
//!
//! This module provides a [`Bytes`] type that wraps `Vec<u8>` and automatically
//! serializes to `Ipld::Bytes` without requiring `#[serde(with = "serde_bytes")]`
//! annotations.

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::ops::{Deref, DerefMut};

/// A newtype wrapper around `Vec<u8>` that serializes as bytes.
///
/// Unlike plain `Vec<u8>` which serializes as a sequence of integers,
/// `Bytes` serializes directly as a byte string, producing `Ipld::Bytes`
/// when used with IPLD serialization.
///
/// This is similar to `serde_bytes::ByteBuf` but with additional convenience
/// methods and `From` implementations.
///
/// # Example
///
/// ```rust
/// use dialog_common::Bytes;
///
/// let bytes = Bytes::from(vec![1, 2, 3]);
/// assert_eq!(bytes.as_slice(), &[1, 2, 3]);
///
/// // Convert back to Vec<u8>
/// let vec: Vec<u8> = bytes.into();
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct Bytes(Vec<u8>);

impl Bytes {
    /// Create a new empty `Bytes`.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Create `Bytes` from a slice.
    pub fn from_slice(slice: &[u8]) -> Self {
        Self(slice.to_vec())
    }

    /// Get the inner bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Get the inner bytes as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }

    /// Convert into the inner `Vec<u8>`.
    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    /// Get the length of the bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the bytes are empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl DerefMut for Bytes {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        Self(vec)
    }
}

impl From<Bytes> for Vec<u8> {
    fn from(bytes: Bytes) -> Self {
        bytes.0
    }
}

impl From<&[u8]> for Bytes {
    fn from(slice: &[u8]) -> Self {
        Self(slice.to_vec())
    }
}

impl<const N: usize> From<[u8; N]> for Bytes {
    fn from(array: [u8; N]) -> Self {
        Self(array.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for Bytes {
    fn from(array: &[u8; N]) -> Self {
        Self(array.to_vec())
    }
}

// Custom Serialize/Deserialize implementations are needed because serde's default
// serialization for Vec<u8> uses `serialize_seq`, which produces `Ipld::List` when
// serialized via `ipld_core::serde::to_ipld`. By using `serialize_bytes` instead,
// we get `Ipld::Bytes` which is the correct IPLD representation for binary data.
impl Serialize for Bytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

// Deserialize implementation adapted from serde_bytes:
// https://github.com/serde-rs/bytes/blob/59c109f07c5231f4832c6b3475dd93b52b1b9c09/src/bytebuf.rs
impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;

        impl<'de> serde::de::Visitor<'de> for BytesVisitor {
            type Value = Bytes;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("byte array")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes(v.to_vec()))
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Bytes(v))
            }
        }

        deserializer.deserialize_byte_buf(BytesVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_from_vec() {
        let bytes = Bytes::from(vec![1, 2, 3]);
        assert_eq!(bytes.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn test_bytes_into_vec() {
        let bytes = Bytes::from(vec![1, 2, 3]);
        let vec: Vec<u8> = bytes.into();
        assert_eq!(vec, vec![1, 2, 3]);
    }

    #[test]
    fn test_bytes_deref() {
        let bytes = Bytes::from(vec![1, 2, 3]);
        assert_eq!(&*bytes, &[1, 2, 3]);
        assert_eq!(bytes.len(), 3);
    }

    #[test]
    fn test_bytes_from_array() {
        let bytes = Bytes::from([1u8, 2, 3]);
        assert_eq!(bytes.as_slice(), &[1, 2, 3]);
    }
}
