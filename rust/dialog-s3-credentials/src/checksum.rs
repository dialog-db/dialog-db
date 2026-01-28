//! Checksum algorithms for S3 object integrity verification.
//!
//! This module provides checksum types used in the `x-amz-checksum-{algorithm}` headers
//! for S3-compatible storage services. See [Checking object integrity] for more details.
//!
//! [Checking object integrity]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html

use base64::Engine;
use sha2::{Digest, Sha256};

/// A checksum algorithm that can compute checksums from data.
///
/// Use a `Hasher` variant to compute a [`Checksum`] from data:
///
/// ```
/// use dialog_s3_credentials::Hasher;
/// let checksum = Hasher::Sha256.checksum(b"hello world");
/// ```
#[derive(Debug, Clone, Copy)]
pub enum Hasher {
    /// SHA-256 hashing algorithm.
    Sha256,
}

impl Hasher {
    /// Compute the checksum of the given data using this algorithm.
    pub fn checksum(&self, data: &[u8]) -> Checksum {
        match self {
            Self::Sha256 => Checksum::Sha256(Sha256::digest(data).into()),
        }
    }
}

/// A checksum algorithm and its computed value.
///
/// This enum represents different checksum algorithms supported for S3 object integrity
/// verification. The checksum is used in the `x-amz-checksum-{algorithm}` header.
///
/// When deserializing from IPLD/CBOR, expects 32 raw bytes.
#[derive(Debug, Clone, PartialEq)]
pub enum Checksum {
    /// SHA-256 checksum.
    Sha256([u8; 32]),
}

impl TryFrom<Vec<u8>> for Checksum {
    type Error = String;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.len() != 32 {
            return Err(format!("Checksum must be 32 bytes, got {}", bytes.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Checksum::Sha256(arr))
    }
}

impl serde::Serialize for Checksum {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        // Wrapper to serialize bytes properly for IPLD
        struct BytesWrapper<'a>(&'a [u8]);
        impl serde::Serialize for BytesWrapper<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_bytes(self.0)
            }
        }

        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("algorithm", self.name())?;
        map.serialize_entry("value", &BytesWrapper(self.as_bytes()))?;
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for Checksum {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{MapAccess, Visitor};

        struct ChecksumVisitor;

        impl<'de> Visitor<'de> for ChecksumVisitor {
            type Value = Checksum;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str(
                    "32 bytes, a sequence of 32 bytes, or a map with algorithm and value",
                )
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Checksum::try_from(v.to_vec()).map_err(E::custom)
            }

            fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Checksum::try_from(v).map_err(E::custom)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut bytes = Vec::with_capacity(32);
                while let Some(byte) = seq.next_element()? {
                    bytes.push(byte);
                }
                Checksum::try_from(bytes).map_err(serde::de::Error::custom)
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut algorithm: Option<String> = None;
                let mut value: Option<Vec<u8>> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "algorithm" => algorithm = Some(map.next_value()?),
                        "value" => value = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let algorithm =
                    algorithm.ok_or_else(|| serde::de::Error::missing_field("algorithm"))?;
                let value = value.ok_or_else(|| serde::de::Error::missing_field("value"))?;

                match algorithm.as_str() {
                    "sha256" => Checksum::try_from(value).map_err(serde::de::Error::custom),
                    other => Err(serde::de::Error::custom(format!(
                        "unsupported algorithm: {}",
                        other
                    ))),
                }
            }
        }

        deserializer.deserialize_any(ChecksumVisitor)
    }
}

impl Checksum {
    /// Returns the raw bytes of the checksum.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Sha256(checksum) => checksum,
        }
    }

    /// Returns the algorithm name used in S3 headers (e.g., "sha256").
    pub fn name(&self) -> &str {
        match self {
            Self::Sha256(_) => "sha256",
        }
    }
}

impl std::fmt::Display for Checksum {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&String::from(self))
    }
}

impl From<&Checksum> for String {
    fn from(checksum: &Checksum) -> Self {
        base64::engine::general_purpose::STANDARD.encode(checksum.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_computes_sha256_checksum() {
        let checksum = Hasher::Sha256.checksum(b"hello world");
        // SHA-256 of "hello world" should be 32 bytes
        assert_eq!(checksum.as_bytes().len(), 32);
    }

    #[test]
    fn it_formats_checksum_as_base64() {
        let checksum = Hasher::Sha256.checksum(b"hello world");
        // SHA-256 of "hello world" base64 encoded
        assert_eq!(
            checksum.to_string(),
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
    }

    #[test]
    fn it_returns_checksum_algorithm_name() {
        let checksum = Hasher::Sha256.checksum(b"test");
        assert_eq!(checksum.name(), "sha256");
    }
}
