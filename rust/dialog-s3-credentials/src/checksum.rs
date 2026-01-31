//! Checksum algorithms for S3 object integrity verification.
//!
//! This module provides checksum types used in the `x-amz-checksum-{algorithm}` headers
//! for S3-compatible storage services. See [Checking object integrity] for more details.
//!
//! Checksums are serialized in [multihash] format: `<varint code><varint length><digest>`.
//! For SHA-256, the code is `0x12` and length is `0x20` (32 bytes).
//!
//! [Checking object integrity]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html
//! [multihash]: https://multiformats.io/multihash/

use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// [Multihash code for SHA-256]
/// (https://github.com/multiformats/multicodec/blob/master/table.csv#L9)
const SHA256_CODE: u8 = 0x12;
/// SHA-256 digest length (32 bytes)
const SHA256_LEN: u8 = 0x20;

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
/// Serializes as multihash
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(into = "Multihash", try_from = "Multihash")]
pub enum Checksum {
    /// SHA-256 checksum.
    Sha256([u8; 32]),
}

/// Wrapper for multihash byte serialization.
#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct Multihash(#[serde(with = "serde_bytes")] Vec<u8>);

impl From<Checksum> for Multihash {
    fn from(checksum: Checksum) -> Self {
        match checksum {
            Checksum::Sha256(digest) => {
                let mut bytes = Vec::with_capacity(2 + digest.len());
                bytes.push(SHA256_CODE);
                bytes.push(SHA256_LEN);
                bytes.extend_from_slice(&digest);
                Multihash(bytes)
            }
        }
    }
}

impl TryFrom<Multihash> for Checksum {
    type Error = String;

    fn try_from(bytes: Multihash) -> Result<Self, Self::Error> {
        Checksum::try_from(bytes.0)
    }
}

impl TryFrom<Vec<u8>> for Checksum {
    type Error = String;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        if bytes.len() < 2 {
            return Err(format!(
                "multihash too short: expected at least 2 bytes, got {}",
                bytes.len()
            ));
        }

        let code = bytes[0];
        let len = bytes[1] as usize;

        if bytes.len() != 2 + len {
            return Err(format!(
                "multihash length mismatch: header says {} bytes, got {}",
                len,
                bytes.len() - 2
            ));
        }

        match code {
            SHA256_CODE => {
                if len != 32 {
                    return Err(format!("SHA-256 digest must be 32 bytes, got {}", len));
                }
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes[2..]);
                Ok(Checksum::Sha256(arr))
            }
            other => Err(format!("unsupported multihash code: 0x{:02x}", other)),
        }
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

    #[test]
    fn it_serializes_as_multihash_bytes() {
        let checksum = Hasher::Sha256.checksum(b"hello world");
        let multihash: Multihash = checksum.clone().into();

        // First byte is SHA-256 code (0x12)
        assert_eq!(multihash.0[0], SHA256_CODE);
        // Second byte is length (0x20 = 32)
        assert_eq!(multihash.0[1], SHA256_LEN);
        // Remaining bytes are the digest
        assert_eq!(&multihash.0[2..], checksum.as_bytes());
        // Total length is 2 + 32 = 34
        assert_eq!(multihash.0.len(), 34);
    }

    #[test]
    fn it_deserializes_from_multihash_bytes() {
        let original = Hasher::Sha256.checksum(b"hello world");
        let multihash: Multihash = original.clone().into();
        let decoded = Checksum::try_from(multihash).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn it_roundtrips_through_ipld() {
        use ipld_core::ipld::Ipld;

        let checksum = Hasher::Sha256.checksum(b"hello world");
        let ipld: Ipld = ipld_core::serde::to_ipld(&checksum).unwrap();

        // Should serialize as Ipld::Bytes (not a map or list)
        assert!(matches!(ipld, Ipld::Bytes(_)));

        // Should roundtrip correctly
        let decoded: Checksum = ipld_core::serde::from_ipld(ipld).unwrap();
        assert_eq!(checksum, decoded);
    }

    #[test]
    fn it_rejects_invalid_multihash_code() {
        let mut bytes = vec![0x99, 0x20]; // Unknown code 0x99
        bytes.extend_from_slice(&[0u8; 32]);
        let result = Checksum::try_from(bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unsupported multihash code"));
    }

    #[test]
    fn it_rejects_wrong_length() {
        let mut bytes = vec![SHA256_CODE, 0x10]; // Claims 16 bytes
        bytes.extend_from_slice(&[0u8; 16]);
        let result = Checksum::try_from(bytes);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must be 32 bytes"));
    }
}
