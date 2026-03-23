//! Checksum types for content integrity verification.
//!
//! Provides checksum types used for content-addressed storage and authorization.
//! Checksums are serialized in [multihash] format: `<varint code><varint length><digest>`.
//! For SHA-256, the code is `0x12` and length is `0x20` (32 bytes).
//!
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
/// ```no_run
/// # use dialog_common::Hasher;
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
/// Serializes as multihash bytes: `<varint code><varint length><digest>`.
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

    fn try_from(mh: Multihash) -> Result<Self, Self::Error> {
        Checksum::try_from(mh.0)
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
    /// Compute the SHA-256 checksum of the given data.
    ///
    /// This is the primary constructor for use with `#[derive(Claim)]`:
    /// `#[claim(with = Checksum::sha256, rename = checksum)]`
    pub fn sha256(data: impl AsRef<[u8]>) -> Self {
        Hasher::Sha256.checksum(data.as_ref())
    }

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
        assert_eq!(checksum.as_bytes().len(), 32);
    }

    #[test]
    fn it_formats_checksum_as_base64() {
        let checksum = Hasher::Sha256.checksum(b"hello world");
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

        assert_eq!(multihash.0[0], SHA256_CODE);
        assert_eq!(multihash.0[1], SHA256_LEN);
        assert_eq!(&multihash.0[2..], checksum.as_bytes());
        assert_eq!(multihash.0.len(), 34);
    }

    #[test]
    fn it_roundtrips_through_multihash() {
        let original = Hasher::Sha256.checksum(b"hello world");
        let multihash: Multihash = original.clone().into();
        let decoded = Checksum::try_from(multihash).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn it_computes_checksum_via_sha256() {
        let data = b"hello world".to_vec();
        let from_method = Checksum::sha256(data);
        let from_hasher = Hasher::Sha256.checksum(b"hello world");
        assert_eq!(from_method, from_hasher);
    }
}
