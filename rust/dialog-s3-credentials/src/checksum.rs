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
#[derive(Debug, Clone)]
pub enum Checksum {
    /// SHA-256 checksum.
    Sha256([u8; 32]),
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
