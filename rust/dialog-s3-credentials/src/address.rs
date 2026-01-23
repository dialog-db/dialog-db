//! S3 storage address types.
//!
//! This module provides the [`Address`] type for specifying S3-compatible storage locations.

use serde::{Deserialize, Serialize};

/// Address for S3-compatible storage.
///
/// Combines endpoint, region, and bucket into a simple data struct that can be used
/// with any S3-compatible service (AWS S3, Cloudflare R2, Wasabi, MinIO, etc.).
///
/// This is a plain data type - URL validation happens when building request URLs.
///
/// # Examples
///
/// ```
/// use dialog_s3_credentials::Address;
///
/// // AWS S3
/// let addr = Address::new(
///     "https://s3.us-east-1.amazonaws.com",
///     "us-east-1",
///     "my-bucket",
/// );
///
/// // Cloudflare R2
/// let addr = Address::new(
///     "https://account-id.r2.cloudflarestorage.com",
///     "auto",
///     "my-bucket",
/// );
///
/// // MinIO (local development)
/// let addr = Address::new(
///     "http://localhost:9000",
///     "us-east-1",
///     "my-bucket",
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Address {
    /// The S3-compatible endpoint URL (e.g., "https://s3.us-east-1.amazonaws.com")
    endpoint: String,
    /// AWS region for signing (e.g., "us-east-1", "auto" for R2)
    region: String,
    /// Bucket name
    bucket: String,
}

impl Address {
    /// Create a new address with the given endpoint, region, and bucket.
    ///
    /// This is infallible - URL validation happens when building request URLs.
    ///
    /// # Arguments
    ///
    /// * `endpoint` - The S3-compatible endpoint URL
    /// * `region` - AWS region for signing (use "auto" for R2, "us-east-1" as default)
    /// * `bucket` - The bucket name
    ///
    /// # Examples
    ///
    /// ```
    /// use dialog_s3_credentials::Address;
    ///
    /// // AWS S3
    /// let addr = Address::new(
    ///     "https://s3.us-east-1.amazonaws.com",
    ///     "us-east-1",
    ///     "my-bucket",
    /// );
    ///
    /// // Cloudflare R2
    /// let addr = Address::new(
    ///     "https://account-id.r2.cloudflarestorage.com",
    ///     "auto",
    ///     "my-bucket",
    /// );
    /// ```
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            bucket: bucket.into(),
        }
    }

    /// Get the endpoint URL string.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Get the region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_creates_s3_address() {
        let addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        assert_eq!(addr.endpoint(), "https://s3.us-east-1.amazonaws.com");
        assert_eq!(addr.region(), "us-east-1");
        assert_eq!(addr.bucket(), "my-bucket");
    }

    #[test]
    fn it_creates_r2_address() {
        let addr = Address::new(
            "https://account-id.r2.cloudflarestorage.com",
            "auto",
            "my-bucket",
        );

        assert_eq!(
            addr.endpoint(),
            "https://account-id.r2.cloudflarestorage.com"
        );
        assert_eq!(addr.region(), "auto");
        assert_eq!(addr.bucket(), "my-bucket");
    }

    #[test]
    fn it_creates_localhost_address() {
        let addr = Address::new("http://localhost:9000", "us-east-1", "my-bucket");

        assert_eq!(addr.endpoint(), "http://localhost:9000");
        assert_eq!(addr.region(), "us-east-1");
        assert_eq!(addr.bucket(), "my-bucket");
    }

    #[test]
    fn it_roundtrips_through_serde() {
        let addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        let json = serde_json::to_string(&addr).unwrap();
        let parsed: Address = serde_json::from_str(&json).unwrap();
        assert_eq!(addr, parsed);
    }
}
