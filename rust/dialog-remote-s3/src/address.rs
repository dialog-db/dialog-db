//! S3 storage address types.
//!
//! This module provides the [`Address`] type for specifying S3-compatible storage locations.

use serde::{Deserialize, Serialize};
use url::Url;

use crate::AccessError;
use crate::capability::Access;
use crate::permit::Permit;

/// Address for S3-compatible storage.
///
/// Combines endpoint, region, bucket, and path-style configuration into a
/// type that can build URLs and authorize S3 requests.
///
/// # Examples
///
/// ```
/// use dialog_remote_s3::Address;
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
/// // MinIO (local development) — auto-detects path-style
/// let addr = Address::new(
///     "http://localhost:9000",
///     "us-east-1",
///     "my-bucket",
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Address {
    /// The S3-compatible endpoint URL (e.g., "https://s3.us-east-1.amazonaws.com")
    endpoint: String,
    /// AWS region for signing (e.g., "us-east-1", "auto" for R2)
    region: String,
    /// Bucket name
    bucket: String,
    /// Whether to use path-style URLs (auto-detected from endpoint)
    #[serde(default)]
    path_style: bool,
}

impl Address {
    /// Create a new address with the given endpoint, region, and bucket.
    ///
    /// Path-style is auto-detected: `true` for localhost/IP addresses,
    /// `false` for domain names (virtual-hosted style).
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
    /// use dialog_remote_s3::Address;
    ///
    /// // AWS S3 (virtual-hosted style)
    /// let addr = Address::new(
    ///     "https://s3.us-east-1.amazonaws.com",
    ///     "us-east-1",
    ///     "my-bucket",
    /// );
    /// assert!(!addr.path_style());
    ///
    /// // localhost (path-style auto-detected)
    /// let addr = Address::new("http://localhost:9000", "us-east-1", "my-bucket");
    /// assert!(addr.path_style());
    /// ```
    pub fn new(
        endpoint: impl Into<String>,
        region: impl Into<String>,
        bucket: impl Into<String>,
    ) -> Self {
        let endpoint = endpoint.into();
        let path_style = Url::parse(&endpoint)
            .map(|url| crate::s3::is_path_style_default(&url))
            .unwrap_or(false);

        Self {
            endpoint,
            region: region.into(),
            bucket: bucket.into(),
            path_style,
        }
    }

    /// Enable path-style URL addressing (e.g. `endpoint/bucket/key`
    /// instead of `bucket.endpoint/key`).
    pub fn with_path_style(mut self) -> Self {
        self.path_style = true;
        self
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

    /// Whether path-style URLs are used.
    pub fn path_style(&self) -> bool {
        self.path_style
    }

    /// Build a URL for the given key path.
    pub fn build_url(&self, path: &str) -> Result<Url, AccessError> {
        let endpoint =
            Url::parse(&self.endpoint).map_err(|e| AccessError::Configuration(e.to_string()))?;
        crate::s3::build_url(&endpoint, &self.bucket, path, self.path_style)
    }

    /// Authorize a request using the given credentials.
    ///
    /// - No credentials → public/unsigned access (no SigV4 signing)
    /// - With credentials → private access with SigV4 signing
    pub async fn authorize<R: Access>(
        &self,
        request: &R,
        credentials: Option<&crate::s3::S3Credentials>,
    ) -> Result<Permit, AccessError> {
        match credentials {
            None => self.build_unsigned_request(request).await,
            Some(creds) => creds.grant(request, self).await,
        }
    }

    /// Build an unsigned request for public access.
    async fn build_unsigned_request<R: Access>(&self, request: &R) -> Result<Permit, AccessError> {
        use crate::capability::Precondition;

        let path = request.path();
        let mut url = self.build_url(&path)?;

        // Add query parameters if specified
        if let Some(params) = request.params() {
            let mut query = url.query_pairs_mut();
            for (key, value) in params {
                query.append_pair(&key, &value);
            }
        }

        let host = crate::s3::extract_host(&url)?;

        let mut headers = vec![("host".to_string(), host)];
        if let Some(checksum) = request.checksum() {
            let header_name = format!("x-amz-checksum-{}", checksum.name());
            headers.push((header_name, checksum.to_string()));
        }
        match request.precondition() {
            Precondition::IfMatch(etag) => {
                headers.push(("if-match".to_string(), format!("\"{}\"", etag)));
            }
            Precondition::IfNoneMatch => {
                headers.push(("if-none-match".to_string(), "*".to_string()));
            }
            Precondition::None => {}
        }

        Ok(Permit {
            url,
            method: request.method().to_string(),
            headers,
        })
    }
}

/// `Address` is the canonical address type for the `S3` site.
impl dialog_capability::site::SiteAddress for Address {
    type Site = crate::s3::S3;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[dialog_common::test]
    fn it_creates_s3_address() {
        let addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        assert_eq!(addr.endpoint(), "https://s3.us-east-1.amazonaws.com");
        assert_eq!(addr.region(), "us-east-1");
        assert_eq!(addr.bucket(), "my-bucket");
        assert!(!addr.path_style());
    }

    #[dialog_common::test]
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

    #[dialog_common::test]
    fn it_creates_localhost_address() {
        let addr = Address::new("http://localhost:9000", "us-east-1", "my-bucket");

        assert_eq!(addr.endpoint(), "http://localhost:9000");
        assert_eq!(addr.region(), "us-east-1");
        assert_eq!(addr.bucket(), "my-bucket");
        assert!(addr.path_style());
    }

    #[dialog_common::test]
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

    #[dialog_common::test]
    fn it_supports_with_path_style() {
        let addr = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        )
        .with_path_style();

        assert!(addr.path_style());
    }
}
