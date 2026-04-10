//! S3 site type, credential types, and Provider implementations.
//!
//! This module provides [`S3`], an S3-compatible storage type
//! that executes pre-authorized HTTP requests via presigned URLs.
//!
//! Submodules:
//! - [`credentials`] — S3 credential types for direct AWS SigV4 signing
//! - [`provider`] — `Provider<Fork<S3, Fx>>` implementations for archive, memory, storage

pub(crate) mod credentials;
pub mod provider;

pub use crate::Address;
pub use credentials::S3Credentials;

use dialog_capability::site::{Authentication, Site, SiteAuthorization};
use url::Url;

use crate::Permit;

/// Extension trait for RequestDescriptor to convert to reqwest RequestBuilder.
pub trait RequestDescriptorExt {
    /// Convert into a reqwest RequestBuilder with the client.
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder;
}

impl RequestDescriptorExt for Permit {
    fn into_request(self, client: &reqwest::Client) -> reqwest::RequestBuilder {
        let mut builder = match self.method.as_str() {
            "GET" => client.get(self.url),
            "PUT" => client.put(self.url),
            "DELETE" => client.delete(self.url),
            _ => client.request(
                reqwest::Method::from_bytes(self.method.as_bytes()).unwrap(),
                self.url,
            ),
        };

        for (key, value) in self.headers {
            builder = builder.header(key, value);
        }

        builder
    }
}

/// S3 direct-access site.
///
/// Uses credential-based [`Authentication`] rather than capability delegation.
/// Authorization is handled via SigV4 presigned URLs on the [`Address`](crate::Address).
#[derive(Debug, Clone, Copy, Default)]
pub struct S3;

/// S3 authorization material.
///
/// Wraps optional credentials. In production the Operator looks up
/// credentials from the secret store. For testing or public access,
/// `None` is used and produces unsigned requests.
#[derive(Debug, Clone, Default)]
pub struct S3Authorization(pub Option<S3Credentials>);

impl S3Authorization {
    /// Authorize a request, producing a presigned URL permit.
    ///
    /// With credentials, signs via SigV4. Without, builds an unsigned request.
    pub async fn grant<R: crate::capability::Access>(
        &self,
        request: &R,
        address: &crate::Address,
    ) -> Result<Permit, crate::AccessError> {
        match &self.0 {
            Some(creds) => creds.authorize(request, address).await,
            None => address.build_unsigned_request(request).await,
        }
    }
}

impl SiteAuthorization for S3Authorization {
    type Protocol = S3;
}

impl From<S3Credentials> for S3Authorization {
    fn from(creds: S3Credentials) -> Self {
        Self(Some(creds))
    }
}

impl Authentication for S3 {
    type Credentials = S3Credentials;
}

impl Site for S3 {
    type Authorization = S3Authorization;
    type Address = crate::Address;
}

/// Determine if path-style URLs should be used by default for this endpoint.
///
/// Returns true for IP addresses and localhost, since virtual-hosted style
/// URLs require DNS resolution of `{bucket}.{host}`.
pub fn is_path_style_default(endpoint: &Url) -> bool {
    use url::Host;
    match endpoint.host() {
        Some(Host::Ipv4(_)) | Some(Host::Ipv6(_)) => true,
        Some(Host::Domain(domain)) => domain == "localhost",
        None => false,
    }
}

/// Build an S3 URL for the given path.
///
/// Handles both path-style and virtual-hosted style URLs.
pub(crate) fn build_url(
    endpoint: &Url,
    bucket: &str,
    path: &str,
    path_style: bool,
) -> Result<Url, crate::AccessError> {
    if path_style {
        // Path-style: https://endpoint/bucket/path
        let mut url = endpoint.clone();
        let new_path = if path.is_empty() {
            format!("{}/", bucket)
        } else {
            format!("{}/{}", bucket, path)
        };
        url.set_path(&new_path);
        Ok(url)
    } else {
        // Virtual-hosted style: https://bucket.endpoint/path
        let host = endpoint
            .host_str()
            .ok_or_else(|| crate::AccessError::Configuration("Invalid endpoint: no host".into()))?;
        let new_host = format!("{}.{}", bucket, host);

        let mut url = endpoint.clone();
        url.set_host(Some(&new_host))
            .map_err(|e| crate::AccessError::Configuration(format!("Invalid host: {}", e)))?;

        let new_path = if path.is_empty() { "/" } else { path };
        url.set_path(new_path);
        Ok(url)
    }
}

/// Extract host string from URL, including port for non-standard ports.
pub(crate) fn extract_host(url: &Url) -> Result<String, crate::AccessError> {
    let hostname = url
        .host_str()
        .ok_or_else(|| crate::AccessError::Configuration("URL missing host".into()))?;

    Ok(match url.port() {
        Some(port) => format!("{}:{}", hostname, port),
        None => hostname.to_string(),
    })
}

/// Default URL expiration: 1 hour.
pub const DEFAULT_EXPIRES: u64 = 3600;

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(dead_code)]
    fn test_address() -> Address {
        Address::new("https://s3.amazonaws.com", "us-east-1", "bucket")
    }

    #[dialog_common::test]
    fn it_creates_address() {
        let address = Address::new(
            "https://s3.us-east-1.amazonaws.com",
            "us-east-1",
            "my-bucket",
        );

        assert_eq!(address.endpoint(), "https://s3.us-east-1.amazonaws.com");
        assert_eq!(address.region(), "us-east-1");
        assert_eq!(address.bucket(), "my-bucket");
    }

    mod url_building_tests {
        use super::*;

        #[dialog_common::test]
        fn it_creates_address_for_virtual_hosted() {
            let address = Address::new("https://s3.amazonaws.com", "us-east-1", "my-bucket");
            assert!(!address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_path_style_for_localhost() {
            let address = Address::new("http://localhost:9000", "us-east-1", "bucket");
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_allows_forcing_path_style() {
            let address = Address::new("https://custom-s3.example.com", "us-east-1", "bucket")
                .with_path_style();
            assert!(address.path_style());
        }

        #[dialog_common::test]
        fn it_creates_r2_address() {
            let address = Address::new("https://abc123.r2.cloudflarestorage.com", "auto", "bucket");
            assert!(!address.path_style());
        }
    }
}
