//! S3 site type, credential types, and Provider implementations.
//!
//! This module provides [`S3`], an S3-compatible storage type
//! that executes pre-authorized HTTP requests via presigned URLs.
//!
//! Submodules:
//! - [`credentials`] — S3 credential types for direct AWS SigV4 signing
//! - [`provider`] — `Provider<Fork<S3, Fx>>` implementations for archive, memory, storage

mod authorization;
pub(crate) mod credentials;
mod invocation;
pub mod provider;

pub use authorization::S3Authorization;
pub use credentials::S3Credentials;
pub use invocation::S3Invocation;

use super::{Address, S3Error};
use dialog_capability::site::{Authentication, Site};
use url::{Host, Url};

/// S3 direct-access site.
///
/// Uses credential-based [`Authentication`] rather than capability delegation.
/// Authorization is handled via SigV4 presigned URLs on the [`Address`](crate::Address).
#[derive(Debug, Clone, Copy, Default)]
pub struct S3;

impl Authentication for S3 {
    type Credentials = S3Credentials;
}

impl Site for S3 {
    type Authorization = S3Authorization;
    type Address = Address;
}

/// Determine if path-style URLs should be used by default for this endpoint.
///
/// Returns true for IP addresses and localhost, since virtual-hosted style
/// URLs require DNS resolution of `{bucket}.{host}`.
pub fn is_path_style_default(endpoint: &Url) -> bool {
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
) -> Result<Url, S3Error> {
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
            .ok_or_else(|| S3Error::Configuration("Invalid endpoint: no host".into()))?;
        let new_host = format!("{}.{}", bucket, host);

        let mut url = endpoint.clone();
        url.set_host(Some(&new_host))
            .map_err(|e| S3Error::Configuration(format!("Invalid host: {}", e)))?;

        let new_path = if path.is_empty() { "/" } else { path };
        url.set_path(new_path);
        Ok(url)
    }
}

/// Extract host string from URL, including port for non-standard ports.
pub(crate) fn extract_host(url: &Url) -> Result<String, S3Error> {
    let hostname = url
        .host_str()
        .ok_or_else(|| S3Error::Configuration("URL missing host".into()))?;

    Ok(match url.port() {
        Some(port) => format!("{}:{}", hostname, port),
        None => hostname.to_string(),
    })
}

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
