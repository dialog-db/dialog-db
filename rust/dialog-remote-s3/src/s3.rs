//! S3 credential types for direct AWS SigV4 signing.
//!
//! This module provides [`S3Credentials`] for authenticated S3 access.
//! Use `Option<S3Credentials>` where `None` means public/unsigned access.

pub(crate) mod credentials;
pub mod provider;
pub mod site;

pub use crate::Address;
pub use credentials::S3Credentials;
use url::Url;

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
