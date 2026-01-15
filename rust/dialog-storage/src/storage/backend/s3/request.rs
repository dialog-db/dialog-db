//! S3 request types and execution.
//!
//! This module contains the request types (`Put`, `Get`, `Delete`) and the `Request` trait
//! for executing requests against an S3 backend.

use super::{Acl, Bucket, Checksum, Hasher, Invocation, RequestInfo, S3StorageError};
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use url::Url;

/// Precondition for PUT operations to enable compare-and-swap semantics.
#[derive(Debug, Clone)]
pub enum Precondition {
    /// No precondition - unconditional write
    None,
    /// Update only if current ETag matches (If-Match: <etag>)
    IfMatch(String),
    /// Update only if key doesn't exist (If-None-Match: *)
    IfNoneMatch,
}

/// A PUT request to upload data.
#[derive(Debug)]
pub struct Put {
    url: Url,
    region: String,
    body: Vec<u8>,
    checksum: Option<Checksum>,
    acl: Option<Acl>,
    precondition: Precondition,
}

impl Put {
    /// Create a new PUT request with the given URL, body, and region.
    ///
    /// Use [`with_checksum`](Self::with_checksum) to add integrity verification.
    pub fn new(url: Url, body: impl AsRef<[u8]>, region: &str) -> Self {
        Self {
            url,
            region: region.to_string(),
            body: body.as_ref().to_vec(),
            checksum: None,
            acl: None,
            precondition: Precondition::None,
        }
    }

    /// Compute and set the checksum using the given hasher.
    pub fn with_checksum(mut self, hasher: &Hasher) -> Self {
        self.checksum = Some(hasher.checksum(&self.body));
        self
    }

    /// Set the ACL for this request.
    pub fn with_acl(mut self, acl: Acl) -> Self {
        self.acl = Some(acl);
        self
    }

    /// Set the precondition for this request.
    pub fn with_precondition(mut self, precondition: Precondition) -> Self {
        self.precondition = precondition;
        self
    }

    /// Get the precondition for this request.
    pub fn precondition(&self) -> &Precondition {
        &self.precondition
    }
}

impl Invocation for Put {
    fn method(&self) -> &'static str {
        "PUT"
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn region(&self) -> &str {
        &self.region
    }

    fn checksum(&self) -> Option<&Checksum> {
        self.checksum.as_ref()
    }

    fn acl(&self) -> Option<Acl> {
        self.acl
    }
}

impl Request for Put {
    fn body(&self) -> Option<&[u8]> {
        Some(&self.body)
    }

    fn precondition_headers(&self) -> Vec<(&'static str, String)> {
        match &self.precondition {
            Precondition::None => Vec::new(),
            Precondition::IfMatch(etag) => vec![("If-Match", format!("\"{}\"", etag))],
            Precondition::IfNoneMatch => vec![("If-None-Match", "*".to_string())],
        }
    }
}

/// A GET request to retrieve data.
#[derive(Debug, Clone)]
pub struct Get {
    url: Url,
    region: String,
}

impl Get {
    /// Create a new GET request for the given URL and region.
    pub fn new(url: Url, region: &str) -> Self {
        Self {
            url,
            region: region.to_string(),
        }
    }
}

impl Invocation for Get {
    fn method(&self) -> &'static str {
        "GET"
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn region(&self) -> &str {
        &self.region
    }
}

impl Request for Get {}

/// A DELETE request to remove an object.
#[derive(Debug, Clone)]
pub struct Delete {
    url: Url,
    region: String,
    precondition: Option<String>,
}

impl Delete {
    /// Create a new DELETE request for the given URL and region.
    pub fn new(url: Url, region: &str) -> Self {
        Self {
            url,
            region: region.to_string(),
            precondition: None,
        }
    }

    /// Set the precondition (If-Match ETag) for this delete request.
    pub fn with_if_match(mut self, etag: String) -> Self {
        self.precondition = Some(etag);
        self
    }

    /// Get the precondition for this request.
    pub fn precondition(&self) -> Option<&str> {
        self.precondition.as_deref()
    }
}

impl Invocation for Delete {
    fn method(&self) -> &'static str {
        "DELETE"
    }

    fn url(&self) -> &Url {
        &self.url
    }

    fn region(&self) -> &str {
        &self.region
    }
}

impl Request for Delete {
    fn precondition_headers(&self) -> Vec<(&'static str, String)> {
        match &self.precondition {
            Some(etag) => vec![("If-Match", format!("\"{}\"", etag))],
            None => Vec::new(),
        }
    }
}

/// Executable S3 request with an optional body.
///
/// This trait extends [`Invocation`] with the request body and the ability to
/// execute the request against an S3 backend. The separation exists because
/// [`Invocation`] lives in the [`access`](super::access) module which handles authorization
/// concerns independently and only needs the metadata required for signing
/// (method, URL, checksum, ACL), not the actual payload.
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Request: Invocation + Sized {
    /// The request body, if any.
    fn body(&self) -> Option<&[u8]> {
        None
    }

    /// Precondition headers for conditional requests (CAS semantics).
    ///
    /// Returns a list of (header-name, header-value) pairs to add to the request.
    /// Common preconditions include:
    /// - `If-Match: <etag>` - Only proceed if current ETag matches
    /// - `If-None-Match: *` - Only proceed if object doesn't exist
    fn precondition_headers(&self) -> Vec<(&'static str, String)> {
        Vec::new()
    }

    /// Perform this request against the given bucket.
    async fn perform<Key, Value>(
        &self,
        bucket: &Bucket<Key, Value>,
    ) -> Result<reqwest::Response, S3StorageError>
    where
        Key: AsRef<[u8]> + Clone + ConditionalSync,
        Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    {
        let request_info = RequestInfo::from_invocation(self);
        let authorized = bucket
            .authorizer
            .authorize(&request_info)
            .await
            .map_err(|e| S3StorageError::AuthorizationError(e.to_string()))?;

        let mut builder = match self.method() {
            "GET" => bucket.client.get(authorized.url),
            "PUT" => bucket.client.put(authorized.url),
            "DELETE" => bucket.client.delete(authorized.url),
            method => bucket.client.request(
                reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                authorized.url,
            ),
        };

        for (key, value) in authorized.headers {
            builder = builder.header(key, value);
        }

        // Add precondition headers for CAS semantics
        for (key, value) in self.precondition_headers() {
            builder = builder.header(key, value);
        }

        if let Some(body) = self.body() {
            builder = builder.body(body.to_vec());
        }

        Ok(builder.send().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_REGION: &str = "us-east-1";

    #[dialog_common::test]
    fn it_creates_put_request_with_checksum() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value", TEST_REGION).with_checksum(&Hasher::Sha256);

        // Checksum should be present after with_checksum
        assert!(request.checksum().is_some());
        // Checksum should have the correct algorithm name
        assert_eq!(request.checksum().unwrap().name(), "sha256");
    }

    #[dialog_common::test]
    fn it_creates_put_request_without_checksum() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value", TEST_REGION);

        // Checksum should be None by default
        assert!(request.checksum().is_none());
    }

    #[dialog_common::test]
    fn it_creates_put_request_with_acl() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Put::new(url, b"test value", TEST_REGION).with_acl(Acl::PublicRead);

        assert_eq!(request.acl(), Some(Acl::PublicRead));
    }

    #[dialog_common::test]
    fn it_creates_get_request() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Get::new(url.clone(), TEST_REGION);

        assert_eq!(request.method(), "GET");
        assert_eq!(request.url(), &url);
        assert_eq!(request.region(), TEST_REGION);
        assert!(request.checksum().is_none());
        assert!(request.acl().is_none());
    }

    #[dialog_common::test]
    fn it_creates_delete_request() {
        let url = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let request = Delete::new(url.clone(), TEST_REGION);

        assert_eq!(request.method(), "DELETE");
        assert_eq!(request.url(), &url);
        assert_eq!(request.region(), TEST_REGION);
        assert!(request.checksum().is_none());
        assert!(request.acl().is_none());
    }
}
