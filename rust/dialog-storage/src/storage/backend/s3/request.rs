//! S3 request types and execution.
//!
//! This module contains the request types (`Put`, `Get`, `Delete`) and the `Request` trait
//! for executing requests against an S3 backend.

use async_trait::async_trait;
use dialog_common::ConditionalSync;
use url::Url;

use super::access::{Acl, Invocation};
use super::checksum::{Checksum, Hasher};
use super::{S3, S3StorageError};

/// A PUT request to upload data.
#[derive(Debug)]
pub struct Put {
    url: Url,
    body: Vec<u8>,
    checksum: Option<Checksum>,
    acl: Option<Acl>,
}

impl Put {
    /// Create a new PUT request with the given URL and body.
    ///
    /// Use [`with_checksum`](Self::with_checksum) to add integrity verification.
    pub fn new(url: Url, body: impl AsRef<[u8]>) -> Self {
        Self {
            url,
            body: body.as_ref().to_vec(),
            checksum: None,
            acl: None,
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
}

impl Invocation for Put {
    fn method(&self) -> &'static str {
        "PUT"
    }

    fn url(&self) -> &Url {
        &self.url
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
}

/// A GET request to retrieve data.
#[derive(Debug, Clone)]
pub struct Get {
    url: Url,
}

impl Get {
    /// Create a new GET request for the given URL.
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

impl Invocation for Get {
    fn method(&self) -> &'static str {
        "GET"
    }

    fn url(&self) -> &Url {
        &self.url
    }
}

impl Request for Get {}

/// A DELETE request to remove an object.
#[derive(Debug, Clone)]
pub struct Delete {
    url: Url,
}

impl Delete {
    /// Create a new DELETE request for the given URL.
    pub fn new(url: Url) -> Self {
        Self { url }
    }
}

impl Invocation for Delete {
    fn method(&self) -> &'static str {
        "DELETE"
    }

    fn url(&self) -> &Url {
        &self.url
    }
}

impl Request for Delete {}

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

    /// Perform this request against the given S3 backend.
    async fn perform<Key, Value>(
        &self,
        s3: &S3<Key, Value>,
    ) -> Result<reqwest::Response, S3StorageError>
    where
        Key: AsRef<[u8]> + Clone + ConditionalSync,
        Value: AsRef<[u8]> + From<Vec<u8>> + Clone + ConditionalSync,
    {
        let authorized = s3
            .session
            .authorize(self)
            .map_err(|e| S3StorageError::AuthorizationError(e.to_string()))?;

        let mut builder = match self.method() {
            "GET" => s3.client.get(authorized.url),
            "PUT" => s3.client.put(authorized.url),
            "DELETE" => s3.client.delete(authorized.url),
            method => s3.client.request(
                reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                authorized.url,
            ),
        };

        for (key, value) in authorized.headers {
            builder = builder.header(key, value);
        }

        if let Some(body) = self.body() {
            builder = builder.body(body.to_vec());
        }

        Ok(builder.send().await?)
    }
}
