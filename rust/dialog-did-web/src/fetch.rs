//! The HTTP fetch abstraction behind `did:web` resolution.
//!
//! The `did:web` provider is generic over a [`Fetch`] so tests can supply a
//! canned response instead of hitting the network. The default implementation,
//! [`ReqwestFetch`], uses the workspace `reqwest` (no `stream` feature, so it
//! builds for both native and wasm).

use dialog_common::{ConditionalSend, ConditionalSync};

use crate::error::ResolveError;

/// Fetches the bytes of an `https` URL.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Fetch: ConditionalSync {
    /// GET `url` and return the response body bytes.
    ///
    /// # Errors
    ///
    /// Returns [`ResolveError::Fetch`] on a transport failure or a non-success
    /// status.
    async fn get(&self, url: &str) -> Result<Vec<u8>, ResolveError>;
}

/// The default [`Fetch`], backed by `reqwest`.
#[derive(Debug, Clone, Default)]
pub struct ReqwestFetch {
    client: reqwest::Client,
}

impl ReqwestFetch {
    /// Create a fetcher with a fresh `reqwest` client.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a fetcher from an existing `reqwest` client.
    #[must_use]
    pub fn with_client(client: reqwest::Client) -> Self {
        Self { client }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Fetch for ReqwestFetch {
    async fn get(&self, url: &str) -> Result<Vec<u8>, ResolveError> {
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| ResolveError::Fetch(format!("request to {url} failed: {e}")))?;

        let status = response.status();
        if !status.is_success() {
            return Err(ResolveError::Fetch(format!(
                "{url} returned status {status}"
            )));
        }

        response
            .bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| ResolveError::Fetch(format!("reading body of {url} failed: {e}")))
    }
}

/// A [`Fetch`] backed by a fixed set of URL to response mappings, for tests.
#[cfg(any(test, feature = "test-fetch"))]
#[derive(Debug, Clone, Default)]
pub struct MapFetch {
    routes: std::collections::HashMap<String, Vec<u8>>,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(any(test, feature = "test-fetch"))]
impl MapFetch {
    /// Create an empty fetcher.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a successful response body for a URL.
    #[must_use]
    pub fn with(mut self, url: impl Into<String>, body: impl Into<Vec<u8>>) -> Self {
        self.routes.insert(url.into(), body.into());
        self
    }

    /// The number of `get` calls made so far.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(any(test, feature = "test-fetch"))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Fetch for MapFetch {
    async fn get(&self, url: &str) -> Result<Vec<u8>, ResolveError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.routes
            .get(url)
            .cloned()
            .ok_or_else(|| ResolveError::Fetch(format!("{url} returned status 404 Not Found")))
    }
}

/// Marker so `ConditionalSend` is exercised on the trait object path.
const _: fn() = || {
    fn assert_send<T: ConditionalSend>() {}
    assert_send::<ReqwestFetch>();
};
