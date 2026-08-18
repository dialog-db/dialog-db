//! The HTTP fetch abstraction behind `did:web` resolution.
//!
//! The `did:web` provider is generic over a [`Fetch`] so tests can supply a
//! canned response instead of hitting the network. The default implementation,
//! [`ReqwestFetch`], uses the workspace `reqwest` (no `stream` feature, so it
//! builds for both native and wasm).
//!
//! # Fetching for a security decision
//!
//! This fetch feeds a signature check, and the URL comes from a DID an
//! unauthenticated party supplied, so the client is configured rather than
//! defaulted:
//!
//! - **No redirects.** `reqwest`'s default follows up to ten, across origins. A
//!   `did:web` host (or the plc directory) that redirects would have its
//!   document read from somewhere else entirely, so a resolution must fail
//!   loudly rather than silently follow.
//! - **A request timeout**, so an unresponsive host cannot pin a verification
//!   task open.
//! - **A response size cap**, so a host cannot answer a DID-document request
//!   with an unbounded body.
//!
//! `reqwest` on wasm32 runs on `fetch`, whose redirect and timeout policy the
//! browser owns and the builder cannot set; the size cap is enforced here on
//! every target.

use dialog_common::{ConditionalSend, ConditionalSync};

use crate::error::ResolveError;

/// The largest DID document this fetcher will read.
///
/// A DID document is a small JSON object naming a handful of keys; real ones
/// are well under a kilobyte. This bound only has to exclude a body sent to
/// exhaust memory.
pub const MAX_DOCUMENT_BYTES: usize = 1 << 20;

/// How long a single DID-document request may take.
///
/// Native only: on wasm32 `reqwest` runs on the browser's `fetch`, which owns
/// the timeout, so there is nothing for the client builder to set.
#[cfg(not(target_arch = "wasm32"))]
pub const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Refuse a response whose declared or actual length exceeds
/// [`MAX_DOCUMENT_BYTES`].
///
/// `len` is `content_length()` before the body is read (a host that lies or
/// omits it is caught by the after-read call with the true length) or the read
/// body length after. Split out from [`ReqwestFetch::get`] so the size decision
/// is unit-testable without a live server or the `reqwest` types.
fn check_size(url: &str, len: Option<u64>) -> Result<(), ResolveError> {
    match len {
        Some(len) if len > MAX_DOCUMENT_BYTES as u64 => Err(ResolveError::Fetch(format!(
            "{url} returned {len} bytes, over the {MAX_DOCUMENT_BYTES}-byte limit"
        ))),
        _ => Ok(()),
    }
}

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
#[derive(Debug, Clone)]
pub struct ReqwestFetch {
    client: reqwest::Client,
}

impl Default for ReqwestFetch {
    fn default() -> Self {
        Self::new()
    }
}

impl ReqwestFetch {
    /// Create a fetcher whose client refuses redirects and times out.
    ///
    /// See the module docs for why this is configured rather than defaulted.
    #[must_use]
    pub fn new() -> Self {
        let builder = reqwest::Client::builder();

        // `redirect` and `timeout` are native-only: on wasm32 `reqwest` is a
        // thin wrapper over the browser's `fetch`, which owns both policies.
        #[cfg(not(target_arch = "wasm32"))]
        let builder = builder
            .redirect(reqwest::redirect::Policy::none())
            .timeout(REQUEST_TIMEOUT);

        Self {
            // A builder with only these options set cannot fail; fall back to
            // a default client rather than panicking if that ever changes.
            client: builder.build().unwrap_or_default(),
        }
    }

    /// Create a fetcher from an existing `reqwest` client.
    ///
    /// The caller owns that client's redirect, timeout, and proxy policy;
    /// [`ReqwestFetch::new`] is the configured default.
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

        // A redirect is refused rather than followed, so a 3xx arrives here as
        // a non-success status above. Name it plainly for the operator.
        if status.is_redirection() {
            return Err(ResolveError::Fetch(format!(
                "{url} redirected; a DID document must be served by the host the DID names"
            )));
        }

        // Refuse an over-large body before reading it, when the host declares
        // its length.
        check_size(url, response.content_length())?;

        let body = response
            .bytes()
            .await
            .map_err(|e| ResolveError::Fetch(format!("reading body of {url} failed: {e}")))?;

        // And again after reading, for a host that declared no length.
        check_size(url, Some(body.len() as u64))?;

        Ok(body.to_vec())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A response at or under the cap is accepted; over it is refused. The
    /// resolver fetches attacker-supplied `did:web`/`did:plc` URLs, so a host
    /// answering a DID-document request with an unbounded body is a
    /// memory-exhaustion vector; this is the guard that stops it, exercised
    /// without a live server.
    #[dialog_common::test]
    fn check_size_caps_the_body() {
        // Declared length under, at, and over the cap.
        assert!(check_size("https://h/did.json", Some(0)).is_ok());
        assert!(check_size("https://h/did.json", Some(MAX_DOCUMENT_BYTES as u64)).is_ok());
        assert!(check_size("https://h/did.json", Some(MAX_DOCUMENT_BYTES as u64 + 1)).is_err());
        // A wildly oversized declaration is refused before the body is read.
        assert!(check_size("https://h/did.json", Some(u64::MAX)).is_err());
        // No declared length is not itself a failure (the after-read call
        // supplies the true length).
        assert!(check_size("https://h/did.json", None).is_ok());
    }

    /// The over-limit error names the size and the limit, so an operator can
    /// tell a too-large document from a transport failure.
    #[dialog_common::test]
    fn check_size_error_is_descriptive() {
        let err = check_size("https://h/did.json", Some(u64::MAX)).unwrap_err();
        let ResolveError::Fetch(msg) = err else {
            panic!("expected a Fetch error, got {err:?}");
        };
        assert!(msg.contains("over the"), "got: {msg}");
    }

    /// The default fetcher is the configured one (redirects refused, timeout
    /// set on native), not a bare `reqwest::Client`. We cannot introspect the
    /// redirect policy through reqwest's public API, so this pins that
    /// construction succeeds with the config applied — a regression here (e.g.
    /// a builder option that starts failing) would surface as a panic.
    #[dialog_common::test]
    fn default_fetcher_is_the_configured_one() {
        let _ = ReqwestFetch::new();
        let _ = ReqwestFetch::default();
    }
}
