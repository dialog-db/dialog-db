//! Providers for the [`Resolve`](crate::Resolve) capability.
//!
//! Each provider implements
//! [`Provider<Resolve>`](dialog_capability::Provider). They compose by DID
//! method ([`MethodResolver`]) and by policy ([`CachingResolver`]), so a caller
//! only ever does `Resolve::new(did).perform(&env)` and never learns whether
//! the answer came from a local parse, the network, or a cache.

use dialog_capability::Provider;
use dialog_credentials::Verifier;

use crate::document::DidDocument;
use crate::error::ResolveError;
use crate::fetch::{Fetch, ReqwestFetch};
use crate::resolve::Resolve;
use crate::url::did_web_url;
use crate::verifier::MultiVerifier;

/// Resolves `did:key` DIDs locally, with no network access.
#[derive(Debug, Clone, Copy, Default)]
pub struct DidKeyProvider;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Provider<Resolve> for DidKeyProvider {
    async fn execute(&self, input: Resolve) -> Result<MultiVerifier, ResolveError> {
        if input.did.method() != "key" {
            return Err(ResolveError::UnsupportedMethod(input.did.method().into()));
        }
        let verifier = Verifier::from_did_key(input.did.as_str())
            .map_err(|_| ResolveError::UnsupportedKey(input.did.as_str().into()))?;
        // A did:key names exactly one key: a single-member set over the same DID.
        Ok(MultiVerifier::single(input.did, verifier))
    }
}

/// Resolves `did:web` DIDs by fetching the DID document over HTTPS.
///
/// Generic over a [`Fetch`] so the network dependency is swappable (and
/// mockable in tests). [`DidWebProvider::new`] uses [`ReqwestFetch`].
#[derive(Debug, Clone, Default)]
pub struct DidWebProvider<F = ReqwestFetch> {
    fetch: F,
}

impl DidWebProvider<ReqwestFetch> {
    /// A provider backed by the default `reqwest` fetcher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            fetch: ReqwestFetch::new(),
        }
    }
}

impl<F: Fetch> DidWebProvider<F> {
    /// A provider backed by a custom [`Fetch`].
    pub fn with_fetch(fetch: F) -> Self {
        Self { fetch }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<F: Fetch> Provider<Resolve> for DidWebProvider<F> {
    async fn execute(&self, input: Resolve) -> Result<MultiVerifier, ResolveError> {
        if input.did.method() != "web" {
            return Err(ResolveError::UnsupportedMethod(input.did.method().into()));
        }

        let fragment = input.did.as_str().rsplit_once('#').map(|(_, f)| f);
        let did_for_url = fragment.map_or(input.did.as_str(), |frag| {
            &input.did.as_str()[..input.did.as_str().len() - frag.len() - 1]
        });

        // The verifier's identity is the did:web DID (without any #fragment),
        // not any single member key's did:key.
        let subject: dialog_varsig::Did = did_for_url
            .parse()
            .map_err(|_| ResolveError::MalformedDid(did_for_url.into()))?;

        let url = did_web_url(did_for_url)?;
        let body = self.fetch.get(&url).await?;
        let document: DidDocument = serde_json::from_slice(&body)
            .map_err(|e| ResolveError::MalformedDocument(e.to_string()))?;
        document.verifier(&subject, fragment)
    }
}

/// Routes a resolution to a sub-provider by the DID's method.
///
/// The varsig `CompositeResolver` composes by signature type, not DID method,
/// so it cannot express "`did:key` here, `did:web` there". This does.
#[derive(Debug, Clone, Default)]
pub struct MethodResolver<K = DidKeyProvider, W = DidWebProvider> {
    key: K,
    web: W,
}

impl MethodResolver<DidKeyProvider, DidWebProvider> {
    /// A resolver that handles `did:key` locally and `did:web` over the network
    /// with the default fetcher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            key: DidKeyProvider,
            web: DidWebProvider::new(),
        }
    }
}

impl<K, W> MethodResolver<K, W> {
    /// A resolver from explicit `did:key` and `did:web` providers.
    pub fn with_providers(key: K, web: W) -> Self {
        Self { key, web }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<K, W> Provider<Resolve> for MethodResolver<K, W>
where
    K: Provider<Resolve> + dialog_common::ConditionalSync,
    W: Provider<Resolve> + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Resolve) -> Result<MultiVerifier, ResolveError> {
        match input.did.method() {
            "key" => self.key.execute(input).await,
            "web" => self.web.execute(input).await,
            other => Err(ResolveError::UnsupportedMethod(other.into())),
        }
    }
}

/// Convenience alias for the default network-capable resolver: `did:key`
/// locally, `did:web` over `reqwest`.
pub type WebResolver = MethodResolver<DidKeyProvider, DidWebProvider>;
