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
use crate::url::{did_plc_url, did_web_url};
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

/// Resolves `did:plc` DIDs by fetching the DID document from the PLC directory.
///
/// A `did:plc` is resolved with a single GET to
/// `https://plc.directory/did:plc:<identifier>`, which returns a W3C DID
/// document of the same shape as a `did:web` document (a `verificationMethod`
/// array of `Multikey` entries carrying `publicKeyMultibase`). So this provider
/// is deliberately `did:web` with a different URL derivation and method guard:
/// it reuses [`DidDocument`] and [`MultiVerifier`] unchanged.
///
/// # Trust model
///
/// This provider TRUSTS the PLC directory. `did:plc` is defined by an
/// append-only operation log with an auditable signature chain, but this
/// resolver does NOT fetch, replay, or validate that log. It takes the document
/// `plc.directory` returns as authoritative for the DID. This is a deliberate
/// design choice (the directory is the trust anchor), not an oversight: a future
/// reader must not "fix" it by adding unrequested audit-chain validation without
/// revisiting this decision.
///
/// Generic over a [`Fetch`] so the network dependency is swappable (and
/// mockable in tests). [`DidPlcProvider::new`] uses [`ReqwestFetch`].
#[derive(Debug, Clone, Default)]
pub struct DidPlcProvider<F = ReqwestFetch> {
    fetch: F,
}

impl DidPlcProvider<ReqwestFetch> {
    /// A provider backed by the default `reqwest` fetcher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            fetch: ReqwestFetch::new(),
        }
    }
}

impl<F: Fetch> DidPlcProvider<F> {
    /// A provider backed by a custom [`Fetch`].
    pub fn with_fetch(fetch: F) -> Self {
        Self { fetch }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<F: Fetch> Provider<Resolve> for DidPlcProvider<F> {
    async fn execute(&self, input: Resolve) -> Result<MultiVerifier, ResolveError> {
        if input.did.method() != "plc" {
            return Err(ResolveError::UnsupportedMethod(input.did.method().into()));
        }

        let fragment = input.did.as_str().rsplit_once('#').map(|(_, f)| f);
        let did_for_url = fragment.map_or(input.did.as_str(), |frag| {
            &input.did.as_str()[..input.did.as_str().len() - frag.len() - 1]
        });

        // The verifier's identity is the did:plc DID (without any #fragment),
        // not any single member key's did:key.
        let subject: dialog_varsig::Did = did_for_url
            .parse()
            .map_err(|_| ResolveError::MalformedDid(did_for_url.into()))?;

        let url = did_plc_url(did_for_url)?;
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
pub struct MethodResolver<K = DidKeyProvider, W = DidWebProvider, P = DidPlcProvider> {
    key: K,
    web: W,
    plc: P,
}

impl MethodResolver<DidKeyProvider, DidWebProvider, DidPlcProvider> {
    /// A resolver that handles `did:key` locally and `did:web`/`did:plc` over
    /// the network with the default fetcher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            key: DidKeyProvider,
            web: DidWebProvider::new(),
            plc: DidPlcProvider::new(),
        }
    }
}

impl<K, W, P> MethodResolver<K, W, P> {
    /// A resolver from explicit `did:key`, `did:web`, and `did:plc` providers.
    pub fn with_providers(key: K, web: W, plc: P) -> Self {
        Self { key, web, plc }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<K, W, P> Provider<Resolve> for MethodResolver<K, W, P>
where
    K: Provider<Resolve> + dialog_common::ConditionalSync,
    W: Provider<Resolve> + dialog_common::ConditionalSync,
    P: Provider<Resolve> + dialog_common::ConditionalSync,
{
    async fn execute(&self, input: Resolve) -> Result<MultiVerifier, ResolveError> {
        match input.did.method() {
            "key" => self.key.execute(input).await,
            "web" => self.web.execute(input).await,
            "plc" => self.plc.execute(input).await,
            other => Err(ResolveError::UnsupportedMethod(other.into())),
        }
    }
}

/// Convenience alias for the default network-capable resolver: `did:key`
/// locally, `did:web` and `did:plc` over `reqwest`.
pub type WebResolver = MethodResolver<DidKeyProvider, DidWebProvider, DidPlcProvider>;
