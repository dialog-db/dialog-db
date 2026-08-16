//! `did:web` resolution as an algorithm-agnostic [`Resolve`] capability.
//!
//! This crate resolves a DID to an algorithm-agnostic
//! [`Verifier`](dialog_credentials::Verifier). Resolution is expressed as an
//! ambient [`Resolve`] capability performed against a
//! [`Provider`](dialog_capability::Provider): `did:key` is parsed locally,
//! `did:web` fetches the DID document over HTTPS, a [`MethodResolver`] routes by
//! DID method, and a [`CachingResolver`] layers a TTL cache on top. The caller
//! only writes `Resolve::new(did).perform(&env)`; where the answer comes from is
//! entirely a provider concern.
//!
//! ```no_run
//! # use dialog_did_web::{Resolve, WebResolver};
//! # use dialog_varsig::Did;
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let env = WebResolver::new();
//! let did: Did = "did:web:example.com".parse()?;
//! let verifier = Resolve::new(did).perform(&env).await?;
//! # let _ = verifier;
//! # Ok(())
//! # }
//! ```

mod cache;
mod document;
mod error;
mod fetch;
mod provider;
mod resolve;
mod url;

#[cfg(test)]
mod tests;

pub use cache::{CachingResolver, DEFAULT_NEGATIVE_TTL, DEFAULT_TTL};
pub use document::{DidDocument, Jwk, VerificationMethod};
pub use error::ResolveError;
pub use fetch::{Fetch, ReqwestFetch};
pub use provider::{DidKeyProvider, DidWebProvider, MethodResolver, WebResolver};
pub use resolve::Resolve;
pub use url::did_web_url;

#[cfg(feature = "test-fetch")]
pub use fetch::MapFetch;
