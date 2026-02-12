//! DID-to-verifier resolution.

use std::future::Future;

use crate::{
    did::Did,
    signature::{Signature, Verifier},
};

/// Type-level index for resolver composition (selects the left resolver).
#[derive(Debug, Clone, Copy)]
pub struct Here;

/// Type-level index for resolver composition (selects the right resolver).
#[derive(Debug, Clone, Copy)]
pub struct There<T>(std::marker::PhantomData<T>);

/// Resolves a DID to a [`Verifier`] for signature type `S`.
///
/// Given a DID string, looks up or derives the public key material
/// needed to verify signatures. Async to support network-based
/// DID methods (e.g. did:web, did:plc).
///
/// The `Index` parameter enables type-level dispatch in
/// [`CompositeResolver`] and should be left at its default.
pub trait Resolver<S: Signature, Index = Here> {
    /// Error type for resolution failures.
    type Error: std::error::Error;

    /// Resolve a DID to a verifier for signature type `S`.
    fn resolve(&self, did: &Did) -> impl Future<Output = Result<impl Verifier<S>, Self::Error>>;

    /// Combine with another resolver that handles a different signature type.
    ///
    /// ```ignore
    /// let resolver = ed25519_resolver.or(p256_resolver).or(rsa_resolver);
    /// ```
    fn or<R>(self, other: R) -> CompositeResolver<Self, R>
    where
        Self: Sized,
    {
        CompositeResolver(self, other)
    }
}

/// A resolver that combines two inner resolvers, each handling
/// different signature types. The compiler selects the correct
/// inner resolver at the call site based on which `S: Signature`
/// is being resolved.
///
/// Built via [`Resolver::or`].
#[derive(Debug, Clone, Copy)]
pub struct CompositeResolver<L, R>(pub L, pub R);

impl<S: Signature, L, R> Resolver<S, Here> for CompositeResolver<L, R>
where
    L: Resolver<S, Here>,
{
    type Error = L::Error;

    fn resolve(&self, did: &Did) -> impl Future<Output = Result<impl Verifier<S>, Self::Error>> {
        self.0.resolve(did)
    }
}

impl<S: Signature, Idx, L, R> Resolver<S, There<Idx>> for CompositeResolver<L, R>
where
    R: Resolver<S, Idx>,
{
    type Error = R::Error;

    fn resolve(&self, did: &Did) -> impl Future<Output = Result<impl Verifier<S>, Self::Error>> {
        self.1.resolve(did)
    }
}
