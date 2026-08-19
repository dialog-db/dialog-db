//! A multi-key verifier for DIDs that resolve to a key *set*.
//!
//! A `did:web` (and, later, `did:plc`) document names a whole array of
//! verification methods. Any one of them may have produced a given signature,
//! and a resolver cannot know which. [`MultiVerifier`] holds every supported
//! key the document named and verifies a signature if *any* of them verifies.
//!
//! Verification is header-authoritative: the [`AnySignature`] already carries
//! its algorithm tag, and each member [`Verifier`] rejects a signature whose tag
//! does not match its own arm (the existing agnostic-verifier behavior). So the
//! algorithm pruning falls out for free: trying every member only ever succeeds
//! on a key of the signature's own algorithm, and no algorithm guessing is
//! reintroduced here.
//!
//! `did:key` resolves to a single key, which is just a [`MultiVerifier`] of one
//! member, so every DID method shares this one return type.
//!
//! # Future: `kid`-hint fast path
//!
//! A DID document's verification methods each carry a fragment id (`#key-1`).
//! [`DidDocument::verifier`](crate::DidDocument::verifier) already accepts an
//! optional fragment and, when given, restricts the built verifier to that one
//! method. A future optimization could stamp the chosen method's fragment as a
//! `kid` hint in the UCAN metadata at signing time; verification would then
//! pre-select that single method via the fragment path and fall back to the
//! multi-key try-all only when the hint is absent or fails to verify. That
//! signing-side change is intentionally not implemented here; the resolver seam
//! (`verifier(Some(kid))`) is already in place for it.

use dialog_credentials::Verifier;
use dialog_varsig::{AnySignature, Did, Principal, Verifier as VarsigVerifier};

/// A verifier over a set of candidate keys that accepts a signature if *any*
/// member key verifies it.
#[derive(Debug, Clone, PartialEq)]
pub struct MultiVerifier {
    /// The DID this set was resolved for. Reported as the verifier's identity,
    /// rather than any single member key's `did:key`.
    did: Did,

    /// The candidate keys, in document order. Never empty: a resolver that finds
    /// no supported key errors instead of building an empty set.
    keys: Vec<Verifier>,
}

impl MultiVerifier {
    /// Build a multi-key verifier for `did` from the given candidate keys.
    ///
    /// The keys should all be supported (a resolver skips unsupported methods
    /// before calling this). An empty set is accepted structurally but will
    /// verify nothing; resolvers guard against it upstream.
    #[must_use]
    pub fn new(did: Did, keys: Vec<Verifier>) -> Self {
        Self { did, keys }
    }

    /// Build a single-key verifier, as `did:key` resolution produces.
    #[must_use]
    pub fn single(did: Did, key: Verifier) -> Self {
        Self {
            did,
            keys: vec![key],
        }
    }

    /// The candidate keys this verifier will try, in document order.
    #[must_use]
    pub fn keys(&self) -> &[Verifier] {
        &self.keys
    }
}

impl Principal for MultiVerifier {
    fn did(&self) -> Did {
        self.did.clone()
    }
}

impl VarsigVerifier<AnySignature> for MultiVerifier {
    async fn verify(
        &self,
        payload: &[u8],
        signature: &AnySignature,
    ) -> Result<(), signature::Error> {
        for key in &self.keys {
            if key.verify(payload, signature).await.is_ok() {
                return Ok(());
            }
        }
        Err(signature::Error::new())
    }
}
