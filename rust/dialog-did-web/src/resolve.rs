//! The `Resolve` capability.
//!
//! Resolution is an ambient effect, like `dialog_effects::Identify`: it does
//! not scope to a subject and carries no authorization. Verifying a signature
//! by resolving the signer's DID is not a granted privilege, it is a lookup.
//! The [`Resolve`] command is a pure request; who performs it (and whether the
//! answer is cached, fetched over the network, or parsed locally) is entirely a
//! [`Provider`](dialog_capability::Provider) concern.

use dialog_capability::{Command, Provider};
use dialog_common::ConditionalSync;
use dialog_credentials::Verifier;
use dialog_varsig::Did;

use crate::error::ResolveError;

/// Resolve a DID to its algorithm-agnostic verifier.
///
/// The output is the agnostic [`Verifier`], so a `did:key`, `did:web`, or
/// (later) `did:plc` all resolve to the same type regardless of the key
/// algorithm the DID's document names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolve {
    /// The DID to resolve.
    pub did: Did,
}

impl Resolve {
    /// Resolve the given DID.
    #[must_use]
    pub fn new(did: Did) -> Self {
        Self { did }
    }

    /// Perform this resolution against an env that can provide it.
    ///
    /// # Errors
    ///
    /// Returns whatever [`ResolveError`] the provider produces: an unsupported
    /// method, a fetch failure, a malformed document, or an unsupported key.
    pub async fn perform<Env>(self, env: &Env) -> Result<Verifier, ResolveError>
    where
        Env: Provider<Resolve> + ConditionalSync,
    {
        env.execute(self).await
    }
}

impl Command for Resolve {
    type Input = Self;
    type Output = Result<Verifier, ResolveError>;
}
