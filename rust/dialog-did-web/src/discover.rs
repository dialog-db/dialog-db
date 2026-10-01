//! The `Discover` capability.
//!
//! Where [`Resolve`](crate::Resolve) answers what a DID's subject signs with,
//! [`Discover`] answers where it is reached: the services its document names.
//! Like resolution it is an ambient lookup, scoped to no subject and carrying
//! no authorization, and who performs it is a provider concern.

use dialog_capability::{Command, Provider};
use dialog_common::ConditionalSync;
use dialog_varsig::Did;

use crate::document::Service;
use crate::error::ResolveError;

/// Discover the services a DID's subject is reached at.
///
/// A DID with no document to name them (a `did:key`) has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Discover {
    /// The DID to discover.
    pub did: Did,
}

impl Discover {
    /// Discover the given DID.
    #[must_use]
    pub fn new(did: Did) -> Self {
        Self { did }
    }

    /// Perform this discovery against an env that can provide it.
    ///
    /// # Errors
    ///
    /// Returns whatever [`ResolveError`] the provider produces: an unsupported
    /// method, a fetch failure, or a malformed document.
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Service>, ResolveError>
    where
        Env: Provider<Discover> + ConditionalSync,
    {
        env.execute(self).await
    }
}

impl Command for Discover {
    type Input = Self;
    type Output = Result<Vec<Service>, ResolveError>;
}
