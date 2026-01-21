//! Access trait for managing access to capability authorizations.
//!
//! The `Access` trait abstracts over stores that can find authorization
//! proofs for capability claims.

use super::ability::Ability;
use super::authorization::Authorization;
use super::claim::Claim;
use crate::ConditionalSend;

/// Store abstraction that finds delegation chains for capability claims.
///
/// Implementors provide a way to look up authorization proofs. For example,
/// one might search stored delegations to establish a chain from subject
/// to an audience of the claim.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Access {
    /// The authorization type for a given capability type.
    ///
    /// Must implement `Authorization<C>` to prove authority over capability `C`.
    type Authorization<C: Ability + Clone + ConditionalSend + 'static>: Authorization<C>;

    /// Error type for authorization lookup failures.
    type Error;

    /// Find an authorization for the given claim.
    ///
    /// Returns an authorization if one can be found for the `claim.audience`,
    /// otherwise returns an error.
    async fn claim<C: Ability + Clone + ConditionalSend + 'static>(
        &self,
        claim: Claim<C>,
    ) -> Result<Self::Authorization<C>, Self::Error>;
}
