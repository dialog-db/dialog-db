//! Access capability hierarchy — authorization for remote execution.
//!
//! Provides the [`Access`] attenuation and [`Authorize`] effect for
//! authorizing capabilities before sending them to remote sites.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (operator DID)
//! └── Access (ability: /access)
//!     └── Authorize<Fx, F> { capability } -> Effect -> Result<Authorization<Fx, F>, AuthorizeError>
//! ```

use crate::{Attenuation, Capability, Claim, Constraint, Effect};
use dialog_common::ConditionalSend;
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use thiserror::Error;

pub use crate::Subject;

/// Root attenuation for access/authorization operations.
///
/// Attaches to Subject and provides the `/access` ability path segment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Access;

impl Attenuation for Access {
    type Of = Subject;
}

/// Allow — trivial authorization that requires no proof.
///
/// Used by sites like S3 and Local where authorization is implicit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Allow;

/// Trait for describing access protocol in terms of authorization formats.
///
/// Different authorization schemes produce different proof types:
/// - [`Allow`]: no extra material (`Authorization<Fx> = ()`)
/// - UCAN format: a signed invocation chain
pub trait Protocol: ConditionalSend + 'static {
    /// The authorization material produced for a given capability.
    type Authorization<Fx: Constraint>: ConditionalSend;
}

impl Protocol for Allow {
    type Authorization<Fx: Constraint> = ();
}

/// Authorized capability with format-specific proof material.
///
/// Created by `Provider<Authorize<Fx, F>>`.
pub struct Authorization<Fx: Constraint, F: Protocol = Allow> {
    /// The authorized capability.
    pub capability: Capability<Fx>,
    /// The format-specific authorization material.
    pub authorization: F::Authorization<Fx>,
}

impl<Fx: Constraint, F: Protocol> Authorization<Fx, F> {
    /// Create a new authorization from a capability and format-specific material.
    pub fn new(capability: Capability<Fx>, authorization: F::Authorization<Fx>) -> Self {
        Self {
            capability,
            authorization,
        }
    }

    /// Unwrap the authorized capability, discarding the proof.
    pub fn into_inner(self) -> Capability<Fx> {
        self.capability
    }
}

impl<Fx: Constraint, F: Protocol> std::ops::Deref for Authorization<Fx, F> {
    type Target = Capability<Fx>;
    fn deref(&self) -> &Self::Target {
        &self.capability
    }
}

/// Authorize a capability for remote execution.
///
/// The `F` type parameter selects the authorization format (Allow, Ucan, etc.).
#[derive(Serialize, Deserialize)]
#[serde(bound(deserialize = ""))]
pub struct Authorize<Fx: Constraint, F: Protocol = Allow> {
    /// The capability to authorize.
    pub capability: Capability<Fx>,
    /// The target format (used for routing to the correct provider).
    #[serde(skip)]
    _format: PhantomData<F>,
}

impl<Fx: Constraint, F: Protocol> Authorize<Fx, F> {
    /// Create a new authorization request for the given capability and format.
    pub fn new(capability: Capability<Fx>) -> Self {
        Self {
            capability,
            _format: PhantomData,
        }
    }
}

impl<Fx, F> Claim for Authorize<Fx, F>
where
    Fx: Effect,
    Fx::Of: Constraint,
    F: Protocol,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + 'static,
{
    type Claim = Self;
    fn claim(self) -> Self {
        self
    }
}

impl<Fx, F> Effect for Authorize<Fx, F>
where
    Fx: Effect,
    Fx::Of: Constraint,
    F: Protocol,
    Capability<Fx>: ConditionalSend,
    Self: ConditionalSend + 'static,
{
    type Of = Access;
    type Output = Result<Authorization<Fx, F>, AuthorizeError>;
}

/// Error during the authorize step.
#[derive(Debug, Error)]
pub enum AuthorizeError {
    /// Authorization was denied.
    #[error("Authorization denied: {0}")]
    Denied(String),

    /// Configuration error (e.g., missing delegation chain).
    #[error("Authorization configuration error: {0}")]
    Configuration(String),
}

/// Blanket impl: any type can authorize with `Allow` format (no proof needed).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<Env, Fx> crate::Provider<Authorize<Fx, Allow>> for Env
where
    Fx: crate::Effect + 'static,
    Fx::Of: Constraint,
    Capability<Fx>: ConditionalSend,
    Authorize<Fx, Allow>: ConditionalSend + 'static,
    Env: ConditionalSend + dialog_common::ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Authorize<Fx, Allow>>,
    ) -> Result<Authorization<Fx, Allow>, AuthorizeError> {
        let auth_request = input.into_inner().constraint;
        Ok(Authorization::new(auth_request.capability, ()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::did;

    #[test]
    fn it_builds_access_claim_path() {
        let claim = Subject::from(did!("key:zSpace")).attenuate(Access);
        assert_eq!(claim.subject(), &did!("key:zSpace"));
        assert_eq!(claim.ability(), "/access");
    }
}
