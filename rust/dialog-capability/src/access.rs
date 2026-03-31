//! Access capability hierarchy — authorization for remote execution.
//!
//! Provides the [`Access`] attenuation and [`Authorizer`] trait for
//! authorizing capabilities before sending them to remote sites.
//!
//! Authorization is dispatched through [`Protocol`] types (e.g. [`Allow`],
//! `Ucan`) via the [`Authorizer`] trait, rather than through the
//! `Provider` effect system.

use crate::{Attenuation, Capability, Constraint, Effect};
use dialog_common::{ConditionalSend, ConditionalSync};
use serde::{Deserialize, Serialize};
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

use crate::{Provider, authority, storage};

/// Access protocol — defines how authorization is produced.
///
/// Different protocols produce different proof types:
/// - [`Allow`]: no extra material (`Authorization<Fx> = ()`)
/// - UCAN: a signed delegation chain
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Protocol: Sized + ConditionalSend + 'static {
    /// The authorization material produced for a given capability.
    type Authorization<Fx: Constraint>: ConditionalSend;

    /// Authorize a capability, producing format-specific proof material.
    async fn authorize<Fx, Env>(
        env: &Env,
        capability: Capability<Fx>,
    ) -> Result<Authorization<Fx, Self>, AuthorizeError>
    where
        Fx: Effect + Clone + ConditionalSend + 'static,
        Capability<Fx>: crate::Ability + ConditionalSend + ConditionalSync,
        Env: Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Protocol for Allow {
    type Authorization<Fx: Constraint> = ();

    async fn authorize<Fx, Env>(
        _env: &Env,
        capability: Capability<Fx>,
    ) -> Result<Authorization<Fx, Self>, AuthorizeError>
    where
        Fx: Effect + Clone + ConditionalSend + 'static,
        Capability<Fx>: crate::Ability + ConditionalSend + ConditionalSync,
        Env: Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        Ok(Authorization::new(capability, ()))
    }
}

/// Authorized capability with format-specific proof material.
///
/// Created by [`Authorizer::authorize`].
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
