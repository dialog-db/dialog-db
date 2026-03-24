//! Credential bridge for UCAN signing.
//!
//! [`Issuer`] adapts an [`Authority`] chain and environment into
//! UCAN's `Principal` + `Signer` interface, allowing the UCAN
//! `InvocationBuilder` to work with the capability system.

use crate::authority::{self, Authority, Operator};
use crate::{Did, Policy, Provider};
use dialog_common::ConditionalSync;
use dialog_varsig::eddsa::Ed25519Signature;

/// Bridge adapter that wraps an authority chain into a UCAN-compatible issuer.
///
/// Implements `Principal` and `Signer<Ed25519Signature>` by delegating
/// signing to the environment via the authority's `Sign` effect.
pub struct Issuer<'a, Env> {
    env: &'a Env,
    /// The authority capability chain (`Subject → Profile → Operator`).
    capability: Authority,
}

impl<'a, Env> Issuer<'a, Env> {
    /// Create an issuer from an authority chain and environment.
    pub fn new(env: &'a Env, capability: Authority) -> Self {
        Self { env, capability }
    }

    /// Get the authority capability chain.
    pub fn capability(&self) -> &Authority {
        &self.capability
    }
}

impl<Env> dialog_varsig::Principal for Issuer<'_, Env> {
    fn did(&self) -> Did {
        Operator::of(&self.capability).operator.clone()
    }
}

impl<Env> dialog_varsig::Signer<Ed25519Signature> for Issuer<'_, Env>
where
    Env: Provider<authority::Sign> + ConditionalSync,
{
    async fn sign(&self, payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        let sign_cap = self
            .capability
            .clone()
            .invoke(authority::Sign::new(payload));

        let bytes = self
            .env
            .execute(sign_cap)
            .await
            .map_err(signature::Error::from_source)?;

        Ed25519Signature::try_from(bytes.as_slice())
    }
}
