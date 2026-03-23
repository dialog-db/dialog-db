//! Credential bridge for UCAN signing.
//!
//! [`Issuer`] adapts capability-based credential effects into
//! UCAN's `Principal` + `Signer` interface, allowing the UCAN
//! `InvocationBuilder` to work with the capability system.

use crate::credential::{self, AuthorizeError};
use crate::{Did, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_varsig::eddsa::Ed25519Signature;

/// Bridge adapter that wraps credential effects into a UCAN-compatible issuer.
///
/// Implements `Principal` and `Signer<Ed25519Signature>` by delegating to the
/// credential effects on the environment.
pub struct Issuer<'a, Env> {
    env: &'a Env,
    subject: Did,
    /// The operator's DID (cached from Identify).
    pub cached_did: Did,
}

impl<'a, Env> Issuer<'a, Env>
where
    Env: Provider<credential::Identify> + Provider<credential::Sign> + ConditionalSync,
{
    /// Create a new issuer by discovering the operator's identity for a subject.
    ///
    /// The `subject` is the DID of the resource being acted upon — the operator
    /// identity is discovered via the subject's credential chain.
    pub async fn for_subject(
        env: &'a Env,
        subject: Did,
    ) -> Result<Issuer<'a, Env>, AuthorizeError> {
        let identify_cap = Subject::from(subject.clone())
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Identify);

        let detail = <Env as Provider<credential::Identify>>::execute(env, identify_cap)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
        let did = detail.operator;

        Ok(Issuer {
            env,
            subject,
            cached_did: did,
        })
    }
}

impl<Env> dialog_varsig::Principal for Issuer<'_, Env> {
    fn did(&self) -> Did {
        self.cached_did.clone()
    }
}

impl<Env> dialog_varsig::Signer<Ed25519Signature> for Issuer<'_, Env>
where
    Env: Provider<credential::Sign> + ConditionalSync,
{
    async fn sign(&self, payload: &[u8]) -> Result<Ed25519Signature, signature::Error> {
        let sign_cap = Subject::from(self.subject.clone())
            .attenuate(credential::Credential)
            .attenuate(credential::Profile::default())
            .invoke(credential::Sign::new(payload));

        let bytes = self
            .env
            .execute(sign_cap)
            .await
            .map_err(signature::Error::from_source)?;

        Ed25519Signature::try_from(bytes.as_slice())
    }
}
