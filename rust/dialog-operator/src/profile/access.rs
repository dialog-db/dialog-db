//! Access API for profile-based UCAN delegation.
//!
//! Provides a fluent builder chain for claiming authority and delegating.

use dialog_capability::access::{self, Authorization as _, AuthorizeError, Proof as _};
use dialog_capability::{Ability, Capability, Constraint, Provider, Subject};
use dialog_common::ConditionalSync;
use dialog_credentials::SignerCredential;
use dialog_ucan::scope::Scope;
use dialog_ucan::{Ucan, UcanDelegation, UcanProof};
use dialog_ucan_core::time::Timestamp;
use dialog_varsig::{Did, Principal};

/// Access handle scoped to a profile's credential.
///
/// Created via [`Profile::access()`](super::Profile::access).
pub struct Access<'a> {
    credential: &'a SignerCredential,
}

impl<'a> Access<'a> {
    /// Create an access handle from a signer credential.
    pub fn new(credential: &'a SignerCredential) -> Self {
        Self { credential }
    }
}

impl<'a> Access<'a> {
    /// Claim authority over a capability.
    pub fn claim<C: Constraint>(&self, capability: impl Into<Capability<C>>) -> Claim<'a, C> {
        Claim {
            by: self.credential,
            capability: capability.into(),
            not_before: None,
            expiration: None,
        }
    }

    /// Save a delegation chain under this profile.
    pub fn save(&self, chain: UcanDelegation) -> super::SaveDelegation {
        super::SaveDelegation {
            did: self.credential.did(),
            chain,
        }
    }
}

/// A claimed capability with optional time bounds.
///
/// Can be executed directly via [`.perform()`](Claim::perform) to get a
/// proof chain, or chained into [`.delegate()`](Claim::delegate) to
/// produce a delegation.
pub struct Claim<'a, C: Constraint> {
    by: &'a SignerCredential,
    capability: Capability<C>,
    not_before: Option<Timestamp>,
    expiration: Option<Timestamp>,
}

impl<'a, C: Constraint> Claim<'a, C> {
    /// Set the earliest time the claim is valid.
    pub fn not_before(mut self, not_before: Timestamp) -> Self {
        self.not_before = Some(not_before);
        self
    }

    /// Set when the claim expires.
    pub fn expires(mut self, expiration: Timestamp) -> Self {
        self.expiration = Some(expiration);
        self
    }

    /// Chain into a delegation to the given audience.
    pub fn delegate(self, audience: impl Into<Did>) -> Delegate<'a, C> {
        Delegate {
            claim: self,
            audience: audience.into(),
        }
    }

    /// Chain into an invocation.
    pub fn invoke(self) -> Invoke<'a, C> {
        Invoke { claim: self }
    }
}

impl<C: Constraint> Claim<'_, C>
where
    Capability<C>: Ability,
{
    fn duration(&self) -> access::TimeRange {
        access::TimeRange {
            not_before: self.not_before.map(|t| t.to_unix()),
            expiration: self.expiration.map(|t| t.to_unix()),
        }
    }

    /// Execute the claim, returning a proof chain.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanProof, AuthorizeError>
    where
        Env: Provider<access::Prove<Ucan>> + ConditionalSync,
    {
        let scope = Scope::from(&self.capability);
        let duration = self.duration();
        let mut claim = access::Prove::<Ucan>::new(self.by.did(), scope);
        claim.duration = duration;
        Subject::from(self.by.did())
            .attenuate(access::Access)
            .invoke(claim)
            .perform(env)
            .await
    }
}

/// An invocation request combining a claim with signing.
///
/// Execute via [`.perform()`](Invoke::perform) to claim authority,
/// bind the profile signer, and produce a signed UCAN invocation.
pub struct Invoke<'a, C: Constraint> {
    claim: Claim<'a, C>,
}

impl<C: Constraint> Invoke<'_, C>
where
    Capability<C>: Ability,
{
    /// Claim authority, sign, and produce a UCAN invocation.
    pub async fn perform<Env>(
        self,
        env: &Env,
    ) -> Result<dialog_ucan::UcanInvocation, AuthorizeError>
    where
        Env: Provider<access::Prove<Ucan>> + ConditionalSync,
    {
        let signer = self.claim.by.signer().clone();
        let proof_chain = self.claim.perform(env).await?;
        let authorization = proof_chain.claim(signer)?;
        authorization.invoke().await
    }
}

/// A delegation request combining a claim with a target audience.
///
/// Execute via [`.perform()`](Delegate::perform) to claim authority,
/// bind the profile signer, and produce a signed delegation chain.
pub struct Delegate<'a, C: Constraint> {
    claim: Claim<'a, C>,
    audience: Did,
}

impl<C: Constraint> Delegate<'_, C>
where
    Capability<C>: Ability,
{
    /// Claim authority, sign a delegation, and return the chain.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanDelegation, AuthorizeError>
    where
        Env: Provider<access::Prove<Ucan>> + ConditionalSync,
    {
        let signer = self.claim.by.signer().clone();
        let duration = self.claim.duration();
        let proof_chain = self.claim.perform(env).await?;
        let mut authorization = proof_chain.claim(signer)?;
        if let Some(nbf) = duration.not_before {
            authorization = authorization.not_before(nbf)?;
        }
        if let Some(exp) = duration.expiration {
            authorization = authorization.expires(exp)?;
        }
        authorization.delegate(self.audience).await
    }
}
