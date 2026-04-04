//! Builder API for UCAN delegations and invocations.
//!
//! Provides [`DelegateRequest`] and [`InvokeRequest`] with typestate tracking
//! of whether an explicit issuer has been provided. When issuer is set,
//! `perform` needs `Provider<Claim<Ucan>>` bounds; when unset, it also needs
//! `Provider<Identify>` and `Provider<Sign>`.

use dialog_capability::access::{self, AuthorizeError, Authorization as _, ProofChain as _};
use dialog_capability::{Ability, Capability, Did, Effect, Provider, Subject, authority};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_ucan::time::Timestamp;
use dialog_ucan::DelegationChain;
use dialog_varsig::Principal;
use dialog_varsig::eddsa::Ed25519Signature;

use super::{Ucan, UcanInvocation};
use super::scope::Scope;

/// No issuer provided — `perform` resolves via `Identify` + `Sign`.
pub struct IssuerUnset;

/// Builder for a UCAN delegation.
///
/// Created via [`Ucan::delegate()`](super::Ucan::delegate). Use `.issuer()` to
/// provide an explicit signer, or leave it unset to resolve via `Identify`/`Sign`.
pub struct DelegateRequest<I = IssuerUnset> {
    scope: Scope,
    audience: Option<Did>,
    issuer: I,
    expiration: Option<Timestamp>,
    not_before: Option<Timestamp>,
}

impl DelegateRequest<IssuerUnset> {
    pub(crate) fn new(capability: &impl Ability) -> Self {
        Self {
            scope: Scope::from(capability),
            audience: None,
            issuer: IssuerUnset,
            expiration: None,
            not_before: None,
        }
    }

    /// Set an explicit issuer (signer) for the delegation.
    pub fn issuer(self, signer: Ed25519Signer) -> DelegateRequest<Ed25519Signer> {
        DelegateRequest {
            scope: self.scope,
            audience: self.audience,
            issuer: signer,
            expiration: self.expiration,
            not_before: self.not_before,
        }
    }
}

impl<I> DelegateRequest<I> {
    /// Set the audience (recipient) of the delegation.
    pub fn audience(mut self, audience: impl Into<Did>) -> Self {
        self.audience = Some(audience.into());
        self
    }

    /// Set when the delegation expires.
    pub fn expires(mut self, expiration: Timestamp) -> Self {
        self.expiration = Some(expiration);
        self
    }

    /// Set the earliest time the delegation becomes valid.
    pub fn not_before(mut self, not_before: Timestamp) -> Self {
        self.not_before = Some(not_before);
        self
    }
}

impl DelegateRequest<Ed25519Signer> {
    /// Sign the delegation and return the chain.
    ///
    /// Uses `Claim<Ucan>` to discover the delegation chain, then signs
    /// a new delegation extending it.
    pub async fn perform<Env>(self, env: &Env) -> Result<DelegationChain, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<access::Claim<Ucan>>
            + ConditionalSync,
    {
        use dialog_capability::Policy;

        let audience = self.audience.ok_or_else(|| {
            AuthorizeError::Configuration("delegation requires an audience".into())
        })?;

        // Discover profile DID via Identify
        let auth = Subject::from(self.issuer.did())
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();

        // Claim authorization via the new system
        let issuer_did = self.issuer.did();
        let proof_chain = Subject::from(profile_did)
            .attenuate(access::Permit)
            .invoke(access::Claim::<Ucan>::new(issuer_did, self.scope))
            .perform(env)
            .await?;

        // Bind signer and delegate
        let authorization = proof_chain.claim(self.issuer)?;
        authorization.delegate(audience).await
    }
}

impl DelegateRequest<IssuerUnset> {
    /// Sign the delegation, resolving issuer via environment.
    ///
    /// Requires `Identify` and `Sign` to discover and use the profile signer.
    pub async fn perform<Env>(self, env: &Env) -> Result<DelegationChain, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<access::Claim<Ucan>>
            + ConditionalSync,
    {
        let audience = self.audience.ok_or_else(|| {
            AuthorizeError::Configuration("delegation requires an audience".into())
        })?;

        // Use a dummy DID for Identify when subject is Any
        let lookup_did = resolve_lookup_did(&self.scope);

        let auth = Subject::from(lookup_did)
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();
        let operator_did = authority::Operator::of(&auth).operator.clone();

        // Claim authorization
        let proof_chain = Subject::from(profile_did)
            .attenuate(access::Permit)
            .invoke(access::Claim::<Ucan>::new(operator_did, self.scope))
            .perform(env)
            .await?;

        // Sign using the authority chain (via Sign effect)
        let issuer = super::issuer::Issuer::new(env, auth);
        let signer = Ed25519Signer::from_signer(&issuer)
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("signing failed: {e}")))?;
        let authorization = proof_chain.claim(signer)?;
        authorization.delegate(audience).await
    }
}

/// Builder for a UCAN invocation.
pub struct InvokeRequest<I = IssuerUnset> {
    scope: Scope,
    issuer: I,
}

impl InvokeRequest<IssuerUnset> {
    pub(crate) fn new<Fx>(capability: &Capability<Fx>) -> Self
    where
        Fx: Effect + Clone,
        Capability<Fx>: Ability,
    {
        Self {
            scope: Scope::invoke(capability),
            issuer: IssuerUnset,
        }
    }

    /// Set an explicit issuer.
    pub fn issuer<S>(self, signer: S) -> InvokeRequest<S>
    where
        S: dialog_varsig::Signer<Ed25519Signature> + Principal,
    {
        InvokeRequest {
            scope: self.scope,
            issuer: signer,
        }
    }
}

impl<S> InvokeRequest<S>
where
    S: dialog_varsig::Signer<Ed25519Signature> + Principal + Clone + Send + Sync,
{
    /// Build and sign the invocation.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanInvocation, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<access::Claim<Ucan>>
            + ConditionalSync,
    {
        // Discover profile DID
        let auth = Subject::from(self.issuer.did())
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();
        let issuer_did = self.issuer.did();

        // Claim authorization
        let proof_chain = Subject::from(profile_did)
            .attenuate(access::Permit)
            .invoke(access::Claim::<Ucan>::new(issuer_did, self.scope))
            .perform(env)
            .await?;

        // Bind signer and invoke
        let signer: Ed25519Signer = to_ed25519_signer(&self.issuer)
            .ok_or_else(|| AuthorizeError::Configuration("issuer must be Ed25519Signer".into()))?;
        let authorization = proof_chain.claim(signer)?;
        authorization.invoke().await
    }
}

impl InvokeRequest<IssuerUnset> {
    /// Build and sign the invocation, resolving issuer via environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanInvocation, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<access::Claim<Ucan>>
            + ConditionalSync,
    {
        let lookup_did = resolve_lookup_did(&self.scope);

        let auth = Subject::from(lookup_did)
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();
        let operator_did = authority::Operator::of(&auth).operator.clone();

        // Claim authorization
        let proof_chain = Subject::from(profile_did)
            .attenuate(access::Permit)
            .invoke(access::Claim::<Ucan>::new(operator_did, self.scope))
            .perform(env)
            .await?;

        // Sign via authority chain
        let issuer = super::issuer::Issuer::new(env, auth);
        let signer = Ed25519Signer::from_signer(&issuer)
            .await
            .map_err(|e| AuthorizeError::Configuration(format!("signing failed: {e}")))?;
        let authorization = proof_chain.claim(signer)?;
        authorization.invoke().await
    }
}

fn resolve_lookup_did(scope: &Scope) -> Did {
    use dialog_ucan::subject::Subject as UcanSubject;
    match &scope.subject {
        UcanSubject::Specific(did) => did.clone(),
        UcanSubject::Any => dialog_capability::did!("key:zDummy").into(),
    }
}

/// Try to extract an Ed25519Signer from a generic signer.
///
/// This works when the signer IS an Ed25519Signer. For the Issuer adapter
/// (environment-based signing), we need a different path.
fn to_ed25519_signer<S>(signer: &S) -> Option<Ed25519Signer>
where
    S: Principal + Clone,
{
    // For now, we rely on the signer being an Ed25519Signer via downcast.
    // This is a limitation — the proper fix is the Claim Projection refactor
    // where the signer travels through the capability chain.
    None // TODO: implement proper signer extraction
}
