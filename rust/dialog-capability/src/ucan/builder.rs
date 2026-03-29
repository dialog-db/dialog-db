//! Builder API for UCAN delegations and invocations.
//!
//! Provides [`DelegateRequest`] and [`InvokeRequest`] with typestate tracking
//! of whether an explicit issuer has been provided. When issuer is set,
//! `perform` only needs storage bounds; when unset, it also needs
//! `Provider<Identify>` and `Provider<Sign>`.

use crate::access::AuthorizeError;
use crate::{Ability, Did, Policy, Provider, Subject, authority, storage};
use dialog_common::ConditionalSync;
use dialog_ucan::time::Timestamp;
use dialog_ucan::{DelegationChain, InvocationChain};
use dialog_varsig::Principal;
use dialog_varsig::eddsa::Ed25519Signature;

use super::UcanInvocation;
use super::claim::find_chain;
use super::delegation::import_delegation_chain;
use super::issuer::Issuer;
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
    ///
    /// The signer must implement `Signer<Ed25519Signature> + Principal`.
    pub fn issuer<S>(self, signer: S) -> DelegateRequest<S>
    where
        S: dialog_varsig::Signer<Ed25519Signature> + Principal,
    {
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

impl<S> DelegateRequest<S>
where
    S: dialog_varsig::Signer<Ed25519Signature> + Principal,
{
    /// Sign and store the delegation.
    ///
    /// When issuer is set explicitly, only `Identify` (for profile discovery)
    /// and storage bounds are needed — no `Sign` required.
    pub async fn perform<Env>(self, env: &Env) -> Result<DelegationChain, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<storage::Set>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        let audience = self.audience.ok_or_else(|| {
            AuthorizeError::Configuration("delegation requires an audience".into())
        })?;

        // Discover profile/operator DIDs via Identify
        let auth = Subject::from(self.issuer.did())
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();
        let operator_did = authority::Operator::of(&auth).operator.clone();

        // Find proof chain if needed (explicit issuer → self-grant shortcuts apply)
        let issuer_did = self.issuer.did();
        let proof = find_proof(
            env,
            &self.scope,
            &profile_did,
            &operator_did,
            &issuer_did,
            true,
        )
        .await?;

        // Build the outermost delegation
        let delegation = build_delegation(
            self.issuer,
            &audience,
            &self.scope,
            self.expiration,
            self.not_before,
        )
        .await?;

        // Extend proof chain with outermost delegation
        let chain = extend_chain(proof, delegation)?;

        // Store delegation chain
        import_delegation_chain(env, &profile_did, &chain)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        Ok(chain)
    }
}

impl DelegateRequest<IssuerUnset> {
    /// Sign and store the delegation, resolving issuer via environment.
    ///
    /// Requires `Identify` and `Sign` to discover and use the profile signer.
    pub async fn perform<Env>(self, env: &Env) -> Result<DelegationChain, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::Set>
            + Provider<storage::List>
            + Provider<storage::Get>
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

        // Find proof chain (no explicit issuer → no self-grant shortcuts)
        let proof = find_proof(
            env,
            &self.scope,
            &profile_did,
            &operator_did,
            &profile_did,
            false,
        )
        .await?;

        // Build outermost delegation using Issuer bridge
        let issuer = Issuer::new(env, auth);
        let delegation = build_delegation(
            issuer,
            &audience,
            &self.scope,
            self.expiration,
            self.not_before,
        )
        .await?;

        let chain = extend_chain(proof, delegation)?;

        import_delegation_chain(env, &profile_did, &chain)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        Ok(chain)
    }
}

/// Builder for a UCAN invocation.
///
/// Created via [`Ucan::invoke()`](super::Ucan::invoke). Use `.issuer()` to
/// provide an explicit signer, or leave it unset to resolve via `Identify`/`Sign`.
pub struct InvokeRequest<I = IssuerUnset> {
    scope: Scope,
    issuer: I,
}

impl InvokeRequest<IssuerUnset> {
    pub(crate) fn new(capability: &impl Ability) -> Self {
        Self {
            scope: Scope::from(capability),
            issuer: IssuerUnset,
        }
    }

    /// Set an explicit issuer (signer) for the invocation.
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
    S: dialog_varsig::Signer<Ed25519Signature> + Principal,
{
    /// Build and sign the invocation.
    ///
    /// When issuer is set explicitly, only `Identify` and storage bounds needed.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanInvocation, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<storage::List>
            + Provider<storage::Get>
            + ConditionalSync,
    {
        let auth = Subject::from(self.issuer.did())
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();
        let operator_did = authority::Operator::of(&auth).operator.clone();

        build_invocation(env, self.issuer, &self.scope, &profile_did, &operator_did).await
    }
}

impl InvokeRequest<IssuerUnset> {
    /// Build and sign the invocation, resolving issuer via environment.
    ///
    /// Requires `Identify` and `Sign` to discover and use the operator signer.
    pub async fn perform<Env>(self, env: &Env) -> Result<UcanInvocation, AuthorizeError>
    where
        Env: Provider<authority::Identify>
            + Provider<authority::Sign>
            + Provider<storage::List>
            + Provider<storage::Get>
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

        let issuer = Issuer::new(env, auth);
        build_invocation(env, issuer, &self.scope, &profile_did, &operator_did).await
    }
}

fn resolve_lookup_did(scope: &Scope) -> Did {
    use dialog_ucan::subject::Subject as UcanSubject;
    match &scope.subject {
        UcanSubject::Any => "did:_:_".parse::<Did>().unwrap(),
        UcanSubject::Specific(did) => did.clone(),
    }
}

/// Find proof chain, with self-grant shortcutting when `explicit_issuer` is true.
///
/// Self-grant shortcuts (issuer == subject, or subject is Any) only apply when
/// the issuer was explicitly provided. When resolving via the environment,
/// the full chain search is always performed.
async fn find_proof<Env>(
    env: &Env,
    scope: &Scope,
    profile_did: &Did,
    operator_did: &Did,
    issuer_did: &Did,
    explicit_issuer: bool,
) -> Result<Option<DelegationChain>, AuthorizeError>
where
    Env: Provider<storage::List> + Provider<storage::Get> + ConditionalSync,
{
    use dialog_ucan::subject::Subject as UcanSubject;

    let subject_did = match &scope.subject {
        UcanSubject::Any => "did:_:_".parse::<Did>().unwrap(),
        UcanSubject::Specific(did) => did.clone(),
    };

    // Self-grant shortcut only applies with an explicit issuer
    if explicit_issuer {
        let is_self_grant = issuer_did == &subject_did;
        let is_powerline = matches!(&scope.subject, UcanSubject::Any);
        if is_self_grant || is_powerline {
            return Ok(None);
        }
    }

    find_chain(
        env,
        profile_did,
        operator_did,
        &subject_did,
        &scope.command,
        scope.parameters.as_map(),
        &Timestamp::now(),
    )
    .await
    .and_then(|chain| match chain {
        Some(c) => Ok(Some(c)),
        None => Err(AuthorizeError::Denied(format!(
            "no delegation chain found for operator '{}' on subject '{}'",
            operator_did, subject_did
        ))),
    })
}

/// Build a delegation using any type that implements Signer + Principal.
async fn build_delegation<S>(
    signer: S,
    audience: &Did,
    scope: &Scope,
    expiration: Option<Timestamp>,
    not_before: Option<Timestamp>,
) -> Result<dialog_ucan::Delegation<Ed25519Signature>, AuthorizeError>
where
    S: dialog_varsig::Signer<Ed25519Signature> + Principal,
{
    use dialog_ucan::delegation::builder::DelegationBuilder;

    let mut builder = DelegationBuilder::new()
        .issuer(signer)
        .audience(audience)
        .subject(scope.subject.clone())
        .command(scope.command.segments().clone())
        .policy(scope.policy());

    if let Some(exp) = expiration {
        builder = builder.expiration(exp);
    }
    if let Some(nbf) = not_before {
        builder = builder.not_before(nbf);
    }

    builder
        .try_build()
        .await
        .map_err(|e| AuthorizeError::Configuration(format!("failed to build delegation: {e:?}")))
}

/// Extend proof chain with outermost delegation.
fn extend_chain(
    proof: Option<DelegationChain>,
    delegation: dialog_ucan::Delegation<Ed25519Signature>,
) -> Result<DelegationChain, AuthorizeError> {
    match proof {
        Some(proof_chain) => proof_chain
            .extend(delegation)
            .map_err(|e| AuthorizeError::Configuration(format!("chain extension failed: {e}"))),
        None => Ok(DelegationChain::new(delegation)),
    }
}

/// Build a signed invocation with any Signer + Principal.
async fn build_invocation<S, Env>(
    env: &Env,
    signer: S,
    scope: &Scope,
    profile_did: &Did,
    operator_did: &Did,
) -> Result<UcanInvocation, AuthorizeError>
where
    S: dialog_varsig::Signer<Ed25519Signature> + Principal,
    Env: Provider<storage::List> + Provider<storage::Get> + ConditionalSync,
{
    use dialog_ucan::InvocationBuilder;
    use dialog_ucan::subject::Subject as UcanSubject;

    let subject_did = match &scope.subject {
        UcanSubject::Specific(did) => did.clone(),
        UcanSubject::Any => "did:_:_".parse::<Did>().unwrap(),
    };

    let ability = if scope.command.segments().is_empty() {
        "/".to_string()
    } else {
        format!("/{}", scope.command.segments().join("/"))
    };

    // Find delegation chain
    let delegation_chain = if &subject_did == operator_did {
        None
    } else {
        let chain = find_chain(
            env,
            profile_did,
            operator_did,
            &subject_did,
            &scope.command,
            scope.parameters.as_map(),
            &Timestamp::now(),
        )
        .await?;

        match chain {
            Some(c) => Some(c),
            None => {
                return Err(AuthorizeError::Denied(format!(
                    "no delegation chain found for operator '{}' to act on subject '{}'",
                    operator_did, subject_did
                )));
            }
        }
    };

    let (proofs, delegations_map) = match &delegation_chain {
        Some(chain) => {
            let chain_audience = chain.audience();
            if operator_did != chain_audience {
                return Err(AuthorizeError::Configuration(format!(
                    "authority '{}' does not match delegation chain audience '{}'",
                    operator_did, chain_audience
                )));
            }
            (chain.proof_cids().into(), chain.delegations().clone())
        }
        None => (vec![], Default::default()),
    };

    let command: Vec<String> = scope.command.segments().clone();
    let args = scope.parameters.args();

    let invocation = InvocationBuilder::new()
        .issuer(signer)
        .audience(&subject_did)
        .subject(&subject_did)
        .command(command)
        .arguments(args)
        .proofs(proofs)
        .try_build()
        .await
        .map_err(|e| AuthorizeError::Denied(format!("{e:?}")))?;

    let chain = InvocationChain::new(invocation, delegations_map);

    Ok(UcanInvocation {
        chain: Box::new(chain),
        subject: subject_did,
        ability,
    })
}
