//! Builder API for UCAN invocations.
//!
//! Provides [`InvokeRequest`] with typestate tracking of whether an explicit
//! issuer has been provided. When issuer is set, `perform` needs
//! `Provider<Claim<Ucan>>` bounds; when unset, it also needs
//! `Provider<Identify>` and `Provider<Sign>`.

use dialog_capability::access::{self, Authorization as _, AuthorizeError, ProofChain as _};
use dialog_capability::{Capability, Did, Effect, Policy, Provider, Subject, authority};
use dialog_common::ConditionalSync;
use dialog_credentials::Ed25519Signer;
use dialog_ucan::DelegationChain;
use dialog_varsig::Principal;
use dialog_varsig::eddsa::Ed25519Signature;

use super::Ucan;
use super::access::UcanPermit;
use super::scope::Scope;

/// No issuer provided — `perform` resolves via `Identify` + `Sign`.
pub struct IssuerUnset;

/// Builder for a UCAN invocation.
pub struct InvokeRequest<I = IssuerUnset> {
    scope: Scope,
    issuer: I,
}

impl InvokeRequest<IssuerUnset> {
    pub(crate) fn new<Fx>(capability: &Capability<Fx>) -> Self
    where
        Fx: Effect + Clone,
        Capability<Fx>: dialog_capability::Ability,
    {
        Self {
            scope: Scope::invoke(capability),
            issuer: IssuerUnset,
        }
    }

    /// Set an explicit issuer.
    pub fn issuer(self, signer: Ed25519Signer) -> InvokeRequest<Ed25519Signer> {
        InvokeRequest {
            scope: self.scope,
            issuer: signer,
        }
    }
}

impl InvokeRequest<Ed25519Signer> {
    /// Build and sign the invocation.
    pub async fn perform<Env>(self, env: &Env) -> Result<super::UcanInvocation, AuthorizeError>
    where
        Env: Provider<authority::Identify> + Provider<access::Claim<Ucan>> + ConditionalSync,
    {
        let auth = Subject::from(self.issuer.did())
            .invoke(authority::Identify)
            .perform(env)
            .await
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;

        let profile_did = authority::Profile::of(&auth).profile.clone();

        let proof_chain = Subject::from(profile_did)
            .attenuate(access::Permit)
            .invoke(access::Claim::<Ucan>::new(
                self.issuer.did(),
                self.scope.clone(),
            ))
            .perform(env)
            .await?;

        let authorization = proof_chain.claim(self.issuer)?;
        authorization.invoke().await
    }
}

impl InvokeRequest<IssuerUnset> {
    /// Build and sign the invocation, resolving issuer via environment.
    pub async fn perform<Env>(self, env: &Env) -> Result<super::UcanInvocation, AuthorizeError>
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

        let proof_chain = Subject::from(profile_did)
            .attenuate(access::Permit)
            .invoke(access::Claim::<Ucan>::new(operator_did, self.scope.clone()))
            .perform(env)
            .await?;

        let existing_chain = proofs_to_chain(&proof_chain)?;
        let issuer = super::issuer::Issuer::new(env, auth);
        build_invocation(issuer, &self.scope, existing_chain).await
    }
}

fn resolve_lookup_did(scope: &Scope) -> Did {
    use dialog_ucan::subject::Subject as UcanSubject;
    match &scope.subject {
        UcanSubject::Specific(did) => did.clone(),
        UcanSubject::Any => dialog_capability::did!("key:zDummy"),
    }
}

/// Convert a UcanPermit's proofs back into a DelegationChain.
fn proofs_to_chain(permit: &UcanPermit) -> Result<Option<DelegationChain>, AuthorizeError> {
    let proofs = permit.proofs();
    if proofs.is_empty() {
        return Ok(None);
    }

    let mut iter = proofs.iter();
    let first = iter.next().expect("non-empty proofs").0.clone();
    let mut chain = DelegationChain::new(first);
    for proof in iter {
        chain = chain
            .push(proof.0.clone())
            .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
    }
    Ok(Some(chain))
}

/// Build a signed invocation using any Signer + Principal.
async fn build_invocation<S>(
    signer: S,
    scope: &Scope,
    delegation_chain: Option<DelegationChain>,
) -> Result<super::UcanInvocation, AuthorizeError>
where
    S: dialog_varsig::Signer<Ed25519Signature> + Principal,
{
    use dialog_ucan::InvocationBuilder;
    use dialog_ucan::subject::Subject as UcanSubject;

    let subject_did = match &scope.subject {
        UcanSubject::Specific(did) => did.clone(),
        UcanSubject::Any => dialog_capability::ANY_SUBJECT.parse().expect("valid DID"),
    };

    let command: Vec<String> = scope.command.segments().clone();
    let args = scope.parameters.args();

    let ability = if command.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", command.join("/"))
    };

    let (proofs, delegations_map) = match &delegation_chain {
        Some(chain) => (chain.proof_cids().into(), chain.delegations().clone()),
        None => (vec![], Default::default()),
    };

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

    let chain = dialog_ucan::InvocationChain::new(invocation, delegations_map);

    Ok(super::UcanInvocation {
        chain: Box::new(chain),
        subject: subject_did,
        ability,
    })
}
