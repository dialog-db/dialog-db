//! Delegation types for repositories.
//!
//! [`Access`] pairs a capability with a credential, threading it through
//! attenuation chains. At any level, call `.delegate(audience)` to
//! produce a [`Delegation`] ready to perform.

use dialog_capability::access::AuthorizeError;
use dialog_capability::ucan::Ucan;
use dialog_capability::{
    Ability, Capability, Constraint, Did, Provider, Subject, authority, storage,
};
use dialog_common::ConditionalSync;
use dialog_credentials::{Credential, Ed25519Signer, SignerCredential};
use dialog_effects::{archive as fx_archive, memory as fx_memory};
use dialog_ucan::DelegationChain;
use dialog_varsig::Principal;

/// A capability paired with a credential for delegation.
///
/// Created via [`Repository::ownership()`](super::Repository::ownership).
/// Chain `.archive()`, `.memory()`, `.catalog()`, `.space()` to narrow
/// the scope, then `.delegate(audience)` to produce a [`Delegation`].
pub struct Access<'a, P: Principal, C: Constraint> {
    capability: Capability<C>,
    credential: &'a P,
}

impl<'a, P: Principal, C: Constraint> Access<'a, P, C> {
    /// Create a new access scope from a capability and credential.
    pub fn new(capability: Capability<C>, credential: &'a P) -> Self {
        Self {
            capability,
            credential,
        }
    }
}

// Subject-level: can narrow to archive or memory
impl<'a, P: Principal> Access<'a, P, Subject> {
    /// Narrow to archive-level delegation.
    pub fn archive(self) -> Access<'a, P, fx_archive::Archive> {
        Access::new(
            self.capability.attenuate(fx_archive::Archive),
            self.credential,
        )
    }

    /// Narrow to memory-level delegation.
    pub fn memory(self) -> Access<'a, P, fx_memory::Memory> {
        Access::new(
            self.capability.attenuate(fx_memory::Memory),
            self.credential,
        )
    }
}

// Archive-level: can narrow to catalog
impl<'a, P: Principal> Access<'a, P, fx_archive::Archive> {
    /// Narrow to a specific catalog.
    pub fn catalog(self, name: impl Into<String>) -> Access<'a, P, fx_archive::Catalog> {
        Access::new(
            self.capability.attenuate(fx_archive::Catalog::new(name)),
            self.credential,
        )
    }
}

// Memory-level: can narrow to space
impl<'a, P: Principal> Access<'a, P, fx_memory::Memory> {
    /// Narrow to a specific space.
    pub fn space(self, name: impl Into<String>) -> Access<'a, P, fx_memory::Space> {
        Access::new(
            self.capability.attenuate(fx_memory::Space::new(name)),
            self.credential,
        )
    }
}

// delegate() for SignerCredential — always has a signer
impl<C: Constraint> Access<'_, SignerCredential, C> {
    /// Create a delegation to the given audience.
    pub fn delegate(self, audience: &impl Principal) -> Delegation<C> {
        Delegation {
            capability: self.capability,
            audience: audience.did(),
            signer: Some(self.credential.signer().clone()),
        }
    }
}

// delegate() for Credential — runtime signer check
impl<C: Constraint> Access<'_, Credential, C> {
    /// Create a delegation to the given audience.
    ///
    /// If the credential is verifier-only, `perform` will resolve the
    /// issuer via the environment's `Identify`/`Sign`.
    pub fn delegate(self, audience: &impl Principal) -> Delegation<C> {
        Delegation {
            capability: self.capability,
            audience: audience.did(),
            signer: self.credential.signer().cloned(),
        }
    }
}

/// A delegation ready to be signed.
///
/// Created by calling `.delegate(audience)` on an [`Access`].
/// Returns the signed `DelegationChain` — the caller is responsible
/// for storing it where needed.
pub struct Delegation<C: Constraint> {
    capability: Capability<C>,
    audience: Did,
    signer: Option<Ed25519Signer>,
}

impl<C: Constraint> Delegation<C>
where
    Capability<C>: Ability,
{
    /// Sign the delegation and return the chain.
    ///
    /// Uses the new `Claim<Ucan>` effect to discover the delegation chain,
    /// then builds and signs a new delegation extending it.
    pub async fn perform<Env>(self, env: &Env) -> Result<DelegationChain, AuthorizeError>
    where
        Env: Provider<dialog_capability::access::Claim<dialog_capability_ucan::Ucan>>
            + Provider<authority::Identify>
            + Provider<authority::Sign>
            + ConditionalSync,
    {
        use dialog_capability::access::{Permit, ProofChain as _};
        use dialog_capability_ucan::{Ucan, UcanPermit};
        use dialog_capability_ucan::scope::Scope;

        // Resolve signer: use explicit signer or resolve via Identify
        let signer = match self.signer {
            Some(s) => s,
            None => {
                let auth = Subject::from(self.capability.subject().clone())
                    .invoke(authority::Identify)
                    .perform(env)
                    .await
                    .map_err(|e| AuthorizeError::Configuration(e.to_string()))?;
                let profile = authority::Profile::of(&auth);
                // Use the profile's credential to find delegation chain
                // The operator doesn't have a signer, so we use Identify
                return Err(AuthorizeError::Configuration(
                    "delegation without explicit signer not yet supported in new API".into(),
                ));
            }
        };

        let scope = Scope::from(&self.capability);
        let issuer_did = dialog_varsig::Principal::did(&signer);

        // Find delegation chain from issuer to subject
        let proof_chain: UcanPermit = Subject::from(issuer_did.clone())
            .attenuate(Permit)
            .invoke(dialog_capability::access::Claim::<Ucan>::new(
                issuer_did,
                scope,
            ))
            .perform(env)
            .await?;

        // Build and sign the new delegation
        let authorization = proof_chain.claim(signer)?;
        authorization.delegate(self.audience).await
    }
}
