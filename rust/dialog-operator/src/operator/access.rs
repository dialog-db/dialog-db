//! Access capability providers for Operator.
//!
//! By default the access effects ([`Prove`], [`Retain`], [`Export`]) route
//! through [`Storage`] to the subject space's certificate provider. An
//! installed [`AccessProvider`] (see [`Operator::with_access`]) overrides
//! where proofs are resolved and delegations retained — the hook the
//! repository layer uses to serve access from the synced delegation
//! records of a branch, which this crate cannot construct itself (the
//! repository sits above it in the crate graph).

use super::Operator;
use dialog_capability::Provider;
use dialog_capability::access::{
    Access, Authorize, AuthorizeError, Export, Proof as _, Protocol, Prove, Retain,
};
use dialog_capability::{Capability, Policy as _, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::provider::storage::Storage;
use dialog_ucan::{Ucan, UcanDelegation, UcanProof};

/// Overrides where the operator resolves and retains UCAN delegations.
///
/// Installed with [`Operator::with_access`]; without one, access effects
/// route to the storage certificate provider.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait AccessProvider: ConditionalSend + ConditionalSync {
    /// Resolve a proof chain for the claim.
    async fn prove(&self, claim: Prove<Ucan>) -> Result<UcanProof, AuthorizeError>;

    /// Retain a delegation for future claims.
    async fn retain(&self, delegation: UcanDelegation) -> Result<(), AuthorizeError>;
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Prove<Ucan>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<Prove<Ucan>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Prove<Ucan>>) -> Result<UcanProof, AuthorizeError> {
        match self.access() {
            Some(access) => {
                let claim = Prove::<Ucan>::of(&input);
                let mut prove = Prove::<Ucan>::new(claim.principal.clone(), claim.access.clone());
                prove.duration = claim.duration;
                access.prove(prove).await
            }
            None => input.perform(&self.storage).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Retain<Ucan>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<Retain<Ucan>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Retain<Ucan>>) -> Result<(), AuthorizeError> {
        match self.access() {
            Some(access) => {
                let delegation = Retain::<Ucan>::of(&input).delegation.clone();
                access.retain(delegation).await
            }
            None => input.perform(&self.storage).await,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, P> Provider<Export<P>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    P: Protocol,
    P::Certificate: ConditionalSend + ConditionalSync,
    Storage<S>: Provider<Export<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Export<P>>,
    ) -> Result<Vec<P::Certificate>, AuthorizeError> {
        // Enumeration always reads the storage certificate provider: it
        // exists to migrate certificates OUT of it, so an installed access
        // override must not redirect it.
        input.perform(&self.storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Authorize<Ucan>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Storage<S>: Provider<Prove<Ucan>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Authorize<Ucan>>,
    ) -> Result<<Ucan as Protocol>::Authorization, AuthorizeError> {
        let subject = input.subject().clone();
        let prove: Prove<Ucan> = input.into_effect().into();

        // Route the proof through this operator's own Prove provider, so
        // an installed access override serves it.
        let proof = Subject::from(subject)
            .attenuate(Access)
            .invoke(prove)
            .perform(self)
            .await?;

        proof.claim(self.authority.operator_signer().clone())
    }
}
