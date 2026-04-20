//! Access capability providers for Operator.

use super::Operator;
use dialog_capability::Provider;
use dialog_capability::access::{
    Access, Authorize, AuthorizeError, Proof as _, Protocol, Prove, Retain,
};
use dialog_capability::{Capability, Subject};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Ed25519Signer;
use dialog_storage::provider::storage::Storage;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, P> Provider<Prove<P>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Certificate: Clone + ConditionalSend + ConditionalSync,
    P::Proof: ConditionalSend,
    Storage<S>: Provider<Prove<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Prove<P>>) -> Result<P::Proof, AuthorizeError> {
        input.perform(&self.storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, P> Provider<Retain<P>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    P: Protocol,
    P::Delegation: ConditionalSend + ConditionalSync,
    Storage<S>: Provider<Retain<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Retain<P>>) -> Result<(), AuthorizeError> {
        input.perform(&self.storage).await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, P> Provider<Authorize<P>> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    P: Protocol,
    P::Access: Clone + ConditionalSend + ConditionalSync,
    P::Certificate: Clone + ConditionalSend + ConditionalSync,
    P::Proof: ConditionalSend,
    P::Signer: From<Ed25519Signer>,
    P::Authorization: ConditionalSend,
    Storage<S>: Provider<Prove<P>>,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<Authorize<P>>,
    ) -> Result<P::Authorization, AuthorizeError> {
        let subject = input.subject().clone();
        let prove: Prove<P> = input.into_effect().into();

        let proof = Subject::from(subject)
            .attenuate(Access)
            .invoke(prove)
            .perform(&self.storage)
            .await?;

        proof.claim(self.authority.operator_signer().clone().into())
    }
}
