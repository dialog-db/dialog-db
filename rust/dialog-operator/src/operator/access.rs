//! Access capability providers for Operator.

use super::Operator;
use dialog_capability::Capability;
use dialog_capability::Provider;
use dialog_capability::access::{AuthorizeError, Protocol, Prove, Retain};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_storage::provider::environment::Storage;

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
