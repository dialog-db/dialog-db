//! Answering "who are you" for [`Operator`].
//!
//! The one capability whose answer is the operator itself rather than
//! something it stores, which is why it is implemented here rather than
//! routed to a space: there is no subject to look up, only the identity
//! already held.
//!
//! Its sibling [`Spaces`](dialog_effects::peer::Spaces) goes the other
//! way and is not here: what spaces an operator has is exactly what its
//! storage has mounted, so it forwards there like every other stored
//! effect.
//!
//! The subject is taken from the invocation rather than reported from
//! this side. An operator answers *for* a subject, and the one it was
//! asked about is the one the caller proved a delegation for — so
//! echoing it back is a statement about what this reply covers, not a
//! claim about what else this peer holds.

use super::Operator;
use dialog_capability::{Capability, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::peer::{Greeting, Hello, PeerError};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S> Provider<Hello> for Operator<S>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Hello>) -> Result<Greeting, PeerError> {
        Ok(Greeting {
            subject: input.subject().clone(),
            profile: self.profile_did(),
            operator: self.did(),
        })
    }
}
