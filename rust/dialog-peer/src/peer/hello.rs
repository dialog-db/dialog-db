//! Answering "who are you" for a [`Peer`].
//!
//! The one effect whose answer is the peer itself rather than something
//! it stores, which is why it is answered here rather than routed to a
//! space: there is no subject to look up, only the identity already
//! held.
//!
//! The subject is taken from the invocation rather than reported from
//! this side. A peer answers *for* a subject, and the one it was asked
//! about is the one the caller proved a delegation for, so echoing it
//! back is a statement about what this reply covers, not a claim about
//! what else this peer holds.

use dialog_capability::{Capability, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::peer::{Greeting, Hello, PeerError};

use super::{Mode, Peer};

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<Hello> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<Hello>) -> Result<Greeting, PeerError> {
        Ok(Greeting {
            subject: input.subject().clone(),
            peer: self.inner.holder.clone(),
            operator: self.did(),
        })
    }
}
