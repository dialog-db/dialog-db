//! A [`Peer`] describing itself: who it is, and which spaces it holds.
//!
//! [`Hello`] is the one effect whose answer is the peer itself rather
//! than something it stores: there is no subject to look up, only the
//! identity already held. The subject is taken from the invocation
//! rather than reported from this side. A peer answers *for* a subject,
//! and the one it was asked about is the one the caller proved a
//! delegation for, so echoing it back is a statement about what this
//! reply covers, not a claim about what else this peer holds.
//!
//! [`Spaces`] is answered from the peer's own records of the
//! repositories it keeps, the same records its space names resolve
//! through, so an offer carries the name the peer knows the space by.

use dialog_capability::{Capability, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::peer::{Greeting, Hello, Offer, PeerError, Spaces};
use dialog_repository::registry::RegistryEnv;
use dialog_repository::spaces;

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
            peer: self.holder(),
            operator: self.did(),
        })
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<Spaces> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend + ConditionalSync,
{
    async fn execute(&self, _input: Capability<Spaces>) -> Result<Vec<Offer>, PeerError> {
        let state = self.state();
        state
            .refresh(self)
            .await
            .map_err(|error| PeerError::Storage(error.to_string()))?;
        let mut offers: Vec<Offer> = spaces::kept_by(state, &self.holder(), self)
            .await
            .map_err(|error| PeerError::Storage(error.to_string()))?
            .into_iter()
            .map(|(subject, name)| Offer {
                subject,
                name: Some(name),
            })
            .collect();
        offers.sort();
        Ok(offers)
    }
}
