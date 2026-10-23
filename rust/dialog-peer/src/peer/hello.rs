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

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::{test_repo, test_session_with_peer};
    use dialog_capability::Subject;
    use dialog_effects::MethodExt as _;
    use dialog_effects::peer::prelude::*;

    /// A peer acting as itself is its own operator; a session of it
    /// answers for the same peer with its own key.
    #[dialog_common::test]
    async fn it_says_who_answers_and_with_which_key() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let ask = || Subject::from(peer.did()).reader().peers().hello();

        let own = ask().perform(&peer).await?;
        assert_eq!(own.subject, peer.did());
        assert_eq!(own.peer, peer.did());
        assert_eq!(own.operator, peer.did());

        let through = ask().perform(&session).await?;
        assert_eq!(through.subject, peer.did());
        assert_eq!(through.peer, peer.did());
        assert_eq!(through.operator, session.did());
        assert_ne!(through.operator, through.peer);
        Ok(())
    }

    /// The spaces a peer offers are the repositories it has recorded,
    /// each under the name it knows it by.
    #[dialog_common::test]
    async fn it_offers_the_spaces_it_keeps() -> anyhow::Result<()> {
        let (session, peer) = test_session_with_peer().await;
        let ask = || Subject::from(peer.did()).reader().peers().spaces();

        assert!(ask().perform(&peer).await?.is_empty());

        let repository = test_repo(&session, &peer).await;
        let offers = ask().perform(&peer).await?;
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].subject, repository.did());
        assert!(
            offers[0]
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("repo"))
        );
        assert_eq!(ask().perform(&session).await?, offers);
        Ok(())
    }
}
