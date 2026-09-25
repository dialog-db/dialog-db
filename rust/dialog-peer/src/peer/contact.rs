//! The peer's contacts: the `peer` effects, answered from its state
//! branch.
//!
//! Which peers this one can reach, and where, is recorded in its own
//! state beside its delegations. Connecting reads the addresses once
//! and keeps the connection, so every sync with a peer shares what the
//! connection learns, such as which address answered last. Adding an
//! address drops the kept connection, and the next connect reads the
//! addresses afresh.

use dialog_capability::identity::Entity;
use dialog_capability::{Capability, Did, Policy as _, Provider};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_effects::peer::{
    AddAddress, Connect, Find, PeerConnection, PeerError, RemoveAddress, RemoveName, SetName,
};
use dialog_repository::contacts;
use dialog_repository::registry::RegistryEnv;
use dialog_repository::{Branch, CommitError, PublishError};

use super::{Mode, Peer};

/// How many times a write that lost a race for the state branch's head is
/// retried before the failure surfaces, as retaining a delegation does.
const RETRY_LIMIT: usize = 3;

impl<S: Clone, M: Mode> Peer<S, M>
where
    Self: RegistryEnv,
{
    /// The state branch contacts live in, re-read so what other handles
    /// wrote is seen.
    async fn contacts(&self) -> Result<&Branch, PeerError> {
        let state = self.state().map_err(|error| PeerError::Stateless {
            reason: error.to_string(),
        })?;
        state
            .refresh(self)
            .await
            .map_err(|error| PeerError::Storage(error.to_string()))?;
        Ok(state)
    }

    /// Write to the state branch, re-reading its head and trying again
    /// when another handle moved it first.
    async fn write_contacts<F, Fut>(&self, write: F) -> Result<(), PeerError>
    where
        F: Fn(Branch) -> Fut,
        Fut: Future<Output = Result<(), CommitError>>,
    {
        let mut attempt = 0;
        loop {
            let state = self.contacts().await?.clone();
            match write(state).await {
                Ok(()) => return Ok(()),
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
                    if attempt < RETRY_LIMIT =>
                {
                    attempt += 1;
                }
                Err(error) => return Err(PeerError::Storage(error.to_string())),
            }
        }
    }
}

impl<S: Clone, M: Mode> Peer<S, M> {
    /// Refuse contacts asked of any subject but this peer's own: they are
    /// kept in its state, which holds no other subject's.
    fn own_contacts(&self, subject: &Did) -> Result<(), PeerError> {
        if subject == self.home() {
            Ok(())
        } else {
            Err(PeerError::Foreign {
                subject: subject.to_string(),
                home: self.home().to_string(),
            })
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<AddAddress> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<AddAddress>) -> Result<(), PeerError> {
        self.own_contacts(input.subject())?;
        let AddAddress { peer, address } = AddAddress::of(&input).clone();
        self.write_contacts(|state| {
            let (peer, address) = (&peer, &address);
            async move { contacts::add_address(&state, peer, address, self).await }
        })
        .await?;
        self.connections().lock().remove(&peer);
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<SetName> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<SetName>) -> Result<(), PeerError> {
        self.own_contacts(input.subject())?;
        let SetName { peer, name } = SetName::of(&input).clone();
        self.write_contacts(|state| {
            let (peer, name) = (&peer, &name);
            async move { contacts::set_name(&state, peer, name, self).await }
        })
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<RemoveAddress> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<RemoveAddress>) -> Result<(), PeerError> {
        self.own_contacts(input.subject())?;
        let RemoveAddress { peer, address } = RemoveAddress::of(&input).clone();
        self.write_contacts(|state| {
            let (peer, address) = (&peer, &address);
            async move { contacts::remove_address(&state, peer, address, self).await }
        })
        .await?;
        // A connection holds the addresses it was made with: the next
        // connect reads them again, without this one.
        self.connections().lock().remove(&peer);
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<RemoveName> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<RemoveName>) -> Result<(), PeerError> {
        self.own_contacts(input.subject())?;
        let RemoveName { peer } = RemoveName::of(&input).clone();
        self.write_contacts(|state| {
            let peer = &peer;
            async move { contacts::remove_name(&state, peer, self).await }
        })
        .await
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<Find> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<Find>) -> Result<Vec<Entity>, PeerError> {
        self.own_contacts(input.subject())?;
        let name = &Find::of(&input).name;
        let state = self.contacts().await?;
        contacts::find(state, name, self)
            .await
            .map_err(|error| PeerError::Storage(error.to_string()))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl<S, M: Mode> Provider<Connect> for Peer<S, M>
where
    S: Clone + ConditionalSend + ConditionalSync + 'static,
    Self: RegistryEnv + ConditionalSend,
{
    async fn execute(&self, input: Capability<Connect>) -> Result<PeerConnection, PeerError> {
        self.own_contacts(input.subject())?;
        let peer = &Connect::of(&input).peer;
        if let Some(connection) = self.connections().lock().get(peer) {
            return Ok(connection.clone());
        }
        let state = self.contacts().await?;
        let addresses = contacts::addresses(state, peer, self)
            .await
            .map_err(|error| PeerError::Storage(error.to_string()))?;
        let connection = PeerConnection::new(peer.clone(), addresses)?;
        // Two connects racing both read the addresses; the first one kept
        // is the one every later connect shares.
        Ok(self
            .connections()
            .lock()
            .entry(peer.clone())
            .or_insert(connection)
            .clone())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use crate::helpers::test_session_with_peer;
    use crate::{Mode, Peer};
    use dialog_capability::Subject;
    use dialog_capability::identity::Entity;
    use dialog_effects::MethodExt as _;
    use dialog_effects::peer::prelude::*;
    use dialog_effects::peer::{PeerConnection, PeerError};
    use dialog_remote_ucan::UcanAddress;
    use dialog_repository::contact;
    use dialog_storage::provider::storage::VolatileSpace;
    use dialog_varsig::did;

    fn peer() -> Entity {
        "did:web:tonk.network".parse().expect("valid entity")
    }

    async fn connect(worker: &Peer<VolatileSpace, impl Mode>) -> Result<PeerConnection, PeerError> {
        Subject::from(worker.home().clone())
            .reader()
            .peers()
            .connect(peer())
            .perform(worker)
            .await
    }

    /// Every connect to a peer shares one connection, so what one sync
    /// learns about which address answers, the next starts from. A new
    /// address replaces the connection, so the next connect sees it.
    #[dialog_common::test]
    async fn it_keeps_a_connection_until_the_peer_is_reached_elsewhere() -> anyhow::Result<()> {
        let (worker, _) = test_session_with_peer().await;
        for endpoint in [
            "https://tonk.network/ucan/",
            "https://backup.tonk.network/ucan/",
        ] {
            contact(did!("web:tonk.network"))
                .add_address(UcanAddress::new(endpoint))
                .perform(&worker)
                .await?;
        }

        let first = connect(&worker).await?;
        first.answer(1);
        let second = connect(&worker).await?;
        assert_eq!(second.addresses().len(), 2);
        assert_eq!(second.answered(), 1, "one connection, shared");

        contact(did!("web:tonk.network"))
            .add_address(UcanAddress::new("https://third.tonk.network/ucan/"))
            .perform(&worker)
            .await?;
        let third = connect(&worker).await?;
        assert_eq!(third.addresses().len(), 3);
        assert_eq!(third.answered(), 0, "a fresh connection");
        Ok(())
    }

    /// A peer keeps the contacts of its own subject. A contact written
    /// against another subject is not this peer's to record, and is
    /// refused rather than recorded among its own.
    #[dialog_common::test]
    async fn it_refuses_contacts_written_for_another_subject() -> anyhow::Result<()> {
        let (worker, _) = test_session_with_peer().await;
        let address = dialog_repository::peer_address(
            &UcanAddress::new("https://tonk.network/ucan/").into(),
        )?;

        let written = Subject::from(did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"))
            .writer()
            .peers()
            .add_address(peer(), address)
            .perform(&worker)
            .await;
        assert!(written.is_err(), "the contact is another subject's");
        assert!(
            matches!(connect(&worker).await, Err(PeerError::Unreachable { .. })),
            "nothing was recorded among the peer's own contacts"
        );
        Ok(())
    }

    /// An address taken back is no longer tried: the next connection is
    /// made with the addresses that remain.
    #[dialog_common::test]
    async fn it_stops_reaching_a_peer_at_a_removed_address() -> anyhow::Result<()> {
        let (worker, _) = test_session_with_peer().await;
        for endpoint in [
            "https://tonk.network/ucan/",
            "https://backup.tonk.network/ucan/",
        ] {
            contact(did!("web:tonk.network"))
                .add_address(UcanAddress::new(endpoint))
                .perform(&worker)
                .await?;
        }
        assert_eq!(connect(&worker).await?.addresses().len(), 2);

        let stale = dialog_repository::peer_address(
            &UcanAddress::new("https://backup.tonk.network/ucan/").into(),
        )?;
        Subject::from(worker.home().clone())
            .writer()
            .peers()
            .remove_address(peer(), stale)
            .perform(&worker)
            .await?;

        assert_eq!(connect(&worker).await?.addresses().len(), 1);
        Ok(())
    }

    /// A name taken back picks out no peer, and is free to give another.
    #[dialog_common::test]
    async fn it_frees_a_removed_name() -> anyhow::Result<()> {
        let (worker, _) = test_session_with_peer().await;
        contact(did!("web:tonk.network"))
            .add_address(UcanAddress::new("https://tonk.network/ucan/"))
            .name("tonk")
            .perform(&worker)
            .await?;
        let peers = Subject::from(worker.home().clone()).reader().peers();
        assert_eq!(
            peers.clone().find("tonk").perform(&worker).await?,
            vec![peer()]
        );

        Subject::from(worker.home().clone())
            .writer()
            .peers()
            .remove_name(peer())
            .perform(&worker)
            .await?;

        assert!(peers.find("tonk").perform(&worker).await?.is_empty());
        Ok(())
    }

    /// A peer with no address is not connected to.
    #[dialog_common::test]
    async fn it_refuses_to_connect_to_a_peer_it_cannot_reach() -> anyhow::Result<()> {
        let (worker, _) = test_session_with_peer().await;
        assert!(matches!(
            connect(&worker).await,
            Err(PeerError::Unreachable { .. })
        ));
        Ok(())
    }

    /// A remote peer is connected to by its DID, and a branch of its
    /// replica is opened through the connection.
    #[dialog_common::test]
    async fn it_connects_to_a_peer_and_opens_a_branch_there() -> anyhow::Result<()> {
        let (worker, _) = test_session_with_peer().await;
        contact(did!("web:tonk.network"))
            .add_address(UcanAddress::new("https://tonk.network/ucan/"))
            .perform(&worker)
            .await?;

        let subject = did!("key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
        let branch = Peer::connect(did!("web:tonk.network"))
            .repository(subject.clone())
            .branch("main")
            .open()
            .perform(&worker)
            .await?;
        assert_eq!(branch.name(), "main");
        assert_eq!(branch.repository().did(), subject);
        assert_eq!(branch.repository().peer(), &peer());
        Ok(())
    }
}
