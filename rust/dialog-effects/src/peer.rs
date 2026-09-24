//! Peer capability hierarchy: a host's contacts, and connections to them.
//!
//! A peer is a role an entity plays: something that holds replicas of
//! repositories, identified by a DID. Which peers a host can reach, and
//! at which addresses, is the host's own business, recorded in its own
//! state as contacts. These effects name that vocabulary on the host's
//! subject, so adding an address or connecting to a peer is something
//! the host decides, and something it can delegate.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject (host DID)
//!   └── Use
//!         ├── Get → Peers
//!         │     ├── Find { name } → Result<Vec<Entity>, PeerError>
//!         │     └── Connect { peer } → Result<PeerConnection, PeerError>
//!         └── Put → Peers
//!               ├── AddAddress { peer, address } → Result<(), PeerError>
//!               └── SetName { peer, name } → Result<(), PeerError>
//! ```

use crate::Rejection;
use crate::memory::MemoryError;
use crate::method;
use dialog_capability::access::AuthorizeError;
use dialog_capability::identity::Entity;
use dialog_capability::{Attenuate, Attenuation, Constraint, Effect};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use thiserror::Error;

pub mod prelude;

/// Root policy for a host's peers.
///
/// Attaches under a [`Method`](crate::Method) and contributes the dialog
/// namespace; the effects name the rest of the command
/// (`/use/put/dialog/peer/add-address`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct Peers<V = method::Get>(#[serde(skip)] PhantomData<V>);

impl<V> Peers<V> {
    /// The dialog namespace under `V`.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<V> Default for Peers<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V: crate::Method> Attenuation for Peers<V>
where
    V::Of: Constraint,
{
    type Of = V;

    fn attenuation() -> &'static str {
        "dialog"
    }
}

/// An address a peer is reached at, encoded.
///
/// Opaque here: which kinds of address exist, and how each is encoded,
/// is the network's business, above this crate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PeerAddress(
    /// The encoded address.
    #[serde(with = "serde_bytes")]
    pub Vec<u8>,
);

/// Record that `peer` is reached at `address`.
///
/// Adding an address the peer already has converges.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct AddAddress {
    /// The peer, by its entity: its DID.
    pub peer: Entity,
    /// Where it is reached.
    pub address: PeerAddress,
}

impl Attenuation for AddAddress {
    type Of = Peers<method::Put>;

    fn attenuation() -> &'static str {
        "peer/add-address"
    }
}

impl Effect for AddAddress {
    type Output = Result<(), PeerError>;
}

/// Give `peer` the name the host knows it by, making it a contact, and
/// replacing any name it had.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct SetName {
    /// The peer, by its entity: its DID.
    pub peer: Entity,
    /// The name.
    pub name: String,
}

impl Attenuation for SetName {
    type Of = Peers<method::Put>;

    fn attenuation() -> &'static str {
        "peer/set-name"
    }
}

impl Effect for SetName {
    type Output = Result<(), PeerError>;
}

/// The peers known by `name`. More than one means the name is
/// ambiguous; none means no contact has it.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Find {
    /// The name.
    pub name: String,
}

impl Attenuation for Find {
    type Of = Peers<method::Get>;

    fn attenuation() -> &'static str {
        "peer/find"
    }
}

impl Effect for Find {
    type Output = Result<Vec<Entity>, PeerError>;
}

/// Connect to `peer`: the connection other effects on its replicas go
/// through.
///
/// The host keeps its connections, so every caller connecting to the
/// same peer shares one, and with it what the connection has learned,
/// such as which address answered last.
#[derive(Debug, Clone, Serialize, Deserialize, Attenuate)]
pub struct Connect {
    /// The peer, by its entity: its DID.
    pub peer: Entity,
}

impl Attenuation for Connect {
    type Of = Peers<method::Get>;

    fn attenuation() -> &'static str {
        "peer/connect"
    }
}

impl Effect for Connect {
    type Output = Result<PeerConnection, PeerError>;
}

/// A host's connection to a peer: the addresses it is reached at, and
/// which of them answered last.
///
/// Cheap to clone; clones share what the connection learns.
#[derive(Debug, Clone)]
pub struct PeerConnection {
    peer: Entity,
    addresses: Arc<[PeerAddress]>,
    answered: Arc<AtomicUsize>,
}

impl PeerConnection {
    /// A connection to `peer`, reached at `addresses`.
    ///
    /// Refused when `addresses` is empty: a peer with nowhere to reach
    /// it is not connected to.
    pub fn new(peer: Entity, addresses: Vec<PeerAddress>) -> Result<Self, PeerError> {
        if addresses.is_empty() {
            return Err(PeerError::Unreachable { peer });
        }
        Ok(Self {
            peer,
            addresses: addresses.into(),
            answered: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The peer this connects to.
    pub fn peer(&self) -> &Entity {
        &self.peer
    }

    /// Every address the peer is reached at.
    pub fn addresses(&self) -> &[PeerAddress] {
        &self.addresses
    }

    /// The index of the address that answered last, or of the first
    /// address until one has.
    pub fn answered(&self) -> usize {
        self.answered.load(Ordering::Relaxed) % self.addresses.len()
    }

    /// Record that the address at `index` answered.
    pub fn answer(&self, index: usize) {
        self.answered.store(index, Ordering::Relaxed);
    }

    /// The shared record of which address answered last, for a handle
    /// that reaches the peer on this connection's behalf.
    pub fn answers(&self) -> Arc<AtomicUsize> {
        self.answered.clone()
    }
}

/// Errors that can occur during peer operations.
#[derive(Debug, Error)]
pub enum PeerError {
    /// The peer has no address to be reached at.
    #[error("No address is known for peer {peer}")]
    Unreachable {
        /// The peer.
        peer: Entity,
    },

    /// The host has nowhere to keep contacts.
    #[error("The host keeps no state to record contacts in: {reason}")]
    Stateless {
        /// Why.
        reason: String,
    },

    /// Reading or writing the host's state failed.
    #[error("Peer state failed: {0}")]
    Storage(String),

    /// A memory cell operation failed.
    #[error(transparent)]
    Memory(#[from] MemoryError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),
}

#[cfg(test)]
mod tests {
    use super::{PeerAddress, PeerConnection, PeerError};
    use crate::prelude::*;
    use dialog_capability::identity::Entity;
    use dialog_capability::{Subject, did};

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    fn peer() -> Entity {
        "did:web:peer.example".parse().expect("valid entity")
    }

    #[dialog_common::test]
    fn it_builds_peer_claim_paths() {
        let host = Subject::from(did!("key:zHost"));
        let address = PeerAddress(vec![1, 2, 3]);

        assert_eq!(
            host.clone()
                .writer()
                .peers()
                .add_address(peer(), address)
                .ability(),
            "/use/put/dialog/peer/add-address"
        );
        assert_eq!(
            host.clone()
                .writer()
                .peers()
                .set_name(peer(), "origin")
                .ability(),
            "/use/put/dialog/peer/set-name"
        );
        assert_eq!(
            host.clone().reader().peers().find("origin").ability(),
            "/use/get/dialog/peer/find"
        );
        assert_eq!(
            host.reader().peers().connect(peer()).ability(),
            "/use/get/dialog/peer/connect"
        );
    }

    #[dialog_common::test]
    fn it_refuses_a_connection_with_no_address() {
        assert!(matches!(
            PeerConnection::new(peer(), Vec::new()),
            Err(PeerError::Unreachable { .. })
        ));
    }

    #[dialog_common::test]
    fn it_shares_the_answered_address_between_clones() {
        let connection =
            PeerConnection::new(peer(), vec![PeerAddress(vec![1]), PeerAddress(vec![2])])
                .expect("addresses given");
        let clone = connection.clone();
        clone.answer(1);
        assert_eq!(connection.answered(), 1);
    }
}
