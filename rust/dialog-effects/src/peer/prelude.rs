//! Extension traits for fluent peer capability chains.
//!
//! Import all traits with:
//! ```
//! use dialog_effects::peer::prelude::*;
//! ```
//!
//! ```text
//! host.writer().peers().add_address(peer, address)
//!                                     = /use/put/dialog/peer/add-address
//! host.writer().peers().set_name(peer, name)
//!                                     = /use/put/dialog/peer/set-name
//! host.reader().peers().find(name)    = /use/get/dialog/peer/find
//! host.reader().peers().connect(peer) = /use/get/dialog/peer/connect
//! ```

use dialog_capability::identity::Entity;
use dialog_capability::{Capability, Constraint};

use super::{AddAddress, Connect, Find, PeerAddress, Peers, RemoveAddress, RemoveName, SetName};
use crate::{Method, method};

/// Scope a method to the host's peers.
pub trait PeersExt {
    /// The resulting peers chain type.
    type Peers;
    /// Scope to the peers of this host.
    fn peers(self) -> Self::Peers;
}

impl<M: Method> PeersExt for Capability<M>
where
    M::Of: Constraint,
{
    type Peers = Capability<Peers<M>>;
    fn peers(self) -> Self::Peers {
        self.attenuate(Peers::new())
    }
}

/// Record a peer's addresses and names.
pub trait WritePeersExt {
    /// Record that `peer` is reached at `address`.
    fn add_address(self, peer: Entity, address: PeerAddress) -> Capability<AddAddress>;
    /// Give `peer` the name it is known by.
    fn set_name(self, peer: Entity, name: impl Into<String>) -> Capability<SetName>;
    /// Record that `peer` is no longer reached at `address`.
    fn remove_address(self, peer: Entity, address: PeerAddress) -> Capability<RemoveAddress>;
    /// Take back the name `peer` is known by.
    fn remove_name(self, peer: Entity) -> Capability<RemoveName>;
}

impl WritePeersExt for Capability<Peers<method::Put>> {
    fn add_address(self, peer: Entity, address: PeerAddress) -> Capability<AddAddress> {
        self.invoke(AddAddress { peer, address })
    }

    fn set_name(self, peer: Entity, name: impl Into<String>) -> Capability<SetName> {
        self.invoke(SetName {
            peer,
            name: name.into(),
        })
    }

    fn remove_address(self, peer: Entity, address: PeerAddress) -> Capability<RemoveAddress> {
        self.invoke(RemoveAddress { peer, address })
    }

    fn remove_name(self, peer: Entity) -> Capability<RemoveName> {
        self.invoke(RemoveName { peer })
    }
}

/// Look peers up, and connect to them.
pub trait ReadPeersExt {
    /// The peers known by `name`.
    fn find(self, name: impl Into<String>) -> Capability<Find>;
    /// Connect to `peer`.
    fn connect(self, peer: Entity) -> Capability<Connect>;
}

impl ReadPeersExt for Capability<Peers<method::Get>> {
    fn find(self, name: impl Into<String>) -> Capability<Find> {
        self.invoke(Find { name: name.into() })
    }

    fn connect(self, peer: Entity) -> Capability<Connect> {
        self.invoke(Connect { peer })
    }
}
