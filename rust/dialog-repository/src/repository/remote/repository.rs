//! A repository held at a peer.

use crate::schema::{DidExt as _, Replica};
use crate::{RemoteAddress, SiteAddress};
use dialog_artifacts::Entity;
use dialog_capability::{Did, Subject};
use dialog_varsig::Principal;

/// A repository held at a peer: which peer, the addresses it is reached
/// at, and the repository whose replica it holds.
///
/// What was a named remote is these two things together. The peer is
/// who holds it and where to reach them; the repository is which of the
/// peer's replicas this is. Their state is cached locally, under the
/// repository this handle was reached from, keyed by entity.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    host: Subject,
    peer: Entity,
    name: Option<String>,
    addresses: Vec<SiteAddress>,
    subject: Did,
}

impl RemoteRepository {
    /// A repository `subject` held at `peer`, reached at `addresses`,
    /// with its state cached under `host`. `addresses` is not empty: a
    /// peer with nowhere to reach it is not connected to.
    pub(crate) fn new(
        host: Subject,
        peer: Entity,
        name: Option<String>,
        addresses: Vec<SiteAddress>,
        subject: Did,
    ) -> Self {
        Self {
            host,
            peer,
            name,
            addresses,
            subject,
        }
    }

    /// The subject DID of the repository.
    pub fn did(&self) -> Did {
        self.subject.clone()
    }

    /// The peer holding the repository.
    pub fn peer(&self) -> &Entity {
        &self.peer
    }

    /// The peer's local name, or its entity when it has none: how the
    /// peer is named in messages.
    pub fn name(&self) -> String {
        self.name.clone().unwrap_or_else(|| self.peer.to_string())
    }

    /// The peer's local name, if it has one.
    pub fn label(&self) -> Option<String> {
        self.name.clone()
    }

    /// Every address the peer is reached at.
    pub fn addresses(&self) -> &[SiteAddress] {
        &self.addresses
    }

    /// Where reads and writes go: the peer's first address, and the
    /// repository there.
    pub fn address(&self) -> RemoteAddress {
        RemoteAddress::new(self.addresses[0].clone(), self.subject.clone())
    }

    /// The peer's replica of the repository.
    pub fn replica(&self) -> Replica {
        Replica::derive(self.peer.clone(), self.subject.this())
    }

    /// Whether `other` is the same repository at the same peer, however
    /// it was reached.
    pub fn same(&self, other: &RemoteRepository) -> bool {
        self.peer == other.peer && self.subject == other.subject
    }

    /// The local repository this handle's state is cached under.
    pub(crate) fn host(&self) -> &Subject {
        &self.host
    }
}

impl Principal for RemoteRepository {
    fn did(&self) -> Did {
        self.subject.clone()
    }
}
