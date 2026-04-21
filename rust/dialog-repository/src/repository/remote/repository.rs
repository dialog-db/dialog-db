//! Remote repository -- a loaded remote with address and branch navigation.

use crate::{RemoteAddress, RemoteReference, Retain};
use dialog_capability::Did;
use dialog_varsig::Principal;

/// A loaded remote repository.
///
/// Holds the retained address and a remote reference scoped to
/// `remote/{name}`, used for branch revision cells.
#[derive(Debug, Clone)]
pub struct RemoteRepository {
    site: RemoteReference,
    address: Retain<RemoteAddress>,
}

impl RemoteRepository {
    /// Construct from a retained address cell and its remote reference.
    pub fn new(address: Retain<RemoteAddress>, remote: RemoteReference) -> Self {
        Self {
            address,
            site: remote,
        }
    }

    /// The subject DID of the remote repository.
    pub fn did(&self) -> Did {
        self.address.get().subject.clone()
    }

    /// The full remote address (site + subject).
    pub fn address(&self) -> RemoteAddress {
        self.address.get().clone()
    }

    /// The site of the remote this repository is on.
    pub fn site(&self) -> &RemoteReference {
        &self.site
    }
}

impl Principal for RemoteRepository {
    fn did(&self) -> Did {
        self.address.get().subject.clone()
    }
}
