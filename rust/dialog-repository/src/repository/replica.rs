//! A repository's replica at a peer, picked out before it is opened.
//!
//! ```text
//! peer.repository("notes")          → ReplicaReference { peer, by }
//!   └── .branch("main").open()      → Branch
//! ```
//!
//! The chain names whose replica it is; the environment the command is
//! performed against is who opens it. A worker opens its parent's
//! repository by naming the parent: the repository is found among the
//! parent's spaces, and the branch is the parent's replica of it.

use dialog_capability::{Did, Provider, Subject};
use dialog_effects::authority::{AuthorityError, Identify, OperatorExt as _};
use dialog_effects::memory::List;
use dialog_effects::space;
use dialog_identity::SpaceHandle;
use thiserror::Error;

use crate::registry::RegistryEnv;
use crate::{
    Branch, By, LoadRepositoryError, PeersEnv, RepositoryExt as _, RepositoryMemoryExt as _,
    ResolveError,
};

/// Pick out a repository held by the peer with this DID.
pub trait RepositoryAtExt {
    /// The peer's replica of the repository `by` picks out: by the name
    /// the peer's space for it has, or by the repository's DID.
    fn repository(self, by: impl Into<By>) -> ReplicaReference;
}

impl<D: Into<Did>> RepositoryAtExt for D {
    fn repository(self, by: impl Into<By>) -> ReplicaReference {
        ReplicaReference::new(self, by)
    }
}

/// A repository's replica at a peer. Nothing is read until a command on
/// it is performed.
#[derive(Debug, Clone)]
pub struct ReplicaReference {
    peer: Did,
    by: By,
}

impl ReplicaReference {
    /// The replica `peer` holds of the repository `by` picks out.
    pub fn new(peer: impl Into<Did>, by: impl Into<By>) -> Self {
        Self {
            peer: peer.into(),
            by: by.into(),
        }
    }

    /// The peer holding the replica.
    pub fn peer(&self) -> &Did {
        &self.peer
    }

    /// A branch of the replica, by name.
    pub fn branch(self, name: impl Into<String>) -> ReplicaBranchReference {
        ReplicaBranchReference {
            replica: self,
            name: name.into(),
        }
    }
}

/// A branch of a replica at a peer. Created by
/// [`ReplicaReference::branch`].
#[derive(Debug, Clone)]
pub struct ReplicaBranchReference {
    replica: ReplicaReference,
    name: String,
}

impl ReplicaBranchReference {
    /// Open the branch.
    pub fn open(self) -> OpenReplicaBranch {
        OpenReplicaBranch { branch: self }
    }
}

/// Command to open a branch of a replica at a peer. Created by
/// [`ReplicaBranchReference::open`].
pub struct OpenReplicaBranch {
    branch: ReplicaBranchReference,
}

impl OpenReplicaBranch {
    /// Find the repository, and open the branch there as the replica of
    /// the peer the chain names.
    ///
    /// A repository picked out by name is loaded, not created. The
    /// environment must act for the peer whose replica it opens: acting
    /// on another peer's replica is not supported yet, and is refused
    /// rather than opened under the wrong replica.
    pub async fn perform<Env>(self, env: &Env) -> Result<Branch, OpenReplicaBranchError>
    where
        Env: RegistryEnv + PeersEnv + Provider<space::Load> + Provider<List>,
    {
        let ReplicaBranchReference { replica, name } = self.branch;
        let acting = Identify.perform(env).await?.profile().clone();
        if acting != replica.peer {
            return Err(OpenReplicaBranchError::Foreign {
                peer: replica.peer,
                acting,
            });
        }
        let subject = match replica.by {
            // A repository named by its DID is loaded as the space named by
            // it: from where the peer recorded it, or else from the location
            // its DID names, and it must be that repository.
            By::Entity(entity) => {
                let did = entity.to_string().parse::<Did>().map_err(|_| {
                    OpenReplicaBranchError::NotRepository {
                        entity: entity.to_string(),
                    }
                })?;
                let loaded = SpaceHandle {
                    peer: replica.peer.clone(),
                    name: did.to_string(),
                }
                .load()
                .perform(env)
                .await?
                .did();
                if loaded != did {
                    return Err(OpenReplicaBranchError::NotRepository {
                        entity: entity.to_string(),
                    });
                }
                did
            }
            By::Name(name) => SpaceHandle {
                peer: replica.peer.clone(),
                name,
            }
            .load()
            .perform(env)
            .await?
            .did(),
        };
        Ok(Subject::from(subject)
            .branch(name)
            .open()
            .perform(env)
            .await?)
    }
}

/// Errors returned when opening a branch of a replica at a peer.
#[derive(Debug, Error)]
pub enum OpenReplicaBranchError {
    /// The environment acts for a different peer than the one whose
    /// replica was named.
    #[error("Cannot open {peer}'s replica while acting for {acting}")]
    Foreign {
        /// The peer whose replica was named.
        peer: Did,
        /// The peer the environment acts for.
        acting: Did,
    },

    /// The entity the repository was picked out by is not a DID.
    #[error("{entity} does not name a repository")]
    NotRepository {
        /// The entity.
        entity: String,
    },

    /// The environment could not say who it acts for.
    #[error(transparent)]
    Authority(#[from] AuthorityError),

    /// The repository could not be loaded by its name.
    #[error(transparent)]
    Load(#[from] LoadRepositoryError),

    /// The branch could not be opened.
    #[error(transparent)]
    Open(#[from] ResolveError),
}
