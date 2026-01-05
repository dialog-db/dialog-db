//! Effectful remote branch operations.
//!
//! This module provides the effectful version of RemoteBranch that works
//! with the algebraic effects system.

use super::error::ReplicaError;
use super::types::{BranchId, Revision, Site};
use crate::fx::effects::{Memory, Store, effectful};
use crate::fx::local::Address as LocalAddress;
use crate::fx::remote::Address as RemoteAddress;
use dialog_common::fx::Effect;

/// Effectful remote branch.
///
/// Unlike the original RemoteBranch which holds storage references,
/// this version holds only state and uses effects for storage operations.
#[derive(Debug, Clone)]
pub struct RemoteBranch {
    /// Local address for caching.
    address: LocalAddress,
    /// Name of the remote this branch is part of.
    site: Site,
    /// Branch id on the remote.
    id: BranchId,
    /// Cached revision (from local cache).
    cached_revision: Option<Revision>,
    /// Remote address for operations.
    remote_address: Option<RemoteAddress>,
}

impl RemoteBranch {
    /// Opens a remote branch.
    ///
    /// This loads the cached revision from local storage and resolves
    /// the remote address from the site configuration.
    #[effectful(Memory<LocalAddress>)]
    pub fn open(address: LocalAddress, site: Site, id: BranchId) -> Result<Self, ReplicaError> {
        // Load cached revision from local storage
        let cache_key = format!("remote/{}/{}", site, id).into_bytes();

        let cached_revision =
            match perform!(Memory::<LocalAddress>().resolve(address.clone(), cache_key))? {
                Some((content, _edition)) => {
                    let revision: Revision =
                        serde_ipld_dagcbor::from_slice(&content).map_err(|e| {
                            ReplicaError::StorageError(format!("Deserialize error: {}", e))
                        })?;
                    Some(revision)
                }
                None => None,
            };

        // Load the remote configuration to get address
        let site_key = format!("site/{}", site).into_bytes();
        let remote_address =
            match perform!(Memory::<LocalAddress>().resolve(address.clone(), site_key))? {
                Some((content, _edition)) => {
                    let state: crate::replica::RemoteState =
                        serde_ipld_dagcbor::from_slice(&content).map_err(|e| {
                            ReplicaError::StorageError(format!("Deserialize remote state: {}", e))
                        })?;
                    Some(RemoteAddress::rest(state.address))
                }
                None => {
                    return Err(ReplicaError::RemoteNotFound {
                        remote: site.clone(),
                    });
                }
            };

        Ok(Self {
            address,
            site,
            id,
            cached_revision,
            remote_address,
        })
    }

    /// Returns the local address for this remote branch.
    pub fn address(&self) -> &LocalAddress {
        &self.address
    }

    /// Returns the site for this remote branch.
    pub fn site(&self) -> &Site {
        &self.site
    }

    /// Returns the branch id.
    pub fn id(&self) -> &BranchId {
        &self.id
    }

    /// Returns the cached revision.
    pub fn revision(&self) -> Option<&Revision> {
        self.cached_revision.as_ref()
    }

    /// Returns the remote address if resolved.
    pub fn remote_address(&self) -> Option<&RemoteAddress> {
        self.remote_address.as_ref()
    }

    /// Fetches the current revision from the remote.
    ///
    /// Returns the updated RemoteBranch along with the fetched revision.
    /// Takes self by value and returns ownership.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn fetch(self) -> Result<(Self, Option<Revision>), ReplicaError> {
        let remote = self
            .remote_address
            .clone()
            .ok_or_else(|| ReplicaError::RemoteNotFound {
                remote: self.site.clone(),
            })?;

        // Key on the remote for this branch's revision
        let remote_key = format!("local/{}", self.id).into_bytes();

        // Fetch from remote
        let remote_revision = match perform!(Memory::<RemoteAddress>().resolve(remote, remote_key))?
        {
            Some((content, _edition)) => {
                let revision: Revision = serde_ipld_dagcbor::from_slice(&content)
                    .map_err(|e| ReplicaError::StorageError(format!("Deserialize error: {}", e)))?;
                Some(revision)
            }
            None => None,
        };

        // Update local cache
        let cache_key = format!("remote/{}/{}", self.site, self.id).into_bytes();

        // Get current cache edition
        let cache_edition = perform!(
            Memory::<LocalAddress>().resolve(self.address.clone(), cache_key.clone())
        )?
        .map(|(_, edition)| edition);

        // Serialize new content
        let new_content = match &remote_revision {
            Some(rev) => Some(
                serde_ipld_dagcbor::to_vec(rev)
                    .map_err(|e| ReplicaError::StorageError(format!("Serialize error: {}", e)))?,
            ),
            None => None,
        };

        perform!(Memory::<LocalAddress>().replace(
            self.address.clone(),
            cache_key,
            cache_edition,
            new_content
        ))?;

        let updated = Self {
            cached_revision: remote_revision.clone(),
            ..self
        };
        Ok((updated, remote_revision))
    }

    /// Publishes a revision to the remote.
    ///
    /// Returns the updated RemoteBranch with the new cached revision.
    #[effectful(Memory<LocalAddress> + Memory<RemoteAddress>)]
    pub fn publish(self, revision: Revision) -> Result<Self, ReplicaError> {
        let remote = self
            .remote_address
            .clone()
            .ok_or_else(|| ReplicaError::RemoteNotFound {
                remote: self.site.clone(),
            })?;

        let remote_key = format!("local/{}", self.id).into_bytes();

        // Get current remote edition
        let edition = perform!(
            Memory::<RemoteAddress>().resolve(remote.clone(), remote_key.clone())
        )?
        .map(|(_, edition)| edition);

        // Serialize and publish
        let content = serde_ipld_dagcbor::to_vec(&revision)
            .map_err(|e| ReplicaError::StorageError(format!("Serialize error: {}", e)))?;

        perform!(Memory::<RemoteAddress>().replace(remote, remote_key, edition, Some(content)))?;

        // Update local cache
        let cache_key = format!("remote/{}/{}", self.site, self.id).into_bytes();

        let cache_edition = perform!(
            Memory::<LocalAddress>().resolve(self.address.clone(), cache_key.clone())
        )?
        .map(|(_, edition)| edition);

        let cache_content = serde_ipld_dagcbor::to_vec(&revision)
            .map_err(|e| ReplicaError::StorageError(format!("Serialize error: {}", e)))?;

        perform!(Memory::<LocalAddress>().replace(
            self.address.clone(),
            cache_key,
            cache_edition,
            Some(cache_content)
        ))?;

        Ok(Self {
            cached_revision: Some(revision),
            ..self
        })
    }

    /// Imports blocks to remote storage.
    #[effectful(Store<RemoteAddress>)]
    pub fn import_blocks(self, blocks: Vec<(Vec<u8>, Vec<u8>)>) -> Result<Self, ReplicaError> {
        let remote = self
            .remote_address
            .clone()
            .ok_or_else(|| ReplicaError::RemoteNotFound {
                remote: self.site.clone(),
            })?;

        perform!(Store::<RemoteAddress>().import(remote, blocks))?;
        Ok(self)
    }
}
