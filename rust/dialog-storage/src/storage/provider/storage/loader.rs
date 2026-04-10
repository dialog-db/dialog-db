//! Space loading and creation.
//!
//! Handles `storage::Load` and `storage::Create` effects, managing
//! location-to-DID mappings and space provider lifecycle.

use std::fmt::Display;
use std::sync::Arc;

use async_trait::async_trait;
use dialog_capability::{Capability, Did, Policy, Provider, did};
use dialog_common::{ConditionalSend, ConditionalSync};
use dialog_credentials::Credential;
use dialog_effects::prelude::*;
use dialog_effects::{credential, storage};
use dialog_varsig::Principal;
use storage::{Location, StorageError};

use super::super::space::SpaceProvider;
use crate::resource::{Pool, Resource};

/// Handles storage::Load and storage::Create, mutating the shared table.
///
/// Maintains a location -> DID mapping so that loading the same location
/// twice returns the existing DID (important for non-persistent backends).
pub struct Loader<S> {
    spaces: Arc<Pool<Did, S>>,
    mounts: Pool<String, Did>,
}

impl<S> Loader<S> {
    pub fn new(spaces: Arc<Pool<Did, S>>) -> Self {
        Self {
            spaces,
            mounts: Pool::new(),
        }
    }

    fn register(&self, did: Did, location_key: String, store: S) {
        self.mounts.insert(location_key, did.clone());
        self.spaces.insert(did, store);
    }

    fn lookup(&self, key: &String) -> Option<Did> {
        self.mounts.get(key)
    }
}

fn location_key(location: &Location) -> String {
    format!("{:?}/{}", location.directory, location.name)
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<storage::Load> for Loader<S>
where
    S: SpaceProvider + Resource<Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(&self, input: Capability<storage::Load>) -> Result<Credential, StorageError> {
        let location = Location::of(&input);
        let key = location_key(location);

        // Return existing credential if this location is already mounted
        if let Some(did) = self.lookup(&key)
            && let Some(store) = self.spaces.get(&did)
        {
            return did!("local:storage")
                .credential()
                .key(credential::SELF)
                .load()
                .perform(&store)
                .await
                .map_err(|e| StorageError::NotFound(e.to_string()));
        }

        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        let cred: Credential = did!("local:storage")
            .credential()
            .key(credential::SELF)
            .load()
            .perform(&store)
            .await
            .map_err(|e| StorageError::NotFound(e.to_string()))?;

        let did = cred.did();
        self.register(did, key, store);
        Ok(cred)
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<S> Provider<storage::Create> for Loader<S>
where
    S: SpaceProvider + Resource<Location> + ConditionalSend,
    S::Error: Display,
    Self: ConditionalSend + ConditionalSync,
{
    async fn execute(
        &self,
        input: Capability<storage::Create>,
    ) -> Result<Credential, StorageError> {
        let location = Location::of(&input);
        let cred = storage::Create::of(&input).credential.clone();
        let key = location_key(location);

        // Check if this location is already mounted
        if self.lookup(&key).is_some() {
            return Err(StorageError::AlreadyExists(key));
        }

        // Check if this DID is already mounted
        let did = cred.did();
        if self.spaces.contains(&did) {
            return Err(StorageError::AlreadyExists(format!("{did}")));
        }

        let store = S::open(location)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        did!("local:storage")
            .credential()
            .key(credential::SELF)
            .save(cred.clone())
            .perform(&store)
            .await
            .map_err(|e| StorageError::Storage(e.to_string()))?;

        self.register(did, key, store);
        Ok(cred)
    }
}
