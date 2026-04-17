//! Publish command for writing a cell value.

use dialog_capability::fork::Fork;
use dialog_capability::site::SiteAddress;
use dialog_capability::{Capability, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::memory;
use dialog_effects::memory::prelude::CellExt;
use dialog_storage::Encoder;
use parking_lot::RwLock;
use serde::Serialize;
use std::fmt::Debug;

use super::cell::Cache;
use crate::RepositoryError;

/// Command to publish a cell value.
///
/// Created by [`Cell::publish`](super::Cell::publish). Execute with
/// `.perform(&env)` for local or `.fork(&address).perform(&env)` for remote.
pub struct Publish<T, Codec: Clone> {
    pub(super) capability: Capability<memory::Cell>,
    pub(super) cache: Cache<T, Codec>,
    pub(super) value: T,
}

impl<T, Codec> Publish<T, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Execute locally.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<memory::Publish>,
    {
        let content = self.cache.encode(&self.value).await?;
        let when = self.cache.edition().map(Into::into);
        let new_edition = self
            .capability
            .publish(content, when)
            .perform(env)
            .await?;
        self.cache.update(self.value, new_edition.into_bytes());
        Ok(())
    }

    /// Fork to a remote site.
    pub fn fork<A: SiteAddress>(self, address: &A) -> ForkPublish<T, A, Codec> {
        ForkPublish {
            capability: self.capability,
            cache: self.cache,
            value: self.value,
            address: address.clone(),
        }
    }
}

/// Command to publish a cell value to a remote site.
pub struct ForkPublish<T, A: SiteAddress, Codec: Clone> {
    capability: Capability<memory::Cell>,
    cache: Cache<T, Codec>,
    value: T,
    address: A,
}

impl<T, A, Codec> ForkPublish<T, A, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    A: SiteAddress,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Execute against a remote site.
    ///
    /// Uses the cached edition from the last resolve. Call
    /// `cell.resolve().fork(&addr).perform(&env)` first to sync the edition.
    pub async fn perform<Env>(self, env: &Env) -> Result<(), RepositoryError>
    where
        Env: Provider<Fork<A::Site, memory::Publish>> + ConditionalSync,
    {
        let content = self.cache.encode(&self.value).await?;
        let when = self.cache.edition().map(Into::into);
        let new_edition = self
            .capability
            .publish(content, when)
            .fork(&self.address)
            .perform(env)
            .await?;
        self.cache.update(self.value, new_edition.into_bytes());
        Ok(())
    }
}

/// Command to publish to a retained cell.
pub struct RetainPublish<'a, T, Codec: Clone> {
    pub(super) inner: Publish<T, Codec>,
    pub(super) sticky: &'a RwLock<T>,
    pub(super) value: T,
}

impl<T, Codec> RetainPublish<'_, T, Codec>
where
    T: Serialize + Clone + ConditionalSync + Debug,
    Codec: Encoder<Bytes = Vec<u8>> + Clone,
{
    /// Execute locally.
    pub async fn perform(
        self,
        env: &(impl Provider<memory::Publish> + ConditionalSync),
    ) -> Result<(), RepositoryError> {
        self.inner.perform(env).await?;
        *self.sticky.write() = self.value;
        Ok(())
    }
}
