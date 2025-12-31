//! Site - a pool of storage resources keyed by address.

use super::connection::Connection;
use std::hash::Hash;

/// A site manages access to storage and memory backends at various addresses.
///
/// It uses connectors to open connections on demand. The backends themselves
/// are expected to handle connection sharing internally (e.g., via Arc).
///
/// # Type Parameters
///
/// - `S`: Storage backend type
/// - `M`: Transactional memory backend type
/// - `SC`: Connector for opening storage backends
/// - `MC`: Connector for opening memory backends
/// - `A`: Address type used to identify resources
///
/// # Example
///
/// ```ignore
/// use dialog_artifacts::fx::{Site, Connection};
/// use dialog_artifacts::fx::remote::Address;
///
/// // Create a site for remote connections
/// let site: Site<RestBackend, RestBackend, RestConnector, RestConnector, Address> =
///     Site::new(store_connector, memory_connector);
///
/// // Open a store at a specific address
/// let store = site.store(&address).await?;
/// ```
#[derive(Clone)]
pub struct Site<S, M, SC, MC, A>
where
    SC: Connection<S, Address = A>,
    MC: Connection<M, Address = A>,
    A: Hash + Eq + Clone,
{
    store_connector: SC,
    memory_connector: MC,
    _marker: std::marker::PhantomData<(S, M, A)>,
}

impl<S, M, SC, MC, A> Site<S, M, SC, MC, A>
where
    SC: Connection<S, Address = A>,
    MC: Connection<M, Address = A>,
    A: Hash + Eq + Clone,
{
    /// Create a new site with the given connectors.
    pub fn new(store_connector: SC, memory_connector: MC) -> Self {
        Self {
            store_connector,
            memory_connector,
            _marker: std::marker::PhantomData,
        }
    }

    /// Open a storage backend at the given address.
    pub async fn store(&self, address: &A) -> Result<S, SC::Error> {
        self.store_connector.open(address).await
    }

    /// Open a memory backend at the given address.
    pub async fn memory(&self, address: &A) -> Result<M, MC::Error> {
        self.memory_connector.open(address).await
    }
}
