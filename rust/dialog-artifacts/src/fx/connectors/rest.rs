//! REST connector for opening REST storage backends from remote addresses.

use super::super::connection::Connection;
use super::super::errors::NetworkError;
use super::super::remote::Address as RemoteAddress;
use dialog_storage::RestStorageBackend;
use std::marker::PhantomData;

/// REST storage backend with Vec<u8> keys and values.
pub type RestBackend = RestStorageBackend<Vec<u8>, Vec<u8>>;

/// Connector that opens REST storage backends from remote addresses.
///
/// This connector creates `RestStorageBackend` instances from `RemoteAddress::Rest`
/// configurations.
#[derive(Clone, Default)]
pub struct RestConnector {
    _marker: PhantomData<()>,
}

impl RestConnector {
    /// Create a new REST connector.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Connection<RestBackend> for RestConnector {
    type Address = RemoteAddress;
    type Error = NetworkError;

    async fn open(&self, address: &Self::Address) -> Result<RestBackend, Self::Error> {
        match address {
            RemoteAddress::Rest(config) => RestStorageBackend::new(config.clone())
                .map_err(|e| NetworkError::Connection(format!("Failed to create REST backend: {}", e))),
        }
    }
}
