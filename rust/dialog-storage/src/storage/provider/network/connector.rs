//! Trait for resolving addresses into connections.

/// Implementations open (or reuse cached) connections to remote sites.
/// The connection type must itself implement [`Provider`] for the effects
/// that will be routed through it.
///
/// [`Provider`]: dialog_capability::Provider
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Connector<Address> {
    /// The connection type returned for a given address.
    type Connection;
    /// Error type for connection failures.
    type Error;

    /// Open a connection to the given address, or return a cached one.
    async fn open(&mut self, address: &Address) -> Result<&mut Self::Connection, Self::Error>;
}
