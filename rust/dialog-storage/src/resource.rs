//! Resource abstraction for address-based connections.
//!
//! A [`Resource`] knows how to open itself from an address. A [`Pool`]
//! caches opened resources by address so repeated access reuses existing
//! instances.

mod pool;
pub use pool::*;

/// A resource that can be opened from an address.
///
/// The address carries all information needed to construct the resource
/// (credentials, endpoint, issuer, etc.).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Resource<Address>: Sized {
    /// Error that can occur when opening the resource.
    type Error;

    /// Open a new resource from the given address.
    async fn open(address: &Address) -> Result<Self, Self::Error>;
}
