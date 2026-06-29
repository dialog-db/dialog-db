//! Resource abstraction for address-based connections.
//!
//! A [`Resource`] knows how to open itself from an address. A [`Pool`]
//! caches opened resources by address so repeated access reuses existing
//! instances.

mod pool;
pub use pool::*;

use dialog_common::ConditionalSync;

/// A resource that can be opened from an address.
///
/// The address carries all information needed to construct the resource
/// (credentials, endpoint, issuer, etc.).
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Resource<Address>: Sized {
    /// Error that can occur when opening the resource.
    type Error;

    /// Open a resource at the given address, creating its backing store
    /// if absent. This is the open-or-create path.
    async fn open(address: &Address) -> Result<Self, Self::Error>;

    /// Open an *existing* resource at the given address, failing if it
    /// does not already exist. Unlike [`open`](Resource::open) this must
    /// never bring the backing store into being.
    ///
    /// The default delegates to [`open`](Resource::open), which is correct
    /// for backends whose `open` is lazy — it constructs a handle without
    /// materializing anything (e.g. a filesystem path is created only on
    /// first write). A backend whose `open` eagerly creates its store
    /// (IndexedDB opens a database into existence) MUST override this to
    /// probe for existence first and error when absent.
    async fn load(address: &Address) -> Result<Self, Self::Error>
    where
        Address: ConditionalSync,
    {
        Self::open(address).await
    }
}
