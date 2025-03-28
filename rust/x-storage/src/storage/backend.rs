use async_trait::async_trait;
use x_common::{ConditionalSend, ConditionalSync};

use crate::XStorageError;

mod memory;
pub use memory::*;

/// A [StorageBackend] is a facade over some generalized storage substrate that
/// is capable of storing and/or retrieving values by some key
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StorageBackend {
    /// The key type used by this [StorageBackend]
    type Key: ConditionalSync;
    /// The value type able to be stored by this [StorageBackend]
    type Value: ConditionalSend;
    /// The error type produced by this [StorageBackend]
    type Error: Into<XStorageError>;

    /// Store the given value against the given key
    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;
    /// Retrieve a value (if any) stored against the given key
    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error>;
}
