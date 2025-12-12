use async_trait::async_trait;
use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};

use crate::{DialogStorageError, HashType};

mod cbor;
pub use cbor::*;

/// An [Encoder] converts to and from content-addressable bytes
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Encoder: Clone {
    /// The encoded byte representation of a block
    type Bytes: AsRef<[u8]> + 'static + ConditionalSync;
    /// The hash type produced by this [Encoder]
    type Hash: HashType + ConditionalSync;
    /// The error type produced by this [Encoder]
    type Error: Into<DialogStorageError>;

    /// Encode a serializable item into its referencable [`Hash`] and its bytes.
    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug;

    /// Decode bytes into some deserializable type.
    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync;
}
