use async_trait::async_trait;
use dialog_common::ConditionalSync;

use crate::{DialogStorageError, HashType};

/// An [Encoder] converts to and from content-addressable bytes
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait Encoder<const HASH_SIZE: usize>: Clone {
    /// The in-memory representation of a block
    type Block: ConditionalSync;
    /// The encoded byte representation of a block
    type Bytes: AsRef<[u8]> + 'static + ConditionalSync;
    /// The hash type produced by this [Encoder]
    type Hash: HashType<HASH_SIZE> + ConditionalSync;
    /// The error type produced by this [Encoder]
    type Error: Into<DialogStorageError>;

    /// Encode a serializable item into its referencable [`Hash`] and its bytes.
    async fn encode(&self, block: &Self::Block) -> Result<(Self::Hash, Self::Bytes), Self::Error>;

    /// Decode bytes into a `Block`.
    async fn decode(&self, bytes: &[u8]) -> Result<Self::Block, Self::Error>;
}
