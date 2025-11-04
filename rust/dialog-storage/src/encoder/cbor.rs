use crate::DialogStorageError;

use super::Encoder;
use async_trait::async_trait;
use dialog_common::ConditionalSync;
use serde::{Serialize, de::DeserializeOwned};

/// A basic [`Encoder`] implementation that encodes data as IPLD-compatible CBOR
#[derive(Clone)]
pub struct CborEncoder;

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl Encoder for CborEncoder {
    type Bytes = Vec<u8>;
    type Hash = [u8; 32];
    type Error = DialogStorageError;

    async fn encode<T>(&self, block: &T) -> Result<(Self::Hash, Self::Bytes), Self::Error>
    where
        T: Serialize + ConditionalSync + std::fmt::Debug,
    {
        let bytes = serde_ipld_dagcbor::to_vec(block)
            .map_err(|error| DialogStorageError::EncodeFailed(format!("{error}")))?;
        let hash = blake3::hash(&bytes).as_bytes().to_owned();

        Ok((hash, bytes))
    }

    async fn decode<T>(&self, bytes: &[u8]) -> Result<T, Self::Error>
    where
        T: DeserializeOwned + ConditionalSync,
    {
        serde_ipld_dagcbor::from_slice::<T>(bytes)
            .map_err(|error| DialogStorageError::DecodeFailed(format!("{error}")))
    }
}
