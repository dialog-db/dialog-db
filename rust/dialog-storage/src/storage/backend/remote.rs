use async_trait::async_trait;
use base58::ToBase58;
use dialog_common::{ConditionalSend, ConditionalSync};
use std::marker::PhantomData;
use url::Url;

use crate::DialogStorageError;

use super::StorageBackend;

#[derive(Clone)]
pub struct CloudflareWorkerStorageBackend<Key, Value> {
    key: PhantomData<Key>,
    value: PhantomData<Value>,
    url: Url,
}

impl<Key, Value> CloudflareWorkerStorageBackend<Key, Value> {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            key: PhantomData,
            value: PhantomData,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Key, Value> StorageBackend for CloudflareWorkerStorageBackend<Key, Value>
where
    Key: Clone + AsRef<[u8]> + ConditionalSync,
    Value: Clone + From<Vec<u8>> + ConditionalSync,
    Vec<u8>: From<Value>,
{
    type Key = Key;
    type Value = Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        let mut url = self.url.clone();
        url.set_path(&format!("/block/{}", key.as_ref().to_base58()));
        reqwest::Client::new()
            .put(url)
            .body(Vec::from(value))
            .send()
            .await
            .map_err(|error| {
                DialogStorageError::StorageBackend(format!(
                    "Failed to send block to CloudFlare Worker: {error}"
                ))
            })?;
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let mut url = self.url.clone();
        url.set_path(&format!("/block/{}", key.as_ref().to_base58()));
        Ok(reqwest::get(url)
            .await
            .map_err(|error| {
                DialogStorageError::StorageBackend(format!(
                    "Failed to fetch block from Cloudflare Worker: {error}"
                ))
            })?
            .bytes()
            .await
            .ok()
            .and_then(|bytes| {
                if bytes.len() == 0 {
                    None
                } else {
                    Some(bytes.to_vec().into())
                }
            }))
    }
}
