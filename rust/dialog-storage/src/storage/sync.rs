use crate::DialogStorageError;

use super::StorageBackend;
use async_trait::async_trait;
use dialog_common::{ConditionalSend, ConditionalSync};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct SynchronizedStorage<Local, Remote> {
    local: Arc<Mutex<Local>>,
    remote: Remote,
}

impl<Local, Remote> SynchronizedStorage<Local, Remote>
where
    Local: StorageBackend,
    Remote: StorageBackend<Key = Local::Key, Value = Local::Value>,
{
    pub fn new(local: Local, remote: Remote) -> Self {
        Self {
            local: Arc::new(Mutex::new(local)),
            remote,
        }
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Local, Remote> StorageBackend for SynchronizedStorage<Local, Remote>
where
    Local: StorageBackend + ConditionalSend,
    Local::Key: Clone,
    Local::Value: Clone,
    DialogStorageError: From<Local::Error> + From<Remote::Error>,
    Remote: StorageBackend<Key = Local::Key, Value = Local::Value> + ConditionalSync,
    Remote::Error: ConditionalSend,
{
    type Key = Local::Key;
    type Value = Local::Value;
    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.local.set(key.clone(), value.clone()).await?;
        // TODO: It would be nice to be able to aggregate blocks and send them
        // in bulk to the remote, since in the majority of cases we will almost
        // certainly be writing > 1 block at a time. That's not even getting to
        // the problem that we really don't want to extend the latency of reads
        // and writes by network RTT.
        self.remote.set(key, value).await?;
        Ok(())
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        Ok(if let local_value @ Some(_) = self.local.get(key).await? {
            local_value
        } else if let Some(remote_value) = self.remote.get(key).await? {
            self.local
                .lock()
                .await
                .set(key.clone(), remote_value.clone())
                .await?;
            Some(remote_value)
        } else {
            None
        })
    }
}
