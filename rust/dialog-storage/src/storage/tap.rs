use async_trait::async_trait;
use dialog_common::ConditionalSync;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::DialogStorageError;

use super::StorageBackend;

pub enum TapOperation<T> {
    Set(T),
    Get(T),
}

#[derive(Clone)]
pub struct TappedStorage<Backend>
where
    Backend: StorageBackend,
{
    backend: Backend,
    tx: Option<UnboundedSender<TapOperation<(Backend::Key, Backend::Value)>>>,
}

impl<Backend> TappedStorage<Backend>
where
    Backend: StorageBackend,
{
    pub fn new(backend: Backend) -> Self {
        Self { backend, tx: None }
    }

    pub fn tap(
        &mut self,
    ) -> Result<UnboundedReceiver<TapOperation<(Backend::Key, Backend::Value)>>, DialogStorageError>
    {
        if let Some(_) = self.tx {
            return Err(DialogStorageError::StorageBackend(
                "Attempt to tap already-tapped storage".into(),
            ));
        }

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        self.tx = Some(tx);

        Ok(rx)
    }

    pub fn untap(&mut self) -> Result<(), DialogStorageError> {
        if self.tx.is_none() {
            return Err(DialogStorageError::StorageBackend(
                "Attempt to untap already-untapped storage".into(),
            ));
        }

        self.tx = None;
        Ok(())
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
impl<Backend> StorageBackend for TappedStorage<Backend>
where
    Backend: StorageBackend + ConditionalSync,
    Backend::Key: Clone,
    Backend::Value: Clone,
    DialogStorageError: From<Backend::Error>,
{
    type Key = Backend::Key;

    type Value = Backend::Value;

    type Error = DialogStorageError;

    async fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        if let Some(tx) = &self.tx {
            tx.send(TapOperation::Set((key.clone(), value.clone())));
        }

        Ok(self.backend.set(key, value).await?)
    }

    async fn get(&self, key: &Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        let value = self.backend.get(key).await?;

        if let (Some(tx), Some(value)) = (&self.tx, &value) {
            tx.send(TapOperation::Get((key.clone(), value.clone())));
        }

        Ok(value)
    }
}
