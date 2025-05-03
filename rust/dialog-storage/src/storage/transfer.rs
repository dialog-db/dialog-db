use async_trait::async_trait;
use dialog_common::ConditionalSend;
use futures_util::Stream;

use crate::StorageBackend;

/// A trait that may be implemented by any [`StorageBackend`] that has the
/// ability to efficiently stream its contents in their entirely (as compared to
/// reading keys individually).
pub trait StorageSource: StorageBackend {
    /// Stream a copy of the contents of the [`StorageBackend`]
    fn read(
        &self,
    ) -> impl Stream<
        Item = Result<
            (
                <Self as StorageBackend>::Key,
                <Self as StorageBackend>::Value,
            ),
            <Self as StorageBackend>::Error,
        >,
    >;

    /// Stream the contents of the [`StorageBackend`], removing it from the
    /// [`StorageSource`] by the time that the [`Stream`] is fully consumed.
    fn drain(
        &mut self,
    ) -> impl Stream<
        Item = Result<
            (
                <Self as StorageBackend>::Key,
                <Self as StorageBackend>::Value,
            ),
            <Self as StorageBackend>::Error,
        >,
    >;
}

/// A trait that may be implemented by any [`StorageBackend`] that has the
/// ability to efficiently persist contents when provided in bulk (as compared
/// to writing entries individually).
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
pub trait StorageSink: StorageBackend {
    /// Consume a [`Stream`] of entries, persisting them to the
    /// [`StorageBackend`]
    async fn write<EntryStream>(
        &mut self,
        stream: EntryStream,
    ) -> Result<(), <Self as StorageBackend>::Error>
    where
        EntryStream: Stream<
                Item = Result<
                    (
                        <Self as StorageBackend>::Key,
                        <Self as StorageBackend>::Value,
                    ),
                    <Self as StorageBackend>::Error,
                >,
            > + ConditionalSend;
}
