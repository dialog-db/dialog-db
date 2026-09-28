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
