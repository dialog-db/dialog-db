use super::Transaction;

/// Types that can contribute write operations to a [`Transaction`].
///
/// This is the extensibility point for the write path. Any type that
/// knows how to express itself as assertions and/or retractions
/// implements `Edit` and can be passed to
/// [`Transaction::edit`](super::Transaction::edit). The [`Statement`](crate::Statement)
/// trait provides a higher-level API on top of `Edit` that separates
/// assert and retract directions.
pub trait Edit {
    /// Merge this item's operations into the transaction
    fn merge(self, transaction: &mut Transaction);
}
