//! Asynchronous promise-like enum for representing async computation states.

/// Represents the state of an asynchronous computation that may or may not be complete.
///
/// This enum is used throughout the diagnose tool to handle data that is loaded
/// asynchronously in the background, allowing the UI to remain responsive while
/// data is being fetched or computed.
#[derive(Debug)]
pub enum Promise<T> {
    /// The computation has completed and the result is available
    Resolved(T),
    /// The computation is still in progress
    Pending,
}

impl<T> Clone for Promise<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        match self {
            Self::Resolved(arg0) => Self::Resolved(arg0.clone()),
            Self::Pending => Self::Pending,
        }
    }
}
