//! Transparent decorator that redirects to in-memory storage, useful for testing.
//!
//! [`Emulator<T>`] wraps any type `T`, opting it into alternative trait
//! implementations that replace real I/O with in-memory [`Volatile`](crate::provider::volatile::Volatile)
//! storage. The wrapper itself holds no extra state; capability-specific
//! modules supply the actual impls.

/// Transparent wrapper that opts `T` into in-memory behaviour.
pub struct Emulator<T>(pub T);

impl<T> Emulator<T> {
    /// Wrap an existing value.
    pub fn of(inner: T) -> Self {
        Self(inner)
    }
}

impl<T> From<T> for Emulator<T> {
    fn from(inner: T) -> Self {
        Self::of(inner)
    }
}
