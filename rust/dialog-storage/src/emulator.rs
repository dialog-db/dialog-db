//! Transparent emulator decorator.
//!
//! [`Emulator<T>`] wraps any type `T`, marking it for emulated (in-memory)
//! behaviour. Capability-specific modules provide the actual impls, e.g.
//! `impl Connector<Address> for Emulator<Network<Issuer>>`.

/// Transparent wrapper that marks `T` for emulated behaviour.
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
