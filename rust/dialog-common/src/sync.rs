//! Cross-target bound compatability traits
//!
//! These traits support writing async code that may target both
//! `wasm32-unknown-unknown` as well as native targets where it may be the case
//! that an implementer will be shared across threads.
//!
//! On `wasm32-unknown-unknown` targets, the traits effectively represent no
//! new bound. But, on other targets they represent `Send` or `Send + Sync`
//! bounds (depending on which one is used).

#[allow(missing_docs)]
#[cfg(not(target_arch = "wasm32"))]
pub trait ConditionalSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<S> ConditionalSend for S where S: Send {}

#[allow(missing_docs)]
#[cfg(not(target_arch = "wasm32"))]
pub trait ConditionalSync: Send + Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<S> ConditionalSync for S where S: Send + Sync {}

#[allow(missing_docs)]
#[cfg(target_arch = "wasm32")]
pub trait ConditionalSend {}

#[cfg(target_arch = "wasm32")]
impl<S> ConditionalSend for S {}

#[allow(missing_docs)]
#[cfg(target_arch = "wasm32")]
pub trait ConditionalSync {}

#[cfg(target_arch = "wasm32")]
impl<S> ConditionalSync for S {}

/// Platform-appropriate shared interior mutability cell.
///
/// - Native: `std::sync::RwLock` (multi-threaded read-write lock)
/// - WASM: `std::cell::RefCell` (single-threaded borrow checking)
///
/// # Example
/// ```
/// use dialog_common::SharedCell;
///
/// let cell = SharedCell::new(42);
///
/// // Reading
/// {
///     let value = cell.read();
///     assert_eq!(*value, 42);
/// }
///
/// // Writing
/// {
///     let mut value = cell.write();
///     *value = 100;
/// }
///
/// assert_eq!(*cell.read(), 100);
/// ```
/// TODO: Remove this and just use RwLock
#[derive(Debug)]
pub struct SharedCell<T>(std::sync::RwLock<T>);

impl<T> SharedCell<T> {
    /// Creates a new SharedCell with the given value
    pub fn new(value: T) -> Self {
        Self(std::sync::RwLock::new(value))
    }

    /// Acquires a read lock, blocking until it can be acquired
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.0.read().expect("lock poisoned")
    }

    /// Acquires a write lock, blocking until it can be acquired
    pub fn write(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.0.write().expect("lock poisoned")
    }
}
