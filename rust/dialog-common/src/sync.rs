//! Cross-target bound compatibility traits
//!
//! These traits support writing async code that may target both
//! `wasm32-unknown-unknown` as well as native targets where it may be the case
//! that an implementer will be shared across threads.
//!
//! On `wasm32-unknown-unknown` targets, the traits effectively represent no
//! new bound. But, on other targets they represent `Send` or `Send + Sync`
//! bounds (depending on which one is used).

/// A marker trait that is `Send` on native targets and no-op on Wasm.
///
/// Use this to conditionally require `Send` bounds in cross-platform code.
#[cfg(not(target_arch = "wasm32"))]
pub trait ConditionalSend: Send {}

#[cfg(not(target_arch = "wasm32"))]
impl<S> ConditionalSend for S where S: Send {}

/// A marker trait that is `Send + Sync` on native targets and no-op on Wasm.
///
/// Use this to conditionally require `Send + Sync` bounds in cross-platform code.
#[cfg(not(target_arch = "wasm32"))]
pub trait ConditionalSync: Send + Sync {}

#[cfg(not(target_arch = "wasm32"))]
impl<S> ConditionalSync for S where S: Send + Sync {}

/// A marker trait that is `Send` on native targets and no-op on Wasm.
///
/// Use this to conditionally require `Send` bounds in cross-platform code.
#[cfg(target_arch = "wasm32")]
pub trait ConditionalSend {}

#[cfg(target_arch = "wasm32")]
impl<S> ConditionalSend for S {}

/// A marker trait that is `Send + Sync` on native targets and no-op on Wasm.
///
/// Use this to conditionally require `Send + Sync` bounds in cross-platform code.
#[cfg(target_arch = "wasm32")]
pub trait ConditionalSync {}

#[cfg(target_arch = "wasm32")]
impl<S> ConditionalSync for S {}
