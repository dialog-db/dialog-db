//! Cross-target bound compatibility traits.
//!
//! These support writing code that targets both `wasm32-unknown-unknown` and
//! native targets, where a native implementer may be shared across threads but
//! a wasm one (holding, say, a `JsValue` WebCrypto handle) cannot be.
//!
//! On `wasm32-unknown-unknown` these are no additional bound; elsewhere
//! [`ConditionalSend`] is `Send` and [`ConditionalSync`] is `Sync`. Each means
//! exactly what its name says, so code needing both writes both.
//!
//! `dialog_common` defines a similar pair, but this crate deliberately does
//! not depend on it: this code is meant to be upstreamable to the UCAN repos
//! it was forked from, so it must not reach into dialog internals. (That pair
//! also defines `ConditionalSync` as `Send + Sync`, which these do not.)

// bare-send-ok: this is the definition site, so it must name the auto traits
// it abstracts over; every other use in the crate goes through these.
#[allow(missing_docs)]
#[cfg(not(target_arch = "wasm32"))]
pub trait ConditionalSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<S> ConditionalSend for S where S: Send {}

// bare-send-ok: definition site, as above.
#[allow(missing_docs)]
#[cfg(not(target_arch = "wasm32"))]
pub trait ConditionalSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<S> ConditionalSync for S where S: Sync {}

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
