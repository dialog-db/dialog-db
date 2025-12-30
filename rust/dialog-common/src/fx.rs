#![allow(async_fn_in_trait)]
//! Algebraic effects system.
//!
//! This module provides core traits and types for building algebraic effect systems
//! using generators/coroutines.
//!
//! # Core Types
//!
//! - [`Capability`] - Trait for capability types that bundle requests with their outputs
//! - [`Effect<Output, Cap>`] - Trait for effectful computations that require a capability
//! - [`Task<Cap, F>`] - A composed effect computation
//! - [`Provider`] - Trait for providing capability implementations
//!
//! # Conceptual Model
//!
//! The effect system works as follows:
//!
//! - **Capability**: Defines what operations are available (e.g., `BlockStore`).
//!   A capability bundles a request enum (the operations) with an output enum
//!   (the results of those operations).
//!
//! - **Effect**: An individual operation that requires a capability to be performed.
//!   For example, `BlockStore::get(key)` returns a `Get` struct that implements
//!   `Effect<Option<Vec<u8>>, Cap>` for any `Cap` that includes `BlockStore`.
//!
//! - **Provider**: Something that can fulfill capability requests. This could be
//!   a concrete implementation or a coroutine handle (`Co`) for composed effects.
//!
//! - **Task**: A composed computation that yields capability requests and receives
//!   outputs, ultimately producing a final result.
//!
//! # Example
//!
//! ```ignore
//! use dialog_macros::effect;
//!
//! #[effect]
//! pub trait BlockStore {
//!     async fn get(&self, key: Vec<u8>) -> Option<Vec<u8>>;
//!     async fn set(&mut self, key: Vec<u8>, value: Vec<u8>);
//! }
//!
//! // Usage - individual effects:
//! let content = BlockStore::get(key).perform(&provider).await;
//! BlockStore::set(other_key, content.unwrap()).perform(&provider).await;
//!
//! // Usage - composed task:
//! let task: Task<BlockStore::Capability, _> = Task::new(|co| async move {
//!     let content = BlockStore::get(key).perform(&co).await;
//!     BlockStore::set(other_key, content.unwrap()).perform(&co).await;
//! });
//! task.perform(&provider).await;
//! ```

use std::future::Future;
use std::pin::Pin;

use genawaiter::sync::{Co, Gen};
use genawaiter::{Coroutine, GeneratorState};

/// A capability bundles a set of operations (requests) with their results (outputs).
///
/// This trait is implemented by the `Capability` enum inside generated effect modules.
/// Each capability represents a set of related operations that a provider can fulfill.
pub trait Capability {
    /// The output type returned when capability requests are fulfilled.
    type Output: Default;
}

/// An effectful computation that produces `Output` when performed.
///
/// Types implementing this trait can be performed with a provider using
/// `.perform(&provider).await`. Inside a generator, pass `&co` as the provider.
///
/// The `Cap: Capability` parameter specifies what capability is required to
/// perform this effect. Effects can work with any capability that includes
/// their operations via `From`/`TryFrom` conversions.
pub trait Effect<Output, Cap: Capability>: Sized {
    /// Perform this effect using a provider that supplies the required capability.
    async fn perform<P>(self, provider: &P) -> Output
    where
        P: Provider<Capability = Cap>;
}

/// A provider supplies implementations for capability requests.
///
/// Providers bridge the gap between abstract effects and concrete implementations.
/// When an effect is performed, the provider receives the request and returns
/// the appropriate output.
pub trait Provider {
    /// The capability this provider can fulfill.
    type Capability: Capability;

    /// Fulfill a capability request and return its output.
    async fn provide(&self, request: Self::Capability) -> <Self::Capability as Capability>::Output;
}

/// A composed effect computation that yields capability requests.
///
/// `Task` wraps a generator and implements [`Effect`]. It allows composing
/// multiple effects into a single computation that can be performed atomically.
pub struct Task<Cap: Capability, F: Future> {
    generator: Gen<Cap, Cap::Output, F>,
}

impl<Cap: Capability, F: Future> Task<Cap, F> {
    /// Create a new task from a producer function.
    ///
    /// The producer receives a coroutine handle (`Co`) that can be used to
    /// perform effects within the task.
    pub fn new<P>(producer: P) -> Self
    where
        P: FnOnce(Co<Cap, Cap::Output>) -> F,
    {
        Self {
            generator: Gen::new(producer),
        }
    }
}

impl<Cap: Capability, F: Future> Effect<F::Output, Cap> for Task<Cap, F> {
    async fn perform<P>(self, provider: &P) -> F::Output
    where
        P: Provider<Capability = Cap>,
    {
        let mut generator = self.generator;
        let mut state = Pin::new(&mut generator).resume_with(Cap::Output::default());

        loop {
            match state {
                GeneratorState::Yielded(request) => {
                    let output = provider.provide(request).await;
                    state = Pin::new(&mut generator).resume_with(output);
                }
                GeneratorState::Complete(result) => {
                    return result;
                }
            }
        }
    }
}

/// Implement Provider for Co, allowing effects to be performed directly
/// inside a generator using `.perform(&co)`.
impl<Cap: Capability> Provider for Co<Cap, Cap::Output> {
    type Capability = Cap;

    async fn provide(&self, request: Cap) -> Cap::Output {
        self.yield_(request).await
    }
}

/// Implement Provider for &P where P: Provider, enabling use of references.
impl<P: Provider + ?Sized> Provider for &P {
    type Capability = P::Capability;

    async fn provide(&self, request: Self::Capability) -> <Self::Capability as Capability>::Output {
        (*self).provide(request).await
    }
}

/// Placeholder macro for use inside `#[effectful]` functions.
///
/// This macro is transformed by the `#[effectful]` attribute macro into
/// `.perform(&__co).await` calls. It should never be invoked directly
/// outside of `#[effectful]` functions.
///
/// # Example
///
/// ```ignore
/// use dialog_macros::effectful;
/// use dialog_common::fx::perform;
///
/// #[effectful(BlockStore)]
/// fn get_value(key: Vec<u8>) -> Option<Vec<u8>> {
///     perform!(BlockStore::get(key))
/// }
/// ```
#[macro_export]
macro_rules! perform {
    ($e:expr) => {
        compile_error!(
            "perform! should only be used inside #[effectful] functions. \
             Did you forget to add #[effectful(...)] to your function?"
        )
    };
}
