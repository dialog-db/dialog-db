#![allow(async_fn_in_trait)]
//! Algebraic effects system.
//!
//! This module provides core traits and types for building algebraic effect systems
//! using generators/coroutines.
//!
//! # Core Types
//!
//! - [`Capability`] - Trait for capability systems with request/response types
//! - [`Effect<Output, Req, Resp>`] - Trait for effectful computations
//! - [`Task<C, F>`] - A composed effect computation
//! - [`Provider<C>`] - Trait for fulfilling capability requests
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
//! // Usage:
//! let content = get(key).perform(&co).await;
//! set(other_key, content.unwrap()).perform(&co).await;
//! ```

use std::future::Future;
use std::pin::Pin;

use genawaiter::sync::{Co, Gen};
use genawaiter::{Coroutine, GeneratorState};

/// Trait for capability systems that have separate request and response types.
///
/// This is implemented by the `Command` struct inside generated effect modules.
pub trait Capability {
    /// The request enum type (yielded by generators).
    type Request;
    /// The response enum type (received by generators).
    type Response: Default;
}

/// An effectful computation that produces `Output`.
///
/// Types implementing this trait can be performed with a provider using
/// `.perform(&provider).await`. Inside a generator, pass `&co` as the provider.
///
/// The `Req` and `Resp` type parameters allow effects to work with any
/// request/response types that can carry their messages via `From`/`TryFrom`.
pub trait Effect<Output, Req, Resp>: Sized {
    /// Perform this effect using a provider.
    async fn perform<P>(self, provider: &P) -> Output
    where
        P: Provider<Request = Req, Response = Resp>;
}

/// A provider that can fulfill capability requests.
pub trait Provider {
    /// The request type this provider accepts.
    type Request;
    /// The response type this provider returns.
    type Response;

    /// Provide a response for the given request.
    async fn provide(&self, request: Self::Request) -> Self::Response;
}

/// A composed effect computation that yields requests.
///
/// `Task` wraps a generator and implements [`Effect`].
pub struct Task<C: Capability, F: Future> {
    generator: Gen<C::Request, C::Response, F>,
}

impl<C: Capability, F: Future> Task<C, F> {
    /// Create a new task from a producer function.
    pub fn new<P>(producer: P) -> Self
    where
        P: FnOnce(Co<C::Request, C::Response>) -> F,
    {
        Self {
            generator: Gen::new(producer),
        }
    }
}

impl<C: Capability, F: Future> Effect<F::Output, C::Request, C::Response> for Task<C, F> {
    async fn perform<P>(self, provider: &P) -> F::Output
    where
        P: Provider<Request = C::Request, Response = C::Response>,
    {
        let mut generator = self.generator;
        let mut state = Pin::new(&mut generator).resume_with(C::Response::default());

        loop {
            match state {
                GeneratorState::Yielded(req) => {
                    let response = provider.provide(req).await;
                    state = Pin::new(&mut generator).resume_with(response);
                }
                GeneratorState::Complete(output) => {
                    return output;
                }
            }
        }
    }
}

/// Implement Provider for Co, allowing effects to be performed directly
/// inside a generator using `.perform(&co)`.
impl<Req, Resp> Provider for Co<Req, Resp> {
    type Request = Req;
    type Response = Resp;

    async fn provide(&self, request: Req) -> Resp {
        self.yield_(request).await
    }
}

/// Implement Provider for &P where P: Provider, enabling use of references.
impl<P: Provider + ?Sized> Provider for &P {
    type Request = P::Request;
    type Response = P::Response;

    async fn provide(&self, request: Self::Request) -> Self::Response {
        (*self).provide(request).await
    }
}
