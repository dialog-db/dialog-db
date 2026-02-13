//! Abstractions over futures.
//!
//! This module provides abstractions to work with both `Send` and `!Send` futures
//!
//! This is useful for supporting both async runtimes that require `Send` futures (like `tokio` or `async-std`)
//! and runtimes that require `!Send` futures. This is a workaround for having to
//! duplicate code in async traits for `Send` and `!Send` futures.

use futures::future::{BoxFuture, Future, LocalBoxFuture};

/// An abstraction over a [`Future`]s.
pub trait FutureKind {
    /// An abstraction over a future type.
    ///
    /// This is especially useful for abstracting over `Send` and `!Send` futures.
    type Future<'a, T: 'a>: Future<Output = T> + 'a;
}

/// Abstraction over [`Send`] futrues.
#[derive(Debug, Clone, Copy)]
pub enum Sendable {}
impl FutureKind for Sendable {
    type Future<'a, T: 'a> = BoxFuture<'a, T>;
}

/// Abstraction over [`!Send`][Send] futures.
#[derive(Debug, Clone, Copy)]
pub enum Local {}
impl FutureKind for Local {
    type Future<'a, T: 'a> = LocalBoxFuture<'a, T>;
}
