#![warn(missing_docs)]
#![warn(clippy::absolute_paths)]
#![warn(clippy::default_trait_access)]
#![warn(clippy::fallible_impl_from)]
#![warn(clippy::panicking_unwrap)]
#![warn(clippy::unused_async)]
#![deny(clippy::partial_pub_fields)]
#![deny(clippy::unnecessary_self_imports)]
#![cfg_attr(not(test), warn(clippy::large_futures))]
#![cfg_attr(not(test), deny(clippy::panic))]

//! Operator layer for Dialog-DB.
//!
//! This crate provides the capability-based operator system: authority
//! credentials, profiles, operator builders, and network dispatch that
//! together form the operational layer above the core artifact store.

mod authority;
pub use authority::*;

mod profile;
pub use profile::*;

mod operator;
pub use operator::*;

/// Test helpers for setting up profiles, operators, and test data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
