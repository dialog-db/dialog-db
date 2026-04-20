//! Repository layer for Dialog-DB.
//!
//! This crate layers a capability-based repository abstraction on top of
//! the operator (`dialog-operator`) and effect (`dialog-effects`) crates.
//! It provides:
//!
//! - [`Repository`] — a subject-scoped handle over a space, generic over
//!   the credential type so the same surface covers both signers (full
//!   access, can delegate) and verifiers (read-only).
//! - [`RepositoryExt`] on `SpaceHandle` — `.open()` / `.load()` /
//!   `.create()` commands that turn into `Repository` values once
//!   performed against an operator.
//! - [`Cell`], [`Retain`], and their `Publish` / `Resolve` commands —
//!   transactional memory cells with built-in edition tracking, plus
//!   `.fork(&address)` variants that retarget the same commands at a
//!   remote site.
//! - [`RepositoryArchiveExt`] and [`LocalIndex`] — extensions and CAS
//!   adapters that bridge archive capabilities with the prolly tree's
//!   `ContentAddressedStorage` trait.
//!
//! Branches, remotes, and sync operations build on this base in
//! follow-up crates / PRs.

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

mod repository;
pub use repository::*;
