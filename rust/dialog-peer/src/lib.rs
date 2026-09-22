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

//! Peers and sessions for Dialog-DB.
//!
//! A [`Peer`] is a site identified by a key, holding replicas: the
//! persisted signer, the storage its spaces are mounted in, the network
//! dispatch and the branch of its own repository that serves as registry.
//! A [`Session`] is a constrained peer: one acting key and the grants it
//! was built with, and the environment every `perform` takes. See
//! `notes/peer-and-session.md`.

pub use dialog_identity::*;

mod session;
pub use session::{Allowance, PeerError, PeerSpace, Session, SessionBuilder, SessionKey};

mod peer;
pub use peer::*;

/// Test helpers for setting up peers, sessions, and test data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
