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

//! Peers: the runtime capability environment for Dialog. One type, built
//! over a key, a storage and the branch of a repository that holds the
//! peer's own state; a worker is a peer with grants from another. See
//! [`Peer`].

pub use dialog_identity::*;

mod peer;
pub use peer::{
    Allowance, Local, Mode, OpenFuture, OpenPeer, Peer, PeerBuilder, PeerError, PeerKey, PeerSpace,
    Runtime, Session, Unset,
};

/// Test helpers: unique names, peers over volatile storage, sample data.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;
