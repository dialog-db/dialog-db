#![warn(missing_docs)]

//! Reaching a dialog peer over iroh.
//!
//! Every replica holder is a peer, and most peers are *services*: S3
//! serves bytes, an access service redeems a UCAN invocation for a
//! presigned permit to fetch them. This crate reaches a peer that is
//! another dialog process instead: an invocation travels over an iroh
//! QUIC stream, and that process performs it against its own
//! repository. Such a peer is named by its own key, the `did:key` of
//! its endpoint id, and an iroh address is one more address a contact
//! can be reached at.
//!
//! So the peer is not a storage backend the client drives. It is an
//! access service and a store at once, which is why authorization is not
//! optional here: the wire is open to anyone who can dial, and a
//! capability is the only thing distinguishing a peer from a stranger.

/// A store and a channel to test against.
#[cfg(any(test, feature = "helpers"))]
pub mod helpers;

pub mod carries;
pub mod channel;
pub mod resolve;
pub mod serve;
pub mod site;

/// The iroh-backed [`Channel`](channel::Channel) and its accept loop.
#[cfg(feature = "transport")]
pub mod transport;

pub mod wire;
