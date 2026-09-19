#![warn(missing_docs)]

//! Peer-to-peer remote for dialog-db.
//!
//! A dialog remote is normally a *service*: S3 serves bytes, an access
//! service redeems a UCAN invocation for a presigned permit to fetch
//! them. This crate is the same protocol with the service replaced by a
//! peer — an invocation travels to another dialog process over an iroh
//! QUIC stream, and that process performs it against its own repository.
//!
//! So the peer is not a storage backend the client drives. It is an
//! access service and a store at once, which is why authorization is not
//! optional here: the wire is open to anyone who can dial, and a
//! capability is the only thing distinguishing a peer from a stranger.

pub mod bind;
pub mod wire;
