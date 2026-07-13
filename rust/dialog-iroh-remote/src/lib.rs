#![warn(missing_docs)]

//! Iroh-powered peer-to-peer remote backend for dialog-db.
//!
//! This crate provides the [`Iroh`] site type for syncing dialog repository
//! data directly between peers over [iroh](https://www.iroh.computer/)
//! QUIC connections, plus a per-space gossip swarm that lets blocks be
//! fetched from *any* replicating peer instead of a single remote store.
//!
//! See `notes/iroh-remote.md` in the repository root for the full design.
//!
//! # Overview
//!
//! An [`IrohAddress`] names a peer by its endpoint id (the ed25519 public
//! key of its iroh endpoint), optionally with relay/direct address hints.
//! Adding one as a remote works like any other transport:
//!
//! ```ignore
//! repo.remote("laptop")
//!     .create("<endpoint-id>".parse::<IrohAddress>()?)
//!     .perform(&env)
//!     .await?;
//! ```
//!
//! At authorize time the [`Iroh`] fork builds the same signed UCAN
//! invocation chain the UCAN-S3 site sends to its HTTP access service. The
//! serving peer verifies that chain exactly as the access service does —
//! but instead of redeeming it for a presigned URL, it performs the effect
//! directly against its local replica and returns the result. The peer is
//! its own access service and its own store.
//!
//! # Serving a space
//!
//! ```ignore
//! let node = IrohNode::builder()
//!     .host(subject.clone(), storage_env)
//!     .spawn()
//!     .await?;
//! node.join_swarm(&subject, bootstrap_addresses).await?;
//! ```
//!
//! # Gossip block swarm
//!
//! Every space derives a deterministic gossip topic from its subject DID.
//! Peers that replicate the space join the topic (bootstrapping from the
//! iroh remotes they already know), broadcast `Want` when a block read
//! misses, answer `Have` when they hold a wanted block, and transfer the
//! bytes over the direct, capability-checked remote protocol — never over
//! the gossip overlay itself.

pub mod iroh;

pub use iroh::*;
