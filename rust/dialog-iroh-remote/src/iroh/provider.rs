//! Provider implementations for the [`Iroh`](super::Iroh) site.
//!
//! By the time these run, [`authorize`](crate::IrohFork) has built the
//! signed UCAN invocation and attested it into the invocation. Each
//! provider serializes the container, dials the addressed peer over the
//! process-global node, and decodes the typed response. `/archive/get`
//! additionally falls back to the space's gossip swarm when the addressed
//! peer misses or is unreachable.
//!
//! On wasm targets the transport is not available yet (iroh's browser
//! support does not fit this workspace's targets); providers fail with an
//! execution error while addresses and authorization still compile, so
//! `dialog_network::Network` composes on every target.

#[cfg(not(target_arch = "wasm32"))]
mod native;

#[cfg(target_arch = "wasm32")]
mod web;
