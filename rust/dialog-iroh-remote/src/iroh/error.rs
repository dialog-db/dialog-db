//! Crate error type.

use thiserror::Error;

/// Errors raised by the iroh remote transport and swarm.
#[derive(Debug, Error)]
pub enum IrohRemoteError {
    /// The address could not be resolved to a dialable iroh endpoint.
    #[error("invalid iroh address: {0}")]
    Address(String),

    /// Binding or configuring the local endpoint failed.
    #[error("iroh endpoint error: {0}")]
    Endpoint(String),

    /// Establishing or using a connection to the peer failed.
    #[error("iroh connection error: {0}")]
    Connection(String),

    /// The wire exchange failed (framing, encoding, protocol violation).
    #[error("iroh protocol error: {0}")]
    Protocol(String),

    /// The peer denied the request.
    #[error("denied by peer: {0}")]
    Denied(String),

    /// The gossip swarm failed.
    #[error("iroh gossip error: {0}")]
    Gossip(String),

    /// The operation is not available on this target.
    #[error("iroh remote is not supported on this target: {0}")]
    Unsupported(String),
}
