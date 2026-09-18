//! Asking a peer who it is.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject
//!   └── Peer (/peer)
//!         └── Hello → Result<Greeting, PeerError>
//! ```
//!
//! Every other effect asks a peer to do something with data. This one
//! asks it to describe itself, which is what a client needs before it
//! can do anything else useful: which identities a peer answers for,
//! and therefore whether it is the one you meant to reach.
//!
//! It is a capability rather than an unauthenticated banner on purpose.
//! A peer's identity is not a secret, but reachability is not permission
//! anywhere else in this system either, and an endpoint that answers
//! before checking a delegation is a different security posture from one
//! that does not. The invocation is signed and verified like any other.
//!
//! [`Identify`](crate::authority::Identify) is the local counterpart and
//! deliberately not this: it is "a direct env query for ambient state
//! rather than a capability invocation", so it names no command and
//! cannot be invoked across a wire.

use crate::Rejection;
use crate::Use;
use dialog_capability::Did;
use dialog_capability::access::AuthorizeError;
pub use dialog_capability::{Attenuate, Capability, Effect, Policy, Subject};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Root attenuation for asking about a peer.
///
/// Contributes no ability segment of its own, as the other domains do:
/// the effect names the whole command.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Peer;

impl Policy for Peer {
    type Of = Use;
}

/// Who a peer is.
///
/// Three DIDs rather than one, because they answer different questions.
/// The *subject* is the authority a peer holds — what it can be asked
/// about. The *profile* is the identity that holds it. The *operator* is
/// the key actually signing, which is session-scoped and is the one that
/// differs between two tonks run by the same person.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Greeting {
    /// The authority this peer answers for.
    pub subject: Did,
    /// The identity holding that authority.
    pub profile: Did,
    /// The session key signing on its behalf.
    pub operator: Did,
}

/// Ask a peer to describe itself.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Attenuate)]
pub struct Hello;

impl Default for Hello {
    fn default() -> Self {
        Self::new()
    }
}

impl Hello {
    /// Ask.
    pub fn new() -> Self {
        Self
    }
}

impl Effect for Hello {
    type Of = Peer;
    type Output = Result<Greeting, PeerError>;

    fn command() -> &'static str {
        "get/peer"
    }
}

/// Why a peer would not describe itself.
#[derive(Debug, Error, Serialize, Deserialize)]
pub enum PeerError {
    /// The request was not authorized.
    #[error(transparent)]
    Authorization(#[from] AuthorizeError),

    /// The request was not carried out, for a reason that is not an
    /// access decision.
    #[error(transparent)]
    Rejected(#[from] Rejection),

    /// The peer could not establish its own identity, which is a fault
    /// in the peer rather than in the request.
    #[error("this peer could not identify itself: {0}")]
    Unidentified(String),
}
