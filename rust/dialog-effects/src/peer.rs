//! Asking a peer who it is.
//!
//! # Capability Hierarchy
//!
//! ```text
//! Subject
//!   └── Peer (/peer)
//!         ├── Hello  → Result<Greeting, PeerError>
//!         └── Spaces → Result<Vec<Offer>, PeerError>
//! ```
//!
//! Every other effect asks a peer to do something with data. This one
//! asks it to describe itself, which is what a client needs before it
//! can do anything else useful: which identities a peer answers for,
//! and therefore whether it is the one you meant to reach.
//!
//! [`Hello`] answers with the identities a peer holds; [`Spaces`] with
//! the spaces behind them. Together they are everything a peer will say
//! about itself before it is asked to do anything.
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

/// One space a peer holds.
///
/// The subject is the space's own identity — the DID its repository is
/// named by, and the one a delegation for it would carry. That is the
/// whole of what a peer can say about a space it has not been asked to
/// open, and it is what a caller needs to ask for access to one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Offer {
    /// The space's subject DID.
    pub subject: Did,
    /// What this peer calls it, when it calls it anything.
    ///
    /// A local label and nothing more: the peer's own, not a fact about
    /// the space. A space's display name lives on its content branch and
    /// is only readable once replicated, so this is what labels a space
    /// a caller has not opened yet. Absent from a peer that knows its
    /// spaces by DID alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Ask a peer which spaces it holds.
///
/// The second half of "who are you": [`Hello`] answers with the
/// identities a peer answers for, and this with the spaces behind them.
/// Both are self-description, which is why both hang off [`Peer`] — a
/// caller invoking this holds no authority over any space in the answer,
/// and by definition cannot, because the answer is what tells it which
/// spaces there are to ask about.
///
/// # What this discloses
///
/// A peer's whole inventory, to anyone whose invocation it verifies. The
/// subject of that invocation is the caller's own, not the peer's, so a
/// peer that answers this answers strangers. What bounds it is the
/// carrier: today the only one is a loopback rendezvous, which is to say
/// the disclosure is to processes already on the machine. A peer reached
/// over anything wider needs a policy here, and does not have one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Attenuate)]
pub struct Spaces;

impl Default for Spaces {
    fn default() -> Self {
        Self::new()
    }
}

impl Spaces {
    /// Ask.
    pub fn new() -> Self {
        Self
    }
}

impl Effect for Spaces {
    type Of = Peer;
    type Output = Result<Vec<Offer>, PeerError>;

    fn command() -> &'static str {
        "get/peer/space"
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
