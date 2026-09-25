//! What a [`Peer`](super::Peer) handle holds of the peer it represents.
//!
//! Every handle represents a peer: the owner of the replicas it opens and
//! commits to. The mode says which key it acts with.
//!
//! - [`Local`]: the peer's own key. It can do anything the peer can,
//!   including signing delegations and opening sessions.
//! - [`Session`]: a separate operator key, acting on the peer's replicas
//!   within what the peer granted it. It is never handed the peer's key,
//!   nor a repository's, so it cannot sign as either: it can neither
//!   delegate the peer's authority nor open further sessions.
//!
//! A session sharing the peer's storage is not yet confined to its
//! grants for local reads and writes; only its requests to other peers
//! are proven from them.

use dialog_common::{ConditionalSend, ConditionalSync};

/// The peer acting with its own key.
#[derive(Debug, Clone, Copy, Default)]
pub struct Local;

/// A session: the peer's replicas, acted on with a separate operator key
/// under the peer's grant.
#[derive(Debug, Clone, Copy, Default)]
pub struct Session;

/// A mode a [`Peer`](super::Peer) handle can be in.
pub trait Mode:
    sealed::Sealed + Clone + Copy + ConditionalSend + ConditionalSync + 'static
{
    /// Whether a handle in this mode is handed the keys of the peer and
    /// of the repositories it holds. A session acts with its own key, so
    /// it is not: it can use what it was granted, never become its
    /// grantor.
    const HOLDS_KEYS: bool;
}

impl Mode for Local {
    const HOLDS_KEYS: bool = true;
}
impl Mode for Session {
    const HOLDS_KEYS: bool = false;
}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Local {}
    impl Sealed for super::Session {}
}
