//! What a [`Peer`](super::Peer) handle holds of the peer it represents.
//!
//! Every handle represents a peer: the owner of the replicas it opens and
//! commits to. The mode says which key it acts with.
//!
//! - [`Local`]: the peer's own key. It can do anything the peer can,
//!   including signing delegations and opening sessions.
//! - [`Session`]: a separate operator key, acting on the peer's replicas
//!   within what the peer granted it. It cannot sign as the peer, so it
//!   can neither delegate the peer's authority nor open further sessions.

use dialog_common::{ConditionalSend, ConditionalSync};

/// The peer acting with its own key.
#[derive(Debug, Clone, Copy, Default)]
pub struct Local;

/// A session: the peer's replicas, acted on with a separate operator key
/// under the peer's grant.
#[derive(Debug, Clone, Copy, Default)]
pub struct Session;

/// A mode a [`Peer`](super::Peer) handle can be in.
pub trait Mode: sealed::Sealed + Clone + Copy + ConditionalSend + ConditionalSync + 'static {}

impl Mode for Local {}
impl Mode for Session {}

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Local {}
    impl Sealed for super::Session {}
}
