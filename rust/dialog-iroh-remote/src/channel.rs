//! Getting a container to a peer and an answer back.
//!
//! Deliberately not iroh. One exchange is one request and one response,
//! which is all this crate needs to say about transport — and saying
//! only that keeps the site, the authorization and the wire testable
//! without a network, and leaves room for the local dial and a relayed
//! route to be the same code path, which is the whole point of putting
//! iroh underneath.

use dialog_common::ConditionalSync;

use crate::site::IrohAddress;

/// Why an exchange did not produce an answer.
///
/// Transport failures only: a peer that answered — even to refuse — did
/// not fail, and its answer is a [`Response`](crate::wire::Response).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChannelError {
    /// The peer could not be reached.
    #[error("could not reach {peer}: {detail}")]
    Unreachable {
        /// Who was being dialed.
        peer: String,
        /// Why, as far as the transport would say.
        detail: String,
    },
    /// The exchange started and did not finish.
    #[error("the exchange with {peer} broke off: {detail}")]
    Interrupted {
        /// Who was being talked to.
        peer: String,
        /// Why, as far as the transport would say.
        detail: String,
    },
}

/// One request, one response.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Channel: ConditionalSync {
    /// Send `request` to `peer` and read its answer.
    ///
    /// `request` is a `ctn-v1` container and the answer is an encoded
    /// [`Response`](crate::wire::Response). Neither is interpreted here.
    async fn exchange(&self, peer: &IrohAddress, request: Vec<u8>)
    -> Result<Vec<u8>, ChannelError>;
}

/// A channel that reaches nobody.
///
/// What an [`Iroh`](crate::site::Iroh) site holds when it was built by
/// [`Default`] rather than handed a channel. It exists because a
/// composite site is a struct of sites and every field must be a site,
/// so there is no representing "this transport was not configured" by
/// leaving it out — only by a channel that says so.
///
/// It says so precisely. A misconfigured client and an offline peer are
/// different bugs with the same symptom, so the failure names the
/// missing configuration rather than reporting the peer down.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unconfigured;

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Channel for Unconfigured {
    async fn exchange(
        &self,
        peer: &IrohAddress,
        _request: Vec<u8>,
    ) -> Result<Vec<u8>, ChannelError> {
        Err(ChannelError::Unreachable {
            peer: peer.to_string(),
            detail: "no channel was configured for this site, so nothing was dialed".into(),
        })
    }
}
