//! Getting a container to a peer and an answer back.
//!
//! Deliberately not iroh. One exchange is one request and one response,
//! which is all this crate needs to say about transport — and saying
//! only that keeps the site, the authorization and the wire testable
//! without a network, and leaves room for the local dial and a relayed
//! route to be the same code path, which is the whole point of putting
//! iroh underneath.

use dialog_common::{ConditionalSend, ConditionalSync};

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
    /// This channel cannot do what was asked of it.
    ///
    /// Not a peer problem and not retryable: the transport in hand does
    /// not carry this kind of exchange, and dialling harder will not
    /// change that.
    #[error("this channel cannot {attempted}: {detail}")]
    Unsupported {
        /// What was asked for.
        attempted: &'static str,
        /// Why it is not available.
        detail: String,
    },
}

/// One exchange that is bytes in both directions.
///
/// What [`Channel::open`] hands back, and the thing a blob rides. The
/// container has already been sent when a caller gets one; what remains
/// is the body, in whichever direction the effect runs, and the framed
/// answer.
///
/// Both directions exist on every transfer because which one an effect
/// uses is the effect's business: a read sends nothing more and receives
/// until the peer is done, a write sends until it is done and then
/// receives one answer. A transport implements both and lets the caller
/// use what it needs.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Transfer: ConditionalSend {
    /// Append `bytes` to what this side is sending.
    async fn send(&mut self, bytes: &[u8]) -> Result<(), ChannelError>;

    /// Signal that this side will send no more.
    ///
    /// Separate from dropping the transfer, and required: it is what
    /// ends the peer's read, so an unfinished transfer is a hang rather
    /// than a leak. Reading continues afterwards, which is how a write
    /// gets its answer.
    async fn finish(&mut self) -> Result<(), ChannelError>;

    /// Exactly `len` more bytes, failing if the peer finished first.
    ///
    /// A short read here is the peer having stopped mid-frame, which is
    /// a broken exchange rather than the end of one.
    async fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, ChannelError>;

    /// The next of whatever the peer is sending, or `None` at the end.
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, ChannelError>;
}

/// One request, one response — and, where the transport can, one stream.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Channel: ConditionalSync {
    /// Send `request` to `peer` and read its answer.
    ///
    /// `request` is a `ctn-v1` container and the answer is an encoded
    /// [`Response`](crate::wire::Response). Neither is interpreted here.
    async fn exchange(&self, peer: &IrohAddress, request: Vec<u8>)
    -> Result<Vec<u8>, ChannelError>;

    /// Start a streamed exchange: send `request`, keep the stream open.
    ///
    /// The framing differs from [`Channel::exchange`] and the two never
    /// mix — see [`crate::wire`]. `request` goes out length-prefixed,
    /// because unlike an exchange there may be a body behind it and the
    /// peer has to know where one ends and the other starts. The send
    /// side is left open: a read finishes it immediately, a write sends
    /// its blob first.
    ///
    /// Defaulted to refusing, because most channels are not transports:
    /// the loopbacks this crate tests the protocol over have no stream
    /// to give, and a blob effect asking one for a stream should fail
    /// where it asked rather than somewhere further in.
    async fn open(
        &self,
        peer: &IrohAddress,
        request: Vec<u8>,
    ) -> Result<Box<dyn Transfer>, ChannelError> {
        let _ = (peer, request);
        Err(ChannelError::Unsupported {
            attempted: "stream",
            detail: "this channel carries whole requests and answers only".into(),
        })
    }
}

/// How a site gets the channel it will reuse.
///
/// The counterpart of `dialog-remote-s3`'s `http_client()`: a
/// [`reqwest::Client`] owns a connection pool, so it is built once and
/// shared rather than made per request. An iroh endpoint is the same
/// kind of thing and more so — its key *is* the peer's name, so a second
/// one is not a second pool but a second identity.
///
/// It is a trait and not just a value because of what an endpoint needs
/// that a `reqwest::Client` does not. `reqwest::Client::new()` takes no
/// arguments, so S3's site can build one on first use from nothing. An
/// endpoint needs a transport, and in a browser it needs a carrier some
/// page has yet to open — so what a site can hold from the start is the
/// knowledge of how to get one, not the thing itself.
///
/// Called at most once per site for as long as it succeeds. A failure is
/// not remembered: the ordinary reason to fail here is that nothing has
/// dialed yet, and that stops being true without anything being rebuilt.
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
pub trait Connect: ConditionalSync {
    /// Produce the channel, or say why there is not one yet.
    async fn connect(&self) -> Result<std::sync::Arc<dyn Channel>, ChannelError>;
}

/// A [`Connect`] that is already connected.
///
/// What [`Iroh::new`](crate::site::Iroh::new) wraps a channel in, so an
/// embedder holding the thing does not have to describe how to build it
/// and a site has one way to reach its channel rather than two.
pub struct Ready(std::sync::Arc<dyn Channel>);

impl Ready {
    /// Hand this channel over whenever asked.
    pub fn new(channel: impl Channel + 'static) -> Self {
        Self(std::sync::Arc::new(channel))
    }
}

#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
impl Connect for Ready {
    async fn connect(&self) -> Result<std::sync::Arc<dyn Channel>, ChannelError> {
        Ok(self.0.clone())
    }
}

impl ChannelError {
    /// Whether this says the channel is gone rather than unsuitable.
    ///
    /// A transport that could not reach the peer, or an exchange that
    /// broke off, is a link that may be down — worth dropping and
    /// rebuilding. [`ChannelError::Unsupported`] is not: it is a
    /// permanent property of the channel in hand, and a fresh one of the
    /// same kind would refuse the same thing. Reconnecting on it would
    /// turn one honest refusal into a rebuild on every blob.
    pub fn is_broken_link(&self) -> bool {
        matches!(
            self,
            ChannelError::Unreachable { .. } | ChannelError::Interrupted { .. }
        )
    }
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
        Err(nothing_was_dialed(peer))
    }

    /// Also unreachable rather than unsupported: nothing was configured,
    /// so what it could have carried was never the question.
    async fn open(
        &self,
        peer: &IrohAddress,
        _request: Vec<u8>,
    ) -> Result<Box<dyn Transfer>, ChannelError> {
        Err(nothing_was_dialed(peer))
    }
}

fn nothing_was_dialed(peer: &IrohAddress) -> ChannelError {
    ChannelError::Unreachable {
        peer: peer.to_string(),
        detail: "no channel was configured for this site, so nothing was dialed".into(),
    }
}
