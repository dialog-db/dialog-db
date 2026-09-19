//! The [`Iroh`] site: a dialog remote that is another dialog process.
//!
//! Mirrors [`UcanSite`] in shape — mint a signed invocation, send it,
//! read the answer — and differs in what is at the far end. A UCAN site
//! redeems its invocation at an access service for a permit and then
//! fetches from S3; this one sends the invocation to a peer that
//! performs the effect itself, which is why the payload travels with it
//! rather than going to a store the peer never sees.
//!
//! [`UcanSite`]: https://docs.rs/dialog-remote-ucan-s3

mod address;
mod authorization;
mod blob;
mod provider;

pub use address::IrohAddress;
pub use authorization::IrohAuthorization;

use futures_util::lock::Mutex;
use std::sync::Arc;

use dialog_capability::{Effect, Fork, Site};

use crate::channel::{Channel, ChannelError, Connect, Ready, Unconfigured};

/// A peer that performs dialog effects, reached over a [`Channel`].
///
/// Holds how to get a channel, and the one it got. What a channel *is*
/// depends on where this runs — a native process dials directly, a
/// browser's worker relays datagrams through a page, and both are the
/// same iroh endpoint from here — so choosing that stays the embedder's
/// job. What changed is when: a site used to demand the finished channel
/// at construction, and now takes the recipe.
///
/// That distinction is the whole point. In a browser the channel cannot
/// exist at construction: it rides a carrier a page opens later, and the
/// worker's environment is built at startup. A site that insisted on the
/// channel up front could only be given [`Unconfigured`], and would
/// refuse every remote for the life of the process no matter what
/// arrived afterwards.
///
/// Built once and shared, the way `dialog-remote-s3` shares one
/// [`reqwest::Client`] rather than making one per request — and for a
/// stronger reason. A client is a connection pool and a second one is
/// waste; an iroh endpoint's key is the peer's *name*, so a second one
/// is a stranger to everything that has spoken to the first.
#[derive(Clone)]
pub struct Iroh {
    connect: Arc<dyn Connect>,
    /// The channel [`Self::connect`] produced, kept for every exchange
    /// after the first.
    ///
    /// An async mutex rather than a `OnceCell` because this crate has no
    /// once-cell that works on both targets — `tokio::sync` is native
    /// only here — and because failure must not be remembered. A
    /// `OnceCell` that stored a failed connect would make "no page has
    /// dialed yet" permanent, when it is the one condition guaranteed to
    /// change on its own.
    channel: Arc<Mutex<Option<Arc<dyn Channel>>>>,
}

impl Iroh {
    /// Reach peers over `channel`.
    pub fn new(channel: impl Channel + 'static) -> Self {
        Self::connecting(Ready::new(channel))
    }

    /// Reach peers over whatever `connect` produces, when first needed.
    pub fn connecting(connect: impl Connect + 'static) -> Self {
        Self {
            connect: Arc::new(connect),
            channel: Arc::new(Mutex::new(None)),
        }
    }

    /// The channel, connecting if this is the first exchange.
    ///
    /// The lock is held across the connect so two concurrent first
    /// exchanges produce one endpoint rather than two. That serializes
    /// them, which is the point: the second waits for the first's
    /// endpoint instead of binding a rival identity.
    pub(crate) async fn channel(&self) -> Result<Arc<dyn Channel>, ChannelError> {
        let mut held = self.channel.lock().await;
        if let Some(channel) = held.as_ref() {
            return Ok(channel.clone());
        }
        let channel = self.connect.connect().await?;
        *held = Some(channel.clone());
        Ok(channel)
    }
}

/// A site that reaches nobody, over [`Unconfigured`].
///
/// Exists so [`Iroh`] can be a field of a composite site built by
/// [`Default`] — see [`Unconfigured`] for why that is a channel rather
/// than an absent one.
impl Default for Iroh {
    fn default() -> Self {
        Self::new(Unconfigured)
    }
}

impl std::fmt::Debug for Iroh {
    /// A channel has no useful rendering and may hold a live connection,
    /// so the site prints as itself.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Iroh")
    }
}

/// Site-owned fork wrapper for [`Iroh`].
///
/// Thin newtype around [`Fork<Iroh, Fx>`] carrying the site's
/// [`SiteFork`](dialog_capability::SiteFork) impl, which is where the
/// invocation is signed and the payload packed beside it.
pub struct IrohFork<Fx: Effect>(pub(crate) Fork<Iroh, Fx>);

impl<Fx: Effect> From<Fork<Iroh, Fx>> for IrohFork<Fx> {
    fn from(fork: Fork<Iroh, Fx>) -> Self {
        Self(fork)
    }
}

impl Site for Iroh {
    type Authorization = IrohAuthorization;
    type Address = IrohAddress;
    type Fork<Fx: Effect> = IrohFork<Fx>;
}

#[cfg(test)]
mod tests;
