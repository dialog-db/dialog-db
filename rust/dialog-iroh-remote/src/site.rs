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

use std::sync::Arc;

use dialog_capability::{Effect, Fork, Site};

use crate::channel::{Channel, Unconfigured};

/// A peer that performs dialog effects, reached over a [`Channel`].
///
/// Holds the channel rather than building one, because what a channel is
/// depends on where this runs: a native process dials directly, a
/// browser's worker relays datagrams through a page, and both are the
/// same iroh endpoint from here. Choosing that is the embedder's job and
/// is done once, not per remote.
#[derive(Clone)]
pub struct Iroh {
    channel: Arc<dyn Channel>,
}

impl Iroh {
    /// Reach peers over `channel`.
    pub fn new(channel: impl Channel + 'static) -> Self {
        Self {
            channel: Arc::new(channel),
        }
    }

    pub(crate) fn channel(&self) -> &Arc<dyn Channel> {
        &self.channel
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
