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

use dialog_ucan_core::time::timestamp::{Duration, SystemTime};
use futures_util::lock::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
    /// What this site currently knows about its link.
    ///
    /// An async mutex rather than a `OnceCell` because this crate has no
    /// once-cell that works on both targets — `tokio::sync` is native
    /// only here — and because a link is not a thing that happens once.
    /// It comes up, it goes down, and it comes back.
    link: Arc<Mutex<Link>>,
    /// Counts links this site has brought up, so a failure report can
    /// name the one it is about. Never reused and never reset.
    generation: Arc<AtomicU64>,
}

/// A live channel, and which link it came from.
///
/// The generation is what makes [`Iroh::broke`] safe under concurrency.
/// Two exchanges can overlap: the first gets a channel, the second finds
/// it broken and reconnects, and then the first fails on the channel it
/// was already holding. Without a generation that late failure would
/// drop the *replacement* — evicting a healthy link and, under steady
/// traffic, never settling. With one, a report about a link that has
/// already been replaced is recognised as stale and ignored.
pub(crate) struct Connection {
    channel: Arc<dyn Channel>,
    generation: u64,
}

/// A channel has no useful rendering and may hold a live connection, so
/// a connection prints as which link it is — the part that is about the
/// site rather than the transport.
impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Connection(link {})", self.generation)
    }
}

impl std::ops::Deref for Connection {
    type Target = dyn Channel;

    fn deref(&self) -> &Self::Target {
        self.channel.as_ref()
    }
}

/// The link, up or down.
enum Link {
    /// Nothing is connected. `failures` counts *consecutive connect
    /// attempts* that failed, and is what `retry_after` is derived from.
    Down {
        failures: u32,
        retry_after: Option<SystemTime>,
    },
    /// A channel that has not been reported broken.
    Up {
        channel: Arc<dyn Channel>,
        generation: u64,
    },
}

impl Default for Link {
    fn default() -> Self {
        Link::Down {
            failures: 0,
            retry_after: None,
        }
    }
}

/// The first retry waits this long, and each further one doubles.
const FIRST_BACKOFF: Duration = Duration::from_millis(100);

/// The longest a site will wait before trying to connect again.
///
/// Low on purpose, and lower than a backoff schedule would usually go.
/// Backoff exists to stop a client hammering something that is failing,
/// and it pays for that with latency on recovery — which is the wrong
/// trade here, because the thing being waited for is a page in the same
/// browser opening a carrier. A minute of backoff would mean a user
/// clicking connect and watching nothing happen long after the carrier
/// landed.
///
/// The right fix is an event: whoever learns the carrier arrived calls
/// [`Iroh::revive`], and the wait ends immediately. This cap is what
/// bounds the damage where nothing does.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

impl Iroh {
    /// Reach peers over `channel`.
    pub fn new(channel: impl Channel + 'static) -> Self {
        Self::connecting(Ready::new(channel))
    }

    /// Reach peers over whatever `connect` produces, when first needed.
    pub fn connecting(connect: impl Connect + 'static) -> Self {
        Self {
            connect: Arc::new(connect),
            link: Arc::new(Mutex::new(Link::default())),
            generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The channel, connecting or reconnecting if there is not one up.
    ///
    /// The lock is held across the connect so two concurrent exchanges
    /// produce one endpoint rather than two. That serializes them, which
    /// is the point: the second waits for the first's endpoint instead
    /// of binding a rival identity.
    pub(crate) async fn connection(&self) -> Result<Connection, ChannelError> {
        let mut link = self.link.lock().await;

        let (failures, retry_after) = match &*link {
            Link::Up {
                channel,
                generation,
            } => {
                return Ok(Connection {
                    channel: channel.clone(),
                    generation: *generation,
                });
            }
            Link::Down {
                failures,
                retry_after,
            } => (*failures, *retry_after),
        };

        if let Some(retry_after) = retry_after
            && SystemTime::now() < retry_after
        {
            return Err(ChannelError::Unreachable {
                peer: "a peer".into(),
                detail: format!(
                    "{failures} attempt(s) to connect have failed; waiting before the next"
                ),
            });
        }

        match self.connect.connect().await {
            Ok(channel) => {
                // Generations only ever go forward, so a report naming
                // an older one can be told from one naming this.
                let generation = self.generation.fetch_add(1, Ordering::Relaxed) + 1;
                *link = Link::Up {
                    channel: channel.clone(),
                    generation,
                };
                Ok(Connection {
                    channel,
                    generation,
                })
            }
            Err(error) => {
                let failures = failures.saturating_add(1);
                *link = Link::Down {
                    failures,
                    retry_after: Some(SystemTime::now() + backoff(failures)),
                };
                Err(error)
            }
        }
    }

    /// Report that an exchange over `connection` failed.
    ///
    /// Drops the link when the failure says it is gone, so the next
    /// exchange rebuilds it. `failures` resets to zero: this link *was*
    /// up, so the thing that just broke is not evidence that connecting
    /// is failing, and the first attempt to restore it should be
    /// immediate rather than served out of a backoff earned by something
    /// else.
    pub(crate) async fn broke(&self, connection: &Connection, error: &ChannelError) {
        if !error.is_broken_link() {
            return;
        }
        let mut link = self.link.lock().await;
        match &*link {
            // Somebody already replaced it; this report is about a link
            // that no longer exists.
            Link::Up { generation, .. } if *generation != connection.generation => {}
            _ => *link = Link::default(),
        }
    }

    /// Try again now, whatever the backoff said.
    ///
    /// For the embedder that *knows* the world changed — a carrier
    /// arrived, an endpoint bound — rather than waiting to find out. A
    /// backoff is a guess about when retrying is worth it, and an event
    /// beats a guess.
    pub async fn revive(&self) {
        let mut link = self.link.lock().await;
        if let Link::Down { .. } = &*link {
            *link = Link::default();
        }
    }
}

/// `FIRST_BACKOFF` doubled per consecutive failure, capped.
fn backoff(failures: u32) -> Duration {
    let shift = failures.saturating_sub(1).min(16);
    FIRST_BACKOFF.saturating_mul(1u32 << shift).min(MAX_BACKOFF)
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
