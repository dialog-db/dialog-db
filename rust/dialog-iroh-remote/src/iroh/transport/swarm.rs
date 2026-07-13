//! Per-space gossip swarm.
//!
//! Every space derives a deterministic topic from its subject DID; peers
//! that replicate the space join the topic, bootstrapping from the iroh
//! remotes they already have configured. The overlay carries *advisory*
//! messages only ([`SwarmMessage`]): `Want` asks who has a block, `Have`
//! answers with the responder's dialable address, `Announce` signals a
//! published revision. Block bytes always travel over the direct remote
//! protocol, where the regular UCAN invocation is presented and verified —
//! so the same authorization built for the addressed remote can be
//! redeemed at whichever peer answers first.
//!
//! # Head updates
//!
//! The swarm doubles as the wake-up signal for reactive sync. A joined
//! handle exposes [`updates`](SwarmHandle::updates), a stream of
//! [`HeadUpdate`]s that fires when
//!
//! - a peer pushes *into this device* — the host publishes into a hosted
//!   space and notifies locally ([`HeadUpdateOrigin::Pushed`]), and
//! - a peer *announces* a publish elsewhere in the swarm
//!   ([`HeadUpdateOrigin::Announced`]).
//!
//! dialog's query subscriptions are deliberately poll-driven; a head
//! update is the signal that a poll (or a `pull`) will find something.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use dialog_capability::Did;
use dialog_common::Blake3Hash;
use dialog_effects::memory::Version;
use futures_util::StreamExt;
use iroh_gossip::api::{Event, GossipSender};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use tokio::sync::{Mutex, broadcast, mpsc, watch};

use super::host::SubjectHost;
use super::node::IrohNode;
use crate::protocol::SwarmMessage;
use crate::{IrohAddress, IrohRemoteError};

/// How long a swarm fetch waits for `Have` answers before giving up.
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Derive the deterministic gossip topic for a space.
pub fn topic_for(subject: &Did) -> TopicId {
    let hash = Blake3Hash::hash(format!("dialog-db/gossip/v0:{subject}").as_bytes());
    TopicId::from_bytes(*hash.as_bytes())
}

type Pending = Arc<Mutex<HashMap<(String, Vec<u8>), mpsc::UnboundedSender<IrohAddress>>>>;

/// How many not-yet-consumed head updates a subscriber may buffer before
/// the oldest are dropped (`RecvError::Lagged`). Updates are wake-up
/// signals, not a log: a lagging consumer that re-resolves the head after
/// a `Lagged` error has lost nothing.
const UPDATE_BUFFER: usize = 64;

/// Where a [`HeadUpdate`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadUpdateOrigin {
    /// A peer pushed into a space hosted on this device.
    Pushed,
    /// A peer announced a publish elsewhere in the swarm.
    Announced,
}

/// A memory cell of the swarm's subject moved to a new version.
///
/// For a branch head, `space` is `branch/{name}` and `cell` is
/// `revision`. Advisory: consumers react by re-resolving the cell (e.g.
/// `branch.pull()`); they must not trust the carried version for
/// anything but change detection.
#[derive(Debug, Clone)]
pub struct HeadUpdate {
    /// The memory space the cell lives in (e.g. `branch/main`).
    pub space: String,
    /// The memory cell that changed (e.g. `revision`).
    pub cell: String,
    /// The version the cell moved to.
    pub version: Version,
    /// Whether the update landed on this device or was announced by a
    /// peer.
    pub origin: HeadUpdateOrigin,
}

/// A joined per-space gossip swarm.
///
/// Obtained from [`IrohNode::join_swarm`]. Fetches route through
/// [`fetch`](Self::fetch); hosting nodes additionally answer `Want`s and
/// announce publishes through the attached [`SubjectHost`].
pub struct SwarmHandle {
    subject: Did,
    sender: GossipSender,
    pending: Pending,
    joined: watch::Receiver<bool>,
    updates: broadcast::Sender<HeadUpdate>,
    /// Last version announced per (space, cell): suppresses duplicate
    /// announces when the same publish is observed through more than one
    /// path (the host's publish handler and the storage publish stream).
    announced: Mutex<HashMap<(String, String), Version>>,
    task: tokio::task::JoinHandle<()>,
}

impl SwarmHandle {
    /// Subscribe to the space's topic and spawn the receiver loop.
    pub(crate) async fn join(
        node: Arc<IrohNode>,
        gossip: &Gossip,
        subject: &Did,
        bootstrap: Vec<IrohAddress>,
        host: Option<Arc<dyn SubjectHost>>,
    ) -> Result<Arc<Self>, IrohRemoteError> {
        // Seed dialing hints so gossip can reach the bootstrap peers by id.
        let mut bootstrap_ids = Vec::new();
        for address in &bootstrap {
            node.remember(address);
            bootstrap_ids.push(address.endpoint_id()?);
        }

        let topic = gossip
            .subscribe(topic_for(subject), bootstrap_ids)
            .await
            .map_err(|e| IrohRemoteError::Gossip(format!("subscribing to swarm: {e}")))?;
        let (sender, mut receiver) = topic.split();

        let pending: Pending = Arc::new(Mutex::new(HashMap::new()));
        let (joined_tx, joined_rx) = watch::channel(false);
        let (updates_tx, _) = broadcast::channel(UPDATE_BUFFER);

        let loop_updates = updates_tx.clone();
        let loop_subject = subject.clone();
        let loop_pending = pending.clone();
        let loop_sender = sender.clone();
        let loop_node = Arc::downgrade(&node);
        let task = tokio::spawn(async move {
            while let Some(event) = receiver.next().await {
                let event = match event {
                    Ok(event) => event,
                    Err(_) => break,
                };
                match event {
                    Event::NeighborUp(_) => {
                        let _ = joined_tx.send(true);
                    }
                    Event::Received(message) => {
                        let Ok(message) =
                            serde_ipld_dagcbor::from_slice::<SwarmMessage>(&message.content)
                        else {
                            continue;
                        };
                        match message {
                            SwarmMessage::Want { catalog, digest } => {
                                let Some(node) = loop_node.upgrade() else {
                                    break;
                                };
                                let Some(host) = &host else { continue };
                                let Ok(digest_bytes) = <[u8; 32]>::try_from(digest.as_slice())
                                else {
                                    continue;
                                };
                                if host
                                    .has_block(&catalog, &Blake3Hash::from(digest_bytes))
                                    .await
                                {
                                    let answer = SwarmMessage::Have {
                                        catalog,
                                        digest,
                                        provider: node.address(),
                                    };
                                    if let Ok(bytes) = serde_ipld_dagcbor::to_vec(&answer) {
                                        let _ = loop_sender.broadcast(Bytes::from(bytes)).await;
                                    }
                                }
                            }
                            SwarmMessage::Have {
                                catalog,
                                digest,
                                provider,
                            } => {
                                if let Some(node) = loop_node.upgrade() {
                                    node.remember(&provider);
                                }
                                let pending = loop_pending.lock().await;
                                if let Some(waiter) = pending.get(&(catalog, digest)) {
                                    let _ = waiter.send(provider);
                                }
                            }
                            // Advisory head movement: surface it to local
                            // subscribers as a wake-up signal.
                            SwarmMessage::Announce {
                                space,
                                cell,
                                version,
                            } => {
                                tracing::trace!(
                                    subject = %loop_subject,
                                    space,
                                    cell,
                                    "revision announced on swarm"
                                );
                                let _ = loop_updates.send(HeadUpdate {
                                    space,
                                    cell,
                                    version,
                                    origin: HeadUpdateOrigin::Announced,
                                });
                            }
                        }
                    }
                    Event::NeighborDown(_) | Event::Lagged => {}
                }
            }
        });

        Ok(Arc::new(Self {
            subject: subject.clone(),
            sender,
            pending,
            joined: joined_rx,
            updates: updates_tx,
            announced: Mutex::new(HashMap::new()),
            task,
        }))
    }

    /// The space this swarm replicates.
    pub fn subject(&self) -> &Did {
        &self.subject
    }

    /// Wait until at least one neighbor is connected on the topic.
    pub async fn joined(&self) {
        let mut joined = self.joined.clone();
        while !*joined.borrow() {
            if joined.changed().await.is_err() {
                return;
            }
        }
    }

    /// Subscribe to head updates for this space.
    ///
    /// Fires when a peer pushes into a space hosted on this device and
    /// when a peer announces a publish elsewhere in the swarm. Updates
    /// are wake-up signals: on receipt (or on a `Lagged` error after
    /// falling behind), re-resolve the cell — typically `branch.pull()`.
    pub fn updates(&self) -> broadcast::Receiver<HeadUpdate> {
        self.updates.subscribe()
    }

    /// Announce a published revision on the swarm (advisory).
    ///
    /// Fired automatically for publishes that arrive over the remote
    /// protocol (the host) and for local publishes forwarded via
    /// [`IrohNode::announce_publishes`]; deduplicated per (space, cell)
    /// version so a publish observed through both paths is announced
    /// once.
    pub async fn announce(&self, space: String, cell: String, version: Version) {
        {
            let mut announced = self.announced.lock().await;
            let key = (space.clone(), cell.clone());
            if announced.get(&key) == Some(&version) {
                return;
            }
            announced.insert(key, version.clone());
        }
        let message = SwarmMessage::Announce {
            space,
            cell,
            version,
        };
        if let Ok(bytes) = serde_ipld_dagcbor::to_vec(&message) {
            let _ = self.sender.broadcast(Bytes::from(bytes)).await;
        }
    }

    /// Spawn a task invoking `react` for every head update on the given
    /// space/cell — the auto-pull loop, packaged:
    ///
    /// ```ignore
    /// swarm.follow("branch/main", "revision", move |_| {
    ///     let branch = branch.clone();
    ///     let operator = operator.clone();
    ///     async move {
    ///         let _ = branch.pull().perform(&*operator).await;
    ///     }
    /// });
    /// ```
    ///
    /// Updates are wake-up signals and the reaction is expected to be
    /// idempotent (a `pull` that finds nothing is a no-op). A subscriber
    /// that falls behind skips the missed signals and reacts to the next
    /// one. Abort the returned handle to stop following; the task also
    /// ends when the swarm handle is dropped.
    pub fn follow<F, Fut>(
        &self,
        space: impl Into<String>,
        cell: impl Into<String>,
        mut react: F,
    ) -> tokio::task::JoinHandle<()>
    where
        F: FnMut(HeadUpdate) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let mut updates = self.updates();
        let space = space.into();
        let cell = cell.into();
        tokio::spawn(async move {
            loop {
                match updates.recv().await {
                    Ok(update) if update.space == space && update.cell == cell => {
                        react(update).await;
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    /// A publish landed on this device (a peer pushed into a hosted
    /// space): notify local subscribers and announce it to the swarm.
    pub(crate) async fn published(&self, space: String, cell: String, version: Version) {
        let _ = self.updates.send(HeadUpdate {
            space: space.clone(),
            cell: cell.clone(),
            version: version.clone(),
            origin: HeadUpdateOrigin::Pushed,
        });
        self.announce(space, cell, version).await;
    }

    /// Fetch a block from the swarm: broadcast `Want`, wait (bounded) for
    /// `Have` answers, and read the block from answering peers over the
    /// direct remote protocol using the caller's `invocation` — the same
    /// signed UCAN container built for the addressed remote, valid at any
    /// peer replicating the subject.
    ///
    /// Returns `None` if no peer produced the block within the timeout.
    pub async fn fetch(
        &self,
        node: &Arc<IrohNode>,
        catalog: &str,
        digest: &Blake3Hash,
        invocation: &[u8],
    ) -> Option<Vec<u8>> {
        let key = (catalog.to_string(), digest.as_bytes().to_vec());
        let (tx, mut rx) = mpsc::unbounded_channel();
        self.pending.lock().await.insert(key.clone(), tx);

        let result = async {
            let want = SwarmMessage::Want {
                catalog: key.0.clone(),
                digest: key.1.clone(),
            };
            let bytes = serde_ipld_dagcbor::to_vec(&want).ok()?;
            self.sender.broadcast(Bytes::from(bytes)).await.ok()?;

            tokio::time::timeout(FETCH_TIMEOUT, async {
                while let Some(provider) = rx.recv().await {
                    // Never fetch from ourselves.
                    if provider.endpoint_id().ok() == Some(node.endpoint().id()) {
                        continue;
                    }
                    let Ok(connection) = node.connect(&provider).await else {
                        continue;
                    };
                    if let Ok(Ok(Some(block))) =
                        super::request::archive_get(&connection, invocation.to_vec()).await
                    {
                        // Content addressing: verify what the swarm handed us.
                        if &Blake3Hash::hash(&block) == digest {
                            return Some(block);
                        }
                    }
                }
                None
            })
            .await
            .ok()
            .flatten()
        }
        .await;

        self.pending.lock().await.remove(&key);
        result
    }
}

impl Drop for SwarmHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl std::fmt::Debug for SwarmHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SwarmHandle")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}
