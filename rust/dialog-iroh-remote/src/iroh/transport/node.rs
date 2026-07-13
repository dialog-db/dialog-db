//! The iroh node: endpoint, router, gossip, connection pool, swarms.
//!
//! `dialog_network::Network` is a zero-state dispatch table (`Copy`,
//! `Default`), so the [`Iroh`](crate::Iroh) site marker cannot own a live
//! endpoint. Instead the process has one [`IrohNode`] — either explicitly
//! built and [`install`]ed (hosts do this, wiring their storage in via
//! [`builder().host(..)`](IrohNodeBuilder::host)), or lazily created with
//! defaults the first time an iroh remote is dialed (pure clients need zero
//! setup).

use std::collections::HashMap;
use std::sync::Arc;

use dialog_capability::Did;
use iroh::address_lookup::memory::MemoryLookup;
use iroh::endpoint::Connection;
use iroh::protocol::Router;
use iroh::{Endpoint, EndpointId, SecretKey};
use iroh_gossip::net::Gossip;
use tokio::sync::{Mutex, OnceCell};

use super::host::{HostRegistry, SubjectHost};
use super::swarm::SwarmHandle;
use crate::protocol::ALPN;
use crate::{IrohAddress, IrohRemoteError};
use dialog_storage::provider::storage::PublishEvent;

static NODE: OnceCell<Arc<IrohNode>> = OnceCell::const_new();

/// Install `node` as the process-global node used by the [`Iroh`](crate::Iroh)
/// site providers. Fails if a node is already installed (or was already
/// lazily created by a provider).
pub fn install(node: Arc<IrohNode>) -> Result<(), IrohRemoteError> {
    NODE.set(node)
        .map_err(|_| IrohRemoteError::Endpoint("an iroh node is already installed".into()))
}

/// The process-global node, lazily creating a default (client-only, n0
/// discovery and relays) node if none was installed.
pub async fn node() -> Result<Arc<IrohNode>, IrohRemoteError> {
    NODE.get_or_try_init(|| async { IrohNode::builder().spawn().await })
        .await
        .cloned()
}

/// The installed node, if any. Unlike [`node`], never creates one.
pub fn installed() -> Option<Arc<IrohNode>> {
    NODE.get().cloned()
}

/// Builder for an [`IrohNode`].
pub struct IrohNodeBuilder {
    endpoint: Option<Endpoint>,
    secret_key: Option<SecretKey>,
    relays: bool,
    hosts: Vec<(Did, Arc<dyn SubjectHost>)>,
}

impl IrohNodeBuilder {
    fn new() -> Self {
        Self {
            endpoint: None,
            secret_key: None,
            relays: true,
            hosts: Vec::new(),
        }
    }

    /// Use a pre-built endpoint instead of binding one. Note that the
    /// node's router takes over accepting connections on it.
    pub fn endpoint(mut self, endpoint: Endpoint) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Bind the endpoint with this secret key (stable endpoint identity
    /// across restarts). Ignored when a pre-built endpoint is supplied.
    pub fn secret_key(mut self, secret_key: SecretKey) -> Self {
        self.secret_key = Some(secret_key);
        self
    }

    /// Disable relays and public address lookup: the node is reachable via
    /// direct addresses only. Intended for tests and closed networks.
    pub fn direct_only(mut self) -> Self {
        self.relays = false;
        self
    }

    /// Serve the space `subject` from the storage environment `env`.
    ///
    /// The environment is any local provider of the effect vocabulary —
    /// the same one a `Repository` over the replica uses. May be called
    /// once per subject.
    pub fn host<Env>(mut self, subject: Did, env: Env) -> Self
    where
        Env: super::host::SpaceStorage,
    {
        let host = super::host::SpaceHost::new(subject.clone(), env);
        self.hosts.push((subject, Arc::new(host)));
        self
    }

    /// Bind the endpoint (unless supplied), spawn gossip and the protocol
    /// router, and return the node.
    pub async fn spawn(self) -> Result<Arc<IrohNode>, IrohRemoteError> {
        let lookup = MemoryLookup::new();
        let endpoint = match self.endpoint {
            Some(endpoint) => endpoint,
            None => {
                use iroh::endpoint::presets;
                let mut builder = if self.relays {
                    Endpoint::builder(presets::N0)
                } else {
                    // Minimal: crypto provider only — no relays, no external
                    // address lookup; the node is dialable by direct address.
                    Endpoint::builder(presets::Minimal)
                };
                if let Some(secret_key) = self.secret_key {
                    builder = builder.secret_key(secret_key);
                }
                builder
                    .address_lookup(lookup.clone())
                    .bind()
                    .await
                    .map_err(|e| IrohRemoteError::Endpoint(format!("binding endpoint: {e}")))?
            }
        };

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let hosts: Arc<HashMap<Did, Arc<dyn SubjectHost>>> =
            Arc::new(self.hosts.into_iter().collect());

        let router = Router::builder(endpoint.clone())
            .accept(ALPN, HostRegistry::new(hosts.clone()))
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();

        Ok(Arc::new(IrohNode {
            endpoint,
            gossip,
            lookup,
            _router: router,
            hosts,
            connections: Mutex::new(HashMap::new()),
            swarms: Mutex::new(HashMap::new()),
        }))
    }
}

/// A running iroh node: the endpoint, its protocol router (serving the
/// remote protocol for hosted spaces and the gossip overlay), a connection
/// pool, and the joined swarms.
pub struct IrohNode {
    endpoint: Endpoint,
    gossip: Gossip,
    lookup: MemoryLookup,
    _router: Router,
    hosts: Arc<HashMap<Did, Arc<dyn SubjectHost>>>,
    connections: Mutex<HashMap<EndpointId, Connection>>,
    swarms: Mutex<HashMap<Did, Arc<SwarmHandle>>>,
}

impl IrohNode {
    /// Start building a node.
    pub fn builder() -> IrohNodeBuilder {
        IrohNodeBuilder::new()
    }

    /// The underlying endpoint.
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// This node's own address (endpoint id plus current dialing hints),
    /// as shareable [`IrohAddress`] data.
    pub fn address(&self) -> IrohAddress {
        IrohAddress::from(self.endpoint.addr())
    }

    /// Remember a peer's dialing hints so later dials by bare endpoint id
    /// (e.g. from gossip membership) can resolve without external lookup.
    pub fn remember(&self, address: &IrohAddress) {
        if let Ok(addr) = address.endpoint_addr()
            && !addr.addrs.is_empty()
        {
            self.lookup.add_endpoint_info(addr);
        }
    }

    /// Connect to a peer over the dialog remote protocol, reusing a pooled
    /// connection when one is alive.
    pub async fn connect(&self, address: &IrohAddress) -> Result<Connection, IrohRemoteError> {
        let addr = address.endpoint_addr()?;
        let id = addr.id;
        self.remember(address);

        let mut connections = self.connections.lock().await;
        if let Some(connection) = connections.get(&id)
            && connection.close_reason().is_none()
        {
            return Ok(connection.clone());
        }

        let connection =
            self.endpoint.connect(addr, ALPN).await.map_err(|e| {
                IrohRemoteError::Connection(format!("connecting to {address}: {e}"))
            })?;
        connections.insert(id, connection.clone());
        Ok(connection)
    }

    /// Join the gossip swarm for `subject`, bootstrapping from the given
    /// peers (typically the space's configured iroh remotes).
    ///
    /// If this node hosts `subject`, the host answers `Want` messages from
    /// the swarm and announces published revisions on it. The returned
    /// handle is also registered so the [`Iroh`](crate::Iroh) site's `get`
    /// provider can fall back to the swarm when the addressed remote
    /// misses.
    pub async fn join_swarm(
        self: &Arc<Self>,
        subject: &Did,
        bootstrap: Vec<IrohAddress>,
    ) -> Result<Arc<SwarmHandle>, IrohRemoteError> {
        let mut swarms = self.swarms.lock().await;
        if let Some(swarm) = swarms.get(subject) {
            return Ok(swarm.clone());
        }

        let host = self.hosts.get(subject).cloned();
        let swarm =
            SwarmHandle::join(self.clone(), &self.gossip, subject, bootstrap, host.clone()).await?;
        if let Some(host) = host {
            host.attach_swarm(swarm.clone());
        }
        swarms.insert(subject.clone(), swarm.clone());
        Ok(swarm.clone())
    }

    /// The joined swarm for `subject`, if any.
    pub async fn swarm(&self, subject: &Did) -> Option<Arc<SwarmHandle>> {
        self.swarms.lock().await.get(subject).cloned()
    }

    /// Forward local branch-head publishes to their swarms as announces,
    /// so peers are woken by this device's *local commits* without a
    /// push — the counterpart of the host announcing publishes that
    /// arrive over the wire.
    ///
    /// `publishes` is a [`Storage::publishes`](dialog_storage::provider::storage::Storage::publishes)
    /// subscription on the environment the device's repository runs on
    /// (the same one hosted via [`IrohNodeBuilder::host`]). Only branch
    /// head cells (`branch/{name}` spaces) are forwarded: internal cells
    /// — remote snapshot caches in particular — must not echo across the
    /// swarm, where every fetch they record would trigger another round
    /// of pulls. Announces are deduplicated per version, so a publish
    /// also observed by the host is broadcast once.
    ///
    /// The task ends when the storage environment (or this node) is
    /// dropped.
    pub fn announce_publishes(
        self: &Arc<Self>,
        mut publishes: tokio::sync::broadcast::Receiver<PublishEvent>,
    ) {
        let node = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                let event = match publishes.recv().await {
                    Ok(event) => event,
                    // Signals, not a log: skip whatever we missed.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
                if !event.space.starts_with("branch/") {
                    continue;
                }
                let Some(node) = node.upgrade() else { break };
                if let Some(swarm) = node.swarm(&event.subject).await {
                    swarm.announce(event.space, event.cell, event.version).await;
                }
            }
        });
    }

    /// Whether this node hosts (serves) the given subject.
    pub fn hosts(&self, subject: &Did) -> bool {
        self.hosts.contains_key(subject)
    }

    /// Gracefully shut down the router and close the endpoint.
    pub async fn shutdown(&self) {
        let _ = self._router.shutdown().await;
        self.endpoint.close().await;
    }
}

impl std::fmt::Debug for IrohNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IrohNode")
            .field("endpoint", &self.endpoint.id())
            .finish_non_exhaustive()
    }
}
