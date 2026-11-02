//! Stacks: layers linked under scope names, read as one composite and
//! written by placement.
//!
//! A **layer** is a [`Branch`], a [`Snapshot`], or an [`Ephemeral`]
//! store. A **stack** is a list of layers, bottom first, where an upper
//! layer may **link** a layer beneath it under a scope name. Reads see
//! every layer as one composite; a transaction routes each fact to the
//! layer its attribute's scope is linked under, commits the layers
//! bottom to top, and every enclosing layer that commits refreshes its
//! links with the heads it saw.
//!
//! ```no_run
//! # use dialog_repository::{Branch, Ephemeral, Stack};
//! # fn example(shared: Branch, local: Branch, state: Ephemeral) -> anyhow::Result<()> {
//! // `state` was created through the environment: `Ephemeral::create()`.
//! let open = Stack::open(state.clone())
//!     .link(&state, &local, "memory:device".parse()?)
//!     .link(&local, &shared, "memory:space".parse()?); // the bottom: placements live here
//! // `.perform(&env).await?` walks the links `state` already records,
//! // then commits the new ones, checking the shape as it goes.
//! # let _ = open;
//! # Ok(())
//! # }
//! ```
//!
//! A stack is opened from a layer, never built: `Stack::open(layer)`
//! walks the `dialog.link/*` facts the layer holds, resolves each target
//! through the environment (a branch by repository and name, a snapshot
//! by its recorded revision, an ephemeral layer by its address), and
//! goes on downward. Wiring changes are transaction edits
//! ([`StackTransaction::link`], [`StackTransaction::unlink`]) that land
//! as link facts on the enclosing layer with the rest of the commit.
//!
//! # Links, as facts
//!
//! A link is held by the enclosing layer, in its own store, as facts on
//! a content-addressed link entity:
//!
//! ```text
//! <link> dialog.link/from      <address entity of the encloser>
//! <link> dialog.link/to        <stack identity of the enclosed layer>
//! <link> dialog.link/name      <scope name>
//! <link> dialog.link/revision  <head of the enclosed layer, as last seen>
//! ```
//!
//! plus `dialog.link/order` (the link's position among the encloser's
//! links, which fixes the order layers are read in) and the enclosed
//! layer's address (`dialog.link/repository` and `dialog.link/branch`
//! for a branch, `dialog.link/snapshot` with the encoded revision for
//! a snapshot, `dialog.link/ephemeral` for an ephemeral layer). Each
//! layer holds only its own links; an opener walks them downward.
//!
//! A stack commit refreshes the wiring on every layer above a layer it
//! moved, bottom to top, so after the commit the top layer's head
//! transitively names the head of every layer beneath it: one hash for
//! the whole composite. A link whose target did not move is a no-op
//! refresh and mints nothing. Linking is capturing: a layer that should
//! not record another's head does not link it, and sits beside it
//! under a common encloser instead.
//!
//! # Captured heads
//!
//! A stack holds a captured head per layer, the way a branch handle
//! holds its head, and everything it does builds on them. Reads see
//! every layer beneath the top at its captured revision, so what a
//! read sees is exactly what the top's hash names. A commit induces
//! against the captured heads and commits each layer on top of its
//! captured revision; a layer that moved outside the stack refuses the
//! publish ([`StackError::Publish`]) rather than building on a head the
//! stack never saw. Movement enters a stack only on
//! [`pull`](Stack::pull), which reconciles each layer with its upstream
//! bottom to top and then captures the live heads, and leaves it on
//! [`push`](Stack::push), which fails when an upstream moved and needs
//! a pull first. Reads and subscription polls never write.
//!
//! # Identity
//!
//! Every layer has an **address** (where its head lives) and a **stack
//! identity**: the hash of its descriptor, `{address, links: sorted
//! [(name, id(to))]}` in canonical dag-cbor. The bottom layer's identity
//! is the same on every replica; an encloser's identity covers the
//! shape beneath it, so a cycle is unconstructible and a re-shaped layer
//! re-identifies everything above it.
//!
//! # The audience rule
//!
//! A layer may link a layer beneath it only if that layer's
//! [`Audience`] contains its own: a link is a revision hash, and a
//! reader of the encloser must be able to resolve it. A replicated
//! branch may not link a process-local store; the store may link the
//! branch.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::{mem, slice};

use base58::ToBase58 as _;
use dialog_artifacts::history::Version as HistoryVersion;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, ArtifactView, AssetChange, Changes, DialogArtifactsError, Entity,
    Instruction, Pick, Statement, Update as _, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::archive::{Get, Put};
use dialog_effects::authority::Identify;
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead, Size as BlobSize};
use dialog_effects::memory::{Publish, Resolve, Version as MemoryVersion};
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use futures_util::lock::Mutex as AsyncMutex;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use thiserror::Error;

mod guide;

pub use guide::Divergence;
pub(crate) use guide::{Divergences, Guide, GuidedLayer};

use crate::placement::{Bindings, Placements, Target, elected};
use crate::repository::branch::QueryLayer;
use crate::repository::branch::registry::RegistryEnv;
use crate::repository::branch::resolve::ResolveEnv;
use crate::repository::branch::session::{Composite, QueryEnv};
use crate::repository::branch::transaction::induce::{Landing, Watermark, Witness, induce};
use crate::repository::branch::transaction::{refuse_nan, settle_successions};
use crate::repository::source::{Source, SourceRef};
use crate::schema::DidExt as _;
use crate::{
    Branch, CommitError, Delta, Ephemeral, EphemeralError, EphemeralRevision, Hold, PullError,
    PushError, RemoteSite, ResolveError, Revision, Snapshot, Staged, Subscription,
    TransactionBatch,
};
use dialog_common::Holds;

/// The environment a stack's reads and writes run against: every
/// effect a branch commit, publish or registry read needs, plus the
/// handles the environment holds (the ephemeral registry among them).
pub trait StackEnv: RegistryEnv + Provider<BlobSize> {}

impl<T> StackEnv for T where T: RegistryEnv + Provider<BlobSize> {}

/// The environment a stack's [`pull`](Stack::pull) and
/// [`push`](Stack::push) run against: a branch pull's, plus the forks
/// a branch push makes to a peer.
pub trait StackSyncEnv:
    StackEnv
    + ResolveEnv
    + Provider<Fork<RemoteSite, Get>>
    + Provider<Fork<RemoteSite, Put>>
    + Provider<Fork<RemoteSite, Publish>>
    + Provider<Fork<RemoteSite, BlobImport>>
    + Provider<Fork<RemoteSite, BlobRead>>
{
}

impl<T> StackSyncEnv for T where
    T: StackEnv
        + ResolveEnv
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Put>>
        + Provider<Fork<RemoteSite, Publish>>
        + Provider<Fork<RemoteSite, BlobImport>>
        + Provider<Fork<RemoteSite, BlobRead>>
{
}

/// Who can read a layer: the set of principals its facts reach.
/// Ordered by inclusion, narrowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Audience {
    /// This process only.
    Process,
    /// This device: durable here, never pushed.
    Device,
    /// The layer's peers: pushed to its upstreams.
    Peers,
}

/// A layer in a stack.
#[derive(Debug, Clone)]
pub enum Layer {
    /// A branch: durable, head in a cell.
    Branch(Branch),
    /// A snapshot: durable, head by value, read-only in a stack.
    Snapshot(Snapshot),
    /// An ephemeral store.
    Ephemeral(Ephemeral),
}

impl From<Branch> for Layer {
    fn from(branch: Branch) -> Self {
        Layer::Branch(branch)
    }
}

impl From<Snapshot> for Layer {
    fn from(snapshot: Snapshot) -> Self {
        Layer::Snapshot(snapshot)
    }
}

impl From<Ephemeral> for Layer {
    fn from(ephemeral: Ephemeral) -> Self {
        Layer::Ephemeral(ephemeral)
    }
}

/// Something a link can target: a layer, or a handle to one.
pub trait AsLayer {
    /// This as a layer.
    fn as_layer(&self) -> Layer;
}

impl AsLayer for Layer {
    fn as_layer(&self) -> Layer {
        self.clone()
    }
}

impl AsLayer for Branch {
    fn as_layer(&self) -> Layer {
        Layer::Branch(self.clone())
    }
}

impl AsLayer for Snapshot {
    fn as_layer(&self) -> Layer {
        Layer::Snapshot(self.clone())
    }
}

impl AsLayer for Ephemeral {
    fn as_layer(&self) -> Layer {
        Layer::Ephemeral(self.clone())
    }
}

impl Layer {
    /// The layer's ephemeral store: a tree layer's session store, or
    /// the ephemeral layer itself. Where its instants are witnessed.
    pub(crate) fn store(&self) -> &Ephemeral {
        match self {
            Layer::Branch(branch) => branch.overlay(),
            Layer::Snapshot(snapshot) => snapshot.overlay(),
            Layer::Ephemeral(ephemeral) => ephemeral,
        }
    }
}

/// Where a layer's head lives, as the descriptor records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Address {
    Branch { repository: String, branch: String },
    Snapshot { repository: String, tree: String },
    Ephemeral { id: String },
}

/// The shape of a layer: its address and what it links, in canonical
/// order. Its hash is the layer's stack identity.
#[derive(Debug, Clone, Serialize)]
struct Descriptor {
    address: Address,
    links: Vec<(String, String)>,
    /// The read-only links, apart from the others, so that a layer with
    /// none keeps the identity it had before read-only links existed.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    read_only: Vec<(String, String)>,
}

/// A head as a link records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Head {
    /// A tree layer's head, or `None` for a branch with no commits.
    Tree(Option<Revision>),
    /// An ephemeral layer's head.
    Ephemeral(EphemeralRevision),
}

impl Head {
    /// The bytes a link's `revision` fact carries: the tree hash, or
    /// the ephemeral chained hash.
    fn bytes(&self) -> Vec<u8> {
        match self {
            Head::Tree(Some(revision)) => revision.tree.hash().to_vec(),
            // A branch captured before its first commit has no tree:
            // no bytes, which no tree hash can collide with.
            Head::Tree(None) => Vec::new(),
            Head::Ephemeral(revision) => revision.hash.as_bytes().to_vec(),
        }
    }
}

impl Layer {
    /// Whether two handles name the same layer.
    fn same(&self, other: &Layer) -> bool {
        match (self, other) {
            (Layer::Branch(a), Layer::Branch(b)) => a.of() == b.of() && a.name() == b.name(),
            (Layer::Snapshot(a), Layer::Snapshot(b)) => {
                a.of() == b.of() && a.revision() == b.revision()
            }
            (Layer::Ephemeral(a), Layer::Ephemeral(b)) => a.is(b),
            _ => false,
        }
    }

    /// Who can read this layer.
    pub fn audience(&self) -> Audience {
        match self {
            Layer::Branch(branch) if !branch.upstreams().is_empty() => Audience::Peers,
            Layer::Branch(_) | Layer::Snapshot(_) => Audience::Device,
            Layer::Ephemeral(_) => Audience::Process,
        }
    }

    fn address(&self) -> Address {
        match self {
            Layer::Branch(branch) => Address::Branch {
                repository: branch.of().to_string(),
                branch: branch.name().to_string(),
            },
            Layer::Snapshot(snapshot) => Address::Snapshot {
                repository: snapshot.of().to_string(),
                tree: snapshot.revision().tree.hash().to_base58(),
            },
            Layer::Ephemeral(ephemeral) => Address::Ephemeral {
                id: ephemeral.entity().to_string(),
            },
        }
    }

    /// The entity standing for this layer's address in link facts.
    pub fn address_entity(&self) -> Entity {
        match self {
            Layer::Branch(branch) => format!("layer:{}/{}", branch.of(), branch.name())
                .parse()
                .expect("a DID and a branch name form an opaque URI path"),
            Layer::Snapshot(snapshot) => format!(
                "layer:{}/{}",
                snapshot.of(),
                snapshot.revision().tree.hash().to_base58()
            )
            .parse()
            .expect("a DID and a hash form an opaque URI path"),
            Layer::Ephemeral(ephemeral) => ephemeral.entity().clone(),
        }
    }

    /// This layer's head now.
    pub fn head(&self) -> Head {
        match self {
            Layer::Branch(branch) => Head::Tree(branch.revision()),
            Layer::Snapshot(snapshot) => Head::Tree(Some(snapshot.revision())),
            Layer::Ephemeral(ephemeral) => Head::Ephemeral(ephemeral.revision()),
        }
    }

    /// The address facts a link to this layer carries, so an opener can
    /// resolve the target from the link alone.
    fn address_facts(&self, link: &Entity, changes: &mut Changes) {
        match self {
            Layer::Branch(branch) => {
                changes.associate(
                    link_attr("repository"),
                    link.clone(),
                    Value::Entity(branch.of().this()),
                    Pick::Last,
                );
                changes.associate(
                    link_attr("branch"),
                    link.clone(),
                    Value::String(branch.name().to_string()),
                    Pick::Last,
                );
            }
            Layer::Snapshot(snapshot) => {
                changes.associate(
                    link_attr("repository"),
                    link.clone(),
                    Value::Entity(snapshot.of().this()),
                    Pick::Last,
                );
                changes.associate(
                    link_attr("tree"),
                    link.clone(),
                    Value::Bytes(snapshot.revision().tree.hash().to_vec()),
                    Pick::Last,
                );
                changes.associate(
                    link_attr("snapshot"),
                    link.clone(),
                    Value::Bytes(
                        serde_ipld_dagcbor::to_vec(&snapshot.revision())
                            .expect("a revision encodes"),
                    ),
                    Pick::Last,
                );
            }
            Layer::Ephemeral(ephemeral) => {
                changes.associate(
                    link_attr("ephemeral"),
                    link.clone(),
                    Value::Entity(ephemeral.entity().clone()),
                    Pick::Last,
                );
            }
        }
    }
}

/// A `dialog.link/<name>` attribute.
fn link_attr(name: &str) -> dialog_artifacts::Relation {
    format!("dialog.link/{name}")
        .parse()
        .expect("a fixed link attribute name is valid")
}

/// A link from an enclosing layer to a layer beneath it.
#[derive(Debug, Clone)]
struct Link {
    /// Index of the enclosed layer in the stack.
    to: usize,
    /// The scope name the link binds.
    name: Entity,
    /// The link entity: `link:<blake3(dagcbor{from, to, name})>` over the
    /// encloser's address entity, the enclosed layer's identity, and the
    /// scope name, so that one layer can link another under two names.
    entity: Entity,
    /// Whether the stack only reads the enclosed layer through this
    /// link: it records the layer's head and advances it, but never
    /// writes, stages, or publishes it.
    read_only: bool,
}

/// Why a stack could not be built.
#[derive(Debug, Error)]
pub enum StackError {
    /// A link names a layer the stack does not hold beneath the
    /// linking layer.
    #[error("Layer {from} links a layer that is not beneath it in the stack")]
    UnknownLayer {
        /// The address entity of the linking layer.
        from: Entity,
    },
    /// A link would let a wider audience reference a head it cannot
    /// resolve.
    #[error("Layer {from} ({from_audience:?}) may not link {to} ({to_audience:?})")]
    Audience {
        /// The address entity of the linking layer.
        from: Box<Entity>,
        /// Its audience.
        from_audience: Audience,
        /// The address entity of the linked layer.
        to: Box<Entity>,
        /// Its audience.
        to_audience: Audience,
    },
    /// A layer links a layer under two names, read-only under one and
    /// writable under the other: the stack writes a layer or only reads
    /// it, whatever scope a write names.
    #[error("Layer {from} links {to} under another name with another access")]
    MixedAccess {
        /// The address entity of the linking layer.
        from: Box<Entity>,
        /// The address entity of the linked layer.
        to: Box<Entity>,
    },
    /// A snapshot cannot hold links: nothing reached through a
    /// snapshot handle can write.
    #[error("Snapshot {from} cannot link other layers")]
    SnapshotEncloser {
        /// The address entity of the snapshot.
        from: Entity,
    },
    /// A descriptor could not be encoded.
    #[error("Failed to encode a stack descriptor: {0}")]
    Encode(String),
    /// A link's facts do not name a target and a scope.
    #[error("Link {link} is malformed")]
    Link {
        /// The link entity.
        link: Entity,
    },
    /// A link's address could not be resolved to a layer.
    #[error("Cannot resolve a layer at {address}")]
    Address {
        /// The address as the link records it.
        address: String,
    },
    /// A link records an identity that is not the identity of the
    /// shape found beneath it: the wiring changed under the encloser.
    #[error("Layer {layer} links a layer whose shape differs from what it recorded")]
    Identity {
        /// The address entity of the encloser.
        layer: Entity,
    },
    /// Reading a layer's link facts failed.
    #[error("Failed to read link facts: {0}")]
    Read(String),
    /// A commit edits the links of a stack that holds staged commits: a
    /// chain built on the old shape could not stage on the new one, and
    /// would be lost. Publish them first.
    #[error("The stack holds staged commits; publish them before its links change")]
    Staged,
    /// A write is placed on a scope that the stack reads through a
    /// read-only link. The commit fails before any layer changes.
    #[error("Scope {scope} is read-only in this stack")]
    ReadOnly {
        /// The scope the write is placed on.
        scope: Entity,
    },
    /// A write or maintenance names a scope no link of this stack binds.
    #[error("No layer is linked under scope {scope}")]
    UnboundScope {
        /// The scope named.
        scope: Entity,
    },
    /// Maintenance names a scope a tree layer is linked under; only
    /// ephemeral layers are cleared or forgotten.
    #[error("Scope {scope} binds a tree layer, which cannot be cleared")]
    NotEphemeral {
        /// The scope named.
        scope: Entity,
    },
    /// An ephemeral layer a link names is not open in this process.
    #[error(transparent)]
    Ephemeral(#[from] EphemeralError),
    /// Writing a layer's link facts failed.
    #[error("Failed to write link facts: {0}")]
    Commit(#[from] CommitError),
    /// Re-resolving a layer's head from storage failed.
    #[error("Failed to resolve a layer's head: {0}")]
    Resolve(#[from] ResolveError),
    /// Pulling a layer from its upstream failed.
    #[error("Failed to pull a layer: {0}")]
    Pull(#[from] PullError),
    /// Pushing a layer to its upstream failed.
    #[error("Failed to push a layer: {0}")]
    Push(#[from] PushError),
    /// Publishing a layer's staged chain failed, most often because its
    /// head moved outside the stack. The chain and every chain above
    /// it are dropped; pull, then re-run the transactions.
    #[error("Failed to publish layer {layer}: {source}")]
    Publish {
        /// The address entity of the layer whose publish failed.
        layer: Box<Entity>,
        /// Why.
        #[source]
        source: CommitError,
    },
}

/// What a link entity hashes: the encloser's address entity, the
/// enclosed layer's stack identity, and the scope name, in canonical
/// dag-cbor like the descriptor.
#[derive(Debug, Clone, Serialize)]
struct LinkKey<'a> {
    from: &'a str,
    to: &'a str,
    name: &'a str,
}

/// `link:<base58(blake3(dagcbor{from, to, name}))>`.
fn link_entity(from: &Entity, to: &Entity, name: &Entity) -> Entity {
    let key = LinkKey {
        from: from.as_str(),
        to: to.as_str(),
        name: name.as_str(),
    };
    let bytes = serde_ipld_dagcbor::to_vec(&key).expect("three strings encode");
    format!("link:{}", Blake3Hash::hash(&bytes).as_bytes().to_base58())
        .parse()
        .expect("a base58 hash is an opaque URI path")
}

/// `stack:<base58(blake3(descriptor))>`.
fn identity(descriptor: &[u8]) -> Entity {
    format!(
        "stack:{}",
        Blake3Hash::hash(descriptor).as_bytes().to_base58()
    )
    .parse()
    .expect("a base58 hash is an opaque URI path")
}

/// The shape of a stack: its layers bottom first, their stack
/// identities, their links, and the scope names the links bind. Held
/// in the stack's shared state, so a link committed through one handle
/// is the shape every clone reads from then on.
#[derive(Debug, Clone, Default)]
struct Topology {
    /// Bottom first.
    layers: Vec<Layer>,
    /// Each layer's stack identity, parallel to `layers`.
    ids: Vec<Entity>,
    /// Each layer's links, parallel to `layers`, in declaration order.
    links: Vec<Vec<Link>>,
    /// Scope name → the layers linked under it.
    bound: HashMap<Entity, Vec<usize>>,
}

impl Topology {
    /// Whether a write of `the` lands only in tree layers, as the commit
    /// routes it under `placements`: a write placed on a scope that links
    /// bind lands on the layers linked under it, and any other on the
    /// bottom.
    fn lands_in_trees(&self, placements: &Placements, the: &dialog_artifacts::Relation) -> bool {
        match placements.placement_of(the) {
            None => true,
            Some(scope) => self.bound.get(scope).is_none_or(|indices| {
                indices
                    .iter()
                    .all(|index| !matches!(self.layers[*index], Layer::Ephemeral(_)))
            }),
        }
    }

    /// The scopes whose writes land in an ephemeral layer: every name a
    /// link binds to one, as [`lands_in_trees`](Self::lands_in_trees)
    /// reads them.
    fn store_scopes(&self) -> HashSet<Entity> {
        self.bound
            .iter()
            .filter(|(_, indices)| {
                indices
                    .iter()
                    .any(|index| matches!(self.layers[*index], Layer::Ephemeral(_)))
            })
            .map(|(scope, _)| scope.clone())
            .collect()
    }

    /// The bottom layer as a branch, or `None` when it is not one.
    fn primary(&self) -> Option<Branch> {
        match self.layers.first() {
            Some(Layer::Branch(branch)) => Some(branch.clone()),
            _ => None,
        }
    }

    /// Where `layer` sits, if it is in the stack.
    fn index_of(&self, layer: &Layer) -> Option<usize> {
        self.layers.iter().position(|held| held.same(layer))
    }

    /// Put `layer` at position `at`, beneath whatever was there, and
    /// shift every link past it.
    fn insert(&mut self, at: usize, layer: Layer) {
        self.layers.insert(at, layer);
        self.links.insert(at, Vec::new());
        for own in &mut self.links {
            for link in own {
                if link.to >= at {
                    link.to += 1;
                }
            }
        }
    }

    /// Link layer `from` to layer `to` beneath it under `name`, read-only
    /// or not. A link that already exists keeps its place, and takes the
    /// access given. One layer can link another under two names, each a
    /// link of its own, but with one access for both.
    fn link(
        &mut self,
        from: usize,
        to: usize,
        name: Entity,
        read_only: bool,
    ) -> Result<(), StackError> {
        let encloser = &self.layers[from];
        let address = encloser.address_entity();
        if matches!(encloser, Layer::Snapshot(_)) {
            return Err(StackError::SnapshotEncloser { from: address });
        }
        if to >= from {
            return Err(StackError::UnknownLayer { from: address });
        }
        let target = &self.layers[to];
        let (from_audience, to_audience) = (encloser.audience(), target.audience());
        if to_audience < from_audience {
            return Err(StackError::Audience {
                from: Box::new(address),
                from_audience,
                to: Box::new(target.address_entity()),
                to_audience,
            });
        }
        if self.links[from]
            .iter()
            .any(|link| link.to == to && link.name != name && link.read_only != read_only)
        {
            return Err(StackError::MixedAccess {
                from: Box::new(address),
                to: Box::new(target.address_entity()),
            });
        }
        if let Some(link) = self.links[from]
            .iter_mut()
            .find(|link| link.to == to && link.name == name)
        {
            if link.read_only == read_only {
                return Ok(());
            }
            link.read_only = read_only;
            return self.identify();
        }
        self.links[from].push(Link {
            to,
            name,
            // Settled by `identify`, once the target's identity is known.
            entity: address,
            read_only,
        });
        self.identify()
    }

    /// Drop the link from layer `from` to layer `to` under `name`,
    /// returning its entity so its facts can be retracted.
    fn unlink(&mut self, from: usize, to: usize, name: &Entity) -> Option<Entity> {
        let position = self.links[from]
            .iter()
            .position(|link| link.to == to && link.name == *name)?;
        let link = self.links[from].remove(position);
        Some(link.entity)
    }

    /// Drop every layer the top does not reach through its links.
    fn prune(&mut self) -> Result<(), StackError> {
        let top = self.layers.len().saturating_sub(1);
        let mut reachable = vec![false; self.layers.len()];
        let mut frontier = vec![top];
        while let Some(index) = frontier.pop() {
            if mem::replace(&mut reachable[index], true) {
                continue;
            }
            frontier.extend(self.links[index].iter().map(|link| link.to));
        }
        if reachable.iter().all(|kept| *kept) {
            return self.identify();
        }
        let mut remap: Vec<Option<usize>> = Vec::with_capacity(self.layers.len());
        let mut kept = 0;
        for keep in &reachable {
            remap.push(keep.then(|| {
                kept += 1;
                kept - 1
            }));
        }
        let layers = mem::take(&mut self.layers);
        let links = mem::take(&mut self.links);
        for (index, (layer, own)) in layers.into_iter().zip(links).enumerate() {
            if !reachable[index] {
                continue;
            }
            self.layers.push(layer);
            self.links.push(
                own.into_iter()
                    .map(|mut link| {
                        link.to = remap[link.to].expect("a reachable layer links reachable layers");
                        link
                    })
                    .collect(),
            );
        }
        self.identify()
    }

    /// Recompute every layer's identity bottom up, every link's entity,
    /// and the names the links bind. Identity is a pure function of
    /// shape: `stack:<hash of {address, sorted (name, id(to))}>`.
    fn identify(&mut self) -> Result<(), StackError> {
        self.ids.clear();
        for index in 0..self.layers.len() {
            let from = self.layers[index].address_entity();
            let mut descriptor_links: Vec<(String, String)> = Vec::new();
            let mut read_only: Vec<(String, String)> = Vec::new();
            for link in &mut self.links[index] {
                let entry = (link.name.to_string(), self.ids[link.to].to_string());
                if link.read_only {
                    read_only.push(entry);
                } else {
                    descriptor_links.push(entry);
                }
                link.entity = link_entity(&from, &self.ids[link.to], &link.name);
            }
            descriptor_links.sort();
            read_only.sort();
            let descriptor = Descriptor {
                address: self.layers[index].address(),
                links: descriptor_links,
                read_only,
            };
            let bytes = serde_ipld_dagcbor::to_vec(&descriptor)
                .map_err(|error| StackError::Encode(error.to_string()))?;
            self.ids.push(identity(&bytes));
        }
        // A name binds the layers *linked under it*, not the linking
        // layers: `local.link(&shared, "memory:shared")` makes
        // `memory:shared` route to shared.
        self.bound.clear();
        for own in &self.links {
            for link in own {
                let targets = self.bound.entry(link.name.clone()).or_default();
                if !targets.contains(&link.to) {
                    targets.push(link.to);
                }
            }
        }
        Ok(())
    }

    /// The link facts layer `index` holds at `heads`, folded into
    /// `changes`: one entity per layer it links, naming the target's
    /// identity, the scope name, the link's position, the target's
    /// head, and its address.
    fn link_facts(&self, index: usize, heads: &[Head], changes: &mut Changes) {
        let from = self.layers[index].address_entity();
        for (order, link) in self.links[index].iter().enumerate() {
            let target = &self.layers[link.to];
            changes.associate(
                link_attr("from"),
                link.entity.clone(),
                Value::Entity(from.clone()),
                Pick::Last,
            );
            changes.associate(
                link_attr("to"),
                link.entity.clone(),
                Value::Entity(self.ids[link.to].clone()),
                Pick::Last,
            );
            changes.associate(
                link_attr("name"),
                link.entity.clone(),
                Value::Entity(link.name.clone()),
                Pick::Last,
            );
            changes.associate(
                link_attr("order"),
                link.entity.clone(),
                Value::UnsignedInt(order as u128),
                Pick::Last,
            );
            changes.associate(
                link_attr("revision"),
                link.entity.clone(),
                Value::Bytes(heads[link.to].bytes()),
                Pick::Last,
            );
            changes.associate(
                link_attr("access"),
                link.entity.clone(),
                Value::String(if link.read_only { "read" } else { "write" }.into()),
                Pick::Last,
            );
            target.address_facts(&link.entity, changes);
        }
    }

    /// Whether any layer reachable through `index`'s links moved.
    fn reaches_moved(&self, index: usize, moved: &[bool]) -> bool {
        self.links[index]
            .iter()
            .any(|link| moved[link.to] || self.reaches_moved(link.to, moved))
    }

    /// Whether the stack writes layer `index`: the top, and each layer
    /// that a path of links other than read-only reaches from the top. A
    /// layer that the stack reaches only through a read-only link is read:
    /// never written, staged, published, pulled, or pushed; nor is a layer
    /// beneath it, whose head it would have to record.
    fn writable(&self, index: usize) -> bool {
        let Some(top) = self.layers.len().checked_sub(1) else {
            return false;
        };
        let mut reached = vec![false; self.layers.len()];
        let mut frontier = vec![top];
        while let Some(at) = frontier.pop() {
            if mem::replace(&mut reached[at], true) {
                continue;
            }
            frontier.extend(
                self.links[at]
                    .iter()
                    .filter(|link| !link.read_only)
                    .map(|link| link.to),
            );
        }
        reached[index]
    }

    /// The scope a write that lands on the bottom is refused under, when
    /// the bottom is read-only: the first name that the bottom is linked
    /// under, else the default scope.
    fn bottom_scope(&self, default: Option<&Entity>) -> Option<Entity> {
        self.linked_under(0).or_else(|| default.cloned())
    }

    /// The first name that layer `index` is linked under.
    fn linked_under(&self, index: usize) -> Option<Entity> {
        self.links
            .iter()
            .flatten()
            .find(|link| link.to == index)
            .map(|link| link.name.clone())
    }

    /// The scope that names layer `index` in an error: the first name it
    /// is linked under, or its address entity when no link names it.
    fn scope_of_layer(&self, index: usize) -> Entity {
        self.linked_under(index)
            .unwrap_or_else(|| self.layers[index].address_entity())
    }
}

/// A change to a stack's wiring, queued on a transaction and applied
/// when it commits.
#[derive(Debug, Clone)]
enum LinkEdit {
    /// Link `from` to `to` under `name`. A `from` not yet in the stack
    /// waits for another edit of the batch to enter it, and is refused
    /// when none does; a `to` not yet in the stack is placed beneath
    /// `from`.
    Link {
        from: Layer,
        to: Layer,
        name: Entity,
        read_only: bool,
    },
    /// Drop the link from `from` to `to` under `name`; layers the top
    /// no longer reaches leave the stack.
    Unlink {
        from: Layer,
        to: Layer,
        name: Entity,
    },
}

/// A link as its facts record it, read back from an enclosing layer.
struct Recorded {
    name: Entity,
    read_only: bool,
    order: u128,
    to: Entity,
    address: Address,
    revision: Option<Vec<u8>>,
}

/// Command opening a stack from a layer: walk its recorded links
/// downward, resolving every target through the environment, then
/// apply any wiring queued on the way. Built by [`Stack::open`].
pub struct OpenStack {
    top: Layer,
    edits: Vec<LinkEdit>,
}

impl OpenStack {
    /// Link `from` to `to` under `name` once the stack is open, as
    /// [`StackTransaction::link`] would. A layer linked beneath another
    /// enters just beneath it, so of the layers that one layer links side
    /// by side, the first linked is lowest: link the bottom first, since
    /// the bottom holds the placements and takes every write that has
    /// none.
    pub fn link<F: AsLayer, T: AsLayer>(mut self, from: &F, to: &T, name: Entity) -> Self {
        self.edits.push(LinkEdit::Link {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
            read_only: false,
        });
        self
    }

    /// Link `from` to `to` under `name`, read-only, once the stack is
    /// open, as [`StackTransaction::link_read_only`] would.
    pub fn link_read_only<F: AsLayer, T: AsLayer>(
        mut self,
        from: &F,
        to: &T,
        name: Entity,
    ) -> Self {
        self.edits.push(LinkEdit::Link {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
            read_only: true,
        });
        self
    }

    /// Walk the links, resolve the layers, verify the identities the
    /// links record against the shape found, and apply the queued
    /// wiring in one commit published bottom to top.
    pub async fn perform<Env>(self, env: &Env) -> Result<Stack, StackError>
    where
        Env: StackSyncEnv,
    {
        let topology = walk(self.top, env).await?;
        let stack = Stack::from_topology(topology);
        if !self.edits.is_empty() {
            let mut transaction = stack.transaction();
            transaction.edits = self.edits;
            transaction.commit().publish().perform(env).await?;
        }
        Ok(stack)
    }
}

/// Read every link `layer` holds, in declaration order.
async fn recorded_links<Env>(layer: &Layer, env: &Env) -> Result<Vec<Recorded>, StackError>
where
    Env: StackEnv,
{
    let address = layer.address_entity();
    let from = ArtifactSelector::new()
        .the(link_attr("from"))
        .is(Value::Entity(address));
    let entities: Vec<Entity> = held_cells(layer, from, env)
        .await?
        .into_iter()
        .map(|fact| fact.of)
        .collect();
    let mut recorded = Vec::with_capacity(entities.len());
    for entity in entities {
        let facts = held_cells(layer, ArtifactSelector::new().of(entity.clone()), env).await?;
        let field = |name: &str| -> Option<&Value> {
            let attribute = link_attr(name);
            facts
                .iter()
                .find(|fact| fact.the == attribute)
                .map(|fact| &fact.is)
        };
        let malformed = || StackError::Link {
            link: entity.clone(),
        };
        let (Some(Value::Entity(name)), Some(Value::Entity(to))) = (field("name"), field("to"))
        else {
            return Err(malformed());
        };
        let order = match field("order") {
            Some(Value::UnsignedInt(order)) => *order,
            _ => u128::MAX,
        };
        let read_only = match field("access") {
            None => false,
            Some(Value::String(access)) if access == "write" => false,
            Some(Value::String(access)) if access == "read" => true,
            Some(_) => return Err(malformed()),
        };
        let address = match (
            field("repository"),
            field("branch"),
            field("snapshot"),
            field("ephemeral"),
        ) {
            (Some(Value::Entity(repository)), Some(Value::String(branch)), _, _) => {
                Address::Branch {
                    repository: repository.to_string(),
                    branch: branch.clone(),
                }
            }
            (Some(Value::Entity(repository)), _, Some(Value::Bytes(snapshot)), _) => {
                let revision: Revision =
                    serde_ipld_dagcbor::from_slice(snapshot).map_err(|_| malformed())?;
                Address::Snapshot {
                    repository: repository.to_string(),
                    tree: revision.tree.hash().to_base58(),
                }
            }
            (_, _, _, Some(Value::Entity(id))) => Address::Ephemeral { id: id.to_string() },
            _ => return Err(malformed()),
        };
        let revision = match field("snapshot") {
            Some(Value::Bytes(bytes)) => Some(bytes.clone()),
            _ => None,
        };
        recorded.push(Recorded {
            name: name.clone(),
            read_only,
            order,
            to: to.clone(),
            address,
            revision,
        });
    }
    recorded.sort_by_key(|link| link.order);
    Ok(recorded)
}

/// The facts a selector matches on a layer's own store: its tree for a
/// tree layer, the store itself for an ephemeral layer.
async fn held<Env>(
    layer: &Layer,
    selector: ArtifactSelector<Constrained>,
    env: &Env,
) -> Result<Vec<Artifact>, StackError>
where
    Env: StackEnv,
{
    use futures_util::{StreamExt as _, TryStreamExt as _};
    let source = match layer {
        Layer::Branch(branch) => SourceRef::from(branch),
        Layer::Snapshot(snapshot) => SourceRef::Snapshot(snapshot),
        Layer::Ephemeral(ephemeral) => return Ok(ephemeral.scan(&selector)),
    };
    let stream = crate::Select::from_source(source, selector)
        .perform(env)
        .await
        .map_err(|error| StackError::Read(error.to_string()))?;
    stream
        .map(|item| item.and_then(|view| view.to_owned()))
        .try_collect()
        .await
        .map_err(|error| StackError::Read(error.to_string()))
}

/// What `layer` holds of `selector`, one claim for each cell: the one
/// that `last` elects, which each field of a link is written under. A
/// merge can leave two claims of one field, from two replicas that each
/// recorded the link.
async fn held_cells<Env>(
    layer: &Layer,
    selector: ArtifactSelector<Constrained>,
    env: &Env,
) -> Result<Vec<Artifact>, StackError>
where
    Env: StackEnv,
{
    use futures_util::TryStreamExt as _;
    let source = match layer {
        Layer::Branch(branch) => SourceRef::from(branch),
        Layer::Snapshot(snapshot) => SourceRef::Snapshot(snapshot),
        Layer::Ephemeral(ephemeral) => return Ok(ephemeral.scan(&selector)),
    };
    let views: Vec<ArtifactView> = crate::Select::from_source(source, selector)
        .perform(env)
        .await
        .map_err(|error| StackError::Read(error.to_string()))?
        .try_collect()
        .await
        .map_err(|error| StackError::Read(error.to_string()))?;
    elected(views).map_err(|error| StackError::Read(error.to_string()))
}

/// Resolve the layer a link's address names, through the environment.
async fn resolve_layer<Env>(link: &Recorded, env: &Env) -> Result<Layer, StackError>
where
    Env: Provider<Resolve> + Holds + ConditionalSync,
{
    use crate::RepositoryMemoryExt as _;
    use dialog_capability::{Did, Subject};
    let subject = |repository: &str| -> Result<Subject, StackError> {
        repository
            .parse::<Did>()
            .map(Subject::from)
            .map_err(|_| StackError::Address {
                address: repository.to_string(),
            })
    };
    match &link.address {
        Address::Branch { repository, branch } => {
            let branch = subject(repository)?
                .branch(branch.clone())
                .open()
                .perform(env)
                .await?;
            Ok(Layer::Branch(branch))
        }
        Address::Snapshot { repository, .. } => {
            let bytes = link.revision.as_ref().ok_or_else(|| StackError::Address {
                address: repository.clone(),
            })?;
            let revision: Revision =
                serde_ipld_dagcbor::from_slice(bytes).map_err(|_| StackError::Address {
                    address: repository.clone(),
                })?;
            Ok(Layer::Snapshot(Snapshot::new(
                subject(repository)?,
                revision,
            )))
        }
        Address::Ephemeral { id } => {
            let address: Entity = id.parse().map_err(|_| StackError::Address {
                address: id.clone(),
            })?;
            Ok(Layer::Ephemeral(
                Ephemeral::open(address).perform(env).await?,
            ))
        }
    }
}

/// A link as the walk resolved it: the target's position among the
/// expanded layers, the scope name, the identity the link recorded, and
/// whether it is read-only.
type Edge = (usize, Entity, Entity, bool);

/// A layer the walk expanded, with its resolved links.
type Expanded = (Layer, Vec<Edge>);

/// Walk `top`'s links downward into a topology, bottom first, every
/// layer's links in their recorded order, and verify that each link's
/// recorded identity is the identity of the shape found beneath it.
async fn walk<Env>(top: Layer, env: &Env) -> Result<Topology, StackError>
where
    Env: StackEnv,
{
    // Expand every reachable layer once, keyed by address; each keeps
    // the addresses its links name, in recorded order.
    let mut expanded: Vec<Expanded> = Vec::new();
    let mut edges: Vec<Vec<(Entity, Entity, Entity, bool)>> = Vec::new();
    let mut index_of: HashMap<Entity, usize> = HashMap::new();
    let mut pending: Vec<Layer> = vec![top];
    while let Some(layer) = pending.pop() {
        let address = layer.address_entity();
        if index_of.contains_key(&address) {
            continue;
        }
        let mut own = Vec::new();
        for link in recorded_links(&layer, env).await? {
            let target = resolve_layer(&link, env).await?;
            own.push((target.address_entity(), link.name, link.to, link.read_only));
            pending.push(target);
        }
        index_of.insert(address, expanded.len());
        expanded.push((layer, Vec::new()));
        edges.push(own);
    }
    for (index, own) in edges.into_iter().enumerate() {
        expanded[index].1 = own
            .into_iter()
            .map(|(address, name, to, read_only)| (index_of[&address], name, to, read_only))
            .collect();
    }

    // Bottom first: post-order from the top, links in recorded order.
    let mut order: Vec<usize> = Vec::with_capacity(expanded.len());
    let mut visited = vec![false; expanded.len()];
    fn visit(index: usize, expanded: &[Expanded], visited: &mut [bool], order: &mut Vec<usize>) {
        if mem::replace(&mut visited[index], true) {
            return;
        }
        for (to, _, _, _) in &expanded[index].1 {
            visit(*to, expanded, visited, order);
        }
        order.push(index);
    }
    visit(0, &expanded, &mut visited, &mut order);
    let placed: HashMap<usize, usize> = order
        .iter()
        .enumerate()
        .map(|(position, index)| (*index, position))
        .collect();

    let mut topology = Topology {
        layers: order
            .iter()
            .map(|index| expanded[*index].0.clone())
            .collect(),
        ids: Vec::new(),
        links: vec![Vec::new(); order.len()],
        bound: HashMap::new(),
    };
    for (index, (_, targets)) in expanded.iter().enumerate() {
        let from = placed[&index];
        for (to, name, _, read_only) in targets {
            topology.link(from, placed[to], name.clone(), *read_only)?;
        }
    }
    topology.identify()?;
    // Every recorded identity must be the identity of the shape found.
    for (index, (_, targets)) in expanded.iter().enumerate() {
        for (to, _, recorded, _) in targets {
            if topology.ids[placed[to]] != *recorded {
                return Err(StackError::Identity {
                    layer: expanded[index].0.address_entity(),
                });
            }
        }
    }
    Ok(topology)
}

/// Layers linked under scope names, read as one composite and written
/// by placement. Opened by [`Stack::open`]; cheap to clone, and every
/// clone shares the wiring and the heads. A stack runs one commit,
/// publish, advance, or pull at a time, across its clones; reads run
/// beside them.
///
/// A stack holds, per layer, the head it last **published** or pulled,
/// and for branch layers a **staged** chain of commits not yet
/// published. Reads and commits build on the staged tip where there
/// is one and on the published head otherwise; [`publish`](Self::publish)
/// moves every branch layer's head to its staged tip, bottom to top.
#[derive(Debug, Clone)]
pub struct Stack {
    /// Wiring, heads, and staged chains, shared by clones.
    state: Arc<RwLock<State>>,
    /// The divergences of placement that reads of the stack found, until
    /// its host takes them. Shared by clones.
    divergences: Divergences,
    /// One commit, publish, advance, or pull at a time, across clones: a
    /// commit staged while a publish runs could otherwise build on a chain
    /// that the publish drops, and its instants be released with no part
    /// of it published.
    turn: Arc<AsyncMutex<()>>,
    /// What entered the ephemeral layers outside the stack, until a
    /// commit induces over it. Shared by clones.
    entered: Entered,
    /// The identity of this stack in the process, which clones share: an
    /// instant minted under a hold of another stack entered this one's
    /// layers from outside it.
    id: u64,
}

/// The next identity of a stack in the process.
static STACKS: AtomicU64 = AtomicU64::new(1);

/// What entered the ephemeral layers of a stack outside it, such as what
/// a channel received: an observer of each layer, and the facts they saw
/// that no commit has induced over yet. The next commit of the stack
/// treats them as changes of its own, as a commit of a branch treats what
/// a pull brought, so a rule fires on them once.
#[derive(Debug, Clone, Default)]
struct Entered(Arc<Mutex<Watched>>);

/// A fact that entered a layer: the layer, and the attribute, entity, and
/// value of the fact.
type EnteredFact = (Entity, String, String, Vec<u8>);

/// What entered, as one net change for each fact: the fact, and whether
/// it was stored or removed. A fact stored and then removed again, or the
/// reverse, changed nothing, and is not there.
type Changed = BTreeMap<EnteredFact, (Artifact, bool)>;

/// What a stack knows of what entered its ephemeral layers: how it
/// watches each, what entered and no commit induced over yet, and what
/// the staged commits induced over.
#[derive(Debug, Default)]
struct Watched {
    /// How the stack watches each ephemeral layer, by its address entity.
    observers: HashMap<Entity, Watch>,
    /// What entered since a commit last induced over it.
    pending: Changed,
    /// What each staged commit induced over, oldest first, until it
    /// publishes. It counts as induced once the commit publishes, and is
    /// offered again, beneath what entered since, if it never does.
    claimed: Vec<Changed>,
}

/// How a stack watches one ephemeral layer: an observer of it, and the
/// facts the layer held as of the last instant the observer saw. When the
/// observer falls behind, the facts the layer holds now, against those,
/// are what entered.
#[derive(Debug)]
struct Watch {
    layer: Ephemeral,
    observer: crate::Observer,
    seen: HashMap<EnteredFact, Artifact>,
}

impl Watch {
    /// Watch `layer` from now on: what it holds now has not entered.
    fn new(address: &Entity, layer: &Ephemeral) -> Self {
        // As many instants as a channel over the layer logs, so that the
        // largest catch-up it receives at once fits.
        let observer = layer.observe_everything_within(crate::DEFAULT_LOG_CAPACITY);
        Self {
            layer: layer.clone(),
            observer,
            seen: Self::held(address, layer),
        }
    }

    fn held(address: &Entity, layer: &Ephemeral) -> HashMap<EnteredFact, Artifact> {
        layer
            .facts()
            .into_iter()
            .map(|fact| (entered_fact(address, &fact), fact))
            .collect()
    }
}

/// Fold the change of `fact`, which `key` names, into `changes`: a change
/// that undoes the one held cancels it.
fn fold(changes: &mut Changed, key: EnteredFact, fact: Artifact, stored: bool) {
    match changes.get(&key) {
        Some((_, held)) if *held != stored => {
            changes.remove(&key);
        }
        _ => {
            changes.insert(key, (fact, stored));
        }
    }
}

/// The key of `fact` in `layer`.
fn entered_fact(layer: &Entity, fact: &Artifact) -> EnteredFact {
    (
        layer.clone(),
        fact.the.to_string(),
        fact.of.to_string(),
        fact.is.to_bytes(),
    )
}

impl Entered {
    /// Observe each ephemeral layer of `topology` that is not observed
    /// yet, from now on, and stop observing each layer that left.
    fn track(&self, topology: &Topology) {
        let mut watched = self.0.lock();
        let layers: HashMap<Entity, &Ephemeral> = topology
            .layers
            .iter()
            .filter_map(|layer| match layer {
                Layer::Ephemeral(ephemeral) => Some((layer.address_entity(), ephemeral)),
                _ => None,
            })
            .collect();
        watched
            .observers
            .retain(|address, _| layers.contains_key(address));
        // What entered a layer that left the stack is no change of it.
        let Watched {
            pending, claimed, ..
        } = &mut *watched;
        pending.retain(|(layer, ..), _| layers.contains_key(layer));
        for group in claimed.iter_mut() {
            group.retain(|(layer, ..), _| layers.contains_key(layer));
        }
        for (address, ephemeral) in layers {
            if let Entry::Vacant(slot) = watched.observers.entry(address) {
                let watch = Watch::new(slot.key(), ephemeral);
                slot.insert(watch);
            }
        }
    }

    /// Everything that entered and no commit has induced over yet: each
    /// fact that an instant from outside the stack stored or removed, as
    /// its net change. What a stack writes itself, and the witness of a
    /// transient, is not part of it.
    fn pending(&self, owner: u64) -> Vec<(Entity, Artifact, bool)> {
        let watched = self.drain(owner);
        watched
            .pending
            .iter()
            .map(|((layer, ..), (fact, stored))| (layer.clone(), fact.clone(), *stored))
            .collect()
    }

    /// Take in what the observers saw: what entered from outside the
    /// stack joins what is pending. A commit drains them as it lands too,
    /// so that its own instants never wait in a queue for the next one.
    fn drain(&self, owner: u64) -> parking_lot::MutexGuard<'_, Watched> {
        let mut watched = self.0.lock();
        let Watched {
            observers, pending, ..
        } = &mut *watched;
        for (address, watch) in observers.iter_mut() {
            match watch.observer.drain() {
                crate::Drained::Instants(instants) => {
                    for instant in instants {
                        if instant.transient {
                            continue;
                        }
                        let outside = !instant.hold.by(owner);
                        for fact in instant.retracted {
                            let key = entered_fact(address, &fact);
                            watch.seen.remove(&key);
                            if outside {
                                fold(pending, key, fact, false);
                            }
                        }
                        for fact in instant.asserted {
                            let key = entered_fact(address, &fact);
                            watch.seen.insert(key.clone(), fact.clone());
                            if outside {
                                fold(pending, key, fact, true);
                            }
                        }
                    }
                }
                crate::Drained::Gap { .. } => {
                    // The observer fell behind: what the layer holds now,
                    // against what it held when the observer last saw it,
                    // is what entered. What the stack wrote itself in the
                    // meantime counts as entered too.
                    tracing::warn!(
                        layer = %address,
                        "more entered an ephemeral layer than a stack observes between two \
                         commits; what entered is taken from the layer"
                    );
                    let now = Watch::held(address, &watch.layer);
                    for (key, fact) in &now {
                        if !watch.seen.contains_key(key) {
                            fold(pending, key.clone(), fact.clone(), true);
                        }
                    }
                    for (key, fact) in mem::take(&mut watch.seen) {
                        if !now.contains_key(&key) {
                            fold(pending, key, fact, false);
                        }
                    }
                    watch.seen = now;
                }
            }
        }
        watched
    }

    /// A commit that stages induced over all that is pending: it waits for
    /// the commit to publish.
    fn claim(&self) {
        let mut watched = self.0.lock();
        let group = mem::take(&mut watched.pending);
        if !group.is_empty() {
            watched.claimed.push(group);
        }
    }

    /// A commit that staged nothing induced over all that is pending: it
    /// goes.
    fn induce(&self) {
        self.0.lock().pending.clear();
    }

    /// The staged commits published: what they induced over goes.
    fn published(&self) {
        self.0.lock().claimed.clear();
    }

    /// What entered goes with nothing induced over it: a stack with no
    /// branch at its bottom runs no rule.
    fn forget(&self) {
        let mut watched = self.0.lock();
        watched.pending.clear();
        watched.claimed.clear();
    }

    /// The staged commits never publish: what they induced over is
    /// offered to the next commit again, with what entered since folded
    /// over it.
    fn unclaim(&self) {
        let mut watched = self.0.lock();
        let mut offered = Changed::new();
        let later = mem::take(&mut watched.pending);
        for group in watched.claimed.drain(..).chain([later]) {
            for (key, (fact, stored)) in group {
                fold(&mut offered, key, fact, stored);
            }
        }
        watched.pending = offered;
    }
}

/// What a stack knows: its shape, and its layers' heads.
struct State {
    topology: Topology,
    /// Per layer, the head the stack last published or pulled: the
    /// base every staged chain builds on. An ephemeral layer's head
    /// moves here directly, since it has nothing to publish.
    published: Vec<Head>,
    /// Per branch layer, the head cell's version at `published`, which
    /// a publish CAS's against. `None` for other layers.
    versions: Vec<Option<MemoryVersion>>,
    /// Per branch layer, the staged chain of commits since
    /// `published`, or `None` when nothing is staged.
    staged: Vec<Option<TransactionBatch>>,
    /// The head of the bottom that the staged commits induced over, with
    /// the version its induction watermark stood at then: once they
    /// publish, the watermark moves to it, though no commit wrote the
    /// bottom. A commit that writes nothing to the bottom would otherwise
    /// leave the watermark behind, and every later commit would fire the
    /// rules over the same lag again.
    induced: Option<(Revision, Option<HistoryVersion>)>,
    /// The induction watermark of a bottom under a read-only link: the
    /// head of the bottom that the last published commit induced over. The
    /// stack never writes a layer that it only reads, so it keeps the
    /// watermark here, for as long as it lives, and not in the induction
    /// cell of the bottom. `None` until a commit induces, which adopts the
    /// head as a branch that never induced does.
    watermark: Option<Revision>,
    /// The holds of the commits staged and not published yet: their
    /// instants wait in each channel until the chains publish, and
    /// never leave if the chains are dropped. See [`Channel`](crate::Channel).
    holds: Vec<Hold>,
}

/// The hold of a commit while it stages. A commit that fails, or whose
/// future is dropped, before it hands the hold to the stack never
/// publishes: its instants never leave the process, and a channel moves
/// past them. Once its layers begin to stage, it also drops the staged
/// chains of the stack, since a chain may hold part of it.
struct Pending {
    hold: Option<Hold>,
    staging: Option<Stack>,
}

impl Pending {
    fn open(owner: u64) -> Self {
        Self {
            hold: Some(Hold::open(owner)),
            staging: None,
        }
    }

    /// The hold, for the instants the commit mints.
    fn hold(&self) -> Hold {
        self.hold
            .clone()
            .expect("a pending hold is held until kept")
    }

    /// The layers of `stack` begin to stage.
    fn staging(&mut self, stack: &Stack) {
        self.staging = Some(stack.clone());
    }

    /// The commit staged: the stack keeps the hold from now on.
    fn keep(mut self) -> Hold {
        self.staging = None;
        self.hold.take().expect("a pending hold is kept once")
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(hold) = self.hold.take() {
            hold.discard();
        }
        if let Some(stack) = self.staging.take() {
            stack.abandon_staged();
        }
    }
}

/// The holds of the commits that a publish settles, taken from the stack
/// while it runs, and what entered its layers that those commits induced
/// over. A publish that fails, or whose future is dropped, before every
/// chain lands discards the holds, since those commits never publish
/// whole, and offers what entered to the next commit again.
struct Settling {
    holds: Vec<Hold>,
    entered: Entered,
}

impl Settling {
    /// Every chain published: the instants may leave, and what entered
    /// is induced.
    fn release(mut self) {
        for hold in self.holds.drain(..) {
            hold.release();
        }
        self.entered.published();
    }
}

impl Drop for Settling {
    fn drop(&mut self) {
        if self.holds.is_empty() {
            return;
        }
        for hold in self.holds.drain(..) {
            hold.discard();
        }
        self.entered.unclaim();
    }
}

/// A stack dropped with commits staged and not published: they never
/// publish, so their instants never leave the process.
impl Drop for State {
    fn drop(&mut self) {
        for hold in self.holds.drain(..) {
            hold.discard();
        }
    }
}

impl State {
    /// The staged chains are dropped: the commits that staged them never
    /// publish, so their instants never leave the process.
    fn drop_staged(&mut self) {
        self.staged = self.staged.iter().map(|_| None).collect();
        self.induced = None;
        for hold in self.holds.drain(..) {
            hold.discard();
        }
    }
}

impl fmt::Debug for State {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let staged: Vec<Option<Revision>> = self
            .staged
            .iter()
            .map(|chain| chain.as_ref().map(TransactionBatch::revision))
            .collect();
        f.debug_struct("State")
            .field("topology", &self.topology)
            .field("published", &self.published)
            .field("versions", &self.versions)
            .field("staged", &staged)
            .finish()
    }
}

impl State {
    /// The head the stack reads layer `index` at: the staged tip, or
    /// the published head.
    fn captured(&self, index: usize) -> Head {
        match &self.staged[index] {
            Some(batch) => Head::Tree(Some(batch.revision())),
            None => self.published[index].clone(),
        }
    }

    /// The head the stack reads each layer at, bottom first.
    fn heads(&self) -> Vec<Head> {
        (0..self.topology.layers.len())
            .map(|index| self.captured(index))
            .collect()
    }
}

/// Every branch layer's head cell version now.
fn versions_of(layers: &[Layer]) -> Vec<Option<MemoryVersion>> {
    layers
        .iter()
        .map(|layer| match layer {
            Layer::Branch(branch) => branch
                .revision_cell()
                .edition()
                .map(|edition| edition.version),
            _ => None,
        })
        .collect()
}

impl Stack {
    /// Open the stack `layer` heads: its recorded links are walked
    /// downward and every target resolved through the environment. A
    /// layer that links nothing opens as a stack of one; link more
    /// through the returned command or a later transaction.
    pub fn open(layer: impl Into<Layer>) -> OpenStack {
        OpenStack {
            top: layer.into(),
            edits: Vec::new(),
        }
    }

    fn from_topology(topology: Topology) -> Self {
        let published: Vec<Head> = topology.layers.iter().map(Layer::head).collect();
        let versions = versions_of(&topology.layers);
        let staged = topology.layers.iter().map(|_| None).collect();
        let stack = Stack {
            state: Arc::new(RwLock::new(State {
                topology,
                published,
                versions,
                staged,
                induced: None,
                watermark: None,
                holds: Vec::new(),
            })),
            divergences: Divergences::default(),
            turn: Arc::default(),
            entered: Entered::default(),
            id: STACKS.fetch_add(1, Ordering::Relaxed),
        };
        stack.entered.track(&stack.topology());
        stack
    }

    /// Takes each divergence of placement that a read of the stack found
    /// since the last call: a fact in a layer outside the scope of its
    /// attribute's placement. No read sees such a fact, and each one is
    /// reported once, with its attribute and its layer. See
    /// [`Divergence`].
    pub fn divergences(&self) -> Vec<Divergence> {
        self.divergences.take()
    }

    /// The shape now.
    fn topology(&self) -> Topology {
        self.state.read().topology.clone()
    }

    /// The layers, bottom first.
    pub fn layers(&self) -> Vec<Layer> {
        self.state.read().topology.layers.clone()
    }

    /// The stack's identity: the top layer's, which covers every layer
    /// beneath it.
    pub fn identity(&self) -> Entity {
        self.state
            .read()
            .topology
            .ids
            .last()
            .cloned()
            .expect("an open stack holds at least one layer")
    }

    /// Every layer's stack identity, bottom first.
    pub fn identities(&self) -> Vec<Entity> {
        self.state.read().topology.ids.clone()
    }

    /// The layers linked under `name`.
    pub fn scope(&self, name: &Entity) -> Vec<Layer> {
        let state = self.state.read();
        state
            .topology
            .bound
            .get(name)
            .map(|indices| {
                indices
                    .iter()
                    .map(|index| state.topology.layers[*index].clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Every layer's live head now, bottom first: what each layer's
    /// own handle reports, whatever the stack has published or
    /// staged.
    pub fn heads(&self) -> Vec<Head> {
        self.state
            .read()
            .topology
            .layers
            .iter()
            .map(Layer::head)
            .collect()
    }

    /// The heads the stack reads each layer at, bottom first: the
    /// staged tip where a chain is staged, the published head
    /// otherwise.
    pub fn captured(&self) -> Vec<Head> {
        self.state.read().heads()
    }

    /// The heads the stack last published or pulled, bottom first:
    /// what every staged chain builds on.
    pub fn published(&self) -> Vec<Head> {
        self.state.read().published.clone()
    }

    /// Whether some branch layer holds commits not yet published.
    pub fn is_staged(&self) -> bool {
        self.state.read().staged.iter().any(Option::is_some)
    }

    /// Whether some layer's live head differs from the head the stack
    /// last published or pulled: it moved outside the stack, through
    /// this handle, and a publish of that layer would fail until an
    /// [`advance`](Self::advance) (or a [`pull`](Self::pull), which
    /// advances). Movement through another handle is not visible here
    /// until an advance re-resolves the heads.
    pub fn behind(&self) -> bool {
        self.heads() != self.published()
    }

    /// Bring movement in: pull every layer that the stack writes and that
    /// tracks an upstream, bottom to top, then [`advance`](Self::advance)
    /// over the result. A layer that the stack only reads is not pulled:
    /// its writer moves it, and the advance reads its new head.
    ///
    /// This is the network half of keeping a stack current, and it
    /// costs a round trip per upstream, so it belongs to whatever
    /// already pulls branches on a schedule: the same sync loop, one
    /// call per stack. Movement this process can already see needs no
    /// pull; `advance` captures it alone.
    pub async fn pull<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: StackSyncEnv,
    {
        let _turn = self.turn.lock().await;
        let topology = self.topology();
        for (index, layer) in topology.layers.iter().enumerate() {
            if let Layer::Branch(branch) = layer
                && !branch.upstreams().is_empty()
            {
                branch.refresh(env).await?;
                // A pull merges into the layer, which is a write: a layer
                // that the stack only reads is advanced to its head alone.
                if topology.writable(index) {
                    Box::pin(branch.pull().perform(env)).await?;
                }
            }
        }
        self.advance_in_turn(env).await
    }

    /// Capture movement this process can see, without the network:
    /// re-resolve every branch layer's head from storage, take the
    /// live heads as the published base, and stage and publish the
    /// wiring of every layer above a layer that moved.
    ///
    /// Anything staged and not yet published is dropped: its chain
    /// built on heads this supersedes, so it is stale wholesale, and
    /// the transaction that staged it is re-run on the fresh heads. An
    /// advance that fails part way has taken the live heads all the
    /// same; the next one stages the wiring again.
    ///
    /// Cheap enough to call on a signal: a poll that found
    /// [`behind`](Self::behind), a commit made through another handle
    /// of a layer, a tab regaining focus. A stack whose layers only
    /// ever move through it never needs one.
    pub async fn advance<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: StackSyncEnv,
    {
        let _turn = self.turn.lock().await;
        self.advance_in_turn(env).await
    }

    /// [`advance`](Self::advance), in the turn of the stack.
    async fn advance_in_turn<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: StackSyncEnv,
    {
        let topology = self.topology();
        for layer in &topology.layers {
            if let Layer::Branch(branch) = layer {
                branch.refresh(env).await?;
            }
        }
        let previous = self.captured();
        let live: Vec<Head> = topology.layers.iter().map(Layer::head).collect();
        {
            let mut state = self.state.write();
            state.published = live.clone();
            state.versions = versions_of(&topology.layers);
            state.drop_staged();
        }
        self.entered.unclaim();
        self.capture(
            &topology,
            BTreeMap::new(),
            &Upkeep::new(),
            live,
            &previous,
            &Hold::released(self.id),
            env,
        )
        .await?;
        self.publish_in_turn(env).await
    }

    /// Send movement out: push every branch layer that the stack writes
    /// and that tracks an upstream, bottom to top, so a pushed layer's wiring never names
    /// a head its upstream lacks. Pushes the published heads; staged
    /// commits are not pushed until [`publish`](Self::publish)ed.
    pub async fn push<Env>(&self, env: &Env) -> Result<(), StackError>
    where
        Env: StackSyncEnv,
    {
        let topology = self.topology();
        for (index, layer) in topology.layers.iter().enumerate() {
            if let Layer::Branch(branch) = layer
                && topology.writable(index)
                && !branch.upstreams().is_empty()
            {
                Box::pin(branch.push().perform(env)).await?;
            }
        }
        Ok(())
    }

    /// Publish every staged chain, bottom to top: each branch layer's
    /// head moves to its staged tip with one CAS against the version
    /// the stack last published or pulled. A layer whose head moved
    /// outside the stack fails the CAS; its chain and every chain
    /// above it are then stale wholesale and dropped (layers beneath
    /// stay published), and the transactions that staged them are
    /// re-run after a [`pull`](Self::pull). The instants that those
    /// commits minted in ephemeral layers never leave the process, even
    /// when a layer beneath the one that failed did publish its part:
    /// the run again mints them again.
    pub async fn publish<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let _turn = self.turn.lock().await;
        self.publish_in_turn(env).await
    }

    /// [`publish`](Self::publish), in the turn of the stack.
    async fn publish_in_turn<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let layers = self.layers();
        // The commits staged now are the ones this publish settles: the
        // turn of the stack keeps any other commit out until it ends.
        let holds = Settling {
            holds: mem::take(&mut self.state.write().holds),
            entered: self.entered.clone(),
        };
        for (index, layer) in layers.iter().enumerate() {
            let Some(batch) = self.state.write().staged[index].take() else {
                continue;
            };
            match batch.publish().perform(env).await {
                Ok(revision) => {
                    let version = match layer {
                        Layer::Branch(branch) => branch
                            .revision_cell()
                            .edition()
                            .map(|edition| edition.version),
                        _ => None,
                    };
                    let mut state = self.state.write();
                    state.published[index] = Head::Tree(Some(revision));
                    state.versions[index] = version;
                }
                Err(source) => {
                    let mut state = self.state.write();
                    for stale in state.staged.iter_mut().skip(index) {
                        *stale = None;
                    }
                    state.induced = None;
                    // The commits did not publish whole: their instants
                    // never leave the process.
                    drop(holds);
                    return Err(StackError::Publish {
                        layer: Box::new(layer.address_entity()),
                        source,
                    });
                }
            }
        }
        // Every staged chain published: the instants of the commits that
        // staged them may leave.
        holds.release();
        let induced = self.state.write().induced.take();
        self.catch_up_induction(induced, env).await?;
        Ok(self.captured())
    }

    /// Moves the induction watermark of the bottom to `induced`, the head
    /// that published commits induced over, while it still stands where it
    /// stood then. A commit that staged the bottom moved it on publish
    /// already; this is for one that wrote only other layers. The
    /// watermark of a bottom under a read-only link moves in memory.
    async fn catch_up_induction<Env>(
        &self,
        induced: Option<(Revision, Option<HistoryVersion>)>,
        env: &Env,
    ) -> Result<(), StackError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let (Some((head, base)), Some(bottom)) = (induced, self.primary()) else {
            return Ok(());
        };
        {
            let mut state = self.state.write();
            if !state.topology.writable(0) {
                state.watermark = Some(head);
                return Ok(());
            }
        }
        let cell = bottom.induction_cell();
        cell.resolve()
            .perform(env)
            .await
            .map_err(CommitError::from)?;
        // A publish of the bottom's own chain moved the watermark past the
        // head the commits read, and another writer may have moved it too:
        // it moves only from where the commits found it.
        if cell.content().as_ref() == Some(&head)
            || cell.content().map(|revision| revision.version()) != base
        {
            return Ok(());
        }
        match cell.publish(head).perform(env).await {
            Err(crate::PublishError::VersionMismatch { .. }) => Ok(()),
            other => other.map_err(|error| CommitError::from(error).into()),
        }
    }

    /// Hold the instants that a commit minted under `hold` until its
    /// chains publish, or release them at once when it staged no durable
    /// layer and nothing waits to publish. What entered an upper layer,
    /// which the commit induced over, counts as induced at the same time.
    /// A commit with no durable change staged behind one that waits
    /// shares its fate: if that publish fails, its instants never leave
    /// either. Publish each commit before the next, as
    /// `commit().publish()` does, to keep them apart.
    fn hold_until_published(&self, hold: Hold) {
        let mut state = self.state.write();
        if state.staged.iter().any(Option::is_some) {
            self.entered.claim();
            state.holds.push(hold);
        } else {
            hold.release();
            self.entered.induce();
        }
    }

    /// The bottom layer as a branch: where placements live and where
    /// undeclared attributes go. `None` when the bottom is not a
    /// branch, in which case the stack is read-only.
    fn primary(&self) -> Option<Branch> {
        self.state.read().topology.primary()
    }

    /// The composite a read sees: every layer beneath the top at the
    /// head the stack reads it at, the top live.
    pub(crate) fn composite(&self) -> Composite {
        self.composite_at(&self.topology(), &self.captured(), self.top_staged())
    }

    /// Whether a chain is staged on the top layer.
    fn top_staged(&self) -> bool {
        self.state.read().staged.last().is_some_and(Option::is_some)
    }

    /// The composite with every branch beneath the top read at the
    /// given heads. Ephemeral layers are always live: they are
    /// process-local, written only through the stack, and cannot be
    /// read at an older sequence. A top branch is read live too, unless
    /// `top_staged`.
    fn composite_at(&self, topology: &Topology, heads: &[Head], top_staged: bool) -> Composite {
        let top = topology.layers.len().saturating_sub(1);
        let mut composite = Composite::default();
        let mut guide = Guide {
            lines: Vec::new(),
            ephemerals: Vec::new(),
            bound: topology.bound.keys().cloned().collect(),
            bottom: match topology.layers.first() {
                Some(Layer::Branch(branch)) => branch.bindings().clone(),
                Some(Layer::Snapshot(snapshot)) => snapshot.bindings().clone(),
                _ => Bindings::default(),
            },
            divergences: self.divergences.clone(),
        };
        for (index, layer) in topology.layers.iter().enumerate() {
            let guided = GuidedLayer {
                address: layer.address_entity(),
                scopes: topology
                    .bound
                    .iter()
                    .filter(|(_, layers)| layers.contains(&index))
                    .map(|(scope, _)| scope.clone())
                    .collect(),
                bottom: index == 0,
            };
            match layer {
                Layer::Ephemeral(_) => guide.ephemerals.push(guided),
                Layer::Branch(_) | Layer::Snapshot(_) => guide.lines.push(guided),
            }
            match layer {
                Layer::Branch(branch) if index == top && !top_staged => composite
                    .sources
                    .push(Source::Branch(Arc::new(branch.clone()))),
                Layer::Branch(branch) => {
                    let revision = match heads.get(index) {
                        Some(Head::Tree(revision)) => revision.clone(),
                        _ => branch.revision(),
                    };
                    composite
                        .sources
                        .push(Source::Pinned(Arc::new(branch.clone()), revision))
                }
                Layer::Snapshot(snapshot) => composite
                    .sources
                    .push(Source::Snapshot(Arc::new(snapshot.clone()))),
                Layer::Ephemeral(ephemeral) => composite.ephemerals.push(ephemeral.clone()),
            }
        }
        // The placements live in the bottom layer, so a read is guided
        // only when the bottom is a tree.
        if matches!(
            topology.layers.first(),
            Some(Layer::Branch(_) | Layer::Snapshot(_))
        ) {
            composite.guide = Some(Arc::new(guide));
        }
        composite
    }

    /// Open a query over the whole stack at the heads it reads at.
    /// Use [`select`](StackQuery::select) or
    /// [`subscribe`](StackQuery::subscribe) on it.
    pub fn query(&self) -> StackQuery {
        StackQuery {
            stack: self.clone(),
            composite: self.composite(),
            changes: Changes::new(),
            layers: Vec::new(),
        }
    }

    /// Start a transaction on this stack.
    pub fn transaction(&self) -> StackTransaction<'_> {
        StackTransaction {
            stack: self,
            changes: Staged::default(),
            transients: Staged::default(),
            edits: Vec::new(),
            stores: Vec::new(),
        }
    }

    /// Stage `batch` on layer `index` on top of `heads[index]`, with
    /// its links at `heads`. A branch layer extends its staged chain
    /// (or opens one on its published head); an ephemeral layer is
    /// written directly, its `maintenance` (clear, forget) landing in
    /// the same instant as the batch. A layer with nothing to write,
    /// no maintenance and no links is left alone; one whose link facts
    /// already hold stages a no-op and keeps its head.
    #[allow(clippy::too_many_arguments)]
    async fn refresh_links<Env>(
        &self,
        topology: &Topology,
        index: usize,
        heads: &[Head],
        mut batch: Changes,
        maintenance: Option<&(bool, HashSet<Entity>)>,
        hold: &Hold,
        env: &Env,
    ) -> Result<Option<Head>, CommitError>
    where
        Env: StackEnv,
    {
        if batch.is_empty() && maintenance.is_none() && topology.links[index].is_empty() {
            return Ok(None);
        }
        match (&topology.layers[index], maintenance) {
            // An ephemeral layer holds its wiring from the commit that
            // stamped it, and a refresh moves the revision alone: the facts
            // it holds already are left out rather than re-asserted, which
            // would change nothing at the price of a cell scan each.
            // Maintenance may clear the layer, so it re-stamps them all.
            (Layer::Ephemeral(ephemeral), None) => {
                let mut wiring = Changes::new();
                topology.link_facts(index, heads, &mut wiring);
                for instruction in ephemeral.unheld(wiring) {
                    if let Instruction::Assert(fact, pick) = instruction {
                        batch.associate(fact.the, fact.of, fact.is, pick);
                    }
                }
            }
            _ => topology.link_facts(index, heads, &mut batch),
        }
        match &topology.layers[index] {
            Layer::Branch(branch) => {
                let staged = self.state.write().staged[index].take();
                let (chain, tip) = match staged {
                    Some(mut chain) => {
                        let tip = match chain.mint(batch, env).await {
                            Ok(tip) => tip,
                            Err(error) => {
                                self.state.write().staged[index] = Some(chain);
                                return Err(error);
                            }
                        };
                        (chain, tip)
                    }
                    None => {
                        let (base, version) = {
                            let state = self.state.read();
                            let base = match &state.published[index] {
                                Head::Tree(revision) => revision.clone(),
                                Head::Ephemeral(_) => None,
                            };
                            (base, state.versions[index].clone())
                        };
                        let chain =
                            TransactionBatch::stage(branch, base, version, batch, env).await?;
                        let tip = chain.revision();
                        (chain, tip)
                    }
                };
                self.state.write().staged[index] = Some(chain);
                Ok(Some(Head::Tree(Some(tip))))
            }
            Layer::Ephemeral(ephemeral) => {
                match maintenance {
                    // Maintenance clears user state while the stack keeps
                    // its wiring, re-stamped by the link facts above, and
                    // the layer's share of the commit lands with it.
                    Some((clear, forgotten)) => ephemeral.maintain(batch, *clear, forgotten, hold),
                    None => {
                        ephemeral.apply_held(batch, hold)?;
                    }
                }
                let head = Head::Ephemeral(ephemeral.revision());
                self.state.write().published[index] = head.clone();
                Ok(Some(head))
            }
            Layer::Snapshot(_) => Err(CommitError::Detached),
        }
    }

    /// Stage each layer's batch bottom to top on top of `heads`, every
    /// layer after everything beneath it, writing its own share and its
    /// wiring at the heads as they stand once the layers beneath it
    /// staged. A layer is touched only when it has something of its
    /// own to write or a layer its wiring reaches moved, either in this
    /// pass or since `previous`; a layer the pass never reaches is
    /// never asked to write, whatever happened to it outside the
    /// stack. Returns the heads the stack reads at afterwards.
    #[allow(clippy::too_many_arguments)]
    async fn capture<Env>(
        &self,
        topology: &Topology,
        mut batches: BTreeMap<usize, Changes>,
        upkeep: &Upkeep,
        mut heads: Vec<Head>,
        previous: &[Head],
        hold: &Hold,
        env: &Env,
    ) -> Result<Vec<Head>, CommitError>
    where
        Env: StackEnv,
    {
        let mut moved: Vec<bool> = heads
            .iter()
            .enumerate()
            .map(|(index, head)| previous.get(index) != Some(head))
            .collect();
        for index in 0..topology.layers.len() {
            let batch = batches.remove(&index).unwrap_or_default();
            let maintenance = upkeep.get(&index);
            // A layer that the stack only reads keeps its head: nothing
            // routes to it, and its own links are not refreshed.
            if !topology.writable(index)
                || (batch.is_empty()
                    && maintenance.is_none()
                    && !topology.reaches_moved(index, &moved))
            {
                continue;
            }
            if let Some(head) = self
                .refresh_links(topology, index, &heads, batch, maintenance, hold, env)
                .await?
            {
                if heads[index] != head {
                    moved[index] = true;
                }
                heads[index] = head;
            }
        }
        Ok(heads)
    }

    /// Apply queued wiring edits to a detached copy of the shape: layers
    /// that enter bring their heads with them, layers the top no longer
    /// reaches leave with theirs, and no chain is staged, since a chain
    /// built on the old shape is stale wholesale. Returns the shape, for
    /// [`Stack::install`] once the commit is sure to land, and the link
    /// entities to retract on their enclosers.
    fn edit(&self, mut edits: Vec<LinkEdit>) -> Result<Rewired, StackError> {
        let live = self.state.read();
        // Validate the entire edit batch on a detached shape. A rejected
        // edit must preserve both the live topology and its staged chains.
        let mut state = State {
            topology: live.topology.clone(),
            published: live.published.clone(),
            versions: live.versions.clone(),
            staged: live.topology.layers.iter().map(|_| None).collect(),
            induced: None,
            watermark: live.watermark.clone(),
            holds: Vec::new(),
        };
        // An edit applies once its encloser is in the stack, so a
        // transaction may name its links in any order: a layer that a
        // later edit enters beneath the top can be the encloser of an
        // earlier one. An encloser nothing enters is refused.
        let mut enclosers: Vec<Layer> = Vec::new();
        while !edits.is_empty() {
            let mut deferred = Vec::with_capacity(edits.len());
            let mut applied = false;
            for edit in edits {
                let from = match &edit {
                    LinkEdit::Link { from, .. } | LinkEdit::Unlink { from, .. } => from,
                };
                if state.topology.index_of(from).is_none() {
                    deferred.push(edit);
                    continue;
                }
                enclosers.push(from.clone());
                applied = true;
                match edit {
                    LinkEdit::Link {
                        from,
                        to,
                        name,
                        read_only,
                    } => {
                        if state.topology.index_of(&to).is_none() {
                            let at = state
                                .topology
                                .index_of(&from)
                                .expect("the encloser is in the stack");
                            state.enter(at, to.clone());
                        }
                        let from_index = state
                            .topology
                            .index_of(&from)
                            .expect("the encloser is in the stack");
                        let to_index = state
                            .topology
                            .index_of(&to)
                            .expect("the target is in the stack");
                        state.topology.link(from_index, to_index, name, read_only)?;
                    }
                    LinkEdit::Unlink { from, to, name } => {
                        let Some(to_index) = state.topology.index_of(&to) else {
                            return Err(StackError::UnknownLayer {
                                from: from.address_entity(),
                            });
                        };
                        let from_index = state
                            .topology
                            .index_of(&from)
                            .expect("the encloser is in the stack");
                        if state.topology.unlink(from_index, to_index, &name).is_none() {
                            return Err(StackError::UnknownLayer {
                                from: from.address_entity(),
                            });
                        }
                    }
                }
            }
            if !applied {
                let from = match &deferred[0] {
                    LinkEdit::Link { from, .. } | LinkEdit::Unlink { from, .. } => from,
                };
                return Err(StackError::UnknownLayer {
                    from: from.address_entity(),
                });
            }
            edits = deferred;
        }
        // Keep enclosers available until every edit has been applied.
        // Detached layers leave with their own stored wiring untouched.
        state.leave_unreachable()?;
        // A layer that the stack only reads, in the shape the edits leave,
        // holds no link that the stack could write.
        for encloser in &enclosers {
            if let Some(index) = state.topology.index_of(encloser)
                && !state.topology.writable(index)
            {
                return Err(StackError::ReadOnly {
                    scope: state.topology.scope_of_layer(index),
                });
            }
        }
        // A watermark held in memory is of the bottom that it was held for,
        // while the stack only reads it.
        let bottom = |state: &State| {
            (
                state.topology.layers.first().map(Layer::address_entity),
                state.topology.writable(0),
            )
        };
        if bottom(&state) != bottom(&live) {
            state.watermark = None;
        }
        let mut retractions = Vec::new();
        for (from, links) in live.topology.layers.iter().zip(&live.topology.links) {
            let Some(index) = state.topology.index_of(from) else {
                continue;
            };
            for link in links {
                if !state.topology.links[index]
                    .iter()
                    .any(|current| current.entity == link.entity)
                {
                    // A descendant's shape change also changes the link
                    // entity of its ancestors, even without an unlink.
                    retractions.push((from.clone(), link.entity.clone()));
                }
            }
        }
        state.staged = state.topology.layers.iter().map(|_| None).collect();
        Ok((state, retractions))
    }

    /// Make `state`, a shape that [`Stack::edit`] built, the stack's own,
    /// and return the state it replaces. No chain is staged on that one: a
    /// commit that rewires a stack with staged commits is refused.
    fn install(&self, state: State) -> State {
        let replaced = mem::replace(&mut *self.state.write(), state);
        self.entered.track(&self.topology());
        replaced
    }

    /// Drop every staged chain, after a commit failed partway through
    /// staging: a chain that holds part of it must never publish, and a
    /// chain cannot be cut back to the commits before it. What its
    /// commits induced over is offered again.
    fn abandon_staged(&self) {
        self.state.write().drop_staged();
        self.entered.unclaim();
    }
}

/// A shape that wiring edits built, and the link entities to retract on
/// their enclosers.
type Rewired = (State, Vec<(Layer, Entity)>);

impl State {
    /// Put `layer` at position `at` with its live head.
    fn enter(&mut self, at: usize, layer: Layer) {
        let head = layer.head();
        let version = versions_of(slice::from_ref(&layer)).remove(0);
        self.topology.insert(at, layer);
        self.published.insert(at, head);
        self.versions.insert(at, version);
        self.staged.insert(at, None);
    }

    /// Drop the layers the top no longer reaches, with their heads.
    fn leave_unreachable(&mut self) -> Result<(), StackError> {
        let before: Vec<Entity> = self
            .topology
            .layers
            .iter()
            .map(Layer::address_entity)
            .collect();
        self.topology.prune()?;
        let after: Vec<Entity> = self
            .topology
            .layers
            .iter()
            .map(Layer::address_entity)
            .collect();
        let published = mem::take(&mut self.published);
        let versions = mem::take(&mut self.versions);
        for (index, address) in before.iter().enumerate() {
            if after.contains(address) {
                self.published.push(published[index].clone());
                self.versions.push(versions[index].clone());
            }
        }
        Ok(())
    }
}

/// Witnesses a stack transaction's induction rounds: each round's
/// transients are minted as an instant on the store of every layer
/// their scope is linked under, or on the bottom's session store when
/// no link binds the scope, mirroring how the settled batch routes. The
/// rounds wait until the commit is sure to route
/// ([`land`](Self::land)), so a commit that fails changes no layer and
/// shows no observer a round of it.
struct StackWitness<'a> {
    topology: &'a Topology,
    placements: &'a Placements,
    bottom: &'a Ephemeral,
    /// The hold of the commit, which each witness carries.
    hold: &'a Hold,
    /// Each round's shares, by layer, or `None` for the bottom's session
    /// store, in the order induction ran them.
    rounds: Vec<BTreeMap<Option<usize>, Changes>>,
}

impl StackWitness<'_> {
    /// Witness each round, in order, once the settled batch routed. A
    /// transient that a rule derives onto a scope that the stack only
    /// reads fails the commit, as a dispatched one does, and nothing is
    /// witnessed.
    fn land(self) -> Result<(), StackError> {
        for shares in &self.rounds {
            for index in shares.keys().flatten() {
                if !self.topology.writable(*index) {
                    return Err(StackError::ReadOnly {
                        scope: self.topology.scope_of_layer(*index),
                    });
                }
            }
        }
        for shares in self.rounds {
            for (target, share) in shares {
                let store = match target {
                    None => self.bottom,
                    Some(index) => self.topology.layers[index].store(),
                };
                store.witness_held(share, self.hold);
            }
        }
        Ok(())
    }
}

impl Witness for StackWitness<'_> {
    fn round(&mut self, transients: &Changes) {
        let mut shares: BTreeMap<Option<usize>, Changes> = BTreeMap::new();
        for instruction in transients.clone().into_instructions() {
            let (op, artifact) = split(instruction);
            // A scope that links bind decides first, as for routing.
            let targets: Vec<Option<usize>> = match self.placements.placement_of(&artifact.the) {
                None => vec![None],
                Some(scope) => match self.topology.bound.get(scope) {
                    Some(indices) => indices.iter().map(|index| Some(*index)).collect(),
                    None => vec![None],
                },
            };
            for target in targets {
                apply(
                    shares.entry(target).or_default(),
                    op.clone(),
                    artifact.clone(),
                );
            }
        }
        self.rounds.push(shares);
    }
}

/// A transaction on a [`Stack`]: accumulates facts and wiring, then
/// routes each fact to the layers its attribute's scope is linked
/// under, applies the wiring, and stages the layers bottom to top.
pub struct StackTransaction<'a> {
    stack: &'a Stack,
    /// The durable writes, and the dispatched transients, held as a
    /// branch transaction holds them (see [`Staged`](crate::Staged)): a
    /// read of the transaction is a range read, and a write under a pick
    /// that elects settles its succession at commit.
    changes: Staged,
    transients: Staged,
    edits: Vec<LinkEdit>,
    /// Maintenance of the ephemeral layers under a scope, in order.
    stores: Vec<(Entity, Maintenance)>,
}

/// The maintenance a commit applies per layer, by index: whether to
/// clear the layer, and the entities to forget. Applied in the same
/// instant as the layer's share of the commit, so readers and
/// observers never see an entity between forget and its re-stamp.
type Upkeep = BTreeMap<usize, (bool, HashSet<Entity>)>;

/// What a transaction does to the ephemeral layers under a scope,
/// besides writing facts.
#[derive(Debug, Clone)]
enum Maintenance {
    /// Drop every fact and tombstone.
    Clear,
    /// Drop every fact and tombstone recorded for these entities.
    Forget(Vec<Entity>),
}

impl<'a> StackTransaction<'a> {
    /// Assert a claim.
    pub fn assert<C: Statement>(mut self, claim: C) -> Self {
        self.changes.assert(claim);
        self
    }

    /// Retract a claim.
    pub fn retract<C: Statement>(mut self, claim: C) -> Self {
        self.changes.retract(claim);
        self
    }

    /// Dispatch a transient claim: visible to rule bodies during this
    /// commit's induction, never written anywhere.
    pub fn dispatch<C: Statement>(mut self, claim: C) -> Self {
        self.transients.assert(claim);
        self
    }

    /// Drop every fact the ephemeral layers under `scope` hold. A
    /// tree layer under the scope is refused at commit.
    pub fn clear(mut self, scope: Entity) -> Self {
        self.stores.push((scope, Maintenance::Clear));
        self
    }

    /// Drop every fact the ephemeral layers under `scope` hold for
    /// `entities`: the garbage-collection form, for per-client facts
    /// keyed by short-lived entities. A tree layer under the scope is
    /// refused at commit.
    pub fn forget(mut self, scope: Entity, entities: Vec<Entity>) -> Self {
        self.stores.push((scope, Maintenance::Forget(entities)));
        self
    }

    /// Link `from` to `to` under `name`: `from` records `to`'s head
    /// and the name routes to `to`. A `from` not yet in the stack
    /// must enter through another link of the transaction; a `to` not
    /// yet in the stack enters just beneath `from`, so of the layers
    /// that `from` links side by side, the first linked is lowest, and
    /// the lowest of all is the bottom, which holds the placements.
    /// The audience rule, the ordering, and a snapshot's inability to
    /// link are checked when the transaction commits, and the link
    /// facts land on `from` with the rest of the commit.
    pub fn link<F: AsLayer, T: AsLayer>(mut self, from: &F, to: &T, name: Entity) -> Self {
        self.edits.push(LinkEdit::Link {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
            read_only: false,
        });
        self
    }

    /// Link `from` to `to` under `name` as [`link`](Self::link) does,
    /// read-only: the stack reads `to`, records its head on `from`, and
    /// advances it, but never writes, stages, publishes, pulls, or pushes
    /// it, or a layer beneath it.
    /// A write placed on `name` fails at commit with
    /// [`StackError::ReadOnly`], before any layer changes. So does a
    /// transient placed on it, and a `clear` or `forget` of it. A write or
    /// a transient that a rule derives onto `name` fails too, once
    /// induction ran: no layer changes, and no observer sees a round of
    /// the commit. A layer that another link reaches other than read-only
    /// is written as usual.
    pub fn link_read_only<F: AsLayer, T: AsLayer>(
        mut self,
        from: &F,
        to: &T,
        name: Entity,
    ) -> Self {
        self.edits.push(LinkEdit::Link {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
            read_only: true,
        });
        self
    }

    /// Drop the link from `from` to `to` under `name`. Its facts are
    /// retracted from `from`, and any layer the top no longer reaches
    /// leaves the stack: unlinking one seed branch and linking another
    /// in the same transaction swaps them.
    pub fn unlink<F: AsLayer, T: AsLayer>(mut self, from: &F, to: &T, name: Entity) -> Self {
        self.edits.push(LinkEdit::Unlink {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
        });
        self
    }

    /// The stack this transaction runs on.
    pub fn stack(&self) -> &'a Stack {
        self.stack
    }

    /// Run queries against this transaction's "as-if committed" view
    /// of the stack: the composite at the heads the stack reads at,
    /// with the pending asserts and retracts folded in over it and the
    /// dispatched transients beside them. Retracts hide what the
    /// layers hold, as the commit would; nothing lands, and the
    /// transaction stays open and committable. The view is taken as
    /// the writes stand now: later writes to the transaction do not
    /// reach a query already opened.
    pub fn query(&self) -> StackQuery {
        StackQuery {
            stack: self.stack.clone(),
            composite: self.stack.composite(),
            changes: Changes::new(),
            layers: vec![self.changes.clone(), self.transients.clone()],
        }
    }

    /// Finalize into a commit command. Its `perform` stages; chain
    /// [`publish`](StackCommit::publish) to stage and publish in one
    /// step.
    ///
    /// A [`Channel`](crate::Channel) holds the instants of a staged
    /// commit until it publishes, and keeps every later instant in its log
    /// past its bound meanwhile. A commit that stages should be published,
    /// or dropped with an advance, a pull, or the stack, or the log grows
    /// without bound.
    pub fn commit(self) -> StackCommit<'a> {
        StackCommit {
            stack: self.stack,
            changes: self.changes,
            transients: self.transients,
            edits: self.edits,
            stores: self.stores,
        }
    }
}

/// Command staging a [`StackTransaction`].
pub struct StackCommit<'a> {
    stack: &'a Stack,
    changes: Staged,
    transients: Staged,
    edits: Vec<LinkEdit>,
    stores: Vec<(Entity, Maintenance)>,
}

impl<'a> StackCommit<'a> {
    /// Stage, then publish every staged chain bottom to top.
    pub fn publish(self) -> StackPublish<'a> {
        StackPublish { commit: self }
    }

    /// Apply the wiring, induce against the composite at the heads the
    /// stack reads at, route by placement, and stage the layers bottom
    /// to top. Never moves a branch head: the staged chains wait for
    /// [`Stack::publish`]. Returns the heads the stack reads at
    /// afterwards, bottom first.
    ///
    /// A commit that fails while the layers stage drops every staged
    /// chain, those of earlier commits not yet published included, since a
    /// chain may hold part of it: run those commits again.
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: StackEnv,
    {
        let stack = self.stack;
        // A NaN is no value: refused before anything changes, the wiring
        // included, as a branch commit refuses it.
        refuse_nan(self.changes.log().iter().map(|(_, _, change)| change))?;
        let _turn = stack.turn.lock().await;
        let mut batches: BTreeMap<usize, Changes> = BTreeMap::new();

        // Wiring first: a re-shaped stack reads and routes as re-shaped.
        // With no edits the previous heads are the captured ones and
        // nothing counts as moved; with edits every layer counts as
        // moved, so every encloser rewrites its wiring (a no-op where
        // it already holds). The new shape stays detached until the
        // layers stage: a commit that fails before leaves the stack as
        // it was, with the chains it had staged.
        let rewired = !self.edits.is_empty();
        if rewired && stack.is_staged() {
            return Err(StackError::Staged);
        }
        let (mut rewiring, previous) = if rewired {
            let (state, retractions) = stack.edit(self.edits)?;
            for (from, entity) in retractions {
                let index = state
                    .topology
                    .index_of(&from)
                    .expect("an unlinked encloser stays in the stack");
                let facts = held(&from, ArtifactSelector::new().of(entity), env).await?;
                let batch = batches.entry(index).or_default();
                for fact in facts {
                    batch.dissociate(fact.the, fact.of, fact.is);
                }
            }
            (Some(state), Vec::new())
        } else {
            (None, stack.captured())
        };
        let topology = match &rewiring {
            Some(state) => state.topology.clone(),
            None => stack.topology(),
        };

        // Resolve and validate every explicit scope before touching any store.
        let mut maintenance = Upkeep::new();
        for (scope, operation) in self.stores {
            let Some(indices) = topology.bound.get(&scope) else {
                return Err(StackError::UnboundScope { scope });
            };
            for index in indices {
                if !matches!(&topology.layers[*index], Layer::Ephemeral(_)) {
                    return Err(StackError::NotEphemeral { scope });
                }
                if !topology.writable(*index) {
                    return Err(StackError::ReadOnly { scope });
                }
                let (clear, forgotten) = maintenance.entry(*index).or_default();
                match &operation {
                    Maintenance::Clear => *clear = true,
                    Maintenance::Forget(entities) => forgotten.extend(entities.iter().cloned()),
                }
            }
        }
        let captured = match &rewiring {
            Some(state) => state.heads(),
            None => stack.captured(),
        };
        // Every instant this commit mints in an ephemeral layer carries
        // its hold, so a channel sends it only once the commit publishes.
        let mut pending = Pending::open(stack.id);
        let hold = pending.hold();
        // What entered an upper layer outside the stack since its last
        // commit is a change of this one: a commit with no facts of its
        // own still runs the rules over it.
        stack.entered.track(&stack.topology());
        let mut entered = stack.entered.pending(stack.id);
        if topology.primary().is_none() {
            // With no branch at the bottom no rule runs, and no commit
            // stages: what entered has nothing to fire, and goes.
            stack.entered.forget();
            entered.clear();
        }
        // What this commit clears or forgets from a layer is no change of
        // it: a fact that entered and fails every commit leaves with the
        // clear, so the stack can drop it itself.
        entered.retain(|(address, fact, _)| {
            topology
                .layers
                .iter()
                .position(|layer| layer.address_entity() == *address)
                .and_then(|index| maintenance.get(&index))
                .is_none_or(|(clear, forgotten)| !*clear && !forgotten.contains(&fact.of))
        });

        if self.changes.is_empty() && self.transients.is_empty() && !rewired && entered.is_empty() {
            // Maintenance alone: each store's lands as one instant, its
            // wiring re-stamped, through the same staging pass a write
            // takes.
            pending.staging(stack);
            let heads = stack
                .capture(
                    &topology,
                    batches,
                    &maintenance,
                    captured.clone(),
                    &previous,
                    &hold,
                    env,
                )
                .await?;
            stack.hold_until_published(pending.keep());
            drop(stack.entered.drain(stack.id));
            return Ok(heads);
        }
        let Some(primary) = topology.primary() else {
            return Err(CommitError::Detached.into());
        };
        // A write builds on the heads the stack reads at, like a
        // branch commit builds on its handle's head: induction reads
        // them, and each layer stages on top of its own.
        // A new shape stages nothing yet.
        let top_staged = rewiring.is_none() && stack.top_staged();
        let composite = stack.composite_at(&topology, &captured, top_staged);
        let source = match composite.sources.first() {
            Some(source) => source.as_ref(),
            None => SourceRef::from(&primary),
        };

        // A write under a pick that elects succeeds the claim a read of
        // the stack would return: settled first, as a branch commit
        // settles, over the view. The row of an ephemeral layer is a
        // candidate that a read elects, but no claim, so no write to a tree
        // succeeds it. A write that lands in an ephemeral layer keeps its
        // pick: the layer elects among its own rows of the cell.
        let operator = Identify.perform(env).await.map_err(CommitError::from)?;
        let mut view = QueryLayer::new();
        for line in &composite.sources {
            view = view.join(QueryLayer::from(line.as_ref()));
        }
        let declared = Placements::resolve(source, &self.changes.export(), env).await?;
        let stores: HashSet<_> = self
            .changes
            .log()
            .iter()
            .filter(|(the, _, change)| change.elects() && !topology.lands_in_trees(&declared, the))
            .map(|(the, ..)| the.clone())
            .collect();
        let changes = Box::pin(settle_successions(
            &composite,
            view.overlay(&operator),
            &self.changes,
            &stores,
            env,
        ))
        .await?;
        let staged = Placements::resolve(source, &changes, env).await?;
        // The bottom's lag is what induction reads past its watermark: the
        // head it reads at, and where the watermark stands, settle once
        // the commit publishes. A bottom under a read-only link is never
        // written, so the stack holds its watermark in memory.
        // A watermark that already stands at the head read has nothing to
        // catch up, and the commit skips reading it again once it lands.
        let (watermark, lagged, caught_up) = if topology.writable(0) {
            let cell = primary.induction_cell();
            cell.resolve()
                .perform(env)
                .await
                .map_err(CommitError::from)?;
            let head = source.revision();
            let caught_up = head.is_some() && cell.content() == head;
            let lagged = head.map(|head| (head, cell.content().map(|revision| revision.version())));
            (Watermark::Cell, lagged, caught_up)
        } else {
            let held = match &rewiring {
                Some(state) => state.watermark.clone(),
                None => stack.state.read().watermark.clone(),
            };
            (
                Watermark::Held(held),
                source.revision().map(|head| (head, None)),
                false,
            )
        };
        // Each round's transients are witnessed on the store of every
        // layer their scope is linked under, else the bottom's session
        // store, so an observer there sees the command that fired a
        // rule even though the commit folds it away. Resolved from the
        // declarations as staged before induction; a declaration
        // induction itself adds routes the settled batch below.
        let mut witness = StackWitness {
            topology: &topology,
            placements: &staged,
            bottom: primary.overlay(),
            rounds: Vec::new(),
            hold: &hold,
        };
        // A write placed on a read-only scope fails before induction; so
        // does a dispatched transient, whose witness would land in that
        // layer. A write that a rule derives onto a read-only scope fails
        // when the settled batch routes, after induction. Either way no
        // layer changes, and no observer sees a round of the commit: the
        // rounds are witnessed only once the batch routes.
        // A transient with no placement is witnessed in the session store
        // of the bottom, which is memory of the process: only one placed
        // on a scope that the stack reads alone is refused. A transient
        // that the transaction asserted is one it dispatched.
        let transient = |artifact: &Artifact| match staged.placement_of(&artifact.the) {
            Some(scope)
                if topology.bound.get(scope).is_some_and(|indices| {
                    indices.iter().any(|index| !topology.writable(*index))
                }) =>
            {
                Err(StackError::ReadOnly {
                    scope: scope.clone(),
                })
            }
            // A transient placed on a scope that no link binds, that is not
            // the default, and that the bottom does not bind to its tree,
            // has no store that a read of the stack takes: it is refused,
            // as a write of it is.
            Some(scope)
                if !topology.bound.contains_key(scope)
                    && Some(scope) != staged.default_scope()
                    && primary.bindings().target(scope) != Some(Target::Tree) =>
            {
                Err(StackError::from(CommitError::UnboundScope {
                    attribute: artifact.the.to_string(),
                    scope: scope.to_string(),
                }))
            }
            _ => Ok(()),
        };
        for instruction in changes.clone().into_instructions() {
            let (_, artifact) = split(instruction);
            if staged.is_transient(&artifact.the) {
                transient(&artifact)?;
            } else {
                route(&topology, &staged, &primary, &artifact)?;
            }
        }
        for instruction in self.transients.export().into_instructions() {
            let (_, artifact) = split(instruction);
            transient(&artifact)?;
        }
        let mut changes = changes;
        // Assets ride apart from the facts: induction and routing rebuild
        // the batch fact by fact, and only a tree can store an asset, so
        // they land on the bottom with the rest of its share below.
        let assets = changes.take_assets();
        // Only the bottom stores an asset: one over a read-only bottom is
        // refused with the writes that would land there.
        if !assets.is_empty() && !topology.writable(0) {
            return Err(match topology.bottom_scope(staged.default_scope()) {
                Some(scope) => StackError::ReadOnly { scope },
                None => CommitError::Detached.into(),
            });
        }
        // A fact that entered a layer outside the scope of its placement
        // fires no rule, as it answers no query.
        let scopes = staged.scopes();
        let stimulus: Vec<Instruction> = entered
            .iter()
            .filter(|(address, fact, _)| match &composite.guide {
                Some(guide) => guide
                    .ephemerals
                    .iter()
                    .find(|layer| layer.address == *address)
                    .is_none_or(|layer| guide.admits(layer, &scopes, fact.the.as_str())),
                None => true,
            })
            .map(|(_, fact, stored)| match stored {
                true => Instruction::Assert(fact.clone(), Pick::All),
                false => Instruction::Retract(fact.clone()),
            })
            .collect();
        induce(
            source,
            watermark,
            &composite,
            &staged,
            Landing::Stores(topology.store_scopes()),
            &mut changes,
            self.transients.export(),
            stimulus,
            &mut witness,
            env,
        )
        .await?;

        // Route by placement: the primary holds the declarations, the
        // stack's links bind the names. A name no link binds falls
        // back to a tree binding on the primary, so a single-layer
        // stack routes exactly as the branch would; the primary's own
        // session store is not a layer and gets nothing.
        let placements = Placements::resolve(source, &changes, env).await?;
        for instruction in changes.into_instructions() {
            let (op, artifact) = split(instruction);
            let targets = route(&topology, &placements, &primary, &artifact)?;
            for target in targets {
                // Nothing reached through a snapshot can write: refused
                // here, before any layer stages.
                if matches!(topology.layers[target], Layer::Snapshot(_)) {
                    return Err(CommitError::Detached.into());
                }
                apply(
                    batches.entry(target).or_default(),
                    op.clone(),
                    artifact.clone(),
                );
            }
        }
        witness.land()?;
        if !assets.is_empty() {
            let bottom = batches.entry(0).or_default();
            for change in assets {
                match change {
                    AssetChange::Import(asset) => bottom.import(asset),
                    AssetChange::Discard(asset) => bottom.discard(asset),
                }
            }
        }

        // Maintenance lands with each layer's share of the settled batch:
        // readers and observers never see an entity between forget and
        // its re-stamp.
        // If staging fails partway, a rewired stack goes back to the shape
        // it had, and any other drops its staged chains: a chain that holds
        // part of the failed commit must never publish.
        let replaced = rewiring.take().map(|state| stack.install(state));
        pending.staging(stack);
        let staging = stack
            .capture(
                &topology,
                batches,
                &maintenance,
                captured.clone(),
                &previous,
                &hold,
                env,
            )
            .await;
        let heads = match staging {
            Ok(heads) => heads,
            Err(error) => {
                // The pending hold drops the staged chains as it goes.
                if let Some(state) = replaced {
                    stack.install(state);
                }
                return Err(error.into());
            }
        };
        // The commit landed: its hold, and what entered that it induced
        // over, settle before the watermark moves, so a failure to move it
        // fires nothing twice.
        let waits = stack.is_staged();
        if waits {
            stack.state.write().induced = lagged.clone();
        }
        stack.hold_until_published(pending.keep());
        drop(stack.entered.drain(stack.id));
        if !waits && !caught_up {
            stack.catch_up_induction(lagged, env).await?;
        }
        Ok(heads)
    }
}

/// Command staging and publishing a [`StackTransaction`] in one step.
pub struct StackPublish<'a> {
    commit: StackCommit<'a>,
}

impl StackPublish<'_> {
    /// Stage the transaction, then publish every staged chain bottom
    /// to top. Returns the heads the stack reads at afterwards.
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: StackSyncEnv,
    {
        let stack = self.commit.stack;
        self.commit.perform(env).await?;
        stack.publish(env).await
    }
}

/// A query over a [`Stack`] at its captured heads, with optional
/// overlay facts. Created by [`Stack::query`], or by
/// [`StackTransaction::query`] with the transaction's pending writes
/// as the overlay.
#[derive(Clone)]
pub struct StackQuery {
    stack: Stack,
    composite: Composite,
    changes: Changes,
    /// The pending writes of a transaction, read above the composite as a
    /// branch transaction reads its own (see [`Staged`](crate::Staged)).
    layers: Vec<Staged>,
}

impl StackQuery {
    /// Fold a [`Statement`] into this query's overlay facts.
    pub fn with<S: Statement>(mut self, statement: S) -> Self {
        statement.assert(&mut self.changes);
        self
    }

    /// Stage a query application. Call `.perform(&env)` to execute.
    pub fn select<Q: Application>(&self, query: Q) -> StackSelect<Q> {
        StackSelect {
            composite: self.composite.clone(),
            changes: self.changes.clone(),
            layers: self.layers.clone(),
            query,
        }
    }

    /// Register a standing query over the stack. A poll reads at the
    /// stack's captured heads and never writes: movement outside the
    /// stack lands as a delta on the first poll after a
    /// [`pull`](Stack::pull).
    pub fn subscribe<Q: Application>(&self, query: Q) -> StackSubscription<Q> {
        StackSubscription {
            stack: self.stack.clone(),
            inner: Subscription::over(self.composite.clone(), self.changes.clone(), query)
                .with_layers(self.layers.clone()),
        }
    }
}

/// A query command over a stack's composite, ready to be performed.
pub struct StackSelect<Q> {
    composite: Composite,
    changes: Changes,
    layers: Vec<Staged>,
    query: Q,
}

impl<Q: Application> StackSelect<Q> {
    /// Execute the query, returning a stream of results.
    pub fn perform<'a, Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: StackEnv,
    {
        let StackSelect {
            composite,
            changes,
            layers,
            query,
        } = self;
        async_stream::try_stream! {
            let operator = Identify
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("identify: {e}")))?;
            let mut layer = QueryLayer::new();
            for source in &composite.sources {
                layer = layer.join(QueryLayer::from(source.as_ref()));
            }
            let overlay = layer.with(changes).overlay(&operator);
            let query_env = QueryEnv::over(composite.clone(), overlay, env).with_layers(layers);
            let results = Box::pin(query.perform(&query_env));
            for await result in results {
                yield result?;
            }
        }
    }
}

/// A standing query over a [`Stack`]. Created by
/// [`StackQuery::subscribe`]; driven by [`poll`](Self::poll).
pub struct StackSubscription<Q: Application> {
    stack: Stack,
    inner: Subscription<Q>,
}

impl<Q> StackSubscription<Q>
where
    Q: Application + Clone + ConditionalSync,
    Q::Conclusion: dialog_query::Conclusion + PartialEq + Clone + ConditionalSync,
{
    /// Poll against what the stack reads at now. A read: nothing is
    /// written, so a layer that moved outside the stack is not seen
    /// until [`Stack::pull`] captures it, and then this poll reports
    /// the change as its delta.
    pub async fn poll<'a, Env>(
        &'a mut self,
        env: &'a Env,
    ) -> Result<Option<Delta<Q::Conclusion>>, EvaluationError>
    where
        Env: StackEnv,
    {
        self.inner.retarget(self.stack.composite());
        self.inner.poll(env).await
    }

    /// Whether the stack is behind its layers' live heads; see
    /// [`Stack::behind`].
    pub fn behind(&self) -> bool {
        self.stack.behind()
    }

    /// The last evaluation's full result.
    pub fn results(&self) -> &[Q::Conclusion] {
        self.inner.results()
    }

    /// Full evaluations performed so far.
    pub fn recomputes(&self) -> usize {
        self.inner.recomputes()
    }

    /// Polls maintained incrementally so far.
    pub fn maintenances(&self) -> usize {
        self.inner.maintenances()
    }
}

/// The layers that a write of `artifact` lands in, by its placement: the
/// layers its scope is linked under, or the bottom for an attribute with
/// no placement, one on the default scope, or one on a scope no link
/// binds that the bottom binds to its tree. A write that would land in a
/// layer the stack only reads fails with [`StackError::ReadOnly`].
fn route(
    topology: &Topology,
    placements: &Placements,
    primary: &Branch,
    artifact: &Artifact,
) -> Result<Vec<usize>, StackError> {
    let default = placements.default_scope();
    let (targets, scope): (Vec<usize>, Option<Entity>) =
        match placements.placement_of(&artifact.the) {
            None => (vec![0], None),
            // A scope that links bind decides first, as for a read, so a
            // default scope never overrides a placement.
            Some(scope) => match topology.bound.get(scope) {
                Some(indices) => (indices.clone(), Some(scope.clone())),
                None if Some(scope) == default => (vec![0], Some(scope.clone())),
                None => match primary.bindings().target(scope) {
                    Some(Target::Tree) => (vec![0], Some(scope.clone())),
                    Some(Target::Session) | None => {
                        return Err(CommitError::UnboundScope {
                            attribute: artifact.the.to_string(),
                            scope: scope.to_string(),
                        }
                        .into());
                    }
                },
            },
        };
    if targets.iter().any(|index| !topology.writable(*index)) {
        let scope = match scope {
            Some(scope) if topology.bound.contains_key(&scope) => Some(scope),
            _ => topology.bottom_scope(default),
        };
        return Err(match scope {
            Some(scope) => StackError::ReadOnly { scope },
            None => CommitError::Detached.into(),
        });
    }
    Ok(targets)
}

/// What an instruction does to its fact, apart from the fact.
#[derive(Debug, Clone)]
enum Op {
    Assert(Pick),
    Retract,
}

fn split(instruction: Instruction) -> (Op, Artifact) {
    match instruction {
        Instruction::Assert(artifact, pick) => (Op::Assert(pick), artifact),
        Instruction::Retract(artifact) => (Op::Retract, artifact),
    }
}

fn apply(changes: &mut Changes, op: Op, artifact: Artifact) {
    let Artifact { the, of, is, .. } = artifact;
    match op {
        Op::Assert(pick) => changes.associate(the, of, is, pick),
        Op::Retract => changes.dissociate(the, of, is),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::Placement;
    use crate::helpers::test_repo;
    use crate::{Demand, Drained};
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::{AttributeStatement, Relation};
    use dialog_query::rule::DeductiveRuleDescriptor;
    use dialog_query::types::Scalar;
    use dialog_query::{AttributeQuery, Term};
    use dialog_search_tree::Manifest;
    use futures_util::TryStreamExt as _;
    use futures_util::stream;

    fn name(name: &str) -> Entity {
        format!("memory:{name}").parse().expect("layer entity")
    }

    /// The values a `(the, of)` pair holds in a stack's composite read.
    async fn values<V: Scalar>(
        stack: &Stack,
        env: &impl StackEnv,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<Value>> {
        let query = AttributeQuery::from(
            Term::<Relation>::from(the.parse::<Relation>()?)
                .of(Term::<Entity>::from(of.clone()))
                .is(Term::<V>::var("v")),
        );
        let claims = stack.query().select(query).perform(env).try_vec().await?;
        Ok(claims.into_iter().map(|claim| claim.is).collect())
    }

    /// The values a `(the, of)` pair holds in a branch's tree only.
    async fn committed(
        branch: &Branch,
        env: &impl StackEnv,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<Value>> {
        let selector = ArtifactSelector::new().the(the.parse()?).of(of.clone());
        let stream = branch.claims().select(selector).perform(env).await?;
        let artifacts: Vec<_> = stream
            .map_ok(|view| view.to_owned())
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(artifacts.into_iter().map(|artifact| artifact.is).collect())
    }

    /// A branch bottom, an ephemeral state layer linked over it: a
    /// transaction routes the placed attribute to the state layer and
    /// the rest to the tree, the composite read joins them, and a
    /// subscription over the stack maintains the state half.
    #[dialog_common::test]
    async fn it_routes_by_name_across_lines() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        shared
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        // A name binds where its link points: `memory:shared` is the
        // bottom, and `memory:state` needs a layer above state to link
        // it under that name.
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&state, &shared, name("shared"))
            .link(&top, &state, name("state"))
            .perform(&operator)
            .await?;
        assert_eq!(stack.layers().len(), 3);

        let doc: Entity = "doc:1".parse()?;
        let heads = stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(heads.len(), 3);
        shared.refresh(&operator).await?;

        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the undeclared attribute lands in the bottom tree"
        );
        assert!(
            committed(&shared, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "the placed attribute never reaches the tree"
        );
        let selected = ArtifactSelector::new().the("ui/selected".parse()?);
        assert_eq!(
            state.scan(&selected).len(),
            1,
            "the placed attribute lands in the state layer"
        );
        assert!(
            !state
                .scan(&ArtifactSelector::new().the("dialog.link/to".parse()?))
                .is_empty(),
            "beside the state layer's own link facts"
        );
        assert!(
            shared.overlay().is_empty(),
            "and not in the bottom's own store"
        );
        assert_eq!(
            values::<bool>(&stack, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "the composite read joins the state layer"
        );

        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("ui/selected"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<bool>::var("v")),
        ));
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(initial.asserted.len(), 1);
        stack
            .transaction()
            .retract(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the state layer's change propagates");
        assert_eq!(delta.retracted.len(), 1);
        assert_eq!(subscription.maintenances(), 1, "maintained from the ring");
        Ok(())
    }

    /// An enclosing branch records its links as facts with the heads
    /// it saw, and a stack commit that moves the bottom refreshes the
    /// link on the branch above it in the same commit.
    #[dialog_common::test]
    async fn it_records_links_and_refreshes_them_when_the_target_moves() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;

        shared
            .transaction()
            .assert(Placement::new("local/note".parse()?, name("local")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&local, &shared, name("shared"))
            .link(&top, &local, name("local"))
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;

        let link = link_entity(
            &Layer::Branch(local.clone()).address_entity(),
            &stack.identities()[0],
            &name("shared"),
        );
        let shared_head = |branch: &Branch| Head::Tree(branch.revision()).bytes();
        assert_eq!(
            committed(&local, &operator, "dialog.link/revision", &link).await?,
            vec![Value::Bytes(shared_head(&shared))],
            "build records the head local saw"
        );
        assert_eq!(
            committed(&local, &operator, "dialog.link/name", &link).await?,
            vec![Value::Entity(name("shared"))]
        );
        assert_eq!(
            committed(&local, &operator, "dialog.link/branch", &link).await?,
            vec![Value::String("main".into())],
            "the link carries the target's address"
        );

        // A commit reaching only the bottom moves shared, and local's
        // link follows in the same stack commit.
        let doc: Entity = "doc:1".parse()?;
        let before = local.revision();
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        local.refresh(&operator).await?;
        assert_ne!(
            local.revision(),
            before,
            "local committed to capture shared"
        );
        assert_eq!(
            committed(&local, &operator, "dialog.link/revision", &link).await?,
            vec![Value::Bytes(shared_head(&shared))],
            "local's link names shared's new head"
        );
        assert!(
            committed(&local, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "and local holds nothing but its links"
        );

        // A commit reaching only local leaves shared alone and mints
        // no refresh anywhere beneath.
        let shared_before = shared.revision();
        stack
            .transaction()
            .assert(
                dialog_query::the!("local/note")
                    .of(doc.clone())
                    .is("draft".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        local.refresh(&operator).await?;
        assert_eq!(
            shared.revision(),
            shared_before,
            "nothing propagates downward"
        );
        assert_eq!(
            committed(&local, &operator, "local/note", &doc).await?,
            vec![Value::String("draft".into())]
        );
        Ok(())
    }

    /// Siblings under one encloser: when local does not link shared,
    /// a commit to shared refreshes only the ephemeral top and local
    /// never moves. Topology chooses what is captured.
    #[dialog_common::test]
    async fn it_refreshes_only_the_lines_above_a_moved_one() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let stack = Stack::open(state.clone())
            .link(&state, &shared, name("shared"))
            .link(&state, &local, name("local"))
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;
        let local_before = local.revision();
        let state_before = state.revision();
        let link = link_entity(
            &state.entity().clone(),
            &stack.identities()[0],
            &name("shared"),
        );

        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        local.refresh(&operator).await?;

        assert_eq!(local.revision(), local_before, "a sibling does not capture");
        assert_ne!(state.revision(), state_before, "the encloser does");
        let selector = ArtifactSelector::new()
            .the("dialog.link/revision".parse()?)
            .of(link);
        let seen: Vec<Value> = state.scan(&selector).into_iter().map(|a| a.is).collect();
        assert_eq!(
            seen,
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())],
            "the top's link names shared's new head"
        );
        Ok(())
    }

    /// Wiring stays where it is made: local holds its link to shared,
    /// the top holds only its own link to local, and a stack commit
    /// that moves shared refreshes local's link in the same commit.
    #[dialog_common::test]
    async fn it_keeps_wiring_where_it_is_made() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let stack = Stack::open(state.clone())
            .link(&local, &shared, name("shared"))
            .link(&state, &local, name("local"))
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;

        let local_address = Layer::Branch(local.clone()).address_entity();
        let link = link_entity(&local_address, &stack.identities()[0], &name("shared"));
        let own = link_entity(state.entity(), &stack.identities()[1], &name("local"));
        let selector = |the: &str, of: &Entity| {
            ArtifactSelector::new()
                .the(the.parse().expect("attribute"))
                .of(of.clone())
        };
        assert!(
            state
                .scan(&selector("dialog.link/revision", &link))
                .is_empty(),
            "the top does not hold local's link"
        );
        assert_eq!(
            state
                .scan(&selector("dialog.link/to", &own))
                .into_iter()
                .map(|a| a.is)
                .collect::<Vec<_>>(),
            vec![Value::Entity(stack.identities()[1].clone())],
            "only its own link to local"
        );
        assert_eq!(
            committed(&local, &operator, "dialog.link/revision", &link).await?,
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())],
            "local holds its link to shared at the head it saw"
        );

        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        local.refresh(&operator).await?;
        assert_eq!(
            committed(&local, &operator, "dialog.link/revision", &link).await?,
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())],
            "local's link follows shared in the same stack commit"
        );
        Ok(())
    }

    /// A stack reads every layer beneath its top at the captured head:
    /// a commit that bypasses the stack is invisible until the stack
    /// pulls, and then the top's wiring names the new head.
    #[dialog_common::test]
    async fn it_reads_at_captured_heads_until_pulled() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let stack = Stack::open(state.clone())
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        assert!(
            values::<String>(&stack, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "the stack still reads the captured head"
        );
        assert!(stack.behind(), "and knows it is behind");

        let heads = stack.pull(&operator).await?;
        assert_eq!(heads, stack.captured());
        assert!(!stack.behind(), "pull catches up");
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())]
        );
        let link = link_entity(
            &state.entity().clone(),
            &stack.identities()[0],
            &name("shared"),
        );
        let selector = ArtifactSelector::new()
            .the("dialog.link/revision".parse()?)
            .of(link);
        let seen: Vec<Value> = state.scan(&selector).into_iter().map(|a| a.is).collect();
        assert_eq!(
            seen,
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())]
        );
        Ok(())
    }

    /// A subscription poll never writes: a commit that bypassed the
    /// stack is not seen until the stack pulls, and then the next poll
    /// reports it as a delta.
    #[dialog_common::test]
    async fn it_lands_external_movement_on_pull() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("doc/title"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        ));
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty());
        assert!(subscription.poll(&operator).await?.is_none());

        let doc: Entity = "doc:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        assert!(
            subscription.poll(&operator).await?.is_none(),
            "a poll is a read: the stack has not captured the commit"
        );
        assert!(subscription.behind());

        stack.pull(&operator).await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the pulled commit lands on this poll");
        assert_eq!(delta.asserted.len(), 1);
        assert!(!subscription.behind());
        assert!(subscription.poll(&operator).await?.is_none());
        Ok(())
    }

    /// A branch moved through another handle is invisible to the
    /// stack's own handle, so `behind` cannot see it; `pull`
    /// re-resolves the head from storage and then captures.
    #[dialog_common::test]
    async fn it_notices_movement_through_another_handle_on_pull() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let other = repo.branch("main").open().perform(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(
            !stack.behind(),
            "the stack's handle has not seen the other handle's commit"
        );
        assert!(
            values::<String>(&stack, &operator, "doc/title", &doc)
                .await?
                .is_empty()
        );

        stack.pull(&operator).await?;
        assert!(!stack.behind());
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "pull re-resolved the head and captured it"
        );
        assert_eq!(shared.revision(), other.revision());
        Ok(())
    }

    /// `advance` captures a commit made through another handle
    /// without the network: it re-resolves the head from storage and
    /// captures it, exactly as `pull` does after its upstream pulls.
    #[dialog_common::test]
    async fn it_captures_movement_through_another_handle_on_advance() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let other = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let stack = Stack::open(state.clone())
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(
            values::<String>(&stack, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "the stack's handle has not seen the other handle's commit"
        );

        let heads = stack.advance(&operator).await?;
        assert_eq!(heads, stack.captured());
        assert!(!stack.behind());
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "advance re-resolved the head and captured it"
        );
        let link = link_entity(
            &state.entity().clone(),
            &stack.identities()[0],
            &name("shared"),
        );
        let selector = ArtifactSelector::new()
            .the("dialog.link/revision".parse()?)
            .of(link);
        let seen: Vec<Value> = state.scan(&selector).into_iter().map(|a| a.is).collect();
        assert_eq!(
            seen,
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())],
            "and refreshed the wiring above it"
        );
        Ok(())
    }

    /// A chain staged on a head another handle moved past stages
    /// fine and fails to publish; the ephemeral share of the same
    /// transaction is already in place, and re-running the
    /// transaction after a pull lands the rest on top of the other
    /// handle's commit.
    #[dialog_common::test]
    async fn it_fails_a_stale_publish_and_recovers_on_pull() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let other = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        shared
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        other.refresh(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&state, &shared, name("shared"))
            .link(&top, &state, name("state"))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        fn write<'a>(stack: &'a Stack, doc: &Entity) -> StackPublish<'a> {
            stack
                .transaction()
                .assert(
                    dialog_query::the!("doc/author")
                        .of(doc.clone())
                        .is("me".to_string()),
                )
                .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
                .commit()
                .publish()
        }
        let result = write(&stack, &doc).perform(&operator).await;
        assert!(
            matches!(
                result,
                Err(StackError::Publish {
                    source: CommitError::Publish(crate::PublishError::VersionMismatch { .. }),
                    ..
                })
            ),
            "expected a version mismatch, got {result:?}"
        );
        assert_eq!(
            values::<bool>(&stack, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "the ephemeral share is in place regardless"
        );

        stack.pull(&operator).await?;
        write(&stack, &doc).perform(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the other handle's commit is kept"
        );
        assert_eq!(
            committed(&shared, &operator, "doc/author", &doc).await?,
            vec![Value::String("me".into())],
            "and the re-run write lands on top of it"
        );
        Ok(())
    }

    /// Commits stage and publish moves heads: after a stack commit the
    /// branch head is unchanged and the stack reads its staged chain;
    /// a second commit chains onto it; one publish makes both visible.
    #[dialog_common::test]
    async fn it_stages_commits_and_publishes_the_chain() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let published = shared.revision();

        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/author")
                    .of(doc.clone())
                    .is("me".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert!(stack.is_staged());
        assert!(!stack.behind(), "nothing moved outside the stack");
        assert_eq!(shared.revision(), published, "the head has not moved");
        assert_ne!(stack.captured()[0], stack.published()[0]);
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the stack reads its staged chain"
        );
        assert!(
            committed(&shared, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "the branch does not"
        );

        let heads = stack.publish(&operator).await?;
        assert!(!stack.is_staged());
        assert_eq!(heads, stack.published());
        assert_ne!(shared.revision(), published);
        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())]
        );
        assert_eq!(
            committed(&shared, &operator, "doc/author", &doc).await?,
            vec![Value::String("me".into())],
            "one publish made both commits visible"
        );
        Ok(())
    }

    /// A commit is not a pull, and staging never fails for a moved
    /// head. In the chain `shared < local < state`, a commit to shared
    /// outside the stack leaves the stack behind; a stack write that
    /// touches only local stages and publishes fine, and a write to
    /// shared stages fine but fails to publish, until a pull brings
    /// the movement in and the write is re-run.
    #[dialog_common::test]
    async fn it_stages_on_captured_heads_and_publishes_against_them() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        shared
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let stack = Stack::open(state.clone())
            .link(&local, &shared, name("shared"))
            .link(&state, &local, name("state"))
            .perform(&operator)
            .await?;
        // `memory:state` is bound where its link points: local.
        local.refresh(&operator).await?;
        let published = stack.published();
        let local_before = local.revision();

        let doc: Entity = "doc:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(stack.behind());

        // A write routed to local only stages and publishes on the
        // heads the stack holds; shared is never touched.
        stack
            .transaction()
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(stack.behind(), "a commit captures nothing it did not move");
        assert_eq!(
            stack.published()[0],
            published[0],
            "shared is still read at the published head"
        );
        assert!(
            values::<String>(&stack, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "so the outside commit stays invisible"
        );
        local.refresh(&operator).await?;
        assert_ne!(local.revision(), local_before, "local took the write");

        // A write to shared itself stages, then fails to publish:
        // shared moved past the head the chain builds on.
        fn write<'a>(stack: &'a Stack, doc: &Entity) -> StackCommit<'a> {
            stack
                .transaction()
                .assert(
                    dialog_query::the!("doc/author")
                        .of(doc.clone())
                        .is("me".to_string()),
                )
                .commit()
        }
        write(&stack, &doc).perform(&operator).await?;
        assert!(stack.is_staged());
        assert_eq!(
            values::<String>(&stack, &operator, "doc/author", &doc).await?,
            vec![Value::String("me".into())],
            "the stack reads its staged chain"
        );
        let result = stack.publish(&operator).await;
        assert!(
            matches!(
                result,
                Err(StackError::Publish {
                    source: CommitError::Publish(crate::PublishError::VersionMismatch { .. }),
                    ..
                })
            ),
            "expected a version mismatch, got {result:?}"
        );
        assert!(!stack.is_staged(), "the stale chain is dropped");
        assert!(
            values::<String>(&stack, &operator, "doc/author", &doc)
                .await?
                .is_empty(),
            "and the stack reads its published heads again"
        );

        stack.pull(&operator).await?;
        assert!(!stack.behind());
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())]
        );
        write(&stack, &doc).perform(&operator).await?;
        stack.publish(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "doc/author", &doc).await?,
            vec![Value::String("me".into())],
            "the re-run write lands on top of the outside commit"
        );
        Ok(())
    }

    /// A layer that moved outside the stack is left alone by a write
    /// that never reaches it, even when it holds links: in the chain
    /// `shared < local < state < tab`, local moving outside the stack
    /// does not stop a write routed only to state.
    #[dialog_common::test]
    async fn it_leaves_a_moved_line_alone_when_the_write_misses_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        shared
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&local, &shared, name("shared"))
            .link(&state, &local, name("local"))
            .link(&top, &state, name("state"))
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        local
            .transaction()
            .assert(
                dialog_query::the!("local/note")
                    .of(doc.clone())
                    .is("draft".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let local_after = local.revision();
        assert!(stack.behind());

        stack
            .transaction()
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;
        assert_eq!(local.revision(), local_after, "local was not touched");
        assert!(stack.behind(), "and the stack is still behind on it");
        assert_eq!(
            values::<bool>(&stack, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)]
        );
        Ok(())
    }

    /// A wider audience may not link a narrower one: a branch cannot
    /// link an ephemeral store beneath it.
    #[dialog_common::test]
    async fn it_rejects_an_audience_violation() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let result = Stack::open(shared.clone())
            .link(&shared, &state, name("state"))
            .perform(&operator)
            .await;
        assert!(
            matches!(
                result,
                Err(StackError::Audience {
                    from_audience: Audience::Device,
                    to_audience: Audience::Process,
                    ..
                })
            ),
            "expected an audience refusal, got {result:?}"
        );
        Ok(())
    }

    /// A commit that writes the bottom leaves its induction watermark at
    /// the head that its publish lands, so the next commit induces over
    /// nothing of it again.
    #[dialog_common::test]
    async fn it_leaves_the_watermark_at_the_head_that_a_publish_lands() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;
        for title in ["First", "Second"] {
            stack
                .transaction()
                .assert(
                    dialog_query::the!("doc/title")
                        .of("doc:1".parse::<Entity>()?)
                        .is(title.to_string()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            shared.refresh(&operator).await?;
            let watermark = shared.induction_cell();
            watermark.resolve().perform(&operator).await?;
            assert_eq!(
                watermark.content(),
                shared.revision(),
                "the watermark stands at the published head"
            );
        }
        Ok(())
    }

    /// A failed wiring batch preserves its original shape and staged data.
    #[dialog_common::test]
    async fn it_preserves_a_stack_after_rejected_wiring() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:staged".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Pending".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let identity = stack.identity();
        let heads = stack.captured();
        let result = stack
            .transaction()
            .link(&top, &state, name("state"))
            .link(&shared, &state, name("invalid"))
            .commit()
            .perform(&operator)
            .await;
        assert!(result.is_err(), "the second link must be refused");
        assert_eq!(stack.identity(), identity);
        assert_eq!(stack.layers().len(), 2);
        assert_eq!(stack.captured(), heads);
        assert!(
            stack.is_staged(),
            "a rejected edit keeps the pending commit"
        );
        stack.publish(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Pending".into())]
        );
        Ok(())
    }

    /// A commit that rewires a stack with staged commits is refused, and
    /// one that fails after its wiring applies, here on a write whose scope
    /// nothing binds, leaves the shape as it was. Either way the stack is
    /// as it was, its staged chains included.
    #[dialog_common::test]
    async fn it_preserves_a_stack_after_a_failed_commit_that_rewires() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:staged".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Pending".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let identity = stack.identity();
        let heads = stack.captured();
        let refused = stack
            .transaction()
            .link(&top, &state, name("state"))
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(StackError::Staged)),
            "a staged commit would be lost: {refused:?}"
        );
        assert_eq!(stack.identity(), identity);
        assert_eq!(stack.captured(), heads);
        assert!(stack.is_staged(), "the staged commit stays");
        stack.publish(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Pending".into())]
        );

        let heads = stack.captured();
        let failed = stack
            .transaction()
            .link(&top, &state, name("state"))
            .assert(Placement::new("doc/other".parse()?, name("nowhere")))
            .assert(
                dialog_query::the!("doc/other")
                    .of(doc.clone())
                    .is("Lost".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(
                failed,
                Err(StackError::Commit(CommitError::UnboundScope { .. }))
            ),
            "a scope nothing is linked under refuses the write: {failed:?}"
        );
        assert_eq!(stack.identity(), identity);
        assert_eq!(stack.layers().len(), 2);
        assert_eq!(stack.captured(), heads);
        assert!(!stack.is_staged());
        Ok(())
    }

    /// A write that routes to a snapshot is refused before any layer
    /// stages: the commits staged before it stay, and no part of it does.
    #[dialog_common::test]
    async fn it_refuses_a_write_to_a_snapshot_before_staging() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let archive = repo.branch("archive").open().perform(&operator).await?;
        archive
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:old".parse::<Entity>()?)
                    .is("Old".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        archive.refresh(&operator).await?;
        let frozen = archive.snapshot().expect("the archive has a revision");
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .link(&top, &frozen, name("archive"))
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Pending".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        let heads = stack.captured();
        let refused = stack
            .transaction()
            .assert(Placement::new("doc/body".parse()?, name("archive")))
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Half".to_string()),
            )
            .assert(
                dialog_query::the!("doc/body")
                    .of(doc.clone())
                    .is("Frozen".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(StackError::Commit(CommitError::Detached))),
            "a snapshot takes no write: {refused:?}"
        );
        assert_eq!(stack.captured(), heads, "no part of it staged");
        stack.publish(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Pending".into())]
        );
        Ok(())
    }

    /// A layer links another under two names: each scope binds the
    /// layer, a write placed on either lands in it once, a read guided by
    /// placement finds both through it with no divergence, and a commit
    /// that moves it advances both links to the one head.
    #[dialog_common::test]
    async fn it_binds_one_layer_under_two_scopes() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("space"))
            .link(&top, &shared, name("persona"))
            .perform(&operator)
            .await?;
        shared
            .overlay()
            .assert(Placement::new("persona/petname".parse()?, name("persona")))?;
        shared
            .overlay()
            .assert(Placement::new("doc/title".parse()?, name("space")))?;
        let bottom = Layer::Branch(shared.clone());
        for scope in ["space", "persona"] {
            let layers = stack.scope(&name(scope));
            assert_eq!(layers.len(), 1, "{scope} binds one layer");
            assert!(layers[0].same(&bottom), "{scope} binds the shared layer");
        }

        let doc: Entity = "doc:1".parse()?;
        let before = shared.revision();
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(
                dialog_query::the!("persona/petname")
                    .of(doc.clone())
                    .is("Ben".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_ne!(shared.revision(), before, "the commit moved the layer");
        assert_eq!(
            committed(&shared, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "a write placed on one scope lands in the layer once"
        );
        assert_eq!(
            committed(&shared, &operator, "persona/petname", &doc).await?,
            vec![Value::String("Ben".into())],
            "and a write placed on the other too"
        );
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())]
        );
        assert_eq!(
            values::<String>(&stack, &operator, "persona/petname", &doc).await?,
            vec![Value::String("Ben".into())]
        );
        assert!(
            stack.divergences().is_empty(),
            "neither read diverges from its placement"
        );

        let head = Head::Tree(shared.revision()).bytes();
        let links = top.scan(&ArtifactSelector::new().the("dialog.link/revision".parse()?));
        assert_eq!(links.len(), 2, "the top holds a link for each scope");
        for link in &links {
            assert_eq!(
                link.is,
                Value::Bytes(head.clone()),
                "each names the new head"
            );
        }
        let space = link_entity(
            &top.entity().clone(),
            &stack.identities()[0],
            &name("space"),
        );
        let persona = link_entity(
            &top.entity().clone(),
            &stack.identities()[0],
            &name("persona"),
        );
        assert_ne!(space, persona, "the two links are two entities");

        // The links read back as two: a new stack opened from the top
        // binds both scopes.
        let reopened = Stack::open(top.clone()).perform(&operator).await?;
        assert_eq!(reopened.layers().len(), 2);
        for scope in ["space", "persona"] {
            assert_eq!(reopened.scope(&name(scope)).len(), 1, "{scope} binds again");
        }
        assert_eq!(
            values::<String>(&reopened, &operator, "persona/petname", &doc).await?,
            vec![Value::String("Ben".into())]
        );
        Ok(())
    }

    /// A layer links another under two names with one access: read-only
    /// under one and writable under the other is refused.
    #[dialog_common::test]
    async fn it_refuses_two_accesses_for_one_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let top = Ephemeral::create().perform(&operator).await;
        let refused = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .link_read_only(&top, &shared, name("other"))
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(StackError::MixedAccess { .. })),
            "one layer holds one access: {refused:?}"
        );
        Ok(())
    }

    /// A link must name a layer already beneath the linking one.
    #[dialog_common::test]
    async fn it_rejects_a_link_to_an_unknown_line() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let elsewhere = Ephemeral::create().perform(&operator).await;

        let top = Ephemeral::create().perform(&operator).await;
        let result = Stack::open(top.clone())
            .link(&elsewhere, &shared, name("state"))
            .perform(&operator)
            .await;
        assert!(
            matches!(result, Err(StackError::UnknownLayer { .. })),
            "expected an unknown-layer refusal, got {result:?}"
        );
        Ok(())
    }

    /// Identities are a pure function of shape: two stacks over the
    /// same branch agree on its identity, and linking changes the
    /// encloser's identity but not the enclosed layer's.
    #[dialog_common::test]
    async fn it_derives_identities_from_shape() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let alone = Stack::open(shared.clone()).perform(&operator).await?;
        let over = Stack::open(state.clone())
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        assert_eq!(
            alone.identities()[0],
            over.identities()[0],
            "the bottom's identity is the same in every stack"
        );
        assert_ne!(over.identities()[1], over.identities()[0]);
        assert!(over.identity().to_string().starts_with("stack:"));

        let before = over.identity();
        over.transaction()
            .unlink(&state, &shared, name("shared"))
            .link(&state, &shared, name("base"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_ne!(
            over.identity(),
            before,
            "a layer name is part of the encloser's shape"
        );
        Ok(())
    }

    /// A name bound by two links from one layer fans a write out to
    /// both layers, and the composite read dedups the fact.
    #[dialog_common::test]
    async fn it_fans_out_a_name_bound_twice() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (left, right) = (
            Ephemeral::create().perform(&operator).await,
            Ephemeral::create().perform(&operator).await,
        );

        shared
            .transaction()
            .assert(Placement::new("ui/cursor".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &left, name("state"))
            .link(&top, &right, name("state"))
            .link(&left, &shared, name("shared"))
            .link(&right, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(dialog_query::the!("ui/cursor").of(doc.clone()).is(3u64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let cursor = ArtifactSelector::new().the("ui/cursor".parse()?);
        assert_eq!(left.scan(&cursor).len(), 1);
        assert_eq!(right.scan(&cursor).len(), 1);
        assert_eq!(
            values::<u64>(&stack, &operator, "ui/cursor", &doc).await?,
            vec![Value::UnsignedInt(3)],
            "one fact in two layers reads as one row"
        );
        Ok(())
    }

    /// A rule head under `last` placed on an ephemeral layer succeeds the
    /// row it concluded before, as a transaction's own `last` write to the
    /// layer does: an increment run three times leaves one count, not four.
    #[dialog_common::test]
    async fn it_succeeds_a_rule_concluded_row_of_an_ephemeral_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let increment: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger", "pick": "last:" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": { "counter": { "the": "cmd.increment/counter", "as": "Entity" } }
                    },
                    "where": { "counter": { "?": { "name": "this" } } }
                },
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger", "pick": "last:" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "with": 1,
                        "is": { "?": { "name": "count" } }
                    }
                }
            ]
        }))?;
        shared
            .transaction()
            .assert(Placement::new("counter/count".parse()?, name("state")))
            .assert(Placement::new(
                "cmd.increment/counter".parse()?,
                name("state"),
            ))
            .assert(crate::TransientAttribute::new(
                "cmd.increment/counter".parse()?,
            ))
            .assert(increment)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let counter: Entity = "ctr:1".parse()?;
        let mut zero = Changes::new();
        zero.associate(
            "counter/count".parse()?,
            counter.clone(),
            Value::UnsignedInt(0),
            Pick::Last,
        );
        stack
            .transaction()
            .assert(zero)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        for index in 0..3u32 {
            stack
                .transaction()
                .assert(
                    dialog_query::the!("cmd.increment/counter")
                        .of(format!("cmd:{index}").parse::<Entity>()?)
                        .is(counter.clone()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }

        let held: Vec<Value> = state
            .scan(&ArtifactSelector::new().the("counter/count".parse()?))
            .into_iter()
            .map(|fact| fact.is)
            .collect();
        assert_eq!(
            held,
            vec![Value::UnsignedInt(3)],
            "each conclusion succeeds the one before"
        );
        Ok(())
    }

    /// A stack whose bottom is not a branch has nowhere for
    /// declarations or undeclared attributes to go: read-only.
    #[dialog_common::test]
    async fn it_refuses_to_transact_without_a_branch_bottom() -> Result<()> {
        let (operator, _peer) = test_session_with_peer().await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone()).perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        let result = stack
            .transaction()
            .assert(dialog_query::the!("doc/title").of(doc).is("x".to_string()))
            .commit()
            .perform(&operator)
            .await;
        assert!(matches!(
            result,
            Err(StackError::Commit(CommitError::Detached))
        ));
        Ok(())
    }

    /// The increment rule the induction tests use: `assert!
    /// counter{count: ?prev + 1} when increment{counter: ?this},
    /// counter{this: ?this, count: ?prev}`.
    fn increment_rule() -> dialog_query::InductiveRule {
        serde_json::from_value(serde_json::json!({
            "description": "Increment a counter on an increment command",
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "counter": { "the": "cmd.increment/counter", "as": "Entity" }
                        }
                    },
                    "where": {
                        "counter": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "count": { "the": "counter/count", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": "math/sum",
                    "where": {
                        "of": { "?": { "name": "prev" } },
                        "with": 1,
                        "is": { "?": { "name": "count" } }
                    }
                }
            ]
        }))
        .expect("increment rule compiles")
    }

    /// A rule committed on an upper branch layer fires in a stack
    /// transaction: the command dispatched through the stack reaches
    /// it, and its durable conclusion routes by placement to the
    /// bottom, where the counter lives.
    #[dialog_common::test]
    async fn it_fires_a_rule_committed_on_an_upper_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;

        let counter: Entity = "ctr:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        local
            .transaction()
            .assert(increment_rule())
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;

        let stack = Stack::open(local.clone())
            .link(&local, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let command: Entity = "cmd:1".parse()?;
        stack
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of(command)
                    .is(counter.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        assert_eq!(
            committed(&shared, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(2)],
            "the upper layer's rule must fire and its conclusion land on the bottom"
        );
        Ok(())
    }

    /// A rule held in an ephemeral layer fires in a stack transaction
    /// too: the layer is part of the view a rule body reads, so it is
    /// part of the slice dispatch discovers rules in.
    #[dialog_common::test]
    async fn it_fires_a_rule_held_in_an_ephemeral_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let counter: Entity = "ctr:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let mut rule = Changes::new();
        increment_rule().assert(&mut rule);
        state.apply(rule).unwrap();

        let stack = Stack::open(state.clone())
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let command: Entity = "cmd:1".parse()?;
        stack
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of(command)
                    .is(counter.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        assert_eq!(
            committed(&shared, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(2)],
            "the ephemeral layer's rule must fire and its conclusion land on the bottom"
        );
        Ok(())
    }

    /// A transient whose attribute is placed on an ephemeral scope is
    /// witnessed on that layer, not on the bottom's session store; the
    /// rule it fires still lands its conclusion by placement.
    #[dialog_common::test]
    async fn it_witnesses_a_placed_transient_on_its_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;

        let counter: Entity = "ctr:1".parse()?;
        shared
            .transaction()
            .assert(Placement::new(
                "cmd.increment/counter".parse()?,
                name("state"),
            ))
            .assert(increment_rule())
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&state, &shared, name("shared"))
            .link(&top, &state, name("state"))
            .perform(&operator)
            .await?;

        let commands = || {
            let demand = Demand::new();
            demand.record(
                &ArtifactSelector::new().the("cmd.increment/counter".parse().unwrap()),
                &Manifest::default(),
            );
            demand
        };
        let on_state = state.observe(commands());
        let on_bottom = shared.overlay().observe(commands());
        let command: Entity = "cmd:1".parse()?;
        stack
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of(command)
                    .is(counter.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let Drained::Instants(witnessed) = on_state.drain() else {
            panic!("gapped")
        };
        assert_eq!(
            witnessed
                .iter()
                .flat_map(|instant| instant.asserted.iter())
                .map(|fact| fact.the.to_string())
                .collect::<Vec<_>>(),
            vec!["cmd.increment/counter".to_string()],
            "the placed command is witnessed on its layer"
        );
        assert!(
            matches!(on_bottom.drain(), Drained::Instants(instants) if instants.is_empty()),
            "and not on the bottom's session store"
        );
        assert!(
            state
                .scan(&ArtifactSelector::new().the("cmd.increment/counter".parse()?))
                .is_empty(),
            "witnessed, not held"
        );
        assert_eq!(
            committed(&shared, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(2)]
        );
        Ok(())
    }

    /// A stack reopens from its top layer alone: the links it recorded
    /// resolve every layer beneath it through the environment — the
    /// branch by repository and name, the ephemeral layer by the
    /// address the process still holds it under — and the shape found
    /// carries the identities the links recorded.
    #[dialog_common::test]
    async fn it_reopens_a_stack_from_its_top_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let tab = Ephemeral::create().perform(&operator).await;

        let built = Stack::open(tab.clone())
            .link(&tab, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        built
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let reopened = Stack::open(tab.clone()).perform(&operator).await?;
        assert_eq!(reopened.identities(), built.identities());
        assert_eq!(reopened.layers().len(), 3);
        assert!(reopened.layers()[0].same(&Layer::Branch(shared.clone())));
        assert!(reopened.layers()[1].same(&Layer::Ephemeral(state.clone())));
        assert_eq!(
            values::<String>(&reopened, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the reopened stack reads the same composite"
        );

        drop(state);
        drop(built);
        drop(reopened);
        let gone = Stack::open(tab.clone()).perform(&operator).await;
        assert!(
            matches!(gone, Err(StackError::Ephemeral(_))),
            "an ephemeral layer no handle holds cannot be reopened: {gone:?}"
        );
        Ok(())
    }

    /// Swapping a seed branch is one transaction: unlink the old one,
    /// link the new one under the same name. The old branch leaves the
    /// stack, its facts leave the composite, the new one's arrive, and
    /// the encloser's identity changes with its shape.
    #[dialog_common::test]
    async fn it_swaps_a_linked_branch_by_unlinking_and_linking() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let main = repo.branch("main").open().perform(&operator).await?;
        let seed_v1 = repo.branch("seed.v1").open().perform(&operator).await?;
        let seed_v2 = repo.branch("seed.v2").open().perform(&operator).await?;
        let doc: Entity = "seed:doc".parse()?;
        for (seed, version) in [(&seed_v1, "one"), (&seed_v2, "two")] {
            seed.transaction()
                .assert(
                    dialog_query::the!("seed/version")
                        .of(doc.clone())
                        .is(version.to_string()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            seed.refresh(&operator).await?;
        }

        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &main, name("main"))
            .link(&state, &seed_v1, name("seed"))
            .perform(&operator)
            .await?;
        let before = stack.identity();
        assert_eq!(
            values::<String>(&stack, &operator, "seed/version", &doc).await?,
            vec![Value::String("one".into())]
        );
        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("seed/version"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        ));
        subscription.poll(&operator).await?.expect("initial");

        stack
            .transaction()
            .unlink(&state, &seed_v1, name("seed"))
            .link(&state, &seed_v2, name("seed"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stack.layers().len(), 4, "the old seed left");
        assert!(
            stack
                .layers()
                .iter()
                .all(|layer| !layer.same(&Layer::Branch(seed_v1.clone()))),
        );
        assert_eq!(
            values::<String>(&stack, &operator, "seed/version", &doc).await?,
            vec![Value::String("two".into())],
            "the composite reads the new seed"
        );
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("a subscription sees the swap");
        assert_eq!(
            (delta.asserted.len(), delta.retracted.len()),
            (1, 1),
            "the new seed's value replaces the old one's"
        );
        assert_ne!(stack.identity(), before, "the shape changed");
        let link = ArtifactSelector::new().the("dialog.link/name".parse()?);
        assert_eq!(
            state.scan(&link).len(),
            2,
            "the old link's facts are gone from the encloser"
        );

        assert_eq!(
            top.scan(&link).len(),
            1,
            "the old ancestor link is retracted"
        );
        let reopened = Stack::open(top.clone()).perform(&operator).await?;
        assert_eq!(reopened.identities(), stack.identities());

        stack
            .transaction()
            .unlink(&top, &state, name("state"))
            .unlink(&state, &seed_v2, name("seed"))
            .link(&top, &main, name("main"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(stack.layers().len(), 2);
        let reopened = Stack::open(top.clone()).perform(&operator).await?;
        assert_eq!(reopened.identities(), stack.identities());
        Ok(())
    }

    /// An asset asserted through a stack is stored by the bottom tree
    /// layer, whatever layer the facts referencing it route to: the
    /// bottom records its size fact and serves its bytes, and the
    /// ephemeral layer above holds no trace of it.
    #[dialog_common::test]
    async fn it_stores_assets_on_the_bottom() -> Result<()> {
        use crate::Blob;
        use dialog_artifacts::Asset;

        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let payload: Vec<u8> = (0..20_000u32).map(|i| (i % 241) as u8).collect();
        let photo = Asset::from(payload.clone());
        let content = photo.entity()?;
        let doc: Entity = "doc:1".parse()?;
        // The referencing fact lands on the ephemeral layer; the asset
        // cannot, and goes to the bottom.
        stack
            .transaction()
            .assert(Placement::new("doc/cover".parse()?, name("state")))
            .assert(photo.clone())
            .assert(
                dialog_query::the!("doc/cover")
                    .of(doc.clone())
                    .is(content.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        assert_eq!(
            values::<Entity>(&stack, &operator, "doc/cover", &doc).await?,
            vec![Value::Entity(content.clone())],
            "the referencing fact reads through the stack"
        );
        assert!(
            committed(&shared, &operator, "doc/cover", &doc)
                .await?
                .is_empty(),
            "and lives on the ephemeral layer, not the tree"
        );
        assert_eq!(
            committed(&shared, &operator, "dialog.asset/size", &content).await?,
            vec![Value::UnsignedInt(payload.len() as u128)],
            "the bottom records the asset"
        );
        assert!(
            state
                .scan(&ArtifactSelector::new().of(content.clone()))
                .is_empty(),
            "the ephemeral layer holds nothing about it"
        );
        let mut reader = Blob::from(content)
            .read((&shared).into())
            .perform(&operator)
            .await?;
        let mut bytes = Vec::new();
        while let Some(chunk) = reader.next().await? {
            bytes.extend(chunk);
        }
        assert_eq!(bytes, payload, "the bottom serves the bytes");
        Ok(())
    }

    /// A placement the bottom branch's session store holds routes a
    /// stack commit like a committed one: the writer's own declaration
    /// sends the fact to the layer under the scope, and the tree holds
    /// neither the fact nor the declaration.
    #[dialog_common::test]
    async fn it_routes_by_a_placement_held_in_the_bottoms_session_store() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        shared
            .overlay()
            .assert(Placement::new("site/path".parse()?, name("state")))?;

        let site: Entity = "site:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("site/path")
                    .of(site.clone())
                    .is("/a".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            values::<String>(&stack, &operator, "site/path", &site).await?,
            vec![Value::String("/a".into())]
        );
        assert_eq!(
            state.scan(&ArtifactSelector::new().of(site.clone())).len(),
            1,
            "the fact landed on the layer the scope names"
        );
        assert!(
            committed(&shared, &operator, "site/path", &site)
                .await?
                .is_empty(),
            "the tree never sees the fact"
        );
        let placement: Entity = crate::attribute_entity(&"site/path".parse()?);
        assert!(
            committed(&shared, &operator, "dialog.attribute/scope", &placement)
                .await?
                .is_empty(),
            "nor the declaration"
        );
        Ok(())
    }

    /// An `org/salary` claim under `pick`, as a stack or a session writes
    /// it.
    fn salary(of: &Entity, value: u32, pick: Option<Pick>) -> AttributeStatement {
        AttributeStatement {
            the: Relation::from(
                "org/salary"
                    .parse::<dialog_artifacts::Relation>()
                    .expect("an attribute"),
            ),
            of: of.clone(),
            is: Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(dialog_query::Cardinality::One),
            pick,
        }
    }

    /// A stack over `shared`, with an ephemeral top linking it.
    async fn over_shared(operator: &impl StackSyncEnv, shared: &Branch) -> Result<Stack> {
        let top = Ephemeral::create().perform(operator).await;
        Ok(Stack::open(top.clone())
            .link(&top, shared, name("shared"))
            .perform(operator)
            .await?)
    }

    /// A write through a stack settles its succession as a branch commit
    /// does: under `last`, the read of the cell elects the row the session
    /// store of the bottom holds, so the write retires no stored claim.
    #[dialog_common::test]
    async fn it_settles_a_last_write_over_a_session_held_cell() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        shared
            .transaction()
            .assert(salary(&alice, 300, Some(Pick::Last)))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        shared.overlay().assert(salary(&alice, 500, None))?;
        let stack = over_shared(&operator, &shared).await?;

        stack
            .transaction()
            .assert(salary(&alice, 400, Some(Pick::Last)))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let mut stored: Vec<Value> = committed(&shared, &operator, "org/salary", &alice).await?;
        stored.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            stored,
            vec![Value::UnsignedInt(300), Value::UnsignedInt(400)],
            "the read elected the session row, so no stored claim is succeeded"
        );
        Ok(())
    }

    /// The row of a standalone ephemeral layer is a candidate that a read
    /// elects, but no claim, as a row of the session store is: under
    /// `last`, a write whose read elects it retires no stored claim.
    #[dialog_common::test]
    async fn it_settles_a_last_write_over_a_cell_an_ephemeral_layer_holds() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        shared
            .transaction()
            .assert(salary(&alice, 300, Some(Pick::Last)))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let top = Ephemeral::create().perform(&operator).await;
        let mut held = Changes::new();
        salary(&alice, 500, None).assert(&mut held);
        top.apply(held)?;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;

        stack
            .transaction()
            .assert(salary(&alice, 400, Some(Pick::Last)))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let mut stored: Vec<Value> = committed(&shared, &operator, "org/salary", &alice).await?;
        stored.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            stored,
            vec![Value::UnsignedInt(300), Value::UnsignedInt(400)],
            "the read elected the row of the ephemeral layer, so no stored claim is succeeded"
        );
        Ok(())
    }

    /// A `last` write that lands in an ephemeral layer succeeds the row the
    /// layer holds of the cell: the layer elects among its own rows.
    #[dialog_common::test]
    async fn it_succeeds_the_row_of_an_ephemeral_layer_it_writes() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("org/salary".parse()?, name("state")))?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let alice: Entity = "id:alice".parse()?;
        for value in [400, 300] {
            stack
                .transaction()
                .assert(salary(&alice, value, Some(Pick::Last)))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        let held: Vec<Value> = state
            .scan(&ArtifactSelector::new().the("org/salary".parse()?).of(alice))
            .into_iter()
            .map(|fact| fact.is)
            .collect();
        assert_eq!(held, vec![Value::UnsignedInt(300)]);
        Ok(())
    }

    /// Under `max`, a write through a stack leaves a candidate that a rule
    /// derives, which the read still elects, and succeeds no stored claim.
    #[dialog_common::test]
    async fn it_settles_a_max_write_beside_a_derived_candidate() -> Result<()> {
        use dialog_query::rule::DeductiveRuleDescriptor;

        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        let descriptor: DeductiveRuleDescriptor = serde_json::from_value(serde_json::json!({
            "deduce": { "with": {
                "salary": { "the": "org/salary", "as": "natural:" }
            }},
            "when": [{
                "assert": { "with": {
                    "bonus": { "the": "org/bonus", "as": "natural:" }
                }},
                "where": {
                    "this": { "?": { "name": "this" } },
                    "bonus": { "?": { "name": "salary" } }
                }
            }]
        }))?;
        let rule = descriptor.compile()?;
        shared
            .transaction()
            .assert(&rule)
            .assert(salary(&alice, 100, Some(Pick::Max)))
            .assert(dialog_query::the!("org/bonus").of(alice.clone()).is(500u32))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let stack = over_shared(&operator, &shared).await?;

        stack
            .transaction()
            .assert(salary(&alice, 200, Some(Pick::Max)))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let mut stored: Vec<Value> = committed(&shared, &operator, "org/salary", &alice).await?;
        stored.sort_by_key(|value| format!("{value:?}"));
        assert_eq!(
            stored,
            vec![Value::UnsignedInt(100), Value::UnsignedInt(200)],
            "the read elected the derived candidate, so no stored claim is succeeded"
        );
        Ok(())
    }

    /// A commit that extends a staged chain, and retracts a fact that only
    /// the session store of the bottom holds, takes the fact from the
    /// store once the chain publishes, as the first commit of a chain
    /// does.
    #[dialog_common::test]
    async fn it_retracts_a_session_row_in_a_later_commit_of_a_chain() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        shared.overlay().assert(
            dialog_query::the!("doc/title")
                .of(doc.clone())
                .is("draft".to_string()),
        )?;
        let stack = over_shared(&operator, &shared).await?;

        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/body")
                    .of(doc.clone())
                    .is("text".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        stack
            .transaction()
            .retract(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("draft".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        stack.publish(&operator).await?;

        assert!(
            values::<String>(&stack, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "the session row does not come back"
        );
        Ok(())
    }

    /// The query of a stack transaction reads its pending writes the same
    /// way whether it selects or subscribes: a write under `max` settles
    /// its succession in both.
    #[dialog_common::test]
    async fn it_subscribes_to_a_transaction_as_it_selects() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let alice: Entity = "id:alice".parse()?;
        shared
            .transaction()
            .assert(salary(&alice, 500, Some(Pick::Max)))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let stack = over_shared(&operator, &shared).await?;
        let transaction = stack
            .transaction()
            .assert(salary(&alice, 400, Some(Pick::Max)));
        let predicate: dialog_query::ConceptDescriptor =
            serde_json::from_value(serde_json::json!({ "with": {
                "salary": { "the": "org/salary", "as": "natural:", "pick": "max:" }
            }}))?;
        let mut terms = dialog_query::Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("salary".into(), Term::var("salary"));
        let query = dialog_query::ConceptQuery { predicate, terms };
        let salaries = |rows: &[dialog_query::ConceptConclusion]| -> Result<Vec<u64>> {
            Ok(rows
                .iter()
                .map(|row| row.get::<u64>("salary"))
                .collect::<Result<_, _>>()?)
        };

        let selected: Vec<dialog_query::ConceptConclusion> = transaction
            .query()
            .select(query.clone())
            .perform(&operator)
            .try_vec()
            .await?;
        let mut subscription = transaction.query().subscribe(query);
        let subscribed = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(salaries(&subscribed.asserted)?, salaries(&selected)?);
        Ok(())
    }

    /// The rules of a library in JSON, as a host holds them beside a
    /// stack: `test.row/shared` gets a candidate for each pair a row is
    /// in, and the candidate 3 for each row; `test.row/suffix` reads the
    /// greatest candidate under the pick `max`, and adds one; and
    /// `test.row/shown` holds each row that `unless` does not hide.
    fn picked_rules() -> Result<Vec<dialog_query::DeductiveRule>> {
        let rules = serde_json::json!([
            {
                "deduce": { "with": {
                    "shared": { "the": "test.row/shared", "as": "natural:", "pick": "all:" }
                }},
                "when": [{
                    "assert": { "with": {
                        "row": { "the": "test.pair/row", "as": "entity:" },
                        "length": { "the": "test.pair/length", "as": "natural:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "pair" } },
                        "row": { "?": { "name": "this" } },
                        "length": { "?": { "name": "shared" } }
                    }
                }]
            },
            {
                "deduce": { "with": {
                    "shared": { "the": "test.row/shared", "as": "natural:", "pick": "all:" }
                }},
                "when": [
                    {
                        "assert": { "with": {
                            "name": { "the": "test.row/name", "as": "text:" }
                        }},
                        "where": {
                            "this": { "?": { "name": "this" } },
                            "name": { "?": { "name": "name" } }
                        }
                    },
                    { "assert": "==", "where": { "this": { "?": { "name": "shared" } }, "is": 3 } }
                ]
            },
            {
                "deduce": { "with": {
                    "suffix": { "the": "test.row/suffix", "as": "natural:" }
                }},
                "when": [
                    {
                        "assert": { "with": {
                            "shared": { "the": "test.row/shared", "as": "natural:", "pick": "max:" }
                        }},
                        "where": {
                            "this": { "?": { "name": "this" } },
                            "shared": { "?": { "name": "shared" } }
                        }
                    },
                    {
                        "assert": "math/sum",
                        "where": {
                            "of": { "?": { "name": "shared" } },
                            "with": 1,
                            "is": { "?": { "name": "suffix" } }
                        }
                    }
                ]
            },
            {
                "deduce": { "with": {
                    "name": { "the": "test.row/shown", "as": "text:" }
                }},
                "when": [{
                    "assert": { "with": {
                        "name": { "the": "test.row/name", "as": "text:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "name": { "?": { "name": "name" } }
                    }
                }],
                "unless": [{
                    "assert": { "with": {
                        "hidden": { "the": "test.row/hidden", "as": "boolean:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "hidden": true
                    }
                }]
            }
        ]);
        let descriptors: Vec<DeductiveRuleDescriptor> = serde_json::from_value(rules)?;
        Ok(descriptors
            .into_iter()
            .map(|descriptor| descriptor.compile())
            .collect::<Result<_, _>>()?)
    }

    /// The query of one field of a row under `pick`.
    fn picked(the: &str, kind: &str, pick: &str) -> Result<dialog_query::ConceptQuery> {
        let predicate: dialog_query::ConceptDescriptor =
            serde_json::from_value(serde_json::json!({ "with": {
                "value": { "the": the, "as": kind, "pick": pick }
            }}))?;
        let mut terms = dialog_query::Parameters::new();
        terms.insert("this".into(), Term::var("this"));
        terms.insert("value".into(), Term::var("value"));
        Ok(dialog_query::ConceptQuery { predicate, terms })
    }

    /// The `(this, value)` of each conclusion, sorted.
    fn pairs(rows: &[dialog_query::ConceptConclusion]) -> Vec<(Entity, Value)> {
        let mut pairs: Vec<(Entity, Value)> = rows
            .iter()
            .map(|row| {
                let value = row
                    .source()
                    .lookup(&Term::var("value"))
                    .and_then(|binding| binding.content())
                    .expect("each row has a value");
                (row.entity().clone(), value)
            })
            .collect();
        pairs.sort_by_key(|pair| format!("{pair:?}"));
        pairs
    }

    /// Two rows, a pair of length 7 for the first, and the second hidden.
    async fn picked_rows(stack: &Stack, operator: &impl StackSyncEnv) -> Result<(Entity, Entity)> {
        let (first, second): (Entity, Entity) = ("row:1".parse()?, "row:2".parse()?);
        stack
            .transaction()
            .assert(
                dialog_query::the!("test.row/name")
                    .of(first.clone())
                    .is("Garden".to_string()),
            )
            .assert(
                dialog_query::the!("test.row/name")
                    .of(second.clone())
                    .is("Garden".to_string()),
            )
            .assert(
                dialog_query::the!("test.row/hidden")
                    .of(second.clone())
                    .is(true),
            )
            .assert(
                dialog_query::the!("test.pair/row")
                    .of("pair:1".parse::<Entity>()?)
                    .is(first.clone()),
            )
            .assert(
                dialog_query::the!("test.pair/length")
                    .of("pair:1".parse::<Entity>()?)
                    .is(7u32),
            )
            .commit()
            .publish()
            .perform(operator)
            .await?;
        Ok((first, second))
    }

    /// A field that reads its candidates under a pick keeps the pick
    /// through JSON, and the rules of a library in JSON, which the
    /// overlay of a query brings, answer in that query: the greatest
    /// candidate, a rule over it, and the rows that `unless` keeps.
    #[dialog_common::test]
    async fn it_answers_a_pick_and_an_unless_from_rules_in_json() -> Result<()> {
        let query = picked("test.row/shared", "natural:", "max:")?;
        let json = serde_json::to_value(&query.predicate)?;
        assert_eq!(
            json["with"]["value"]["pick"],
            serde_json::json!("max:"),
            "the pick is written"
        );
        let back: dialog_query::ConceptDescriptor = serde_json::from_value(json)?;
        assert_eq!(back, query.predicate, "and read back");

        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let stack = over_shared(&operator, &shared).await?;
        let (first, second) = picked_rows(&stack, &operator).await?;
        let select = |query: dialog_query::ConceptQuery| {
            let mut layer = stack.query();
            for rule in picked_rules().expect("the rules compile") {
                layer = layer.with(rule);
            }
            layer.select(query).perform(&operator).try_vec()
        };
        assert_eq!(
            pairs(&select(picked("test.row/shared", "natural:", "max:")?).await?),
            vec![
                (first.clone(), Value::UnsignedInt(7)),
                (second.clone(), Value::UnsignedInt(3)),
            ],
            "the greatest candidate of each row"
        );
        assert_eq!(
            pairs(&select(picked("test.row/suffix", "natural:", "last:")?).await?),
            vec![
                (first.clone(), Value::UnsignedInt(8)),
                (second.clone(), Value::UnsignedInt(4)),
            ],
            "a rule reads the greatest candidate"
        );
        assert_eq!(
            pairs(&select(picked("test.row/shown", "text:", "last:")?).await?),
            vec![(first.clone(), Value::String("Garden".into()))],
            "unless hides the second row"
        );
        assert!(
            pairs(
                &stack
                    .query()
                    .select(picked("test.row/suffix", "natural:", "last:")?)
                    .perform(&operator)
                    .try_vec()
                    .await?
            )
            .is_empty(),
            "no rule reaches a query that does not bring it"
        );
        Ok(())
    }

    /// An inductive rule of a dispatch reads the greatest of the
    /// candidates that the dispatched rules of a library in JSON derive.
    #[dialog_common::test]
    async fn it_reads_a_pick_in_a_dispatch() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let stack = over_shared(&operator, &shared).await?;
        let (first, _) = picked_rows(&stack, &operator).await?;
        let record: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": { "with": {
                "length": { "the": "test.picked/length", "as": "natural:" }
            }},
            "when": [
                {
                    "assert": { "with": {
                        "row": { "the": "test.command/row", "as": "entity:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "command" } },
                        "row": { "?": { "name": "this" } }
                    }
                },
                {
                    "assert": { "with": {
                        "shared": { "the": "test.row/shared", "as": "natural:", "pick": "max:" }
                    }},
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "shared": { "?": { "name": "length" } }
                    }
                }
            ]
        }))?;
        shared
            .overlay()
            .assert(crate::TransientAttribute::new("test.command/row".parse()?))?;
        let mut transaction = stack.transaction().dispatch(record).dispatch(
            dialog_query::the!("test.command/row")
                .of("cmd:1".parse::<Entity>()?)
                .is(first.clone()),
        );
        for rule in picked_rules()? {
            transaction = transaction.dispatch(rule);
        }
        transaction.commit().publish().perform(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "test.picked/length", &first).await?,
            vec![Value::UnsignedInt(7)],
            "the inductive rule read the greatest candidate"
        );
        Ok(())
    }

    /// A subscription over a field that reads derived candidates under
    /// `max` sees a new answer once a commit adds a greater candidate.
    #[dialog_common::test]
    async fn it_subscribes_to_a_pick_over_rules_in_json() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let stack = over_shared(&operator, &shared).await?;
        let (first, _) = picked_rows(&stack, &operator).await?;
        let mut layer = stack.query();
        for rule in picked_rules()? {
            layer = layer.with(rule);
        }
        let mut subscription = layer.subscribe(picked("test.row/shared", "natural:", "max:")?);
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(
            pairs(&initial.asserted).contains(&(first.clone(), Value::UnsignedInt(7))),
            "the greatest candidate first"
        );
        stack
            .transaction()
            .assert(
                dialog_query::the!("test.pair/row")
                    .of("pair:2".parse::<Entity>()?)
                    .is(first.clone()),
            )
            .assert(
                dialog_query::the!("test.pair/length")
                    .of("pair:2".parse::<Entity>()?)
                    .is(11u32),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let delta = subscription.poll(&operator).await?.expect("a new answer");
        assert_eq!(
            pairs(&delta.asserted),
            vec![(first.clone(), Value::UnsignedInt(11))],
            "the new greatest candidate"
        );
        assert_eq!(
            pairs(&delta.retracted),
            vec![(first, Value::UnsignedInt(7))],
            "in place of the old one"
        );
        Ok(())
    }

    /// A stack refuses a NaN, as a branch commit does.
    #[dialog_common::test]
    async fn it_refuses_a_nan_through_a_stack() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let stack = over_shared(&operator, &shared).await?;
        let refused = stack
            .transaction()
            .assert(
                dialog_query::the!("org/ratio")
                    .of("id:alice".parse::<Entity>()?)
                    .is(f64::NAN),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(matches!(refused, Err(StackError::Commit(_))), "{refused:?}");
        Ok(())
    }

    /// A commit that writes nothing to the bottom still settles the lag of
    /// the bottom once it publishes: a rule fires on what a raw commit
    /// brought once, not again at each later commit.
    #[dialog_common::test]
    async fn it_fires_once_over_the_lag_of_the_bottom() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        // A rule that answers a result with a notice, witnessed on the
        // application layer each time it fires.
        let notice: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "status": { "the": "notice/seen", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "status": { "the": "result/status", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "status": { "?": { "name": "status" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(notice)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        for attribute in ["notice/seen", "app/note"] {
            shared
                .overlay()
                .assert(Placement::new(attribute.parse()?, name("application")))?;
        }
        shared
            .overlay()
            .assert(crate::TransientAttribute::new("notice/seen".parse()?))?;
        let application = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let observer = application.observe_everything();
        let notices = || match observer.drain() {
            crate::Drained::Instants(instants) => instants
                .iter()
                .filter(|instant| {
                    instant.transient
                        && instant
                            .asserted
                            .iter()
                            .any(|fact| fact.the.as_str() == "notice/seen")
                })
                .count(),
            crate::Drained::Gap { .. } => panic!("no gap"),
        };

        // A raw commit, which runs no rule, brings a result to the bottom.
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "result/status".parse()?,
                    of: "cmd:1".parse()?,
                    is: Value::String("done".into()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(&operator)
            .await?;
        stack.advance(&operator).await?;
        notices();

        let mut fired = Vec::new();
        for index in 0..3u32 {
            stack
                .transaction()
                .assert(
                    dialog_query::the!("app/note")
                        .of(format!("note:{index}").parse::<Entity>()?)
                        .is("hi".to_string()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            fired.push(notices());
        }
        assert_eq!(fired, vec![1, 0, 0], "the rule fires once over the lag");
        Ok(())
    }

    /// A stack over `shared`, with the ephemeral layer `state` linked
    /// above it under `memory:state`, and `cmd.increment/counter` placed
    /// on that scope by the session store of the bottom. The increment
    /// rule is held in `state`, so only the stack's commits run it, and
    /// the counter starts at 1.
    async fn guided_stack(
        operator: &impl StackSyncEnv,
        shared: &Branch,
    ) -> Result<(Stack, Ephemeral, Entity)> {
        let counter: Entity = "ctr:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
            .perform(operator)
            .await?;
        shared.refresh(operator).await?;
        let state = Ephemeral::create().perform(operator).await;
        let mut rule = Changes::new();
        increment_rule().assert(&mut rule);
        state.apply(rule).unwrap();
        shared.overlay().assert(Placement::new(
            "cmd.increment/counter".parse()?,
            name("state"),
        ))?;
        let top = Ephemeral::create().perform(operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, shared, name("shared"))
            .perform(operator)
            .await?;
        Ok((stack, state, counter))
    }

    /// A fact placed on one scope and found in the layer of another
    /// reaches no query and no rule, and the stack reports it once.
    /// Here a raw commit through another handle of the bottom, which
    /// holds no placement, brings a command that belongs to the ephemeral
    /// layer.
    #[dialog_common::test]
    async fn it_reads_no_fact_that_a_commit_put_outside_its_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, _, counter) = guided_stack(&operator, &shared).await?;
        // The scope `memory:state` names the ephemeral layer, not the
        // bottom.
        let mut layers = stack.layers();
        let bottom = layers.remove(0).address_entity();

        // A raw commit through another handle: it runs no rule and
        // knows no placement, so the fact waits in the induction lag of
        // the bottom until the next commit of the stack.
        let other = repo.branch("main").open().perform(&operator).await?;
        let command: Entity = "cmd:1".parse()?;
        other
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "cmd.increment/counter".parse()?,
                    of: command.clone(),
                    is: Value::Entity(counter.clone()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(&operator)
            .await?;
        assert_eq!(
            committed(&other, &operator, "cmd.increment/counter", &command)
                .await?
                .len(),
            1,
            "the bottom holds the misplaced fact"
        );
        stack.advance(&operator).await?;

        assert!(
            values::<Entity>(&stack, &operator, "cmd.increment/counter", &command)
                .await?
                .is_empty(),
            "no query reads it"
        );
        // A commit of the stack catches up with what entered the bottom:
        // the misplaced command fires no rule.
        stack
            .transaction()
            .assert(
                dialog_query::the!("note/text")
                    .of("note:1".parse::<Entity>()?)
                    .is("hello".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(1)],
            "the rule did not fire on the misplaced command"
        );
        assert_eq!(
            stack.divergences(),
            vec![Divergence {
                attribute: "cmd.increment/counter".into(),
                layer: bottom,
                scope: Some(name("state")),
            }],
            "the divergence is reported once, with the attribute and the layer"
        );
        assert!(stack.divergences().is_empty(), "and taken once");
        Ok(())
    }

    /// The same holds for a fact that a pull brings: a replica with no
    /// placement commits a command, the bottom pulls it, and neither a
    /// query nor a subscription of the stack reads it.
    #[dialog_common::test]
    async fn it_reads_no_fact_that_a_pull_put_outside_its_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, _, counter) = guided_stack(&operator, &shared).await?;

        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("cmd.increment/counter"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<Entity>::var("v")),
        ));
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty());

        let foreign = repo.branch("foreign").open().perform(&operator).await?;
        foreign.pull().from(&shared).perform(&operator).await?;
        let command: Entity = "cmd:1".parse()?;
        foreign
            .transaction()
            .assert(
                dialog_query::the!("cmd.increment/counter")
                    .of(command.clone())
                    .is(counter.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.pull().from(&foreign).perform(&operator).await?;
        stack.advance(&operator).await?;
        // The poll after the advance maintains the result from the diff of
        // the bottom: the misplaced fact is no change of it.
        let delta = subscription.poll(&operator).await?;
        assert!(
            delta.is_none_or(|delta| delta.asserted.is_empty()),
            "no subscription reads it"
        );
        assert_eq!(
            subscription.recomputes(),
            1,
            "the poll maintained the result"
        );
        stack
            .transaction()
            .assert(
                dialog_query::the!("note/text")
                    .of("note:1".parse::<Entity>()?)
                    .is("hello".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(1)],
            "the rule did not fire on the pulled command"
        );
        assert_eq!(stack.divergences().len(), 1);
        Ok(())
    }

    /// A fact that enters an upper ephemeral layer, outside the scope of
    /// its placement, reaches no query and no subscription, on the
    /// incremental path too, and is reported. So does one that the session
    /// store of the bottom holds.
    #[dialog_common::test]
    async fn it_reads_no_fact_that_an_upper_layer_holds_outside_its_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, state, counter) = guided_stack(&operator, &shared).await?;
        shared
            .overlay()
            .assert(Placement::new("site/path".parse()?, name("shared")))?;
        let site: Entity = "site:1".parse()?;
        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("site/path"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        ));
        subscription.poll(&operator).await?.expect("initial");

        let mut misplaced = Changes::new();
        dialog_query::the!("site/path")
            .of(site.clone())
            .is("/upper".to_string())
            .assert(&mut misplaced);
        state.apply(misplaced)?;
        let delta = subscription.poll(&operator).await?;
        assert!(
            delta.is_none_or(|delta| delta.asserted.is_empty()),
            "no subscription reads it"
        );
        assert!(
            values::<String>(&stack, &operator, "site/path", &site)
                .await?
                .is_empty()
        );
        assert_eq!(
            stack.divergences(),
            vec![Divergence {
                attribute: "site/path".into(),
                layer: state.entity().clone(),
                scope: Some(name("shared")),
            }]
        );

        // The bottom's session store is memory of the bottom: a command
        // placed on the ephemeral layer is not read from it.
        let command: Entity = "cmd:1".parse()?;
        shared.overlay().assert(
            dialog_query::the!("cmd.increment/counter")
                .of(command.clone())
                .is(counter),
        )?;
        assert!(
            values::<Entity>(&stack, &operator, "cmd.increment/counter", &command)
                .await?
                .is_empty()
        );
        Ok(())
    }

    /// A subscription evaluates afresh when the placements that guide it
    /// move: a placement in the session store of the bottom moves an
    /// attribute off the bottom, and the row it read goes.
    #[dialog_common::test]
    async fn it_reevaluates_a_subscription_when_a_placement_moves() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let site: Entity = "site:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("site/path")
                    .of(site.clone())
                    .is("/committed".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared
            .transaction()
            .assert(Placement::new("site/path".parse()?, name("shared")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("site/path"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        ));
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert_eq!(initial.asserted.len(), 1);

        shared
            .overlay()
            .assert(Placement::new("site/path".parse()?, name("state")))?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the moved placement re-evaluates the subscription");
        assert_eq!(delta.retracted.len(), 1, "the row of the bottom goes");
        Ok(())
    }

    /// A placement in the session store of the bottom guides reads, and
    /// outranks a placement that the bottom commits: the bottom commits
    /// `site/path` onto its own scope, the session store moves it to the
    /// ephemeral layer, and a fact of it in the bottom is not read.
    #[dialog_common::test]
    async fn it_reads_by_the_placement_of_the_session_store_first() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let site: Entity = "site:1".parse()?;
        // The fact first: the branch alone binds no scope but its tree,
        // so it refuses a fact placed elsewhere.
        shared
            .transaction()
            .assert(
                dialog_query::the!("site/path")
                    .of(site.clone())
                    .is("/committed".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared
            .transaction()
            .assert(Placement::new("site/path".parse()?, name("shared")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        assert_eq!(
            values::<String>(&stack, &operator, "site/path", &site).await?,
            vec![Value::String("/committed".into())],
            "the committed placement names the bottom"
        );

        shared
            .overlay()
            .assert(Placement::new("site/path".parse()?, name("state")))?;
        assert!(
            values::<String>(&stack, &operator, "site/path", &site)
                .await?
                .is_empty(),
            "the session store moves the attribute to the ephemeral layer"
        );
        assert_eq!(stack.divergences().len(), 1);
        Ok(())
    }

    /// A stack whose application layer reads `index` through a read-only
    /// link under `memory:device`, and writes `shared` under
    /// `memory:shared`. `idx/name` is placed on `memory:device` and
    /// `app/note` on `memory:application`, by the session store of the
    /// bottom. The index holds one name.
    async fn read_only_stack(
        operator: &impl StackSyncEnv,
        shared: &Branch,
        index: &Branch,
    ) -> Result<(Stack, Ephemeral, Ephemeral)> {
        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:1".parse::<Entity>()?)
                    .is("Ana".to_string()),
            )
            .commit()
            .publish()
            .perform(operator)
            .await?;
        index.refresh(operator).await?;
        shared
            .overlay()
            .assert(Placement::new("idx/name".parse()?, name("device")))?;
        shared
            .overlay()
            .assert(Placement::new("app/note".parse()?, name("application")))?;
        let application = Ephemeral::create().perform(operator).await;
        let top = Ephemeral::create().perform(operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, shared, name("shared"))
            .link_read_only(&application, index, name("device"))
            .perform(operator)
            .await?;
        Ok((stack, application, top))
    }

    /// A stack reads facts through a read-only link, and a commit records
    /// the head of the linked layer on the encloser, but never moves it.
    #[dialog_common::test]
    async fn it_reads_through_a_read_only_link_and_records_its_head() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        let (stack, application, _) = read_only_stack(&operator, &shared, &index).await?;
        let before = index.revision();

        assert_eq!(
            values::<String>(&stack, &operator, "idx/name", &"persona:1".parse()?).await?,
            vec![Value::String("Ana".into())]
        );
        stack
            .transaction()
            .assert(
                dialog_query::the!("app/note")
                    .of("note:1".parse::<Entity>()?)
                    .is("hi".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.refresh(&operator).await?;
        assert_eq!(index.revision(), before, "the stack never moves the layer");
        let recorded =
            application.scan(&ArtifactSelector::new().the("dialog.link/revision".parse()?));
        let head = Head::Tree(before).bytes();
        assert!(
            recorded
                .iter()
                .any(|fact| fact.is == Value::Bytes(head.clone())),
            "the encloser records the head it read: {recorded:?}"
        );
        // Reopened from its top, the stack keeps the link read-only.
        let identity = stack.identity();
        let reopened = Stack::open(stack.layers().pop().unwrap())
            .perform(&operator)
            .await?;
        assert_eq!(reopened.identity(), identity);
        Ok(())
    }

    /// A write placed on a read-only scope fails at commit, with an error
    /// that names the scope. No layer changes, and the ephemeral layers
    /// get no part of the commit.
    #[dialog_common::test]
    async fn it_refuses_a_write_placed_on_a_read_only_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        let (stack, application, _) = read_only_stack(&operator, &shared, &index).await?;
        let (index_head, shared_head) = (index.revision(), shared.revision());
        let note: Entity = "note:1".parse()?;

        let refused = stack
            .transaction()
            .assert(
                dialog_query::the!("app/note")
                    .of(note.clone())
                    .is("hi".to_string()),
            )
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:2".parse::<Entity>()?)
                    .is("Eve".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await;

        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a write on a read-only scope is refused: {other:?}"),
        }
        index.refresh(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(index.revision(), index_head);
        assert_eq!(shared.revision(), shared_head);
        assert!(
            application
                .scan(&ArtifactSelector::new().of(note))
                .is_empty(),
            "the application layer got no part of the commit"
        );
        Ok(())
    }

    /// A `clear` or `forget` of a scope that the stack reads alone fails
    /// with an error that names the scope, rather than doing nothing.
    #[dialog_common::test]
    async fn it_refuses_maintenance_of_a_read_only_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let notes = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .link_read_only(&top, &notes, name("notes"))
            .perform(&operator)
            .await?;
        let refused = stack
            .transaction()
            .clear(name("notes"))
            .commit()
            .perform(&operator)
            .await;
        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("notes")),
            other => panic!("a clear of a read-only scope is refused: {other:?}"),
        }
        Ok(())
    }

    /// A write that a rule derives onto a read-only scope fails at commit,
    /// with an error that names the scope, and no layer changes.
    #[dialog_common::test]
    async fn it_refuses_a_derived_write_on_a_read_only_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        // `idx/name` of a note's entity, from the note.
        let copy: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "name": { "the": "idx/name", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "note": { "the": "app/note", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "note": { "?": { "name": "name" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(copy)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let (stack, application, _) = read_only_stack(&operator, &shared, &index).await?;
        let (index_head, shared_head) = (index.revision(), shared.revision());
        let note: Entity = "note:1".parse()?;
        let observers = [
            application.observe_everything(),
            shared.overlay().observe_everything(),
        ];

        let refused = stack
            .transaction()
            .assert(
                dialog_query::the!("app/note")
                    .of(note.clone())
                    .is("Eve".to_string()),
            )
            .dispatch(dialog_query::the!("cmd/open").of(note.clone()).is(true))
            .commit()
            .perform(&operator)
            .await;
        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a derived write on a read-only scope is refused: {other:?}"),
        }
        for observer in &observers {
            assert!(
                matches!(observer.drain(), Drained::Instants(instants) if instants.is_empty()),
                "no observer sees a round of the refused commit"
            );
        }
        index.refresh(&operator).await?;
        shared.refresh(&operator).await?;
        assert_eq!(index.revision(), index_head);
        assert_eq!(shared.revision(), shared_head);
        assert!(
            application
                .scan(&ArtifactSelector::new().of(note))
                .is_empty()
        );
        Ok(())
    }

    /// A pull merges into a layer, which is a write: `Stack::pull` leaves a
    /// layer that it reads through a read-only link at its own head.
    #[dialog_common::test]
    async fn it_pulls_no_read_only_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        let (stack, _, _) = read_only_stack(&operator, &shared, &index).await?;
        let upstream = repo
            .branch("index.upstream")
            .open()
            .perform(&operator)
            .await?;
        upstream
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:9".parse::<Entity>()?)
                    .is("Upstream".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.set_upstream(&upstream).perform(&operator).await?;
        let head = index.revision();

        stack.pull(&operator).await?;
        index.refresh(&operator).await?;
        assert_eq!(index.revision(), head, "the read-only layer is not pulled");
        Ok(())
    }

    /// A stack whose bottom is under a read-only link commits to its
    /// ephemeral layers, and refuses a write that would land on the
    /// bottom, naming the scope the bottom is linked under.
    #[dialog_common::test]
    async fn it_commits_to_ephemeral_layers_over_a_read_only_bottom() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let index = repo.branch("index").open().perform(&operator).await?;
        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:1".parse::<Entity>()?)
                    .is("Ana".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.refresh(&operator).await?;
        index
            .overlay()
            .assert(Placement::new("app/note".parse()?, name("application")))?;
        let application = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link_read_only(&application, &index, name("device"))
            .perform(&operator)
            .await?;
        let head = index.revision();

        let note: Entity = "note:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("app/note")
                    .of(note.clone())
                    .is("hi".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            values::<String>(&stack, &operator, "app/note", &note).await?,
            vec![Value::String("hi".into())]
        );
        index.refresh(&operator).await?;
        assert_eq!(index.revision(), head);

        let refused = stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("Notes".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a write with no placement lands on the bottom: {other:?}"),
        }
        Ok(())
    }

    /// A stack never writes a bottom under a read-only link, its induction
    /// cell included: it holds the watermark of that bottom in memory. A
    /// rule over a fact that another writer adds to the bottom fires once,
    /// at the next commit, and not again at the one after.
    #[dialog_common::test]
    async fn it_holds_the_watermark_of_a_read_only_bottom_in_memory() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let index = repo.branch("index").open().perform(&operator).await?;
        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:1".parse::<Entity>()?)
                    .is("Ana".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.refresh(&operator).await?;
        index
            .overlay()
            .assert(Placement::new(
                "counter/count".parse()?,
                name("application"),
            ))?
            .assert(Placement::new("app/note".parse()?, name("application")))?;
        let application = Ephemeral::create().perform(&operator).await;
        let mut rule = Changes::new();
        increment_rule().assert(&mut rule);
        application.apply(rule).unwrap();
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link_read_only(&application, &index, name("device"))
            .perform(&operator)
            .await?;
        let watermark = || async {
            let cell = index.induction_cell();
            cell.resolve().perform(&operator).await?;
            anyhow::Ok(cell.content())
        };
        let before = watermark().await?;

        // The first commit adopts the head of the bottom.
        let counter: Entity = "ctr:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("counter/count")
                    .of(counter.clone())
                    .is(1u64),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        // Another writer of the bottom, as the daemon writes the index.
        let other = repo.branch("index").open().perform(&operator).await?;
        other
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "cmd.increment/counter".parse()?,
                    of: "cmd:1".parse()?,
                    is: Value::Entity(counter.clone()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(&operator)
            .await?;
        stack.advance(&operator).await?;
        for _ in 0..2 {
            stack
                .transaction()
                .assert(
                    dialog_query::the!("app/note")
                        .of("note:1".parse::<Entity>()?)
                        .is("hi".to_string()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        // The count that the rule concludes succeeds the first one in the
        // ephemeral layer. A second firing would succeed it with a third.
        assert_eq!(
            values::<u64>(&stack, &operator, "counter/count", &counter).await?,
            vec![Value::UnsignedInt(2)],
            "the rule fired once over what the other writer added"
        );
        assert_eq!(
            watermark().await?,
            before,
            "the induction cell of the bottom is unchanged"
        );
        Ok(())
    }

    /// A transient that a transaction asserts is one it dispatches: over a
    /// read-only bottom, one with no placement is witnessed in the session
    /// store of the bottom, and the commit lands.
    #[dialog_common::test]
    async fn it_dispatches_an_asserted_transient_over_a_read_only_bottom() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let index = repo.branch("index").open().perform(&operator).await?;
        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:1".parse::<Entity>()?)
                    .is("Ana".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.refresh(&operator).await?;
        index
            .overlay()
            .assert(crate::TransientAttribute::new("cmd/open".parse()?))?;
        let application = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link_read_only(&application, &index, name("device"))
            .perform(&operator)
            .await?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("cmd/open")
                    .of("doc:1".parse::<Entity>()?)
                    .is(true),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        Ok(())
    }

    /// A layer that the stack reaches only through a read-only link holds
    /// no link that the stack could write: an edit of its links is
    /// refused, naming the scope it is read under.
    #[dialog_common::test]
    async fn it_refuses_a_link_from_a_layer_it_only_reads() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let index = repo.branch("index").open().perform(&operator).await?;
        let other = repo.branch("other").open().perform(&operator).await?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link_read_only(&top, &index, name("device"))
            .perform(&operator)
            .await?;
        let refused = stack
            .transaction()
            .link(&index, &other, name("other"))
            .commit()
            .publish()
            .perform(&operator)
            .await;
        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a read-only encloser takes no link: {other:?}"),
        }
        assert_eq!(stack.layers().len(), 2);
        Ok(())
    }

    /// An asset is stored only on the bottom: over a read-only bottom, a
    /// commit that carries one is refused, naming the scope of the bottom.
    #[dialog_common::test]
    async fn it_refuses_an_asset_over_a_read_only_bottom() -> Result<()> {
        use dialog_artifacts::Asset;

        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let index = repo.branch("index").open().perform(&operator).await?;
        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of("persona:1".parse::<Entity>()?)
                    .is("Ana".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.refresh(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link_read_only(&application, &index, name("device"))
            .perform(&operator)
            .await?;
        let head = index.revision();

        let refused = stack
            .transaction()
            .assert(Asset::from(vec![7u8; 64]))
            .commit()
            .publish()
            .perform(&operator)
            .await;
        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("an asset over a read-only bottom is refused: {other:?}"),
        }
        index.refresh(&operator).await?;
        assert_eq!(index.revision(), head);
        Ok(())
    }

    /// A push sends no read-only layer to its upstream: only its writer
    /// moves it.
    #[dialog_common::test]
    async fn it_pushes_no_read_only_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        let (stack, _, _) = read_only_stack(&operator, &shared, &index).await?;
        let upstream = repo
            .branch("index.upstream")
            .open()
            .perform(&operator)
            .await?;
        index.set_upstream(&upstream).perform(&operator).await?;

        stack.push(&operator).await?;
        upstream.refresh(&operator).await?;
        assert_eq!(
            upstream.revision(),
            None,
            "the read-only layer is not pushed"
        );
        Ok(())
    }

    /// A transient placed on a read-only scope is refused, whether the
    /// transaction dispatches it or a rule derives it, and no observer
    /// sees a round of the refused commit.
    #[dialog_common::test]
    async fn it_refuses_a_transient_placed_on_a_read_only_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        // `idx/sync` of a note's entity, from the note.
        let derive: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "name": { "the": "idx/sync", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "note": { "the": "app/note", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "note": { "?": { "name": "name" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(derive)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("idx/sync".parse()?, name("device")))?
            .assert(crate::TransientAttribute::new("idx/sync".parse()?))?;
        let (stack, application, _) = read_only_stack(&operator, &shared, &index).await?;
        let note: Entity = "note:1".parse()?;

        let dispatched = stack
            .transaction()
            .dispatch(
                dialog_query::the!("idx/sync")
                    .of(note.clone())
                    .is("now".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        match dispatched {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a dispatched transient on a read-only scope is refused: {other:?}"),
        }

        let observer = application.observe_everything();
        let derived = stack
            .transaction()
            .assert(
                dialog_query::the!("app/note")
                    .of(note.clone())
                    .is("hi".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        match derived {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a derived transient on a read-only scope is refused: {other:?}"),
        }
        assert!(
            matches!(observer.drain(), Drained::Instants(instants) if instants.is_empty()),
            "no observer sees a round of the refused commit"
        );
        Ok(())
    }

    /// After the head of a read-only layer moves outside the stack, the
    /// stack reads the new head once it advances.
    #[dialog_common::test]
    async fn it_reads_a_read_only_layer_at_its_new_head_after_advance() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        let (stack, application, _) = read_only_stack(&operator, &shared, &index).await?;
        let persona: Entity = "persona:2".parse()?;

        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of(persona.clone())
                    .is("Ben".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(
            values::<String>(&stack, &operator, "idx/name", &persona)
                .await?
                .is_empty(),
            "a read stays at the captured head"
        );
        stack.advance(&operator).await?;
        assert_eq!(
            values::<String>(&stack, &operator, "idx/name", &persona).await?,
            vec![Value::String("Ben".into())]
        );
        index.refresh(&operator).await?;
        let recorded: Vec<Value> = application
            .scan(&ArtifactSelector::new().the(link_attr("revision")))
            .into_iter()
            .map(|fact| fact.is)
            .collect();
        assert!(
            recorded.contains(&Value::Bytes(Head::Tree(index.revision()).bytes())),
            "the link records the new head of the read-only layer"
        );
        Ok(())
    }

    /// A stack whose application layer is shared with a peer, the daemon,
    /// over a channel: `cmd.create/name` is a transient placed on
    /// `memory:application`, so a command is a witness of the commit on
    /// that layer, and `intent/name` lands on the bottom.
    async fn channel_stack(
        operator: &impl StackSyncEnv,
        shared: &Branch,
    ) -> Result<(Stack, crate::Channel, Entity)> {
        shared
            .overlay()
            .assert(Placement::new(
                "cmd.create/name".parse()?,
                name("application"),
            ))?
            .assert(crate::TransientAttribute::new("cmd.create/name".parse()?))?;
        let application = Ephemeral::create().perform(operator).await;
        let top = Ephemeral::create().perform(operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, shared, name("shared"))
            .perform(operator)
            .await?;
        let channel = crate::Channel::new(application);
        let daemon: Entity = "did:key:z6MkDaemon".parse()?;
        channel.join(daemon.clone());
        Ok((stack, channel, daemon))
    }

    /// A commit that records an intent on the bottom and witnesses a
    /// command on the application layer.
    fn create_garden(stack: &Stack) -> Result<StackTransaction<'_>> {
        let intent: Entity = "intent:zI".parse()?;
        Ok(stack
            .transaction()
            .assert(
                dialog_query::the!("intent/name")
                    .of(intent.clone())
                    .is("Garden".to_string()),
            )
            .dispatch(
                dialog_query::the!("cmd.create/name")
                    .of(intent)
                    .is("Garden".to_string()),
            ))
    }

    /// The commands that a catch-up of `channel` carries to `peer`.
    fn commands(channel: &crate::Channel, peer: &Entity) -> usize {
        match channel.since(peer) {
            crate::Catchup::Instants { instants, .. } => instants
                .iter()
                .filter(|instant| {
                    instant
                        .asserted
                        .iter()
                        .any(|fact| fact.the.as_str() == "cmd.create/name")
                })
                .count(),
            crate::Catchup::Resync { .. } => panic!("the peer is caught up from the log"),
        }
    }

    /// The holds that a publish takes from the stack are settled however
    /// it ends: released when every chain lands, and discarded when the
    /// publish fails or its future is dropped, so that no hold stays open
    /// and stalls a channel.
    #[test]
    fn it_settles_the_holds_of_a_publish_however_it_ends() {
        let (released, dropped) = (Hold::open(1), Hold::open(1));
        let settling = |hold: &Hold| Settling {
            holds: vec![hold.clone()],
            entered: Entered::default(),
        };
        settling(&released).release();
        drop(settling(&dropped));
        assert_eq!(released.state(), crate::Held::Released);
        assert_eq!(dropped.state(), crate::Held::Dropped);
    }

    /// A channel holds the instants of a commit until it publishes. Here
    /// the publish loses its compare and set: no catch-up carries the
    /// command, though a rule and an observer in the process saw it at
    /// stage time. The run again, after an advance, sends it once.
    #[dialog_common::test]
    async fn it_holds_the_instants_of_a_commit_until_it_publishes() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let pending: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "name": { "the": "pending/name", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "name": { "the": "cmd.create/name", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(pending)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let (stack, channel, daemon) = channel_stack(&operator, &shared).await?;
        let observer = channel.layer().observe_everything();

        create_garden(&stack)?.commit().perform(&operator).await?;
        match observer.drain() {
            Drained::Instants(instants) => assert!(
                instants.iter().any(|instant| instant.transient),
                "an observer in the process sees the command at stage time"
            ),
            Drained::Gap { .. } => panic!("no gap"),
        }
        let intent: Entity = "intent:zI".parse()?;
        assert_eq!(
            values::<String>(&stack, &operator, "pending/name", &intent).await?,
            vec![Value::String("Garden".into())],
            "a rule in the process reacts to the command at stage time"
        );
        shared.refresh(&operator).await?;
        assert!(
            committed(&shared, &operator, "pending/name", &intent)
                .await?
                .is_empty(),
            "while the head of the bottom has not moved"
        );
        assert_eq!(commands(&channel, &daemon), 0, "the commit is staged");

        // Another device moves the head: the publish loses.
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("elsewhere".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(matches!(
            stack.publish(&operator).await,
            Err(StackError::Publish { .. })
        ));
        assert_eq!(
            commands(&channel, &daemon),
            0,
            "a lost publish sends no command"
        );

        // The run again, on the new head.
        stack.advance(&operator).await?;
        create_garden(&stack)?
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            commands(&channel, &daemon),
            1,
            "the run again sends it once"
        );
        assert_eq!(commands(&channel, &daemon), 0);
        Ok(())
    }

    /// The facts that a commit stores in an ephemeral layer stay there when
    /// its publish fails, so a peer of the channel receives them all the
    /// same, and holds what the layer holds. Only the witness waits for a
    /// publish.
    #[dialog_common::test]
    async fn it_sends_the_facts_of_a_commit_that_lost_its_publish() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("app/note".parse()?, name("application")))?;
        let (stack, channel, daemon) = channel_stack(&operator, &shared).await?;
        let note: Entity = "note:1".parse()?;
        let noted = || -> Result<StackTransaction<'_>> {
            Ok(create_garden(&stack)?.assert(
                dialog_query::the!("app/note")
                    .of(note.clone())
                    .is("hi".to_string()),
            ))
        };
        noted()?.commit().perform(&operator).await?;
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("elsewhere".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(stack.publish(&operator).await.is_err(), "the publish loses");
        stack.advance(&operator).await?;
        noted()?.commit().publish().perform(&operator).await?;

        let crate::Catchup::Instants { instants, .. } = channel.since(&daemon) else {
            panic!("the peer is caught up from the log");
        };
        assert!(
            instants.iter().any(|instant| instant
                .asserted
                .iter()
                .any(|fact| fact.the.as_str() == "app/note")),
            "the peer receives the note that the layer holds"
        );
        Ok(())
    }

    /// A command that waited for its publish is not lost to the ring bound
    /// of the channel once it is released: a peer that has not read it
    /// still receives it, since no resync could rebuild it.
    #[dialog_common::test]
    async fn it_keeps_a_released_command_past_the_ring_bound() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new(
                "cmd.create/name".parse()?,
                name("application"),
            ))?
            .assert(crate::TransientAttribute::new("cmd.create/name".parse()?))?;
        let application = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let channel = crate::Channel::over(application.clone(), 4);
        let daemon: Entity = "did:key:z6MkDaemon".parse()?;
        channel.join(daemon.clone());

        create_garden(&stack)?.commit().perform(&operator).await?;
        let result = |index: usize| -> Result<Changes> {
            let mut result = Changes::new();
            result.associate(
                "result/status".parse()?,
                format!("cmd:{index}").parse()?,
                Value::String("done".into()),
                Pick::All,
            );
            Ok(result)
        };
        for index in 0..5 {
            application.apply(result(index)?)?;
        }
        assert_eq!(commands(&channel, &daemon), 0, "the command waits");
        for index in 5..10 {
            application.apply(result(index)?)?;
        }
        stack.publish(&operator).await?;
        assert_eq!(
            commands(&channel, &daemon),
            1,
            "the released command reaches the peer"
        );
        Ok(())
    }

    /// A peer that joins while a command waits for its publish is owed
    /// the command: it receives it once the commit publishes.
    #[dialog_common::test]
    async fn it_owes_a_waiting_command_to_a_peer_that_joins() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, channel, _) = channel_stack(&operator, &shared).await?;
        create_garden(&stack)?.commit().perform(&operator).await?;
        let late: Entity = "did:key:z6MkLate".parse()?;
        channel.join(late.clone());
        // A second join, as a reconnect makes, owes the command once.
        channel.join(late.clone());
        assert_eq!(commands(&channel, &late), 0, "the command waits");
        stack.publish(&operator).await?;
        assert_eq!(commands(&channel, &late), 1, "and goes once it publishes");
        assert_eq!(commands(&channel, &late), 0);
        Ok(())
    }

    /// An advance drops a staged commit: its command never leaves, and
    /// the run again sends it once.
    #[dialog_common::test]
    async fn it_sends_no_command_of_a_commit_that_an_advance_dropped() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, channel, daemon) = channel_stack(&operator, &shared).await?;
        create_garden(&stack)?.commit().perform(&operator).await?;
        stack.advance(&operator).await?;
        assert!(!stack.is_staged(), "the advance dropped the staged commit");
        assert_eq!(commands(&channel, &daemon), 0);
        create_garden(&stack)?
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            commands(&channel, &daemon),
            1,
            "the run again sends it once"
        );
        Ok(())
    }

    /// The shape of a stack in Sapling: a wiring layer at the top links the
    /// space first, so it is the bottom, and the application layer beside
    /// it, which a channel shares. A command that a rule derives reaches
    /// the channel once its commit publishes, a fact placed on the
    /// application scope lands in that layer, a commit with no durable
    /// change sends at once, and no link enters the application layer.
    #[dialog_common::test]
    async fn it_shares_the_application_layer_beside_the_space() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let space = repo.branch("main").open().perform(&operator).await?;
        let command: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "name": { "the": "cmd.create/name", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "name": { "the": "intent/name", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }]
        }))?;
        space
            .transaction()
            .assert(command)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        space.refresh(&operator).await?;
        space
            .overlay()
            .assert(Placement::new(
                "cmd.create/name".parse()?,
                name("application"),
            ))?
            .assert(crate::TransientAttribute::new("cmd.create/name".parse()?))?
            .assert(Placement::new(
                "result/status".parse()?,
                name("application"),
            ))?;
        let wiring = Ephemeral::create().perform(&operator).await;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(wiring.clone())
            .link(&wiring, &space, name("space"))
            .link(&wiring, &application, name("application"))
            .perform(&operator)
            .await?;
        assert!(stack.layers()[0].same(&Layer::Branch(space.clone())));
        let channel = crate::Channel::new(application.clone());
        let daemon: Entity = "did:key:z6MkDaemon".parse()?;
        channel.join(daemon.clone());

        stack
            .transaction()
            .assert(
                dialog_query::the!("intent/name")
                    .of("intent:zI".parse::<Entity>()?)
                    .is("Garden".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(commands(&channel, &daemon), 0, "the command waits");
        stack.publish(&operator).await?;
        assert_eq!(commands(&channel, &daemon), 1, "and goes once it publishes");

        let result: Entity = "cmd:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("result/status")
                    .of(result.clone())
                    .is("done".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert!(!stack.is_staged(), "the commit changed no durable layer");
        let crate::Catchup::Instants { instants, .. } = channel.since(&daemon) else {
            panic!("the peer is caught up from the log");
        };
        assert!(
            instants.iter().any(|instant| instant
                .asserted
                .iter()
                .any(|fact| fact.the.as_str() == "result/status")),
            "the fact lands in the application layer and goes at once"
        );
        assert!(
            application
                .scan(&ArtifactSelector::new().the(link_attr("from")))
                .is_empty(),
            "no link is a fact of the application layer"
        );
        Ok(())
    }

    /// A peer that is the only one, and that a reconnect places past a
    /// command that waits, is owed it still: the log keeps the command
    /// until it is repaid.
    #[dialog_common::test]
    async fn it_owes_a_waiting_command_to_the_only_peer_across_a_reconnect() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, channel, daemon) = channel_stack(&operator, &shared).await?;
        create_garden(&stack)?.commit().perform(&operator).await?;
        channel.leave(&daemon);
        channel.join(daemon.clone());
        stack.publish(&operator).await?;
        assert_eq!(commands(&channel, &daemon), 1, "the command goes once");
        assert_eq!(commands(&channel, &daemon), 0);
        Ok(())
    }

    /// A commit whose induction derives a write that fails to route never
    /// publishes, and its command never leaves: the rounds are witnessed
    /// only once the batch routes. A later commit still reaches the peer.
    #[dialog_common::test]
    async fn it_sends_no_command_of_a_commit_that_failed_to_route() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        // A rule that answers the command with a fact placed on a scope no
        // layer binds, so the routing of the settled batch fails.
        let misplace: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "name": { "the": "lost/name", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "name": { "the": "cmd.create/name", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "name": { "?": { "name": "name" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(misplace)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("lost/name".parse()?, name("nowhere")))?
            .assert(Placement::new("app/note".parse()?, name("application")))?;
        let (stack, channel, daemon) = channel_stack(&operator, &shared).await?;

        assert!(
            create_garden(&stack)?
                .commit()
                .perform(&operator)
                .await
                .is_err(),
            "the derived fact routes nowhere"
        );
        stack
            .transaction()
            .assert(
                dialog_query::the!("app/note")
                    .of("note:1".parse::<Entity>()?)
                    .is("hi".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        match channel.since(&daemon) {
            crate::Catchup::Instants { instants, .. } => {
                assert_eq!(instants.len(), 1, "the later commit reaches the peer");
                assert!(
                    instants[0]
                        .asserted
                        .iter()
                        .all(|fact| fact.the.as_str() == "app/note")
                );
            }
            crate::Catchup::Resync { .. } => panic!("the peer is caught up from the log"),
        }
        Ok(())
    }

    /// A peer that falls off the log while a command waits for its
    /// publish resyncs to just before the command, so the command still
    /// reaches it once the commit publishes.
    #[dialog_common::test]
    async fn it_keeps_a_held_command_through_a_resync() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, channel, _) = channel_stack(&operator, &shared).await?;
        let small = crate::Channel::over(channel.layer().clone(), 4);
        let daemon: Entity = "did:key:z6MkSlow".parse()?;
        small.join(daemon.clone());
        // More than the log holds enters before the command.
        for index in 0..10u32 {
            let mut changes = Changes::new();
            changes.associate(
                "note/n".parse()?,
                format!("note:{index}").parse()?,
                Value::UnsignedInt(index.into()),
                Pick::All,
            );
            channel.layer().apply(changes)?;
        }
        create_garden(&stack)?.commit().perform(&operator).await?;
        assert!(
            matches!(small.since(&daemon), crate::Catchup::Resync { .. }),
            "the peer fell off the log"
        );

        stack.publish(&operator).await?;
        assert_eq!(
            commands(&small, &daemon),
            1,
            "the command follows the resync"
        );
        Ok(())
    }

    /// A commit that changes no durable layer has no publish to wait for:
    /// a channel sends its instants at once.
    #[dialog_common::test]
    async fn it_sends_at_once_the_instants_of_a_commit_with_no_durable_change() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (stack, channel, daemon) = channel_stack(&operator, &shared).await?;

        stack
            .transaction()
            .dispatch(
                dialog_query::the!("cmd.create/name")
                    .of("intent:zJ".parse::<Entity>()?)
                    .is("Office".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert!(!stack.is_staged(), "no durable layer staged");
        assert_eq!(commands(&channel, &daemon), 1);
        Ok(())
    }

    /// A fact that a channel receives into an upper layer fires a rule at
    /// the next commit of the stack, and only once; `receive` itself runs
    /// no rule, and a commit with no facts of its own runs the rules over
    /// what entered.
    #[dialog_common::test]
    async fn it_runs_rules_over_what_a_channel_received() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        // A rule that answers each result with a notice: a transient,
        // witnessed on the application layer each time the rule fires.
        let notice: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "status": { "the": "notice/seen", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "status": { "the": "result/status", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "status": { "?": { "name": "status" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(notice)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        for attribute in ["result/status", "notice/seen"] {
            shared
                .overlay()
                .assert(Placement::new(attribute.parse()?, name("application")))?;
        }
        shared
            .overlay()
            .assert(crate::TransientAttribute::new("notice/seen".parse()?))?;
        let application = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let observer = application.observe_everything();
        let notices = || match observer.drain() {
            Drained::Instants(instants) => instants
                .iter()
                .filter(|instant| {
                    instant.transient
                        && instant
                            .asserted
                            .iter()
                            .any(|fact| fact.the.as_str() == "notice/seen")
                })
                .count(),
            Drained::Gap { .. } => panic!("no gap"),
        };

        // The daemon writes a result into its end of the channel, and the
        // worker receives it.
        let (worker, daemon): (Entity, Entity) =
            ("did:key:z6MkWorker".parse()?, "did:key:z6MkDaemon".parse()?);
        let theirs = crate::Channel::new(Ephemeral::create().perform(&operator).await);
        let ours = crate::Channel::new(application.clone());
        theirs.join(worker.clone());
        ours.join(daemon.clone());
        let mut result = Changes::new();
        result.associate(
            "result/status".parse()?,
            "cmd:1".parse()?,
            Value::String("done".into()),
            Pick::All,
        );
        theirs.layer().apply(result)?;
        assert_eq!(ours.receive(&daemon, theirs.since(&worker)), 1);
        assert_eq!(notices(), 0, "receive runs no rule");

        let commit = || async {
            stack
                .transaction()
                .commit()
                .publish()
                .perform(&operator)
                .await
        };
        commit().await?;
        assert_eq!(
            notices(),
            1,
            "the next commit, with no facts of its own, runs the rule"
        );
        commit().await?;
        assert_eq!(notices(), 0, "and only once");
        Ok(())
    }

    /// A rule that records each result on the bottom, as `seen/status`.
    fn record_results() -> Result<dialog_query::InductiveRule> {
        Ok(serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "status": { "the": "seen/status", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "status": { "the": "result/status", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "status": { "?": { "name": "status" } }
                }
            }]
        }))?)
    }

    /// A stack over `shared` whose application layer holds the results,
    /// with a rule on the bottom that sends a transient notice of each
    /// result, and an observer of the bottom's session store, where each
    /// notice is witnessed: one witness for each time the rule fires.
    async fn notices_stack(
        operator: &impl StackSyncEnv,
        shared: &Branch,
        application: &Ephemeral,
    ) -> Result<(Stack, crate::Observer)> {
        let notify: dialog_query::InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "status": { "the": "notice/status", "as": "text:" } }
            },
            "when": [{
                "assert": {
                    "with": { "status": { "the": "result/status", "as": "text:" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "status": { "?": { "name": "status" } }
                }
            }]
        }))?;
        shared
            .transaction()
            .assert(notify)
            .commit()
            .publish()
            .perform(operator)
            .await?;
        shared.refresh(operator).await?;
        shared
            .overlay()
            .assert(Placement::new(
                "result/status".parse()?,
                name("application"),
            ))?
            .assert(crate::TransientAttribute::new("notice/status".parse()?))?;
        let top = Ephemeral::create().perform(operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, application, name("application"))
            .link(application, shared, name("shared"))
            .perform(operator)
            .await?;
        Ok((stack, shared.overlay().observe_everything()))
    }

    /// How many notices `observer` saw witnessed since it last drained.
    fn notices(observer: &crate::Observer) -> usize {
        match observer.drain() {
            Drained::Instants(instants) => instants
                .iter()
                .filter(|instant| {
                    instant
                        .asserted
                        .iter()
                        .any(|fact| fact.the.as_str() == "notice/status")
                })
                .count(),
            Drained::Gap { .. } => panic!("no gap"),
        }
    }

    /// A result applied to the application layer, as a channel applies
    /// what it receives.
    fn received(layer: &Ephemeral, command: &str) -> Result<()> {
        let mut result = Changes::new();
        result.associate(
            "result/status".parse()?,
            command.parse()?,
            Value::String("done".into()),
            Pick::All,
        );
        layer.apply(result)?;
        Ok(())
    }

    /// What entered counts as induced once the commit that induced over it
    /// publishes: a staged commit, its publish, and a later commit fire the
    /// rule once.
    #[dialog_common::test]
    async fn it_fires_once_over_what_entered_through_a_publish() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let (stack, observer) = notices_stack(&operator, &shared, &application).await?;
        received(&application, "cmd:1")?;

        stack
            .transaction()
            .assert(
                dialog_query::the!("note/text")
                    .of("note:1".parse::<Entity>()?)
                    .is("staged".to_string()),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert!(stack.is_staged());
        stack.publish(&operator).await?;
        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(notices(&observer), 1, "the rule fired once");
        Ok(())
    }

    /// What a stack writes into its own layers is a change of its commit,
    /// not something that entered from outside: a later commit does not
    /// fire on it again.
    #[dialog_common::test]
    async fn it_runs_no_rule_again_over_what_it_wrote_itself() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let (stack, observer) = notices_stack(&operator, &shared, &application).await?;

        stack
            .transaction()
            .assert(
                dialog_query::the!("result/status")
                    .of("cmd:1".parse::<Entity>()?)
                    .is("done".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(notices(&observer), 1, "the rule fired at the commit alone");
        Ok(())
    }

    /// A fact that entered a layer outside the scope of its placement
    /// fires no rule, as it answers no query.
    #[dialog_common::test]
    async fn it_runs_no_rule_over_what_entered_outside_its_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let (stack, observer) = notices_stack(&operator, &shared, &application).await?;
        let top = stack.layers().pop().expect("a stack has a top");
        let Layer::Ephemeral(top) = top else {
            panic!("the top is ephemeral");
        };
        received(&top, "cmd:1")?;

        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(notices(&observer), 0, "the misplaced result fired nothing");
        Ok(())
    }

    /// A stack over `shared` whose application layer holds the results,
    /// with the rule of [`record_results`] on the bottom.
    async fn results_stack(
        operator: &impl StackSyncEnv,
        shared: &Branch,
        application: &Ephemeral,
    ) -> Result<Stack> {
        shared
            .transaction()
            .assert(record_results()?)
            .commit()
            .publish()
            .perform(operator)
            .await?;
        shared.refresh(operator).await?;
        shared.overlay().assert(Placement::new(
            "result/status".parse()?,
            name("application"),
        ))?;
        let top = Ephemeral::create().perform(operator).await;
        Ok(Stack::open(top.clone())
            .link(&top, application, name("application"))
            .link(application, shared, name("shared"))
            .perform(operator)
            .await?)
    }

    /// What entered an upper layer counts as induced only once the commit
    /// that induced over it publishes: a commit that loses its publish
    /// leaves it for the run again, whose rule records it.
    #[dialog_common::test]
    async fn it_keeps_what_entered_for_a_commit_that_did_not_publish() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = results_stack(&operator, &shared, &application).await?;
        let command: Entity = "cmd:1".parse()?;
        let mut result = Changes::new();
        result.associate(
            "result/status".parse()?,
            command.clone(),
            Value::String("done".into()),
            Pick::All,
        );
        application.apply(result)?;

        stack.transaction().commit().perform(&operator).await?;
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("elsewhere".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(stack.publish(&operator).await.is_err(), "the publish loses");

        stack.advance(&operator).await?;
        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "seen/status", &command).await?,
            vec![Value::String("done".into())],
            "the run again records the result"
        );
        Ok(())
    }

    /// A fact that entered an upper layer and left it again before the
    /// next commit changed nothing: no rule fires on it.
    #[dialog_common::test]
    async fn it_runs_no_rule_over_what_entered_and_left() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = results_stack(&operator, &shared, &application).await?;
        let command: Entity = "cmd:1".parse()?;
        let mut result = Changes::new();
        result.associate(
            "result/status".parse()?,
            command.clone(),
            Value::String("done".into()),
            Pick::All,
        );
        application.apply(result)?;
        let mut removed = Changes::new();
        removed.dissociate(
            "result/status".parse()?,
            command.clone(),
            Value::String("done".into()),
        );
        application.apply(removed)?;

        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert!(
            committed(&shared, &operator, "seen/status", &command)
                .await?
                .is_empty(),
            "no rule fired on a result that the layer no longer holds"
        );
        Ok(())
    }

    /// A fact that entered an upper layer, and whose derived write fails to
    /// route, fails each commit until the layer drops it. Then the stack
    /// commits again.
    #[dialog_common::test]
    async fn it_commits_again_once_what_failed_its_commits_leaves() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = results_stack(&operator, &shared, &application).await?;
        shared
            .overlay()
            .assert(Placement::new("seen/status".parse()?, name("nowhere")))?;
        let command: Entity = "cmd:1".parse()?;
        let mut result = Changes::new();
        result.associate(
            "result/status".parse()?,
            command.clone(),
            Value::String("done".into()),
            Pick::All,
        );
        application.apply(result)?;
        for _ in 0..2 {
            let refused = stack.transaction().commit().perform(&operator).await;
            assert!(
                matches!(
                    refused,
                    Err(StackError::Commit(CommitError::UnboundScope { .. }))
                ),
                "the derived write has nowhere to land: {refused:?}"
            );
        }

        let mut removed = Changes::new();
        removed.dissociate(
            "result/status".parse()?,
            command.clone(),
            Value::String("done".into()),
        );
        application.apply(removed)?;
        let note: Entity = "note:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("note/text")
                    .of(note.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "note/text", &note).await?,
            vec![Value::String("hello".into())]
        );
        Ok(())
    }

    /// A stack drops itself a fact that entered and fails every commit: a
    /// commit that clears the layer lands, and so does the next one.
    #[dialog_common::test]
    async fn it_clears_what_failed_its_commits() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = results_stack(&operator, &shared, &application).await?;
        shared
            .overlay()
            .assert(Placement::new("seen/status".parse()?, name("nowhere")))?;
        let mut result = Changes::new();
        result.associate(
            "result/status".parse()?,
            "cmd:1".parse()?,
            Value::String("done".into()),
            Pick::All,
        );
        application.apply(result)?;
        assert!(
            stack
                .transaction()
                .commit()
                .perform(&operator)
                .await
                .is_err(),
            "the derived write has nowhere to land"
        );

        stack
            .transaction()
            .clear(name("application"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(
            application
                .scan(&ArtifactSelector::new().the("result/status".parse()?))
                .is_empty(),
            "the clear dropped the result"
        );
        let note: Entity = "note:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("note/text")
                    .of(note.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "note/text", &note).await?,
            vec![Value::String("hello".into())]
        );
        Ok(())
    }

    /// More instants than a stack observes between two commits enter an
    /// upper layer: the stack takes what entered from the layer itself, and
    /// its rules still fire on each fact.
    #[dialog_common::test]
    async fn it_runs_rules_over_what_entered_past_its_observer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = results_stack(&operator, &shared, &application).await?;
        let count = crate::DEFAULT_LOG_CAPACITY + 8;
        for index in 0..count {
            let mut result = Changes::new();
            result.associate(
                "result/status".parse()?,
                format!("cmd:{index}").parse()?,
                Value::String("done".into()),
                Pick::All,
            );
            application.apply(result)?;
        }

        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        for index in [0, count - 1] {
            let command: Entity = format!("cmd:{index}").parse()?;
            assert_eq!(
                committed(&shared, &operator, "seen/status", &command).await?,
                vec![Value::String("done".into())],
                "the rule recorded the result of {command}"
            );
        }
        Ok(())
    }

    /// What a commit writes into an upper layer is never taken for what
    /// entered, even when more enters than the stack observes before the
    /// next commit: the rule fires on the stack's own result once.
    #[dialog_common::test]
    async fn it_runs_no_rule_again_over_its_own_write_past_its_observer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let application = Ephemeral::create().perform(&operator).await;
        let (stack, observer) = notices_stack(&operator, &shared, &application).await?;
        let own: Entity = "cmd:own".parse()?;
        let notices_of = |observer: &crate::Observer| match observer.drain() {
            Drained::Instants(instants) => instants
                .iter()
                .flat_map(|instant| instant.asserted.iter())
                .filter(|fact| fact.the.as_str() == "notice/status" && fact.of == own)
                .count(),
            Drained::Gap { .. } => panic!("no gap"),
        };

        stack
            .transaction()
            .assert(
                dialog_query::the!("result/status")
                    .of(own.clone())
                    .is("done".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(notices_of(&observer), 1, "the rule fired at the commit");
        for index in 0..crate::DEFAULT_LOG_CAPACITY + 8 {
            received(&application, &format!("cmd:{index}"))?;
        }
        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            notices_of(&observer),
            0,
            "and not again over its own result"
        );
        Ok(())
    }

    /// The persona stack of Sapling: a wiring layer at the top links the
    /// space first, so it is the bottom, the index under a read-only link,
    /// and the application layer beside them. An index fact that a member
    /// commits into the space reaches no read, a write of the index fails,
    /// and a result that enters the application layer fires a rule once.
    #[dialog_common::test]
    async fn it_guides_holds_and_reacts_in_the_shape_of_a_sapling_stack() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let space = repo.branch("main").open().perform(&operator).await?;
        let index = repo.branch("index").open().perform(&operator).await?;
        let persona: Entity = "persona:1".parse()?;
        index
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of(persona.clone())
                    .is("Ana".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        index.refresh(&operator).await?;
        space
            .transaction()
            .assert(record_results()?)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        space.refresh(&operator).await?;
        space
            .overlay()
            .assert(crate::DefaultScope::new(&repo.did(), name("space")))?
            .assert(Placement::new("idx/name".parse()?, name("device")))?
            .assert(Placement::new(
                "result/status".parse()?,
                name("application"),
            ))?;
        let wiring = Ephemeral::create().perform(&operator).await;
        let application = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(wiring.clone())
            .link(&wiring, &space, name("space"))
            .link_read_only(&wiring, &index, name("device"))
            .link(&wiring, &application, name("application"))
            .perform(&operator)
            .await?;
        assert!(stack.layers()[0].same(&Layer::Branch(space.clone())));

        // A member commits an index fact into the space.
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "idx/name".parse()?,
                    of: persona.clone(),
                    is: Value::String("Eve".into()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(&operator)
            .await?;
        stack.advance(&operator).await?;
        assert_eq!(
            values::<String>(&stack, &operator, "idx/name", &persona).await?,
            vec![Value::String("Ana".into())],
            "the index is read from the index layer alone"
        );

        let refused = stack
            .transaction()
            .assert(
                dialog_query::the!("idx/name")
                    .of(persona.clone())
                    .is("Mallory".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        match refused {
            Err(StackError::ReadOnly { scope }) => assert_eq!(scope, name("device")),
            other => panic!("a write of the index is refused: {other:?}"),
        }

        received(&application, "cmd:1")?;
        for _ in 0..2 {
            stack
                .transaction()
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        space.refresh(&operator).await?;
        assert_eq!(
            committed(&space, &operator, "seen/status", &"cmd:1".parse()?).await?,
            vec![Value::String("done".into())],
            "the rule recorded the result that entered"
        );
        Ok(())
    }

    /// A write of another stack into a layer that this stack links entered
    /// this stack from outside it: its rules run over it.
    #[dialog_common::test]
    async fn it_runs_rules_over_what_another_stack_wrote() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared.overlay().assert(Placement::new(
            "result/status".parse()?,
            name("application"),
        ))?;
        let application = Ephemeral::create().perform(&operator).await;
        // The rule lives in this stack's own top, which the writer does not
        // link, so only a commit of this stack runs it.
        let top = Ephemeral::create().perform(&operator).await;
        let mut rule = Changes::new();
        record_results()?.assert(&mut rule);
        top.apply(rule)?;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let other_top = Ephemeral::create().perform(&operator).await;
        let writer = Stack::open(other_top.clone())
            .link(&other_top, &application, name("application"))
            .link(&application, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let command: Entity = "cmd:2".parse()?;
        writer
            .transaction()
            .assert(
                dialog_query::the!("result/status")
                    .of(command.clone())
                    .is("done".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert!(
            committed(&shared, &operator, "seen/status", &command)
                .await?
                .is_empty(),
            "the writer runs no rule of this stack"
        );

        stack.advance(&operator).await?;
        stack
            .transaction()
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "seen/status", &command).await?,
            vec![Value::String("done".into())],
            "this stack's next commit runs its rule over what the writer wrote"
        );
        Ok(())
    }

    /// Placement says where a fact belongs: a declared attribute lands
    /// under its scope, `forget` drops an entity's facts from the
    /// scope's layer, `clear` empties it, and none of it touches the
    /// tree.
    #[dialog_common::test]
    async fn it_maintains_a_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let site: Entity = "site:1".parse()?;
        let other: Entity = "site:2".parse()?;
        stack
            .transaction()
            .assert(Placement::new("site/path".parse()?, name("state")))
            .assert(
                dialog_query::the!("site/path")
                    .of(site.clone())
                    .is("/a".to_string()),
            )
            .assert(
                dialog_query::the!("site/path")
                    .of(other.clone())
                    .is("/b".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            values::<String>(&stack, &operator, "site/path", &site).await?,
            vec![Value::String("/a".into())],
            "the composite reads the placed fact"
        );
        assert!(
            committed(&shared, &operator, "site/path", &site)
                .await?
                .is_empty(),
            "the tree never sees it"
        );
        let paths = ArtifactSelector::new().the("site/path".parse()?);
        assert_eq!(state.scan(&paths).len(), 2);

        let demand = crate::Demand::new();
        demand.record(&paths, &Manifest::default());
        let observer = state.observe(demand);
        stack
            .transaction()
            .forget(name("state"), vec![site.clone()])
            .assert(
                dialog_query::the!("site/path")
                    .of(site.clone())
                    .is("/restamped".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let crate::Drained::Instants(instants) = observer.drain() else {
            panic!("one restamp cannot overflow");
        };
        assert_eq!(instants.len(), 1, "forget and replacement are one instant");
        assert_eq!(instants[0].retracted[0].is, Value::String("/a".into()));
        assert_eq!(
            instants[0].asserted[0].is,
            Value::String("/restamped".into())
        );
        let refused = stack
            .transaction()
            .clear(name("state"))
            .clear(name("shared"))
            .commit()
            .perform(&operator)
            .await;
        assert!(matches!(refused, Err(StackError::NotEphemeral { .. })));
        assert_eq!(
            state.scan(&paths).len(),
            2,
            "invalid maintenance changes no store"
        );

        stack
            .transaction()
            .forget(name("state"), vec![site.clone()])
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(state.scan(&paths).len(), 1, "one site forgotten");
        assert!(
            values::<String>(&stack, &operator, "site/path", &site)
                .await?
                .is_empty()
        );

        stack
            .transaction()
            .clear(name("state"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(state.scan(&paths).len(), 0, "cleared");
        let reopened = Stack::open(top.clone()).perform(&operator).await?;
        assert_eq!(
            reopened.identities(),
            stack.identities(),
            "clearing state preserves stack wiring"
        );
        assert_eq!(reopened.layers().len(), 3);

        let refused = stack
            .transaction()
            .clear(name("shared"))
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(StackError::NotEphemeral { .. })),
            "a tree layer is never cleared: {refused:?}"
        );
        let unbound = stack
            .transaction()
            .assert(Placement::new("site/other".parse()?, name("nowhere")))
            .assert(
                dialog_query::the!("site/other")
                    .of(site)
                    .is("/c".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(
                unbound,
                Err(StackError::Commit(CommitError::UnboundScope { .. }))
            ),
            "a scope nothing is linked under refuses the write: {unbound:?}"
        );
        Ok(())
    }

    /// A placement that a transaction declares guides a query of it, as
    /// it routes the commit: a row outside the scope it names is no longer
    /// read.
    #[dialog_common::test]
    async fn it_reads_by_a_placement_that_the_transaction_declares() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let doc: Entity = "doc:1".parse()?;
        let mut held = Changes::new();
        dialog_query::the!("ui/selected")
            .of(doc.clone())
            .is(true)
            .assert(&mut held);
        state.apply(held).unwrap();
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;

        assert_eq!(
            seen::<bool>(&stack.transaction().query(), &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "an attribute with no placement is read from every layer"
        );
        let placed = stack
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("shared")))
            .query();
        assert!(
            seen::<bool>(&placed, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "the placement that the transaction declares leads the read to the bottom"
        );
        Ok(())
    }

    /// A default scope that a writer of the bottom commits never overrides
    /// a placement: an attribute placed on a scope that a link binds is
    /// read from that layer, and written there, even when the default
    /// names the same scope.
    #[dialog_common::test]
    async fn it_reads_by_a_placement_over_a_default_that_names_its_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let space: Entity = "space:1".parse()?;
        shared
            .transaction()
            .assert(crate::DefaultScope::new(&repo.did(), name("device")))
            .assert(
                dialog_query::the!("idx/origin")
                    .of(space.clone())
                    .is("forged".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("idx/origin".parse()?, name("device")))?;
        let device = Ephemeral::create().perform(&operator).await;
        let mut held = Changes::new();
        dialog_query::the!("idx/origin")
            .of(space.clone())
            .is("real".to_string())
            .assert(&mut held);
        device.apply(held)?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &device, name("device"))
            .link(&device, &shared, name("shared"))
            .perform(&operator)
            .await?;

        assert_eq!(
            values::<String>(&stack, &operator, "idx/origin", &space).await?,
            vec![Value::String("real".into())],
            "the placement leads the read to the device layer alone"
        );
        let other: Entity = "space:2".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("idx/origin")
                    .of(other.clone())
                    .is("new".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            device
                .scan(&ArtifactSelector::new().the("idx/origin".parse()?).of(other))
                .len(),
            1,
            "and its write lands there"
        );

        // An attribute with no placement belongs to the bottom, whatever
        // layer the default scope binds: its write lands there, and the
        // stack reads it back.
        let note: Entity = "note:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("note/text")
                    .of(note.clone())
                    .is("hello".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        assert_eq!(
            committed(&shared, &operator, "note/text", &note).await?,
            vec![Value::String("hello".into())],
            "the unplaced write lands on the bottom"
        );
        assert_eq!(
            values::<String>(&stack, &operator, "note/text", &note).await?,
            vec![Value::String("hello".into())],
            "and the stack reads it"
        );
        Ok(())
    }

    /// A retraction that a layer holds outside the scope of its attribute
    /// hides nothing, as a fact there reaches no read: a guided read takes
    /// the tombstones of a layer only for the attributes it reads there.
    #[dialog_common::test]
    async fn it_hides_nothing_with_a_retraction_outside_its_scope() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        shared
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        shared
            .overlay()
            .assert(crate::DefaultScope::new(&repo.did(), name("shared")))?;
        let application = Ephemeral::create().perform(&operator).await;
        let mut retracted = Changes::new();
        retracted.dissociate(
            "doc/title".parse()?,
            doc.clone(),
            Value::String("Notes".into()),
        );
        application.apply(retracted)?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &application, name("application"))
            .link(&application, &shared, name("shared"))
            .perform(&operator)
            .await?;

        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the misplaced retraction hides nothing"
        );
        Ok(())
    }

    /// A read that skips the bottom for an attribute placed on another
    /// layer, because the bottom stored no fact of it, still meets a fact
    /// of it that a later head of the bottom stores: what the bottom
    /// stores is read again at each head, and a misplaced fact there is
    /// reported as a read meets it.
    #[dialog_common::test]
    async fn it_reports_a_misplaced_fact_of_a_later_head() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared
            .transaction()
            .assert(Placement::new("site/path".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &state, name("state"))
            .link(&state, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let bottom = stack.layers()[0].address_entity();
        let site: Entity = "site:1".parse()?;
        assert!(
            values::<String>(&stack, &operator, "site/path", &site)
                .await?
                .is_empty()
        );
        assert!(stack.divergences().is_empty(), "nothing misplaced yet");

        // Another writer of the bottom stores a fact of the attribute.
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "site/path".parse()?,
                    of: site.clone(),
                    is: Value::String("/misplaced".into()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(&operator)
            .await?;
        stack.advance(&operator).await?;

        assert!(
            values::<String>(&stack, &operator, "site/path", &site)
                .await?
                .is_empty(),
            "no read takes the misplaced fact"
        );
        assert_eq!(
            stack.divergences(),
            vec![Divergence {
                attribute: "site/path".into(),
                layer: bottom,
                scope: Some(name("state")),
            }],
            "the read meets it at the new head"
        );
        Ok(())
    }

    /// An attribute placed on a scope that no link of the stack binds, and
    /// that the bottom does not bind to its tree, has no layer in the
    /// stack: a fact of it in the bottom reaches no read, and is reported,
    /// as in a space whose stack links no `memory:device`.
    #[dialog_common::test]
    async fn it_reads_no_fact_of_a_scope_that_the_stack_does_not_bind() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("idx/name".parse()?, name("device")))?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let bottom = stack.layers()[0].address_entity();

        // A member commits the index fact into the subject.
        let other = repo.branch("main").open().perform(&operator).await?;
        let persona: Entity = "persona:1".parse()?;
        other
            .commit(stream::iter(vec![Instruction::Assert(
                Artifact {
                    the: "idx/name".parse()?,
                    of: persona.clone(),
                    is: Value::String("Eve".into()),
                    cause: None,
                },
                Pick::All,
            )]))
            .perform(&operator)
            .await?;
        stack.advance(&operator).await?;

        assert!(
            values::<String>(&stack, &operator, "idx/name", &persona)
                .await?
                .is_empty(),
            "no read takes the fact"
        );
        assert_eq!(
            stack.divergences(),
            vec![Divergence {
                attribute: "idx/name".into(),
                layer: bottom,
                scope: Some(name("device")),
            }]
        );
        Ok(())
    }

    /// A transient placed on a scope that the stack does not bind has no
    /// store that a read of the stack takes: dispatching it is refused, as
    /// a write of it is.
    #[dialog_common::test]
    async fn it_refuses_a_transient_of_a_scope_that_the_stack_does_not_bind() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        shared
            .overlay()
            .assert(Placement::new("cmd/open".parse()?, name("nowhere")))?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;
        let refused = stack
            .transaction()
            .dispatch(
                dialog_query::the!("cmd/open")
                    .of("doc:1".parse::<Entity>()?)
                    .is(true),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(
                refused,
                Err(StackError::Commit(CommitError::UnboundScope { .. }))
            ),
            "a transient of an unbound scope is refused: {refused:?}"
        );
        Ok(())
    }

    /// The values a `(the, of)` pair holds in a query's view.
    async fn seen<V: Scalar>(
        query: &StackQuery,
        env: &impl StackEnv,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<Value>> {
        let select = AttributeQuery::from(
            Term::<Relation>::from(the.parse::<Relation>()?)
                .of(Term::<Entity>::from(of.clone()))
                .is(Term::<V>::var("v")),
        );
        let claims = query.select(select).perform(env).try_vec().await?;
        Ok(claims.into_iter().map(|claim| claim.is).collect())
    }

    /// A transaction's query is the stack as the commit would leave
    /// it: a pending assert shows, a pending retract hides what a
    /// layer holds, a dispatched transient shows beside them, and the
    /// stack itself is untouched until the commit lands.
    #[dialog_common::test]
    async fn it_queries_a_transaction_as_if_committed() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::create().perform(&operator).await;
        shared
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("state")))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&state, &shared, name("shared"))
            .link(&top, &state, name("state"))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;

        let transaction = stack
            .transaction()
            .retract(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Draft".to_string()),
            )
            .retract(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .dispatch(dialog_query::the!("cmd/open").of(doc.clone()).is(true));
        let view = transaction.query();

        assert_eq!(
            seen::<String>(&view, &operator, "doc/title", &doc).await?,
            vec![Value::String("Draft".into())],
            "the pending assert shows and the pending retract hides the tree fact"
        );
        assert!(
            seen::<bool>(&view, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "the pending retract hides the state layer's fact too"
        );
        assert_eq!(
            seen::<bool>(&view, &operator, "cmd/open", &doc).await?,
            vec![Value::Boolean(true)],
            "the dispatched transient is in the view"
        );
        assert_eq!(
            values::<String>(&stack, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the stack itself is unchanged until the commit lands"
        );
        assert!(
            values::<bool>(&stack, &operator, "cmd/open", &doc)
                .await?
                .is_empty(),
            "and never sees the transient"
        );
        assert_eq!(
            stack
                .transaction()
                .query()
                .select(AttributeQuery::from(
                    Term::<Relation>::from(dialog_query::the!("doc/title"))
                        .of(Term::<Entity>::from(doc.clone()))
                        .is(Term::<String>::var("v")),
                ))
                .perform(&operator)
                .try_vec()
                .await?
                .len(),
            1,
            "an empty transaction's view is the stack's"
        );
        Ok(())
    }
}
