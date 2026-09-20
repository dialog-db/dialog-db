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
//!     .link(&state, &local, "memory:local".parse()?)
//!     .link(&local, &shared, "memory:shared".parse()?); // the bottom: placements live here
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
//! write ([`CommitError::Behind`]) rather than building on a head the
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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::{mem, slice};

use base58::ToBase58 as _;
use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    Artifact, ArtifactSelector, Changes, DialogArtifactsError, Entity, Instruction, Statement,
    Update as _, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve, Version as MemoryVersion};
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use parking_lot::RwLock;
use serde::Serialize;
use thiserror::Error;

use crate::placement::{Placements, Target};
use crate::repository::branch::session::{Composite, QueryEnv, session_metadata};
use crate::repository::branch::transaction::induce::{Witness, induce};
use crate::repository::source::{Source, SourceRef};
use crate::schema::DidExt as _;
use crate::{
    Branch, CommitError, Delta, Ephemeral, EphemeralError, EphemeralRevision, OpenEphemeral,
    PullError, PushError, RemoteSite, ResolveError, Revision, Snapshot, Subscription,
    TransactionBatch,
};

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
            Head::Tree(None) => crate::EMPTY_TREE_HASH.to_vec(),
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
            Layer::Branch(branch) if branch.upstream().is_some() => Audience::Peers,
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
                changes.associate_unique(
                    link_attr("repository"),
                    link.clone(),
                    Value::Entity(branch.of().this()),
                );
                changes.associate_unique(
                    link_attr("branch"),
                    link.clone(),
                    Value::String(branch.name().to_string()),
                );
            }
            Layer::Snapshot(snapshot) => {
                changes.associate_unique(
                    link_attr("repository"),
                    link.clone(),
                    Value::Entity(snapshot.of().this()),
                );
                changes.associate_unique(
                    link_attr("tree"),
                    link.clone(),
                    Value::Bytes(snapshot.revision().tree.hash().to_vec()),
                );
                changes.associate_unique(
                    link_attr("snapshot"),
                    link.clone(),
                    Value::Bytes(
                        serde_ipld_dagcbor::to_vec(&snapshot.revision())
                            .expect("a revision encodes"),
                    ),
                );
            }
            Layer::Ephemeral(ephemeral) => {
                changes.associate_unique(
                    link_attr("ephemeral"),
                    link.clone(),
                    Value::Entity(ephemeral.entity().clone()),
                );
            }
        }
    }
}

/// A `dialog.link/<name>` attribute.
fn link_attr(name: &str) -> dialog_artifacts::Attribute {
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
    /// The link entity: `link:<blake3(dagcbor{from, to})>` over the
    /// encloser's address entity and the enclosed layer's identity.
    entity: Entity,
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

/// What a link entity hashes: the encloser's address entity and the
/// enclosed layer's stack identity, in canonical dag-cbor like the
/// descriptor.
#[derive(Debug, Clone, Serialize)]
struct LinkKey<'a> {
    from: &'a str,
    to: &'a str,
}

/// `link:<base58(blake3(dagcbor{from, to}))>`.
fn link_entity(from: &Entity, to: &Entity) -> Entity {
    let key = LinkKey {
        from: from.as_str(),
        to: to.as_str(),
    };
    let bytes = serde_ipld_dagcbor::to_vec(&key).expect("two strings encode");
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

    /// Link layer `from` to layer `to` beneath it under `name`. A
    /// link that already exists is left alone.
    fn link(&mut self, from: usize, to: usize, name: Entity) -> Result<(), StackError> {
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
            .any(|link| link.to == to && link.name == name)
        {
            return Ok(());
        }
        self.links[from].push(Link {
            to,
            name,
            // Settled by `identify`, once the target's identity is known.
            entity: address,
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
            for link in &mut self.links[index] {
                descriptor_links.push((link.name.to_string(), self.ids[link.to].to_string()));
                link.entity = link_entity(&from, &self.ids[link.to]);
            }
            descriptor_links.sort();
            let descriptor = Descriptor {
                address: self.layers[index].address(),
                links: descriptor_links,
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
            changes.associate_unique(
                link_attr("from"),
                link.entity.clone(),
                Value::Entity(from.clone()),
            );
            changes.associate_unique(
                link_attr("to"),
                link.entity.clone(),
                Value::Entity(self.ids[link.to].clone()),
            );
            changes.associate_unique(
                link_attr("name"),
                link.entity.clone(),
                Value::Entity(link.name.clone()),
            );
            changes.associate_unique(
                link_attr("order"),
                link.entity.clone(),
                Value::UnsignedInt(order as u128),
            );
            changes.associate_unique(
                link_attr("revision"),
                link.entity.clone(),
                Value::Bytes(heads[link.to].bytes()),
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
}

/// A change to a stack's wiring, queued on a transaction and applied
/// when it commits.
#[derive(Debug, Clone)]
enum LinkEdit {
    /// Link `from` to `to` under `name`. A `from` not yet in the stack
    /// is placed beneath the top; a `to` not yet in the stack is
    /// placed beneath `from`.
    Link {
        from: Layer,
        to: Layer,
        name: Entity,
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
    /// [`StackTransaction::link`] would.
    pub fn link<F: AsLayer, T: AsLayer>(mut self, from: &F, to: &T, name: Entity) -> Self {
        self.edits.push(LinkEdit::Link {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
        });
        self
    }

    /// Walk the links, resolve the layers, verify the identities the
    /// links record against the shape found, and apply the queued
    /// wiring in one commit published bottom to top.
    pub async fn perform<Env>(self, env: &Env) -> Result<Stack, StackError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<crate::Hydrate>
            + Provider<dialog_artifacts::Preload>
            + Provider<dialog_artifacts::Speculation>
            + Provider<OpenEphemeral>
            + ConditionalSync
            + 'static,
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
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + Provider<crate::Hydrate>
        + ConditionalSync
        + 'static,
{
    let address = layer.address_entity();
    let from = ArtifactSelector::new()
        .the(link_attr("from"))
        .is(Value::Entity(address));
    let entities: Vec<Entity> = held(layer, from, env)
        .await?
        .into_iter()
        .map(|fact| fact.of)
        .collect();
    let mut recorded = Vec::with_capacity(entities.len());
    for entity in entities {
        let facts = held(layer, ArtifactSelector::new().of(entity.clone()), env).await?;
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
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + Provider<crate::Hydrate>
        + ConditionalSync
        + 'static,
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

/// Resolve the layer a link's address names, through the environment.
async fn resolve_layer<Env>(link: &Recorded, env: &Env) -> Result<Layer, StackError>
where
    Env: Provider<Resolve> + Provider<OpenEphemeral> + ConditionalSync,
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
/// expanded layers, the scope name, and the identity the link recorded.
type Edge = (usize, Entity, Entity);

/// A layer the walk expanded, with its resolved links.
type Expanded = (Layer, Vec<Edge>);

/// Walk `top`'s links downward into a topology, bottom first, every
/// layer's links in their recorded order, and verify that each link's
/// recorded identity is the identity of the shape found beneath it.
async fn walk<Env>(top: Layer, env: &Env) -> Result<Topology, StackError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + Provider<crate::Hydrate>
        + Provider<OpenEphemeral>
        + ConditionalSync
        + 'static,
{
    // Expand every reachable layer once, keyed by address; each keeps
    // the addresses its links name, in recorded order.
    let mut expanded: Vec<Expanded> = Vec::new();
    let mut edges: Vec<Vec<(Entity, Entity, Entity)>> = Vec::new();
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
            own.push((target.address_entity(), link.name, link.to));
            pending.push(target);
        }
        index_of.insert(address, expanded.len());
        expanded.push((layer, Vec::new()));
        edges.push(own);
    }
    for (index, own) in edges.into_iter().enumerate() {
        expanded[index].1 = own
            .into_iter()
            .map(|(address, name, to)| (index_of[&address], name, to))
            .collect();
    }

    // Bottom first: post-order from the top, links in recorded order.
    let mut order: Vec<usize> = Vec::with_capacity(expanded.len());
    let mut visited = vec![false; expanded.len()];
    fn visit(index: usize, expanded: &[Expanded], visited: &mut [bool], order: &mut Vec<usize>) {
        if mem::replace(&mut visited[index], true) {
            return;
        }
        for (to, _, _) in &expanded[index].1 {
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
        for (to, name, _) in targets {
            topology.link(from, placed[to], name.clone())?;
        }
    }
    topology.identify()?;
    // Every recorded identity must be the identity of the shape found.
    for (index, (_, targets)) in expanded.iter().enumerate() {
        for (to, _, recorded) in targets {
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
/// clone shares the wiring and the heads.
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
        Stack {
            state: Arc::new(RwLock::new(State {
                topology,
                published,
                versions,
                staged,
            })),
        }
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
        let state = self.state.read();
        (0..state.topology.layers.len())
            .map(|index| state.captured(index))
            .collect()
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

    /// Bring movement in: pull every layer that tracks an upstream,
    /// bottom to top, then [`advance`](Self::advance) over the result.
    ///
    /// This is the network half of keeping a stack current, and it
    /// costs a round trip per upstream, so it belongs to whatever
    /// already pulls branches on a schedule: the same sync loop, one
    /// call per stack. Movement this process can already see needs no
    /// pull; `advance` captures it alone.
    pub async fn pull<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        for layer in self.layers() {
            if let Layer::Branch(branch) = &layer
                && branch.upstream().is_some()
            {
                branch.refresh(env).await?;
                Box::pin(branch.pull().perform(env)).await?;
            }
        }
        self.advance(env).await
    }

    /// Capture movement this process can see, without the network:
    /// re-resolve every branch layer's head from storage, take the
    /// live heads as the published base, and stage and publish the
    /// wiring of every layer above a layer that moved.
    ///
    /// Anything staged and not yet published is dropped: its chain
    /// built on heads this supersedes, so it is stale wholesale, and
    /// the transaction that staged it is re-run on the fresh heads. An
    /// advance that fails part way leaves the published heads where
    /// they were, and the next one captures them.
    ///
    /// Cheap enough to call on a signal: a poll that found
    /// [`behind`](Self::behind), a commit made through another handle
    /// of a layer, a tab regaining focus. A stack whose layers only
    /// ever move through it never needs one.
    pub async fn advance<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
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
            state.staged = topology.layers.iter().map(|_| None).collect();
        }
        self.capture(&topology, BTreeMap::new(), live, &previous, env)
            .await?;
        self.publish(env).await
    }

    /// Send movement out: push every branch layer that tracks an
    /// upstream, bottom to top, so a pushed layer's wiring never names
    /// a head its upstream lacks. Pushes the published heads; staged
    /// commits are not pushed until [`publish`](Self::publish)ed.
    pub async fn push<Env>(&self, env: &Env) -> Result<(), StackError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<BlobRead>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Put>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<Fork<RemoteSite, Publish>>
            + Provider<Fork<RemoteSite, BlobImport>>
            + Provider<Fork<RemoteSite, BlobRead>>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        for layer in self.layers() {
            if let Layer::Branch(branch) = &layer
                && branch.upstream().is_some()
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
    /// re-run after a [`pull`](Self::pull).
    pub async fn publish<Env>(&self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: Provider<Publish> + Provider<Resolve> + ConditionalSync,
    {
        let layers = self.layers();
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
                    return Err(StackError::Publish {
                        layer: Box::new(layer.address_entity()),
                        source,
                    });
                }
            }
        }
        Ok(self.captured())
    }

    /// The bottom layer as a branch: where placements live and where
    /// undeclared attributes go. `None` when the bottom is not a
    /// branch, in which case the stack is read-only.
    fn primary(&self) -> Option<Branch> {
        match self.state.read().topology.layers.first() {
            Some(Layer::Branch(branch)) => Some(branch.clone()),
            _ => None,
        }
    }

    /// The composite a read sees: every layer beneath the top at the
    /// head the stack reads it at, the top live.
    pub(crate) fn composite(&self) -> Composite {
        self.composite_at(&self.topology(), &self.captured())
    }

    /// The composite with every branch beneath the top read at the
    /// given heads. Ephemeral layers are always live: they are
    /// process-local, written only through the stack, and cannot be
    /// read at an older sequence.
    fn composite_at(&self, topology: &Topology, heads: &[Head]) -> Composite {
        let top = topology.layers.len().saturating_sub(1);
        let mut composite = Composite::default();
        for (index, layer) in topology.layers.iter().enumerate() {
            match layer {
                Layer::Branch(branch)
                    if index == top && self.state.read().staged[index].is_none() =>
                {
                    composite.sources.push(Source::Branch(branch.clone()))
                }
                Layer::Branch(branch) => {
                    let revision = match heads.get(index) {
                        Some(Head::Tree(revision)) => revision.clone(),
                        _ => branch.revision(),
                    };
                    composite
                        .sources
                        .push(Source::Pinned(branch.clone(), revision))
                }
                Layer::Snapshot(snapshot) => {
                    composite.sources.push(Source::Snapshot(snapshot.clone()))
                }
                Layer::Ephemeral(ephemeral) => composite.ephemerals.push(ephemeral.clone()),
            }
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
        }
    }

    /// Start a transaction on this stack.
    pub fn transaction(&self) -> StackTransaction<'_> {
        StackTransaction {
            stack: self,
            changes: Changes::new(),
            transients: Changes::new(),
            edits: Vec::new(),
            into: BTreeMap::new(),
            stores: Vec::new(),
        }
    }

    /// Stage `batch` on layer `index` on top of `heads[index]`, with
    /// its links at `heads`. A branch layer extends its staged chain
    /// (or opens one on its published head); an ephemeral layer is
    /// written directly. A layer with nothing to write and no links
    /// is left alone; one whose link facts already hold stages a
    /// no-op and keeps its head.
    async fn refresh_links<Env>(
        &self,
        topology: &Topology,
        index: usize,
        heads: &[Head],
        mut batch: Changes,
        env: &Env,
    ) -> Result<Option<Head>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        if batch.is_empty() && topology.links[index].is_empty() {
            return Ok(None);
        }
        topology.link_facts(index, heads, &mut batch);
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
                ephemeral.apply(batch);
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
    async fn capture<Env>(
        &self,
        topology: &Topology,
        mut batches: BTreeMap<usize, Changes>,
        mut heads: Vec<Head>,
        previous: &[Head],
        env: &Env,
    ) -> Result<Vec<Head>, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let mut moved: Vec<bool> = heads
            .iter()
            .enumerate()
            .map(|(index, head)| previous.get(index) != Some(head))
            .collect();
        for index in 0..topology.layers.len() {
            let batch = batches.remove(&index).unwrap_or_default();
            if batch.is_empty() && !topology.reaches_moved(index, &moved) {
                continue;
            }
            if let Some(head) = self
                .refresh_links(topology, index, &heads, batch, env)
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

    /// Apply queued wiring edits to the shape: layers that enter bring
    /// their heads with them, layers the top no longer reaches leave
    /// with theirs, and every staged chain is dropped, since a chain
    /// built on the old shape is stale wholesale. Returns the link
    /// entities to retract on their enclosers.
    fn edit(&self, mut edits: Vec<LinkEdit>) -> Result<Vec<(Layer, Entity)>, StackError> {
        let mut live = self.state.write();
        // Validate the entire edit batch on a detached shape. A rejected
        // edit must preserve both the live topology and its staged chains.
        let mut state = State {
            topology: live.topology.clone(),
            published: live.published.clone(),
            versions: live.versions.clone(),
            staged: live.topology.layers.iter().map(|_| None).collect(),
        };
        // An edit applies once its encloser is in the stack, so a
        // transaction may name its links in any order: a layer that a
        // later edit enters beneath the top can be the encloser of an
        // earlier one. An encloser nothing enters is refused.
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
                applied = true;
                match edit {
                    LinkEdit::Link { from, to, name } => {
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
                        state.topology.link(from_index, to_index, name)?;
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
        *live = state;
        Ok(retractions)
    }
}

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
/// no link binds the scope, mirroring how the settled batch routes.
struct StackWitness<'a> {
    topology: &'a Topology,
    placements: &'a Placements,
    default: Option<Entity>,
    bottom: &'a Ephemeral,
}

impl Witness for StackWitness<'_> {
    fn round(&mut self, transients: &Changes) {
        let mut shares: BTreeMap<Option<usize>, Changes> = BTreeMap::new();
        for instruction in transients.clone().into_instructions() {
            let (op, artifact) = split(instruction);
            let targets: Vec<Option<usize>> = match self.placements.scope_of(&artifact.the) {
                None => vec![None],
                Some(scope) if Some(scope) == self.default.as_ref() => vec![None],
                Some(scope) => match self.topology.bound.get(scope) {
                    Some(indices) => indices.iter().map(|index| Some(*index)).collect(),
                    None => vec![None],
                },
            };
            for target in targets {
                apply(shares.entry(target).or_default(), op, artifact.clone());
            }
        }
        for (target, share) in shares {
            let store = match target {
                None => self.bottom,
                Some(index) => self.topology.layers[index].store(),
            };
            store.witness(share);
        }
    }
}

/// A transaction on a [`Stack`]: accumulates facts and wiring, then
/// routes each fact to the layers its attribute's scope is linked
/// under, applies the wiring, and stages the layers bottom to top.
pub struct StackTransaction<'a> {
    stack: &'a Stack,
    changes: Changes,
    transients: Changes,
    edits: Vec<LinkEdit>,
    /// Facts bound for a named scope regardless of their attributes'
    /// placements, by scope.
    into: BTreeMap<Entity, Changes>,
    /// Maintenance of the ephemeral layers under a scope, in order.
    stores: Vec<(Entity, Maintenance)>,
}

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
        claim.assert(&mut self.changes);
        self
    }

    /// Retract a claim.
    pub fn retract<C: Statement>(mut self, claim: C) -> Self {
        claim.retract(&mut self.changes);
        self
    }

    /// Dispatch a transient claim: visible to rule bodies during this
    /// commit's induction, never written anywhere.
    pub fn dispatch<C: Statement>(mut self, claim: C) -> Self {
        claim.assert(&mut self.transients);
        self
    }

    /// Assert a claim into the layers linked under `scope`, whatever
    /// its attributes' placements say. The explicit form of placement,
    /// for a writer that knows where a fact belongs when the schema
    /// does not say: session facts a process keeps for itself. Induction
    /// does not see these facts; they are written as given.
    pub fn assert_into<C: Statement>(mut self, scope: Entity, claim: C) -> Self {
        claim.assert(self.into.entry(scope).or_default());
        self
    }

    /// Retract a claim from the layers linked under `scope`; see
    /// [`assert_into`](Self::assert_into).
    pub fn retract_from<C: Statement>(mut self, scope: Entity, claim: C) -> Self {
        claim.retract(self.into.entry(scope).or_default());
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
    /// enters beneath the top; a `to` not yet in the stack enters
    /// beneath `from`. The audience rule, the ordering, and a
    /// snapshot's inability to link are checked when the transaction
    /// commits, and the link facts land on `from` with the rest of
    /// the commit.
    pub fn link<F: AsLayer, T: AsLayer>(mut self, from: &F, to: &T, name: Entity) -> Self {
        self.edits.push(LinkEdit::Link {
            from: from.as_layer(),
            to: to.as_layer(),
            name,
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

    /// Finalize into a commit command. Its `perform` stages; chain
    /// [`publish`](StackCommit::publish) to stage and publish in one
    /// step.
    pub fn commit(self) -> StackCommit<'a> {
        StackCommit {
            stack: self.stack,
            changes: self.changes,
            transients: self.transients,
            edits: self.edits,
            into: self.into,
            stores: self.stores,
        }
    }
}

/// Command staging a [`StackTransaction`].
pub struct StackCommit<'a> {
    stack: &'a Stack,
    changes: Changes,
    transients: Changes,
    edits: Vec<LinkEdit>,
    into: BTreeMap<Entity, Changes>,
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
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Head>, StackError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<dialog_artifacts::Speculation>
            + Provider<dialog_artifacts::Preload>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let stack = self.stack;
        let mut batches: BTreeMap<usize, Changes> = BTreeMap::new();

        // Wiring first: a re-shaped stack reads and routes as re-shaped.
        // With no edits the previous heads are the captured ones and
        // nothing counts as moved; with edits every layer counts as
        // moved, so every encloser rewrites its wiring (a no-op where
        // it already holds).
        let rewired = !self.edits.is_empty();
        let previous = if rewired {
            let retractions = stack.edit(self.edits)?;
            let topology = stack.topology();
            for (from, entity) in retractions {
                let index = topology
                    .index_of(&from)
                    .expect("an unlinked encloser stays in the stack");
                let facts = held(&from, ArtifactSelector::new().of(entity), env).await?;
                let batch = batches.entry(index).or_default();
                for fact in facts {
                    batch.dissociate(fact.the, fact.of, fact.is);
                }
            }
            Vec::new()
        } else {
            stack.captured()
        };
        let topology = stack.topology();

        // Resolve and validate every explicit scope before touching any store.
        let mut maintenance: BTreeMap<usize, (bool, HashSet<Entity>)> = BTreeMap::new();
        for (scope, operation) in self.stores {
            let Some(indices) = topology.bound.get(&scope) else {
                return Err(StackError::UnboundScope { scope });
            };
            for index in indices {
                if !matches!(&topology.layers[*index], Layer::Ephemeral(_)) {
                    return Err(StackError::NotEphemeral { scope });
                }
                let (clear, forgotten) = maintenance.entry(*index).or_default();
                match &operation {
                    Maintenance::Clear => *clear = true,
                    Maintenance::Forget(entities) => forgotten.extend(entities.iter().cloned()),
                }
            }
        }
        // Facts placed by the writer land where the scope's links say,
        // untouched by induction or the declarations.
        for (scope, changes) in self.into {
            let Some(indices) = topology.bound.get(&scope) else {
                return Err(StackError::UnboundScope { scope });
            };
            for instruction in changes.into_instructions() {
                let (op, artifact) = split(instruction);
                for index in indices {
                    apply(batches.entry(*index).or_default(), op, artifact.clone());
                }
            }
        }
        // Maintenance and explicitly placed replacement facts land together:
        // readers and observers never see an entity between forget and assert.
        for (index, (clear, forgotten)) in maintenance {
            let Layer::Ephemeral(ephemeral) = &topology.layers[index] else {
                unreachable!("maintenance targets were validated above");
            };
            let mut batch = batches.remove(&index).unwrap_or_default();
            // Scope maintenance clears user state, while the stack retains
            // its declared wiring. Re-stamp it in the same atomic instant.
            topology.link_facts(index, &stack.captured(), &mut batch);
            ephemeral.maintain(batch, clear, &forgotten);
            stack.state.write().published[index] = Head::Ephemeral(ephemeral.revision());
        }
        // Maintenance moved ephemeral heads: re-read what the stack reads at.
        let captured = stack.captured();

        if self.changes.is_empty() && self.transients.is_empty() && !rewired {
            return Ok(stack
                .capture(&topology, batches, captured.clone(), &previous, env)
                .await?);
        }
        let Some(primary) = stack.primary() else {
            return Err(CommitError::Detached.into());
        };
        // A write builds on the heads the stack reads at, like a
        // branch commit builds on its handle's head: induction reads
        // them, and each layer stages on top of its own.
        let composite = stack.composite_at(&topology, &captured);
        let source = match composite.sources.first() {
            Some(source) => source.as_ref(),
            None => SourceRef::from(&primary),
        };

        // Each round's transients are witnessed on the store of every
        // layer their scope is linked under, else the bottom's session
        // store, so an observer there sees the command that fired a
        // rule even though the commit folds it away. Resolved from the
        // declarations as staged before induction; a declaration
        // induction itself adds routes the settled batch below.
        let staged = Placements::resolve(source, &self.changes, env).await?;
        let mut witness = StackWitness {
            topology: &topology,
            default: staged.default_scope().cloned(),
            placements: &staged,
            bottom: primary.overlay(),
        };
        let mut changes = self.changes;
        induce(
            source,
            &composite,
            &staged,
            &mut changes,
            self.transients,
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
        let default = placements.default_scope().cloned();
        for instruction in changes.into_instructions() {
            let (op, artifact) = split(instruction);
            let targets: Vec<usize> = match placements.scope_of(&artifact.the) {
                None => vec![0],
                Some(scope) if Some(scope) == default.as_ref() => vec![0],
                Some(scope) => match topology.bound.get(scope) {
                    Some(indices) => indices.clone(),
                    None => match primary.bindings().target(scope) {
                        Some(Target::Tree) => vec![0],
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
            for target in targets {
                apply(batches.entry(target).or_default(), op, artifact.clone());
            }
        }

        Ok(stack
            .capture(&topology, batches, captured.clone(), &previous, env)
            .await?)
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
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Import>
            + Provider<Resolve>
            + Provider<Publish>
            + Provider<Identify>
            + Provider<Attest>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<dialog_artifacts::Speculation>
            + Provider<dialog_artifacts::Preload>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let stack = self.commit.stack;
        self.commit.perform(env).await?;
        stack.publish(env).await
    }
}

/// A query over a [`Stack`] at its captured heads, with optional
/// overlay facts. Created by [`Stack::query`].
pub struct StackQuery {
    stack: Stack,
    composite: Composite,
    changes: Changes,
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
            inner: Subscription::over(self.composite.clone(), self.changes.clone(), query),
        }
    }
}

/// A query command over a stack's composite, ready to be performed.
pub struct StackSelect<Q> {
    composite: Composite,
    changes: Changes,
    query: Q,
}

impl<Q: Application> StackSelect<Q> {
    /// Execute the query, returning a stream of results.
    pub fn perform<'a, Env>(self, env: &'a Env) -> impl Output<Q::Conclusion> + 'a
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<dialog_artifacts::Preload>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let StackSelect {
            composite,
            changes,
            query,
        } = self;
        async_stream::try_stream! {
            let operator = Identify
                .perform(env)
                .await
                .map_err(|e| DialogArtifactsError::Storage(format!("identify: {e}")))?;
            let mut overlay = changes;
            session_metadata(composite.sources.iter().map(Source::as_ref), &operator)
                .assert(&mut overlay);
            let query_env = QueryEnv::new(composite, overlay, env);
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
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<dialog_artifacts::Speculation>
            + Provider<dialog_artifacts::Preload>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
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

/// What an instruction does to its fact, apart from the fact.
#[derive(Debug, Clone, Copy)]
enum Op {
    Assert,
    Replace,
    Retract,
}

fn split(instruction: Instruction) -> (Op, Artifact) {
    match instruction {
        Instruction::Assert(artifact) => (Op::Assert, artifact),
        Instruction::Replace(artifact) => (Op::Replace, artifact),
        Instruction::Retract(artifact) => (Op::Retract, artifact),
    }
}

fn apply(changes: &mut Changes, op: Op, artifact: Artifact) {
    let Artifact { the, of, is, .. } = artifact;
    match op {
        Op::Assert => changes.associate(the, of, is),
        Op::Replace => changes.associate_unique(the, of, is),
        Op::Retract => changes.dissociate(the, of, is),
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::TestEnv;
    use crate::helpers::test_repo;
    use crate::{Demand, Drained};
    use crate::{Placement, RemoteSite};
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Value};
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::attribute::The;
    use dialog_query::types::Scalar;
    use dialog_query::{AttributeQuery, Term};
    use futures_util::TryStreamExt as _;

    fn name(name: &str) -> Entity {
        format!("memory:{name}").parse().expect("layer entity")
    }

    /// The values a `(the, of)` pair holds in a stack's composite read.
    async fn values<V: Scalar>(
        stack: &Stack,
        env: &(
             impl Provider<Get>
             + Provider<Put>
             + Provider<Resolve>
             + Provider<Identify>
             + Provider<crate::Hydrate>
             + Provider<dialog_artifacts::Preload>
             + Provider<dialog_artifacts::Speculation>
             + Provider<Fork<RemoteSite, Get>>
             + Provider<Fork<RemoteSite, Resolve>>
             + ConditionalSync
             + 'static
         ),
        the: &str,
        of: &Entity,
    ) -> Result<Vec<Value>> {
        let query = AttributeQuery::from(
            Term::<The>::from(the.parse::<The>()?)
                .of(Term::<Entity>::from(of.clone()))
                .is(Term::<V>::var("v")),
        );
        let claims = stack.query().select(query).perform(env).try_vec().await?;
        Ok(claims.into_iter().map(|claim| claim.is).collect())
    }

    /// The values a `(the, of)` pair holds in a branch's tree only.
    async fn committed(
        branch: &Branch,
        env: &(
             impl Provider<Get>
             + Provider<Put>
             + Provider<Resolve>
             + Provider<crate::Hydrate>
             + Provider<Fork<RemoteSite, Get>>
             + Provider<Fork<RemoteSite, Resolve>>
             + ConditionalSync
             + 'static
         ),
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
            Term::<The>::from(dialog_query::the!("ui/selected"))
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let link = link_entity(&state.entity().clone(), &stack.identities()[0]);

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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let link = link_entity(&local_address, &stack.identities()[0]);
        let own = link_entity(state.entity(), &stack.identities()[1]);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let link = link_entity(&state.entity().clone(), &stack.identities()[0]);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
        let shared = repo.branch("main").open().perform(&operator).await?;

        let top = Ephemeral::create().perform(&operator).await;
        let stack = Stack::open(top.clone())
            .link(&top, &shared, name("shared"))
            .perform(&operator)
            .await?;

        let mut subscription = stack.query().subscribe(AttributeQuery::from(
            Term::<The>::from(dialog_query::the!("doc/title"))
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let link = link_entity(&state.entity().clone(), &stack.identities()[0]);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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

    /// A failed wiring batch preserves its original shape and staged data.
    #[dialog_common::test]
    async fn it_preserves_a_stack_after_rejected_wiring() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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

    /// A link must name a layer already beneath the linking one.
    #[dialog_common::test]
    async fn it_rejects_a_link_to_an_unknown_line() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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

        let renamed = Stack::open(state.clone())
            .link(&state, &shared, name("base"))
            .perform(&operator)
            .await?;
        assert_ne!(
            renamed.identity(),
            over.identity(),
            "a layer name is part of the encloser's shape"
        );
        Ok(())
    }

    /// A name bound by two links from one layer fans a write out to
    /// both layers, and the composite read dedups the fact.
    #[dialog_common::test]
    async fn it_fans_out_a_name_bound_twice() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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

    /// A stack whose bottom is not a branch has nowhere for
    /// declarations or undeclared attributes to go: read-only.
    #[dialog_common::test]
    async fn it_refuses_to_transact_without_a_branch_bottom() -> Result<()> {
        let (operator, _profile) = test_operator_with_profile().await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        state.apply(rule);

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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
            demand.record(&ArtifactSelector::new().the("cmd.increment/counter".parse().unwrap()));
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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

    /// A writer that knows where a fact belongs says so: `assert_into`
    /// lands it under the scope regardless of placements, `forget`
    /// drops an entity's facts from the scope's layer, `clear` empties
    /// it, and none of it touches the tree.
    #[dialog_common::test]
    async fn it_writes_and_maintains_a_scope_explicitly() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let operator = TestEnv::new(operator);
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
            .assert_into(
                name("state"),
                dialog_query::the!("site/path")
                    .of(site.clone())
                    .is("/a".to_string()),
            )
            .assert_into(
                name("state"),
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
        demand.record(&paths);
        let observer = state.observe(demand);
        stack
            .transaction()
            .forget(name("state"), vec![site.clone()])
            .assert_into(
                name("state"),
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
            .assert_into(
                name("nowhere"),
                dialog_query::the!("site/path")
                    .of(site)
                    .is("/c".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(matches!(unbound, Err(StackError::UnboundScope { .. })));
        Ok(())
    }
}
