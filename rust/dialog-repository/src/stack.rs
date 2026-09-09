//! Stacks: lines linked under layer names, read as one composite and
//! written by placement.
//!
//! A **line** is a [`Branch`], a [`Snapshot`], or an [`Ephemeral`]
//! store. A **stack** is a list of lines, bottom first, where an upper
//! line may **link** a line beneath it under a layer name. Reads see
//! every line as one composite; a transaction routes each fact to the
//! line its attribute's layer is linked under, commits the lines
//! bottom to top, and every enclosing line that commits refreshes its
//! links with the heads it saw.
//!
//! ```no_run
//! # use dialog_repository::{Branch, Ephemeral, Stack};
//! # fn example(shared: Branch, local: Branch) -> anyhow::Result<()> {
//! let state = Ephemeral::new();
//! let build = Stack::builder()
//!     .line(shared.clone()) // the bottom: placements live here
//!     .line(local.clone())
//!     .link(&shared, "memory:shared".parse()?)
//!     .line(state.clone())
//!     .link(&local, "memory:local".parse()?)
//!     .build(); // `.perform(&env).await?` checks the shape and writes the links
//! # let _ = build;
//! # Ok(())
//! # }
//! ```
//!
//! # Links, as facts
//!
//! A link is held by the enclosing line, in its own store, as facts on
//! a content-addressed link entity:
//!
//! ```text
//! <link> dialog.link/from      <address entity of the encloser>
//! <link> dialog.link/to        <stack identity of the enclosed line>
//! <link> dialog.link/name      <layer name>
//! <link> dialog.link/revision  <head of the enclosed line, as last seen>
//! ```
//!
//! plus the enclosed line's address (`dialog.link/repository` and
//! `dialog.link/branch` for a branch, `dialog.link/ephemeral` for an
//! ephemeral line). Wiring lifts: an encloser also holds a copy of
//! every link fact its enclosed lines hold, verbatim, so the top line
//! carries the whole stack's wiring and every edge is queryable from
//! it alone.
//!
//! A stack commit refreshes the wiring on every line above a line
//! that moved, bottom to top, so after the commit the top line's head
//! transitively names the head of every line beneath it: one hash for
//! the whole composite. A link whose target did not move is a no-op
//! refresh and mints nothing. Linking is capturing: a line that should
//! not record another's head does not link it, and sits beside it
//! under a common encloser instead.
//!
//! # Reads are pinned
//!
//! A stack is read at its top's head. Every line beneath the top is
//! read at the revision the wiring captured, not at its live head, so
//! what a read sees is exactly what the top's hash names. Movement
//! enters a stack the way it enters a branch: on [`pull`](Stack::pull),
//! which pulls each line from its upstream and then captures every
//! live head, and leaves it on [`push`](Stack::push). A stack commit
//! captures too, since a write builds on the live heads. Reads and
//! subscription polls never write: a line that moved outside the
//! stack stays invisible until the next pull.
//!
//! # Identity
//!
//! Every line has an **address** (where its head lives) and a **stack
//! identity**: the hash of its descriptor, `{address, links: sorted
//! [(name, id(to))]}` in canonical dag-cbor. The bottom line's identity
//! is the same on every replica; an encloser's identity covers the
//! shape beneath it, so a cycle is unconstructible and a re-shaped line
//! re-identifies everything above it.
//!
//! # The audience rule
//!
//! A line may link a line beneath it only if that line's
//! [`Audience`] contains its own: a link is a revision hash, and a
//! reader of the encloser must be able to resolve it. A replicated
//! branch may not link a process-local store; the store may link the
//! branch.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use base58::ToBase58 as _;
use dialog_artifacts::{
    Artifact, Changes, DialogArtifactsError, Entity, Instruction, Statement, Update as _, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::{Blake3Hash, ConditionalSync};
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{Import as BlobImport, Read as BlobRead};
use dialog_effects::memory::{Publish, Resolve};
use dialog_query::error::EvaluationError;
use dialog_query::query::{Application, Output};
use parking_lot::RwLock;
use serde::Serialize;
use thiserror::Error;

use crate::placement::{Placements, Target};
use crate::repository::branch::session::{Composite, QueryEnv, session_metadata};
use crate::repository::branch::transaction::commit_settled;
use crate::repository::branch::transaction::induce::induce;
use crate::repository::source::{Source, SourceRef};
use crate::schema::DidExt as _;
use crate::{
    Branch, CommitError, Delta, Ephemeral, EphemeralRevision, PullError, PushError, RemoteSite,
    Revision, Snapshot, Subscription,
};

/// Who can read a line: the set of principals its facts reach.
/// Ordered by inclusion, narrowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Audience {
    /// This process only.
    Process,
    /// This device: durable here, never pushed.
    Device,
    /// The line's peers: pushed to its upstreams.
    Peers,
}

/// A line in a stack.
#[derive(Debug, Clone)]
pub enum Line {
    /// A branch: durable, head in a cell.
    Branch(Branch),
    /// A snapshot: durable, head by value, read-only in a stack.
    Snapshot(Snapshot),
    /// An ephemeral store.
    Ephemeral(Ephemeral),
}

impl From<Branch> for Line {
    fn from(branch: Branch) -> Self {
        Line::Branch(branch)
    }
}

impl From<Snapshot> for Line {
    fn from(snapshot: Snapshot) -> Self {
        Line::Snapshot(snapshot)
    }
}

impl From<Ephemeral> for Line {
    fn from(ephemeral: Ephemeral) -> Self {
        Line::Ephemeral(ephemeral)
    }
}

/// Something a link can target: a line, or a handle to one.
pub trait AsLine {
    /// This as a line.
    fn as_line(&self) -> Line;
}

impl AsLine for Line {
    fn as_line(&self) -> Line {
        self.clone()
    }
}

impl AsLine for Branch {
    fn as_line(&self) -> Line {
        Line::Branch(self.clone())
    }
}

impl AsLine for Snapshot {
    fn as_line(&self) -> Line {
        Line::Snapshot(self.clone())
    }
}

impl AsLine for Ephemeral {
    fn as_line(&self) -> Line {
        Line::Ephemeral(self.clone())
    }
}

/// Where a line's head lives, as the descriptor records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Address {
    Branch { repository: String, branch: String },
    Snapshot { repository: String, tree: String },
    Ephemeral { id: String },
}

/// The shape of a line: its address and what it links, in canonical
/// order. Its hash is the line's stack identity.
#[derive(Debug, Clone, Serialize)]
struct Descriptor {
    address: Address,
    links: Vec<(String, String)>,
}

/// A head as a link records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Head {
    /// A tree line's head, or `None` for a branch with no commits.
    Tree(Option<Revision>),
    /// An ephemeral line's head.
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

impl Line {
    /// Whether two handles name the same line.
    fn same(&self, other: &Line) -> bool {
        match (self, other) {
            (Line::Branch(a), Line::Branch(b)) => a.of() == b.of() && a.name() == b.name(),
            (Line::Snapshot(a), Line::Snapshot(b)) => {
                a.of() == b.of() && a.revision() == b.revision()
            }
            (Line::Ephemeral(a), Line::Ephemeral(b)) => a.is(b),
            _ => false,
        }
    }

    /// Who can read this line.
    pub fn audience(&self) -> Audience {
        match self {
            Line::Branch(branch) if branch.upstream().is_some() => Audience::Peers,
            Line::Branch(_) | Line::Snapshot(_) => Audience::Device,
            Line::Ephemeral(_) => Audience::Process,
        }
    }

    fn address(&self) -> Address {
        match self {
            Line::Branch(branch) => Address::Branch {
                repository: branch.of().to_string(),
                branch: branch.name().to_string(),
            },
            Line::Snapshot(snapshot) => Address::Snapshot {
                repository: snapshot.of().to_string(),
                tree: snapshot.revision().tree.hash().to_base58(),
            },
            Line::Ephemeral(ephemeral) => Address::Ephemeral {
                id: ephemeral.entity().to_string(),
            },
        }
    }

    /// The entity standing for this line's address in link facts.
    pub fn address_entity(&self) -> Entity {
        match self {
            Line::Branch(branch) => format!("line:{}/{}", branch.of(), branch.name())
                .parse()
                .expect("a DID and a branch name form an opaque URI path"),
            Line::Snapshot(snapshot) => format!(
                "line:{}/{}",
                snapshot.of(),
                snapshot.revision().tree.hash().to_base58()
            )
            .parse()
            .expect("a DID and a hash form an opaque URI path"),
            Line::Ephemeral(ephemeral) => ephemeral.entity().clone(),
        }
    }

    /// This line's head now.
    pub fn head(&self) -> Head {
        match self {
            Line::Branch(branch) => Head::Tree(branch.revision()),
            Line::Snapshot(snapshot) => Head::Tree(Some(snapshot.revision())),
            Line::Ephemeral(ephemeral) => Head::Ephemeral(ephemeral.revision()),
        }
    }

    /// The address facts a link to this line carries, so an opener can
    /// resolve the target from the link alone.
    fn address_facts(&self, link: &Entity, changes: &mut Changes) {
        match self {
            Line::Branch(branch) => {
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
            Line::Snapshot(snapshot) => {
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
            }
            Line::Ephemeral(ephemeral) => {
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

/// A link from an enclosing line to a line beneath it.
#[derive(Debug, Clone)]
struct Link {
    /// Index of the enclosed line in the stack.
    to: usize,
    /// The layer name the link binds.
    name: Entity,
    /// The link entity: `link:<blake3(dagcbor{from, to})>` over the
    /// encloser's address entity and the enclosed line's identity.
    entity: Entity,
}

/// Why a stack could not be built.
#[derive(Debug, Error)]
pub enum StackError {
    /// A link names a line the stack does not hold beneath the
    /// linking line.
    #[error("Line {from} links a line that is not beneath it in the stack")]
    UnknownLine {
        /// The address entity of the linking line.
        from: Entity,
    },
    /// A link would let a wider audience reference a head it cannot
    /// resolve.
    #[error("Line {from} ({from_audience:?}) may not link {to} ({to_audience:?})")]
    Audience {
        /// The address entity of the linking line.
        from: Entity,
        /// Its audience.
        from_audience: Audience,
        /// The address entity of the linked line.
        to: Entity,
        /// Its audience.
        to_audience: Audience,
    },
    /// A snapshot cannot hold links: nothing reached through a
    /// snapshot handle can write.
    #[error("Snapshot {from} cannot link other lines")]
    SnapshotEncloser {
        /// The address entity of the snapshot.
        from: Entity,
    },
    /// A descriptor could not be encoded.
    #[error("Failed to encode a stack descriptor: {0}")]
    Encode(String),
    /// Writing a line's link facts failed.
    #[error("Failed to write link facts: {0}")]
    Commit(#[from] CommitError),
    /// Pulling a line from its upstream failed.
    #[error("Failed to pull a line: {0}")]
    Pull(#[from] PullError),
    /// Pushing a line to its upstream failed.
    #[error("Failed to push a line: {0}")]
    Push(#[from] PushError),
}

/// A line as the builder holds it: with the links declared so far.
#[derive(Debug, Clone)]
struct Pending {
    line: Line,
    links: Vec<(usize, Entity)>,
}

/// Builder for a [`Stack`]: add lines bottom first, link each upper
/// line to lines beneath it under layer names, then
/// [`build`](StackBuilder::build).
#[derive(Debug, Default)]
pub struct StackBuilder {
    lines: Vec<Pending>,
}

impl StackBuilder {
    /// Add a line above every line added so far.
    pub fn line(mut self, line: impl Into<Line>) -> Self {
        self.lines.push(Pending {
            line: line.into(),
            links: Vec::new(),
        });
        self
    }

    /// Link the most recently added line to `to`, which must already
    /// be in the stack beneath it, under `name`. A name may be bound
    /// by several links from one line; a write to it then lands in
    /// every line so bound.
    pub fn link<L: AsLine>(mut self, to: &L, name: Entity) -> Self {
        let to = to.as_line();
        let Some(last) = self.lines.len().checked_sub(1) else {
            return self;
        };
        // An unknown target is recorded as a link to the linking line
        // itself, which `build` rejects with the right error.
        let index = self.lines[..last]
            .iter()
            .position(|pending| pending.line.same(&to))
            .unwrap_or(last);
        self.lines[last].links.push((index, name));
        self
    }

    /// Validate and assemble the stack, writing every enclosing line's
    /// link facts.
    pub fn build(self) -> Build {
        Build { lines: self.lines }
    }
}

/// Command assembling a stack; see [`StackBuilder::build`].
#[derive(Debug)]
pub struct Build {
    lines: Vec<Pending>,
}

impl Build {
    /// Check the shape, derive identities, write link facts, and
    /// return the stack.
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
            + ConditionalSync
            + 'static,
    {
        let mut ids: Vec<Entity> = Vec::with_capacity(self.lines.len());
        let mut links: Vec<Vec<Link>> = Vec::with_capacity(self.lines.len());
        for (index, pending) in self.lines.iter().enumerate() {
            let from = pending.line.address_entity();
            if !pending.links.is_empty() && matches!(pending.line, Line::Snapshot(_)) {
                return Err(StackError::SnapshotEncloser { from });
            }
            let mut own: Vec<Link> = Vec::with_capacity(pending.links.len());
            let mut descriptor_links: Vec<(String, String)> = Vec::new();
            for (to, name) in &pending.links {
                if *to >= index {
                    return Err(StackError::UnknownLine { from });
                }
                let target = &self.lines[*to].line;
                let (from_audience, to_audience) = (pending.line.audience(), target.audience());
                if to_audience < from_audience {
                    return Err(StackError::Audience {
                        from,
                        from_audience,
                        to: target.address_entity(),
                        to_audience,
                    });
                }
                descriptor_links.push((name.to_string(), ids[*to].to_string()));
                own.push(Link {
                    to: *to,
                    name: name.clone(),
                    entity: link_entity(&from, &ids[*to]),
                });
            }
            descriptor_links.sort();
            let descriptor = Descriptor {
                address: pending.line.address(),
                links: descriptor_links,
            };
            let bytes = serde_ipld_dagcbor::to_vec(&descriptor)
                .map_err(|error| StackError::Encode(error.to_string()))?;
            ids.push(identity(&bytes));
            links.push(own);
        }

        // A name binds the lines *linked under it*, not the linking
        // lines: `local.link(&shared, "memory:shared")` makes
        // `memory:shared` route to shared.
        let mut bound: HashMap<Entity, Vec<usize>> = HashMap::new();
        for own in &links {
            for link in own {
                let targets = bound.entry(link.name.clone()).or_default();
                if !targets.contains(&link.to) {
                    targets.push(link.to);
                }
            }
        }

        let stack = Stack {
            lines: self.lines.into_iter().map(|pending| pending.line).collect(),
            ids,
            links,
            bound,
            captured: Arc::new(RwLock::new(Vec::new())),
        };
        // Every encloser records its wiring now, at the heads it sees,
        // and the stack reads at those heads from here on.
        stack.capture(BTreeMap::new(), env).await?;
        Ok(stack)
    }
}

/// What a link entity hashes: the encloser's address entity and the
/// enclosed line's stack identity, in canonical dag-cbor like the
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

/// Lines linked under layer names, read as one composite and written
/// by placement. Built by [`Stack::builder`]; cheap to clone.
#[derive(Debug, Clone)]
pub struct Stack {
    /// Bottom first.
    lines: Vec<Line>,
    /// Each line's stack identity, parallel to `lines`.
    ids: Vec<Entity>,
    /// Each line's links, parallel to `lines`.
    links: Vec<Vec<Link>>,
    /// Layer name → the lines linked under it.
    bound: HashMap<Entity, Vec<usize>>,
    /// What the stack reads each line at: the heads the last stack
    /// commit or pull captured, bottom first. Shared by clones.
    captured: Arc<RwLock<Vec<Head>>>,
}

impl Stack {
    /// Start building a stack.
    pub fn builder() -> StackBuilder {
        StackBuilder::default()
    }

    /// The lines, bottom first.
    pub fn lines(&self) -> &[Line] {
        &self.lines
    }

    /// The stack's identity: the top line's, which covers every line
    /// beneath it.
    pub fn identity(&self) -> &Entity {
        self.ids
            .last()
            .expect("a built stack holds at least one line")
    }

    /// Every line's stack identity, bottom first.
    pub fn identities(&self) -> &[Entity] {
        &self.ids
    }

    /// The lines linked under `name`.
    pub fn layer(&self, name: &Entity) -> Vec<&Line> {
        self.bound
            .get(name)
            .map(|indices| indices.iter().map(|index| &self.lines[*index]).collect())
            .unwrap_or_default()
    }

    /// Every line's live head now, bottom first. What the stack reads
    /// at is [`captured`](Self::captured); the two differ exactly when
    /// a line moved outside the stack since the last capture.
    pub fn heads(&self) -> Vec<Head> {
        self.lines.iter().map(Line::head).collect()
    }

    /// The heads the stack reads each line at, bottom first: what the
    /// last stack commit or [`pull`](Self::pull) captured.
    pub fn captured(&self) -> Vec<Head> {
        self.captured.read().clone()
    }

    /// Whether some line moved outside the stack since the last
    /// capture: its live head differs from what the stack reads at.
    /// [`pull`](Self::pull) catches up.
    pub fn behind(&self) -> bool {
        self.heads() != self.captured()
    }

    /// Bring movement in: pull every branch line that tracks an
    /// upstream, bottom to top, then capture every line's live head,
    /// refreshing the wiring on every line above a line that moved.
    /// The stack reads at the result from now on. Lines with no
    /// upstream are only captured, so on a stack of local lines this
    /// is exactly "notice what moved outside the stack".
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
            + ConditionalSync
            + 'static,
    {
        for line in &self.lines {
            if let Line::Branch(branch) = line
                && branch.upstream().is_some()
            {
                Box::pin(branch.pull().perform(env)).await?;
            }
        }
        Ok(self.capture(BTreeMap::new(), env).await?)
    }

    /// Send movement out: push every branch line that tracks an
    /// upstream, bottom to top, so a pushed line's wiring never names
    /// a head its upstream lacks. A stack commit never pushes, like a
    /// branch commit; ephemeral lines never leave the process.
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
            + ConditionalSync
            + 'static,
    {
        for line in &self.lines {
            if let Line::Branch(branch) = line
                && branch.upstream().is_some()
            {
                Box::pin(branch.push().perform(env)).await?;
            }
        }
        Ok(())
    }

    /// The bottom line as a branch: where placements live and where
    /// undeclared attributes go. `None` when the bottom is not a
    /// branch, in which case the stack is read-only.
    fn primary(&self) -> Option<&Branch> {
        match self.lines.first() {
            Some(Line::Branch(branch)) => Some(branch),
            _ => None,
        }
    }

    /// The composite a read sees: every line beneath the top pinned at
    /// its captured head, the top live.
    pub(crate) fn composite(&self) -> Composite {
        self.composite_at(&self.captured())
    }

    /// The composite with every branch beneath the top read at the
    /// given heads. Ephemeral lines are always live: they are
    /// process-local and cannot be read at an older sequence.
    fn composite_at(&self, heads: &[Head]) -> Composite {
        let top = self.lines.len().saturating_sub(1);
        let mut composite = Composite::default();
        for (index, line) in self.lines.iter().enumerate() {
            match line {
                Line::Branch(branch) if index == top => {
                    composite.sources.push(Source::Branch(branch.clone()))
                }
                Line::Branch(branch) => {
                    let revision = match heads.get(index) {
                        Some(Head::Tree(revision)) => revision.clone(),
                        _ => branch.revision(),
                    };
                    composite
                        .sources
                        .push(Source::Pinned(branch.clone(), revision))
                }
                Line::Snapshot(snapshot) => {
                    composite.sources.push(Source::Snapshot(snapshot.clone()))
                }
                Line::Ephemeral(ephemeral) => composite.ephemerals.push(ephemeral.clone()),
            }
        }
        composite
    }

    /// Open a query over the whole stack at its captured heads. Use
    /// [`select`](StackQuery::select) or
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
        }
    }

    /// The wiring line `index` holds, at the heads its targets have
    /// now, folded into `changes`: its own link facts, plus a verbatim
    /// copy of every link fact each linked line holds. Bottom to top
    /// commits refresh the linked lines first, so the copy is what
    /// they hold after the same stack commit.
    fn link_facts(&self, index: usize, changes: &mut Changes) {
        let from = self.lines[index].address_entity();
        for link in &self.links[index] {
            let target = &self.lines[link.to];
            self.link_facts(link.to, changes);
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
                link_attr("revision"),
                link.entity.clone(),
                Value::Bytes(target.head().bytes()),
            );
            target.address_facts(&link.entity, changes);
        }
    }

    /// Commit `batch` to line `index` with its links refreshed to the
    /// heads its targets have now. A line with nothing to write and
    /// no links is left alone; one whose link facts are already
    /// current commits as a no-op and keeps its head.
    async fn refresh_links<Env>(
        &self,
        index: usize,
        mut batch: Changes,
        env: &Env,
    ) -> Result<Option<Head>, CommitError>
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
            + ConditionalSync
            + 'static,
    {
        if batch.is_empty() && self.links[index].is_empty() {
            return Ok(None);
        }
        self.link_facts(index, &mut batch);
        match &self.lines[index] {
            Line::Branch(branch) => {
                let revision =
                    commit_settled(SourceRef::from(branch), batch, false, false, env).await?;
                Ok(Some(Head::Tree(Some(revision))))
            }
            Line::Ephemeral(ephemeral) => {
                ephemeral.apply(batch);
                Ok(Some(Head::Ephemeral(ephemeral.revision())))
            }
            Line::Snapshot(_) => Err(CommitError::Detached),
        }
    }
}

/// A transaction on a [`Stack`]: accumulates facts, then routes each
/// to the line its attribute's layer is linked under and commits the
/// lines bottom to top.
pub struct StackTransaction<'a> {
    stack: &'a Stack,
    changes: Changes,
    transients: Changes,
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

    /// Finalize into a commit command.
    pub fn commit(self) -> StackCommit<'a> {
        StackCommit {
            stack: self.stack,
            changes: self.changes,
            transients: self.transients,
        }
    }
}

/// Command committing a [`StackTransaction`].
pub struct StackCommit<'a> {
    stack: &'a Stack,
    changes: Changes,
    transients: Changes,
}

impl StackCommit<'_> {
    /// Induce against the composite, route by placement, commit bottom
    /// to top. Returns every line's head afterwards, bottom first.
    pub async fn perform<Env>(self, env: &Env) -> Result<Vec<Head>, CommitError>
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
            + ConditionalSync
            + 'static,
    {
        let stack = self.stack;
        let mut batches: BTreeMap<usize, Changes> = BTreeMap::new();
        if self.changes.is_empty() && self.transients.is_empty() {
            return stack.capture(batches, env).await;
        }
        let Some(primary) = stack.primary() else {
            return Err(CommitError::Detached);
        };
        let source = SourceRef::from(primary);
        // A write builds on the live heads, so its induction reads
        // them: the commit captures the stack as it is now before it
        // writes, then captures what it wrote.
        let composite = stack.composite_at(&stack.heads());

        let mut changes = self.changes;
        induce(source, &composite, &mut changes, self.transients, env).await?;

        // Route by placement: the primary holds the declarations, the
        // stack's links bind the names. A name no link binds falls
        // back to the primary's own bindings, so a single-line stack
        // routes exactly as the branch would.
        let placements = Placements::resolve(source, &changes, env).await?;
        let default = placements.default_layer().cloned();
        for instruction in changes.into_instructions() {
            let (op, artifact) = split(instruction);
            let targets: Vec<usize> = match placements.layer_of(&artifact.the) {
                None => vec![0],
                Some(layer) if Some(layer) == default.as_ref() => vec![0],
                Some(layer) => match stack.bound.get(layer) {
                    Some(indices) => indices.clone(),
                    None => match primary.bindings().target(layer) {
                        Some(Target::Tree) => vec![0],
                        Some(Target::Session) => {
                            // The primary's own store is not a stack
                            // line; write it directly.
                            let mut own = Changes::new();
                            apply(&mut own, op, artifact);
                            primary.overlay().apply(own);
                            continue;
                        }
                        None => {
                            return Err(CommitError::UnboundLayer {
                                attribute: artifact.the.to_string(),
                                layer: layer.to_string(),
                            });
                        }
                    },
                },
            };
            for target in targets {
                apply(batches.entry(target).or_default(), op, artifact.clone());
            }
        }

        stack.capture(batches, env).await
    }
}

impl Stack {
    /// Commit each line's batch bottom to top, every line after
    /// everything beneath it, writing its own share and its wiring at
    /// the heads it now sees. A line whose wiring already names the
    /// current heads and that has nothing of its own to write is a
    /// no-op commit and keeps its head, so the refresh reaches
    /// exactly the lines above a line that moved. The resulting heads
    /// are what the stack reads at from now on.
    async fn capture<Env>(
        &self,
        mut batches: BTreeMap<usize, Changes>,
        env: &Env,
    ) -> Result<Vec<Head>, CommitError>
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
            + ConditionalSync
            + 'static,
    {
        let mut heads = Vec::with_capacity(self.lines.len());
        for index in 0..self.lines.len() {
            let batch = batches.remove(&index).unwrap_or_default();
            let head = self
                .refresh_links(index, batch, env)
                .await?
                .unwrap_or_else(|| self.lines[index].head());
            heads.push(head);
        }
        *self.captured.write() = heads.clone();
        Ok(heads)
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
    /// written, so a line that moved outside the stack is not seen
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
            + ConditionalSync
            + 'static,
    {
        self.inner.retarget(self.stack.composite());
        self.inner.poll(env).await
    }

    /// Whether the stack is behind its lines' live heads; see
    /// [`Stack::behind`].
    pub fn behind(&self) -> bool {
        self.stack.behind()
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
    use crate::helpers::test_repo;
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

    /// A branch bottom, an ephemeral state line linked over it: a
    /// transaction routes the placed attribute to the state line and
    /// the rest to the tree, the composite read joins them, and a
    /// subscription over the stack maintains the state half.
    #[dialog_common::test]
    async fn it_routes_by_name_across_lines() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::new();

        shared
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, name("state")))
            .commit()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        // A name binds where its link points: `memory:shared` is the
        // bottom, and `memory:state` needs a line above state to link
        // it under that name.
        let stack = Stack::builder()
            .line(shared.clone())
            .line(state.clone())
            .link(&shared, name("shared"))
            .line(Ephemeral::new())
            .link(&state, name("state"))
            .build()
            .perform(&operator)
            .await?;
        assert_eq!(stack.lines().len(), 3);

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
            "the placed attribute lands in the state line"
        );
        assert!(
            !state
                .scan(&ArtifactSelector::new().the("dialog.link/to".parse()?))
                .is_empty(),
            "beside the state line's own link facts"
        );
        assert!(
            shared.overlay().is_empty(),
            "and not in the bottom's own store"
        );
        assert_eq!(
            values::<bool>(&stack, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "the composite read joins the state line"
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
            .perform(&operator)
            .await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the state line's change propagates");
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
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;

        shared
            .transaction()
            .assert(Placement::new("local/note".parse()?, name("local")))
            .commit()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let stack = Stack::builder()
            .line(shared.clone())
            .line(local.clone())
            .link(&shared, name("shared"))
            .line(Ephemeral::new())
            .link(&local, name("local"))
            .build()
            .perform(&operator)
            .await?;
        local.refresh(&operator).await?;

        let link = link_entity(
            &Line::Branch(local.clone()).address_entity(),
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
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;
        let state = Ephemeral::new();

        let stack = Stack::builder()
            .line(shared.clone())
            .line(local.clone())
            .line(state.clone())
            .link(&shared, name("shared"))
            .link(&local, name("local"))
            .build()
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

    /// Wiring lifts: the top holds a verbatim copy of every link fact
    /// beneath it, so the whole topology is readable from the top
    /// alone, and a stack commit refreshes the copy with the
    /// original.
    #[dialog_common::test]
    async fn it_copies_wiring_upward() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let local = repo.branch("main.local").open().perform(&operator).await?;
        let state = Ephemeral::new();

        let stack = Stack::builder()
            .line(shared.clone())
            .line(local.clone())
            .link(&shared, name("shared"))
            .line(state.clone())
            .link(&local, name("local"))
            .build()
            .perform(&operator)
            .await?;

        // local's own link to shared, as local holds it.
        let local_address = Line::Branch(local.clone()).address_entity();
        let link = link_entity(&local_address, &stack.identities()[0]);
        let revision_of = |link: &Entity| {
            ArtifactSelector::new()
                .the("dialog.link/revision".parse().expect("attribute"))
                .of(link.clone())
        };
        let from_of = |link: &Entity| {
            ArtifactSelector::new()
                .the("dialog.link/from".parse().expect("attribute"))
                .of(link.clone())
        };
        let values = |facts: Vec<Artifact>| facts.into_iter().map(|a| a.is).collect::<Vec<_>>();
        assert_eq!(
            values(state.scan(&from_of(&link))),
            vec![Value::Entity(local_address.clone())],
            "the top holds local's link with local as its from"
        );
        assert_eq!(
            values(state.scan(&revision_of(&link))),
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())],
            "at the head local captured"
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
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;
        local.refresh(&operator).await?;
        assert_eq!(
            values(state.scan(&revision_of(&link))),
            vec![Value::Bytes(Head::Tree(shared.revision()).bytes())],
            "the copy follows the original in the same commit"
        );
        assert_eq!(
            committed(&local, &operator, "dialog.link/revision", &link).await?,
            values(state.scan(&revision_of(&link))),
            "and the two agree"
        );
        Ok(())
    }

    /// A stack reads every line beneath its top at the captured head:
    /// a commit that bypasses the stack is invisible until the stack
    /// pulls, and then the top's wiring names the new head.
    #[dialog_common::test]
    async fn it_reads_at_captured_heads_until_pulled() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::new();

        let stack = Stack::builder()
            .line(shared.clone())
            .line(state.clone())
            .link(&shared, name("shared"))
            .build()
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
        let shared = repo.branch("main").open().perform(&operator).await?;

        let stack = Stack::builder()
            .line(shared.clone())
            .line(Ephemeral::new())
            .link(&shared, name("shared"))
            .build()
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

    /// A wider audience may not link a narrower one: a branch cannot
    /// link an ephemeral store beneath it.
    #[dialog_common::test]
    async fn it_rejects_an_audience_violation() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::new();

        let result = Stack::builder()
            .line(state.clone())
            .line(shared.clone())
            .link(&state, name("state"))
            .build()
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

    /// A link must name a line already beneath the linking one.
    #[dialog_common::test]
    async fn it_rejects_a_link_to_an_unknown_line() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let elsewhere = Ephemeral::new();

        let result = Stack::builder()
            .line(shared.clone())
            .line(Ephemeral::new())
            .link(&elsewhere, name("state"))
            .build()
            .perform(&operator)
            .await;
        assert!(
            matches!(result, Err(StackError::UnknownLine { .. })),
            "expected an unknown-line refusal, got {result:?}"
        );
        Ok(())
    }

    /// Identities are a pure function of shape: two stacks over the
    /// same branch agree on its identity, and linking changes the
    /// encloser's identity but not the enclosed line's.
    #[dialog_common::test]
    async fn it_derives_identities_from_shape() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let state = Ephemeral::new();

        let alone = Stack::builder()
            .line(shared.clone())
            .build()
            .perform(&operator)
            .await?;
        let over = Stack::builder()
            .line(shared.clone())
            .line(state.clone())
            .link(&shared, name("shared"))
            .build()
            .perform(&operator)
            .await?;
        assert_eq!(
            alone.identities()[0],
            over.identities()[0],
            "the bottom's identity is the same in every stack"
        );
        assert_ne!(over.identities()[1], over.identities()[0]);
        assert!(over.identity().to_string().starts_with("stack:"));

        let renamed = Stack::builder()
            .line(shared.clone())
            .line(state.clone())
            .link(&shared, name("base"))
            .build()
            .perform(&operator)
            .await?;
        assert_ne!(
            renamed.identity(),
            over.identity(),
            "a layer name is part of the encloser's shape"
        );
        Ok(())
    }

    /// A name bound by two links from one line fans a write out to
    /// both lines, and the composite read dedups the fact.
    #[dialog_common::test]
    async fn it_fans_out_a_name_bound_twice() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let shared = repo.branch("main").open().perform(&operator).await?;
        let (left, right) = (Ephemeral::new(), Ephemeral::new());

        shared
            .transaction()
            .assert(Placement::new("ui/cursor".parse()?, name("state")))
            .commit()
            .perform(&operator)
            .await?;
        shared.refresh(&operator).await?;

        let stack = Stack::builder()
            .line(shared.clone())
            .line(left.clone())
            .line(right.clone())
            .line(Ephemeral::new())
            .link(&left, name("state"))
            .link(&right, name("state"))
            .build()
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        stack
            .transaction()
            .assert(dialog_query::the!("ui/cursor").of(doc.clone()).is(3u64))
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        assert_eq!(
            values::<u64>(&stack, &operator, "ui/cursor", &doc).await?,
            vec![Value::UnsignedInt(3)],
            "one fact in two lines reads as one row"
        );
        Ok(())
    }

    /// A stack whose bottom is not a branch has nowhere for
    /// declarations or undeclared attributes to go: read-only.
    #[dialog_common::test]
    async fn it_refuses_to_transact_without_a_branch_bottom() -> Result<()> {
        let (operator, _profile) = test_operator_with_profile().await;
        let stack = Stack::builder()
            .line(Ephemeral::new())
            .build()
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        let result = stack
            .transaction()
            .assert(dialog_query::the!("doc/title").of(doc).is("x".to_string()))
            .commit()
            .perform(&operator)
            .await;
        assert!(matches!(result, Err(CommitError::Detached)));
        Ok(())
    }
}
