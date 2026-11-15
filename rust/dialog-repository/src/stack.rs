//! A stack: the primary branch read and written as one with the
//! layers its schema declares beside it.
//!
//! The primary's schema says which layers exist ([`LayerDeclaration`]).
//! Opening a stack on the primary builds every layer the schema
//! declares in memory and reads them with the primary as one composite.
//! Where a row lies is a dimension of the row, `in`: a read with `in`
//! unbound reads every layer and binds each row's, one with `in` bound
//! reads that layer alone, and a write lands in the layer its
//! descriptor names (`in:`, or [`Statement::at`]), the primary with none
//! named. A transaction through the stack routes each write so: the
//! primary's share is committed and published as any branch commit is,
//! and each memory layer's share lands in it once the publish does, so
//! no layer shows a commit that was refused. The layers every stack
//! has are named [`MEMORY`], the primary, [`SESSION`], the stack's own,
//! and [`CONTROL`], the loopback a command lands in. [`Stack::layer`]
//! hands each out by its type ([`Layer`]).
//!
//! ```no_run
//! # use dialog_repository::{Branch, LayerDeclaration, LayerType};
//! # use dialog_corpus::Statement as _;
//! # async fn example(branch: Branch, env: impl dialog_repository::StackEnv) -> anyhow::Result<()> {
//! let state: dialog_corpus::Entity = "session:state".parse()?;
//! branch
//!     .transaction()
//!     .assert(LayerDeclaration::new(state.clone(), LayerType::Session))
//!     .commit()
//!     .publish()
//!     .perform(&env)
//!     .await?;
//! let stack = branch.stack().perform(&env).await?;
//! stack
//!     .assert(dialog_query::the!("ui/selected").of("doc:1".parse()?).is(true).at(state))
//!     .perform(&env)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! A stack is driven by invoking ([`Stack::invoke`] for a command,
//! [`Stack::transact`] for a body that reads the stack and writes what
//! it decides on, [`Stack::assert`] for writes alone). Each invocation
//! runs one **dispatch**, the unit of publication, re-run from the new
//! head when the primary's head moved under it, the `transact` body
//! first (see [`invoke`]), and reports what it did
//! ([`InvocationReport`](crate::InvocationReport)).
//! [`Stack::transaction`] is the same commit without the re-run, for a
//! caller that handles a lost head itself.
//!
//! A **channel** ([`Channel`]) is a layer whose rows live one tick, with
//! two ends: what its outside end sends is read by the next dispatch's
//! first tick as rows of the channel, and what a dispatch writes into it
//! lands on its outside end once the dispatch has published. A command
//! is a row of [`CONTROL`], the built-in channel, the loopback: one
//! invoked ([`Stack::invoke`] sends it and runs a dispatch), one written
//! `.at(control_layer())`, or a rule head landing there (its descriptor
//! names `in: control:`, or its concept carries the
//! `dialog.concept/transient` marker). It is the stimulus of the next
//! tick, read as a row of `control:` by a rule premised on it, and gone
//! after that tick. Once a dispatch has published, its commands land on
//! the loopback's outside end as one tick, and the invocation's report
//! names them. Its effect ([`InvocationReport::effect`](crate::InvocationReport::effect)),
//! a separate step the embedder performs when it chooses, runs the
//! stack's **providers** ([`Stack::provide`]) over them and writes their
//! answers in one more dispatch, with each command's lifecycle rows (see
//! [`effect`]). A consumer outside the process, or one that reads the
//! log itself, reads them with [`Stack::arrivals`], or reads another
//! channel: one the schema declares with [`LayerType::Channel`], or one
//! [`Stack::channel`] creates, named by any entity outside [`MEMORY`]
//! and [`SESSION`].
//!
//! A layer declared with [`LayerType::Branch`], `memory:<name>`, is
//! durable: a tree the primary points at, with no head of its own. The
//! primary names it by two facts of the layer's entity, `memory:<name>
//! dialog.branch/revision <revision entity>` and `memory:<name>
//! dialog.branch/tree "<tree, base58>"`, and the stack reads that tree
//! as a line beside the primary, its rows binding `in` to
//! `memory:<name>`; a layer the primary names nothing for is empty. A
//! write into it is minted as the layer's next tree, built on the one
//! the primary pointed at when the dispatch began, and the dispatch's
//! commit of the primary carries the pointer facts naming it: one
//! compare-and-swap, the primary's. A lost one leaves the minted tree an
//! orphan nothing names, and the dispatch re-runs from the new head,
//! reading the layer where the new head points it. [`Stack::sync`] pulls
//! and pushes the primary, and the pointed trees with it.
//!
//! Every stack has its own layer, [`SESSION`], whether or not the
//! schema declares it: a store in memory, the stack's alone, minted when
//! it opens and gone when the process is. [`Stack::layer`] hands it out
//! for [`session_layer`](crate::session_layer), a write
//! `.at(session_layer())` lands in it once the dispatch publishes, and a
//! read binds its rows' `in` to it. `session:` is made of two things:
//! the stack's own rows (what was written into it, the revision rows
//! below, the selection, the layer facts, the providers' discovery rows)
//! and the branch metadata every query of the primary folds in
//! (`dialog.branch/*`, `dialog.replica/*`, `dialog.peer/*`), read as rows
//! of `session:` through the stack. The primary's session overlay,
//! [`Branch::overlay`], is not a part of it: it is the embedder's, the
//! stack writes nothing into it, and a read through the stack binds its
//! rows to the primary, [`MEMORY`]. A second stack opened on the branch
//! mints a `session:` of its own; [`Stack::backup`] and
//! [`OpenStack::restore`] carry one stack's session layers to another
//! (see [`backup`]).
//!
//! The stack records in its own layer the revision every memory layer
//! and channel stands at, one row per layer, under `last`: of each
//! memory layer its `dialog.layer/revision`, the hash of its instant
//! chain as text; of each channel, [`CONTROL`] among them, the same, the
//! hash of the sequence of its last tick ([`Channel::revision`]); and of
//! [`SESSION`] the hash of its own chain as it stood before the
//! recordings that followed its last change. The primary's revision is
//! one row of `session:`, the branch metadata's `dialog.branch/revision`
//! of the primary's branch entity, naming its revision's entity: the
//! stack records no second row of that cell, so it reads one value at
//! every point, and keeps instead where it last recorded the primary, to
//! diff the head against. A durable layer has no row: the primary's
//! pointer fact of it is the record. The rows exist from the moment the
//! stack opens, which writes them as one instant with the facts of the
//! three layers every stack has (`control: dialog.layer/type channel:`,
//! `session: dialog.layer/type session:`, `memory: dialog.layer/type
//! branch:` and `memory: dialog.layer/branch <the primary's branch
//! entity>`). After that a row is written only when its layer **moved**,
//! when the revision it stands at is not the one the stack holds for it:
//! after a dispatch publishes and lands its commands, for the layers its
//! commit, its writes and its commands moved, and after [`Stack::sync`]
//! pulls and pushes, for the layers the pulls moved. Writing the rows is
//! not itself a move of the stack's own layer: its row moves only when
//! something else was written into `session:`, a dispatch's writes into
//! it among them, and then names the revision those writes left it at.
//! So a dispatch that writes nothing into `session:` and moves no other
//! layer writes no row and mints no instant in `session:`. Of the same
//! rows, as the layers stood when a dispatch began, those the stack has
//! not recorded are its stimulus, the primary's among them, so a rule
//! premised on a layer's revision fires once when the layer moved
//! outside a dispatch (a pull or another handle moved the primary,
//! something wrote into a memory layer directly). A rule premised on a
//! durable layer's revision reads its pointer fact, which the dispatch
//! or the sync that moves the layer writes.
//!
//! The stack's own layer also holds which layer is **selected**
//! ([`Stack::select`]): while one is, a write with no layer named
//! lands in it rather than the primary, as a stroke lands on the layer
//! an image editor has selected. The machinery under `dialog.` (rules,
//! layer declarations, the revision rows) never follows the selection.
//!
//! What the stack holds of its own are its **declarations**
//! ([`Stack::declare`]): layers this process declares for itself,
//! which outrank what the primary's tree and session store declare, and
//! reach no layer and no peer.
//!
//! [`MEMORY`]: crate::MEMORY
//! [`SESSION`]: crate::SESSION
//! [`CONTROL`]: crate::CONTROL

pub mod arrivals;
pub mod backup;
pub(crate) mod durable;
pub mod effect;
pub mod invoke;
pub mod sync;

pub use arrivals::Arrivals;
pub use backup::{Backup, LayerBackup};
pub use sync::{SyncEnv, SyncError, SyncStack};

use std::iter::once;
use std::sync::Arc;

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Changes, Entity, Statement};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify, OperatorExt as _};
use dialog_effects::blob::{
    Import as BlobImport, Load as BlobLoad, Read as BlobRead, Size as BlobSize,
};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::nodes;
use parking_lot::RwLock;

use crate::placement::{
    LayerDeclaration, LayerType, Layers, Observed, Recorded, Selection, channel_name,
    control_layer, durable_name, memory_layer, selected, session_layer,
};
use crate::repository::branch::session::Composite;
use crate::repository::branch::transaction::{DurableLayer, StackContext};
use crate::repository::source::Source;
use crate::schema::Replica;
use crate::{Branch, Channel, CommitError, Ephemeral, QueryLayer, RemoteSite, Transaction};
pub use durable::Durable;
use durable::Durables;

/// What opening a stack and reading through it need of the
/// environment: a read of the primary, to resolve the layers its schema
/// declares and where it points each durable layer, and what a query
/// through the stack reads with. Nothing in it commits or publishes:
/// an environment that can open branches and query them opens a stack
/// beside them ([`OpenStack::perform`]), and reads what arrives in its
/// layers ([`Arrivals`]). Dispatching through the stack needs
/// [`StackEnv`].
pub trait OpenStackEnv:
    Provider<BlobRead>
    + Provider<Get>
    + Provider<Put>
    + Provider<Resolve>
    + Provider<Identify>
    + Provider<ArchiveLoad>
    + Provider<nodes::Read<CorpusNode>>
    + Provider<nodes::Preload<CorpusNode>>
    + Provider<nodes::Write<CorpusNode>>
    + Provider<BlobLoad>
    + Provider<dialog_corpus::Preload>
    + Provider<dialog_corpus::Speculation>
    + Provider<Fork<RemoteSite, Resolve>>
    + Holds
    + ConditionalSync
    + 'static
{
}

impl<T> OpenStackEnv for T where
    T: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<ArchiveLoad>
        + Provider<nodes::Read<CorpusNode>>
        + Provider<nodes::Preload<CorpusNode>>
        + Provider<nodes::Write<CorpusNode>>
        + Provider<BlobLoad>
        + Provider<dialog_corpus::Preload>
        + Provider<dialog_corpus::Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static
{
}

/// What a stack needs of its environment to invoke through it: what
/// opening it and reading through it need ([`OpenStackEnv`]), and what
/// a transaction's commit needs, to publish through it.
pub trait StackEnv:
    OpenStackEnv
    + Provider<Attest>
    + Provider<Publish>
    + Provider<Import>
    + Provider<BlobImport>
    + Provider<BlobSize>
{
}

impl<T> StackEnv for T where
    T: OpenStackEnv
        + Provider<Attest>
        + Provider<Publish>
        + Provider<Import>
        + Provider<BlobImport>
        + Provider<BlobSize>
{
}

/// Command opening a [`Stack`] on a branch. Built by
/// [`Branch::stack`]; run with [`perform`](Self::perform).
pub struct OpenStack {
    primary: Branch,
    declared: Changes,
    backup: Option<Backup>,
}

impl OpenStack {
    /// Hold `declaration` in the stack once it is open, as
    /// [`Stack::declare`] would: a layer it declares is built with the
    /// rest.
    pub fn declare<S: Statement>(mut self, declaration: S) -> Self {
        declaration.assert(&mut self.declared);
        self
    }

    /// Read the primary's schema at its head and build every layer it
    /// declares, each durable layer at the tree the primary points it
    /// at, and mint the stack's own layer, [`SESSION`](crate::SESSION),
    /// empty but for the rows the stack records in it, or holding what a
    /// [`restore`](Self::restore) gave it. Opening reads and commits
    /// nothing, so it needs no more of `env` than a read does
    /// ([`OpenStackEnv`]).
    pub async fn perform<Env: OpenStackEnv>(self, env: &Env) -> Result<Stack, CommitError> {
        // The primary is named by the entity the meta branch derives
        // from `(replica, name)`, the replica being the primary's: this
        // profile's view of the repository.
        let operator = Identify.perform(env).await?;
        let replica = Replica::new(operator.profile().clone(), self.primary.of().clone());
        let branch = replica.branch(self.primary.name()).this;
        // A backup of another branch is refused before any layer is
        // read or built.
        if let Some(backup) = &self.backup {
            backup.check(&branch)?;
        }
        let primary = Source::from(self.primary.clone());
        let resolved = Layers::resolve(&primary, &self.declared, env).await?;
        let layers = resolved.layers();
        // The primary, the stack's own layer and the loopback are
        // every stack's, whatever the schema says of them: a
        // declaration naming one builds nothing apart.
        let (memory, own_name, control) = (memory_layer(), session_layer(), control_layer());
        let mut memories = Vec::new();
        let mut channels = Vec::new();
        let mut order = Vec::new();
        let mut durables = Vec::new();
        // Every declared layer's name is checked before anything is read
        // or written, so a refused declaration writes nothing.
        for (entity, kind) in layers {
            if entity == memory || entity == own_name || entity == control {
                continue;
            }
            match kind {
                LayerType::Session => {
                    order.push(entity.clone());
                    memories.push((entity, Ephemeral::new()));
                }
                LayerType::Channel => {
                    channel_name(&entity)?;
                    order.push(entity.clone());
                    channels.push(Channel::new(entity));
                }
                LayerType::Branch => {
                    durable_name(&entity)?;
                    durables.push(entity);
                }
            }
        }
        // A durable layer is where the primary, as the stack read it,
        // points it.
        let durables = Arc::new(Durables::new(&self.primary, durables, operator.profile()));
        durables.refresh(primary.revision(), env).await?;
        let control = Channel::new(control);
        // The stack's own layer is its own store, never the primary's
        // session overlay ([`Branch::overlay`]), which is the
        // embedder's: what the stack records lands here alone.
        let own = Ephemeral::new();
        let head = self.primary.revision();
        let every: Vec<Channel> = once(control.clone()).chain(channels.clone()).collect();
        // A restored stack starts from the rows the backup holds, and
        // from where the backup saw the primary; a fresh one from
        // nothing, and from the primary's head.
        let (observed, restored) = match &self.backup {
            Some(backup) => (
                Observed::new(backup.primary().cloned()),
                Some(backup.replay(&own, &memories)?),
            ),
            None => (
                Observed::new(head.as_ref().map(crate::Revision::entity)),
                None,
            ),
        };
        // The revision rows of every layer exist from the moment the
        // stack opens, with the facts of the three layers every stack
        // has, written as one record into its own layer. A restored
        // stack records again the layers that stood where the backup's
        // rows said, since replaying their rows moved them, and leaves
        // the ones that had moved unrecorded when the backup was taken,
        // and the primary, to the first dispatch's stimulus.
        let recorded = Recorded {
            branch: &branch,
            primary: head.as_ref(),
            observed: &observed,
            memories: &memories,
            channels: &every,
            own: &own,
        };
        let mut opening = recorded.implied();
        match &restored {
            Some(stale) => opening.merge(recorded.rows_of(|layer| !stale.contains(layer))),
            None => opening.merge(recorded.rows()),
        }
        own.record(opening)?;
        Ok(Stack {
            inner: Arc::new(Inner {
                primary: self.primary,
                branch,
                own,
                observed,
                layers: memories,
                order,
                durables,
                declared: RwLock::new(self.declared),
                control,
                channels: RwLock::new(channels),
                providers: RwLock::new(Vec::new()),
            }),
        })
    }
}

struct Inner {
    primary: Branch,
    /// The primary's branch entity, the meta branch's for the
    /// primary's `(replica, name)`: what its `dialog.branch/revision`
    /// cell in [`SESSION`](crate::SESSION) is of.
    branch: Entity,
    /// The stack's own layer, [`SESSION`](crate::SESSION): a store of
    /// its own, minted when it opened, apart from the primary's session
    /// overlay ([`Branch::overlay`]).
    own: Ephemeral,
    /// Where the stack last recorded the primary (see [`Observed`]).
    observed: Observed,
    /// The session layers the schema declares, each with the entity that
    /// names it, in the order the schema lists them. The stack's own
    /// layer, [`SESSION`](crate::SESSION), is not among them.
    layers: Vec<(Entity, Ephemeral)>,
    /// The session layers and channels the schema declares, by name, in
    /// the order the schema lists them.
    order: Vec<Entity>,
    /// The durable layers the schema declares, `memory:<name>`, in the
    /// order the schema lists them, and where the primary points them.
    durables: Arc<Durables>,
    declared: RwLock<Changes>,
    /// The loopback, [`CONTROL`](crate::CONTROL): the channel every
    /// stack has.
    control: Channel,
    /// The other channels: those the schema declares, in its order, then
    /// those [`Stack::channel`] created, in the order it did.
    channels: RwLock<Vec<Channel>>,
    /// The providers [`Stack::provide`] registered, in the order it did.
    providers: RwLock<Vec<effect::Provider>>,
}

/// The primary branch and the layers its schema declares, read and
/// written as one. Cheap to clone: clones share the layers and the
/// declarations.
#[derive(Clone)]
pub struct Stack {
    inner: Arc<Inner>,
}

/// A layer of a stack, by its type, as [`Stack::layer`] hands it out.
#[derive(Debug, Clone)]
pub enum Layer<'a> {
    /// A layer held in memory, whose rows stay until retracted: the
    /// stack's own, [`SESSION`](crate::SESSION), or a session layer the
    /// schema declares ([`LayerType::Session`]).
    Ephemeral(&'a Ephemeral),
    /// A durable layer: the primary, [`MEMORY`](crate::MEMORY), or a tree
    /// the primary points at ([`LayerType::Branch`]).
    Durable(Durable),
    /// A channel, whose rows live one tick: the loopback,
    /// [`CONTROL`](crate::CONTROL), or another ([`LayerType::Channel`],
    /// [`Stack::channel`]).
    Channel(Channel),
}

impl<'a> Layer<'a> {
    /// The layer held in memory, if this is one.
    pub fn ephemeral(&self) -> Option<&'a Ephemeral> {
        match self {
            Layer::Ephemeral(memory) => Some(memory),
            _ => None,
        }
    }

    /// The durable layer, if this is one.
    pub fn durable(&self) -> Option<&Durable> {
        match self {
            Layer::Durable(durable) => Some(durable),
            _ => None,
        }
    }

    /// The channel, if this is one.
    pub fn channel(&self) -> Option<&Channel> {
        match self {
            Layer::Channel(channel) => Some(channel),
            _ => None,
        }
    }
}

impl Branch {
    /// Open a stack on this branch: it becomes the primary, and every
    /// layer its schema declares is built beside it.
    pub fn stack(&self) -> OpenStack {
        OpenStack {
            primary: self.clone(),
            declared: Changes::new(),
            backup: None,
        }
    }
}

impl Stack {
    /// The branch this stack reads and writes as its primary.
    pub fn primary(&self) -> &Branch {
        &self.inner.primary
    }

    /// The layer named `name`, if the stack holds one, by its type:
    /// [`Layer::Ephemeral`] for the stack's own, [`SESSION`](crate::SESSION),
    /// which every stack has, and a session layer the schema declares;
    /// [`Layer::Durable`] for the primary, [`MEMORY`](crate::MEMORY), and
    /// a durable layer the schema declares; [`Layer::Channel`] for the
    /// loopback, [`CONTROL`](crate::CONTROL), which every stack has too,
    /// and every other channel the stack holds. The stack's own layer is
    /// its own store, apart from the primary's session overlay
    /// ([`Branch::overlay`]): a clone of it shares its state with the
    /// stack and every clone of the stack.
    pub fn layer(&self, name: &Entity) -> Option<Layer<'_>> {
        if *name == session_layer() {
            return Some(Layer::Ephemeral(&self.inner.own));
        }
        if *name == memory_layer() {
            return Some(Layer::Durable(Durable::new(
                name.clone(),
                self.inner.primary.revision(),
            )));
        }
        if let Some(channel) = self.channel_of(name) {
            return Some(Layer::Channel(channel));
        }
        if let Some((_, memory)) = self.inner.layers.iter().find(|(layer, _)| layer == name) {
            return Some(Layer::Ephemeral(memory));
        }
        let durables = &self.inner.durables;
        let at = durables.layers().iter().position(|layer| layer == name)?;
        let revision = durables.pinned().revisions.get(at).cloned().flatten();
        Some(Layer::Durable(Durable::new(name.clone(), revision)))
    }

    /// The channel named `name`, creating it if the stack holds none of
    /// that name: [`control_layer`] is the
    /// loopback, a channel the schema declares is the one the stack built
    /// when it opened, and any other entity is a new channel, listed by
    /// [`layers`](Self::layers) from then on. Creating one asserts
    /// `<name> dialog.layer/type channel:` into the stack's own layer,
    /// [`SESSION`](crate::SESSION), as one instant; a second call returns
    /// the same channel and writes nothing.
    ///
    /// A channel is any entity but one under [`MEMORY`](crate::MEMORY)
    /// or [`SESSION`](crate::SESSION), which name the other types of
    /// layer: those are refused with [`CommitError::InvalidPlacement`],
    /// and nothing is written.
    pub fn channel(&self, name: &Entity) -> Result<Channel, CommitError> {
        if let Some(channel) = self.channel_of(name) {
            return Ok(channel);
        }
        channel_name(name)?;
        let mut channels = self.inner.channels.write();
        if let Some(channel) = channels.iter().find(|channel| channel.entity() == name) {
            return Ok(channel.clone());
        }
        let mut changes = Changes::new();
        LayerDeclaration::new(name.clone(), LayerType::Channel).assert(&mut changes);
        self.inner.own.apply(changes)?;
        let channel = Channel::new(name.clone());
        channels.push(channel.clone());
        Ok(channel)
    }

    /// The channel named `name`, if the stack holds one: the loopback, or
    /// one the schema declares or [`channel`](Self::channel) created.
    pub(crate) fn channel_of(&self, name: &Entity) -> Option<Channel> {
        if name == self.inner.control.entity() {
            return Some(self.inner.control.clone());
        }
        self.inner
            .channels
            .read()
            .iter()
            .find(|channel| channel.entity() == name)
            .cloned()
    }

    /// Every channel the stack holds, the loopback first.
    pub(crate) fn channels(&self) -> Vec<Channel> {
        once(self.inner.control.clone())
            .chain(self.inner.channels.read().iter().cloned())
            .collect()
    }

    /// The layers this stack holds, by name: the three every stack has,
    /// [`MEMORY`](crate::MEMORY), the primary, first, then its own,
    /// [`SESSION`](crate::SESSION), then the loopback,
    /// [`CONTROL`](crate::CONTROL); then the session layers and channels
    /// the schema declares, in the schema's order, then its durable
    /// layers, in the schema's order, then the channels
    /// [`channel`](Self::channel) created, in the order it did.
    pub fn layers(&self) -> Vec<Entity> {
        let created: Vec<Entity> = self
            .inner
            .channels
            .read()
            .iter()
            .map(|channel| channel.entity().clone())
            .filter(|channel| !self.inner.order.contains(channel))
            .collect();
        [memory_layer(), session_layer(), control_layer()]
            .into_iter()
            .chain(self.inner.order.iter().cloned())
            .chain(self.inner.durables.layers().iter().cloned())
            .chain(created)
            .collect()
    }

    /// The branch entity of [`MEMORY`](crate::MEMORY), the primary: the
    /// entity the meta branch derives from the primary's replica and its
    /// name ([`schema::Branch`](crate::schema::Branch)), what the
    /// primary's `dialog.branch/revision` row in
    /// [`SESSION`](crate::SESSION) is of. `None` for any other layer: a
    /// durable layer has no branch, and its pointer facts in the primary
    /// are of the layer's own entity.
    pub fn branch_of(&self, name: &Entity) -> Option<&Entity> {
        (*name == memory_layer()).then_some(&self.inner.branch)
    }

    /// The providers [`provide`](Self::provide) registered.
    pub(crate) fn providers(&self) -> &RwLock<Vec<effect::Provider>> {
        &self.inner.providers
    }

    /// The stack's durable layers and where the primary points them.
    pub(crate) fn durables(&self) -> &Arc<Durables> {
        &self.inner.durables
    }

    /// The revision the primary points the durable layer `name` at, as
    /// the stack last read it: `None` for a layer the primary points
    /// nowhere, or one the stack does not hold.
    #[cfg(test)]
    pub(crate) fn pointed(&self, name: &Entity) -> Option<crate::Revision> {
        let durables = &self.inner.durables;
        let at = durables.layers().iter().position(|layer| layer == name)?;
        durables.pinned().revisions.get(at).cloned().flatten()
    }

    /// Read again where the primary, at its head, points the durable
    /// layers, when the stack last read them at another head.
    pub(crate) async fn repoint<Env: OpenStackEnv>(&self, env: &Env) -> Result<(), CommitError> {
        let head = self.inner.primary.revision();
        if self.inner.durables.stale(head.as_ref()) {
            self.inner.durables.refresh(head.as_ref(), env).await?;
        }
        Ok(())
    }

    /// Select `layer`, or clear the selection with `None`: a dispatch
    /// writing the stack's own layer's `dialog.session/layer` row of
    /// [`SESSION`](crate::SESSION), under `last`. While a layer is
    /// selected, a write with no layer named, and a rule head whose
    /// descriptor names none, lands in it rather than the primary,
    /// except a write under `dialog.` (rules, layer declarations,
    /// revisions), which never follows the selection. Selecting
    /// [`MEMORY`](crate::MEMORY) is selecting the primary. A selected
    /// layer the stack does not hold refuses the commit that would
    /// land a write in it with [`CommitError::UnboundLayer`].
    ///
    /// The selection takes effect from the dispatch after the one that
    /// writes it, once it has published.
    pub fn select(&self, layer: Option<Entity>) -> invoke::Invocation {
        match (layer, self.selected()) {
            (Some(layer), _) => self.assert(Selection(layer)),
            (None, Some(current)) => self.invocation().retract(Selection(current)),
            (None, None) => self.invocation(),
        }
    }

    /// The layer selected in the stack's own layer, if any (see
    /// [`select`](Self::select)).
    pub fn selected(&self) -> Option<Entity> {
        selected(&self.inner.own)
    }

    /// Hold `declaration` in this stack: a
    /// [`LayerDeclaration`](crate::LayerDeclaration), or a [`Changes`] of
    /// them, whose retractions undo what an earlier call declared or what
    /// the primary declares. It is this process's own: it outranks a
    /// declaration the primary's session store or tree holds and reaches
    /// no layer and no peer. A layer declared here after the stack
    /// opened is not built: declare it on [`Branch::stack`] instead.
    /// Chainable.
    pub fn declare<S: Statement>(&self, declaration: S) -> &Self {
        declaration.assert(&mut *self.inner.declared.write());
        self
    }

    /// The declarations this stack holds, as [`declare`](Self::declare)
    /// accumulated them.
    pub fn declared(&self) -> Changes {
        self.inner.declared.read().clone()
    }

    /// Whether a layer of the stack stands somewhere other than the
    /// `dialog.layer/revision` row its own layer holds of it: what the
    /// next dispatch would record (see the [module docs](self)).
    pub(crate) fn unrecorded(&self) -> bool {
        let revision = self.inner.primary.revision();
        let channels = self.channels();
        self.recorded(revision.as_ref(), &channels).any_moved()
    }

    /// What the stack records of its layers' revisions, with the
    /// primary at `primary`.
    fn recorded<'a>(
        &'a self,
        primary: Option<&'a crate::Revision>,
        channels: &'a [Channel],
    ) -> Recorded<'a> {
        Recorded {
            branch: &self.inner.branch,
            primary,
            observed: &self.inner.observed,
            memories: &self.inner.layers,
            channels,
            own: &self.inner.own,
        }
    }

    /// The memory layers of the stack, by name: its own,
    /// [`SESSION`](crate::SESSION), first, then those the schema
    /// declares, in the schema's order.
    pub(crate) fn memories(&self) -> Vec<(Entity, Ephemeral)> {
        once((session_layer(), self.inner.own.clone()))
            .chain(self.inner.layers.iter().cloned())
            .collect()
    }

    /// The stack's own layer, [`SESSION`](crate::SESSION).
    pub(crate) fn own(&self) -> &Ephemeral {
        &self.inner.own
    }

    /// The layers a transaction through this stack reads and induces
    /// over: the primary pinned as `primary`, each durable layer at the
    /// revision `durables` holds it at, and the memory layers.
    fn composite(
        &self,
        primary: Source,
        durables: &[DurableLayer],
        channels: &[Channel],
    ) -> Composite {
        let mut sources = vec![primary];
        let mut names = vec![None];
        for durable in durables {
            sources.push(durable.source());
            names.push(Some(durable.layer.clone()));
        }
        Composite {
            sources,
            memories: self.memories(),
            names,
            channels: channels
                .iter()
                .map(|channel| channel.entity().clone())
                .collect(),
        }
    }

    /// A query over the stack: the primary and every layer. A read
    /// with `in` unbound reads them all and binds each row's layer; one
    /// with `in` bound reads that layer alone. A durable layer is read at
    /// the tree the primary pointed it at when the stack last read the
    /// primary: when it opened, after each dispatch through it and each
    /// sync, and before each dispatch that finds the primary moved.
    pub fn query(&self) -> QueryLayer<'_> {
        // The stack's own layer is read as `session:` beside the branch
        // metadata; the primary's session overlay, read with it, is then
        // a part of the primary, `memory:`.
        let mut layer =
            QueryLayer::from(&self.inner.primary).with_memory(session_layer(), &self.inner.own);
        for (name, memory) in &self.inner.layers {
            layer = layer.with_memory(name.clone(), memory);
        }
        for (name, source) in self.inner.durables.sources() {
            layer = layer.with_pinned(name, source);
        }
        layer
    }

    /// Start a transaction through the stack. Its reads see every
    /// layer; its commit routes each write to the layer its descriptor
    /// names (`in:`), the primary with none named, and the memory layers
    /// take their shares once the primary's commit publishes. Once its
    /// publish has won, the commands it dispatched (with
    /// [`dispatch`](Transaction::dispatch), or written
    /// `.at(control_layer())`) and the ones its rules dispatched land in
    /// the stack's [`CONTROL`](crate::CONTROL) for one tick, as
    /// [`Invocation::perform`](invoke::Invocation::perform) lands them,
    /// and wake every [`Arrivals`] waiting. Unlike an invocation it is
    /// not re-run when the head moved under it: its publish fails with
    /// [`PublishError::VersionMismatch`](crate::PublishError::VersionMismatch)
    /// and lands nothing, and the caller runs it again.
    pub fn transaction(&self) -> Transaction<&Branch> {
        self.receiving(Changes::new())
    }

    /// A transaction through the stack, as [`transaction`](Self::transaction)
    /// starts one, whose first tick also reads `received`: the rows the
    /// outside ends of the stack's channels other than the loopback sent,
    /// each named for its channel. They are stimulus, read as rows of the
    /// channel, and the publish does not land them back on its outside
    /// end.
    pub(crate) fn receiving(&self, received: Changes) -> Transaction<&Branch> {
        let transaction = self.inner.primary.transaction();
        let primary = transaction.view().to_source();
        let pinned = self.inner.durables.pinned();
        let durables = self.inner.durables.lines(&pinned);
        let channels = self.channels();
        transaction.with_stack(StackContext {
            branch: self.inner.branch.clone(),
            own: self.inner.own.clone(),
            observed: self.inner.observed.clone(),
            composite: self.composite(primary, &durables, &channels),
            memories: self.memories(),
            durables,
            pinned,
            state: self.inner.durables.clone(),
            channels,
            received,
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_repo;
    use crate::placement::{revision_entity, tree_value};
    use crate::{LayerDeclaration, PublishError};
    use anyhow::Result;
    use dialog_common::ConditionalSend;
    use dialog_corpus::Selector;
    use dialog_corpus::Update as _;
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Cardinality;
    use dialog_query::attribute::Relation as QueryRelation;
    use dialog_query::attribute::statement::AttributeStatement;
    use dialog_query::query::Output as _;
    use dialog_query::types::Scalar;
    use dialog_query::{AttributeQuery, InductiveRule, Term};
    #[cfg(any(feature = "integration-tests", feature = "web-integration-tests"))]
    use dialog_remote_s3::helpers::S3Address;
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::future::join;
    use parking_lot::Mutex;
    use std::fmt::Debug;
    use std::slice::from_ref;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::Ordering::SeqCst;

    type Operator = Peer<VolatileSpace, dialog_peer::Session>;

    /// The layers every stack has, [`MEMORY`](crate::MEMORY),
    /// [`SESSION`](crate::SESSION) and [`CONTROL`](crate::CONTROL), in
    /// the order [`Stack::layers`] lists them, followed by `declared`.
    fn implied_and(declared: impl IntoIterator<Item = Entity>) -> Vec<Entity> {
        [memory_layer(), session_layer(), control_layer()]
            .into_iter()
            .chain(declared)
            .collect()
    }

    /// The entity of the branch `name` on `primary`'s replica under
    /// `env`'s profile: what the meta branch derives from
    /// `(replica, name)`.
    async fn branch_entity(primary: &Branch, env: &Operator, name: &str) -> Result<Entity> {
        let operator = Identify.perform(env).await?;
        let replica = Replica::new(operator.profile().clone(), primary.of().clone());
        Ok(replica.branch(name).this)
    }

    fn layer(name: &str) -> Entity {
        format!("session:{name}").parse().expect("a layer entity")
    }

    /// The values of type `T` of `the` on `of` that `query` reads, as
    /// text, sorted.
    async fn read<T>(
        query: QueryLayer<'_>,
        env: &Operator,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<String>>
    where
        T: Scalar + Debug + Clone + ConditionalSend + ConditionalSync + 'static,
    {
        let rows = query
            .select(AttributeQuery::from(
                Term::<QueryRelation>::from(QueryRelation::from(
                    the.parse::<dialog_corpus::Relation>()?,
                ))
                .of(Term::<Entity>::from(of.clone()))
                .is(Term::<T>::var("v")),
            ))
            .perform(env)
            .try_vec()
            .await?;
        let mut rows: Vec<String> = rows
            .into_iter()
            .map(|row| format!("{:?}", row.is))
            .collect();
        rows.sort();
        Ok(rows)
    }

    /// The values of type `T` of `the` on `of` committed in `branch`'s
    /// tree.
    async fn committed<T>(
        branch: &Branch,
        env: &Operator,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<String>>
    where
        T: Scalar + Debug + Clone + ConditionalSend + ConditionalSync + 'static,
    {
        read::<T>(branch.query(), env, the, of).await
    }

    /// The layer `name` of `stack`, if it is one held in memory.
    fn ephemeral<'a>(stack: &'a Stack, name: &Entity) -> Option<&'a Ephemeral> {
        stack.layer(name)?.ephemeral()
    }

    /// Whether the channel `name` of `stack` was sent rows no dispatch
    /// has taken.
    fn waiting(stack: &Stack, name: &Entity) -> bool {
        stack.channel_of(name).expect("a channel").is_waiting()
    }

    /// A channel's revision as its `dialog.layer/revision` row names it.
    fn channel_text(stack: &Stack, name: &Entity) -> String {
        use base58::ToBase58 as _;
        let channel = stack.channel_of(name).expect("a channel");
        format!(
            "{:?}",
            dialog_corpus::Value::String(channel.revision().as_bytes().to_base58())
        )
    }

    /// The values of `the` on `of` a memory layer holds.
    fn held(memory: &Ephemeral, the: &str, of: &Entity) -> Result<Vec<String>> {
        let selector = Selector::new()
            .the(the.parse::<dialog_corpus::Relation>()?)
            .of(of.clone());
        let mut rows: Vec<String> = memory
            .scan(&selector)
            .into_iter()
            .map(|fact| format!("{:?}", fact.is))
            .collect();
        rows.sort();
        Ok(rows)
    }

    /// `counter/count` of `of` as `value`, under `last`, written into
    /// `at`: the count is single-valued.
    fn count(of: &Entity, value: u64, at: &Entity) -> impl Statement {
        AttributeStatement {
            the: QueryRelation::from("counter/count".parse::<dialog_corpus::Relation>().unwrap()),
            of: of.clone(),
            is: dialog_corpus::Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(Cardinality::One),
            pick: Some(dialog_corpus::Pick::Last),
        }
        .at(at.clone())
    }

    /// Declare a memory layer `state`, and a counter rule whose head
    /// `counter/count`, under `last`, lands there (`in:`) and whose
    /// premise reads the count from there.
    async fn declare_state(branch: &Branch, env: &Operator) -> Result<Entity> {
        let state = layer("state");
        let increment: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "count": {
                        "the": "counter/count", "as": "UnsignedInteger", "pick": "last",
                        "in": "session:state"
                    }
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
                            "count": {
                                "the": "counter/count", "as": "UnsignedInteger", "pick": "last",
                                "in": "session:state"
                            }
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
        branch
            .transaction()
            .assert(LayerDeclaration::new(state.clone(), LayerType::Session))
            .assert(increment)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    /// A stack builds the layers the primary's schema declares and
    /// routes each write to the layer it names: the tree never holds a
    /// fact written into the memory layer, and the layer never holds one
    /// written with no layer named.
    #[dialog_common::test]
    async fn it_routes_each_write_to_the_layer_it_names() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        assert_eq!(
            stack.layers(),
            implied_and([state.clone()]),
            "the three layers every stack has come first, then the schema's"
        );
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();

        let doc: Entity = "doc:1".parse()?;
        let revision = stack
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(
                dialog_query::the!("ui/selected")
                    .of(doc.clone())
                    .is(true)
                    .at(state.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            branch.revision(),
            Some(revision),
            "the primary's share published"
        );

        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &doc).await?,
            vec!["String(\"Notes\")".to_string()],
            "the unplaced fact lands in the tree"
        );
        assert!(
            committed::<bool>(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "the tree never holds the placed fact"
        );
        assert_eq!(
            held(&memory, "ui/selected", &doc)?,
            vec!["Boolean(true)".to_string()],
            "the placed fact lands in its layer"
        );
        assert!(held(&memory, "doc/title", &doc)?.is_empty());

        assert_eq!(
            read::<bool>(stack.query(), &operator, "ui/selected", &doc).await?,
            vec!["Boolean(true)".to_string()]
        );
        assert_eq!(
            read::<String>(stack.query(), &operator, "doc/title", &doc).await?,
            vec!["String(\"Notes\")".to_string()]
        );
        Ok(())
    }

    /// Every stack has its own layer, declared or not: a store of its
    /// own, apart from the primary's session overlay. A write into
    /// `session:` lands there and never in [`Branch::overlay`]; a row of
    /// the overlay, the embedder's, is read through the stack as a part
    /// of the primary, `memory:`; a read binds each row once. A schema
    /// that declares `session:` builds nothing apart, and a second stack
    /// on the branch has a `session:` of its own, holding nothing the
    /// first's does.
    #[dialog_common::test]
    async fn it_holds_its_own_layer_whether_or_not_the_schema_declares_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let own = crate::session_layer();
        let stack = branch.stack().perform(&operator).await?;
        assert_eq!(stack.layers(), implied_and([]), "undeclared, still held");

        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(flag(&doc, true, &own))
            .perform(&operator)
            .await?;
        let layer = ephemeral(&stack, &own).expect("the stack's own layer");
        assert_eq!(
            held(layer, "ui/selected", &doc)?,
            vec!["Boolean(true)".to_string()],
            "a write into `session:` lands in the stack's own layer"
        );
        assert!(
            held(branch.overlay(), "ui/selected", &doc)?.is_empty(),
            "and not in the branch's overlay"
        );
        branch
            .overlay()
            .assert(primary_flag(&"doc:2".parse()?, true))?;
        assert!(
            held(layer, "ui/selected", &"doc:2".parse()?)?.is_empty(),
            "a write into the overlay does not reach the stack's own layer"
        );

        let claims: Vec<dialog_query::Claim> = stack
            .query()
            .select(selected_query().with_layer(Term::var("where")))
            .perform(&operator)
            .try_vec()
            .await?;
        let mut rows: Vec<String> = claims
            .iter()
            .map(|claim| {
                format!(
                    "{} in {}",
                    claim.of,
                    claim.layer().map(ToString::to_string).unwrap_or_default()
                )
            })
            .collect();
        rows.sort();
        assert_eq!(
            rows,
            vec![format!("{doc} in session:"), "doc:2 in memory:".to_string()],
            "each row is read once: the stack's own as `session:`, the overlay's as the primary's"
        );

        // A schema that declares `session:` gets the same one layer, and
        // a second stack gets one of its own.
        branch
            .transaction()
            .assert(LayerDeclaration::new(own.clone(), LayerType::Session))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let declared = branch.stack().perform(&operator).await?;
        assert_eq!(declared.layers(), implied_and([]));
        assert!(
            held(
                ephemeral(&declared, &own).expect("its own layer"),
                "ui/selected",
                &doc
            )?
            .is_empty(),
            "a second stack's `session:` holds nothing of the first's"
        );
        assert_eq!(
            held(layer, "ui/selected", &doc)?,
            vec!["Boolean(true)".to_string()],
            "and the first's keeps its rows"
        );
        Ok(())
    }

    /// A rule's head whose descriptor names a memory layer lands there,
    /// with its pick:
    /// a dispatched command increments a count the layer holds, and the
    /// commit, which writes nothing to the tree, still publishes.
    #[dialog_common::test]
    async fn it_lands_a_rules_head_on_the_layer_its_descriptor_names() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        stack
            .transaction()
            .assert(count(&counter, 1, &state))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let before = branch.revision();
        for expected in ["UnsignedInt(2)", "UnsignedInt(3)"] {
            stack
                .transaction()
                .dispatch(
                    dialog_query::the!("cmd.increment/counter")
                        .of("cmd:1".parse::<Entity>()?)
                        .is(counter.clone()),
                )
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            assert_eq!(
                held(&memory, "counter/count", &counter)?,
                vec![expected.to_string()],
                "the rule's head lands in the layer, succeeding the count it read"
            );
        }
        branch.refresh(&operator).await?;
        assert_eq!(branch.revision(), before, "nothing was written to the tree");
        assert_eq!(
            read::<u64>(stack.query(), &operator, "counter/count", &counter).await?,
            vec!["UnsignedInt(3)".to_string()]
        );
        Ok(())
    }

    /// A dispatch whose head moved under it is re-run from the new head
    /// with the original stimulus: the other writer's commit and this
    /// one's both stand, the rule fires once on the re-run alone, and the
    /// memory layer takes exactly the re-run's share.
    #[dialog_common::test]
    async fn it_re_runs_a_dispatch_from_the_head_that_moved_under_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        // Another handle moves the head; this stack's handle is stale.
        let other = repo.branch("main").open().perform(&operator).await?;
        let moved = other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:2".parse::<Entity>()?)
                    .is("Other".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        let published = stack
            .invoke(
                dialog_query::the!("cmd.increment/counter")
                    .of("cmd:1".parse::<Entity>()?)
                    .is(counter.clone()),
            )
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(
            published.revision().version().edition,
            moved.version().edition.successor(),
            "the re-run published on the moved head"
        );
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &doc).await?,
            vec!["String(\"Notes\")".to_string()]
        );
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &"doc:2".parse()?).await?,
            vec!["String(\"Other\")".to_string()],
            "the other writer's commit stands"
        );
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "the rule fired once, on the re-run"
        );
        Ok(())
    }

    /// A dispatch that writes only to memory layers publishes nothing: the
    /// primary's head stays, and the dispatch reports it.
    #[dialog_common::test]
    async fn it_publishes_nothing_for_a_dispatch_that_writes_only_to_memory() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        let before = stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let after = stack
            .invoke(
                dialog_query::the!("cmd.increment/counter")
                    .of("cmd:1".parse::<Entity>()?)
                    .is(counter.clone()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(
            after.revision(),
            before.revision(),
            "nothing durable was written"
        );
        assert_eq!(branch.revision().as_ref(), Some(before.revision()));
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()]
        );
        Ok(())
    }

    /// A write named for a layer the stack does not hold is refused
    /// before anything changes: the primary's head and every layer stay as
    /// they were.
    #[dialog_common::test]
    async fn it_refuses_a_write_named_for_a_layer_the_stack_does_not_hold() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let before = branch.revision();
        let doc: Entity = "doc:1".parse()?;
        let refused = stack
            .transaction()
            .assert(
                dialog_query::the!("ui/selected")
                    .of(doc.clone())
                    .is(true)
                    .at(state.clone()),
            )
            .assert(
                dialog_query::the!("doc/note")
                    .of(doc.clone())
                    .is("x".to_string())
                    .at(layer("nowhere")),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await;
        match refused {
            Err(CommitError::UnboundLayer {
                attribute,
                layer: named,
            }) => {
                assert_eq!(attribute, "doc/note");
                assert_eq!(named, layer("nowhere").to_string());
            }
            other => {
                panic!("a write named for a layer the stack does not hold is refused: {other:?}")
            }
        }
        branch.refresh(&operator).await?;
        assert_eq!(branch.revision(), before, "the head stays");
        assert!(
            held(&memory, "ui/selected", &doc)?.is_empty(),
            "the layer stays"
        );
        Ok(())
    }

    /// A transaction that lost its head shows in no layer: the memory
    /// layers take their shares only once the primary's commit publishes.
    #[dialog_common::test]
    async fn it_lands_no_share_of_a_commit_that_lost_its_head() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let doc: Entity = "doc:1".parse()?;
        let transaction = stack
            .transaction()
            .assert(
                dialog_query::the!("ui/selected")
                    .of(doc.clone())
                    .is(true)
                    .at(state.clone()),
            )
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            );
        // Another writer moves the head under the open transaction.
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:2".parse::<Entity>()?)
                    .is("Other".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let refused = transaction.commit().publish().perform(&operator).await;
        assert!(
            matches!(
                refused,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "the moved head refuses the commit: {refused:?}"
        );
        assert!(
            held(&memory, "ui/selected", &doc)?.is_empty(),
            "no share landed"
        );

        // A batch staged before the head moved carries a share for each
        // tick that wrote the layer, and lands none of them when its
        // publish loses the compare-and-swap.
        let counter: Entity = "ctr:1".parse()?;
        stack.primary().refresh(&operator).await?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let start = memory.revision();
        let batch = stack
            .transaction()
            .assert(flag(&doc, true, &state))
            .dispatch(
                dialog_query::the!("cmd.increment/counter")
                    .of("cmd:1".parse::<Entity>()?)
                    .is(counter.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(
            batch.shares().len(),
            2,
            "the transaction's own write and the rule's are two ticks"
        );
        other.refresh(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:3".parse::<Entity>()?)
                    .is("Third".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let refused = batch.publish().perform(&operator).await;
        assert!(
            matches!(
                refused,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "the moved head refuses the publish: {refused:?}"
        );
        assert_eq!(memory.revision(), start, "no tick landed an instant");
        assert!(held(&memory, "ui/selected", &doc)?.is_empty());
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(1)".to_string()]
        );
        Ok(())
    }

    /// The `dialog.layer/revision` row `own` holds of `layer`, as text.
    fn revision_of(own: &Ephemeral, layer: &Entity) -> Result<Vec<String>> {
        held(own, "dialog.layer/revision", layer)
    }

    /// The values of the primary's `dialog.branch/revision` cell read
    /// through `stack`, as text: the branch metadata's row, read as
    /// `session:`, the only one of the cell.
    async fn primary_revision_of(stack: &Stack, env: &Operator) -> Result<Vec<String>> {
        let primary = stack
            .branch_of(&crate::memory_layer())
            .expect("the primary's branch")
            .clone();
        read::<Entity>(stack.query(), env, "dialog.branch/revision", &primary).await
    }

    /// A branch's revision as its `dialog.branch/revision` row names it:
    /// the revision's entity.
    fn entity_text(revision: &crate::Revision) -> String {
        format!("{:?}", revision_entity(revision))
    }

    /// A memory layer's revision as its `dialog.layer/revision` row names
    /// it.
    fn chain_text(memory: &Ephemeral) -> String {
        use base58::ToBase58 as _;
        let hash = memory.revision().hash;
        format!(
            "{:?}",
            dialog_corpus::Value::String(hash.as_bytes().to_base58())
        )
    }

    /// The stack's own layer's revision as its `dialog.layer/revision`
    /// row names it: its chain as it stood before the records that
    /// followed its last change.
    fn settled_text(own: &Ephemeral) -> String {
        use base58::ToBase58 as _;
        format!(
            "{:?}",
            dialog_corpus::Value::String(own.settled().as_bytes().to_base58())
        )
    }

    /// A stack freshly opened on a plain branch holds, in its own
    /// `session:`, the `dialog.layer/revision` rows of `session:` and
    /// `control:`, written as one instant when it opened, and beside them
    /// the three layers' type facts and the primary's branch. The
    /// primary's `dialog.branch/revision`, of its branch entity, naming
    /// its revision's entity, is read in `session:` too, from the branch
    /// metadata, as its one row: the stack writes none of its own. A
    /// second stack opened on the branch mints a `session:` of its own,
    /// holding the same rows, and writes nothing into the first's.
    #[dialog_common::test]
    async fn it_holds_the_revision_rows_of_its_layers_from_open() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("Plain".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let head = branch.revision().expect("a head");
        let stack = branch.stack().perform(&operator).await?;
        let own = ephemeral(&stack, &session_layer())
            .expect("its own layer")
            .clone();
        let primary = branch_entity(&branch, &operator, "main").await?;
        assert_eq!(
            stack.branch_of(&crate::memory_layer()),
            Some(&primary),
            "the primary's branch entity is the meta branch's"
        );
        assert_eq!(own.revision().sequence, 1, "one instant, at open");
        assert_eq!(
            revision_rows_of(&own),
            vec![
                format!("control: = {}", channel_text(&stack, &control_layer())),
                format!("session: = {}", settled_text(&own)),
            ],
            "the memory layers' rows"
        );
        assert_eq!(
            primary_revision_of(&stack, &operator).await?,
            vec![entity_text(&head)],
            "the primary's row names its revision's entity"
        );
        assert!(
            held(&own, "dialog.branch/revision", &primary)?.is_empty(),
            "the stack writes no row of the primary's cell: the metadata's is the one"
        );
        let implied = |of: &Entity, the: &str| held(&own, the, of);
        assert_eq!(
            implied(&control_layer(), "dialog.layer/type")?,
            vec!["Entity(channel:)".to_string()]
        );
        assert_eq!(
            implied(&session_layer(), "dialog.layer/type")?,
            vec!["Entity(session:)".to_string()]
        );
        assert_eq!(
            implied(&memory_layer(), "dialog.layer/type")?,
            vec!["Entity(branch:)".to_string()]
        );
        assert_eq!(
            implied(&memory_layer(), "dialog.layer/branch")?,
            vec![format!("Entity({primary})")]
        );

        let again = branch.stack().perform(&operator).await?;
        assert_eq!(again.layers(), stack.layers());
        let second = ephemeral(&again, &session_layer()).expect("its own layer");
        assert_eq!(second.revision().sequence, 1, "its own, one instant");
        assert_eq!(second.export(), own.export(), "holding the same rows");
        assert_eq!(
            own.revision().sequence,
            1,
            "a second stack writes nothing into the first's"
        );
        assert!(branch.overlay().is_empty(), "nor into the overlay");
        Ok(())
    }

    /// A schema declaring one layer of each type opens a stack that
    /// lists them after the three every stack has: the session layer and
    /// the channel, held in memory, in the schema's order, then the
    /// durable layer, which is neither a cell nor a branch. The
    /// declarations are stored in the primary as `dialog.layer/type`
    /// alone; the stack's own layer holds the implied layers' type
    /// facts and the primary's branch.
    #[dialog_common::test]
    async fn it_opens_a_layer_of_each_type_the_schema_declares() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let tab: Entity = "session:tab".parse()?;
        let mail: Entity = "mail:box".parse()?;
        let notes: Entity = "memory:notes".parse()?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(tab.clone(), LayerType::Session))
            .assert(LayerDeclaration::new(mail.clone(), LayerType::Channel))
            .assert(LayerDeclaration::new(notes.clone(), LayerType::Branch))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let stack = branch.stack().perform(&operator).await?;
        assert_eq!(
            stack.layers(),
            implied_and([mail.clone(), tab.clone(), notes.clone()])
        );
        assert!(
            matches!(stack.layer(&tab), Some(Layer::Ephemeral(_))),
            "the session layer is in memory"
        );
        assert!(
            matches!(stack.layer(&mail), Some(Layer::Channel(_))),
            "the channel is a channel"
        );
        assert!(
            matches!(stack.layer(&notes), Some(Layer::Durable(_))),
            "the durable layer is no cell"
        );
        assert_eq!(stack.branch_of(&notes), None, "and no branch");

        for (layer, kind) in [(&tab, "session:"), (&mail, "channel:"), (&notes, "branch:")] {
            assert_eq!(
                stored(&branch, &operator, "dialog.layer/type", layer).await?,
                vec![format!("Entity({kind})")],
                "{layer}'s type is stored in the primary"
            );
        }
        assert!(
            stored(&branch, &operator, "dialog.layer/branch", &notes)
                .await?
                .is_empty(),
            "a durable layer names no branch"
        );

        let own = ephemeral(&stack, &session_layer()).expect("its own layer");
        let primary = branch_entity(&branch, &operator, "main").await?;
        for (layer, the, is) in [
            (
                control_layer(),
                "dialog.layer/type",
                "Entity(channel:)".to_string(),
            ),
            (
                session_layer(),
                "dialog.layer/type",
                "Entity(session:)".to_string(),
            ),
            (
                memory_layer(),
                "dialog.layer/type",
                "Entity(branch:)".to_string(),
            ),
            (
                memory_layer(),
                "dialog.layer/branch",
                format!("Entity({primary})"),
            ),
        ] {
            assert_eq!(held(own, the, &layer)?, vec![is], "{layer} {the}");
        }
        Ok(())
    }

    /// The `dialog.layer/store` declarations of earlier builds are not
    /// read: a schema holding only those opens a stack with the three
    /// layers every stack has and no other.
    #[dialog_common::test]
    async fn it_ignores_a_layer_store_declaration() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let store: dialog_corpus::Relation = "dialog.layer/store".parse()?;
        let mut declarations = Changes::new();
        declarations.associate(
            store.clone(),
            "session:state".parse()?,
            dialog_corpus::Value::String("memory".into()),
            dialog_corpus::Pick::Last,
        );
        declarations.associate(
            store,
            "memory:notes".parse()?,
            dialog_corpus::Value::String("durable".into()),
            dialog_corpus::Pick::Last,
        );
        branch
            .transaction()
            .assert(declarations)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let stack = branch.stack().perform(&operator).await?;
        assert_eq!(stack.layers(), implied_and([]));
        assert!(
            stack.durables().layers().is_empty(),
            "no durable layer is declared"
        );
        Ok(())
    }

    /// After each dispatch publishes, the stack's own layer holds one
    /// `dialog.layer/revision` row per memory layer and channel, under
    /// `last`; a second dispatch replaces the rows the first wrote. The
    /// primary's `dialog.branch/revision`, read through the stack from
    /// the branch metadata, follows the primary's head, one value at a
    /// time.
    #[dialog_common::test]
    async fn it_records_each_layers_revision_in_its_own_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own_name = crate::session_layer();
        let own = ephemeral(&stack, &own_name).expect("its own layer").clone();
        let control = crate::control_layer();
        let doc: Entity = "doc:1".parse()?;

        let mut primary_rows = Vec::new();
        for title in ["First", "Second"] {
            let revision = stack
                .assert(
                    dialog_query::the!("doc/title")
                        .of(doc.clone())
                        .is(title.to_string()),
                )
                .assert(flag(&doc, title == "First", &state))
                .perform(&operator)
                .await?;
            let rows = own.scan(&Selector::new().the(crate::revision_attr()));
            assert_eq!(rows.len(), 3, "one row per memory layer: {rows:?}");
            for layer in [&state, &control, &own_name] {
                assert_eq!(revision_of(&own, layer)?.len(), 1, "one row of {layer}");
            }
            let primary = primary_revision_of(&stack, &operator).await?;
            assert_eq!(
                primary,
                vec![entity_text(revision.revision())],
                "the published revision"
            );
            primary_rows.push(primary);
        }
        assert_ne!(
            primary_rows[0], primary_rows[1],
            "the primary's row moved with its head"
        );
        Ok(())
    }

    /// A rule premised on a layer's revision fires for a layer that moved
    /// since the stack last recorded it, with the revision it stood at
    /// as the dispatch began. The stack records every layer when it
    /// opens, so its first dispatch finds nothing moved and the rule
    /// fires for no layer. Once the primary, a memory layer and the
    /// stack's own layer move outside a dispatch (a commit on the
    /// branch, writes into the layers directly), the next dispatch fires
    /// it for each, the primary's through its `dialog.branch/revision`
    /// row, the metadata's, with no join on `dialog.layer/branch`; the one
    /// after finds each where that dispatch recorded it, so the rule does
    /// not fire again. The primary's rule is installed through the stack,
    /// since the metadata row is read on a plain branch too and installing
    /// a rule evaluates it once, on the head the install began at.
    #[dialog_common::test]
    async fn it_fires_a_rule_premised_on_a_layers_revision() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let watch: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "revision": {
                        "the": "seen/revision", "as": "Text", "pick": "last",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": { "revision": { "the": "dialog.layer/revision", "as": "Text" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "revision": { "?": { "name": "revision" } }
                    }
                }
            ]
        }))?;
        // The primary's `dialog.branch/revision` is one row of
        // `session:`, the branch metadata's, which the stack records no
        // second row beside: a rule premised on it alone fires once per
        // move, with no join to tell a stack apart from a plain branch.
        let watch_primary: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "revision": {
                        "the": "seen/branch-revision", "as": "Entity", "pick": "last",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "revision": {
                                "the": "dialog.branch/revision", "as": "Entity",
                                "in": "session:"
                            }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "revision": { "?": { "name": "revision" } }
                    }
                }
            ]
        }))?;
        branch
            .transaction()
            .assert(watch)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let primary = stack
            .branch_of(&crate::memory_layer())
            .expect("the primary's branch")
            .clone();
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let own_name = crate::session_layer();
        let own = ephemeral(&stack, &own_name).expect("its own layer").clone();
        let doc: Entity = "doc:1".parse()?;
        let seen = |of: &Entity| held(&memory, "seen/revision", of);
        let seen_primary = || held(&memory, "seen/branch-revision", &primary);

        let installed_on = stack.primary().revision().expect("a head");
        stack.assert(watch_primary).perform(&operator).await?;
        assert_eq!(
            seen_primary()?,
            vec![entity_text(&installed_on)],
            "installing the rule evaluated it on the head it began at, the one row of the cell"
        );
        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        assert_eq!(
            seen_primary()?,
            vec![entity_text(&installed_on)],
            "the install's own commit was recorded once it published: the rule did not fire"
        );
        for layer in [&state, &own_name, &crate::control_layer()] {
            assert!(seen(layer)?.is_empty(), "{layer} was recorded at open");
        }

        // Every layer moves outside a dispatch: the primary by a commit
        // on the branch, the memory layer and the stack's own by writes
        // into them directly.
        branch
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
        stack.primary().refresh(&operator).await?;
        let moved = stack.primary().revision().expect("a revision");
        memory.assert(primary_flag(&"doc:2".parse()?, true))?;
        own.assert(primary_flag(&"doc:3".parse()?, true))?;
        let (state_at, own_at) = (chain_text(&memory), settled_text(&own));

        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            seen_primary()?,
            vec![entity_text(&moved)],
            "the rule fired, on the primary's revision as the dispatch began"
        );
        assert_eq!(
            seen(&state)?,
            vec![state_at.clone()],
            "and on the memory layer's"
        );
        assert_eq!(
            seen(&own_name)?,
            vec![own_at.clone()],
            "and on the stack's own layer's"
        );
        assert!(
            seen(&crate::control_layer())?.is_empty(),
            "control: did not move"
        );

        // The next dispatch finds every layer where the last dispatch
        // recorded it (the memory layer as the rule's own writes left
        // it), so no revision is stimulus and the rule does not fire.
        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(seen_primary()?, vec![entity_text(&moved)]);
        assert_eq!(seen(&state)?, vec![state_at]);
        assert_eq!(seen(&own_name)?, vec![own_at]);
        Ok(())
    }

    /// Every `dialog.layer/revision` row `own` holds, as `of = is` text,
    /// sorted.
    fn revision_rows_of(own: &Ephemeral) -> Vec<String> {
        let mut rows: Vec<String> = own
            .scan(&Selector::new().the(crate::revision_attr()))
            .into_iter()
            .map(|fact| format!("{} = {:?}", fact.of, fact.is))
            .collect();
        rows.sort();
        rows
    }

    /// A dispatch after which no layer stands anywhere new writes no
    /// revision row and mints no instant in `session:`. One that only
    /// dispatches a command no rule answers moves `control:` alone, by
    /// landing the command there for one tick, so it rewrites
    /// `control:`'s row alone, as one instant.
    #[dialog_common::test]
    async fn it_records_no_revision_when_nothing_moved() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own = ephemeral(&stack, &session_layer())
            .expect("its own layer")
            .clone();
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        let sequence = own.revision().sequence;
        let rows = revision_rows_of(&own);
        assert_eq!(rows.len(), 3, "every memory layer is recorded");
        let primary = primary_revision_of(&stack, &operator).await?;

        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            own.revision().sequence,
            sequence,
            "a dispatch that wrote nothing minted nothing in session:"
        );
        assert_eq!(revision_rows_of(&own), rows);
        assert_eq!(primary_revision_of(&stack, &operator).await?, primary);

        let control = crate::control_layer();
        let before = revision_of(&own, &control)?;
        stack
            .assert(flag(&doc, false, &control))
            .perform(&operator)
            .await?;
        assert_eq!(
            own.revision().sequence,
            sequence + 1,
            "a dispatch of a command no rule answers records control: as one instant"
        );
        let after = revision_of(&own, &control)?;
        assert_ne!(after, before, "the command moved control:");
        assert_eq!(
            after,
            vec![channel_text(&stack, &control)],
            "control:'s row names where the landing left it"
        );
        let others = |rows: Vec<String>| -> Vec<String> {
            rows.into_iter()
                .filter(|row| !row.starts_with("control: "))
                .collect()
        };
        assert_eq!(
            others(revision_rows_of(&own)),
            others(rows),
            "no other memory layer's row was written"
        );
        assert_eq!(primary_revision_of(&stack, &operator).await?, primary);
        Ok(())
    }

    /// A dispatch that moves the primary alone writes nothing into the
    /// stack's own layer: the primary's row, read in `session:` from the
    /// branch metadata, names the revision it published, and the memory
    /// layer's row and the stack's own stay as the dispatch before
    /// recorded them.
    #[dialog_common::test]
    async fn it_records_only_the_layer_that_moved() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own_name = crate::session_layer();
        let own = ephemeral(&stack, &own_name).expect("its own layer").clone();
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        let primary_before = primary_revision_of(&stack, &operator).await?;
        let state_before = revision_of(&own, &state)?;
        let own_before = revision_of(&own, &own_name)?;
        let sequence = own.revision().sequence;

        let published = stack
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Moved".to_string()),
            )
            .perform(&operator)
            .await?;
        let primary_after = primary_revision_of(&stack, &operator).await?;
        assert_eq!(
            primary_after,
            vec![entity_text(published.revision())],
            "the new revision"
        );
        assert_ne!(primary_after, primary_before, "the primary's row moved");
        assert_eq!(
            revision_of(&own, &state)?,
            state_before,
            "the memory layer did not move, so its row was not written"
        );
        assert_eq!(
            revision_of(&own, &own_name)?,
            own_before,
            "nothing was written into session:, so its row was not either"
        );
        assert_eq!(
            own.revision().sequence,
            sequence,
            "the primary's move is recorded in no row of the stack's own"
        );
        Ok(())
    }

    /// While a layer is selected, a write naming no layer lands in it,
    /// and the primary's head does not move for it; machinery under
    /// `dialog.` still lands in the primary; with the selection cleared,
    /// the same write lands in the primary again.
    #[dialog_common::test]
    async fn it_lands_a_write_naming_no_layer_in_the_selected_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let doc: Entity = "doc:1".parse()?;
        let title = |text: &str| {
            dialog_query::the!("doc/title")
                .of(doc.clone())
                .is(text.to_string())
        };

        assert_eq!(stack.selected(), None);
        stack.select(Some(state.clone())).perform(&operator).await?;
        assert_eq!(stack.selected(), Some(state.clone()));

        let before = branch.revision();
        stack.assert(title("Notes")).perform(&operator).await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            held(&memory, "doc/title", &doc)?,
            vec!["String(\"Notes\")".to_string()],
            "the write lands in the selected layer"
        );
        assert!(
            committed::<String>(&branch, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "and not in the tree"
        );
        assert_eq!(branch.revision(), before, "the primary's head stays");

        let other = layer("other");
        stack
            .assert(LayerDeclaration::new(other.clone(), LayerType::Session))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_ne!(branch.revision(), before, "the declaration committed");
        assert_eq!(
            committed::<Entity>(&branch, &operator, "dialog.layer/type", &other).await?,
            vec!["Entity(session:)".to_string()],
            "a declaration never follows the selection"
        );
        assert!(held(&memory, "dialog.layer/type", &other)?.is_empty());

        stack.select(None).perform(&operator).await?;
        assert_eq!(stack.selected(), None, "the selection is cleared");
        stack.assert(title("Other")).perform(&operator).await?;
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &doc).await?,
            vec!["String(\"Other\")".to_string()],
            "with none selected, the write lands in the primary"
        );
        assert_eq!(
            held(&memory, "doc/title", &doc)?,
            vec!["String(\"Notes\")".to_string()],
            "and not in the layer"
        );
        Ok(())
    }

    /// A rule head whose descriptor names no layer follows the selection.
    #[dialog_common::test]
    async fn it_lands_a_rule_head_naming_no_layer_in_the_selected_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let mark: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "note": { "the": "doc/note", "as": "Text", "pick": "last" } }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "doc": { "the": "cmd.mark/doc", "as": "Entity" },
                            "note": { "the": "cmd.mark/note", "as": "Text" }
                        }
                    },
                    "where": {
                        "doc": { "?": { "name": "this" } },
                        "note": { "?": { "name": "note" } }
                    }
                }
            ]
        }))?;
        branch
            .transaction()
            .assert(mark)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        stack.select(Some(state.clone())).perform(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        let command: Entity = "cmd:1".parse()?;
        let mut marked = Changes::new();
        dialog_query::the!("cmd.mark/doc")
            .of(command.clone())
            .is(doc.clone())
            .assert(&mut marked);
        dialog_query::the!("cmd.mark/note")
            .of(command)
            .is("hi".to_string())
            .assert(&mut marked);
        let before = branch.revision();
        stack.invoke(marked).perform(&operator).await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            held(&memory, "doc/note", &doc)?,
            vec!["String(\"hi\")".to_string()],
            "the head lands in the selected layer"
        );
        assert!(
            committed::<String>(&branch, &operator, "doc/note", &doc)
                .await?
                .is_empty()
        );
        assert_eq!(branch.revision(), before, "nothing reached the tree");
        Ok(())
    }

    /// A selected layer the stack does not hold refuses the commit
    /// that would land a write in it, and nothing is written: not the
    /// tree, not a layer.
    #[dialog_common::test]
    async fn it_refuses_a_write_into_a_selected_layer_the_stack_does_not_hold() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let nowhere = layer("nowhere");
        stack
            .select(Some(nowhere.clone()))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        let before = branch.revision();
        let refused = stack
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await;
        assert!(
            matches!(
                &refused,
                Err(CommitError::UnboundLayer { attribute, layer })
                    if attribute == "doc/title" && *layer == nowhere.to_string()
            ),
            "{:?}",
            refused.err()
        );
        branch.refresh(&operator).await?;
        assert_eq!(branch.revision(), before, "the head stays");
        assert!(
            committed::<String>(&branch, &operator, "doc/title", &doc)
                .await?
                .is_empty()
        );
        assert!(
            held(&memory, "ui/selected", &doc)?.is_empty(),
            "the layer stays"
        );
        assert!(held(branch.overlay(), "doc/title", &doc)?.is_empty());
        Ok(())
    }

    /// The selected flags, as `(entity, value)` strings, sorted.
    fn selected(claims: &[dialog_query::Claim]) -> Vec<String> {
        let mut rows: Vec<String> = claims
            .iter()
            .map(|claim| format!("{} {:?}", claim.of, claim.is))
            .collect();
        rows.sort();
        rows
    }

    /// `ui/selected` of every entity, with the value a variable.
    fn selected_query() -> AttributeQuery {
        AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "ui/selected".parse::<dialog_corpus::Relation>().unwrap(),
            ))
            .of(Term::<Entity>::var("e"))
            .is(Term::<bool>::var("v")),
        )
    }

    /// What a fresh evaluation of `query` through the stack reads.
    async fn fresh(stack: &Stack, env: &Operator, query: AttributeQuery) -> Result<Vec<String>> {
        let claims: Vec<dialog_query::Claim> =
            stack.query().select(query).perform(env).try_vec().await?;
        Ok(selected(&claims))
    }

    /// `ui/selected` of `doc` as `value`, written into `at`.
    fn flag(doc: &Entity, value: bool, at: &Entity) -> impl Statement {
        dialog_query::the!("ui/selected")
            .of(doc.clone())
            .is(value)
            .at(at.clone())
    }

    /// `ui/selected` of `doc` as `value`, written with no layer named:
    /// into the primary.
    fn primary_flag(doc: &Entity, value: bool) -> impl Statement {
        dialog_query::the!("ui/selected").of(doc.clone()).is(value)
    }

    /// A subscription through a stack reads every layer, and a share a
    /// dispatch lands in a memory layer propagates as a delta on the
    /// next poll, maintained from the layer's instants. A commit to the
    /// primary outside the cover advances the pins for free.
    #[dialog_common::test]
    async fn it_subscribes_through_a_stack() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        let mut subscription = stack.subscribe(selected_query());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty());

        stack
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("a share landed in the state layer");
        assert_eq!(
            selected(&delta.asserted),
            vec![format!("{doc} Boolean(true)")]
        );
        assert!(delta.retracted.is_empty());
        assert_eq!(
            subscription.maintenances(),
            1,
            "maintained from the layer's instants"
        );
        assert_eq!(subscription.recomputes(), 1);

        stack
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .perform(&operator)
            .await?;
        assert!(
            subscription.poll(&operator).await?.is_none(),
            "a commit outside the cover changes nothing"
        );

        stack
            .assert(Changes::new())
            .retract(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        let delta = subscription.poll(&operator).await?.expect("retracted");
        assert!(delta.asserted.is_empty());
        assert_eq!(
            selected(&delta.retracted),
            vec![format!("{doc} Boolean(true)")]
        );
        assert!(subscription.poll(&operator).await?.is_none());
        Ok(())
    }

    /// A read with `in` bound reads that layer alone; with `in` a
    /// variable it reads every layer and binds each row's. The same cell
    /// lies in two layers as two rows.
    #[dialog_common::test]
    async fn it_reads_one_layer_with_in_bound_and_binds_it_unbound() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(flag(&doc, true, &state))
            .assert(primary_flag(&doc, false))
            .perform(&operator)
            .await?;

        let rows = |query: AttributeQuery| {
            let stack = stack.clone();
            let operator = &operator;
            async move {
                let claims: Vec<dialog_query::Claim> = stack
                    .query()
                    .select(query)
                    .perform(operator)
                    .try_vec()
                    .await?;
                let mut rows: Vec<String> = claims
                    .iter()
                    .map(|claim| {
                        // A row with no layer stamped is the primary's.
                        format!(
                            "{:?} in {}",
                            claim.is,
                            claim
                                .layer()
                                .map(ToString::to_string)
                                .unwrap_or_else(|| crate::MEMORY.to_string())
                        )
                    })
                    .collect();
                rows.sort();
                Ok::<_, anyhow::Error>(rows)
            }
        };
        assert_eq!(
            rows(
                selected_query()
                    .with_layer(Term::Constant(dialog_corpus::Value::Entity(state.clone())))
            )
            .await?,
            vec![format!("Boolean(true) in {state}")],
            "`in` bound to the layer reads it alone"
        );
        assert_eq!(
            rows(
                selected_query().with_layer(Term::Constant(dialog_corpus::Value::Entity(
                    crate::memory_layer()
                )))
            )
            .await?,
            vec!["Boolean(false) in memory:".to_string()],
            "`in` bound to the primary reads it alone"
        );
        assert_eq!(
            rows(selected_query().with_layer(Term::var("where"))).await?,
            vec![
                "Boolean(false) in memory:".to_string(),
                format!("Boolean(true) in {state}")
            ],
            "`in` unbound reads every layer and binds each row's"
        );
        Ok(())
    }

    /// Declare, beside [`declare_state`]'s counter, a rule stepping the
    /// count the state layer holds to the next one a primary table of
    /// `succ/from` and `succ/to` names, and the table: 2 steps to 3,
    /// 3 to 4, and 4 to nothing. A count of 2 therefore steps twice, in
    /// two ticks, and stops.
    async fn declare_steps(branch: &Branch, env: &Operator) -> Result<()> {
        declare_steps_in(branch, env, "session:state").await
    }

    /// [`declare_steps`], with the count read from and written into the
    /// layer named `at`.
    async fn declare_steps_in(branch: &Branch, env: &Operator, at: &str) -> Result<()> {
        let step: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "count": {
                        "the": "counter/count", "as": "UnsignedInteger", "pick": "last",
                        "in": at
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "count": {
                                "the": "counter/count", "as": "UnsignedInteger", "pick": "last",
                                "in": at
                            }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "count": { "?": { "name": "prev" } }
                    }
                },
                {
                    "assert": {
                        "with": {
                            "from": { "the": "succ/from", "as": "UnsignedInteger" },
                            "to": { "the": "succ/to", "as": "UnsignedInteger" }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "step" } },
                        "from": { "?": { "name": "prev" } },
                        "to": { "?": { "name": "count" } }
                    }
                }
            ]
        }))?;
        let mut transaction = branch.transaction().assert(step);
        for (index, (from, to)) in [(2u64, 3u64), (3, 4)].into_iter().enumerate() {
            let entity: Entity = format!("step:{index}").parse()?;
            transaction = transaction
                .assert(dialog_query::the!("succ/from").of(entity.clone()).is(from))
                .assert(dialog_query::the!("succ/to").of(entity).is(to));
        }
        transaction.commit().publish().perform(env).await?;
        branch.refresh(env).await?;
        Ok(())
    }

    /// `counter/count` of every entity, with the value a variable.
    fn count_query() -> AttributeQuery {
        AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "counter/count".parse::<dialog_corpus::Relation>().unwrap(),
            ))
            .of(Term::<Entity>::var("e"))
            .is(Term::<u64>::var("v")),
        )
    }

    /// The `counter/count` values each instant asserts, instant by instant.
    fn counts(instants: &[crate::Instant]) -> Vec<Vec<String>> {
        instants
            .iter()
            .map(|instant| {
                instant
                    .asserted
                    .iter()
                    .filter(|fact| fact.the.to_string() == "counter/count")
                    .map(|fact| format!("{:?}", fact.is))
                    .collect()
            })
            .collect()
    }

    /// A dispatch whose rules write the same memory cell in three ticks
    /// mints three instants in the layer, in tick order: a reader of the
    /// layer's instants sees each count the rules saw, and a
    /// subscription polled after the dispatch equals a fresh evaluation
    /// without evaluating afresh.
    #[dialog_common::test]
    async fn it_mints_one_instant_per_tick_that_wrote_a_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        declare_steps(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let mut subscription = stack.subscribe(count_query());
        subscription.poll(&operator).await?;

        let start = memory.revision().sequence;
        stack
            .invoke(
                dialog_query::the!("cmd.increment/counter")
                    .of("cmd:1".parse::<Entity>()?)
                    .is(counter.clone()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(
            memory.revision().sequence,
            start + 3,
            "the increment and the two steps are three ticks"
        );
        let instants = memory.since(start).expect("the ring reaches back");
        assert_eq!(
            counts(&instants),
            vec![
                vec!["UnsignedInt(2)".to_string()],
                vec!["UnsignedInt(3)".to_string()],
                vec!["UnsignedInt(4)".to_string()],
            ],
            "each tick's count, in order"
        );
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(4)".to_string()]
        );

        subscription.poll(&operator).await?;
        let mut retained = selected(subscription.results());
        retained.sort();
        assert_eq!(retained, fresh(&stack, &operator, count_query()).await?);
        assert_eq!(retained, vec![format!("{counter} UnsignedInt(4)")]);
        assert_eq!(
            subscription.recomputes(),
            1,
            "the instants were maintained, in order"
        );
        Ok(())
    }

    /// The link's own writes into a layer are its first tick: a rule
    /// premised on the layer's row fires on them in the same dispatch,
    /// reading the layer as that tick leaves it, and the chain lands one
    /// instant per tick.
    #[dialog_common::test]
    async fn it_fires_a_rule_on_the_links_own_write_into_a_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        declare_steps(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;

        let start = memory.revision().sequence;
        stack
            .assert(count(&counter, 2, &state))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(4)".to_string()],
            "the write of 2 stepped to 3, then 4"
        );
        assert_eq!(
            memory.revision().sequence,
            start + 3,
            "the link's own tick and the two steps"
        );
        let instants = memory.since(start).expect("the ring reaches back");
        assert_eq!(
            counts(&instants),
            vec![
                vec!["UnsignedInt(2)".to_string()],
                vec!["UnsignedInt(3)".to_string()],
                vec!["UnsignedInt(4)".to_string()],
            ],
            "each tick's count, in order"
        );
        Ok(())
    }

    /// A head under `last` landing in the stack's own layer succeeds
    /// the row that layer already holds, within the dispatch: the rounds
    /// after read the head, not the older row, so a chain over
    /// `session:` runs as one over a declared layer does.
    #[dialog_common::test]
    async fn it_succeeds_the_stacks_own_row_within_a_dispatch() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_steps_in(&branch, &operator, crate::SESSION).await?;
        branch.refresh(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own = crate::session_layer();
        let layer = ephemeral(&stack, &own).expect("its own layer").clone();
        let counter: Entity = "ctr:1".parse()?;

        // A row the layer holds before the dispatch, which no step follows.
        stack
            .assert(count(&counter, 1, &own))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&layer, "counter/count", &counter)?,
            vec!["UnsignedInt(1)".to_string()]
        );
        // The dispatch writes 2 above it; the steps follow to 4.
        stack
            .assert(count(&counter, 2, &own))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&layer, "counter/count", &counter)?,
            vec!["UnsignedInt(4)".to_string()],
            "each step read the step before it, not the row the layer held"
        );
        Ok(())
    }

    /// A tick that writes no memory layer mints nothing there: a
    /// dispatch whose own writes land in the primary and whose rule
    /// writes the layer mints one instant, one writing the layer in both
    /// ticks mints two, and one writing only the primary mints none.
    #[dialog_common::test]
    async fn it_mints_nothing_in_a_layer_for_a_tick_that_wrote_none() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let increment = || {
            dialog_query::the!("cmd.increment/counter")
                .of("cmd:1".parse::<Entity>().expect("an entity"))
                .is(counter.clone())
        };

        let start = memory.revision().sequence;
        stack
            .invoke(increment())
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(
            memory.revision().sequence,
            start + 1,
            "only the rule's tick wrote the layer"
        );

        let start = memory.revision().sequence;
        stack
            .invoke(increment())
            .assert(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        assert_eq!(
            memory.revision().sequence,
            start + 2,
            "the dispatch's own tick and the rule's both wrote the layer"
        );

        let start = memory.revision().sequence;
        stack
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Other".to_string()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(memory.revision().sequence, start, "no tick wrote the layer");
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(3)".to_string()]
        );
        Ok(())
    }

    /// Declare a chain of documents in the primary, `doc:1` to `doc:4`
    /// by `doc/next`, and two rules selecting into the state layer: a
    /// `cmd.spread/doc` command selects its document, and a selected
    /// document selects the next one. A spread from `doc:1` writes the
    /// layer in a tick per document it newly selects.
    async fn declare_spread(branch: &Branch, env: &Operator) -> Result<()> {
        let spread: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "selected": { "the": "ui/selected", "as": "Boolean", "in": "session:state" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": { "doc": { "the": "cmd.spread/doc", "as": "Entity" } }
                    },
                    "where": { "doc": { "?": { "name": "this" } } }
                },
                { "assert": "==", "where": { "this": { "?": { "name": "selected" } }, "is": true } }
            ]
        }))?;
        let follow: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "selected": { "the": "ui/selected", "as": "Boolean", "in": "session:state" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "selected": {
                                "the": "ui/selected", "as": "Boolean", "in": "session:state"
                            }
                        }
                    },
                    "where": { "this": { "?": { "name": "prev" } }, "selected": true }
                },
                {
                    "assert": {
                        "with": { "next": { "the": "doc/next", "as": "Entity" } }
                    },
                    "where": {
                        "this": { "?": { "name": "prev" } },
                        "next": { "?": { "name": "this" } }
                    }
                },
                { "assert": "==", "where": { "this": { "?": { "name": "selected" } }, "is": true } }
            ]
        }))?;
        let mut transaction = branch.transaction().assert(spread).assert(follow);
        for index in 1..4 {
            transaction = transaction.assert(
                dialog_query::the!("doc/next")
                    .of(format!("doc:{index}").parse::<Entity>()?)
                    .is(format!("doc:{}", index + 1).parse::<Entity>()?),
            );
        }
        transaction.commit().publish().perform(env).await?;
        branch.refresh(env).await?;
        Ok(())
    }

    /// A spread whose rules select the same chain in successive ticks
    /// mints an instant per tick that selected a document, and a
    /// subscription polled after it reads the instants in order.
    #[dialog_common::test]
    async fn it_spreads_a_selection_one_tick_at_a_time() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        declare_spread(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let mut subscription = stack.subscribe(selected_query());
        subscription.poll(&operator).await?;

        let start = memory.revision().sequence;
        stack
            .invoke(
                dialog_query::the!("cmd.spread/doc")
                    .of("cmd:1".parse::<Entity>()?)
                    .is("doc:1".parse::<Entity>()?),
            )
            .perform(&operator)
            .await?;
        assert_eq!(
            memory.revision().sequence,
            start + 4,
            "one tick per document the spread selected"
        );
        let selected_in: Vec<Vec<String>> = memory
            .since(start)
            .expect("the ring reaches back")
            .iter()
            .map(|instant| {
                instant
                    .asserted
                    .iter()
                    .map(|fact| fact.of.to_string())
                    .collect()
            })
            .collect();
        assert_eq!(
            selected_in,
            (1..=4)
                .map(|index| vec![format!("doc:{index}")])
                .collect::<Vec<_>>(),
            "the chain, a document per tick, in order"
        );
        subscription.poll(&operator).await?;
        let mut retained = selected(subscription.results());
        retained.sort();
        assert_eq!(retained, fresh(&stack, &operator, selected_query()).await?);
        assert_eq!(retained.len(), 4);
        assert_eq!(subscription.recomputes(), 1);
        Ok(())
    }

    /// A small deterministic generator for the churn below.
    struct Xorshift(u64);

    impl Xorshift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Through any sequence of dispatches across the primary and a
    /// memory layer, with a rule landing on the layer and the same cell
    /// written in both, and rules writing the layer in several ticks of
    /// one dispatch, a maintained subscription equals a fresh
    /// evaluation after every step, and nothing after the first poll
    /// evaluates afresh.
    #[dialog_common::test]
    async fn it_agrees_with_a_fresh_evaluation_through_stack_churn() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        declare_spread(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let docs: Vec<Entity> = (1..=4)
            .map(|index| format!("doc:{index}").parse())
            .collect::<Result<_, _>>()?;
        let counter: Entity = "ctr:1".parse()?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let mut subscription = stack.subscribe(selected_query());
        subscription.poll(&operator).await?;

        for seed in [7u64, 99, 2024] {
            let mut rng = Xorshift(seed);
            for step in 0..24 {
                let doc = docs[rng.below(docs.len() as u64) as usize].clone();
                let label = match rng.below(7) {
                    0 => {
                        stack
                            .assert(flag(&doc, true, &state))
                            .perform(&operator)
                            .await?;
                        "select"
                    }
                    1 => {
                        stack
                            .assert(flag(&doc, false, &state))
                            .perform(&operator)
                            .await?;
                        "deselect"
                    }
                    2 => {
                        stack
                            .assert(Changes::new())
                            .retract(flag(&doc, true, &state))
                            .perform(&operator)
                            .await?;
                        "retract"
                    }
                    3 => {
                        stack
                            .invoke(
                                dialog_query::the!("cmd.increment/counter")
                                    .of("cmd:1".parse::<Entity>()?)
                                    .is(counter.clone()),
                            )
                            .assert(
                                dialog_query::the!("doc/title")
                                    .of(doc.clone())
                                    .is(format!("Title {step}")),
                            )
                            .perform(&operator)
                            .await?;
                        "increment and title"
                    }
                    4 => {
                        stack
                            .assert(primary_flag(&doc, step % 2 == 0))
                            .perform(&operator)
                            .await?;
                        "select in the primary"
                    }
                    5 => {
                        stack
                            .invoke(
                                dialog_query::the!("cmd.spread/doc")
                                    .of("cmd:2".parse::<Entity>()?)
                                    .is(doc.clone()),
                            )
                            .perform(&operator)
                            .await?;
                        "spread"
                    }
                    _ => "nothing",
                };
                subscription.poll(&operator).await?;
                let mut retained = selected(subscription.results());
                retained.sort();
                assert_eq!(
                    retained,
                    fresh(&stack, &operator, selected_query()).await?,
                    "seed {seed} step {step} ({label}): the maintained result drifted"
                );
            }
        }
        assert_eq!(
            subscription.recomputes(),
            1,
            "only the first poll evaluates afresh"
        );
        Ok(())
    }

    /// Declare a memory layer `state`, and a rule capturing a
    /// `cmd.fetched/node` answer there (`in:`): the node records every
    /// command that fetched it.
    async fn declare_fetch(branch: &Branch, env: &Operator) -> Result<Entity> {
        let state = layer("state");
        let capture: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "fetched": {
                        "the": "node/fetched", "as": "Entity", "pick": "all",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": { "node": { "the": "cmd.fetched/node", "as": "Entity" } }
                    },
                    "where": {
                        "this": { "?": { "name": "fetched" } },
                        "node": { "?": { "name": "this" } }
                    }
                }
            ]
        }))?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(state.clone(), LayerType::Session))
            .assert(capture)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    fn fetch(command: &str, node: &Entity) -> impl Statement {
        dialog_query::the!("cmd.fetch/node")
            .of(command.parse::<Entity>().expect("entity"))
            .is(node.clone())
    }

    /// `cmd.fetch/node` of every command, with the node a variable: the
    /// command a fetching provider performs.
    fn fetch_query() -> AttributeQuery {
        AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "cmd.fetch/node".parse::<dialog_corpus::Relation>().unwrap(),
            ))
            .of(Term::<Entity>::var("command"))
            .is(Term::<Entity>::var("node")),
        )
    }

    /// The answer to the fetch `command` reads: `cmd.fetched/node` of the
    /// same command, naming it by its entity.
    fn fetched(command: &dialog_query::Claim) -> Result<impl Statement + use<>> {
        let dialog_corpus::Value::Entity(node) = &command.is else {
            anyhow::bail!("a fetch names a node, not {:?}", command.is);
        };
        Ok(dialog_query::the!("cmd.fetched/node")
            .of(command.of.clone())
            .is(node.clone()))
    }

    /// The entities of `claims`, as text, sorted.
    fn subjects(claims: &[dialog_query::Claim]) -> Vec<String> {
        let mut subjects: Vec<String> = claims.iter().map(|claim| claim.of.to_string()).collect();
        subjects.sort();
        subjects
    }

    /// A provider reads each command a published dispatch leaves in
    /// `control:`, once, as a row of `control:`, and answers by invoking:
    /// the answer is the next tick's stimulus, where a rule captures it on
    /// the state layer. A subscription through the stack sees the
    /// capture, nothing durable is written, the expiry of the command is
    /// no arrival, and nothing stays in `control:`.
    #[dialog_common::test]
    async fn it_answers_a_command_through_a_provider() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let control = crate::control_layer();
        let mut fetches = stack
            .arrivals(&control, fetch_query())
            .expect("every stack has control:");
        assert!(
            fetches.poll(&operator).await?.is_empty(),
            "nothing dispatched yet"
        );
        let node: Entity = "node:1".parse()?;
        let mut subscription = stack.subscribe(AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "node/fetched".parse::<dialog_corpus::Relation>()?,
            ))
            .of(Term::<Entity>::var("e"))
            .is(Term::<Entity>::var("v")),
        ));
        assert!(
            subscription
                .poll(&operator)
                .await?
                .expect("initial")
                .asserted
                .is_empty()
        );
        let before = branch.revision();

        stack
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        assert!(
            held(&memory, "node/fetched", &node)?.is_empty(),
            "nothing answered yet"
        );
        let commands = fetches.poll(&operator).await?;
        assert_eq!(subjects(&commands), vec!["cmd:1".to_string()]);
        assert_eq!(
            commands[0].layer(),
            Some(&control),
            "read as a row of control:"
        );
        for command in &commands {
            stack.invoke(fetched(command)?).perform(&operator).await?;
        }

        assert_eq!(branch.revision(), before, "nothing durable was written");
        assert_eq!(
            held(&memory, "node/fetched", &node)?,
            vec!["Entity(cmd:1)".to_string()],
            "the rule captured the answer on the state layer"
        );
        let delta = subscription.poll(&operator).await?.expect("the capture");
        assert_eq!(delta.asserted.len(), 1);
        assert_eq!(delta.asserted[0].of, node);
        assert!(
            fetches.poll(&operator).await?.is_empty(),
            "read once: the answer is no fetch, and the expiry is no arrival"
        );
        assert!(!waiting(&stack, &control), "nothing stays in control:");
        Ok(())
    }

    /// A provider waiting for the next batch is woken by the dispatch that
    /// lands one, and reads it.
    #[dialog_common::test]
    async fn it_wakes_a_provider_waiting_for_the_next_dispatch() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_fetch(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let mut fetches = stack
            .arrivals(&crate::control_layer(), fetch_query())
            .expect("every stack has control:");
        let node: Entity = "node:1".parse()?;
        let (commands, invoked) = join(fetches.next(&operator), async {
            stack.invoke(fetch("cmd:1", &node)).perform(&operator).await
        })
        .await;
        invoked?;
        assert_eq!(subjects(&commands?), vec!["cmd:1".to_string()]);
        Ok(())
    }

    /// Declare, beside [`declare_fetch`]'s capture, a channel
    /// `mail:host`: a layer for a consumer outside the invoke loop.
    async fn declare_host(branch: &Branch, env: &Operator) -> Result<(Entity, Entity)> {
        let state = declare_fetch(branch, env).await?;
        let host: Entity = "mail:host".parse()?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(host.clone(), LayerType::Channel))
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok((state, host))
    }

    /// A rule answering a `cmd.ping/target` command with a
    /// `mail.out/target` of the same entity whose descriptor names
    /// `in: <at>`: a rule's write into a channel.
    async fn declare_forward(branch: &Branch, env: &Operator, at: &Entity) -> Result<()> {
        let forward: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "target": { "the": "mail.out/target", "as": "Entity", "in": at.to_string() }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": { "target": { "the": "cmd.ping/target", "as": "Entity" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "target" } }
                    }
                }
            ]
        }))?;
        branch
            .transaction()
            .assert(forward)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(())
    }

    /// What a dispatch writes into a channel, `.at(channel)` and through a
    /// rule's head, lands on the channel's outside end once its publish
    /// has won, and a reader reads each row once, as a row of the channel.
    /// Nothing of it is a row of any layer after its dispatch. A
    /// transaction through the stack whose publish lost its head is
    /// refused with `VersionMismatch` and lands nothing.
    #[dialog_common::test]
    async fn it_lands_what_a_dispatch_wrote_into_a_channel_on_its_outside_end() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let mailbox: Entity = "mail:box".parse()?;
        declare_forward(&branch, &operator, &mailbox).await?;
        let stack = branch.stack().perform(&operator).await?;
        let channel = stack.channel(&mailbox)?;
        let mut rows = stack.arrivals_rows(&mailbox).expect("the channel");
        let node: Entity = "node:1".parse()?;
        let target: Entity = "doc:1".parse()?;
        let before = branch.revision();

        stack
            .assert(fetch("cmd:1", &node).at(mailbox.clone()))
            .perform(&operator)
            .await?;
        stack
            .invoke(ping("cmd:2", &target))
            .perform(&operator)
            .await?;
        let text = |facts: Vec<dialog_corpus::Fact>| -> Vec<String> {
            facts
                .into_iter()
                .map(|fact| format!("{} {} {:?}", fact.of, fact.the, fact.is))
                .collect()
        };
        assert_eq!(
            text(rows.rows()),
            vec![
                "cmd:1 cmd.fetch/node Entity(node:1)".to_string(),
                "cmd:2 mail.out/target Entity(doc:1)".to_string(),
            ],
            "the write, then the rule's, each a tick of the channel"
        );
        assert!(rows.rows().is_empty(), "read once");
        assert_eq!(channel.sequence(), 2, "two ticks landed");
        assert!(
            fresh(&stack, &operator, whole(&mailbox)).await?.is_empty(),
            "no row of the channel outlives its tick"
        );
        assert_eq!(branch.revision(), before, "nothing durable was written");

        let transaction = stack
            .transaction()
            .assert(fetch("cmd:3", &node).at(mailbox.clone()));
        branch
            .transaction()
            .assert(title(&"doc:2".parse()?, "moved"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let refused = transaction.commit().publish().perform(&operator).await;
        assert!(
            matches!(
                refused,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "{refused:?}"
        );
        assert!(rows.rows().is_empty(), "a lost publish lands nothing");
        assert_eq!(channel.sequence(), 2, "and moves no tick");
        Ok(())
    }

    /// What the outside end of `mail:box` sends is read by the next
    /// dispatch's first tick as rows of `mail:box`, so a rule premised on
    /// it fires once, and by no later tick; it is not landed back on the
    /// outside end. A send no dispatch has taken waits for one.
    #[dialog_common::test]
    async fn it_reads_a_send_at_the_next_dispatchs_first_tick_alone() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let mailbox: Entity = "mail:box".parse()?;
        let state = declare_counter_on(&branch, &operator, &mailbox).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let channel = stack.channel(&mailbox)?;
        let mut rows = stack.arrivals_rows(&mailbox).expect("the channel");

        channel.send(increment("cmd:1", &counter));
        assert!(channel.is_waiting(), "sent, not yet taken");
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(1)".to_string()],
            "nothing reads a send before a dispatch takes it"
        );
        stack.assert(Changes::new()).perform(&operator).await?;
        assert!(!channel.is_waiting(), "taken");
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "the rule read the send as a row of mail:box, once"
        );
        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "no later dispatch reads it"
        );
        assert!(rows.rows().is_empty(), "a send is not landed back outside");
        assert_eq!(channel.sequence(), 0);

        stack
            .assert(increment("cmd:2", &counter))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "a row of the primary is not a row of the channel"
        );
        Ok(())
    }

    /// `stack.channel` creates a channel the stack does not hold, writing
    /// its type into `session:` as one instant, and lists it; a second
    /// call writes nothing and hands out the same channel. The loopback is
    /// the channel of `control:`: what is sent into it is read back on
    /// its outside end once a dispatch has taken it. The channel is the
    /// stack's alone: a stack opened later on the branch does not hold it.
    #[dialog_common::test]
    async fn it_creates_a_channel_at_run_time() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own = ephemeral(&stack, &session_layer())
            .expect("its own layer")
            .clone();
        let mailbox: Entity = "mail:box".parse()?;
        let sequence = own.revision().sequence;

        let channel = stack.channel(&mailbox)?;
        assert_eq!(channel.entity(), &mailbox);
        assert_eq!(own.revision().sequence, sequence + 1, "one instant");
        assert_eq!(
            held(&own, "dialog.layer/type", &mailbox)?,
            vec!["Entity(channel:)".to_string()]
        );
        assert_eq!(stack.layers(), implied_and([mailbox.clone()]));
        assert!(matches!(stack.layer(&mailbox), Some(Layer::Channel(_))));
        let again = stack.channel(&mailbox)?;
        assert_eq!(own.revision().sequence, sequence + 1, "writes nothing");
        again.send(fetch("cmd:1", &"node:1".parse()?));
        assert!(channel.is_waiting(), "the same channel");
        assert_eq!(stack.layers(), implied_and([mailbox.clone()]));

        let control = control_layer();
        let loopback = stack.channel(&control)?;
        assert_eq!(loopback.entity(), &control);
        assert_eq!(own.revision().sequence, sequence + 1, "control: is held");
        let mut fetches = stack.arrivals(&control, fetch_query()).expect("control:");
        loopback.send(fetch("cmd:2", &"node:2".parse()?));
        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            subjects(&fetches.poll(&operator).await?),
            vec!["cmd:2".to_string()],
            "the loopback hands back what it was sent"
        );

        let reopened = branch.stack().perform(&operator).await?;
        assert_eq!(
            reopened.layers(),
            implied_and([]),
            "a stack opened later has a session: of its own, which created no channel"
        );
        assert!(
            branch.overlay().is_empty(),
            "the channel's type was written into no overlay"
        );
        Ok(())
    }

    /// A channel named under `memory:` or `session:` is refused with
    /// `InvalidPlacement`, declared in the schema or created at run time,
    /// and nothing is written into `session:`.
    #[dialog_common::test]
    async fn it_refuses_a_channel_named_for_another_type_of_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own = ephemeral(&stack, &session_layer())
            .expect("its own layer")
            .clone();
        let sequence = own.revision().sequence;
        for name in ["memory:x", "session:x"] {
            let refused: Entity = name.parse()?;
            let created = stack.channel(&refused);
            assert!(
                matches!(created, Err(CommitError::InvalidPlacement { .. })),
                "{name}: {created:?}"
            );
            assert_eq!(own.revision().sequence, sequence, "{name}: nothing written");
            assert_eq!(stack.layers(), implied_and([]), "{name}: nothing held");
        }

        for name in ["memory:x", "session:x"] {
            let (operator, peer) = test_session_with_peer().await;
            let repo = test_repo(&operator, &peer).await;
            let branch = repo.branch("main").open().perform(&operator).await?;
            branch
                .transaction()
                .assert(LayerDeclaration::new(name.parse()?, LayerType::Channel))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            branch.refresh(&operator).await?;
            let opened = branch.stack().perform(&operator).await;
            assert!(
                matches!(opened, Err(CommitError::InvalidPlacement { .. })),
                "{name}: {:?}",
                opened.err()
            );
            assert_eq!(
                branch.overlay().revision().sequence,
                0,
                "{name}: nothing written"
            );
        }
        Ok(())
    }

    /// A channel's log holds each tick until every reader has passed it:
    /// a reader that was not reading reads every tick once, oldest first,
    /// a reader built late reads every tick the log holds, and a tick
    /// both have passed is dropped. With no reader, the latest tick alone
    /// is kept.
    #[dialog_common::test]
    async fn it_keeps_a_channels_ticks_for_every_reader() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let (_, host) = declare_host(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let channel = stack.channel_of(&host).expect("the channel");
        let node: Entity = "node:1".parse()?;
        let send = |index: usize| {
            stack
                .assert(fetch(&format!("cmd:{index}"), &node).at(host.clone()))
                .perform(&operator)
        };
        send(0).await?;
        send(1).await?;
        assert_eq!(channel.held(), 1, "no reader: the latest tick alone");

        let mut behind = stack.arrivals(&host, fetch_query()).expect("the channel");
        for index in 2..5 {
            send(index).await?;
        }
        let mut late = stack.arrivals(&host, fetch_query()).expect("the channel");
        assert_eq!(channel.held(), 4);
        let every = vec![
            "cmd:1".to_string(),
            "cmd:2".to_string(),
            "cmd:3".to_string(),
            "cmd:4".to_string(),
        ];
        assert_eq!(subjects(&late.poll(&operator).await?), every);
        assert!(late.poll(&operator).await?.is_empty(), "once");
        assert_eq!(channel.held(), 4, "the reader behind holds them");
        assert_eq!(subjects(&behind.poll(&operator).await?), every);
        assert!(behind.poll(&operator).await?.is_empty(), "once");
        assert_eq!(channel.held(), 0, "a tick both passed is dropped");
        Ok(())
    }

    /// [`declare_state`]'s counter, with its command premise read from
    /// `control:` alone (`in: control:`).
    async fn declare_state_on_control(branch: &Branch, env: &Operator) -> Result<Entity> {
        declare_counter_on(branch, env, &control_layer()).await
    }

    /// [`declare_state`]'s counter, with its command premise read from
    /// the layer `at` alone (`in: <at>`).
    async fn declare_counter_on(branch: &Branch, env: &Operator, at: &Entity) -> Result<Entity> {
        let state = layer("state");
        let increment: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "count": {
                        "the": "counter/count", "as": "UnsignedInteger", "pick": "last",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "counter": {
                                "the": "cmd.increment/counter", "as": "Entity",
                                "in": at.to_string()
                            }
                        }
                    },
                    "where": { "counter": { "?": { "name": "this" } } }
                },
                {
                    "assert": {
                        "with": {
                            "count": {
                                "the": "counter/count", "as": "UnsignedInteger", "pick": "last",
                                "in": "session:state"
                            }
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
        branch
            .transaction()
            .assert(LayerDeclaration::new(state.clone(), LayerType::Session))
            .assert(increment)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    /// `cmd.increment/counter` of `command`, naming `counter`.
    fn increment(command: &str, counter: &Entity) -> impl Statement + use<> {
        dialog_query::the!("cmd.increment/counter")
            .of(command.parse::<Entity>().expect("entity"))
            .is(counter.clone())
    }

    /// A rule premised on `in: control:` fires on the commands of its
    /// tick and not on the next tick's: a dispatched increment steps the
    /// count once, though the step lands as the next tick's stimulus and
    /// the rule reads it there. A write `.at(control_layer())` is a
    /// command too, and the same attribute written into the primary is
    /// not one: it steps nothing.
    #[dialog_common::test]
    async fn it_fires_a_rule_premised_on_control_on_its_ticks_commands_alone() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state_on_control(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        stack
            .assert(count(&counter, 1, &state))
            .perform(&operator)
            .await?;

        stack
            .invoke(increment("cmd:1", &counter))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "one step, in the tick the command was stimulus of"
        );

        stack
            .assert(increment("cmd:2", &counter))
            .perform(&operator)
            .await?;
        assert_eq!(
            committed::<Entity>(
                &branch,
                &operator,
                "cmd.increment/counter",
                &"cmd:2".parse()?
            )
            .await?,
            vec![format!("Entity({counter})")],
            "written into the primary"
        );
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "a row of the primary is no command"
        );

        stack
            .assert(increment("cmd:3", &counter).at(crate::control_layer()))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(3)".to_string()],
            "a write into control: is a command"
        );
        Ok(())
    }

    /// Declare a memory layer `state` and three rules over a
    /// `cmd.ping/target` command: one records, in `state`, the layer the
    /// ping is read from (`in` bound to a variable); one answers it with a
    /// `cmd.pong/target` whose descriptor names `in: control:`; one, premised
    /// on that pong `in: control:`, records it in `state`.
    async fn declare_ping(branch: &Branch, env: &Operator) -> Result<Entity> {
        let state = layer("state");
        let seen: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "seen": { "the": "log/seen", "as": "Entity", "in": "session:state" } }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": {
                                "the": "cmd.ping/target", "as": "Entity",
                                "in": { "?": { "name": "at" } }
                            }
                        }
                    },
                    "where": {
                        "target": { "?": { "name": "this" } },
                        "at": { "?": { "name": "seen" } }
                    }
                }
            ]
        }))?;
        let pong: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "pong": { "the": "cmd.pong/target", "as": "Entity", "in": "control:" } }
            },
            "when": [
                {
                    "assert": {
                        "with": { "target": { "the": "cmd.ping/target", "as": "Entity" } }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "target": { "?": { "name": "pong" } }
                    }
                }
            ]
        }))?;
        let ponged: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "ponged": { "the": "log/ponged", "as": "Boolean", "in": "session:state" }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "target": { "the": "cmd.pong/target", "as": "Entity", "in": "control:" }
                        }
                    },
                    "where": { "target": { "?": { "name": "this" } } }
                },
                { "assert": "==", "where": { "this": { "?": { "name": "ponged" } }, "is": true } }
            ]
        }))?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(state.clone(), LayerType::Session))
            .assert(seen)
            .assert(pong)
            .assert(ponged)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    /// `cmd.ping/target` of `command`, naming `target`.
    fn ping(command: &str, target: &Entity) -> impl Statement + use<> {
        dialog_query::the!("cmd.ping/target")
            .of(command.parse::<Entity>().expect("entity"))
            .is(target.clone())
    }

    /// `the` of every entity, its value an entity, read from wherever
    /// `in` binds.
    fn anywhere(the: &str) -> AttributeQuery {
        AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                the.parse::<dialog_corpus::Relation>().unwrap(),
            ))
            .of(Term::<Entity>::var("e"))
            .is(Term::<Entity>::var("v")),
        )
        .with_layer(Term::var("where"))
    }

    /// A read binding `in` sees a command stamped `control:` during the
    /// tick that dispatched it, a transaction's read as a rule's; and a
    /// rule head whose descriptor names `in: control:` is a dispatch: the
    /// next tick's stimulus, read as a row of `control:`, and in no layer
    /// once its tick is over.
    #[dialog_common::test]
    async fn it_reads_a_command_as_a_row_of_control_during_its_tick() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_ping(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let control = crate::control_layer();
        let target: Entity = "doc:1".parse()?;

        let transaction = stack.transaction().dispatch(ping("cmd:1", &target));
        let claims: Vec<dialog_query::Claim> = transaction
            .query()
            .select(anywhere("cmd.ping/target"))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].layer(), Some(&control), "a transaction reads it");

        let before = branch.revision();
        let mut pongs = stack
            .arrivals(&control, anywhere("cmd.pong/target"))
            .expect("every stack has control:");
        stack
            .invoke(ping("cmd:1", &target))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "log/seen", &target)?,
            vec![format!("Entity({control})")],
            "the rule bound the ping's layer to control:"
        );
        assert_eq!(
            held(&memory, "log/ponged", &target)?,
            vec!["Boolean(true)".to_string()],
            "the pong was the next tick's stimulus, read from control:"
        );
        assert_eq!(
            subjects(&pongs.poll(&operator).await?),
            vec!["cmd:1".to_string()],
            "the pong is a command of the dispatch"
        );
        for the in ["cmd.ping/target", "cmd.pong/target"] {
            let rows: Vec<dialog_query::Claim> = stack
                .query()
                .select(anywhere(the))
                .perform(&operator)
                .try_vec()
                .await?;
            assert!(rows.is_empty(), "{the} is in no layer after its tick");
        }
        assert_eq!(branch.revision(), before, "nothing durable was written");
        Ok(())
    }

    /// A write `.at(control_layer())` of an attribute no concept marks as
    /// a command is a dispatch all the same: a provider reads it once its
    /// dispatch has published, and it leaves nothing in any layer.
    #[dialog_common::test]
    async fn it_dispatches_a_write_into_control_and_keeps_it_nowhere() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let mut arrivals = stack
            .arrivals(&control, selected_query())
            .expect("every stack has control:");
        let before = branch.revision();
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(flag(&doc, true, &control))
            .perform(&operator)
            .await?;

        assert_eq!(
            selected(&arrivals.poll(&operator).await?),
            vec![format!("{doc} Boolean(true)")],
            "dispatched"
        );
        assert!(
            committed::<bool>(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty()
        );
        assert!(held(branch.overlay(), "ui/selected", &doc)?.is_empty());
        assert!(
            held(
                ephemeral(&stack, &state).expect("state"),
                "ui/selected",
                &doc
            )?
            .is_empty()
        );
        assert!(!waiting(&stack, &control));
        assert!(
            fresh(
                &stack,
                &operator,
                selected_query().with_layer(Term::var("where"))
            )
            .await?
            .is_empty(),
            "a read of every layer finds it in none"
        );
        assert_eq!(branch.revision(), before, "nothing durable was written");
        Ok(())
    }

    /// The durable layer `memory:notes`, declared in `branch`'s tree.
    async fn declare_notes(branch: &Branch, env: &Operator) -> Result<Entity> {
        let notes: Entity = "memory:notes".parse()?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(notes.clone(), LayerType::Branch))
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(notes)
    }

    /// `doc/note` of `doc` as `text`, written into `at`.
    fn note(doc: &Entity, text: &str, at: &Entity) -> impl Statement {
        dialog_query::the!("doc/note")
            .of(doc.clone())
            .is(text.to_string())
            .at(at.clone())
    }

    /// `doc/headline` of `doc` as `text`, under `last`, written into
    /// `at`: a cell that holds one value.
    fn headline(doc: &Entity, text: &str, at: &Entity) -> impl Statement {
        AttributeStatement {
            the: QueryRelation::from("doc/headline".parse::<dialog_corpus::Relation>().unwrap()),
            of: doc.clone(),
            is: dialog_corpus::Value::String(text.to_string()),
            cause: None,
            cardinality: Some(Cardinality::One),
            pick: Some(dialog_corpus::Pick::Last),
        }
        .at(at.clone())
    }

    /// The `doc/headline` of `doc` a read through `stack` elects in
    /// `layer`, the one a read of the cell under `last` returns.
    async fn headline_read(
        stack: &Stack,
        env: &Operator,
        doc: &Entity,
        layer: &Entity,
    ) -> Result<Vec<String>> {
        let query = AttributeQuery::new(
            Term::from(QueryRelation::from(
                "doc/headline".parse::<dialog_corpus::Relation>()?,
            )),
            Term::from(doc.clone()),
            Term::var("v"),
            Term::blank(),
            Some(Cardinality::One),
        )
        .with_layer(Term::Constant(dialog_corpus::Value::Entity(layer.clone())));
        let claims: Vec<dialog_query::Claim> =
            stack.query().select(query).perform(env).try_vec().await?;
        Ok(claims
            .iter()
            .map(|claim| format!("{:?}", claim.is))
            .collect())
    }

    /// The claims of `the` on `of` that `branch`'s committed tree holds,
    /// every claim of the cell, read past the session store: what the
    /// branch itself stores.
    async fn stored(
        branch: &Branch,
        env: &Operator,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<String>> {
        use futures_util::TryStreamExt as _;
        let selector = Selector::new()
            .the(the.parse::<dialog_corpus::Relation>()?)
            .of(of.clone());
        let source = Source::from(branch.clone());
        let views: Vec<dialog_corpus::FactView> = crate::select_from_source(&source, env, selector)
            .await?
            .try_collect()
            .await?;
        let mut rows = Vec::with_capacity(views.len());
        for view in views {
            rows.push(format!("{:?}", view.to_owned()?.is));
        }
        rows.sort();
        Ok(rows)
    }

    /// Every claim of the pointer facts `branch`'s committed tree holds of
    /// `layer`: its `dialog.branch/revision`, then its
    /// `dialog.branch/tree`, as [`stored`] renders them.
    async fn pointers(branch: &Branch, env: &Operator, layer: &Entity) -> Result<Vec<String>> {
        let mut claims = stored(branch, env, "dialog.branch/revision", layer).await?;
        claims.extend(stored(branch, env, "dialog.branch/tree", layer).await?);
        Ok(claims)
    }

    /// A revision a durable layer is pointed at, by what the pointer
    /// names of it: its entity and its tree.
    fn named(revision: Option<crate::Revision>) -> Option<(Entity, crate::TreeReference)> {
        revision.map(|revision| (revision.entity(), revision.tree))
    }

    /// The pointer facts naming `revision`, as [`pointers`] renders them.
    fn pointer_to(revision: &crate::Revision) -> Vec<String> {
        vec![
            format!("{:?}", revision_entity(revision)),
            format!("{:?}", tree_value(&revision.tree)),
        ]
    }

    /// The `doc/note` values `query` reads through `stack`, each with
    /// the layer its `in` binds, sorted. A row with no layer stamped is
    /// the primary's.
    async fn notes_read(
        stack: &Stack,
        env: &Operator,
        query: AttributeQuery,
    ) -> Result<Vec<String>> {
        let claims: Vec<dialog_query::Claim> =
            stack.query().select(query).perform(env).try_vec().await?;
        let mut rows: Vec<String> = claims
            .iter()
            .map(|claim| {
                format!(
                    "{:?} in {}",
                    claim.is,
                    claim
                        .layer()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| crate::MEMORY.to_string())
                )
            })
            .collect();
        rows.sort();
        Ok(rows)
    }

    /// `doc/note` of every entity, with the value a variable.
    fn note_query() -> AttributeQuery {
        AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "doc/note".parse::<dialog_corpus::Relation>().unwrap(),
            ))
            .of(Term::<Entity>::var("e"))
            .is(Term::<String>::var("v")),
        )
    }

    /// `doc/note` of every entity in `layer` alone.
    fn notes_in(layer: &Entity) -> AttributeQuery {
        note_query().with_layer(Term::Constant(dialog_corpus::Value::Entity(layer.clone())))
    }

    /// A schema declaring `memory:notes` durable opens a stack holding
    /// it with no memory cell and no branch of its own, empty while the
    /// primary points it nowhere. A write `in: memory:notes` lands in a
    /// tree the primary points at and nowhere in the primary's tree: the
    /// primary gains one `dialog.branch/revision` and one
    /// `dialog.branch/tree` of the layer's entity, naming the revision
    /// and the tree the write left the layer at. The next write builds on
    /// that tree, and its pointer succeeds the first. No branch is
    /// written for the layer.
    #[dialog_common::test]
    async fn it_writes_a_durable_layer_into_the_tree_the_primary_points_at() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        assert_eq!(stack.layers(), implied_and([notes.clone()]));
        assert!(
            matches!(
                stack.layer(&notes),
                Some(Layer::Durable(durable)) if durable.revision().is_none()
            ),
            "no memory cell holds it"
        );
        assert_eq!(stack.branch_of(&notes), None, "and no branch is it");
        assert_eq!(stack.pointed(&notes), None, "the primary points it nowhere");
        assert!(pointers(&branch, &operator, &notes).await?.is_empty());

        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "first", &notes))
            .perform(&operator)
            .await?;
        let first = stack.pointed(&notes).expect("the layer moved");
        assert_eq!(
            pointers(&branch, &operator, &notes).await?,
            pointer_to(&first),
            "the primary points at the layer's new tree, one claim of each"
        );
        assert_eq!(
            notes_read(&stack, &operator, notes_in(&notes)).await?,
            vec!["String(\"first\") in memory:notes".to_string()],
            "the write is in the pointed tree"
        );
        assert!(
            stored(&branch, &operator, "doc/note", &doc)
                .await?
                .is_empty(),
            "the primary's tree holds none of it"
        );
        assert!(held(branch.overlay(), "doc/note", &doc)?.is_empty());

        stack
            .assert(note(&doc, "second", &notes))
            .perform(&operator)
            .await?;
        let second = stack.pointed(&notes).expect("the layer moved");
        assert_eq!(
            second.edition,
            first.edition.successor(),
            "the second tree is built on the first"
        );
        assert_eq!(
            pointers(&branch, &operator, &notes).await?,
            pointer_to(&second),
            "the new pointer succeeds the old one"
        );
        let reopened = branch.stack().perform(&operator).await?;
        assert_eq!(
            notes_read(&reopened, &operator, notes_in(&notes)).await?,
            vec![
                "String(\"first\") in memory:notes".to_string(),
                "String(\"second\") in memory:notes".to_string(),
            ],
            "a stack opened afresh reads the layer where the primary points it"
        );
        let nowhere = repo.branch("notes").open().perform(&operator).await?;
        assert_eq!(nowhere.revision(), None, "no branch was written for it");
        Ok(())
    }

    /// A read with `in` a variable binds a row of the durable layer to
    /// `memory:notes`, and the primary's row to `memory:`; one with `in`
    /// bound to `memory:notes` reads the layer alone.
    #[dialog_common::test]
    async fn it_reads_a_durable_layer_as_a_line_named_for_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "kept", &notes))
            .assert(note(&doc, "primary", &crate::memory_layer()))
            .perform(&operator)
            .await?;
        assert_eq!(
            notes_read(
                &stack,
                &operator,
                note_query().with_layer(Term::var("where"))
            )
            .await?,
            vec![
                "String(\"kept\") in memory:notes".to_string(),
                "String(\"primary\") in memory:".to_string(),
            ],
            "`in` unbound reads every layer and binds each row's"
        );
        assert_eq!(
            notes_read(&stack, &operator, notes_in(&notes)).await?,
            vec!["String(\"kept\") in memory:notes".to_string()],
            "`in` bound to the durable layer reads it alone"
        );
        assert_eq!(
            notes_read(&stack, &operator, notes_in(&crate::memory_layer())).await?,
            vec!["String(\"primary\") in memory:".to_string()],
            "`in` bound to the primary reads it alone"
        );
        Ok(())
    }

    /// A dispatch writing into the durable layer, whose primary another
    /// handle moved under it, loses the primary's compare-and-swap and
    /// re-runs from the new head, reading the layer where the new head
    /// points it: it lands once, one commit on the primary after the
    /// other writer's, and the layer's tree holds both its writes, the
    /// pointer naming the tree the re-run minted.
    #[dialog_common::test]
    async fn it_re_runs_a_dispatch_whose_primary_moved_under_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "first", &notes))
            .perform(&operator)
            .await?;
        let first = stack.pointed(&notes).expect("the layer moved");

        // Another handle of the primary moves its head; the stack's
        // handle has not seen it.
        let other = repo.branch("main").open().perform(&operator).await?;
        let moved = other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:2".parse::<Entity>()?)
                    .is("other".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_ne!(branch.revision().as_ref(), Some(&moved));

        let published = stack
            .assert(note(&doc, "second", &notes))
            .perform(&operator)
            .await?;
        assert_eq!(
            published.revision().version().edition,
            moved.version().edition.successor(),
            "the primary took one commit, after the other writer's"
        );
        let second = stack.pointed(&notes).expect("the layer moved");
        assert_eq!(
            second.edition,
            first.edition.successor(),
            "the layer's tree was minted once, on the one the new head points at"
        );
        assert_eq!(
            pointers(&branch, &operator, &notes).await?,
            pointer_to(&second),
            "the primary points at it alone"
        );
        assert_eq!(
            notes_read(&stack, &operator, notes_in(&notes)).await?,
            vec![
                "String(\"first\") in memory:notes".to_string(),
                "String(\"second\") in memory:notes".to_string(),
            ]
        );
        Ok(())
    }

    /// A transaction through the stack writing into the durable layer,
    /// whose primary another handle moved before its publish, loses the
    /// compare-and-swap with `VersionMismatch`: the primary's pointer
    /// facts stay the one claim of each they were, naming the tree the
    /// layer stood at, and the layer reads as it did. The tree the lost
    /// commit minted is named by nothing.
    #[dialog_common::test]
    async fn it_leaves_a_durable_layer_where_it_was_when_the_head_is_lost() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "kept", &notes))
            .perform(&operator)
            .await?;
        let kept = stack.pointed(&notes).expect("the layer moved");

        let staged = stack
            .transaction()
            .assert(note(&doc, "lost", &notes))
            .commit()
            .perform(&operator)
            .await?;
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:2".parse::<Entity>()?)
                    .is("other".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let lost = staged.publish().perform(&operator).await;
        assert!(
            matches!(
                lost,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "{lost:?}"
        );
        other.refresh(&operator).await?;
        assert_eq!(
            pointers(&other, &operator, &notes).await?,
            pointer_to(&kept),
            "the primary still points at the tree the layer stood at, once"
        );
        assert_eq!(named(stack.pointed(&notes)), named(Some(kept)));
        branch.refresh(&operator).await?;
        let reopened = branch.stack().perform(&operator).await?;
        assert_eq!(
            notes_read(&reopened, &operator, notes_in(&notes)).await?,
            vec!["String(\"kept\") in memory:notes".to_string()],
            "the lost write is nowhere"
        );
        Ok(())
    }

    /// A write `in: memory:missing`, a layer declared nowhere, is refused
    /// with `UnboundLayer` before anything is committed: the primary does
    /// not move, its pointer of the durable layer stays as it was, and it
    /// gains none of the missing layer.
    #[dialog_common::test]
    async fn it_refuses_a_write_into_a_durable_layer_declared_nowhere() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "kept", &notes))
            .perform(&operator)
            .await?;
        let primary = branch.revision();
        let pointed = pointers(&branch, &operator, &notes).await?;
        let missing: Entity = "memory:missing".parse()?;

        let refused = stack
            .assert(note(&doc, "lost", &notes))
            .assert(note(&doc, "lost", &missing))
            .perform(&operator)
            .await;
        assert!(
            matches!(
                &refused,
                Err(CommitError::UnboundLayer { layer, .. }) if *layer == missing.to_string()
            ),
            "{refused:?}"
        );
        assert_eq!(branch.revision(), primary, "the primary did not move");
        assert_eq!(
            pointers(&branch, &operator, &notes).await?,
            pointed,
            "the layer did not move"
        );
        assert!(pointers(&branch, &operator, &missing).await?.is_empty());
        assert_eq!(
            notes_read(&stack, &operator, notes_in(&notes)).await?,
            vec!["String(\"kept\") in memory:notes".to_string()]
        );
        Ok(())
    }

    /// A layer declared durable under a name that is not `memory:<name>`
    /// is refused with `InvalidPlacement` when the stack opens, and the
    /// refused open writes nothing: no row in `session:`, and the
    /// primary does not move.
    #[dialog_common::test]
    async fn it_refuses_a_durable_layer_named_outside_memory() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let head = branch.revision();
        let refused = branch
            .stack()
            .declare(LayerDeclaration::new(
                "session:notes".parse()?,
                LayerType::Branch,
            ))
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(CommitError::InvalidPlacement { .. })),
            "the open is refused"
        );
        assert_eq!(
            branch.overlay().revision().sequence,
            0,
            "nothing was written into session:"
        );
        branch.refresh(&operator).await?;
        assert_eq!(branch.revision(), head, "the primary did not move");
        Ok(())
    }

    /// `stack.sync()` pushes the primary to the upstream it pushes to,
    /// carrying the tree it points the durable layer at, and a second
    /// replica tracking that upstream reads the layer once it pulls: the
    /// pointer came with the primary, and the tree with it. A durable
    /// layer has no upstream of its own. A sync after which nothing moved
    /// writes nothing.
    #[dialog_common::test]
    async fn it_syncs_the_tree_a_primary_points_a_durable_layer_at() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let upstream = repo.branch("upstream").open().perform(&operator).await?;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.set_upstream(&upstream).perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        branch.push().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "kept", &notes))
            .perform(&operator)
            .await?;
        stack
            .sync(crate::Download::QueriedRanges)
            .perform(&operator)
            .await?;
        upstream.refresh(&operator).await?;
        assert_eq!(
            upstream.revision(),
            branch.revision(),
            "the primary was pushed"
        );
        let pointed = branch.revision();
        stack
            .sync(crate::Download::QueriedRanges)
            .perform(&operator)
            .await?;
        assert_eq!(
            branch.revision(),
            pointed,
            "nothing moved, nothing was written"
        );

        let replica = repo.branch("replica").open().perform(&operator).await?;
        replica.set_upstream(&upstream).perform(&operator).await?;
        let mirrored = replica
            .stack()
            .declare(LayerDeclaration::new(notes.clone(), LayerType::Branch))
            .perform(&operator)
            .await?;
        mirrored
            .sync(crate::Download::QueriedRanges)
            .perform(&operator)
            .await?;
        assert_eq!(replica.revision(), branch.revision(), "the replica pulled");
        assert_eq!(
            named(mirrored.pointed(&notes)),
            named(stack.pointed(&notes)),
            "the replica's primary points the layer where the first's does"
        );
        assert_eq!(
            notes_read(&mirrored, &operator, notes_in(&notes)).await?,
            vec!["String(\"kept\") in memory:notes".to_string()],
            "the second replica reads the layer"
        );
        Ok(())
    }

    /// Two replicas each write the durable layer, different cells and the
    /// same single-valued cell with different values, then each syncs
    /// through one upstream, the first and then the second, and the first
    /// again. The second's pull merges two primaries that each moved the
    /// layer: its changes are replayed onto the first's tree rather than
    /// `last` picking one tree. Both then read the union, with the
    /// second-synced replica's value in the shared cell, and each primary
    /// points at one tree, the same one, which both load.
    #[dialog_common::test]
    async fn it_merges_a_durable_layer_two_replicas_moved() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let upstream = repo.branch("upstream").open().perform(&operator).await?;
        let first = repo.branch("main").open().perform(&operator).await?;
        first.set_upstream(&upstream).perform(&operator).await?;
        let notes = declare_notes(&first, &operator).await?;
        first.push().perform(&operator).await?;
        let second = repo.branch("replica").open().perform(&operator).await?;
        second.set_upstream(&upstream).perform(&operator).await?;
        second
            .pull(crate::Download::QueriedRanges)
            .perform(&operator)
            .await?;
        let a = first.stack().perform(&operator).await?;
        let b = second.stack().perform(&operator).await?;
        assert_eq!(b.layers(), a.layers(), "the declaration replicated");

        let shared: Entity = "doc:shared".parse()?;
        a.assert(note(&"doc:a".parse()?, "from a", &notes))
            .assert(headline(&shared, "a", &notes))
            .perform(&operator)
            .await?;
        b.assert(note(&"doc:b".parse()?, "from b", &notes))
            .assert(headline(&shared, "b", &notes))
            .perform(&operator)
            .await?;
        for stack in [&a, &b, &a] {
            stack
                .sync(crate::Download::QueriedRanges)
                .perform(&operator)
                .await?;
        }

        for (stack, replica) in [(&a, "first"), (&b, "second")] {
            assert_eq!(
                notes_read(stack, &operator, notes_in(&notes)).await?,
                vec![
                    "String(\"from a\") in memory:notes".to_string(),
                    "String(\"from b\") in memory:notes".to_string(),
                ],
                "the {replica} replica reads both replicas' notes"
            );
            assert_eq!(
                headline_read(stack, &operator, &shared, &notes).await?,
                vec!["String(\"b\")".to_string()],
                "the {replica} replica reads the second-synced value"
            );
        }
        let merged = b.pointed(&notes).expect("the layer is pointed at");
        assert_eq!(named(a.pointed(&notes)), named(Some(merged.clone())));
        for primary in [&first, &second] {
            assert_eq!(
                pointers(primary, &operator, &notes).await?,
                pointer_to(&merged),
                "{}'s primary points at the merged tree alone",
                primary.name()
            );
        }
        Ok(())
    }

    /// A sync whose pull moved the primary reads the primary's new
    /// revision in `session:`, as a commit would, and a rule premised on
    /// the primary's `dialog.branch/revision` there fires on it once,
    /// with that revision's entity. A second sync, which pulls nothing, records nothing:
    /// `session:` mints no instant, and the rule does not fire again.
    /// Replica A is a second branch of the repository tracking the same
    /// upstream as the stack's primary.
    #[dialog_common::test]
    async fn it_records_the_revisions_a_pull_moved() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        // The primary's `dialog.branch/revision` is one row of
        // `session:`, the branch metadata's: the rule needs no join.
        let watch: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "revision": {
                        "the": "seen/revision", "as": "Entity", "pick": "last",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "revision": {
                                "the": "dialog.branch/revision", "as": "Entity",
                                "in": "session:"
                            }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "revision": { "?": { "name": "revision" } }
                    }
                }
            ]
        }))?;
        let upstream = repo.branch("upstream").open().perform(&operator).await?;
        upstream
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:0".parse::<Entity>()?)
                    .is("Upstream".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.set_upstream(&upstream).perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let own = ephemeral(&stack, &session_layer())
            .expect("its own layer")
            .clone();
        let primary = stack
            .branch_of(&crate::memory_layer())
            .expect("the primary's branch")
            .clone();
        // Installed through the stack: the metadata row is read on a
        // plain branch too, and installing evaluates the rule once.
        stack.assert(watch).perform(&operator).await?;
        stack
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        upstream.refresh(&operator).await?;
        assert_eq!(upstream.revision(), branch.revision(), "B pushed");

        let replica = repo.branch("replica-a").open().perform(&operator).await?;
        replica.set_upstream(&upstream).perform(&operator).await?;
        replica
            .pull(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        replica
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("From A".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        replica.push().perform(&operator).await?;

        let fired = memory.revision().sequence;
        stack
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        let head = branch.revision().expect("B has a head");
        assert_eq!(
            Some(&head),
            replica.revision().as_ref(),
            "B pulled A's commit"
        );
        assert_eq!(
            primary_revision_of(&stack, &operator).await?,
            vec![entity_text(&head)],
            "session: reads the primary's revision as the pull left it, one value"
        );
        assert_eq!(
            held(&memory, "seen/revision", &primary)?,
            vec![entity_text(&head)],
            "the rule fired on the pulled revision"
        );
        assert_eq!(
            memory.revision().sequence,
            fired + 1,
            "the rule fired once: one instant in its layer"
        );

        let (own_sequence, memory_sequence) = (own.revision().sequence, memory.revision().sequence);
        stack
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        assert_eq!(branch.revision(), Some(head), "nothing was pulled");
        assert_eq!(
            own.revision().sequence,
            own_sequence,
            "a sync that moved nothing recorded nothing"
        );
        assert_eq!(
            memory.revision().sequence,
            memory_sequence,
            "and the rule did not fire again"
        );
        Ok(())
    }

    /// A subscription through a stack reads its durable layers too: a
    /// write into one is a delta on the next poll, read afresh since the
    /// layer's tree moved, and the subscription then agrees with a fresh
    /// read.
    #[dialog_common::test]
    async fn it_subscribes_to_a_durable_layer_through_a_stack() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let every = || note_query().with_layer(Term::var("where"));
        let mut subscription = stack.subscribe(every());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty());

        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "kept", &notes))
            .perform(&operator)
            .await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the layer moved");
        let asserted: Vec<String> = delta
            .asserted
            .iter()
            .map(|claim| {
                format!(
                    "{:?} in {:?}",
                    claim.is,
                    claim.layer().map(ToString::to_string)
                )
            })
            .collect();
        assert_eq!(
            asserted,
            vec!["String(\"kept\") in Some(\"memory:notes\")".to_string()]
        );
        assert!(delta.retracted.is_empty());
        assert!(subscription.poll(&operator).await?.is_none());
        assert_eq!(
            notes_read(&stack, &operator, every()).await?,
            vec!["String(\"kept\") in memory:notes".to_string()]
        );
        Ok(())
    }

    /// Two replicas of a space sync a stack through one S3 upstream:
    /// the first writes into its primary and its durable layer and
    /// syncs, which uploads the tree the primary points the layer at
    /// before the primary's head; the second, whose primary tracks the
    /// same remote branch and whose stack declares the layer, syncs with
    /// `Download::Everything` and reads the same rows with the same `in`.
    /// The pull brought the layer's tree local before the primary moved:
    /// every block of it is in the second replica's own store, read with
    /// no remote to fall back on.
    #[cfg(any(feature = "integration-tests", feature = "web-integration-tests"))]
    #[dialog_common::test]
    async fn it_syncs_a_durable_layer_to_a_second_replica(s3: S3Address) -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::connect;
        use dialog_peer::helpers::unique_name;
        use dialog_remote_s3::{Address as S3SiteAddress, S3Credential};
        use futures_util::StreamExt as _;

        let (operator, profile) = test_session_with_peer().await;
        let site = crate::SiteAddress::from(
            S3SiteAddress::builder(&s3.endpoint)
                .region("us-east-1")
                .bucket(&s3.bucket)
                .build()?,
        );
        profile
            .secrets()
            .site(&site)
            .save(S3Credential::new(&s3.access_key_id, &s3.secret_access_key))
            .perform(&profile)
            .await?;

        let first = profile
            .space(unique_name("first"))
            .create()
            .perform(&operator)
            .await?;
        let origin = connect("origin", site.clone(), first.did(), &operator).await?;
        let main = first.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&main, &operator).await?;
        let stack = main.stack().perform(&operator).await?;
        main.set_upstream(origin.branch("main").open().perform(&operator).await?)
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(note(&doc, "kept", &notes))
            .assert(note(&doc, "primary", &crate::memory_layer()))
            .perform(&operator)
            .await?;
        // Enough rows that the layer's tree is more than the one leaf a
        // read of its revision record reaches.
        let mut bulk = Changes::new();
        for at in 0..200 {
            note(
                &format!("doc:bulk-{at}").parse()?,
                &format!("bulk {at} ").repeat(16),
                &notes,
            )
            .assert(&mut bulk);
        }
        stack.assert(bulk).perform(&operator).await?;
        stack
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;

        let second = profile
            .space(unique_name("second"))
            .open()
            .perform(&operator)
            .await?;
        let origin = connect("origin", site, first.did(), &operator).await?;
        let replica = second.branch("main").open().perform(&operator).await?;
        replica
            .set_upstream(origin.branch("main").open().perform(&operator).await?)
            .perform(&operator)
            .await?;
        let mirrored = replica
            .stack()
            .declare(LayerDeclaration::new(notes.clone(), LayerType::Branch))
            .perform(&operator)
            .await?;
        mirrored
            .sync(crate::Download::Everything)
            .perform(&operator)
            .await?;

        let pointed = mirrored
            .pointed(&notes)
            .expect("the pull pointed the layer");
        assert_eq!(named(Some(pointed.clone())), named(stack.pointed(&notes)));
        // Every block of the pointed tree is in the second replica's own
        // store: an export that must find each one locally, and fails on
        // the first it lacks, reads the whole tree.
        let local = crate::Snapshot::new(replica.subject(), pointed).export();
        let items = local.perform(&operator);
        futures_util::pin_mut!(items);
        let mut blocks = 0;
        while let Some(item) = items.next().await {
            item?;
            blocks += 1;
        }
        assert!(blocks > 0, "the layer's tree was read from the local store");

        let every = note_query().with_layer(Term::var("where"));
        assert_eq!(
            notes_read(&mirrored, &operator, every.clone()).await?,
            notes_read(&stack, &operator, every).await?,
            "the second replica reads the same rows with the same `in`"
        );
        let doc_notes = AttributeQuery::from(
            Term::<QueryRelation>::from(QueryRelation::from(
                "doc/note".parse::<dialog_corpus::Relation>()?,
            ))
            .of(Term::<Entity>::from(doc.clone()))
            .is(Term::<String>::var("v")),
        )
        .with_layer(Term::var("where"));
        assert_eq!(
            notes_read(&mirrored, &operator, doc_notes).await?,
            vec![
                "String(\"kept\") in memory:notes".to_string(),
                "String(\"primary\") in memory:".to_string(),
            ]
        );
        Ok(())
    }

    /// `cmd.fetch/priority` of the fetch `command`: a second attribute of
    /// the command's entity, so a command reads as more than one concept.
    fn priority(command: &str, level: u32) -> impl Statement + use<> {
        dialog_query::the!("cmd.fetch/priority")
            .of(command.parse::<Entity>().expect("entity"))
            .is(level)
    }

    /// `doc/title` of `doc` as `text`, in the primary.
    fn title(doc: &Entity, text: &str) -> impl Statement + use<> {
        dialog_query::the!("doc/title")
            .of(doc.clone())
            .is(text.to_string())
    }

    /// Every row of `layer`, with every term a variable but `in`.
    fn whole(layer: &Entity) -> AttributeQuery {
        AttributeQuery::new(
            Term::var("the"),
            Term::var("of"),
            Term::var("is"),
            Term::blank(),
            None,
        )
        .with_layer(Term::from(layer.clone()))
    }

    /// The rows of `claims` as `of the`, sorted.
    fn cells(claims: &[dialog_query::Claim]) -> Vec<String> {
        let mut cells: Vec<String> = claims
            .iter()
            .map(|claim| format!("{} {}", claim.of, claim.the))
            .collect();
        cells.sort();
        cells
    }

    /// A transaction the stack hands out, committed and published
    /// through the transaction API rather than invoked, lands the
    /// commands it dispatched in `control:` once its publish has won: a
    /// write `.at(control_layer())` published in one step, and a
    /// `dispatch` of a staged chain published after. Each is read once,
    /// as a row of `control:`, nothing stays there, and the durable
    /// write beside the first is in the tree.
    #[dialog_common::test]
    async fn it_lands_the_commands_of_a_transaction_published_through_the_stack() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let mut fetches = stack
            .arrivals(&control, fetch_query())
            .expect("every stack has control:");
        let node: Entity = "node:1".parse()?;
        let doc: Entity = "doc:1".parse()?;

        stack
            .transaction()
            .assert(fetch("cmd:1", &node).at(control.clone()))
            .assert(title(&doc, "kept"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let commands = fetches.poll(&operator).await?;
        assert_eq!(subjects(&commands), vec!["cmd:1".to_string()]);
        assert_eq!(commands[0].layer(), Some(&control), "a row of control:");
        assert!(fetches.poll(&operator).await?.is_empty(), "read once");
        assert!(!waiting(&stack, &control), "nothing stays in control:");
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &doc).await?,
            vec!["String(\"kept\")".to_string()],
            "the durable write is in the tree"
        );

        let batch = stack
            .transaction()
            .dispatch(fetch("cmd:2", &node))
            .commit()
            .perform(&operator)
            .await?;
        assert!(
            fetches.poll(&operator).await?.is_empty(),
            "a staged chain lands nothing before it publishes"
        );
        batch.publish().perform(&operator).await?;
        assert_eq!(
            subjects(&fetches.poll(&operator).await?),
            vec!["cmd:2".to_string()]
        );
        assert!(fetches.poll(&operator).await?.is_empty(), "read once");
        Ok(())
    }

    /// A transaction through the stack whose head moved under it is
    /// refused at commit with `VersionMismatch`, and lands nothing: no
    /// command in `control:`, and no durable write in the tree.
    #[dialog_common::test]
    async fn it_lands_nothing_of_a_stack_transaction_that_lost_its_head() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let mut fetches = stack
            .arrivals(&control, fetch_query())
            .expect("every stack has control:");
        let node: Entity = "node:1".parse()?;
        let doc: Entity = "doc:1".parse()?;

        let transaction = stack
            .transaction()
            .assert(fetch("cmd:1", &node).at(control.clone()))
            .assert(title(&doc, "lost"));
        branch
            .transaction()
            .assert(title(&"doc:2".parse()?, "moved"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let refused = transaction.commit().publish().perform(&operator).await;
        assert!(
            matches!(
                refused,
                Err(CommitError::Publish(PublishError::VersionMismatch { .. }))
            ),
            "{refused:?}"
        );
        assert!(fetches.poll(&operator).await?.is_empty(), "nothing landed");
        assert!(!waiting(&stack, &control));
        assert!(
            committed::<String>(&branch, &operator, "doc/title", &doc)
                .await?
                .is_empty(),
            "nothing was written"
        );
        Ok(())
    }

    /// A scan with `in: control:` constant and every other term a
    /// variable reads the tick's commands and nothing else: within the
    /// transaction, every row of each command dispatched, and none of the
    /// durable write or the write into a memory layer beside them; once
    /// it has published, the same rows as they arrive in `control:`.
    #[dialog_common::test]
    async fn it_scans_the_whole_of_control() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let mut arrivals = stack
            .arrivals(&control, whole(&control))
            .expect("every stack has control:");
        let node: Entity = "node:1".parse()?;
        let doc: Entity = "doc:1".parse()?;
        let expected = vec![
            "cmd:1 cmd.fetch/node".to_string(),
            "cmd:1 cmd.fetch/priority".to_string(),
        ];

        let transaction = stack
            .transaction()
            .dispatch(fetch("cmd:1", &node))
            .assert(priority("cmd:1", 2).at(control.clone()))
            .assert(title(&doc, "kept"))
            .assert(flag(&doc, true, &state));
        let during: Vec<dialog_query::Claim> = transaction
            .query()
            .select(whole(&control))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(cells(&during), expected, "the tick's commands alone");
        assert!(during.iter().all(|claim| claim.layer() == Some(&control)));
        transaction.commit().publish().perform(&operator).await?;

        let arrived = arrivals.poll(&operator).await?;
        assert_eq!(cells(&arrived), expected, "what arrived in control:");
        assert!(arrivals.poll(&operator).await?.is_empty(), "read once");
        Ok(())
    }

    /// A reader of the rows arriving in `control:`, with no query, yields
    /// every fact each published dispatch's commands assert, each once,
    /// oldest first: a command of one attribute, then one of two, then
    /// nothing.
    #[dialog_common::test]
    async fn it_reads_the_rows_arriving_in_control() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let mut rows = stack
            .arrivals_rows(&control)
            .expect("every stack has control:");
        let node: Entity = "node:1".parse()?;
        let text = |facts: Vec<dialog_corpus::Fact>| -> Vec<String> {
            facts
                .into_iter()
                .map(|fact| format!("{} {} {:?}", fact.of, fact.the, fact.is))
                .collect()
        };
        assert!(rows.rows().is_empty(), "nothing dispatched yet");

        stack
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        assert_eq!(
            text(rows.rows()),
            vec!["cmd:1 cmd.fetch/node Entity(node:1)".to_string()]
        );

        stack
            .invoke(fetch("cmd:2", &node))
            .invoke(priority("cmd:2", 3))
            .perform(&operator)
            .await?;
        let mut second = text(rows.rows());
        second.sort();
        assert_eq!(
            second,
            vec![
                "cmd:2 cmd.fetch/node Entity(node:1)".to_string(),
                "cmd:2 cmd.fetch/priority UnsignedInt(3)".to_string(),
            ]
        );
        assert!(rows.rows().is_empty(), "each row once");
        Ok(())
    }

    /// An environment that provides what opening a stack and reading
    /// through it need, and nothing that commits: no `Publish`, `Attest`,
    /// `Import`, `BlobImport` or `BlobSize`. Each command it provides is
    /// performed on the peer it wraps.
    struct ReadOnly(Operator);

    macro_rules! reads {
        ($($command:ty),* $(,)?) => {
            $(
                impl Provider<$command> for ReadOnly {
                    fn execute(
                        &self,
                        input: <$command as dialog_capability::Command>::Input,
                    ) -> dialog_capability::Resume<'_, <$command as dialog_capability::Command>::Output>
                    {
                        <Operator as Provider<$command>>::execute(&self.0, input)
                    }
                }
            )*
        };
    }

    reads!(
        BlobRead,
        Get,
        Put,
        Resolve,
        Identify,
        ArchiveLoad,
        nodes::Read<CorpusNode>,
        nodes::Preload<CorpusNode>,
        nodes::Write<CorpusNode>,
        BlobLoad,
        dialog_corpus::Preload,
        dialog_corpus::Speculation,
        Fork<RemoteSite, Resolve>,
    );

    impl Holds for ReadOnly {
        fn held(&self, key: &str) -> Option<dialog_common::Held> {
            self.0.held(key)
        }

        fn hold(&self, key: String, handle: dialog_common::Held) {
            self.0.hold(key, handle)
        }

        fn held_or(
            &self,
            key: &str,
            make: &dyn Fn() -> dialog_common::Held,
        ) -> dialog_common::Held {
            self.0.held_or(key, make)
        }
    }

    /// A stack opens through an environment that can only read: one
    /// that opens branches and queries them, and provides nothing a
    /// commit needs. It builds the memory layer, reads where the primary
    /// points the durable layer it declares, and reads the primary and
    /// what arrives in `control:` through the same environment.
    #[dialog_common::test]
    async fn it_opens_a_stack_through_an_environment_that_only_reads() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(title(&doc, "kept"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch
            .stack()
            .perform(&operator)
            .await?
            .assert(note(&doc, "noted", &notes))
            .perform(&operator)
            .await?;
        let reader = ReadOnly(operator.clone());

        let stack = branch.stack().perform(&reader).await?;
        assert_eq!(stack.layers(), implied_and([state, notes.clone()]));
        assert!(
            stack.pointed(&notes).is_some(),
            "the durable layer is read where the primary points it"
        );
        let titles: Vec<dialog_query::Claim> = stack
            .query()
            .select(AttributeQuery::from(
                Term::<QueryRelation>::from(QueryRelation::from(
                    "doc/title".parse::<dialog_corpus::Relation>()?,
                ))
                .of(Term::<Entity>::from(doc.clone()))
                .is(Term::<String>::var("v")),
            ))
            .perform(&reader)
            .try_vec()
            .await?;
        assert_eq!(subjects(&titles), vec!["doc:1".to_string()]);
        let mut fetches = stack
            .arrivals(&crate::control_layer(), fetch_query())
            .expect("every stack has control:");
        assert!(fetches.poll(&reader).await?.is_empty());
        Ok(())
    }

    /// `counter/value` of `of` as `value`, under `last`, in the primary.
    fn counter_value(of: &Entity, value: u64) -> impl Statement + use<> {
        AttributeStatement {
            the: QueryRelation::from("counter/value".parse::<dialog_corpus::Relation>().unwrap()),
            of: of.clone(),
            is: dialog_corpus::Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(Cardinality::One),
            pick: Some(dialog_corpus::Pick::Last),
        }
    }

    /// The `counter/value` of `of` that `query` reads, zero for none.
    async fn counter_read(
        query: QueryLayer<'_>,
        env: &Operator,
        of: &Entity,
    ) -> Result<u64, CommitError> {
        let rows = query
            .select(AttributeQuery::from(
                Term::<QueryRelation>::from(QueryRelation::from(
                    "counter/value"
                        .parse::<dialog_corpus::Relation>()
                        .expect("an attribute"),
                ))
                .of(Term::<Entity>::from(of.clone()))
                .is(Term::<u64>::var("v")),
            ))
            .perform(env)
            .try_vec()
            .await?;
        Ok(match rows.first().map(|row| &row.is) {
            Some(dialog_corpus::Value::UnsignedInt(n)) => u64::try_from(*n).unwrap_or(0),
            _ => 0,
        })
    }

    /// A transact body is run again from the head that moved under it:
    /// another handle moves the counter between the first run's read and
    /// its publish, so the publish loses, the body runs a second time,
    /// reads the moved value and stages its increment of that, and the
    /// value lands once, computed from the moved head.
    #[dialog_common::test]
    async fn it_re_runs_a_transact_body_from_the_head_that_moved_under_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let counter: Entity = "ctr:1".parse()?;
        stack
            .assert(counter_value(&counter, 1))
            .perform(&operator)
            .await?;
        let other = repo.branch("main").open().perform(&operator).await?;
        let runs = Arc::new(AtomicU32::new(0));
        let read = Arc::new(Mutex::new(Vec::new()));

        let (env, ctr, ran, seen) = (
            operator.clone(),
            counter.clone(),
            runs.clone(),
            read.clone(),
        );
        let report = stack
            .transact(move |tx| {
                let (env, ctr, other) = (env.clone(), ctr.clone(), other.clone());
                let (ran, seen) = (ran.clone(), seen.clone());
                async move {
                    ran.fetch_add(1, SeqCst);
                    let n = counter_read(tx.query(), &env, &ctr).await?;
                    seen.lock().push(n);
                    if tx.number() == 1 {
                        // Another handle moves the counter after this
                        // run read it, before its publish.
                        other
                            .transaction()
                            .assert(counter_value(&ctr, 10))
                            .commit()
                            .publish()
                            .perform(&env)
                            .await?;
                    }
                    tx.assert(counter_value(&ctr, n + 1));
                    Ok(())
                }
            })
            .perform(&operator)
            .await?;

        assert_eq!(
            runs.load(SeqCst),
            2,
            "the body ran again from the moved head"
        );
        assert_eq!(*read.lock(), vec![1, 10], "the re-run read the moved value");
        assert_eq!(
            committed::<u64>(&branch, &operator, "counter/value", &counter).await?,
            vec!["UnsignedInt(11)".to_string()],
            "the value landed once, computed from the moved head"
        );
        assert_eq!(branch.revision().as_ref(), Some(report.revision()));
        Ok(())
    }

    /// A transact body that gives up fails the invocation with its
    /// reason, and nothing it staged is written: not the write, not the
    /// command beside it.
    #[dialog_common::test]
    async fn it_writes_nothing_of_a_transact_body_that_gives_up() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let mut arrivals = stack.arrivals_rows(&control).expect("control:");
        let doc: Entity = "doc:1".parse()?;
        let before = branch.revision();

        let staged = doc.clone();
        let refused = stack
            .transact(move |tx| {
                let doc = staged.clone();
                async move {
                    tx.assert(title(&doc, "staged"));
                    tx.invoke(fetch("cmd:1", &doc));
                    Err(CommitError::Aborted("nothing to do".into()))
                }
            })
            .perform(&operator)
            .await;
        assert!(
            matches!(&refused, Err(CommitError::Aborted(reason)) if reason == "nothing to do"),
            "the body's reason fails the invocation: {refused:?}"
        );
        branch.refresh(&operator).await?;
        assert_eq!(branch.revision(), before, "nothing durable was written");
        assert!(
            committed::<String>(&branch, &operator, "doc/title", &doc)
                .await?
                .is_empty()
        );
        assert!(arrivals.rows().is_empty(), "no command landed");
        Ok(())
    }

    /// An invocation's report names the head it left the primary at, the
    /// ticks its dispatch ran and exactly the commands it landed in
    /// `control:`: the ping it sent and the pong a rule dispatched, and
    /// not the writes beside them.
    #[dialog_common::test]
    async fn it_reports_what_an_invocation_landed() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_ping(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let target: Entity = "doc:1".parse()?;

        let report = stack
            .invoke(ping("cmd:1", &target))
            .assert(title(&target, "pinged"))
            .perform(&operator)
            .await?;
        assert_eq!(branch.revision().as_ref(), Some(report.revision()));
        let mut landed: Vec<String> = report
            .commands()
            .iter()
            .map(|fact| format!("{} {} {:?}", fact.of, fact.the, fact.is))
            .collect();
        landed.sort();
        assert_eq!(
            landed,
            vec![
                "cmd:1 cmd.ping/target Entity(doc:1)".to_string(),
                "cmd:1 cmd.pong/target Entity(doc:1)".to_string(),
            ],
            "the command sent and the one its rule dispatched"
        );
        // The ping and the writes are the first tick's stimulus, where
        // the rules record the ping and dispatch the pong; the pong and
        // that record are the second's, where a rule records the pong;
        // that record is the third's, where nothing fires.
        assert_eq!(report.ticks(), 3);

        let quiet = stack
            .assert(title(&target, "again"))
            .perform(&operator)
            .await?;
        assert!(
            quiet.commands().is_empty(),
            "a write alone lands no command"
        );
        Ok(())
    }

    /// A rule copying `from`, of type `kind`, from `control:` into
    /// `to` of the same entity in `session:state`, every value kept.
    fn copy_rule(from: &str, kind: &str, to: &str) -> Result<InductiveRule> {
        Ok(serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": { "v": { "the": to, "as": kind, "pick": "all", "in": "session:state" } }
            },
            "when": [
                {
                    "assert": { "with": { "v": { "the": from, "as": kind, "in": "control:" } } },
                    "where": { "this": { "?": { "name": "this" } }, "v": { "?": { "name": "v" } } }
                }
            ]
        }))?)
    }

    /// Rules copying each lifecycle row of a command into
    /// `session:state`, which an earlier declaration declared: `log/invoked`,
    /// `log/answered`, `log/failed` and `log/unhandled`.
    async fn declare_lifecycle(branch: &Branch, env: &Operator) -> Result<()> {
        branch
            .transaction()
            .assert(copy_rule(
                "dialog.command/invoked",
                "UnsignedInteger",
                "log/invoked",
            )?)
            .assert(copy_rule(
                "dialog.command/answered",
                "Entity",
                "log/answered",
            )?)
            .assert(copy_rule("dialog.command/failed", "Text", "log/failed")?)
            .assert(copy_rule(
                "dialog.command/unhandled",
                "Boolean",
                "log/unhandled",
            )?)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(())
    }

    /// A command is marked `dialog.command/invoked` in the tick it is
    /// stimulus of, beside its own rows: a rule premised on the mark
    /// copies it into a session layer, the ping invoked in the first
    /// tick, the pong a rule dispatched in the second. The mark is in no
    /// layer after its tick, and no command a report names.
    #[dialog_common::test]
    async fn it_marks_a_command_invoked_in_the_tick_it_is_stimulus_of() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_ping(&branch, &operator).await?;
        declare_lifecycle(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let control = crate::control_layer();
        let target: Entity = "doc:1".parse()?;
        let command: Entity = "cmd:1".parse()?;

        let report = stack
            .invoke(ping("cmd:1", &target))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "log/invoked", &command)?,
            vec!["UnsignedInt(1)".to_string(), "UnsignedInt(2)".to_string()],
            "the ping marked in the first tick, the pong in the second"
        );
        assert!(
            report
                .commands()
                .iter()
                .all(|fact| fact.the.as_str() != "dialog.command/invoked"),
            "the mark is no command"
        );
        let rows: Vec<dialog_query::Claim> = stack
            .query()
            .select(whole(&control))
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(rows.is_empty(), "nothing stays in control: after its tick");
        Ok(())
    }

    /// A concept of one attribute, `the`, named `field`.
    fn concept_of(field: &str, the: QueryRelation) -> dialog_query::ConceptDescriptor {
        dialog_query::ConceptDescriptor::try_from([(
            field,
            dialog_query::AttributeDescriptor::new(the, "", Cardinality::One, None),
        )])
        .expect("a concept")
    }

    /// The concept of a fetch command, `cmd.fetch/node`.
    fn fetch_concept() -> dialog_query::ConceptDescriptor {
        concept_of("node", dialog_query::the!("cmd.fetch/node"))
    }

    /// The concept of a fetch's answer, `cmd.fetched/node`.
    fn fetched_concept() -> dialog_query::ConceptDescriptor {
        concept_of("node", dialog_query::the!("cmd.fetched/node"))
    }

    /// The node a command's rows name, the value of its first row.
    fn node_of(command: &crate::Command) -> Result<Entity, crate::Failure> {
        match command.rows().first().map(|row| &row.is) {
            Some(dialog_corpus::Value::Entity(node)) => Ok(node.clone()),
            other => Err(crate::Failure::new(format!("no node: {other:?}"))),
        }
    }

    /// The `dialog.provider/command` rows the stack's own layer holds.
    fn provided(stack: &Stack) -> Result<Vec<String>> {
        held(
            ephemeral(stack, &session_layer()).expect("session:"),
            "dialog.provider/command",
            &session_layer(),
        )
    }

    /// Registering a provider writes its concept into `session:` once,
    /// as `dialog.provider/command`; registering the concept again
    /// replaces the handler and writes nothing; another concept is one
    /// more row.
    #[dialog_common::test]
    async fn it_records_each_provided_concept_in_session_once() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_fetch(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let own = ephemeral(&stack, &session_layer())
            .expect("session:")
            .clone();
        assert!(provided(&stack)?.is_empty());

        stack.provide(crate::Provider::new(&fetch_concept(), |_| async {
            Err(crate::Failure::new("the first handler"))
        }))?;
        assert_eq!(
            provided(&stack)?,
            vec![format!("Entity({})", fetch_concept().this())]
        );
        let once = own.revision();
        stack.provide(crate::Provider::new(&fetch_concept(), |_| async {
            Ok(Changes::new())
        }))?;
        assert_eq!(own.revision(), once, "a second registration writes nothing");
        assert_eq!(provided(&stack)?.len(), 1);
        stack.provide(crate::Provider::new(&fetched_concept(), |_| async {
            Ok(Changes::new())
        }))?;
        assert_eq!(provided(&stack)?.len(), 2, "one row per concept");

        let report = stack
            .invoke(fetch("cmd:1", &"node:1".parse()?))
            .perform(&operator)
            .await?;
        let effect = report.effect().perform(&operator).await?;
        assert_eq!(
            effect.answered(),
            &["cmd:1".parse::<Entity>()?],
            "the second handler replaced the first"
        );
        Ok(())
    }

    /// Performing an invocation runs no provider; its effect does: the
    /// provider answers the landed command, the answer lands in the
    /// state layer with the command's `answered` row, which a rule
    /// copies there, and nothing remains.
    #[dialog_common::test]
    async fn it_answers_a_landed_command_through_its_effect() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        declare_lifecycle(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let node: Entity = "node:1".parse()?;
        let command: Entity = "cmd:1".parse()?;
        let at = state.clone();
        stack.provide(crate::Provider::new(&fetch_concept(), move |command| {
            let at = at.clone();
            async move {
                let node = node_of(&command)?;
                let mut answer = Changes::new();
                dialog_query::the!("node/fetched")
                    .of(node)
                    .is(command.entity().clone())
                    .at(at)
                    .assert(&mut answer);
                Ok(answer)
            }
        }))?;

        let report = stack
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        assert_eq!(cells_of(report.commands()), vec!["cmd:1 cmd.fetch/node"]);
        assert!(
            held(&memory, "node/fetched", &node)?.is_empty(),
            "performing the invocation ran no provider"
        );

        let effect = report.effect().perform(&operator).await?;
        assert_eq!(effect.answered(), from_ref(&command));
        assert!(effect.failed().is_empty() && effect.unhandled().is_empty());
        assert!(!effect.remaining(), "the answer is no command");
        assert_eq!(
            held(&memory, "node/fetched", &node)?,
            vec!["Entity(cmd:1)".to_string()],
            "the answer landed"
        );
        assert_eq!(
            held(&memory, "log/answered", &command)?,
            vec![format!("Entity({})", fetch_concept().this())],
            "the answered row named the provider's concept"
        );
        Ok(())
    }

    /// A provider whose answer is a command leaves it remaining, and the
    /// next effect answers it through the provider of its concept.
    #[dialog_common::test]
    async fn it_continues_the_chain_while_commands_remain() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let node: Entity = "node:1".parse()?;
        let command: Entity = "cmd:1".parse()?;
        stack.provide(crate::Provider::new(
            &fetch_concept(),
            |command| async move {
                let node = node_of(&command)?;
                let mut answer = Changes::new();
                dialog_query::the!("cmd.fetched/node")
                    .of(command.entity().clone())
                    .is(node)
                    .at(crate::control_layer())
                    .assert(&mut answer);
                Ok(answer)
            },
        ))?;
        let at = state.clone();
        stack.provide(crate::Provider::new(&fetched_concept(), move |command| {
            let at = at.clone();
            async move {
                let mut answer = Changes::new();
                dialog_query::the!("log/acked")
                    .of(command.entity().clone())
                    .is(true)
                    .at(at)
                    .assert(&mut answer);
                Ok(answer)
            }
        }))?;

        let report = stack
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        let first = report.effect().perform(&operator).await?;
        assert!(first.remaining(), "the answer is a command");
        assert_eq!(
            cells_of(first.invocation().commands()),
            vec!["cmd:1 cmd.fetched/node"]
        );
        assert_eq!(
            held(&memory, "node/fetched", &node)?,
            vec!["Entity(cmd:1)".to_string()],
            "a rule captured the answering command in its tick"
        );
        assert!(held(&memory, "log/acked", &command)?.is_empty());

        let second = first.effect().perform(&operator).await?;
        assert_eq!(second.answered(), from_ref(&command));
        assert!(!second.remaining());
        assert_eq!(
            held(&memory, "log/acked", &command)?,
            vec!["Boolean(true)".to_string()]
        );
        Ok(())
    }

    /// The facts `rows` as `of the`, sorted.
    fn cells_of(rows: &[dialog_corpus::Fact]) -> Vec<String> {
        let mut cells: Vec<String> = rows
            .iter()
            .map(|fact| format!("{} {}", fact.of, fact.the))
            .collect();
        cells.sort();
        cells
    }

    /// A provider that fails answers its command with the `failed` row
    /// and its reason, and the other provider's answer lands beside it.
    #[dialog_common::test]
    async fn it_answers_a_failed_provider_with_its_reason() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        declare_lifecycle(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let doc: Entity = "doc:1".parse()?;
        stack.provide(crate::Provider::new(&fetch_concept(), |_| async {
            Err(crate::Failure::new("unreachable"))
        }))?;
        let at = state.clone();
        stack.provide(crate::Provider::new(
            &concept_of("doc", dialog_query::the!("cmd.store/doc")),
            move |command| {
                let at = at.clone();
                async move {
                    let doc = node_of(&command)?;
                    let mut answer = Changes::new();
                    dialog_query::the!("log/stored")
                        .of(doc)
                        .is(true)
                        .at(at)
                        .assert(&mut answer);
                    Ok(answer)
                }
            },
        ))?;

        let report = stack
            .invoke(fetch("cmd:1", &"node:1".parse()?))
            .invoke(
                dialog_query::the!("cmd.store/doc")
                    .of("cmd:2".parse::<Entity>()?)
                    .is(doc.clone()),
            )
            .perform(&operator)
            .await?;
        let effect = report.effect().perform(&operator).await?;
        assert_eq!(
            effect.failed(),
            &[("cmd:1".parse::<Entity>()?, "unreachable".to_string())]
        );
        assert_eq!(effect.answered(), &["cmd:2".parse::<Entity>()?]);
        assert_eq!(
            held(&memory, "log/failed", &"cmd:1".parse()?)?,
            vec!["String(\"unreachable\")".to_string()]
        );
        assert!(held(&memory, "log/answered", &"cmd:1".parse()?)?.is_empty());
        assert_eq!(
            held(&memory, "log/stored", &doc)?,
            vec!["Boolean(true)".to_string()],
            "the other provider's answer landed"
        );
        Ok(())
    }

    /// A command no provider claims is answered `unhandled`, not an
    /// error.
    #[dialog_common::test]
    async fn it_marks_a_command_no_provider_claims_unhandled() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        declare_lifecycle(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        stack.provide(crate::Provider::new(&fetch_concept(), |_| async {
            Ok(Changes::new())
        }))?;
        let command: Entity = "cmd:9".parse()?;

        let report = stack
            .invoke(increment("cmd:9", &"ctr:1".parse()?))
            .perform(&operator)
            .await?;
        let effect = report.effect().perform(&operator).await?;
        assert_eq!(effect.unhandled(), from_ref(&command));
        assert!(effect.answered().is_empty() && effect.failed().is_empty());
        assert!(!effect.remaining());
        assert_eq!(
            held(&memory, "log/unhandled", &command)?,
            vec!["Boolean(true)".to_string()]
        );
        Ok(())
    }

    /// An effect whose answer dispatch loses its head re-runs the
    /// dispatch from the new head, not the provider: the provider runs
    /// once, the other writer's commit stands, and the answer and its
    /// `answered` row land once.
    #[dialog_common::test]
    async fn it_re_runs_an_answer_dispatch_from_the_head_that_moved_under_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        declare_lifecycle(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let other = repo.branch("main").open().perform(&operator).await?;
        let runs = Arc::new(AtomicU32::new(0));
        let (env, ran) = (operator.clone(), runs.clone());
        stack.provide(crate::Provider::new(&fetch_concept(), move |command| {
            let (env, ran, other) = (env.clone(), ran.clone(), other.clone());
            async move {
                ran.fetch_add(1, SeqCst);
                // Another handle moves the head while the provider runs,
                // before the answer dispatch begins.
                other
                    .transaction()
                    .assert(title(&"doc:2".parse().expect("entity"), "Other"))
                    .commit()
                    .publish()
                    .perform(&env)
                    .await?;
                let mut answer = Changes::new();
                title(&node_of(&command)?, "answered").assert(&mut answer);
                Ok(answer)
            }
        }))?;
        let node: Entity = "node:1".parse()?;
        let report = stack
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;

        let effect = report.effect().perform(&operator).await?;
        assert_eq!(runs.load(SeqCst), 1);
        assert_eq!(
            effect.invocation().revision().version().edition,
            report.revision().version().edition.successor().successor(),
            "the answer published on the moved head"
        );
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &node).await?,
            vec!["String(\"answered\")".to_string()]
        );
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &"doc:2".parse()?).await?,
            vec!["String(\"Other\")".to_string()],
            "the other writer's commit stands"
        );
        assert_eq!(
            held(&memory, "log/answered", &"cmd:1".parse()?)?.len(),
            1,
            "the answered row landed once"
        );
        Ok(())
    }

    /// The effect of a report that landed no command runs no dispatch:
    /// `control:`'s revision row and `session:`'s revision are unchanged,
    /// and nothing remains.
    #[dialog_common::test]
    async fn it_performs_no_dispatch_for_an_effect_with_no_commands() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let control = crate::control_layer();
        let own = ephemeral(&stack, &session_layer())
            .expect("session:")
            .clone();
        let report = stack
            .assert(title(&"doc:1".parse()?, "x"))
            .perform(&operator)
            .await?;
        let effect = report.effect();
        assert!(effect.is_empty());
        let (row, at, head) = (
            revision_of(&own, &control)?,
            own.revision(),
            branch.revision(),
        );

        let performed = effect.perform(&operator).await?;
        assert!(!performed.remaining());
        assert_eq!(performed.invocation().ticks(), 0);
        assert_eq!(performed.invocation().revision(), report.revision());
        assert_eq!(revision_of(&own, &control)?, row, "control: did not move");
        assert_eq!(own.revision(), at, "session: did not move");
        assert_eq!(branch.revision(), head);
        Ok(())
    }

    /// The `dialog.*` rows `memory` holds, as `of the = is` text, sorted.
    fn dialog_rows(memory: &Ephemeral) -> Vec<String> {
        let mut rows: Vec<String> = memory
            .export()
            .iter()
            .filter(|(_, the, _)| the.as_str().starts_with("dialog."))
            .map(|(of, the, change)| format!("{of} {the} = {change:?}"))
            .collect();
        rows.sort();
        rows
    }

    /// A rule writing, into `session:state`, the primary's
    /// `dialog.branch/revision` it reads in `session:`, under `last`, as
    /// `seen/branch-revision` of the branch entity: premised on the one
    /// row of the cell, with no join.
    fn watch_primary() -> Result<InductiveRule> {
        Ok(serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "revision": {
                        "the": "seen/branch-revision", "as": "Entity", "pick": "last",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "revision": {
                                "the": "dialog.branch/revision", "as": "Entity",
                                "in": "session:"
                            }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "revision": { "?": { "name": "revision" } }
                    }
                }
            ]
        }))?)
    }

    /// The stack writes nothing into the primary's session overlay,
    /// which is the embedder's: after it opens, a dispatch, a selection,
    /// a provider's registration, a channel's creation and a sync,
    /// [`Branch::overlay`] holds no row under `dialog.`, and the
    /// embedder's own row there is untouched. Every one of those rows is
    /// in the stack's own `session:` instead.
    #[dialog_common::test]
    async fn it_writes_no_dialog_row_into_the_branch_overlay() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let embedder: Entity = "tab:1".parse()?;
        branch.overlay().assert(primary_flag(&embedder, true))?;
        let before = branch.overlay().export();

        let stack = branch.stack().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        stack
            .assert(flag(&doc, true, &state))
            .assert(primary_flag(&doc, true))
            .perform(&operator)
            .await?;
        stack.select(Some(state.clone())).perform(&operator).await?;
        stack.provide(crate::Provider::new(&fetch_concept(), |_| async {
            Ok(Changes::new())
        }))?;
        stack.channel(&"mail:box".parse()?)?;
        stack
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;

        assert!(
            dialog_rows(branch.overlay()).is_empty(),
            "no row of dialog's in the overlay: {:?}",
            dialog_rows(branch.overlay())
        );
        assert_eq!(
            branch.overlay().export(),
            before,
            "the embedder's row is all the overlay holds"
        );
        let own = ephemeral(&stack, &session_layer()).expect("its own layer");
        let rows = dialog_rows(own);
        for the in [
            "dialog.layer/type",
            "dialog.layer/branch",
            "dialog.layer/revision",
            "dialog.session/layer",
            "dialog.provider/command",
        ] {
            assert!(
                rows.iter().any(|row| row.contains(&format!(" {the} "))),
                "session: holds {the}: {rows:?}"
            );
        }
        Ok(())
    }

    /// The primary's `dialog.branch/revision` cell reads one value
    /// through the stack at every point: at open, after a commit through
    /// another handle of the branch moved the head (before any dispatch
    /// recorded it), and after the dispatch that follows. A rule premised
    /// on it, with no `dialog.layer/branch` join, fires once per move:
    /// the dispatch after the move fires it, and the one after that does
    /// not.
    #[dialog_common::test]
    async fn it_reads_one_primary_revision_per_cell_and_fires_once_per_move() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let primary = stack
            .branch_of(&memory_layer())
            .expect("the primary's branch")
            .clone();
        let memory = ephemeral(&stack, &state).expect("the state layer").clone();
        let seen = || held(&memory, "seen/branch-revision", &primary);
        let own = ephemeral(&stack, &session_layer()).expect("its own layer");
        assert!(held(own, "dialog.branch/revision", &primary)?.is_empty());

        let head = stack.primary().revision().expect("a head");
        assert_eq!(
            primary_revision_of(&stack, &operator).await?,
            vec![entity_text(&head)],
            "one value at open"
        );
        stack.assert(watch_primary()?).perform(&operator).await?;
        let installed = stack.primary().revision().expect("a head");
        assert_eq!(
            primary_revision_of(&stack, &operator).await?,
            vec![entity_text(&installed)],
            "one value after a dispatch"
        );

        // A commit through another handle moves the head.
        let other = repo.branch("main").open().perform(&operator).await?;
        other
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:1".parse::<Entity>()?)
                    .is("Moved".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        stack.primary().refresh(&operator).await?;
        let moved = stack.primary().revision().expect("a head");
        assert_ne!(moved, installed);
        assert_eq!(
            primary_revision_of(&stack, &operator).await?,
            vec![entity_text(&moved)],
            "one value between the move and the dispatch that records it"
        );

        let sequence = memory.revision().sequence;
        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            seen()?,
            vec![entity_text(&moved)],
            "the rule fired on the move"
        );
        assert_eq!(memory.revision().sequence, sequence + 1, "once");
        assert_eq!(
            primary_revision_of(&stack, &operator).await?,
            vec![entity_text(&moved)],
            "one value after the dispatch"
        );

        stack.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            memory.revision().sequence,
            sequence + 1,
            "nothing moved, so the rule did not fire again"
        );
        Ok(())
    }

    /// Tonk's `retain_overlay_entities` is the embedder's own
    /// `Branch::overlay().retain_entities`, untouched by the stack: the
    /// overlay it walks holds the embedder's rows alone, never the
    /// stack's. Tonk's `clear_overlay` is `Branch::overlay().clear()`,
    /// which leaves the stack's `session:` as it was: its layers, its
    /// revision rows and its selection.
    #[dialog_common::test]
    async fn it_leaves_its_own_layer_to_an_overlay_retain_or_clear() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let stack = branch.stack().perform(&operator).await?;
        let (kept, gone): (Entity, Entity) = ("tab:1".parse()?, "tab:2".parse()?);
        branch
            .overlay()
            .assert(primary_flag(&kept, true))?
            .assert(primary_flag(&gone, true))?;
        stack
            .assert(flag(&"doc:1".parse()?, true, &state))
            .perform(&operator)
            .await?;
        stack.select(Some(state.clone())).perform(&operator).await?;
        let own = ephemeral(&stack, &session_layer())
            .expect("its own layer")
            .clone();
        let (layers, rows, selected) = (stack.layers(), own.export(), stack.selected());
        assert_eq!(selected, Some(state.clone()));

        let mut walked = Vec::new();
        branch.overlay().retain_entities(|entity| {
            walked.push(entity.to_string());
            *entity == kept
        });
        walked.sort();
        walked.dedup();
        assert_eq!(
            walked,
            vec![kept.to_string(), gone.to_string()],
            "the overlay holds the embedder's rows alone"
        );
        assert!(held(branch.overlay(), "ui/selected", &gone)?.is_empty());
        assert_eq!(own.export(), rows, "a retain leaves session: as it was");

        branch.overlay().clear();
        assert!(branch.overlay().is_empty());
        assert_eq!(stack.layers(), layers, "the layers are unchanged");
        assert_eq!(
            own.export(),
            rows,
            "and so are session:'s rows, revisions among them"
        );
        assert_eq!(stack.selected(), selected, "and the selection");
        Ok(())
    }
}
