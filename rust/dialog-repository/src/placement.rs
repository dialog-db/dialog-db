//! The layers a primary declares beside itself, and the names of the
//! layers every stack has.
//!
//! A stack reads several layers as one (see [`Stack`](crate::Stack)):
//! the **primary**, a branch whose schema defines the others, and the
//! layers that schema declares beside it. A layer is declared on the
//! primary and replicated with it, by its **type**, a bare-scheme entity
//! ([`LayerType`]):
//!
//! ```text
//! session:state   dialog.layer/type     session:
//! mail:box        dialog.layer/type     channel:
//! memory:notes    dialog.layer/type     branch:
//! ```
//!
//! Where a row lies is a dimension of the row (`in:`), said by the
//! descriptor that reads or writes it (see
//! [`AttributeDescriptor::in_layer`](dialog_query::AttributeDescriptor::in_layer)):
//! a write with no layer named lands in the primary, a read with none
//! reads every layer. A layer's name says its store, and the bare name
//! is that store's default: [`MEMORY`] is the primary, [`SESSION`] the
//! in-memory layer every stack has, [`CONTROL`] the loopback a command
//! lands in. Every stack has those three, and writes their facts into
//! [`SESSION`] when it opens:
//!
//! ```text
//! control:   dialog.layer/type     channel:
//! session:   dialog.layer/type     session:
//! memory:    dialog.layer/type     branch:
//! memory:    dialog.layer/branch   <the primary's branch entity>
//! ```
//!
//! A **durable** layer, `memory:<name>` ([`LayerType::Branch`]), has no
//! head of its own: it is the tree the primary points at. The primary
//! holds, keyed by the layer's entity, the meta branch's
//! `dialog.branch/revision`, naming the entity of the revision the layer
//! stands at ([`Revision::entity`]), and beside it `dialog.branch/tree`,
//! that revision's tree hash in base58, as the meta branch renders a
//! branch's tree, so the tree is found without the revision. A dispatch
//! that writes into the layer builds the layer's new tree and names it
//! in the same commit of the primary, so the primary's history says which
//! tree of each durable layer goes with each of its own:
//!
//! ```text
//! memory:notes   dialog.branch/revision   <its revision's entity>
//! memory:notes   dialog.branch/tree       "<its tree, base58>"
//! ```
//!
//! A **channel** ([`LayerType::Channel`], [`Channel`](crate::Channel))
//! holds a row for one tick: what a dispatch writes into it (`.at(..)`,
//! a rule head whose descriptor names it, or, for [`CONTROL`], a
//! command, whose concept carries the `dialog.concept/transient`
//! marker) is the next tick's stimulus, read as a row of the channel,
//! and is gone after that tick; once the dispatch has published, it
//! lands on the channel's outside end. [`CONTROL`] is the built-in one,
//! the loopback. A channel is named by any entity but one under
//! [`MEMORY`] or [`SESSION`].
//!
//! The declarations in force are resolved from three places, each
//! outranking the one before: the primary's tree, the primary's session
//! store, and what the stack itself holds
//! ([`Stack::declare`](crate::Stack::declare)). The
//! `dialog.layer/store` declarations of earlier builds are not read: a
//! schema holding only those declares no layer.

use std::collections::HashMap;
use std::sync::LazyLock;

use dialog_corpus::selector::Constrained;
use dialog_corpus::{
    Change, Changes, DialogCorpusError, Entity, Fact, FactView, Pick, Relation, Selector,
    Statement, Update, Value,
};
use dialog_effects::branch::invalid_name;
use dialog_query::the;
use futures_util::TryStreamExt as _;

use crate::repository::source::Source;
use crate::{Channel, CommitError, Ephemeral, Revision};
use crate::{Erased, select_from_source};
use base58::ToBase58 as _;

// The declaration attributes are parsed once: every stack open asks
// for them.
static TYPE_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.layer/type").into());
static BRANCH_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.layer/branch").into());
// The revision and selection attributes are read on every dispatch
// through a stack.
static REVISION_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.layer/revision").into());
static BRANCH_REVISION_ATTR: LazyLock<Relation> =
    LazyLock::new(|| the!("dialog.branch/revision").into());
static BRANCH_TREE_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.branch/tree").into());
static SELECTION_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.session/layer").into());

pub use dialog_corpus::{MEMORY, memory_layer};

/// The name of the layer every stack mints: in memory, the stack's
/// own, gone when the stack is.
pub const SESSION: &str = "session:";
/// The name of the loopback: where a command lands, for one tick.
pub const CONTROL: &str = "control:";

/// The type entity of a channel layer, [`LayerType::Channel`].
const CHANNEL_TYPE: &str = "channel:";
/// The type entity of a durable layer, [`LayerType::Branch`].
const BRANCH_TYPE: &str = "branch:";

// The layer entities are parsed once: a read asks for them per line it
// reads, a commit per head it writes.
static SESSION_LAYER: LazyLock<Entity> =
    LazyLock::new(|| SESSION.parse().expect("a bare scheme is an entity"));
static CONTROL_LAYER: LazyLock<Entity> =
    LazyLock::new(|| CONTROL.parse().expect("a bare scheme is an entity"));
static CHANNEL_TYPE_ENTITY: LazyLock<Entity> =
    LazyLock::new(|| CHANNEL_TYPE.parse().expect("a bare scheme is an entity"));
static BRANCH_TYPE_ENTITY: LazyLock<Entity> =
    LazyLock::new(|| BRANCH_TYPE.parse().expect("a bare scheme is an entity"));

/// The entity of the stack's own layer, [`SESSION`].
pub fn session_layer() -> Entity {
    SESSION_LAYER.clone()
}

/// The entity of the loopback, [`CONTROL`].
pub fn control_layer() -> Entity {
    CONTROL_LAYER.clone()
}

/// The `dialog.layer/type` declaration attribute: of a layer's entity,
/// its [`LayerType`], as the bare-scheme entity naming it.
pub fn layer_type_attr() -> Relation {
    TYPE_ATTR.clone()
}

/// The `dialog.layer/branch` attribute: of [`MEMORY`], the entity of the
/// primary's branch, the one the meta branch derives from
/// `(replica, name)` ([`schema::Branch`](crate::schema::Branch)), as a
/// stack writes it into [`SESSION`] when it opens. A durable layer has no
/// branch, and no such fact.
pub fn layer_branch_attr() -> Relation {
    BRANCH_ATTR.clone()
}

/// The `dialog.layer/revision` attribute: of a memory layer's entity
/// ([`SESSION`] or a declared session layer) or a channel's ([`CONTROL`]
/// or another), the revision the layer stands at, as text: the base58
/// hash of a memory layer's instant chain, or of the sequence of the
/// last tick that landed in a channel ([`Channel::revision`](crate::Channel::revision)).
/// A stack holds one row of it per memory layer and channel in
/// [`SESSION`], rewritten by the dispatch, or the sync, after which the
/// layer stands somewhere else. A durable layer's revision is the meta
/// branch's `dialog.branch/revision` of the layer's entity in the
/// primary instead, which names the revision entity; neither an
/// [`Ephemeral`]'s chain nor a channel's revision has an entity form
/// yet, so they stay text.
pub fn revision_attr() -> Relation {
    REVISION_ATTR.clone()
}

/// The meta branch's `dialog.branch/revision` attribute: of a branch
/// entity, the entity of the revision it stands at
/// ([`Revision::entity`]). The primary's row of it lives in
/// [`SESSION`]; a durable layer's is a pointer fact in the primary, of
/// the layer's entity.
pub(crate) fn branch_revision_attr() -> Relation {
    BRANCH_REVISION_ATTR.clone()
}

/// The meta branch's `dialog.branch/tree` attribute: of a durable
/// layer's entity, in the primary, the tree hash of the revision its
/// `dialog.branch/revision` names, in base58.
pub(crate) fn branch_tree_attr() -> Relation {
    BRANCH_TREE_ATTR.clone()
}

/// The `dialog.session/layer` attribute: of [`SESSION`], the layer a
/// write with no layer named lands in while it is set (see
/// [`Stack::select`](crate::Stack::select)).
pub fn selection_attr() -> Relation {
    SELECTION_ATTR.clone()
}

/// Whether a write of `the` with no layer named follows the stack's
/// selected layer: every attribute does but the machinery under
/// `dialog.` (rules, layer declarations, revisions, the selection
/// itself), which lands in the primary or where it names.
pub(crate) fn follows_selection(the: &Relation) -> bool {
    !the.as_str().starts_with("dialog.")
}

/// The layer `own`, a stack's own layer, holds as selected, if any:
/// its `dialog.session/layer` row of [`SESSION`].
pub(crate) fn selected(own: &Ephemeral) -> Option<Entity> {
    own.scan(&Selector::new().the(selection_attr()).of(session_layer()))
        .into_iter()
        .find_map(|fact| match fact.is {
            Value::Entity(layer) => Some(layer),
            _ => None,
        })
}

/// [`Statement`] selecting `layer` in a stack's own layer: its
/// `dialog.session/layer` row of [`SESSION`], under `last`, written into
/// [`SESSION`]. See [`Stack::select`](crate::Stack::select).
pub(crate) struct Selection(pub(crate) Entity);

impl Statement for Selection {
    fn assert(self, update: &mut impl Update) {
        update.associate_in(
            session_layer(),
            selection_attr(),
            session_layer(),
            Value::Entity(self.0),
            Pick::Last,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate_in(
            session_layer(),
            selection_attr(),
            session_layer(),
            Value::Entity(self.0),
        );
    }
}

/// The revision a branch stands at, as its `dialog.branch/revision` row
/// and a durable layer's pointer fact carry it: the revision's entity.
pub(crate) fn revision_entity(revision: &Revision) -> Value {
    Value::Entity(revision.entity())
}

/// One row a stack records in its own layer: of `of`, under `the`,
/// `is`.
type Row = (Entity, Relation, Value);

/// The revision every layer of a stack holds a row for stands at: of
/// the primary, `branch`'s `dialog.branch/revision`, the entity of
/// `primary`'s revision (none for a primary with no revision); of each
/// of `memories` the `dialog.layer/revision` hash of its instant chain;
/// of each of `channels`, [`CONTROL`] among them, the
/// `dialog.layer/revision` hash of the sequence of its last tick; and of
/// [`SESSION`], `own`, the hash of its chain as it stood before the
/// records that followed its last change ([`Ephemeral::settled`]), so
/// that recording the revisions is not itself a move of the stack's own
/// layer. A memory layer's and a channel's hash is base58 text: neither
/// revision has an entity form yet. A durable layer has no row: the
/// primary's pointer fact of it is the record.
fn revisions(
    branch: &Entity,
    primary: Option<&Revision>,
    memories: &[(Entity, Ephemeral)],
    channels: &[Channel],
    own: &Ephemeral,
) -> Vec<Row> {
    let chain = |memory: &Ephemeral| Value::String(memory.revision().hash.as_bytes().to_base58());
    let session = session_layer();
    let mut rows = Vec::with_capacity(memories.len() + channels.len() + 2);
    if let Some(revision) = primary {
        rows.push((
            branch.clone(),
            branch_revision_attr(),
            revision_entity(revision),
        ));
    }
    for (layer, memory) in memories {
        if *layer != session {
            rows.push((layer.clone(), revision_attr(), chain(memory)));
        }
    }
    for channel in channels {
        rows.push((
            channel.entity().clone(),
            revision_attr(),
            Value::String(channel.revision().as_bytes().to_base58()),
        ));
    }
    rows.push((
        session,
        revision_attr(),
        Value::String(own.settled().as_bytes().to_base58()),
    ));
    rows
}

/// What a stack records of its layers' revisions, and where: the
/// primary's branch entity, the primary's revision, the memory layers,
/// the channels and the stack's own layer (see [`revisions`]).
pub(crate) struct Recorded<'a> {
    /// The primary's branch entity.
    pub(crate) branch: &'a Entity,
    /// The revision the primary stands at, if any.
    pub(crate) primary: Option<&'a Revision>,
    /// The memory layers the schema declares, by name.
    pub(crate) memories: &'a [(Entity, Ephemeral)],
    /// The channels, the loopback, [`CONTROL`], among them.
    pub(crate) channels: &'a [Channel],
    /// The stack's own layer, [`SESSION`].
    pub(crate) own: &'a Ephemeral,
}

impl Recorded<'_> {
    /// The rows whose revision moved since the stack's own layer last
    /// recorded them: each row (see [`revisions`]) whose value as it
    /// stands now is not the one the own layer holds of its cell, with
    /// the facts the own layer holds of it. A row the own layer holds
    /// nothing of has moved: nothing recorded it yet. What both the
    /// recording ([`rows`](Self::rows)) and the stimulus
    /// ([`stimulus`](Self::stimulus)) of a dispatch are made of.
    fn moved(&self) -> Vec<(Row, Vec<Fact>)> {
        revisions(
            self.branch,
            self.primary,
            self.memories,
            self.channels,
            self.own,
        )
        .into_iter()
        .filter_map(|(of, the, is)| {
            let recorded = self
                .own
                .scan(&Selector::new().the(the.clone()).of(of.clone()));
            if recorded.iter().any(|fact| fact.is == is) {
                None
            } else {
                Some(((of, the, is), recorded))
            }
        })
        .collect()
    }

    /// The revision rows that moved since the stack's own layer last
    /// recorded them (see [`moved`](Self::moved)), each under `last`,
    /// bound for no layer: what a stack records in its own layer once a
    /// dispatch has published, and when it opens. Empty when no layer
    /// moved, so the recording mints no instant.
    pub(crate) fn rows(&self) -> Changes {
        let mut rows = Changes::new();
        for ((of, the, is), _) in self.moved() {
            rows.associate(the, of, is, Pick::Last);
        }
        rows
    }

    /// The change in the revision rows since the stack's own layer last
    /// recorded them (see [`moved`](Self::moved)): for each that moved,
    /// the retraction of the row the own layer holds and the row as it
    /// stands now. What a dispatch through a stack takes as stimulus, so
    /// a rule premised on a layer's revision fires when the layer moved
    /// outside a dispatch. The retraction matters: the recorded row
    /// stands in `session:` above the dispatch's own rows, and would be
    /// elected over the new one.
    pub(crate) fn stimulus(&self) -> Changes {
        let mut rows = Changes::new();
        for ((of, the, is), recorded) in self.moved() {
            for fact in recorded {
                rows.dissociate(the.clone(), of.clone(), fact.is);
            }
            rows.associate(the, of, is, Pick::Last);
        }
        rows
    }

    /// The facts of the three layers every stack has, as the stack
    /// writes them into its own layer when it opens: of [`CONTROL`] its
    /// type, `channel:`; of [`SESSION`] its type, `session:`; of
    /// [`MEMORY`] its type, `branch:`, and its branch, `branch`. Those
    /// the own layer already holds are left out, so a second stack
    /// opened on the same branch writes none of them again.
    pub(crate) fn implied(&self) -> Changes {
        let rows: [Row; 4] = [
            (
                control_layer(),
                layer_type_attr(),
                LayerType::Channel.value(),
            ),
            (
                session_layer(),
                layer_type_attr(),
                LayerType::Session.value(),
            ),
            (
                memory_layer(),
                layer_type_attr(),
                Value::Entity(BRANCH_TYPE_ENTITY.clone()),
            ),
            (
                memory_layer(),
                layer_branch_attr(),
                Value::Entity(self.branch.clone()),
            ),
        ];
        let mut implied = Changes::new();
        for (of, the, is) in rows {
            let selector = Selector::new()
                .the(the.clone())
                .of(of.clone())
                .is(is.clone());
            if self.own.scan(&selector).is_empty() {
                implied.associate(the, of, is, Pick::Last);
            }
        }
        implied
    }
}

/// Take the writes into [`CONTROL`] out of `changes`, as a batch bound
/// for no layer: what a dispatch carries as the next tick's stimulus
/// rather than writing anywhere. The writes into every other layer stay.
pub(crate) fn take_dispatched(changes: &mut Changes) -> Changes {
    let mut dispatched = Changes::new();
    if !changes.has_shares() {
        return dispatched;
    }
    let control = control_layer();
    for (layer, share) in changes.take_shares() {
        if layer == control {
            dispatched.merge(share);
        } else {
            changes.merge(share.into_layer(layer));
        }
    }
    dispatched
}

/// What a layer a stack builds from the primary's schema is: its
/// `dialog.layer/type`, a bare-scheme entity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LayerType {
    /// `session:`, an in-memory layer: the stack mints it when it opens,
    /// and it is gone when the stack is.
    Session,
    /// `channel:`, a channel ([`Channel`]): a layer
    /// whose rows live one tick, with two ends. What the outside end
    /// sends is read by the next dispatch's first tick as rows of the
    /// channel; what a dispatch writes into it lands on the outside end
    /// as one tick of its log once the dispatch has published, read with
    /// [`Stack::arrivals`](crate::Stack::arrivals). Named by any entity
    /// but one under [`MEMORY`] or [`SESSION`]: a channel named so fails
    /// the open with [`CommitError::InvalidPlacement`]. Bare
    /// [`CONTROL`], the loopback, is every stack's, and declaring it
    /// builds nothing; [`Stack::channel`](crate::Stack::channel) creates
    /// one at run time.
    Channel,
    /// `branch:`, a durable layer, `memory:<name>`: a tree the primary
    /// points at. It has no head and no branch of its own: the layer is
    /// at the tree the primary's `dialog.branch/revision` and
    /// `dialog.branch/tree` of the layer's entity name, and is empty
    /// while the primary names none. A stack reads that tree as a line
    /// stamped with the layer's name, and a dispatch that writes into
    /// the layer builds its next tree and names it in the same commit of
    /// the primary, so the primary's compare-and-swap is the layer's
    /// too. No memory cell holds its rows.
    /// [`Stack::sync`](crate::Stack::sync) carries the pointed trees with
    /// the primary: a pull downloads them before the primary moves, and
    /// a push uploads them before the primary's head.
    Branch,
}

impl LayerType {
    /// The bare-scheme entity a `dialog.layer/type` fact carries for
    /// this type: `session:`, `channel:` or `branch:`.
    pub fn entity(&self) -> Entity {
        match self {
            LayerType::Session => session_layer(),
            LayerType::Channel => CHANNEL_TYPE_ENTITY.clone(),
            LayerType::Branch => BRANCH_TYPE_ENTITY.clone(),
        }
    }

    fn value(&self) -> Value {
        Value::Entity(self.entity())
    }
}

/// Read a `dialog.layer/type` value as a layer type, or fail the open.
fn scheme(layer: &Entity, value: &Value) -> Result<LayerType, CommitError> {
    let scheme = match value {
        Value::Entity(kind) if *kind == *SESSION_LAYER => Some(LayerType::Session),
        Value::Entity(kind) if *kind == *CHANNEL_TYPE_ENTITY => Some(LayerType::Channel),
        Value::Entity(kind) if *kind == *BRANCH_TYPE_ENTITY => Some(LayerType::Branch),
        _ => None,
    };
    scheme.ok_or_else(|| CommitError::InvalidPlacement {
        attribute: format!("dialog.layer/type of {layer}"),
        value: format!("{value:?}"),
    })
}

/// [`Statement`] declaring a layer beside the primary: `layer` is the
/// entity a descriptor's `in:` names, and `kind` what it is. A stack
/// opened on the primary builds the layer from this. Asserting it
/// writes the layer's `dialog.layer/type`, under `last`.
///
/// ```no_run
/// # use dialog_repository::{Branch, LayerDeclaration, LayerType};
/// # fn main() -> anyhow::Result<()> {
/// # let branch: Branch = todo!();
/// let state: dialog_corpus::Entity = "session:state".parse()?;
/// let tx = branch
///     .transaction()
///     .assert(LayerDeclaration::new(state, LayerType::Session));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerDeclaration {
    /// The layer.
    pub layer: Entity,
    /// What it is.
    pub kind: LayerType,
}

impl LayerDeclaration {
    /// Declare `layer` as a `kind` layer.
    pub fn new(layer: Entity, kind: LayerType) -> Self {
        Self { layer, kind }
    }
}

impl Statement for LayerDeclaration {
    fn assert(self, update: &mut impl Update) {
        update.associate(layer_type_attr(), self.layer, self.kind.value(), Pick::Last);
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(layer_type_attr(), self.layer, self.kind.value());
    }
}

/// [`Statement`] recording, in the primary, where a durable layer
/// stands: its pointer facts, the `dialog.branch/revision` of the
/// layer's entity, naming the revision entity, and beside it its
/// `dialog.branch/tree`, the revision's tree in base58, both under
/// `last`, bound for no layer, so they land in the primary and never
/// follow a selection. A dispatch that moves a durable layer writes
/// them; so does [`Stack::sync`](crate::Stack::sync) when it merges what
/// a pull brought.
pub(crate) struct Pointer<'a> {
    /// The layer's entity.
    pub(crate) layer: &'a Entity,
    /// The revision it stands at.
    pub(crate) revision: &'a Revision,
}

impl Pointer<'_> {
    /// The two cells and the values this pointer writes into them.
    fn facts(&self) -> [(Relation, Value); 2] {
        [
            (branch_revision_attr(), revision_entity(self.revision)),
            (branch_tree_attr(), tree_value(&self.revision.tree)),
        ]
    }
}

impl Statement for Pointer<'_> {
    fn assert(self, update: &mut impl Update) {
        for (the, is) in self.facts() {
            update.associate(the, self.layer.clone(), is, Pick::Last);
        }
    }

    fn retract(self, update: &mut impl Update) {
        for (the, is) in self.facts() {
            update.dissociate(the, self.layer.clone(), is);
        }
    }
}

/// A tree as a durable layer's `dialog.branch/tree` fact carries it:
/// its hash in base58, as the meta branch renders a branch's tree.
pub(crate) fn tree_value(tree: &crate::TreeReference) -> Value {
    Value::String(tree.hash().to_base58())
}

/// Check that `layer`, declared durable, is named `memory:<name>`: a
/// layer declared durable under any other name, or with a name that is
/// not a branch name, fails the open. The name is the layer's alone; no
/// branch is opened for it.
pub(crate) fn durable_name(layer: &Entity) -> Result<(), CommitError> {
    let text = layer.to_string();
    let name = text.strip_prefix(MEMORY).unwrap_or_default();
    if name.is_empty() || invalid_name(name).is_some() {
        return Err(CommitError::InvalidPlacement {
            attribute: format!("dialog.layer/type of {layer}"),
            value: "branch: names a layer memory:<name>".into(),
        });
    }
    Ok(())
}

/// Check that `layer`, a channel, is not named under [`MEMORY`] or
/// [`SESSION`], which name the other types of layer: a channel named so
/// is refused with [`CommitError::InvalidPlacement`].
pub(crate) fn channel_name(layer: &Entity) -> Result<(), CommitError> {
    let text = layer.to_string();
    if text.starts_with(MEMORY) || text.starts_with(SESSION) {
        return Err(CommitError::InvalidPlacement {
            attribute: format!("dialog.layer/type of {layer}"),
            value: "channel: names a layer outside memory: and session:".into(),
        });
    }
    Ok(())
}

/// The declarations among `types`, the `dialog.layer/type` facts one
/// place holds, by layer.
fn declarations(
    types: impl IntoIterator<Item = Fact>,
) -> Result<HashMap<Entity, LayerType>, CommitError> {
    let mut declared = HashMap::new();
    for fact in types {
        let kind = scheme(&fact.of, &fact.is)?;
        declared.insert(fact.of, kind);
    }
    Ok(declared)
}

/// The layers declared for a stack: the primary's committed
/// declarations, with the ones its session store and the stack hold
/// laid over, in that order.
#[derive(Debug, Default)]
pub(crate) struct Layers {
    committed: HashMap<Entity, LayerType>,
    declared: HashMap<Entity, LayerType>,
    retracted: HashMap<Entity, LayerType>,
}

impl Layers {
    /// Resolve the layers declared over `primary`, pinned: its committed
    /// declarations, then those its session store holds, then `held`
    /// (what the stack declares), a later one for the same layer
    /// outranking an earlier.
    pub(crate) async fn resolve(
        primary: &Source,
        held: &Changes,
        env: &Erased,
    ) -> Result<Self, CommitError> {
        let mut layers = Layers::default();
        if primary.revision().is_some() {
            let types = committed(primary, Selector::new().the(layer_type_attr()), env).await?;
            layers.committed = declarations(types)?;
        }
        // A declaration held in the primary's session store is this
        // process's own: it never reaches the tree or a peer, outranks
        // the committed one, and yields to what the stack holds.
        let overlay = primary.overlay();
        layers.declared = declarations(overlay.scan(&Selector::new().the(layer_type_attr())))?;
        layers.lay(held)?;
        Ok(layers)
    }

    /// Lay the declarations of `changes` over what is resolved so far: a
    /// later one for the same layer outranks an earlier, and a retraction
    /// of what was declared undeclares it.
    fn lay(&mut self, changes: &Changes) -> Result<(), CommitError> {
        let type_attr = layer_type_attr();
        for (entity, the, change) in changes.iter() {
            if *the != type_attr {
                continue;
            }
            let (Change::Assert(value, _) | Change::Retract(value)) = change;
            let kind = scheme(entity, value)?;
            match change {
                Change::Retract(_) => {
                    if self.declared.get(entity) == Some(&kind) {
                        self.declared.remove(entity);
                    }
                    self.retracted.insert(entity.clone(), kind);
                }
                _ => {
                    self.declared.insert(entity.clone(), kind);
                }
            }
        }
        Ok(())
    }

    /// Every layer declared, with what it is, by name.
    pub(crate) fn layers(&self) -> Vec<(Entity, LayerType)> {
        let mut layers: HashMap<Entity, LayerType> = self
            .committed
            .iter()
            .filter(|(layer, kind)| self.retracted.get(*layer) != Some(kind))
            .map(|(layer, kind)| (layer.clone(), *kind))
            .collect();
        for (layer, kind) in &self.declared {
            layers.insert(layer.clone(), *kind);
        }
        let mut layers: Vec<(Entity, LayerType)> = layers.into_iter().collect();
        layers.sort_by_key(|a| a.0.to_string());
        layers
    }
}

/// Collect the facts a selector matches on the primary's committed tree,
/// one for each cell: the claim that `last` elects, which every
/// declaration is written under.
async fn committed(
    primary: &Source,
    selector: Selector<Constrained>,
    env: &Erased,
) -> Result<Vec<Fact>, CommitError> {
    let views: Vec<FactView> = select_from_source(primary, env, selector)
        .await
        .map_err(|error| CommitError::Induction(format!("layer scan: {error}")))?
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("layer scan: {error}")))?;
    elected(views).map_err(|error| CommitError::Induction(format!("layer scan: {error}")))
}

/// Of the claims of each cell among `views`, the one that `last` elects:
/// the one a read of the cell returns. A declaration is written under
/// `last`, but two replicas can each declare one, and a merge keeps both
/// claims in the cell. Every replica elects the same one. The cells keep
/// the order they are first met in.
pub(crate) fn elected(views: Vec<FactView>) -> Result<Vec<Fact>, DialogCorpusError> {
    let mut cells: Vec<FactView> = Vec::new();
    let mut index: HashMap<(Vec<u8>, Vec<u8>), usize> = HashMap::new();
    for view in views {
        let cell = (
            view.the_bytes()?.into_owned(),
            view.of_bytes()?.into_owned(),
        );
        match index.get(&cell) {
            Some(&at) => {
                let incumbent = cells[at].clone();
                cells[at] = incumbent.elect(view)?;
            }
            None => {
                index.insert(cell, cells.len());
                cells.push(view);
            }
        }
    }
    cells.iter().map(FactView::to_owned).collect()
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;

    /// The bare names parse as entities and render unchanged.
    #[dialog_common::test]
    fn it_names_the_layers_every_stack_has() {
        assert_eq!(memory_layer().to_string(), MEMORY);
        assert_eq!(session_layer().to_string(), SESSION);
        assert_eq!(control_layer().to_string(), CONTROL);
    }

    /// A layer type names itself as the bare-scheme entity its
    /// `dialog.layer/type` fact carries, and that entity names it back;
    /// any other value, the text a `dialog.layer/store` fact carried
    /// among them, is refused with `InvalidPlacement`.
    #[dialog_common::test]
    fn it_reads_back_every_layer_type_it_names() -> anyhow::Result<()> {
        let layer: Entity = "memory:notes".parse()?;
        for (kind, named) in [
            (LayerType::Session, "session:"),
            (LayerType::Channel, "channel:"),
            (LayerType::Branch, "branch:"),
        ] {
            assert_eq!(kind.entity().to_string(), named);
            assert_eq!(scheme(&layer, &kind.value())?, kind);
        }
        for refused in [
            Value::String("memory".into()),
            Value::String("session:".into()),
            Value::Entity("disk:".parse()?),
        ] {
            assert!(
                matches!(
                    scheme(&layer, &refused),
                    Err(CommitError::InvalidPlacement { .. })
                ),
                "{refused:?}"
            );
        }
        Ok(())
    }

    /// A declaration writes the layer's type and nothing else, under
    /// `last`, a durable layer's as any other's; laid over nothing it
    /// resolves to the type it declared, and its retraction undeclares
    /// it.
    #[dialog_common::test]
    fn it_resolves_what_a_declaration_writes() -> anyhow::Result<()> {
        let notes: Entity = "memory:notes".parse()?;
        let state: Entity = "session:state".parse()?;
        let mut changes = Changes::new();
        LayerDeclaration::new(notes.clone(), LayerType::Branch).assert(&mut changes);
        LayerDeclaration::new(state.clone(), LayerType::Session).assert(&mut changes);
        let facts: Vec<(Entity, Relation, Value)> = changes
            .iter()
            .filter_map(|(of, the, change)| match change {
                Change::Assert(is, pick) if *pick == Pick::Last => {
                    Some((of.clone(), the.clone(), is.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(facts.len(), 2, "{facts:?}");
        for fact in [
            (
                notes.clone(),
                layer_type_attr(),
                Value::Entity("branch:".parse()?),
            ),
            (
                state.clone(),
                layer_type_attr(),
                Value::Entity("session:".parse()?),
            ),
        ] {
            assert!(facts.contains(&fact), "{fact:?} among {facts:?}");
        }
        let mut layers = Layers::default();
        layers.lay(&changes)?;
        assert_eq!(
            layers.layers(),
            vec![
                (notes.clone(), LayerType::Branch),
                (state.clone(), LayerType::Session),
            ]
        );
        let mut retraction = Changes::new();
        LayerDeclaration::new(state, LayerType::Session).retract(&mut retraction);
        layers.lay(&retraction)?;
        assert_eq!(layers.layers(), vec![(notes, LayerType::Branch)]);
        Ok(())
    }

    /// A durable layer is named `memory:<name>`; one declared under
    /// another store, or with no name after `memory:`, is refused with
    /// `InvalidPlacement`.
    #[dialog_common::test]
    fn it_names_a_durable_layer_under_memory() -> anyhow::Result<()> {
        durable_name(&"memory:notes".parse()?)?;
        for refused in ["session:notes", "memory:", "control:notes"] {
            let refused: Entity = refused.parse()?;
            assert!(
                matches!(
                    durable_name(&refused),
                    Err(CommitError::InvalidPlacement { .. })
                ),
                "{refused}"
            );
        }
        Ok(())
    }

    /// Taking the writes into `control:` out of a batch leaves the
    /// writes into the primary and into every other layer in it.
    #[dialog_common::test]
    fn it_takes_the_writes_into_control_out_of_a_batch() -> anyhow::Result<()> {
        let the: Relation = "cmd.ping/target".parse()?;
        let of: Entity = "cmd:1".parse()?;
        let state: Entity = "session:state".parse()?;
        let mut changes = Changes::new();
        changes.associate(the.clone(), of.clone(), Value::Boolean(true), Pick::All);
        changes.associate_in(
            control_layer(),
            the.clone(),
            of.clone(),
            Value::Boolean(false),
            Pick::All,
        );
        changes.associate_in(
            state.clone(),
            the.clone(),
            of.clone(),
            Value::UnsignedInt(1u32.into()),
            Pick::All,
        );
        let dispatched = take_dispatched(&mut changes);
        assert_eq!(dispatched.iter().count(), 1);
        assert!(!dispatched.has_shares());
        assert_eq!(changes.iter().count(), 1, "the primary's write stays");
        let layers: Vec<&Entity> = changes.iter_in().map(|(layer, ..)| layer).collect();
        assert_eq!(layers, vec![&state], "the other layer's write stays");
        Ok(())
    }
}
