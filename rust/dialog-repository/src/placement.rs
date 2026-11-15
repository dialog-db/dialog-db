//! The layers a primary declares beside itself, and the names of the
//! layers every session has.
//!
//! A session reads several layers as one (see [`Session`](crate::Session)):
//! the **primary**, a branch whose schema defines the others, and the
//! layers that schema declares beside it. A layer is declared on the
//! primary and replicated with it:
//!
//! ```text
//! session:state   dialog.layer/store   "memory"
//! ```
//!
//! Where a row lies is a dimension of the row (`in:`), said by the
//! descriptor that reads or writes it (see
//! [`AttributeDescriptor::in_layer`](dialog_query::AttributeDescriptor::in_layer)):
//! a write with no layer named lands in the primary, a read with none
//! reads every layer. A layer's name says its store, and the bare name
//! is that store's default: [`MEMORY`] is the primary, [`SESSION`] the
//! in-memory layer every session has, [`CONTROL`] the loopback a command
//! lands in.
//!
//! A **durable** layer, `memory:<name>` ([`Store::Durable`]), is the
//! repository's branch `<name>`: a session reads it as a line beside the
//! primary and commits what a dispatch writes into it on that branch.
//! The primary records the revision each durable layer stands at as a
//! pointer fact of its own (`memory:<name> dialog.layer/revision
//! <tree>`), written by the dispatch that moved the layer, so the
//! primary's history says which revision of each durable layer goes with
//! each of its own:
//!
//! ```text
//! memory:notes   dialog.layer/store      "durable"
//! memory:notes   dialog.layer/revision   "<the notes branch's tree, base58>"
//! ```
//!
//! [`CONTROL`] holds a row for one tick: what a dispatch writes into it
//! (`.at(control_layer())`, a rule head whose descriptor names
//! `in: control:`, or a command, whose concept carries the
//! `dialog.concept/transient` marker) is the next tick's stimulus, read
//! as a row of `control:`, and is gone after that tick. A **channel**,
//! `control:<name>`, is a layer for a consumer outside the dispatch
//! ([`Store::Control`]): its rows stay until the consumer retracts them.
//!
//! The declarations in force are resolved from three places, each
//! outranking the one before: the primary's tree, the primary's session
//! store, and what the session itself holds
//! ([`Session::declare`](crate::Session::declare)).

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
use crate::{CommitError, Ephemeral, Revision};
use crate::{Erased, select_from_source};
use base58::ToBase58 as _;

// The declaration attribute is parsed once: every session open asks for it.
static STORE_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.layer/store").into());
// The revision and selection attributes are written on every dispatch
// through a session.
static REVISION_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.layer/revision").into());
static SELECTION_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.session/layer").into());

pub use dialog_corpus::{MEMORY, memory_layer};

/// The name of the layer every session mints: in memory, the session's
/// own, gone when the session is.
pub const SESSION: &str = "session:";
/// The name of the loopback: where a command lands, for one tick.
pub const CONTROL: &str = "control:";

// The layer entities are parsed once: a read asks for them per line it
// reads, a commit per head it writes.
static SESSION_LAYER: LazyLock<Entity> =
    LazyLock::new(|| SESSION.parse().expect("a bare scheme is an entity"));
static CONTROL_LAYER: LazyLock<Entity> =
    LazyLock::new(|| CONTROL.parse().expect("a bare scheme is an entity"));

/// The entity of the session's own layer, [`SESSION`].
pub fn session_layer() -> Entity {
    SESSION_LAYER.clone()
}

/// The entity of the loopback, [`CONTROL`].
pub fn control_layer() -> Entity {
    CONTROL_LAYER.clone()
}

/// The `dialog.layer/store` declaration attribute: of a layer's entity,
/// what backs it.
pub(crate) fn store_attr() -> Relation {
    STORE_ATTR.clone()
}

/// The `dialog.layer/revision` attribute: of a layer's entity, the
/// revision the layer stands at, as text. A session writes one row of it
/// per layer into [`SESSION`] after each dispatch publishes.
pub fn revision_attr() -> Relation {
    REVISION_ATTR.clone()
}

/// The `dialog.session/layer` attribute: of [`SESSION`], the layer a
/// write with no layer named lands in while it is set (see
/// [`Session::select`](crate::Session::select)).
pub fn selection_attr() -> Relation {
    SELECTION_ATTR.clone()
}

/// Whether a write of `the` with no layer named follows the session's
/// selected layer: every attribute does but the machinery under
/// `dialog.` (rules, layer declarations, revisions, the selection
/// itself), which lands in the primary or where it names.
pub(crate) fn follows_selection(the: &Relation) -> bool {
    !the.as_str().starts_with("dialog.")
}

/// The layer `own`, a session's own layer, holds as selected, if any:
/// its `dialog.session/layer` row of [`SESSION`].
pub(crate) fn selected(own: &Ephemeral) -> Option<Entity> {
    own.scan(&Selector::new().the(selection_attr()).of(session_layer()))
        .into_iter()
        .find_map(|fact| match fact.is {
            Value::Entity(layer) => Some(layer),
            _ => None,
        })
}

/// [`Statement`] selecting `layer` in a session's own layer: its
/// `dialog.session/layer` row of [`SESSION`], under `last`, written into
/// [`SESSION`]. See [`Session::select`](crate::Session::select).
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

/// The revision a branch stands at, as a layer's revision row and a
/// durable layer's pointer fact carry it: its tree, base58, as
/// `dialog.branch/tree` renders a tree.
pub(crate) fn tree_revision(revision: &Revision) -> Value {
    let tree: &[u8] = revision.tree.hash();
    Value::String(tree.to_base58())
}

/// The revision every layer of a session stands at, as text: of
/// [`MEMORY`] the tree of `primary`'s revision (none for a primary with
/// no revision); of each of `durables` the tree of its branch's (none
/// for one with no revision); of each of `memories` and of [`SESSION`],
/// `own`, the hash of its instant chain. Each is base58, as
/// `dialog.branch/tree` renders a tree.
fn revisions(
    primary: Option<&Revision>,
    durables: &[(Entity, Option<Revision>)],
    memories: &[(Entity, Ephemeral)],
    own: &Ephemeral,
) -> Vec<(Entity, Value)> {
    let chain = |memory: &Ephemeral| Value::String(memory.revision().hash.as_bytes().to_base58());
    let session = session_layer();
    let mut rows = Vec::with_capacity(memories.len() + durables.len() + 2);
    if let Some(revision) = primary {
        rows.push((memory_layer(), tree_revision(revision)));
    }
    for (layer, revision) in durables {
        if let Some(revision) = revision {
            rows.push((layer.clone(), tree_revision(revision)));
        }
    }
    for (layer, memory) in memories {
        if *layer != session {
            rows.push((layer.clone(), chain(memory)));
        }
    }
    rows.push((session, chain(own)));
    rows
}

/// The `dialog.layer/revision` rows of a session's layers as they stand
/// (see [`revisions`]), each under `last`, bound for no layer: what a
/// session records in its own layer once a dispatch has published.
pub(crate) fn revision_rows(
    primary: Option<&Revision>,
    durables: &[(Entity, Option<Revision>)],
    memories: &[(Entity, Ephemeral)],
    own: &Ephemeral,
) -> Changes {
    let mut rows = Changes::new();
    for (layer, revision) in revisions(primary, durables, memories, own) {
        rows.associate(revision_attr(), layer, revision, Pick::Last);
    }
    rows
}

/// The change in the `dialog.layer/revision` rows since `own` last
/// recorded them: for each layer whose revision `own` does not hold,
/// the retraction of the row it holds and the row as it stands now.
/// What a dispatch through a session takes as stimulus, so a rule
/// premised on a layer's revision fires when the layer moved, and on
/// the first dispatch, which finds nothing recorded. The retraction
/// matters: the recorded row stands in `session:` above the dispatch's
/// own rows, and would be elected over the new one.
pub(crate) fn revision_stimulus(
    primary: Option<&Revision>,
    durables: &[(Entity, Option<Revision>)],
    memories: &[(Entity, Ephemeral)],
    own: &Ephemeral,
) -> Changes {
    let mut rows = Changes::new();
    for (layer, revision) in revisions(primary, durables, memories, own) {
        let recorded = own.scan(&Selector::new().the(revision_attr()).of(layer.clone()));
        if recorded.iter().any(|fact| fact.is == revision) {
            continue;
        }
        for fact in recorded {
            rows.dissociate(revision_attr(), layer.clone(), fact.is);
        }
        rows.associate(revision_attr(), layer, revision, Pick::Last);
    }
    rows
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

/// What backs a layer a session builds from the primary's schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Store {
    /// A table in memory: the session mints it when it opens, and it is
    /// gone when the session is.
    Memory,
    /// A channel, `control:<name>`: a table in memory the session mints
    /// when it opens, for a consumer outside the dispatch (the embedder,
    /// another process). What lands in it stays until the consumer
    /// retracts it, so a consumer that was not reading loses nothing:
    /// it reads the layer's instants ([`Ephemeral::since`]) or
    /// [`Session::arrivals`](crate::Session::arrivals), and retracts
    /// what it consumed. Bare [`CONTROL`], the loopback, is every
    /// session's, and declaring it builds nothing.
    Control,
    /// A branch, `memory:<name>`: the repository's branch `<name>`,
    /// which the session opens beside the primary, reads as a line
    /// stamped with the layer's name, and commits what a dispatch writes
    /// into the layer on. No memory cell holds its rows. The primary
    /// records the revision the layer stands at as a pointer fact
    /// (`memory:<name> dialog.layer/revision`), and
    /// [`Session::sync`](crate::Session::sync) pulls and pushes it with
    /// the primary when its branch has upstreams.
    Durable,
}

impl Store {
    /// The text a `dialog.layer/store` fact carries for this store.
    pub fn as_str(&self) -> &'static str {
        match self {
            Store::Memory => "memory",
            Store::Control => "control",
            Store::Durable => "durable",
        }
    }

    /// The store a `dialog.layer/store` fact names, or `None` for a text
    /// that names none.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "memory" => Some(Store::Memory),
            "control" => Some(Store::Control),
            "durable" => Some(Store::Durable),
            _ => None,
        }
    }
}

/// [`Statement`] declaring a layer beside the primary: `layer` is the
/// entity a descriptor's `in:` names, and `store` what backs it. A
/// session opened on the primary builds the layer from this.
///
/// ```no_run
/// # use dialog_repository::{Branch, LayerDeclaration, Store};
/// # fn main() -> anyhow::Result<()> {
/// # let branch: Branch = todo!();
/// let state: dialog_corpus::Entity = "session:state".parse()?;
/// let tx = branch
///     .transaction()
///     .assert(LayerDeclaration::new(state, Store::Memory));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerDeclaration {
    /// The layer.
    pub layer: Entity,
    /// What backs it.
    pub store: Store,
}

impl LayerDeclaration {
    /// Declare `layer`, backed by `store`.
    pub fn new(layer: Entity, store: Store) -> Self {
        Self { layer, store }
    }
}

impl Statement for LayerDeclaration {
    fn assert(self, update: &mut impl Update) {
        update.associate(
            store_attr(),
            self.layer,
            Value::String(self.store.as_str().into()),
            Pick::Last,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(
            store_attr(),
            self.layer,
            Value::String(self.store.as_str().into()),
        );
    }
}

/// [`Statement`] recording, in the primary, the revision a durable layer
/// stands at: its `dialog.layer/revision` pointer fact, under `last`,
/// bound for no layer, so it lands in the primary and never follows a
/// selection. A dispatch that moves a durable layer writes it; so does
/// [`Session::sync`](crate::Session::sync) when a pull moved the layer.
pub(crate) struct Pointer {
    pub(crate) layer: Entity,
    pub(crate) revision: Value,
}

impl Statement for Pointer {
    fn assert(self, update: &mut impl Update) {
        update.associate(revision_attr(), self.layer, self.revision, Pick::Last);
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(revision_attr(), self.layer, self.revision);
    }
}

/// The branch a durable layer is: `<name>` of `memory:<name>`. A layer
/// declared durable under any other name, or with a name that is not a
/// branch name, fails the open.
pub(crate) fn durable_branch(layer: &Entity) -> Result<String, CommitError> {
    let text = layer.to_string();
    let name = text.strip_prefix(MEMORY).unwrap_or_default();
    if name.is_empty() || invalid_name(name).is_some() {
        return Err(CommitError::InvalidPlacement {
            attribute: format!("dialog.layer/store of {layer}"),
            value: "\"durable\" names a layer memory:<branch>".into(),
        });
    }
    Ok(name.to_string())
}

/// Read a layer declaration's value as a store, or fail the commit.
fn store_value(layer: &Entity, value: &Value) -> Result<Store, CommitError> {
    let store = match value {
        Value::String(text) => Store::parse(text),
        _ => None,
    };
    store.ok_or_else(|| CommitError::InvalidPlacement {
        attribute: format!("dialog.layer/store of {layer}"),
        value: format!("{value:?}"),
    })
}

/// The layers declared for a session: the primary's committed
/// declarations, with the ones its session store and the session hold
/// laid over, in that order.
#[derive(Debug, Default)]
pub(crate) struct Layers {
    committed: HashMap<Entity, Store>,
    declared: HashMap<Entity, Store>,
    retracted: HashMap<Entity, Store>,
}

impl Layers {
    /// Resolve the layers declared over `primary`, pinned: its committed
    /// declarations, then those its session store holds, then `held`
    /// (what the session declares), a later one for the same layer
    /// outranking an earlier.
    pub(crate) async fn resolve(
        primary: &Source,
        held: &Changes,
        env: &Erased,
    ) -> Result<Self, CommitError> {
        let mut layers = Layers::default();
        if primary.revision().is_some() {
            for claim in committed(primary, store_selector(), env).await? {
                let store = store_value(&claim.of, &claim.is)?;
                layers.committed.insert(claim.of, store);
            }
        }
        // A declaration held in the primary's session store is this
        // process's own: it never reaches the tree or a peer, outranks
        // the committed one, and yields to what the session holds.
        for claim in primary.overlay().scan(&store_selector()) {
            let store = store_value(&claim.of, &claim.is)?;
            layers.declared.insert(claim.of, store);
        }
        layers.lay(held)?;
        Ok(layers)
    }

    /// Lay the declarations of `changes` over what is resolved so far: a
    /// later one for the same layer outranks an earlier, and a retraction
    /// of what was declared undeclares it.
    fn lay(&mut self, changes: &Changes) -> Result<(), CommitError> {
        let store_attr = store_attr();
        for (entity, the, change) in changes.iter() {
            if *the != store_attr {
                continue;
            }
            let (Change::Assert(value, _) | Change::Retract(value)) = change;
            let store = store_value(entity, value)?;
            match change {
                Change::Retract(_) => {
                    if self.declared.get(entity) == Some(&store) {
                        self.declared.remove(entity);
                    }
                    self.retracted.insert(entity.clone(), store);
                }
                _ => {
                    self.declared.insert(entity.clone(), store);
                }
            }
        }
        Ok(())
    }

    /// Every layer declared, with what backs it, by name.
    pub(crate) fn layers(&self) -> Vec<(Entity, Store)> {
        let mut layers: HashMap<Entity, Store> = self
            .committed
            .iter()
            .filter(|(layer, store)| self.retracted.get(*layer) != Some(*store))
            .map(|(layer, store)| (layer.clone(), *store))
            .collect();
        for (layer, store) in &self.declared {
            layers.insert(layer.clone(), *store);
        }
        let mut layers: Vec<(Entity, Store)> = layers.into_iter().collect();
        layers.sort_by_key(|a| a.0.to_string());
        layers
    }
}

/// The pointer facts `primary`'s tree holds, by layer: the revision it
/// records each durable layer at, as the cell's elected claim.
pub(crate) async fn pointers(
    primary: &Source,
    env: &Erased,
) -> Result<Vec<(Entity, Value)>, CommitError> {
    if primary.revision().is_none() {
        return Ok(Vec::new());
    }
    Ok(
        committed(primary, Selector::new().the(revision_attr()), env)
            .await?
            .into_iter()
            .map(|fact| (fact.of, fact.is))
            .collect(),
    )
}

/// Selector for every committed `dialog.layer/store` declaration.
fn store_selector() -> Selector<Constrained> {
    Selector::new().the(store_attr())
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
    fn it_names_the_layers_every_session_has() {
        assert_eq!(memory_layer().to_string(), MEMORY);
        assert_eq!(session_layer().to_string(), SESSION);
        assert_eq!(control_layer().to_string(), CONTROL);
    }

    /// A store names itself as its declaration's text, and that text
    /// names it back.
    #[dialog_common::test]
    fn it_reads_back_every_store_it_names() {
        for store in [Store::Memory, Store::Control, Store::Durable] {
            assert_eq!(Store::parse(store.as_str()), Some(store));
        }
        assert_eq!(Store::parse("disk"), None);
    }

    /// A durable layer is the branch its name gives after `memory:`; a
    /// durable layer named under another store, or with no branch name,
    /// is refused with `InvalidPlacement`.
    #[dialog_common::test]
    fn it_names_the_branch_a_durable_layer_is() -> anyhow::Result<()> {
        assert_eq!(durable_branch(&"memory:notes".parse()?)?, "notes");
        for refused in ["session:notes", "memory:", "control:notes"] {
            let refused: Entity = refused.parse()?;
            assert!(
                matches!(
                    durable_branch(&refused),
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
