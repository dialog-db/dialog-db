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

/// The name of the primary: the base durable branch of a session.
pub const MEMORY: &str = "memory:";
/// The name of the layer every session mints: in memory, the session's
/// own, gone when the session is.
pub const SESSION: &str = "session:";
/// The name of the loopback: where a command lands, for one tick.
pub const CONTROL: &str = "control:";

/// The entity of the primary, [`MEMORY`].
pub fn memory_layer() -> Entity {
    MEMORY.parse().expect("a bare scheme is an entity")
}

/// The entity of the session's own layer, [`SESSION`].
pub fn session_layer() -> Entity {
    SESSION.parse().expect("a bare scheme is an entity")
}

/// The entity of the loopback, [`CONTROL`].
pub fn control_layer() -> Entity {
    CONTROL.parse().expect("a bare scheme is an entity")
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

/// The `dialog.layer/revision` rows of a session's layers as they stand:
/// of [`MEMORY`] the tree of `primary`'s revision, base58 as
/// `dialog.branch/tree` renders it (none for a primary with no
/// revision); of each of `memories` and of [`SESSION`], `own`, the hash
/// of its instant chain, in hex. Each under `last`, bound for no layer.
pub(crate) fn revision_rows(
    primary: Option<&Revision>,
    memories: &[(Entity, Ephemeral)],
    own: &Ephemeral,
) -> Changes {
    let hex = |memory: &Ephemeral| {
        let hash = memory.revision().hash;
        let mut text = String::with_capacity(64);
        for byte in hash.as_bytes() {
            text.push_str(&format!("{byte:02x}"));
        }
        Value::String(text)
    };
    let mut rows = Changes::new();
    if let Some(revision) = primary {
        let tree: &[u8] = revision.tree.hash();
        rows.associate(
            revision_attr(),
            memory_layer(),
            Value::String(tree.to_base58()),
            Pick::Last,
        );
    }
    let session = session_layer();
    for (layer, memory) in memories {
        if *layer != session {
            rows.associate(revision_attr(), layer.clone(), hex(memory), Pick::Last);
        }
    }
    rows.associate(revision_attr(), session, hex(own), Pick::Last);
    rows
}

/// What backs a layer a session builds from the primary's schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Store {
    /// A table in memory: the session mints it when it opens, and it is
    /// gone when the session is.
    Memory,
}

impl Store {
    /// The text a `dialog.layer/store` fact carries for this store.
    pub fn as_str(&self) -> &'static str {
        match self {
            Store::Memory => "memory",
        }
    }

    /// The store a `dialog.layer/store` fact names, or `None` for a text
    /// that names none.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "memory" => Some(Store::Memory),
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

/// Read a layer declaration's value as a store, or fail the commit.
fn store_value(layer: &Entity, value: &Value) -> Result<Store, CommitError> {
    match value {
        Value::String(text) if let Some(store) = Store::parse(text) => Ok(store),
        other => Err(CommitError::InvalidPlacement {
            attribute: format!("dialog.layer/store of {layer}"),
            value: format!("{other:?}"),
        }),
    }
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
}
