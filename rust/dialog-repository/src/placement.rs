//! Placement: the layer an attribute's facts live in.
//!
//! A session reads several layers as one (see [`Session`](crate::Session)):
//! the **primary**, a branch whose schema defines the others, and the
//! layers that schema declares beside it. Where an attribute's facts live
//! is a property of the attribute, declared on the primary and replicated
//! with it:
//!
//! ```text
//! attribute:ui/selected   dialog.attribute/in   layer:session
//! layer:session           dialog.layer/store    "memory"
//! ```
//!
//! An attribute is placed on exactly one layer. With no placement it
//! lives on the primary, the way a query with no db named reads the
//! current one. The layer is part of the attribute's identity: the same
//! name placed on two layers is two attributes, which a rule joins or
//! chooses between.
//!
//! A write routes by placement, so a session itself never puts a fact in
//! a layer outside its placement. A fact can still arrive there another
//! way: a commit to the primary that does not go through a session, or a
//! pull from a peer that holds no such placement. Such a fact is a
//! **divergence of placement**: no read of the session takes it, and the
//! session reports it once (see [`guide`]).
//!
//! The declarations in force are resolved from three places, each
//! outranking the one before: the primary's tree, the primary's session
//! store, and what the session itself holds
//! ([`Session::declare`](crate::Session::declare)). A transaction's own
//! declarations outrank all three for its commit.

pub(crate) mod guide;

use std::collections::HashMap;
use std::sync::LazyLock;

use dialog_corpus::selector::Constrained;
use dialog_corpus::{
    Change, Changes, DialogCorpusError, Entity, Fact, FactView, Pick, Relation, Selector,
    Statement, Update, Value,
};
use dialog_query::the;
use futures_util::TryStreamExt as _;

use crate::CommitError;
use crate::repository::source::Source;
use crate::{Erased, select_from_source};

// Each declaration attribute is parsed once: every session read and
// every session commit asks for them.
static IN_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.attribute/in").into());
static STORE_ATTR: LazyLock<Relation> = LazyLock::new(|| the!("dialog.layer/store").into());

/// The `dialog.attribute/in` declaration attribute: of an attribute's
/// entity, the layer its facts live in.
pub(crate) fn in_attr() -> Relation {
    IN_ATTR.clone()
}

/// The `dialog.layer/store` declaration attribute: of a layer's entity,
/// what backs it.
pub(crate) fn store_attr() -> Relation {
    STORE_ATTR.clone()
}

/// Every attribute a placement declaration is written under.
pub(crate) fn declaration_attrs() -> [Relation; 2] {
    [in_attr(), store_attr()]
}

/// The scheme of the entity standing for an attribute in a declaration.
const ATTRIBUTE_SCHEME: &str = "attribute:";

/// The entity standing for `attribute` in a placement declaration:
/// `attribute:<name>`.
pub fn attribute_entity(attribute: &Relation) -> Entity {
    format!("{ATTRIBUTE_SCHEME}{attribute}")
        .parse()
        .expect("an attribute name is a valid opaque URI path")
}

/// The attribute an [`attribute_entity`] stands for, if the entity is
/// one.
fn entity_attribute(entity: &Entity) -> Option<Relation> {
    entity
        .to_string()
        .strip_prefix(ATTRIBUTE_SCHEME)?
        .parse()
        .ok()
}

/// [`Statement`] placing an attribute on a layer: its facts live there,
/// a session reads them from there alone, and a session commit routes a
/// write of them there. Retracting it returns the attribute to the
/// primary.
///
/// ```no_run
/// # use dialog_repository::{Branch, Placement};
/// # fn main() -> anyhow::Result<()> {
/// # let branch: Branch = todo!();
/// let session: dialog_corpus::Entity = "layer:session".parse()?;
/// let tx = branch
///     .transaction()
///     .assert(Placement::new("ui/selected".parse()?, session));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The attribute placed.
    pub attribute: Relation,
    /// The layer its facts live in.
    pub layer: Entity,
}

impl Placement {
    /// Declare that `attribute`'s facts live in `layer`.
    pub fn new(attribute: Relation, layer: Entity) -> Self {
        Self { attribute, layer }
    }
}

impl Statement for Placement {
    fn assert(self, update: &mut impl Update) {
        update.associate(
            in_attr(),
            attribute_entity(&self.attribute),
            Value::Entity(self.layer),
            Pick::Last,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(
            in_attr(),
            attribute_entity(&self.attribute),
            Value::Entity(self.layer),
        );
    }
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
/// entity a [`Placement`] names, and `store` what backs it. A session
/// opened on the primary builds the layer from this.
///
/// ```no_run
/// # use dialog_repository::{Branch, LayerDeclaration, Placement, Store};
/// # fn main() -> anyhow::Result<()> {
/// # let branch: Branch = todo!();
/// let session: dialog_corpus::Entity = "layer:session".parse()?;
/// let tx = branch
///     .transaction()
///     .assert(LayerDeclaration::new(session.clone(), Store::Memory))
///     .assert(Placement::new("ui/selected".parse()?, session));
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

/// Read a placement's value as a layer entity, or fail the commit.
fn layer_value(attribute: &str, value: &Value) -> Result<Entity, CommitError> {
    match value {
        Value::Entity(layer) => Ok(layer.clone()),
        other => Err(CommitError::InvalidPlacement {
            attribute: attribute.to_string(),
            value: format!("{other:?}"),
        }),
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

/// The placements in force for a read or a commit of a session: the
/// primary's committed declarations, with the ones its session store,
/// the session and the transaction hold laid over, in that order.
#[derive(Debug, Default)]
pub(crate) struct Placements {
    committed: HashMap<Relation, Entity>,
    declared: HashMap<Relation, Entity>,
    retracted: HashMap<Relation, Entity>,
    committed_layers: HashMap<Entity, Store>,
    layers: HashMap<Entity, Store>,
    layers_retracted: HashMap<Entity, Store>,
}

impl Placements {
    /// Resolve the placements in force over `primary`, pinned: its
    /// committed declarations, then those its session store holds, then
    /// `held` (what the session declares), then `changes` (what the
    /// transaction declares), a later one for the same attribute or
    /// layer outranking an earlier.
    pub(crate) async fn resolve(
        primary: &Source,
        held: &Changes,
        changes: &Changes,
        env: &Erased,
    ) -> Result<Self, CommitError> {
        let mut placements = Placements::default();
        if primary.revision().is_some() {
            for claim in committed(primary, in_selector(), env).await? {
                if let Some(attribute) = entity_attribute(&claim.of) {
                    let layer = layer_value(attribute.as_str(), &claim.is)?;
                    placements.committed.insert(attribute, layer);
                }
            }
            for claim in committed(primary, store_selector(), env).await? {
                let store = store_value(&claim.of, &claim.is)?;
                placements.committed_layers.insert(claim.of, store);
            }
        }
        // A declaration held in the primary's session store is this
        // process's own: it never reaches the tree or a peer, outranks
        // the committed one, and yields to what the session and the
        // transaction hold.
        let store = primary.overlay();
        for claim in store.scan(&in_selector()) {
            if let Some(attribute) = entity_attribute(&claim.of) {
                let layer = layer_value(attribute.as_str(), &claim.is)?;
                placements.declared.insert(attribute, layer);
            }
        }
        for claim in store.scan(&store_selector()) {
            let backing = store_value(&claim.of, &claim.is)?;
            placements.layers.insert(claim.of, backing);
        }
        placements.lay(held)?;
        placements.lay(changes)?;
        Ok(placements)
    }

    /// Lay the declarations of `changes` over what is resolved so far: a
    /// later one for the same attribute or layer outranks an earlier, and
    /// a retraction of what was declared undeclares it.
    fn lay(&mut self, changes: &Changes) -> Result<(), CommitError> {
        let in_attr = in_attr();
        let store_attr = store_attr();
        for (entity, the, change) in changes.iter() {
            if *the == in_attr {
                let Some(attribute) = entity_attribute(entity) else {
                    continue;
                };
                let (Change::Assert(value, _) | Change::Retract(value)) = change;
                let layer = layer_value(attribute.as_str(), value)?;
                match change {
                    Change::Retract(_) => {
                        if self.declared.get(&attribute) == Some(&layer) {
                            self.declared.remove(&attribute);
                        }
                        self.retracted.insert(attribute, layer);
                    }
                    _ => {
                        self.declared.insert(attribute, layer);
                    }
                }
            } else if *the == store_attr {
                let (Change::Assert(value, _) | Change::Retract(value)) = change;
                let store = store_value(entity, value)?;
                match change {
                    Change::Retract(_) => {
                        if self.layers.get(entity) == Some(&store) {
                            self.layers.remove(entity);
                        }
                        self.layers_retracted.insert(entity.clone(), store);
                    }
                    _ => {
                        self.layers.insert(entity.clone(), store);
                    }
                }
            }
        }
        Ok(())
    }

    /// The layer `attribute` is placed on, or `None` for the primary.
    pub(crate) fn layer_of(&self, attribute: &Relation) -> Option<&Entity> {
        if let Some(layer) = self.declared.get(attribute) {
            return Some(layer);
        }
        match self.committed.get(attribute) {
            Some(layer) if self.retracted.get(attribute) != Some(layer) => Some(layer),
            _ => None,
        }
    }

    /// Every layer declared, with what backs it, in no particular order.
    pub(crate) fn layers(&self) -> Vec<(Entity, Store)> {
        let mut layers: HashMap<Entity, Store> = self
            .committed_layers
            .iter()
            .filter(|(layer, store)| self.layers_retracted.get(*layer) != Some(*store))
            .map(|(layer, store)| (layer.clone(), *store))
            .collect();
        for (layer, store) in &self.layers {
            layers.insert(layer.clone(), *store);
        }
        let mut layers: Vec<(Entity, Store)> = layers.into_iter().collect();
        layers.sort_by_key(|a| a.0.to_string());
        layers
    }

    /// The placements in force, as a [`PlacementMap`].
    pub(crate) fn map(&self) -> PlacementMap {
        let mut layers: HashMap<String, Entity> = self
            .committed
            .iter()
            .filter(|(attribute, layer)| self.retracted.get(*attribute) != Some(*layer))
            .map(|(attribute, layer)| (attribute.to_string(), layer.clone()))
            .collect();
        for (attribute, layer) in &self.declared {
            layers.insert(attribute.to_string(), layer.clone());
        }
        PlacementMap { layers }
    }
}

/// The layer of each placed attribute, by the name of the attribute:
/// what a read guided by placement asks of each row it meets. A row
/// carries its attribute as bytes, so the map is keyed by name and a
/// read parses no attribute.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct PlacementMap {
    layers: HashMap<String, Entity>,
}

impl PlacementMap {
    /// The layer `attribute` is placed on, or `None` for the primary.
    pub(crate) fn layer_of(&self, attribute: &str) -> Option<&Entity> {
        self.layers.get(attribute)
    }
}

/// The placement declarations `store` holds, as its
/// [`export`](crate::Ephemeral::export) carries them.
pub(crate) fn declarations_held(store: &crate::Ephemeral) -> Changes {
    store.export_of(&declaration_attrs())
}

/// The pending placement declarations of a read: each batch of `held`
/// (what the session holds, then what a query asserts, in that order,
/// so a later one outranks an earlier) and those of each staged layer
/// after them, folded in order as the read sees them. It is all that
/// [`Placements::resolve`] reads of the pending writes, at the cost of
/// the declarations alone rather than of everything the layers hold.
pub(crate) fn pending_declarations(
    held: impl IntoIterator<Item = Changes>,
    layers: &[crate::Staged],
) -> Changes {
    let attrs = declaration_attrs();
    let mut pending = crate::Staged::default();
    for held in held {
        pending.apply(held);
    }
    for layer in layers {
        pending.apply(layer.export_cells(|the| attrs.contains(the)));
    }
    pending.export()
}

/// Selector for every committed `dialog.attribute/in` declaration.
fn in_selector() -> Selector<Constrained> {
    Selector::new().the(in_attr())
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
        .map_err(|error| CommitError::Induction(format!("placement scan: {error}")))?
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("placement scan: {error}")))?;
    elected(views).map_err(|error| CommitError::Induction(format!("placement scan: {error}")))
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

    use std::sync::Arc;

    use super::guide::{Divergences, Guide, GuidedLayer};
    use super::*;
    use crate::helpers::test_repo;
    use crate::schema::DidExt as _;
    use crate::{Branch, Ephemeral, RemoteSite};
    use anyhow::Result;
    use dialog_capability::{Fork, Provider};
    use dialog_common::{ConditionalSync, Holds};
    use dialog_corpus::tree::CorpusNode;
    use dialog_corpus::{Preload, Speculation};
    use dialog_effects::archive::Load as ArchiveLoad;
    use dialog_effects::archive::{Get, Put};
    use dialog_effects::authority::Identify;
    use dialog_effects::blob::{Load as BlobLoad, Read as BlobRead};
    use dialog_effects::memory::Resolve;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Relation as QueryRelation;
    use dialog_query::query::Output as _;
    use dialog_query::{AttributeQuery, Term};
    use dialog_search_tree::nodes;

    /// What a read of a query layer needs of its environment.
    trait ReadEnv:
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
        + Provider<Preload>
        + Provider<Speculation>
        + Provider<Fork<RemoteSite, Resolve>>
        + Holds
        + ConditionalSync
        + 'static
    {
    }
    impl<T> ReadEnv for T where
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
            + Provider<Preload>
            + Provider<Speculation>
            + Provider<Fork<RemoteSite, Resolve>>
            + Holds
            + ConditionalSync
            + 'static
    {
    }

    /// How [`read`] renders a text value.
    fn text(value: &str) -> String {
        format!("{:?}", Value::String(value.into()))
    }

    fn layer(name: &str) -> Entity {
        format!("layer:{name}").parse().expect("a layer entity")
    }

    /// The `(entity, value)` pairs a read of `the` through `query`
    /// returns, as text, sorted.
    async fn read(
        query: crate::QueryLayer<'_>,
        operator: &impl ReadEnv,
        the: &str,
    ) -> Result<Vec<(String, String)>> {
        let rows = query
            .select(AttributeQuery::from(
                Term::<QueryRelation>::from(QueryRelation::from(
                    the.parse::<dialog_corpus::Relation>()?,
                ))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
            ))
            .perform(operator)
            .try_vec()
            .await?;
        let mut rows: Vec<(String, String)> = rows
            .into_iter()
            .map(|row| (row.of.to_string(), format!("{:?}", row.is)))
            .collect();
        rows.sort();
        Ok(rows)
    }

    /// A guide over `primary` and one memory layer named `memory`.
    fn guide(primary: &Branch, memory: &Entity) -> Arc<Guide> {
        Arc::new(Guide {
            lines: vec![GuidedLayer {
                entity: primary.subject().did().this(),
                primary: true,
            }],
            memories: vec![GuidedLayer {
                entity: memory.clone(),
                primary: false,
            }],
            divergences: Divergences::default(),
            declared: Changes::new(),
        })
    }

    /// A read guided by placement takes each attribute from the layer it
    /// is placed on alone: `ui/selected`, placed on the memory layer, is
    /// read from it and not from the tree; `doc/title`, placed nowhere,
    /// is read from the primary and not from the memory layer. The rows
    /// skipped are reported once each as divergences. The same read with
    /// no guide takes every row.
    #[dialog_common::test]
    async fn it_reads_each_attribute_from_the_layer_it_is_placed_on() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let session = layer("session");
        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(session.clone(), Store::Memory))
            .assert(Placement::new("ui/selected".parse()?, session.clone()))
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            // Misplaced: `ui/selected` lives on the memory layer.
            .assert(
                dialog_query::the!("ui/selected")
                    .of(doc.clone())
                    .is("tree".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let memory = Ephemeral::new();
        memory.assert(
            dialog_query::the!("ui/selected")
                .of(doc.clone())
                .is("memory".to_string()),
        )?;
        // Misplaced: `doc/title` lives on the primary.
        memory.assert(
            dialog_query::the!("doc/title")
                .of(doc.clone())
                .is("Memory".to_string()),
        )?;

        let guide = guide(&branch, &session);
        let guided = || branch.query().with_memory(&memory).guided(guide.clone());
        assert_eq!(
            read(guided(), &operator, "ui/selected").await?,
            vec![(doc.to_string(), text("memory"))],
            "a placed attribute is read from its layer alone"
        );
        assert_eq!(
            read(guided(), &operator, "doc/title").await?,
            vec![(doc.to_string(), text("Notes"))],
            "an attribute placed nowhere is read from the primary alone"
        );
        let mut divergences: Vec<(String, bool)> = guide
            .divergences
            .take()
            .into_iter()
            .map(|divergence| (divergence.attribute, divergence.layer == session))
            .collect();
        divergences.sort();
        assert_eq!(
            divergences,
            vec![
                ("doc/title".to_string(), true),
                ("ui/selected".to_string(), false)
            ],
            "each skipped row is reported once, with the layer that holds it"
        );
        assert!(guide.divergences.take().is_empty(), "and only once");

        let unguided = || branch.query().with_memory(&memory);
        assert_eq!(
            read(unguided(), &operator, "ui/selected").await?,
            vec![
                (doc.to_string(), text("memory")),
                (doc.to_string(), text("tree")),
            ],
            "a read with no guide takes every row"
        );
        Ok(())
    }
}
