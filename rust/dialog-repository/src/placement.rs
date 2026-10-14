//! Attribute placement: which scope an attribute's facts live in.
//!
//! A layer is read as one composite, but its facts live in scopes that
//! differ in whether they survive a restart and whether they
//! replicate. Rather than having every writer pick a scope per call,
//! the *attribute* declares its scope once, as a fact on the layer, and
//! every write routes by it: a transaction's `assert` / `retract`
//! lands each instruction in the scope its attribute declares, a
//! rule's head does the same, and a concept whose attributes span
//! scopes fans out on write and joins back on read.
//!
//! # Scopes are names
//!
//! A scope is an entity, conventionally under a `memory:` scheme
//! (`memory:space`, `memory:session`), and nothing about the name is
//! fixed. What is *shared* — which attribute goes to which name — is
//! declared in the replicated tree. What is *local* — which store
//! stands under a name on this replica — is a [binding](Bindings)
//! made on the layer by API. Today a layer can bind a name to its tree
//! or to its [`Ephemeral`](crate::Ephemeral) store; a stack of layers
//! will bind names to other layers.
//!
//! # Declarations, as facts on the layer
//!
//! ```text
//! <repository did>          dialog.attribute/default    memory:space    # the implicit scope
//! attribute:ui/selected     dialog.attribute/scope      memory:session  # an override
//! attribute:cmd.rename/name dialog.attribute/transient  true            # lives one round
//! ```
//!
//! The default names the scope an attribute with no placement belongs
//! to, and it is the tree's name: the tree is where the declarations
//! themselves live, so it is always bound. With no default declared,
//! undeclared attributes go to the tree as before. A placement is a
//! branch-level fact, deliberately outside any concept's content
//! address, so the same descriptor may be session-scoped on one layer
//! and durable on another; it takes effect in the commit that declares
//! it, so a transaction can declare and use a placement together.
//!
//! A write naming a scope the layer does not bind fails the commit
//! ([`CommitError::UnboundScope`]) rather than routing elsewhere. The
//! fix is in the binding, which is local, not in the schema.
//!
//! # Transient attributes
//!
//! An attribute declared transient ([`TransientAttribute`]) never
//! settles anywhere: a fact asserted under it is a command. At commit
//! it moves into the transaction's transient bucket before induction —
//! so `assert` and `dispatch` say the same thing for it — and a rule
//! whose conclusion is transient attributes alone has a transient
//! head, exactly as if its concept carried the
//! [`Transient`](crate::Transient) marker. Each round's transients are
//! witnessed on the store of the scope they are placed under, so an
//! observer of that store sees them.
//!
//! # Observability
//!
//! A scope bound to the ephemeral store is folded into every read of
//! the layer and every standing subscription maintains from its
//! instants. So a rule concluding a session-scoped head writes
//! something a subscriber sees; a transient conclusion is seen only by
//! the store's observers, for the one round it lives.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    ArtifactSelector, ArtifactView, Change, Changes, DialogArtifactsError, Entity, Instruction,
    Pick, Relation, Statement, Update, Value,
};
use dialog_query::the;
use futures_util::TryStreamExt as _;
use parking_lot::RwLock;

use crate::CommitError;
use crate::repository::branch::session::{Erased, select_from_source};
use crate::repository::source::SourceRef;

/// The `dialog.attribute/scope` declaration attribute.
pub(crate) fn scope_attr() -> Relation {
    the!("dialog.attribute/scope").into()
}

/// The `dialog.attribute/default` declaration attribute.
pub(crate) fn default_attr() -> Relation {
    the!("dialog.attribute/default").into()
}

/// The `dialog.attribute/transient` declaration attribute.
pub(crate) fn transient_placement_attr() -> Relation {
    the!("dialog.attribute/transient").into()
}

/// The URI scheme attribute entities are minted under.
const ATTRIBUTE_SCHEME: &str = "attribute:";

/// The entity that stands for an attribute in facts *about* the
/// attribute: `attribute:<namespace>/<name>`.
pub fn attribute_entity(attribute: &Relation) -> Entity {
    format!("{ATTRIBUTE_SCHEME}{attribute}")
        .parse()
        .expect("an attribute name is a valid opaque URI path")
}

/// The attribute an [`attribute_entity`] stands for, if the entity
/// is one.
fn entity_attribute(entity: &Entity) -> Option<Relation> {
    entity
        .to_string()
        .strip_prefix(ATTRIBUTE_SCHEME)?
        .parse()
        .ok()
}

/// [`Statement`] declaring the scope an attribute's facts live in.
/// Asserting it places the attribute; retracting it returns the
/// attribute to the layer's default scope.
///
/// ```no_run
/// # use dialog_repository::{Branch, Placement};
/// # use dialog_repository::placement::Target;
/// # async fn example(branch: &Branch) -> anyhow::Result<()> {
/// let session: dialog_artifacts::Entity = "memory:session".parse()?;
/// branch.bind(session.clone(), Target::Session);
/// let tx = branch
///     .transaction()
///     .assert(Placement::new("ui/selected".parse()?, session));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The attribute being placed.
    pub attribute: Relation,
    /// The scope its facts live in.
    pub scope: Entity,
}

impl Placement {
    /// Declare that `attribute`'s facts live in `scope`.
    pub fn new(attribute: Relation, scope: Entity) -> Self {
        Self { attribute, scope }
    }
}

impl Statement for Placement {
    fn assert(self, update: &mut impl Update) {
        update.associate(
            scope_attr(),
            attribute_entity(&self.attribute),
            Value::Entity(self.scope),
            Pick::Last,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(
            scope_attr(),
            attribute_entity(&self.attribute),
            Value::Entity(self.scope),
        );
    }
}

/// [`Statement`] declaring an attribute transient: a fact asserted
/// under it lives for one induction round and is never held, so
/// asserting it is dispatching a command. Retracting the declaration
/// makes the attribute ordinary again.
///
/// ```no_run
/// # use dialog_repository::{Branch, TransientAttribute};
/// # async fn example(branch: &Branch) -> anyhow::Result<()> {
/// let tx = branch
///     .transaction()
///     .assert(TransientAttribute::new("cmd.rename/name".parse()?));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransientAttribute {
    /// The attribute declared transient.
    pub attribute: Relation,
}

impl TransientAttribute {
    /// Declare that facts under `attribute` live one round.
    pub fn new(attribute: Relation) -> Self {
        Self { attribute }
    }
}

impl Statement for TransientAttribute {
    fn assert(self, update: &mut impl Update) {
        update.associate(
            transient_placement_attr(),
            attribute_entity(&self.attribute),
            Value::Boolean(true),
            Pick::Last,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(
            transient_placement_attr(),
            attribute_entity(&self.attribute),
            Value::Boolean(true),
        );
    }
}

/// [`Statement`] declaring the scope an attribute with no placement
/// belongs to, for one repository: the name of the tree.
///
/// ```no_run
/// # use dialog_repository::{Branch, DefaultScope};
/// # async fn example(branch: &Branch) -> anyhow::Result<()> {
/// let tx = branch
///     .transaction()
///     .assert(DefaultScope::new(branch.of(), "memory:shared".parse()?));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultScope {
    /// The repository the default is for.
    pub repository: Entity,
    /// The scope undeclared attributes belong to.
    pub scope: Entity,
}

impl DefaultScope {
    /// Declare `scope` as the default for `repository`'s attributes.
    pub fn new(repository: &dialog_capability::Did, scope: Entity) -> Self {
        use crate::schema::DidExt as _;
        Self {
            repository: repository.this(),
            scope,
        }
    }
}

impl Statement for DefaultScope {
    fn assert(self, update: &mut impl Update) {
        update.associate(
            default_attr(),
            self.repository,
            Value::Entity(self.scope),
            Pick::Last,
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(default_attr(), self.repository, Value::Entity(self.scope));
    }
}

/// Where a scope name is bound on a layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// The layer's tree: durable, replicated with the layer.
    Tree,
    /// The layer's [`Ephemeral`](crate::Ephemeral) store: this process
    /// only, never committed.
    Session,
}

/// A layer's local bindings from scope names to its stores. Shared
/// across clones of the layer, like its caches. The tree needs no
/// binding: the repository default names it, and with no default
/// declared undeclared attributes reach it anyway. Every other name a
/// placement can target must be bound here before a write names it.
#[derive(Debug, Clone, Default)]
pub struct Bindings {
    targets: Arc<RwLock<HashMap<Entity, Target>>>,
}

impl Bindings {
    /// Bind `scope` to `target` on this layer, replacing any prior
    /// binding of the name.
    pub fn bind(&self, scope: Entity, target: Target) {
        self.targets.write().insert(scope, target);
    }

    /// Drop the binding of `scope`, if any.
    pub fn unbind(&self, scope: &Entity) -> bool {
        self.targets.write().remove(scope).is_some()
    }

    /// Where `scope` is bound, if it is.
    pub fn target(&self, scope: &Entity) -> Option<Target> {
        self.targets.read().get(scope).copied()
    }
}

/// The committed placements at a layer head: the scope each declared
/// attribute belongs to, and the repository default. Cached per head
/// on the layer's [`RuleCache`](crate::RuleCache), like the trigger
/// footprint.
#[derive(Debug, Default)]
pub(crate) struct Declared {
    attributes: HashMap<Relation, Entity>,
    default: Option<Entity>,
    transient: HashSet<Relation>,
}

/// A shared handle to the committed declarations at a head.
pub(crate) type CommittedPlacements = Arc<Declared>;

/// The resolved placements a commit routes by: the committed slice at
/// the head, with the transaction's own declarations (and
/// retractions) laid over it.
#[derive(Debug, Default)]
pub(crate) struct Placements {
    committed: CommittedPlacements,
    declared: HashMap<Relation, Entity>,
    retracted: HashMap<Relation, Entity>,
    default: Option<Entity>,
    default_retracted: Option<Entity>,
    transient: HashSet<Relation>,
    untransient: HashSet<Relation>,
}

/// Read a declaration value as a scope entity, or fail the commit.
fn scope_value(attribute: &str, value: &Value) -> Result<Entity, CommitError> {
    match value {
        Value::Entity(scope) => Ok(scope.clone()),
        other => Err(CommitError::InvalidPlacement {
            attribute: attribute.to_string(),
            value: format!("{other:?}"),
        }),
    }
}

impl Placements {
    /// Resolve the placements in force for a commit on `source` whose
    /// settled batch is `changes`.
    pub(crate) async fn resolve(
        source: SourceRef<'_>,
        changes: &Changes,
        env: &Erased,
    ) -> Result<Self, CommitError> {
        let committed = committed_placements(source, env).await?;
        let mut placements = Placements {
            committed,
            ..Placements::default()
        };
        // A declaration held in the layer's session store is this
        // process's own: a writer that knows where its facts belong
        // declares the attribute there and the declaration never reaches
        // the tree or a peer. It outranks the committed one and yields
        // to one the batch itself carries.
        let store = source.overlay();
        for claim in store.scan(&declarations_selector()) {
            if let Some(attribute) = entity_attribute(&claim.of) {
                let target = scope_value(attribute.as_str(), &claim.is)?;
                placements.declared.insert(attribute, target);
            }
        }
        // A default that the session store declares outranks the committed
        // one, as a placement there does: a writer of the tree cannot move
        // the attributes of this process by declaring another default.
        use crate::schema::DidExt as _;
        let repository = source.subject().did().this();
        for claim in store.scan(&defaults_selector()) {
            if claim.of == repository {
                placements.default = Some(scope_value("dialog.attribute/default", &claim.is)?);
            }
        }
        for claim in store.scan(&transients_selector()) {
            if let (Some(attribute), Value::Boolean(true)) =
                (entity_attribute(&claim.of), &claim.is)
            {
                placements.transient.insert(attribute);
            }
        }
        let scope = scope_attr();
        let default = default_attr();
        let transient = transient_placement_attr();
        for (entity, the, change) in changes.iter() {
            if *the == transient {
                let Some(placed) = entity_attribute(entity) else {
                    continue;
                };
                match change {
                    Change::Retract(_) => placements.untransient.insert(placed),
                    _ => placements.transient.insert(placed),
                };
            } else if *the == scope {
                let Some(placed) = entity_attribute(entity) else {
                    continue;
                };
                let (Change::Assert(value, _) | Change::Retract(value)) = change;
                let target = scope_value(placed.as_str(), value)?;
                match change {
                    Change::Retract(_) => placements.retracted.insert(placed, target),
                    _ => placements.declared.insert(placed, target),
                };
            } else if *the == default {
                let (Change::Assert(value, _) | Change::Retract(value)) = change;
                let target = scope_value("dialog.attribute/default", value)?;
                match change {
                    Change::Retract(_) => placements.default_retracted = Some(target),
                    _ => placements.default = Some(target),
                }
            }
        }
        Ok(placements)
    }

    /// The scope undeclared attributes belong to, if one is declared.
    pub(crate) fn default_scope(&self) -> Option<&Entity> {
        if let Some(scope) = &self.default {
            return Some(scope);
        }
        match &self.committed.default {
            Some(scope) if self.default_retracted.as_ref() != Some(scope) => Some(scope),
            _ => None,
        }
    }

    /// Whether facts under `attribute` live one round: declared
    /// transient, in this commit or at the head, and not undeclared in
    /// this commit.
    pub(crate) fn is_transient(&self, attribute: &Relation) -> bool {
        if self.transient.contains(attribute) {
            return true;
        }
        self.committed.transient.contains(attribute) && !self.untransient.contains(attribute)
    }

    /// The scope `attribute`'s facts live in, or `None` for the tree
    /// when nothing names it.
    pub(crate) fn scope_of(&self, attribute: &Relation) -> Option<&Entity> {
        self.placement_of(attribute)
            .or_else(|| self.default_scope())
    }

    /// The scope that a placement of `attribute` names, if one does: no
    /// default scope stands in.
    pub(crate) fn placement_of(&self, attribute: &Relation) -> Option<&Entity> {
        if let Some(scope) = self.declared.get(attribute) {
            return Some(scope);
        }
        match self.committed.attributes.get(attribute) {
            Some(scope) if self.retracted.get(attribute) != Some(scope) => Some(scope),
            _ => None,
        }
    }

    /// Split a settled batch by destination store: the instructions
    /// for the tree and the ones for the session store. Declarations
    /// themselves are tree facts. An instruction bound for a scope the
    /// layer does not bind fails the whole batch.
    /// Whether a write of `attribute` lands in the tree of the layer, as
    /// [`partition`](Self::partition) sends it, and not in its session
    /// store. A scope that `bindings` does not bind fails the partition;
    /// until then it counts as the tree.
    pub(crate) fn lands_in_tree(&self, attribute: &Relation, bindings: &Bindings) -> bool {
        match self.scope_of(attribute) {
            None => true,
            Some(scope) if Some(scope) == self.default_scope() => true,
            Some(scope) => bindings.target(scope) != Some(Target::Session),
        }
    }

    /// The attributes that `staged` writes under a choosing pick, to the
    /// session store of `source` rather than its tree: a settlement leaves
    /// their writes as written, and the store elects among its own rows.
    pub(crate) async fn stores(
        source: SourceRef<'_>,
        staged: &crate::Staged,
        env: &Erased,
    ) -> Result<HashSet<Relation>, CommitError> {
        if !staged.has_successions() {
            return Ok(HashSet::new());
        }
        let declared = Self::resolve(source, &staged.export(), env).await?;
        Ok(staged
            .log()
            .iter()
            .filter(|(the, _, change)| {
                change.elects() && !declared.lands_in_tree(the, source.bindings())
            })
            .map(|(the, ..)| the.clone())
            .collect())
    }

    pub(crate) fn partition(
        &self,
        changes: Changes,
        bindings: &Bindings,
    ) -> Result<Partitioned, CommitError> {
        let mut tree = Changes::new();
        let mut session = Changes::new();
        let default = self.default_scope();
        for instruction in changes.into_instructions() {
            let attribute = match &instruction {
                Instruction::Assert(a, _) | Instruction::Retract(a) => a.the.clone(),
            };
            let target = match self.scope_of(&attribute) {
                None => Target::Tree,
                Some(scope) if Some(scope) == default => Target::Tree,
                Some(scope) => match bindings.target(scope) {
                    Some(target) => target,
                    None => {
                        return Err(CommitError::UnboundScope {
                            attribute: attribute.to_string(),
                            scope: scope.to_string(),
                        });
                    }
                },
            };
            let into = match target {
                Target::Tree => &mut tree,
                Target::Session => &mut session,
            };
            match instruction {
                Instruction::Assert(a, pick) => into.associate(a.the, a.of, a.is, pick),
                Instruction::Retract(a) => into.dissociate(a.the, a.of, a.is),
            }
        }
        Ok(Partitioned { tree, session })
    }
}

/// The scope of each placed attribute, by the name of the attribute, and
/// the default scope: what a read guided by placement asks of each row it
/// meets. A row carries its attribute as bytes, so the map is keyed by
/// name and a read parses no attribute.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct ScopeMap {
    scopes: HashMap<String, Entity>,
    default: Option<Entity>,
}

impl ScopeMap {
    /// The scope that `attribute` is placed on, if any placement names
    /// it.
    pub(crate) fn scope_of(&self, attribute: &str) -> Option<&Entity> {
        self.scopes.get(attribute)
    }

    /// The scope that an attribute with no placement belongs to, if one
    /// is declared.
    pub(crate) fn default(&self) -> Option<&Entity> {
        self.default.as_ref()
    }
}

impl Placements {
    /// The placements in force, as a [`ScopeMap`].
    pub(crate) fn scopes(&self) -> ScopeMap {
        let mut scopes: HashMap<String, Entity> = self
            .committed
            .attributes
            .iter()
            .filter(|(attribute, scope)| self.retracted.get(*attribute) != Some(*scope))
            .map(|(attribute, scope)| (attribute.to_string(), scope.clone()))
            .collect();
        for (attribute, scope) in &self.declared {
            scopes.insert(attribute.to_string(), scope.clone());
        }
        ScopeMap {
            scopes,
            default: self.default_scope().cloned(),
        }
    }
}

/// A settled batch split by destination store.
pub(crate) struct Partitioned {
    /// Bound for the layer's tree.
    pub(crate) tree: Changes,
    /// Bound for the layer's ephemeral store.
    pub(crate) session: Changes,
}

/// Selector for every committed `dialog.attribute/scope` declaration.
fn declarations_selector() -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(scope_attr())
}

/// Selector for every committed `dialog.attribute/default` declaration.
fn defaults_selector() -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(default_attr())
}

/// Selector for every committed `dialog.attribute/transient` declaration.
fn transients_selector() -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(transient_placement_attr())
}

/// The committed declarations at `source`'s head — three range scans,
/// cached per head.
async fn committed_placements(
    source: SourceRef<'_>,
    env: &Erased,
) -> Result<CommittedPlacements, CommitError> {
    let Some(head) = source.revision() else {
        // A layer with no commits declares nothing.
        return Ok(Arc::default());
    };
    let cache = source.rule_cache();
    if let Some(placements) = cache.placements(&head) {
        return Ok(placements);
    }

    let mut declared = Declared::default();
    for claim in committed(source, declarations_selector(), env).await? {
        let Some(attribute) = entity_attribute(&claim.of) else {
            continue;
        };
        let scope = scope_value(attribute.as_str(), &claim.is)?;
        declared.attributes.insert(attribute, scope);
    }
    // One default per repository; a layer reads the one for its own
    // repository. Any other subject is ignored.
    use crate::schema::DidExt as _;
    let repository = source.subject().did().this();
    for claim in committed(source, defaults_selector(), env).await? {
        if claim.of == repository {
            declared.default = Some(scope_value("dialog.attribute/default", &claim.is)?);
        }
    }
    for claim in committed(source, transients_selector(), env).await? {
        if let (Some(attribute), Value::Boolean(true)) = (entity_attribute(&claim.of), &claim.is) {
            declared.transient.insert(attribute);
        }
    }
    let placements = Arc::new(declared);
    cache.record_placements(head, placements.clone());
    Ok(placements)
}

/// Collect the artifacts a selector matches on the layer's committed
/// tree, one for each cell: the claim that `last` elects, which every
/// declaration is written under.
async fn committed(
    source: SourceRef<'_>,
    selector: ArtifactSelector<Constrained>,
    env: &Erased,
) -> Result<Vec<dialog_artifacts::Artifact>, CommitError> {
    let views: Vec<ArtifactView> = select_from_source(source, env, selector)
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
pub(crate) fn elected(
    views: Vec<ArtifactView>,
) -> Result<Vec<dialog_artifacts::Artifact>, DialogArtifactsError> {
    let mut cells: Vec<ArtifactView> = Vec::new();
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
    cells.iter().map(ArtifactView::to_owned).collect()
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_repo;
    use crate::{Branch, RemoteSite};
    use anyhow::Result;
    use dialog_artifacts::{ArtifactSelector, Entity, Value};
    use dialog_capability::{Fork, Provider};
    use dialog_common::ConditionalSync;
    use dialog_effects::archive::{Get, Put};
    use dialog_effects::authority::Identify;
    use dialog_effects::blob::Read as BlobRead;
    use dialog_effects::memory::Resolve;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Relation;
    use dialog_query::query::Output as _;
    use dialog_query::types::Scalar;
    use dialog_query::{AttributeQuery, InductiveRule, Term};
    use futures_util::StreamExt as _;
    use serde_json::json;

    fn session() -> Entity {
        "memory:session".parse().expect("scope entity")
    }

    /// The values a `(the, of)` pair holds in the layer's composite
    /// read (tree plus session store), typed by the caller.
    async fn values<V, Env>(
        branch: &Branch,
        env: &Env,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<Value>>
    where
        V: Scalar,
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<dialog_artifacts::Speculation>
            + Provider<dialog_artifacts::Preload>
            + Provider<crate::Hydrate>
            + dialog_common::Holds
            + ConditionalSync
            + 'static,
    {
        let query = AttributeQuery::from(
            Term::<Relation>::from(the.parse::<Relation>()?)
                .of(Term::<Entity>::from(of.clone()))
                .is(Term::<V>::var("v")),
        );
        let claims = branch.select(query).perform(env).try_vec().await?;
        Ok(claims.into_iter().map(|claim| claim.is).collect())
    }

    /// The values a `(the, of)` pair holds in the committed tree only.
    async fn committed<Env>(
        branch: &Branch,
        env: &Env,
        the: &str,
        of: &Entity,
    ) -> Result<Vec<Value>>
    where
        Env: Provider<BlobRead>
            + Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Resolve>>
            + Provider<crate::Hydrate>
            + ConditionalSync
            + 'static,
    {
        let selector = ArtifactSelector::new().the(the.parse()?).of(of.clone());
        let stream = branch.claims().select(selector).perform(env).await?;
        let artifacts: Vec<_> = stream
            .map(|item| item.and_then(|view| view.to_owned()))
            .try_collect()
            .await?;
        Ok(artifacts.into_iter().map(|artifact| artifact.is).collect())
    }

    #[dialog_common::test]
    fn it_mints_an_entity_per_attribute() -> Result<()> {
        let attribute: dialog_artifacts::Relation = "ui/selected".parse()?;
        let entity = attribute_entity(&attribute);
        assert_eq!(entity.to_string(), "attribute:ui/selected");
        assert_eq!(entity_attribute(&entity), Some(attribute));
        assert_eq!(entity_attribute(&"doc:1".parse()?), None);
        Ok(())
    }

    /// A declared attribute's facts route to the store its scope is
    /// bound to: the composite read sees them beside tree facts of the
    /// same entity, the tree never holds them, and clearing the session
    /// drops exactly them.
    #[dialog_common::test]
    async fn it_routes_a_placed_attribute_to_its_bound_store() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);

        branch
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, session()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        branch
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
        branch.refresh(&operator).await?;

        assert_eq!(
            values::<String, _>(&branch, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())]
        );
        assert_eq!(
            values::<bool, _>(&branch, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "the composite read joins the session store"
        );
        assert!(
            committed(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "a session-placed fact never reaches the tree"
        );

        branch.overlay().clear();
        assert!(
            values::<bool, _>(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty()
        );
        assert_eq!(
            values::<String, _>(&branch, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the tree half is untouched"
        );
        Ok(())
    }

    /// A placement takes effect in the commit that declares it.
    #[dialog_common::test]
    async fn it_routes_in_the_declaring_commit() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, session()))
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values::<bool, _>(&branch, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)]
        );
        assert!(
            committed(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty()
        );
        Ok(())
    }

    /// Two replicas that each declare a scope for one attribute leave
    /// both claims in the cell when they merge. The placement in force is
    /// the claim that `last` elects, the one a read of the cell returns,
    /// whatever the order of the scan.
    #[dialog_common::test]
    async fn it_places_by_the_claim_that_last_elects_after_a_merge() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let doc: Entity = "doc:1".parse()?;
        let main = repo.branch("main").open().perform(&operator).await?;
        main.transaction()
            .assert(
                dialog_query::the!("doc/body")
                    .of(doc)
                    .is("Base".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let feature = repo.branch("feature").open().perform(&operator).await?;
        feature.set_upstream(&main).perform(&operator).await?;
        feature.pull().perform(&operator).await?;
        for (branch, scope) in [(&main, "memory:main"), (&feature, "memory:feature")] {
            branch
                .transaction()
                .assert(Placement::new("doc/title".parse()?, scope.parse()?))
                .commit()
                .publish()
                .perform(&operator)
                .await?;
        }
        feature
            .pull()
            .perform(&operator)
            .await?
            .expect("the pull merges");

        let source = SourceRef::from(&feature);
        let views: Vec<ArtifactView> = crate::Select::from_source(source, declarations_selector())
            .perform(&operator)
            .await?
            .try_collect()
            .await?;
        assert_eq!(views.len(), 2, "the merge keeps both claims");
        let forward = views[0].clone().elect(views[1].clone())?.to_owned()?;
        let backward = views[1].clone().elect(views[0].clone())?.to_owned()?;
        assert_eq!(forward, backward, "every order elects one claim");
        let Value::Entity(elected) = forward.is else {
            panic!("a placement names a scope");
        };
        let placements = Placements::resolve(source, &Changes::new(), &operator).await?;
        assert_eq!(placements.scope_of(&"doc/title".parse()?), Some(&elected));
        Ok(())
    }

    /// A write under a choosing pick of an attribute placed on the session
    /// store succeeds the row that the store holds of the cell, as a write
    /// to the tree succeeds its claim.
    #[dialog_common::test]
    async fn it_succeeds_the_session_row_of_a_placed_cell() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);
        branch
            .overlay()
            .assert(Placement::new("org/salary".parse()?, session()))?;
        let alice: Entity = "id:alice".parse()?;
        for salary in [400u32, 300] {
            branch
                .transaction()
                .assert(dialog_query::AttributeStatement {
                    the: Relation::from("org/salary".parse::<dialog_artifacts::Relation>()?),
                    of: alice.clone(),
                    is: Value::UnsignedInt(salary.into()),
                    cause: None,
                    cardinality: Some(dialog_query::Cardinality::One),
                    pick: Some(Pick::Last),
                })
                .commit()
                .publish()
                .perform(&operator)
                .await?;
            branch.refresh(&operator).await?;
        }
        let held: Vec<Value> = branch
            .overlay()
            .scan(&ArtifactSelector::new().the("org/salary".parse()?).of(alice))
            .into_iter()
            .map(|fact| fact.is)
            .collect();
        assert_eq!(held, vec![Value::UnsignedInt(300)]);
        Ok(())
    }

    /// A placement held in the branch's session store is this process's
    /// own declaration: a commit routes by it like a committed one, the
    /// tree never holds the placement, and a peer never learns of it.
    #[dialog_common::test]
    async fn it_routes_by_a_placement_held_in_the_session_store() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);
        branch
            .overlay()
            .assert(Placement::new("ui/selected".parse()?, session()))?;

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            values::<bool, _>(&branch, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "the composite read sees the placed fact"
        );
        assert!(
            committed(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "the fact never reaches the tree"
        );
        let placement: Entity = attribute_entity(&"ui/selected".parse()?);
        assert!(
            committed(&branch, &operator, "dialog.attribute/scope", &placement)
                .await?
                .is_empty(),
            "nor does the declaration"
        );
        Ok(())
    }

    /// A transaction touching only session-placed attributes moves no
    /// head: the store changes, the tree does not.
    #[dialog_common::test]
    async fn it_keeps_a_session_only_transaction_off_the_tree() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);

        let before = branch
            .transaction()
            .assert(Placement::new("ui/cursor".parse()?, session()))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        let after = branch
            .transaction()
            .assert(dialog_query::the!("ui/cursor").of(doc.clone()).is(7u64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(after, before, "no tree change, no new revision");
        assert_eq!(
            values::<u64, _>(&branch, &operator, "ui/cursor", &doc).await?,
            vec![Value::UnsignedInt(7)]
        );

        // A retract removes the fact from the store outright, and a
        // retract-and-assert in one transaction nets to the new value.
        branch
            .transaction()
            .retract(dialog_query::the!("ui/cursor").of(doc.clone()).is(7u64))
            .assert(dialog_query::the!("ui/cursor").of(doc.clone()).is(9u64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_eq!(
            values::<u64, _>(&branch, &operator, "ui/cursor", &doc).await?,
            vec![Value::UnsignedInt(9)]
        );
        branch
            .transaction()
            .retract(dialog_query::the!("ui/cursor").of(doc.clone()).is(9u64))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert!(
            values::<u64, _>(&branch, &operator, "ui/cursor", &doc)
                .await?
                .is_empty()
        );
        Ok(())
    }

    /// A rule watching a session-placed attribute fires when a
    /// transaction writes it, and its tree head lands in the tree.
    #[dialog_common::test]
    async fn it_induces_over_a_session_write() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);

        let audit: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "target": { "the": "audit/selected", "as": "Entity" } }
            },
            "when": [{
                "assert": {
                    "with": { "target": { "the": "ui/select", "as": "Entity" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "target": { "?": { "name": "target" } }
                }
            }]
        }))?;

        branch
            .transaction()
            .assert(Placement::new("ui/select".parse()?, session()))
            .assert(audit)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let who: Entity = "session:1".parse()?;
        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("ui/select")
                    .of(who.clone())
                    .is(doc.clone()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            committed(&branch, &operator, "audit/selected", &who).await?,
            vec![Value::Entity(doc)],
            "the tree head lands in the tree"
        );
        assert!(
            committed(&branch, &operator, "ui/select", &who)
                .await?
                .is_empty(),
            "the session trigger stays out of the tree"
        );
        Ok(())
    }

    /// The observable ephemeral conclusion: a rule whose head is
    /// session-placed writes into the store, where a standing
    /// subscription sees it and the tree never does.
    #[dialog_common::test]
    async fn it_concludes_into_the_session_store_observably() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        branch.bind(session(), Target::Session);

        let navigate: InductiveRule = serde_json::from_value(json!({
            "assert!": {
                "with": { "href": { "the": "site/navigate", "as": "Text" } }
            },
            "when": [{
                "assert": {
                    "with": { "title": { "the": "doc/title", "as": "Text" } }
                },
                "where": {
                    "this": { "?": { "name": "this" } },
                    "title": { "?": { "name": "href" } }
                }
            }]
        }))?;

        branch
            .transaction()
            .assert(Placement::new("site/navigate".parse()?, session()))
            .assert(navigate)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let navigations = AttributeQuery::from(
            Term::<Relation>::from(dialog_query::the!("site/navigate"))
                .of(Term::<Entity>::var("e"))
                .is(Term::<String>::var("v")),
        );
        let mut subscription = branch.subscribe(navigations);
        let initial = subscription.poll(&operator).await?.expect("first poll");
        assert!(initial.asserted.is_empty());

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("/docs/1".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the derived session fact propagates");
        assert_eq!(delta.asserted.len(), 1);
        assert_eq!(delta.asserted[0].of, doc);
        assert_eq!(delta.asserted[0].is, Value::String("/docs/1".into()));
        assert!(
            committed(&branch, &operator, "site/navigate", &doc)
                .await?
                .is_empty(),
            "the conclusion never reaches the tree"
        );
        Ok(())
    }

    /// A write to an attribute placed on a scope the layer does not
    /// bind fails the commit instead of landing somewhere else, and
    /// binding it afterwards makes the same write succeed.
    #[dialog_common::test]
    async fn it_refuses_an_unbound_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        let local: Entity = "memory:local".parse()?;
        fn write<'a>(
            branch: &'a Branch,
            doc: &Entity,
            local: &Entity,
        ) -> Result<crate::TransactionPublish<&'a Branch>> {
            Ok(branch
                .transaction()
                .assert(Placement::new("local/note".parse()?, local.clone()))
                .assert(
                    dialog_query::the!("local/note")
                        .of(doc.clone())
                        .is("draft".to_string()),
                )
                .commit()
                .publish())
        }
        let result = write(&branch, &doc, &local)?.perform(&operator).await;
        assert!(
            matches!(result, Err(CommitError::UnboundScope { ref scope, .. }) if scope == "memory:local"),
            "expected an unbound-scope refusal, got {result:?}"
        );

        branch.bind(local.clone(), Target::Session);
        write(&branch, &doc, &local)?.perform(&operator).await?;
        assert_eq!(
            values::<String, _>(&branch, &operator, "local/note", &doc).await?,
            vec![Value::String("draft".into())]
        );
        Ok(())
    }

    /// The repository default names the tree: an attribute placed on
    /// the default scope explicitly, or on a second name bound to the
    /// tree, commits to the tree; retracting a placement returns the
    /// attribute to the default.
    #[dialog_common::test]
    async fn it_routes_the_default_and_tree_bound_names_to_the_tree() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let shared: Entity = "memory:shared".parse()?;
        let durable: Entity = "memory:durable".parse()?;
        branch.bind(durable.clone(), Target::Tree);
        branch.bind(session(), Target::Session);

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(DefaultScope::new(branch.of(), shared.clone()))
            .assert(Placement::new("doc/title".parse()?, shared.clone()))
            .assert(Placement::new("doc/body".parse()?, durable.clone()))
            .assert(Placement::new("ui/selected".parse()?, session()))
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .assert(
                dialog_query::the!("doc/body")
                    .of(doc.clone())
                    .is("Body".to_string()),
            )
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            committed(&branch, &operator, "doc/title", &doc).await?,
            vec![Value::String("Notes".into())],
            "the default scope is the tree"
        );
        assert_eq!(
            committed(&branch, &operator, "doc/body", &doc).await?,
            vec![Value::String("Body".into())],
            "a second name bound to the tree is the tree"
        );
        assert!(
            committed(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty()
        );

        // Unplacing returns the attribute to the default.
        branch
            .transaction()
            .retract(Placement::new("ui/selected".parse()?, session()))
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(false))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_eq!(
            committed(&branch, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(false)],
            "an unplaced attribute belongs to the default again"
        );
        Ok(())
    }

    /// A placement whose value is not an entity is refused, so a
    /// malformed declaration cannot silently route to the tree.
    #[dialog_common::test]
    async fn it_refuses_a_malformed_placement() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let result = branch
            .transaction()
            .assert(
                dialog_query::the!("dialog.attribute/scope")
                    .of(attribute_entity(&"ui/selected".parse()?))
                    .is("session".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await;
        assert!(
            matches!(result, Err(CommitError::InvalidPlacement { .. })),
            "expected a malformed-placement refusal, got {result:?}"
        );
        Ok(())
    }
}
