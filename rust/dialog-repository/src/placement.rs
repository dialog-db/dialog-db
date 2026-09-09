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
//! (`memory:shared`, `memory:session`), and nothing about the name is
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
//! <repository did>       dialog.attribute/default  memory:shared   # the implicit scope
//! attribute:ui/selected  dialog.attribute/scope    memory:session  # an override
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
//! # Observability
//!
//! A scope bound to the ephemeral store is folded into every read of
//! the layer and every standing subscription maintains from its
//! instants. So a rule concluding a session-scoped head writes
//! something a subscriber sees — the observable ephemeral conclusion
//! a transient (one induction round, never written anywhere) cannot
//! be.

use std::collections::HashMap;
use std::sync::Arc;

use dialog_artifacts::selector::Constrained;
use dialog_artifacts::{
    ArtifactSelector, Attribute, Change, Changes, Entity, Instruction, Statement, Update, Value,
};
use dialog_capability::{Fork, Provider};
use dialog_common::ConditionalSync;
use dialog_effects::archive::{Get, Put};
use dialog_effects::memory::Resolve;
use dialog_query::the;
use futures_util::{StreamExt as _, TryStreamExt as _};
use parking_lot::RwLock;

use crate::repository::source::SourceRef;
use crate::{CommitError, RemoteSite};

/// The `dialog.attribute/scope` declaration attribute.
pub(crate) fn scope_attr() -> Attribute {
    the!("dialog.attribute/scope").into()
}

/// The `dialog.attribute/default` declaration attribute.
pub(crate) fn default_attr() -> Attribute {
    the!("dialog.attribute/default").into()
}

/// The URI scheme attribute entities are minted under.
const ATTRIBUTE_SCHEME: &str = "attribute:";

/// The entity that stands for an attribute in facts *about* the
/// attribute: `attribute:<namespace>/<name>`.
pub fn attribute_entity(attribute: &Attribute) -> Entity {
    format!("{ATTRIBUTE_SCHEME}{attribute}")
        .parse()
        .expect("an attribute name is a valid opaque URI path")
}

/// The attribute an [`attribute_entity`] stands for, if the entity
/// is one.
fn entity_attribute(entity: &Entity) -> Option<Attribute> {
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
/// # use dialog_repository::{Branch, Placement, Target};
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
    pub attribute: Attribute,
    /// The scope its facts live in.
    pub scope: Entity,
}

impl Placement {
    /// Declare that `attribute`'s facts live in `scope`.
    pub fn new(attribute: Attribute, scope: Entity) -> Self {
        Self { attribute, scope }
    }
}

impl Statement for Placement {
    fn assert(self, update: &mut impl Update) {
        update.associate_unique(
            scope_attr(),
            attribute_entity(&self.attribute),
            Value::Entity(self.scope),
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
        update.associate_unique(default_attr(), self.repository, Value::Entity(self.scope));
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
    attributes: HashMap<Attribute, Entity>,
    default: Option<Entity>,
}

/// A shared handle to the committed declarations at a head.
pub(crate) type CommittedPlacements = Arc<Declared>;

/// The resolved placements a commit routes by: the committed slice at
/// the head, with the transaction's own declarations (and
/// retractions) laid over it.
#[derive(Debug, Default)]
pub(crate) struct Placements {
    committed: CommittedPlacements,
    declared: HashMap<Attribute, Entity>,
    retracted: HashMap<Attribute, Entity>,
    default: Option<Entity>,
    default_retracted: Option<Entity>,
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
    pub(crate) async fn resolve<Env>(
        source: SourceRef<'_>,
        changes: &Changes,
        env: &Env,
    ) -> Result<Self, CommitError>
    where
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let committed = committed_placements(source, env).await?;
        let mut placements = Placements {
            committed,
            ..Placements::default()
        };
        let scope = scope_attr();
        let default = default_attr();
        for (entity, the, change) in changes.iter() {
            if *the == scope {
                let Some(placed) = entity_attribute(entity) else {
                    continue;
                };
                let (Change::Assert(value) | Change::Replace(value) | Change::Retract(value)) =
                    change;
                let target = scope_value(placed.as_str(), value)?;
                match change {
                    Change::Retract(_) => placements.retracted.insert(placed, target),
                    _ => placements.declared.insert(placed, target),
                };
            } else if *the == default {
                let (Change::Assert(value) | Change::Replace(value) | Change::Retract(value)) =
                    change;
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

    /// The scope `attribute`'s facts live in, or `None` for the tree
    /// when nothing names it.
    pub(crate) fn scope_of(&self, attribute: &Attribute) -> Option<&Entity> {
        if let Some(scope) = self.declared.get(attribute) {
            return Some(scope);
        }
        match self.committed.attributes.get(attribute) {
            Some(scope) if self.retracted.get(attribute) != Some(scope) => Some(scope),
            _ => self.default_scope(),
        }
    }

    /// Split a settled batch by destination store: the instructions
    /// for the tree and the ones for the session store. Declarations
    /// themselves are tree facts. An instruction bound for a scope the
    /// layer does not bind fails the whole batch.
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
                Instruction::Assert(a) | Instruction::Replace(a) | Instruction::Retract(a) => {
                    a.the.clone()
                }
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
                Instruction::Assert(a) => into.associate(a.the, a.of, a.is),
                Instruction::Replace(a) => into.associate_unique(a.the, a.of, a.is),
                Instruction::Retract(a) => into.dissociate(a.the, a.of, a.is),
            }
        }
        Ok(Partitioned { tree, session })
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

/// The committed declarations at `source`'s head — two range scans,
/// cached per head.
async fn committed_placements<Env>(
    source: SourceRef<'_>,
    env: &Env,
) -> Result<CommittedPlacements, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
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
    let placements = Arc::new(declared);
    cache.record_placements(head, placements.clone());
    Ok(placements)
}

/// Collect the artifacts a selector matches on the layer's committed
/// tree.
async fn committed<Env>(
    source: SourceRef<'_>,
    selector: ArtifactSelector<Constrained>,
    env: &Env,
) -> Result<Vec<dialog_artifacts::Artifact>, CommitError>
where
    Env: Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Fork<RemoteSite, Get>>
        + Provider<Fork<RemoteSite, Resolve>>
        + ConditionalSync
        + 'static,
{
    let stream = crate::Select::from_source(source, selector)
        .perform(env)
        .await
        .map_err(|error| CommitError::Induction(format!("placement scan: {error}")))?;
    stream
        .map(|item| item.and_then(|view| view.to_owned()))
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("placement scan: {error}")))
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
    use dialog_effects::memory::Resolve;
    use dialog_operator::helpers::test_operator_with_profile;
    use dialog_query::attribute::The;
    use dialog_query::query::Output as _;
    use dialog_query::types::Scalar;
    use dialog_query::{AttributeQuery, InductiveRule, Term};
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
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Identify>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
            + ConditionalSync
            + 'static,
    {
        let query = AttributeQuery::from(
            Term::<The>::from(the.parse::<The>()?)
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
        Env: Provider<Get>
            + Provider<Put>
            + Provider<Resolve>
            + Provider<Fork<RemoteSite, Get>>
            + Provider<Fork<RemoteSite, Resolve>>
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
        let attribute: Attribute = "ui/selected".parse()?;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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

    /// A transaction touching only session-placed attributes moves no
    /// head: the store changes, the tree does not.
    #[dialog_common::test]
    async fn it_keeps_a_session_only_transaction_off_the_tree() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
            Term::<The>::from(dialog_query::the!("site/navigate"))
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
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
