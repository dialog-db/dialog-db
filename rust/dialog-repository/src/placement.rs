//! Attribute placement: which layer an attribute's facts live in.
//!
//! A branch is read as one composite, but its facts live in layers
//! that differ in two properties — whether they survive a restart and
//! whether they replicate. Rather than having every writer pick a
//! layer per call (a durable `assert`, a `dispatch`, an
//! `overlay().assert(..)`), the *attribute* declares its layer once,
//! as a fact on the branch, and every write routes by it: a
//! transaction's `assert` / `retract` lands each instruction in the
//! layer its attribute declares, a rule's head does the same, and a
//! concept whose attributes span layers fans out on write and joins
//! back on read.
//!
//! # Layers
//!
//! The set is fixed and every layer always exists, so a declaration
//! can never name a layer some replica lacks:
//!
//! | layer | durable | replicated | backing today |
//! |---|---|---|---|
//! | [`Semantic`](Layer::Semantic) | yes | yes | the branch tree (the default) |
//! | [`Episodic`](Layer::Episodic) | yes | no | none yet |
//! | [`Procedural`](Layer::Procedural) | no | no | the line's [`Ephemeral`](crate::Ephemeral) store |
//! | [`Sensory`](Layer::Sensory) | no | yes | none yet |
//!
//! A write to an attribute declared on a layer with no backing fails
//! the commit ([`CommitError::UnbackedLayer`]) rather than silently
//! landing somewhere else.
//!
//! # Declaration
//!
//! `dialog.attribute/layer` `of` the attribute's entity (see
//! [`attribute_entity`]) `is` the layer's name. It is a branch-level
//! fact, deliberately outside any concept's content address — the
//! same descriptor may be procedural on one branch and semantic on
//! another — and it takes effect in the commit that declares it, so a
//! transaction can declare and use a placement together. Undeclared
//! attributes are semantic.
//!
//! # Observability
//!
//! The procedural layer is the session overlay, which every read of
//! the branch folds in and every standing subscription re-evaluates
//! on. So a rule concluding a procedural head writes something a
//! subscriber sees — the observable ephemeral conclusion a transient
//! (one induction round, never written anywhere) cannot be.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
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

use crate::repository::source::SourceRef;
use crate::{CommitError, RemoteSite};

/// The layer an attribute's facts live in. See the [module
/// docs](self) for the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Layer {
    /// Durable and replicated: the branch tree. The default.
    Semantic,
    /// Durable on this replica, never replicated. Not backed yet.
    Episodic,
    /// This process only, lost on restart: the session overlay.
    Procedural,
    /// Replicated but never stored. Not backed yet.
    Sensory,
}

impl Layer {
    /// Every layer, in declaration order.
    pub const ALL: [Layer; 4] = [
        Layer::Semantic,
        Layer::Episodic,
        Layer::Procedural,
        Layer::Sensory,
    ];

    /// The layer's name, as written in a declaration.
    pub fn name(self) -> &'static str {
        match self {
            Layer::Semantic => "semantic",
            Layer::Episodic => "episodic",
            Layer::Procedural => "procedural",
            Layer::Sensory => "sensory",
        }
    }

    /// Whether facts on this layer survive a restart.
    pub fn is_durable(self) -> bool {
        matches!(self, Layer::Semantic | Layer::Episodic)
    }

    /// Whether facts on this layer replicate to peers.
    pub fn is_replicated(self) -> bool {
        matches!(self, Layer::Semantic | Layer::Sensory)
    }

    /// Whether this layer has a store behind it today.
    pub fn is_backed(self) -> bool {
        matches!(self, Layer::Semantic | Layer::Procedural)
    }
}

impl fmt::Display for Layer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl FromStr for Layer {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        Layer::ALL
            .into_iter()
            .find(|layer| layer.name() == name)
            .ok_or_else(|| format!("unknown layer {name:?}"))
    }
}

/// The `dialog.attribute/layer` declaration attribute.
pub(crate) fn layer_attr() -> Attribute {
    the!("dialog.attribute/layer").into()
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

/// [`Statement`] declaring the layer an attribute's facts live in.
/// Asserting it places the attribute; retracting it returns the
/// attribute to the default ([`Layer::Semantic`]).
///
/// ```no_run
/// # use dialog_repository::{Branch, Layer, Placement};
/// # async fn example(branch: &Branch, env: &impl std::any::Any) -> anyhow::Result<()> {
/// let selected = "ui/selected".parse()?;
/// let tx = branch
///     .transaction()
///     .assert(Placement::new(selected, Layer::Procedural));
/// # let _ = tx;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The attribute being placed.
    pub attribute: Attribute,
    /// The layer its facts live in.
    pub layer: Layer,
}

impl Placement {
    /// Declare that `attribute`'s facts live in `layer`.
    pub fn new(attribute: Attribute, layer: Layer) -> Self {
        Self { attribute, layer }
    }
}

impl Statement for Placement {
    fn assert(self, update: &mut impl Update) {
        update.associate_unique(
            layer_attr(),
            attribute_entity(&self.attribute),
            Value::String(self.layer.name().to_string()),
        );
    }

    fn retract(self, update: &mut impl Update) {
        update.dissociate(
            layer_attr(),
            attribute_entity(&self.attribute),
            Value::String(self.layer.name().to_string()),
        );
    }
}

/// The committed placements at a branch head: attribute → layer for
/// every declared attribute. Cached per head on the line's
/// [`RuleCache`](crate::RuleCache), like the trigger footprint.
pub(crate) type CommittedPlacements = Arc<HashMap<Attribute, Layer>>;

/// The resolved placements a commit routes by: the committed slice at
/// the head, with the transaction's own declarations (and
/// retractions) laid over it.
#[derive(Debug, Default)]
pub(crate) struct Placements {
    committed: CommittedPlacements,
    declared: HashMap<Attribute, Layer>,
    retracted: HashMap<Attribute, Layer>,
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
        let attribute = layer_attr();
        for (entity, the, change) in changes.iter() {
            if *the != attribute {
                continue;
            }
            let Some(placed) = entity_attribute(entity) else {
                continue;
            };
            let (Change::Assert(Value::String(name))
            | Change::Replace(Value::String(name))
            | Change::Retract(Value::String(name))) = change
            else {
                continue;
            };
            let Ok(layer) = name.parse::<Layer>() else {
                return Err(CommitError::UnknownLayer {
                    attribute: placed.to_string(),
                    layer: name.clone(),
                });
            };
            match change {
                Change::Retract(_) => placements.retracted.insert(placed, layer),
                _ => placements.declared.insert(placed, layer),
            };
        }
        Ok(placements)
    }

    /// The layer `attribute`'s facts live in.
    pub(crate) fn layer_of(&self, attribute: &Attribute) -> Layer {
        if let Some(layer) = self.declared.get(attribute) {
            return *layer;
        }
        match self.committed.get(attribute) {
            Some(layer) if self.retracted.get(attribute) != Some(layer) => *layer,
            _ => Layer::Semantic,
        }
    }

    /// Split a settled batch by layer: the semantic instructions (for
    /// the tree) and the procedural ones (for the session overlay).
    /// Declarations themselves are semantic facts and stay in the
    /// tree. An instruction bound for a layer with no backing fails
    /// the whole batch.
    pub(crate) fn partition(&self, changes: Changes) -> Result<Partitioned, CommitError> {
        let mut semantic = Changes::new();
        let mut procedural = Changes::new();
        for instruction in changes.into_instructions() {
            let attribute = match &instruction {
                Instruction::Assert(a) | Instruction::Replace(a) | Instruction::Retract(a) => {
                    a.the.clone()
                }
            };
            let target = match self.layer_of(&attribute) {
                Layer::Semantic => &mut semantic,
                Layer::Procedural => &mut procedural,
                unbacked => {
                    return Err(CommitError::UnbackedLayer {
                        attribute: attribute.to_string(),
                        layer: unbacked,
                    });
                }
            };
            match instruction {
                Instruction::Assert(a) => target.associate(a.the, a.of, a.is),
                Instruction::Replace(a) => target.associate_unique(a.the, a.of, a.is),
                Instruction::Retract(a) => target.dissociate(a.the, a.of, a.is),
            }
        }
        Ok(Partitioned {
            semantic,
            procedural,
        })
    }
}

/// A settled batch split by destination layer.
pub(crate) struct Partitioned {
    /// Bound for the branch tree.
    pub(crate) semantic: Changes,
    /// Bound for the session overlay.
    pub(crate) procedural: Changes,
}

/// Selector for every committed `dialog.attribute/layer` declaration.
fn declarations_selector() -> ArtifactSelector<Constrained> {
    ArtifactSelector::new().the(layer_attr())
}

/// The committed placements at `source`'s head — one range scan over
/// `dialog.attribute/layer`, cached per head.
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
        // A line with no commits declares nothing.
        return Ok(Arc::default());
    };
    let cache = source.rule_cache();
    if let Some(placements) = cache.placements(&head) {
        return Ok(placements);
    }
    let stream = crate::Select::from_source(source, declarations_selector())
        .perform(env)
        .await
        .map_err(|error| CommitError::Induction(format!("placement scan: {error}")))?;
    let claims: Vec<_> = stream
        .map(|item| item.and_then(|view| view.to_owned()))
        .try_collect()
        .await
        .map_err(|error| CommitError::Induction(format!("placement scan: {error}")))?;

    let mut placements = HashMap::with_capacity(claims.len());
    for claim in claims {
        let Some(attribute) = entity_attribute(&claim.of) else {
            continue;
        };
        let Value::String(name) = &claim.is else {
            continue;
        };
        let Ok(layer) = name.parse::<Layer>() else {
            return Err(CommitError::UnknownLayer {
                attribute: attribute.to_string(),
                layer: name.clone(),
            });
        };
        placements.insert(attribute, layer);
    }
    let placements = Arc::new(placements);
    cache.record_placements(head, placements.clone());
    Ok(placements)
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

    /// The values a `(the, of)` pair holds in the branch's composite
    /// read (tree plus session layer), typed by the caller.
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
    fn it_names_every_layer_round_trip() {
        for layer in Layer::ALL {
            assert_eq!(layer.name().parse::<Layer>(), Ok(layer));
            assert_eq!(layer.to_string(), layer.name());
        }
        assert!("working".parse::<Layer>().is_err());
        assert!(Layer::Semantic.is_durable() && Layer::Semantic.is_replicated());
        assert!(Layer::Episodic.is_durable() && !Layer::Episodic.is_replicated());
        assert!(!Layer::Procedural.is_durable() && !Layer::Procedural.is_replicated());
        assert!(!Layer::Sensory.is_durable() && Layer::Sensory.is_replicated());
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

    /// A declared attribute's facts route to the session layer: the
    /// composite read sees them beside tree facts of the same entity,
    /// the tree never holds them, and clearing the session drops
    /// exactly them.
    #[dialog_common::test]
    async fn it_routes_a_declared_attribute_to_the_procedural_layer() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        branch
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, Layer::Procedural))
            .commit()
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
            "the composite read joins the session layer"
        );
        assert!(
            committed(&branch, &operator, "ui/selected", &doc)
                .await?
                .is_empty(),
            "a procedural fact never reaches the tree"
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
            "the semantic half is untouched"
        );
        Ok(())
    }

    /// A placement takes effect in the commit that declares it.
    #[dialog_common::test]
    async fn it_routes_in_the_declaring_commit() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(Placement::new("ui/selected".parse()?, Layer::Procedural))
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
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

    /// A transaction touching only procedural attributes moves no
    /// head: the session layer changes, the tree does not.
    #[dialog_common::test]
    async fn it_keeps_a_procedural_only_transaction_off_the_tree() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let before = branch
            .transaction()
            .assert(Placement::new("ui/cursor".parse()?, Layer::Procedural))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        let after = branch
            .transaction()
            .assert(dialog_query::the!("ui/cursor").of(doc.clone()).is(7u64))
            .commit()
            .perform(&operator)
            .await?;
        assert_eq!(after, before, "no semantic change, no new revision");
        assert_eq!(
            values::<u64, _>(&branch, &operator, "ui/cursor", &doc).await?,
            vec![Value::UnsignedInt(7)]
        );

        // A retract removes the fact from the session layer outright
        // (it is the store, so there is nothing to tombstone), and a
        // retract-and-assert in one transaction nets to the new value.
        branch
            .transaction()
            .retract(dialog_query::the!("ui/cursor").of(doc.clone()).is(7u64))
            .assert(dialog_query::the!("ui/cursor").of(doc.clone()).is(9u64))
            .commit()
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
            .perform(&operator)
            .await?;
        assert!(
            values::<u64, _>(&branch, &operator, "ui/cursor", &doc)
                .await?
                .is_empty()
        );
        Ok(())
    }

    /// A rule watching a procedural attribute fires when a transaction
    /// writes it, and its semantic head lands in the tree.
    #[dialog_common::test]
    async fn it_induces_over_a_procedural_write() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

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
            .assert(Placement::new("ui/select".parse()?, Layer::Procedural))
            .assert(audit)
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let session: Entity = "session:1".parse()?;
        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .assert(
                dialog_query::the!("ui/select")
                    .of(session.clone())
                    .is(doc.clone()),
            )
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            committed(&branch, &operator, "audit/selected", &session).await?,
            vec![Value::Entity(doc)],
            "the semantic head lands in the tree"
        );
        assert!(
            committed(&branch, &operator, "ui/select", &session)
                .await?
                .is_empty(),
            "the procedural trigger stays out of the tree"
        );
        Ok(())
    }

    /// The observable ephemeral conclusion: a rule whose head is
    /// procedural writes into the session layer, where a standing
    /// subscription sees it and the tree never does.
    #[dialog_common::test]
    async fn it_concludes_into_the_procedural_layer_observably() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

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
            .assert(Placement::new("site/navigate".parse()?, Layer::Procedural))
            .assert(navigate)
            .commit()
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

    /// A write to an attribute placed on a layer with no backing fails
    /// the commit instead of landing somewhere else.
    #[dialog_common::test]
    async fn it_refuses_an_unbacked_layer() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        let result = branch
            .transaction()
            .assert(Placement::new("local/note".parse()?, Layer::Episodic))
            .assert(
                dialog_query::the!("local/note")
                    .of(doc)
                    .is("draft".to_string()),
            )
            .commit()
            .perform(&operator)
            .await;
        assert!(
            matches!(
                result,
                Err(CommitError::UnbackedLayer {
                    layer: Layer::Episodic,
                    ..
                })
            ),
            "expected an unbacked-layer refusal, got {result:?}"
        );
        Ok(())
    }

    /// Retracting a placement returns the attribute to the tree.
    #[dialog_common::test]
    async fn it_returns_an_attribute_to_the_tree_when_unplaced() -> Result<()> {
        let (operator, profile) = test_operator_with_profile().await;
        let repo = test_repo(&operator, &profile).await;
        let branch = repo.branch("main").open().perform(&operator).await?;

        let placement = Placement::new("ui/selected".parse()?, Layer::Procedural);
        branch
            .transaction()
            .assert(placement.clone())
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        let doc: Entity = "doc:1".parse()?;
        branch
            .transaction()
            .retract(placement)
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .commit()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;

        assert_eq!(
            committed(&branch, &operator, "ui/selected", &doc).await?,
            vec![Value::Boolean(true)],
            "an unplaced attribute is semantic again"
        );
        Ok(())
    }
}
