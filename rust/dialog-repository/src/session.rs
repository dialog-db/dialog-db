//! A session: the primary branch read and written as one with the
//! layers its schema declares beside it.
//!
//! The primary's schema says which layers exist ([`LayerDeclaration`])
//! and where each attribute lives ([`Placement`]). Opening a session on
//! the primary builds every layer the schema declares in memory and
//! reads them with the primary as one composite: each attribute from the
//! layer it is placed on, and from it alone (see [`placement::guide`]).
//! A transaction through the session routes each write to the layer its
//! attribute is placed on: the primary's share is committed and
//! published as any branch commit is, and each memory layer's share lands
//! in it once the publish does, so no layer shows a commit that was
//! refused.
//!
//! ```no_run
//! # use dialog_repository::{Branch, LayerDeclaration, Placement, Store};
//! # async fn example(branch: Branch, env: impl dialog_repository::SessionEnv) -> anyhow::Result<()> {
//! let state: dialog_corpus::Entity = "layer:state".parse()?;
//! branch
//!     .transaction()
//!     .assert(LayerDeclaration::new(state.clone(), Store::Memory))
//!     .assert(Placement::new("ui/selected".parse()?, state))
//!     .commit()
//!     .publish()
//!     .perform(&env)
//!     .await?;
//! let session = branch.session().perform(&env).await?;
//! session
//!     .transaction()
//!     .assert(dialog_query::the!("ui/selected").of("doc:1".parse()?).is(true))
//!     .commit()
//!     .publish()
//!     .perform(&env)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! What the session holds of its own are its **declarations**
//! ([`Session::declare`]): placements and layers this process declares
//! for itself, which outrank what the primary's tree and session store
//! declare, yield to what a transaction declares, and reach no layer and
//! no peer.
//!
//! [`placement::guide`]: crate::placement::guide

use std::sync::Arc;

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Changes, Entity, Statement};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{
    Import as BlobImport, Load as BlobLoad, Read as BlobRead, Size as BlobSize,
};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::nodes;
use parking_lot::RwLock;

use crate::placement::guide::{Divergence, Divergences, Guide, GuidedLayer};
use crate::placement::{Placements, Store};
use crate::repository::branch::session::Composite;
use crate::repository::branch::transaction::SessionContext;
use crate::repository::source::Source;
use crate::schema::DidExt as _;
use crate::{Branch, CommitError, Ephemeral, QueryLayer, RemoteSite, Transaction};

/// What a session needs of its environment: a read of the primary, to
/// resolve the layers its schema declares when it opens, and what a
/// transaction's commit needs, to publish through it.
pub trait SessionEnv:
    Provider<BlobRead>
    + Provider<Get>
    + Provider<Put>
    + Provider<Resolve>
    + Provider<Identify>
    + Provider<Attest>
    + Provider<Publish>
    + Provider<Import>
    + Provider<BlobImport>
    + Provider<BlobSize>
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

impl<T> SessionEnv for T where
    T: Provider<BlobRead>
        + Provider<Get>
        + Provider<Put>
        + Provider<Resolve>
        + Provider<Identify>
        + Provider<Attest>
        + Provider<Publish>
        + Provider<Import>
        + Provider<BlobImport>
        + Provider<BlobSize>
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

/// Command opening a [`Session`] on a branch. Built by
/// [`Branch::session`]; run with [`perform`](Self::perform).
pub struct OpenSession {
    primary: Branch,
    declared: Changes,
}

impl OpenSession {
    /// Hold `declaration` in the session once it is open, as
    /// [`Session::declare`] would: a layer it declares is built with the
    /// rest.
    pub fn declare<S: Statement>(mut self, declaration: S) -> Self {
        declaration.assert(&mut self.declared);
        self
    }

    /// Read the primary's schema at its head and build every layer it
    /// declares.
    pub async fn perform<Env: SessionEnv>(self, env: &Env) -> Result<Session, CommitError> {
        let primary = Source::from(self.primary.clone());
        let placements =
            Placements::resolve(&primary, &self.declared, &Changes::new(), env).await?;
        let layers = placements
            .layers()
            .into_iter()
            .map(|(entity, store)| match store {
                Store::Memory => (entity, Ephemeral::new()),
            })
            .collect();
        Ok(Session {
            inner: Arc::new(Inner {
                primary: self.primary,
                layers,
                declared: RwLock::new(self.declared),
                divergences: Divergences::default(),
            }),
        })
    }
}

struct Inner {
    primary: Branch,
    /// The layers the schema declares, each with the entity that names
    /// it, in the order the schema lists them.
    layers: Vec<(Entity, Ephemeral)>,
    declared: RwLock<Changes>,
    divergences: Divergences,
}

/// The primary branch and the layers its schema declares, read and
/// written as one. Cheap to clone: clones share the layers and the
/// declarations.
#[derive(Clone)]
pub struct Session {
    inner: Arc<Inner>,
}

impl Branch {
    /// Open a session on this branch: it becomes the primary, and every
    /// layer its schema declares is built beside it.
    pub fn session(&self) -> OpenSession {
        OpenSession {
            primary: self.clone(),
            declared: Changes::new(),
        }
    }
}

impl Session {
    /// The branch this session reads and writes as its primary.
    pub fn primary(&self) -> &Branch {
        &self.inner.primary
    }

    /// The layer the schema declares as `name`, if the session built one.
    pub fn layer(&self, name: &Entity) -> Option<&Ephemeral> {
        self.inner
            .layers
            .iter()
            .find(|(layer, _)| layer == name)
            .map(|(_, memory)| memory)
    }

    /// The layers this session holds beside the primary, by name.
    pub fn layers(&self) -> Vec<Entity> {
        self.inner
            .layers
            .iter()
            .map(|(layer, _)| layer.clone())
            .collect()
    }

    /// Hold `declaration` in this session: a [`Placement`](crate::Placement)
    /// or a [`LayerDeclaration`](crate::LayerDeclaration), or a
    /// [`Changes`] of them, whose retractions undo what an earlier call
    /// declared or what the primary declares. It is this process's own:
    /// it outranks a declaration the primary's session store or tree
    /// holds, yields to one a transaction carries, guides every read and
    /// routes every commit of the session from now on, and reaches no
    /// layer and no peer. A layer declared here after the session opened
    /// is not built: declare it on [`Branch::session`] instead.
    /// Chainable.
    pub fn declare<S: Statement>(&self, declaration: S) -> &Self {
        declaration.assert(&mut *self.inner.declared.write());
        self
    }

    /// The declarations this session holds, as [`declare`](Self::declare)
    /// accumulated them.
    pub fn declared(&self) -> Changes {
        self.inner.declared.read().clone()
    }

    /// Every divergence of placement the session's reads found since
    /// the last call: a fact in a layer other than the one its attribute
    /// is placed on, which no read takes.
    pub fn divergences(&self) -> Vec<Divergence> {
        self.inner.divergences.take()
    }

    /// How a read of this session meets each layer.
    fn guide(&self) -> Arc<Guide> {
        Arc::new(Guide {
            lines: vec![GuidedLayer {
                entity: self.inner.primary.subject().did().this(),
                primary: true,
            }],
            memories: self
                .inner
                .layers
                .iter()
                .map(|(entity, _)| GuidedLayer {
                    entity: entity.clone(),
                    primary: false,
                })
                .collect(),
            divergences: self.inner.divergences.clone(),
            declared: self.declared(),
        })
    }

    /// The layers a transaction through this session reads and induces
    /// over, pinned as `primary`.
    fn composite(&self, primary: Source) -> Composite {
        Composite {
            sources: vec![primary],
            memories: self
                .inner
                .layers
                .iter()
                .map(|(_, memory)| memory.clone())
                .collect(),
            guide: Some(self.guide()),
        }
    }

    /// A query over the session: the primary and every layer, each
    /// attribute read from the layer it is placed on.
    pub fn query(&self) -> QueryLayer<'_> {
        let mut layer = QueryLayer::from(&self.inner.primary);
        for (_, memory) in &self.inner.layers {
            layer = layer.with_memory(memory);
        }
        layer.guided(self.guide())
    }

    /// Start a transaction through the session. Its reads see every
    /// layer; its commit routes each write to the layer its attribute is
    /// placed on, and the memory layers take their shares once the
    /// primary's commit publishes.
    pub fn transaction(&self) -> Transaction<&Branch> {
        let transaction = self.inner.primary.transaction();
        let primary = transaction.view().to_source();
        transaction.with_session(SessionContext {
            composite: self.composite(primary),
            declared: self.declared(),
            memories: self.inner.layers.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_repo;
    use crate::{LayerDeclaration, Placement, PublishError};
    use anyhow::Result;
    use dialog_corpus::Selector;
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Cardinality;
    use dialog_query::attribute::Relation as QueryRelation;
    use dialog_query::attribute::statement::AttributeStatement;
    use dialog_query::query::Output as _;
    use dialog_query::types::Scalar;
    use dialog_query::{AttributeQuery, InductiveRule, Term};
    use dialog_storage::provider::storage::VolatileSpace;
    use std::fmt::Debug;

    type Operator = Peer<VolatileSpace, dialog_peer::Session>;

    fn layer(name: &str) -> Entity {
        format!("layer:{name}").parse().expect("a layer entity")
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
        T: Scalar + Debug + Clone + Send + Sync + 'static,
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
        T: Scalar + Debug + Clone + Send + Sync + 'static,
    {
        read::<T>(branch.query(), env, the, of).await
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

    /// `counter/count` of `of` as `value`, under `last`: the count is
    /// single-valued.
    fn count(of: &Entity, value: u64) -> AttributeStatement {
        AttributeStatement {
            the: QueryRelation::from("counter/count".parse::<dialog_corpus::Relation>().unwrap()),
            of: of.clone(),
            is: dialog_corpus::Value::UnsignedInt(value.into()),
            cause: None,
            cardinality: Some(Cardinality::One),
            pick: Some(dialog_corpus::Pick::Last),
        }
    }

    /// Declare a memory layer `state` with `ui/selected` placed on it, and
    /// a counter rule whose head `counter/count`, under `last`, is placed
    /// there too.
    async fn declare_state(branch: &Branch, env: &Operator) -> Result<Entity> {
        let state = layer("state");
        let increment: InductiveRule = serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "count": { "the": "counter/count", "as": "UnsignedInteger", "pick": "last" }
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
                            "count": { "the": "counter/count", "as": "UnsignedInteger", "pick": "last" }
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
            .assert(LayerDeclaration::new(state.clone(), Store::Memory))
            .assert(Placement::new("ui/selected".parse()?, state.clone()))
            .assert(Placement::new("counter/count".parse()?, state.clone()))
            .assert(increment)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    /// A session builds the layers the primary's schema declares, routes
    /// each write to the layer its attribute is placed on, and reads
    /// each attribute from that layer alone: the tree never holds a fact
    /// placed on the memory layer, and the layer never holds one placed
    /// nowhere.
    #[dialog_common::test]
    async fn it_routes_each_write_to_the_layer_its_attribute_is_placed_on() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        assert_eq!(
            session.layers(),
            vec![state.clone()],
            "the schema's layer is built"
        );
        let memory = session.layer(&state).expect("the state layer").clone();

        let doc: Entity = "doc:1".parse()?;
        let revision = session
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
            read::<bool>(session.query(), &operator, "ui/selected", &doc).await?,
            vec!["Boolean(true)".to_string()]
        );
        assert_eq!(
            read::<String>(session.query(), &operator, "doc/title", &doc).await?,
            vec!["String(\"Notes\")".to_string()]
        );
        assert!(session.divergences().is_empty(), "nothing is misplaced");
        Ok(())
    }

    /// A rule's head placed on a memory layer lands there, with its pick:
    /// a dispatched command increments a count the layer holds, and the
    /// commit, which writes nothing to the tree, still publishes.
    #[dialog_common::test]
    async fn it_lands_a_rules_head_on_the_layer_it_is_placed_on() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        session
            .transaction()
            .assert(count(&counter, 1))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let before = branch.revision();
        for expected in ["UnsignedInt(2)", "UnsignedInt(3)"] {
            session
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
            read::<u64>(session.query(), &operator, "counter/count", &counter).await?,
            vec!["UnsignedInt(3)".to_string()]
        );
        Ok(())
    }

    /// A write placed on a layer the session does not hold is refused
    /// before anything changes: the primary's head and every layer stay as
    /// they were.
    #[dialog_common::test]
    async fn it_refuses_a_write_placed_on_a_layer_the_session_does_not_hold() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let before = branch.revision();
        let doc: Entity = "doc:1".parse()?;
        // The session declares a placement on a layer nothing built.
        session.declare(Placement::new("doc/note".parse()?, layer("nowhere")));
        let refused = session
            .transaction()
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
            .assert(
                dialog_query::the!("doc/note")
                    .of(doc.clone())
                    .is("x".to_string()),
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
                panic!("a write placed on a layer the session does not hold is refused: {other:?}")
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let doc: Entity = "doc:1".parse()?;
        let transaction = session
            .transaction()
            .assert(dialog_query::the!("ui/selected").of(doc.clone()).is(true))
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
        Ok(())
    }
}
