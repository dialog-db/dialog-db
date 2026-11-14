//! A session: the primary branch read and written as one with the
//! layers its schema declares beside it.
//!
//! The primary's schema says which layers exist ([`LayerDeclaration`]).
//! Opening a session on the primary builds every layer the schema
//! declares in memory and reads them with the primary as one composite.
//! Where a row lies is a dimension of the row, `in`: a read with `in`
//! unbound reads every layer and binds each row's, one with `in` bound
//! reads that layer alone, and a write lands in the layer its
//! descriptor names (`in:`, or [`Statement::at`]), the primary with none
//! named. A transaction through the session routes each write so: the
//! primary's share is committed and published as any branch commit is,
//! and each memory layer's share lands in it once the publish does, so
//! no layer shows a commit that was refused. The layers every session
//! has are named [`MEMORY`], the primary, [`SESSION`], the session's own,
//! and [`CONTROL`], the loopback a command lands in.
//!
//! ```no_run
//! # use dialog_repository::{Branch, LayerDeclaration, Store};
//! # use dialog_corpus::Statement as _;
//! # async fn example(branch: Branch, env: impl dialog_repository::SessionEnv) -> anyhow::Result<()> {
//! let state: dialog_corpus::Entity = "session:state".parse()?;
//! branch
//!     .transaction()
//!     .assert(LayerDeclaration::new(state.clone(), Store::Memory))
//!     .commit()
//!     .publish()
//!     .perform(&env)
//!     .await?;
//! let session = branch.session().perform(&env).await?;
//! session
//!     .transact(dialog_query::the!("ui/selected").of("doc:1".parse()?).is(true).at(state))
//!     .perform(&env)
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! A session is driven by dispatch ([`Session::invoke`], and
//! [`Session::transact`] for writes with no command): each dispatch is
//! one unit of publication, re-run from the new head when the primary's
//! head moved under it (see [`invoke`]). [`Session::transaction`] is the
//! same commit without the re-run, for a caller that handles a lost head
//! itself. Once a dispatch has published, the session's handlers
//! ([`Session::handle`], [`invoke::Handler`]) answer its commands, and
//! their answers are the next tick's stimulus.
//!
//! Every session has its own layer, [`SESSION`], whether or not the
//! schema declares it: a table in memory, the session's alone, gone
//! when the process is. It is how a session addresses the process-local
//! store a branch has always had: [`Session::layer`] hands it out for
//! [`session_layer`](crate::session_layer), a write `.at(session_layer())`
//! lands in it once the dispatch publishes, and a read binds its rows'
//! `in` to it. [`Branch::overlay`] stays, as the same store under its
//! older name: what is written through one is read through the other.
//!
//! After each dispatch publishes, the session records in its own layer
//! the revision every layer stands at, one `dialog.layer/revision` row
//! per layer, under `last`: of [`MEMORY`] the primary's tree, of each
//! declared layer its instant chain, and of [`SESSION`] its own as it
//! stood before the row was written. The same rows, as the layers stood
//! when the dispatch began, are its stimulus, so a rule premised on a
//! layer's revision fires once per dispatch.
//!
//! The session's own layer also holds which layer is **selected**
//! ([`Session::select`]): while one is, a write with no layer named
//! lands in it rather than the primary, as a stroke lands on the layer
//! an image editor has selected. The machinery under `dialog.` (rules,
//! layer declarations, the revision rows) never follows the selection.
//!
//! What the session holds of its own are its **declarations**
//! ([`Session::declare`]): layers this process declares for itself,
//! which outrank what the primary's tree and session store declare, and
//! reach no layer and no peer.
//!
//! [`MEMORY`]: crate::MEMORY
//! [`SESSION`]: crate::SESSION
//! [`CONTROL`]: crate::CONTROL

pub mod invoke;

use std::iter::once;
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

use crate::placement::{Layers, Selection, Store, memory_layer, selected, session_layer};
use crate::repository::branch::session::Composite;
use crate::repository::branch::transaction::SessionContext;
use crate::repository::source::Source;
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
        let layers = Layers::resolve(&primary, &self.declared, env).await?;
        // The primary and the session's own layer are every session's,
        // whatever the schema says of them: a declaration naming either
        // builds nothing apart.
        let (memory, own) = (memory_layer(), session_layer());
        let layers = layers
            .layers()
            .into_iter()
            .filter(|(entity, _)| *entity != memory && *entity != own)
            .map(|(entity, store)| match store {
                Store::Memory => (entity, Ephemeral::new()),
            })
            .collect();
        Ok(Session {
            inner: Arc::new(Inner {
                primary: self.primary,
                layers,
                declared: RwLock::new(self.declared),
                handlers: RwLock::new(Vec::new()),
                feed: RwLock::new(invoke::Feed::default()),
            }),
        })
    }
}

struct Inner {
    primary: Branch,
    /// The layers the schema declares, each with the entity that names
    /// it, in the order the schema lists them. The session's own layer,
    /// [`SESSION`](crate::SESSION), is not among them: it is the
    /// primary's process-local store ([`Branch::overlay`]), read with the
    /// primary.
    layers: Vec<(Entity, Ephemeral)>,
    declared: RwLock<Changes>,
    /// What answers a dispatch's commands, in registration order. See
    /// [`Session::handle`].
    handlers: RwLock<Vec<Arc<dyn invoke::Handler>>>,
    /// The session's dispatches, for readers of its command feed. See
    /// [`Session::commands`].
    feed: RwLock<invoke::Feed>,
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

    /// The layer named `name`, if the session holds one: the session's
    /// own, [`SESSION`](crate::SESSION), which every session has, or one
    /// the schema declares. The session's own is the primary's
    /// process-local store, the one [`Branch::overlay`] hands out: a
    /// clone of either shares its state.
    pub fn layer(&self, name: &Entity) -> Option<&Ephemeral> {
        if *name == session_layer() {
            return Some(self.inner.primary.overlay());
        }
        self.inner
            .layers
            .iter()
            .find(|(layer, _)| layer == name)
            .map(|(_, memory)| memory)
    }

    /// The layers this session holds beside the primary, by name: its
    /// own, [`SESSION`](crate::SESSION), first, then the ones the schema
    /// declares, in the schema's order.
    pub fn layers(&self) -> Vec<Entity> {
        once(session_layer())
            .chain(self.inner.layers.iter().map(|(layer, _)| layer.clone()))
            .collect()
    }

    /// Select `layer`, or clear the selection with `None`: a dispatch
    /// writing the session's own layer's `dialog.session/layer` row of
    /// [`SESSION`](crate::SESSION), under `last`. While a layer is
    /// selected, a write with no layer named, and a rule head whose
    /// descriptor names none, lands in it rather than the primary,
    /// except a write under `dialog.` (rules, layer declarations,
    /// revisions), which never follows the selection. Selecting
    /// [`MEMORY`](crate::MEMORY) is selecting the primary. A selected
    /// layer the session does not hold refuses the commit that would
    /// land a write in it with [`CommitError::UnboundLayer`].
    ///
    /// The selection takes effect from the dispatch after the one that
    /// writes it, once it has published.
    pub fn select(&self, layer: Option<Entity>) -> invoke::Invocation {
        match (layer, self.selected()) {
            (Some(layer), _) => self.transact(Selection(layer)),
            (None, Some(current)) => self.transact(Changes::new()).retract(Selection(current)),
            (None, None) => self.transact(Changes::new()),
        }
    }

    /// The layer selected in the session's own layer, if any (see
    /// [`select`](Self::select)).
    pub fn selected(&self) -> Option<Entity> {
        selected(self.inner.primary.overlay())
    }

    /// Hold `declaration` in this session: a
    /// [`LayerDeclaration`](crate::LayerDeclaration), or a [`Changes`] of
    /// them, whose retractions undo what an earlier call declared or what
    /// the primary declares. It is this process's own: it outranks a
    /// declaration the primary's session store or tree holds and reaches
    /// no layer and no peer. A layer declared here after the session
    /// opened is not built: declare it on [`Branch::session`] instead.
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

    /// The memory layers the schema declares, by name, in the schema's
    /// order. The session's own layer is not among them: a commit and a
    /// read take it from the primary, as its session overlay.
    pub(crate) fn memories(&self) -> Vec<(Entity, Ephemeral)> {
        self.inner.layers.clone()
    }

    /// The layers a transaction through this session reads and induces
    /// over, pinned as `primary`.
    fn composite(&self, primary: Source) -> Composite {
        Composite {
            sources: vec![primary],
            memories: self.memories(),
        }
    }

    /// A query over the session: the primary and every layer. A read
    /// with `in` unbound reads them all and binds each row's layer; one
    /// with `in` bound reads that layer alone.
    pub fn query(&self) -> QueryLayer<'_> {
        // The primary's line reads its session overlay as `session:`, so
        // the session's own layer is read once, with the primary.
        let mut layer = QueryLayer::from(&self.inner.primary);
        for (name, memory) in &self.inner.layers {
            layer = layer.with_memory(name.clone(), memory);
        }
        layer
    }

    /// Start a transaction through the session. Its reads see every
    /// layer; its commit routes each write to the layer its descriptor
    /// names (`in:`), the primary with none named, and the memory layers
    /// take their shares once the primary's commit publishes.
    pub fn transaction(&self) -> Transaction<&Branch> {
        let transaction = self.inner.primary.transaction();
        let primary = transaction.view().to_source();
        transaction.with_session(SessionContext {
            composite: self.composite(primary),
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
    use crate::{Answering, FEED_CAPACITY, Handler, HandlerError, Lost, MAX_ANSWERS};
    use crate::{LayerDeclaration, PublishError};
    use anyhow::Result;
    use dialog_common::ConditionalSend;
    use dialog_corpus::Pick;
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    type Operator = Peer<VolatileSpace, dialog_peer::Session>;

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
            .assert(LayerDeclaration::new(state.clone(), Store::Memory))
            .assert(increment)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    /// A session builds the layers the primary's schema declares and
    /// routes each write to the layer it names: the tree never holds a
    /// fact written into the memory layer, and the layer never holds one
    /// written with no layer named.
    #[dialog_common::test]
    async fn it_routes_each_write_to_the_layer_it_names() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        assert_eq!(
            session.layers(),
            vec![crate::session_layer(), state.clone()],
            "the session's own layer comes first, then the schema's"
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
            read::<bool>(session.query(), &operator, "ui/selected", &doc).await?,
            vec!["Boolean(true)".to_string()]
        );
        assert_eq!(
            read::<String>(session.query(), &operator, "doc/title", &doc).await?,
            vec!["String(\"Notes\")".to_string()]
        );
        Ok(())
    }

    /// Every session has its own layer, declared or not: it is the
    /// primary's process-local store, so what is written through one name
    /// is read through the other, and a read binds its rows once, to
    /// `session:`. A schema that declares `session:` builds nothing apart.
    #[dialog_common::test]
    async fn it_holds_its_own_layer_whether_or_not_the_schema_declares_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let own = crate::session_layer();
        let session = branch.session().perform(&operator).await?;
        assert_eq!(
            session.layers(),
            vec![own.clone()],
            "undeclared, still held"
        );

        let doc: Entity = "doc:1".parse()?;
        session
            .transact(flag(&doc, true, &own))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(branch.overlay(), "ui/selected", &doc)?,
            vec!["Boolean(true)".to_string()],
            "a write into `session:` lands in the branch's overlay"
        );
        branch
            .overlay()
            .assert(primary_flag(&"doc:2".parse()?, true))?;
        let layer = session.layer(&own).expect("the session's own layer");
        assert_eq!(
            held(layer, "ui/selected", &"doc:2".parse()?)?,
            vec!["Boolean(true)".to_string()],
            "a write into the overlay is read through `session:`"
        );

        let claims: Vec<dialog_query::Claim> = session
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
            vec![
                format!("{doc} in session:"),
                "doc:2 in session:".to_string()
            ],
            "each row is read once, as `session:`"
        );

        // A schema that declares `session:` gets the same one layer.
        branch
            .transaction()
            .assert(LayerDeclaration::new(own.clone(), Store::Memory))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let declared = branch.session().perform(&operator).await?;
        assert_eq!(declared.layers(), vec![own.clone()]);
        assert!(
            !held(
                declared.layer(&own).expect("its own layer"),
                "ui/selected",
                &doc
            )?
            .is_empty(),
            "still the branch's overlay"
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        session
            .transaction()
            .assert(count(&counter, 1, &state))
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        // Another handle moves the head; this session's handle is stale.
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
        let published = session
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
            published.version().edition,
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        let before = session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let after = session
            .invoke(
                dialog_query::the!("cmd.increment/counter")
                    .of("cmd:1".parse::<Entity>()?)
                    .is(counter.clone()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(after, before, "nothing durable was written");
        assert_eq!(branch.revision(), Some(before));
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()]
        );
        Ok(())
    }

    /// A write named for a layer the session does not hold is refused
    /// before anything changes: the primary's head and every layer stay as
    /// they were.
    #[dialog_common::test]
    async fn it_refuses_a_write_named_for_a_layer_the_session_does_not_hold() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let before = branch.revision();
        let doc: Entity = "doc:1".parse()?;
        let refused = session
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
                panic!("a write named for a layer the session does not hold is refused: {other:?}")
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
        Ok(())
    }

    /// The `dialog.layer/revision` row `own` holds of `layer`, as text.
    fn revision_of(own: &Ephemeral, layer: &Entity) -> Result<Vec<String>> {
        held(own, "dialog.layer/revision", layer)
    }

    /// The primary's revision as its `dialog.layer/revision` row names it.
    fn tree_text(revision: &crate::Revision) -> String {
        use base58::ToBase58 as _;
        let tree: &[u8] = revision.tree.hash();
        format!("{:?}", dialog_corpus::Value::String(tree.to_base58()))
    }

    /// After each dispatch publishes, the session's own layer holds one
    /// `dialog.layer/revision` row per layer, under `last`: a second
    /// dispatch replaces the rows the first wrote, and the primary's row
    /// follows the primary's head.
    #[dialog_common::test]
    async fn it_records_each_layers_revision_in_its_own_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let own_name = crate::session_layer();
        let own = session.layer(&own_name).expect("its own layer").clone();
        let memory = crate::memory_layer();
        let doc: Entity = "doc:1".parse()?;

        let mut primary_rows = Vec::new();
        for title in ["First", "Second"] {
            let revision = session
                .transact(
                    dialog_query::the!("doc/title")
                        .of(doc.clone())
                        .is(title.to_string()),
                )
                .assert(flag(&doc, title == "First", &state))
                .perform(&operator)
                .await?;
            let rows = own.scan(&Selector::new().the(crate::revision_attr()));
            assert_eq!(rows.len(), 3, "one row per layer: {rows:?}");
            for layer in [&memory, &state, &own_name] {
                assert_eq!(revision_of(&own, layer)?.len(), 1, "one row of {layer}");
            }
            let primary = revision_of(&own, &memory)?;
            assert_eq!(primary, vec![tree_text(&revision)], "the published tree");
            primary_rows.push(primary);
        }
        assert_ne!(
            primary_rows[0], primary_rows[1],
            "the primary's row moved with its head"
        );
        Ok(())
    }

    /// A rule premised on `dialog.layer/revision` fires on every dispatch,
    /// with the revisions the layers stood at as the dispatch began.
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
        branch
            .transaction()
            .assert(watch)
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory_name = crate::memory_layer();
        let memory = session.layer(&state).expect("the state layer").clone();
        let own = session
            .layer(&crate::session_layer())
            .expect("its own layer")
            .clone();
        let doc: Entity = "doc:1".parse()?;

        let start = branch.revision().expect("a revision");
        session
            .transact(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "seen/revision", &memory_name)?,
            vec![tree_text(&start)],
            "the rule fired, on the primary's revision as the dispatch began"
        );

        let recorded = revision_of(&own, &memory_name)?;
        session
            .transact(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "seen/revision", &memory_name)?,
            recorded,
            "it fires again, on the revision the last dispatch recorded"
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let doc: Entity = "doc:1".parse()?;
        let title = |text: &str| {
            dialog_query::the!("doc/title")
                .of(doc.clone())
                .is(text.to_string())
        };

        assert_eq!(session.selected(), None);
        session
            .select(Some(state.clone()))
            .perform(&operator)
            .await?;
        assert_eq!(session.selected(), Some(state.clone()));

        let before = branch.revision();
        session.transact(title("Notes")).perform(&operator).await?;
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
        session
            .transact(LayerDeclaration::new(other.clone(), Store::Memory))
            .perform(&operator)
            .await?;
        branch.refresh(&operator).await?;
        assert_ne!(branch.revision(), before, "the declaration committed");
        assert_eq!(
            committed::<String>(&branch, &operator, "dialog.layer/store", &other).await?,
            vec!["String(\"memory\")".to_string()],
            "a declaration never follows the selection"
        );
        assert!(held(&memory, "dialog.layer/store", &other)?.is_empty());

        session.select(None).perform(&operator).await?;
        assert_eq!(session.selected(), None, "the selection is cleared");
        session.transact(title("Other")).perform(&operator).await?;
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        session
            .select(Some(state.clone()))
            .perform(&operator)
            .await?;

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
        session.invoke(marked).perform(&operator).await?;
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

    /// A selected layer the session does not hold refuses the commit
    /// that would land a write in it, and nothing is written: not the
    /// tree, not a layer.
    #[dialog_common::test]
    async fn it_refuses_a_write_into_a_selected_layer_the_session_does_not_hold() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let nowhere = layer("nowhere");
        session
            .select(Some(nowhere.clone()))
            .perform(&operator)
            .await?;

        let doc: Entity = "doc:1".parse()?;
        let before = branch.revision();
        let refused = session
            .transact(
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

    /// What a fresh evaluation of `query` through the session reads.
    async fn fresh(
        session: &Session,
        env: &Operator,
        query: AttributeQuery,
    ) -> Result<Vec<String>> {
        let claims: Vec<dialog_query::Claim> =
            session.query().select(query).perform(env).try_vec().await?;
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

    /// A subscription through a session reads every layer, and a share a
    /// dispatch lands in a memory layer propagates as a delta on the
    /// next poll, maintained from the layer's instants. A commit to the
    /// primary outside the cover advances the pins for free.
    #[dialog_common::test]
    async fn it_subscribes_through_a_session() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        let mut subscription = session.subscribe(selected_query());
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty());

        session
            .transact(flag(&doc, true, &state))
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

        session
            .transact(
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

        session
            .transact(Changes::new())
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
        let session = branch.session().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(flag(&doc, true, &state))
            .assert(primary_flag(&doc, false))
            .perform(&operator)
            .await?;

        let rows = |query: AttributeQuery| {
            let session = session.clone();
            let operator = &operator;
            async move {
                let claims: Vec<dialog_query::Claim> = session
                    .query()
                    .select(query)
                    .perform(operator)
                    .try_vec()
                    .await?;
                let mut rows: Vec<String> = claims
                    .iter()
                    .map(|claim| {
                        format!(
                            "{:?} in {}",
                            claim.is,
                            claim.layer().map(ToString::to_string).unwrap_or_default()
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
    /// written in both, a maintained subscription equals a fresh
    /// evaluation after every step, and nothing after the first poll
    /// evaluates afresh.
    #[dialog_common::test]
    async fn it_agrees_with_a_fresh_evaluation_through_session_churn() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let docs: Vec<Entity> = (1..=4)
            .map(|index| format!("doc:{index}").parse())
            .collect::<Result<_, _>>()?;
        let counter: Entity = "ctr:1".parse()?;
        session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let mut subscription = session.subscribe(selected_query());
        subscription.poll(&operator).await?;

        for seed in [7u64, 99, 2024] {
            let mut rng = Xorshift(seed);
            for step in 0..24 {
                let doc = docs[rng.below(docs.len() as u64) as usize].clone();
                let label = match rng.below(6) {
                    0 => {
                        session
                            .transact(flag(&doc, true, &state))
                            .perform(&operator)
                            .await?;
                        "select"
                    }
                    1 => {
                        session
                            .transact(flag(&doc, false, &state))
                            .perform(&operator)
                            .await?;
                        "deselect"
                    }
                    2 => {
                        session
                            .transact(Changes::new())
                            .retract(flag(&doc, true, &state))
                            .perform(&operator)
                            .await?;
                        "retract"
                    }
                    3 => {
                        session
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
                        session
                            .transact(primary_flag(&doc, step % 2 == 0))
                            .perform(&operator)
                            .await?;
                        "select in the primary"
                    }
                    _ => "nothing",
                };
                subscription.poll(&operator).await?;
                let mut retained = selected(subscription.results());
                retained.sort();
                assert_eq!(
                    retained,
                    fresh(&session, &operator, selected_query()).await?,
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
            .assert(LayerDeclaration::new(state.clone(), Store::Memory))
            .assert(capture)
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(state)
    }

    /// A handler answering every `cmd.fetch/node` with `cmd.fetched/node`
    /// of the same command, counting its calls.
    fn fetcher(calls: Arc<AtomicUsize>) -> Arc<dyn Handler> {
        use dialog_corpus::{Change, Update as _};
        Arc::new(Answering(move |commands: &Changes| {
            calls.fetch_add(1, Ordering::SeqCst);
            let fetch: dialog_corpus::Relation = "cmd.fetch/node".parse().expect("attribute");
            let fetched: dialog_corpus::Relation = "cmd.fetched/node".parse().expect("attribute");
            let mut answers = Changes::new();
            for (of, the, change) in commands.iter() {
                if *the == fetch
                    && let Change::Assert(node, _) = change
                {
                    answers.associate(fetched.clone(), of.clone(), node.clone(), Pick::All);
                }
            }
            Ok(answers)
        }))
    }

    fn fetch(command: &str, node: &Entity) -> impl Statement {
        dialog_query::the!("cmd.fetch/node")
            .of(command.parse::<Entity>().expect("entity"))
            .is(node.clone())
    }

    /// A command a dispatch leaves to the session's handlers is answered
    /// with a command, dispatched as the next tick's stimulus, where a
    /// rule captures it on the layer its attribute is placed on; a
    /// subscription through the session sees the capture, and nothing
    /// durable is written.
    #[dialog_common::test]
    async fn it_answers_a_command_through_a_handler() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let calls = Arc::new(AtomicUsize::new(0));
        session.handle(fetcher(calls.clone()));
        let node: Entity = "node:1".parse()?;
        let mut subscription = session.subscribe(AttributeQuery::from(
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

        let after = session
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;

        assert_eq!(Some(after), before, "nothing durable was written");
        assert_eq!(
            held(&memory, "node/fetched", &node)?,
            vec!["Entity(cmd:1)".to_string()],
            "the rule captured the answer on the state layer"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "offered the fetch, then the answer it did not take"
        );
        let delta = subscription.poll(&operator).await?.expect("the capture");
        assert_eq!(delta.asserted.len(), 1);
        assert_eq!(delta.asserted[0].of, node);
        Ok(())
    }

    /// Handlers that answer every command with another are stopped at
    /// the bound, with `AnswerDivergence`, and nothing of theirs lands.
    #[dialog_common::test]
    async fn it_stops_handlers_that_never_stop_answering() -> Result<()> {
        use dialog_corpus::Update as _;
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        session.handle(Arc::new(Answering(|commands: &Changes| {
            let mut answers = Changes::new();
            for (of, _, _) in commands.iter() {
                answers.associate(
                    "cmd.again/node".parse().expect("attribute"),
                    of.clone(),
                    dialog_corpus::Value::Boolean(true),
                    Pick::All,
                );
            }
            Ok(answers)
        })));
        let before = branch.revision();
        let refused = session
            .invoke(fetch("cmd:1", &"node:1".parse()?))
            .perform(&operator)
            .await;
        assert!(
            matches!(refused, Err(CommitError::AnswerDivergence(MAX_ANSWERS))),
            "{refused:?}"
        );
        assert_eq!(branch.revision(), before);
        assert!(held(&memory, "node/fetched", &"node:1".parse()?)?.is_empty());
        Ok(())
    }

    /// A handler that fails fails the invocation with `Handler`, after
    /// the dispatch it answers has published: what the dispatch wrote
    /// stands, since the effect runs only once the writes are durable.
    #[dialog_common::test]
    async fn it_reports_a_handler_that_fails_after_the_dispatch_published() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_fetch(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        session.handle(Arc::new(Answering(|_: &Changes| {
            Err(HandlerError("no network".into()))
        })));
        let doc: Entity = "doc:1".parse()?;
        let failed = session
            .invoke(fetch("cmd:1", &"node:1".parse()?))
            .assert(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Notes".to_string()),
            )
            .perform(&operator)
            .await;
        match failed {
            Err(CommitError::Handler(reason)) => assert_eq!(reason, "no network"),
            other => panic!("expected the handler's failure, got {other:?}"),
        }
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &doc).await?,
            vec!["String(\"Notes\")".to_string()],
            "the dispatch published before its handler ran"
        );
        Ok(())
    }

    /// A consumer outside the invoke loop reads the session's command
    /// feed, in order and once, and answers by invoking: the answer is a
    /// dispatch of its own, which the feed records too, and the rule
    /// captures it as it captures a handler's answer.
    #[dialog_common::test]
    async fn it_feeds_each_dispatch_to_a_reader_that_answers_by_invoking() -> Result<()> {
        use dialog_corpus::{Change, Update as _};
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let mut feed = session.commands();
        assert!(feed.poll()?.is_empty(), "nothing dispatched yet");
        let node: Entity = "node:1".parse()?;
        session
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        session
            .invoke(fetch("cmd:2", &node))
            .perform(&operator)
            .await?;
        assert!(
            held(&memory, "node/fetched", &node)?.is_empty(),
            "no handler: nothing answered yet"
        );

        let dispatches = feed.poll()?;
        assert_eq!(
            dispatches
                .iter()
                .map(|dispatched| dispatched.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        let fetch_attr: dialog_corpus::Relation = "cmd.fetch/node".parse()?;
        let mut answers = Changes::new();
        for dispatched in &dispatches {
            for (of, the, change) in dispatched.commands.iter() {
                if *the == fetch_attr
                    && let Change::Assert(is, _) = change
                {
                    answers.associate(
                        "cmd.fetched/node".parse()?,
                        of.clone(),
                        is.clone(),
                        Pick::All,
                    );
                }
            }
        }
        session.invoke(answers).perform(&operator).await?;
        assert_eq!(
            held(&memory, "node/fetched", &node)?,
            vec!["Entity(cmd:1)".to_string(), "Entity(cmd:2)".to_string()]
        );
        let answered = feed.poll()?;
        assert_eq!(answered.len(), 1, "the answer was one dispatch");
        assert_eq!(answered[0].sequence, 3);
        assert!(feed.poll()?.is_empty(), "read once");
        Ok(())
    }

    /// A reader the feed moved past is told so, and starts over from the
    /// newest dispatch.
    #[dialog_common::test]
    async fn it_tells_a_reader_the_feed_moved_past_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_fetch(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let mut feed = session.commands();
        let node: Entity = "node:1".parse()?;
        for index in 0..=FEED_CAPACITY {
            session
                .invoke(fetch(&format!("cmd:{index}"), &node))
                .perform(&operator)
                .await?;
        }
        let lost = feed.poll();
        assert!(
            matches!(
                lost,
                Err(Lost {
                    behind: 0,
                    oldest: 2
                })
            ),
            "{lost:?}"
        );
        feed.catch_up();
        assert!(feed.poll()?.is_empty());
        session
            .invoke(fetch("cmd:again", &node))
            .perform(&operator)
            .await?;
        assert_eq!(feed.poll()?.len(), 1);
        Ok(())
    }
}
