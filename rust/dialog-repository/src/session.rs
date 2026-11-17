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
//! itself.
//!
//! A command is a write into [`CONTROL`], the loopback: one dispatched
//! ([`Invocation::dispatch`](invoke::Invocation::dispatch)), one written
//! `.at(control_layer())`, or a rule head landing there (its descriptor
//! names `in: control:`, or its concept carries the
//! `dialog.concept/transient` marker). It is the stimulus of the next
//! tick, read as a row of `control:` by a rule premised on it, and gone
//! after that tick. Once a dispatch has published, its commands land in
//! the session's `control:` layer as one instant and leave it in the
//! next: a **provider**, something that performs an effect, reads them
//! there with [`Session::arrivals`], each once, and answers by invoking
//! ([`Session::invoke`]), naming the command it answers by its entity.
//! A provider outside the invoke loop, or outside the process, reads a
//! **channel** instead, `control:<name>`, a layer declared with
//! [`Store::Control`] whose rows stay until the consumer retracts them.
//!
//! A layer declared with [`Store::Durable`], `memory:<name>`, is a
//! branch: the repository's branch `<name>`, opened with the session and
//! read as a line beside the primary, its rows binding `in` to
//! `memory:<name>` ([`Session::durable`] hands the branch out). A write
//! into it is committed on that branch as part of the dispatch's
//! publication, and the primary records the revision it lands at as a
//! pointer fact, `memory:<name> dialog.layer/revision <tree>`, in the
//! same dispatch: the layers publish bottom to top, the primary last,
//! and a lost compare-and-swap on any of them re-runs the dispatch from
//! the new heads. [`Session::sync`] pulls and pushes the primary and
//! every durable layer with upstreams.
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
//! The session records in its own layer the revision every layer
//! stands at, one `dialog.layer/revision` row per layer, under `last`:
//! of [`MEMORY`] the primary's tree, of each durable layer its branch's
//! tree, of each memory layer its instant chain, and of [`SESSION`] its
//! own chain as it stood before the recordings that followed its last
//! change. A row is written only when its layer **moved**, when the
//! revision it stands at is not the one the session holds for it: after
//! a dispatch publishes, for the layers its commit and its writes moved,
//! and after [`Session::sync`] pulls and pushes, for the layers the
//! pulls moved. Writing the rows is not itself a move of the session's
//! own layer: its row moves only when something else was written into
//! `session:`, a dispatch's writes into it among them, and then names
//! the revision those writes left it at. So a dispatch that writes
//! nothing into `session:` and moves no other layer writes no row and
//! mints no instant in `session:`. Of the same rows, as the layers stood
//! when a dispatch began, those the session has not recorded are its
//! stimulus, so a rule premised on a layer's revision fires when the
//! layer moved outside a dispatch (a pull moved the primary or a durable
//! layer, something wrote into a memory layer directly) and on the
//! session's first dispatch, which finds nothing recorded.
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

pub mod arrivals;
pub mod invoke;
pub mod sync;

pub use arrivals::Arrivals;
pub use sync::{SyncEnv, SyncError, SyncSession};

use std::iter::once;
use std::sync::Arc;

use dialog_capability::{Fork, Provider};
use dialog_common::{ConditionalSync, Holds};
use dialog_corpus::tree::CorpusNode;
use dialog_corpus::{Change, Changes, Entity, Statement, Update as _};
use dialog_effects::archive::Load as ArchiveLoad;
use dialog_effects::archive::{Get, Import, Put};
use dialog_effects::authority::{Attest, Identify};
use dialog_effects::blob::{
    Import as BlobImport, Load as BlobLoad, Read as BlobRead, Size as BlobSize,
};
use dialog_effects::memory::{Publish, Resolve};
use dialog_search_tree::nodes;
use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::RepositoryMemoryExt as _;
use crate::placement::{
    Layers, Selection, Store, control_layer, durable_branch, memory_layer, revision_rows, selected,
    session_layer,
};
use crate::repository::branch::session::Composite;
use crate::repository::branch::transaction::{DurableLayer, SessionContext};
use crate::repository::source::Source;
use crate::{Branch, CommitError, Ephemeral, QueryLayer, RemoteSite, Revision, Transaction};

/// What opening a session and reading through it need of the
/// environment: a read of the primary, to resolve the layers its schema
/// declares, the open of each durable layer's branch, and what a query
/// through the session reads with. Nothing in it commits or publishes:
/// an environment that can open branches and query them opens a session
/// beside them ([`OpenSession::perform`]), and reads what arrives in its
/// layers ([`Arrivals`]). Dispatching through the session needs
/// [`SessionEnv`].
pub trait OpenSessionEnv:
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

impl<T> OpenSessionEnv for T where
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

/// What a session needs of its environment to dispatch through it: what
/// opening it and reading through it need ([`OpenSessionEnv`]), and what
/// a transaction's commit needs, to publish through it.
pub trait SessionEnv:
    OpenSessionEnv
    + Provider<Attest>
    + Provider<Publish>
    + Provider<Import>
    + Provider<BlobImport>
    + Provider<BlobSize>
{
}

impl<T> SessionEnv for T where
    T: OpenSessionEnv
        + Provider<Attest>
        + Provider<Publish>
        + Provider<Import>
        + Provider<BlobImport>
        + Provider<BlobSize>
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
    /// declares. Opening reads and opens branches, and commits nothing,
    /// so it needs no more of `env` than a read does
    /// ([`OpenSessionEnv`]).
    pub async fn perform<Env: OpenSessionEnv>(self, env: &Env) -> Result<Session, CommitError> {
        let primary = Source::from(self.primary.clone());
        let layers = Layers::resolve(&primary, &self.declared, env).await?;
        // The primary, the session's own layer and the loopback are
        // every session's, whatever the schema says of them: a
        // declaration naming one builds nothing apart.
        let (memory, own, control) = (memory_layer(), session_layer(), control_layer());
        let mut memories = Vec::new();
        let mut durables = Vec::new();
        for (entity, store) in layers.layers() {
            if entity == memory || entity == own || entity == control {
                continue;
            }
            match store {
                Store::Memory | Store::Control => memories.push((entity, Ephemeral::new())),
                // A durable layer is the repository's branch it names,
                // opened through `env` as any branch is.
                Store::Durable => {
                    let name = durable_branch(&entity)?;
                    let branch = self
                        .primary
                        .subject()
                        .branch(name)
                        .open()
                        .perform(env)
                        .await?;
                    durables.push((entity, branch));
                }
            }
        }
        Ok(Session {
            inner: Arc::new(Inner {
                primary: self.primary,
                layers: memories,
                durables,
                declared: RwLock::new(self.declared),
                loopback: Loopback::default(),
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
    /// The durable layers the schema declares, each with the entity that
    /// names it, `memory:<name>`, and the branch `<name>` it is, in the
    /// order the schema lists them.
    durables: Vec<(Entity, Branch)>,
    declared: RwLock<Changes>,
    /// The loopback, [`CONTROL`](crate::CONTROL), as the session holds
    /// it between dispatches, and what wakes a reader of it.
    loopback: Loopback,
}

/// The session's loopback, [`CONTROL`](crate::CONTROL), as it holds it
/// between dispatches: empty, but for the one instant each published
/// dispatch's commands stand in it, so its instants are every command,
/// in order, once. A dispatch's own ticks read their commands as rows of
/// `control:` without it. Every transaction through the session carries
/// it, so the publish that lands the dispatch lands its commands here,
/// whichever way the transaction was driven. Cheap to clone: clones
/// share the layer and the waiters.
#[derive(Clone, Default)]
pub(crate) struct Loopback {
    control: Ephemeral,
    /// Woken once each dispatch through the session has published and
    /// landed its commands: what [`Arrivals::next`] waits on.
    landed: Arc<Notify>,
}

impl Loopback {
    /// Land a published dispatch's `commands` for one tick: asserted as
    /// one instant, gone in the next, so a reader of the layer's
    /// instants sees each once and nothing of them stays. A retraction
    /// among them dispatched nothing to read and lands nothing. Wakes
    /// every [`Arrivals`] waiting for the next dispatch.
    pub(crate) fn land(&self, commands: &Changes) -> Result<(), CommitError> {
        let mut asserted = Changes::new();
        for (of, the, change) in commands.iter() {
            if let Change::Assert(is, pick) = change {
                asserted.associate(the.clone(), of.clone(), is.clone(), pick.clone());
            }
        }
        if !asserted.is_empty() {
            self.control.apply(asserted)?;
            self.control.clear();
        }
        self.landed.notify_waiters();
        Ok(())
    }
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
    /// own, [`SESSION`](crate::SESSION), which every session has, the
    /// loopback, [`CONTROL`](crate::CONTROL), which every session has
    /// too, or one the schema declares (a memory layer or a channel).
    /// The session's own is the primary's process-local store, the one
    /// [`Branch::overlay`] hands out: a clone of either shares its state.
    /// The loopback holds nothing between dispatches; its instants are
    /// the commands of each published dispatch (see [`invoke`]).
    pub fn layer(&self, name: &Entity) -> Option<&Ephemeral> {
        if *name == session_layer() {
            return Some(self.inner.primary.overlay());
        }
        if *name == control_layer() {
            return Some(&self.inner.loopback.control);
        }
        self.inner
            .layers
            .iter()
            .find(|(layer, _)| layer == name)
            .map(|(_, memory)| memory)
    }

    /// The layers this session holds beside the primary, by name: its
    /// own, [`SESSION`](crate::SESSION), first, then the memory layers
    /// the schema declares, in the schema's order, then its durable
    /// layers, in the schema's order. The loopback,
    /// [`CONTROL`](crate::CONTROL), holds no row between dispatches and
    /// is not among them, though [`layer`](Self::layer) hands it out.
    pub fn layers(&self) -> Vec<Entity> {
        once(session_layer())
            .chain(self.inner.layers.iter().map(|(layer, _)| layer.clone()))
            .chain(self.inner.durables.iter().map(|(layer, _)| layer.clone()))
            .collect()
    }

    /// The branch the durable layer `name` is, if the session holds one
    /// by that name: the repository's branch `<name>` of
    /// `memory:<name>`, declared with [`Store::Durable`]. A durable layer
    /// holds no [`Ephemeral`]: [`layer`](Self::layer) answers `None` for
    /// it.
    pub fn durable(&self, name: &Entity) -> Option<&Branch> {
        self.inner
            .durables
            .iter()
            .find(|(layer, _)| layer == name)
            .map(|(_, branch)| branch)
    }

    /// The durable layers this session holds, by name, with the branch
    /// each is, in the schema's order.
    pub fn durables(&self) -> &[(Entity, Branch)] {
        &self.inner.durables
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

    /// Whether a layer of the session stands somewhere other than the
    /// `dialog.layer/revision` row its own layer holds of it: what the
    /// next dispatch would record (see the [module docs](self)).
    pub(crate) fn unrecorded(&self) -> bool {
        let durables: Vec<(Entity, Option<Revision>)> = self
            .inner
            .durables
            .iter()
            .map(|(layer, branch)| (layer.clone(), branch.revision()))
            .collect();
        !revision_rows(
            self.inner.primary.revision().as_ref(),
            &durables,
            &self.inner.layers,
            self.inner.primary.overlay(),
        )
        .is_empty()
    }

    /// The memory layers the schema declares, by name, in the schema's
    /// order. The session's own layer is not among them: a commit and a
    /// read take it from the primary, as its session overlay.
    pub(crate) fn memories(&self) -> Vec<(Entity, Ephemeral)> {
        self.inner.layers.clone()
    }

    /// The layers a transaction through this session reads and induces
    /// over: the primary pinned as `primary`, each durable layer as
    /// `durables` pins it, and the memory layers.
    fn composite(&self, primary: Source, durables: &[DurableLayer]) -> Composite {
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
        for (name, branch) in &self.inner.durables {
            layer = layer.with_line(name.clone(), branch);
        }
        layer
    }

    /// Start a transaction through the session. Its reads see every
    /// layer; its commit routes each write to the layer its descriptor
    /// names (`in:`), the primary with none named, and the memory layers
    /// take their shares once the primary's commit publishes. Once its
    /// publish has won, the commands it dispatched (with
    /// [`dispatch`](Transaction::dispatch), or written
    /// `.at(control_layer())`) and the ones its rules dispatched land in
    /// the session's [`CONTROL`](crate::CONTROL) for one tick, as
    /// [`Invocation::perform`](invoke::Invocation::perform) lands them,
    /// and wake every [`Arrivals`] waiting. Unlike an invocation it is
    /// not re-run when the head moved under it: its publish fails with
    /// [`PublishError::VersionMismatch`](crate::PublishError::VersionMismatch)
    /// and lands nothing, and the caller runs it again.
    pub fn transaction(&self) -> Transaction<&Branch> {
        let transaction = self.inner.primary.transaction();
        let primary = transaction.view().to_source();
        let durables: Vec<DurableLayer> = self
            .inner
            .durables
            .iter()
            .map(|(layer, branch)| DurableLayer::pin(layer.clone(), branch.clone()))
            .collect();
        transaction.with_session(SessionContext {
            composite: self.composite(primary, &durables),
            memories: self.inner.layers.clone(),
            durables,
            loopback: self.inner.loopback.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_repo;
    use crate::placement::tree_revision;
    use crate::{LayerDeclaration, PublishError};
    use anyhow::Result;
    use dialog_common::ConditionalSend;
    use dialog_corpus::Selector;
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Cardinality;
    use dialog_query::attribute::Relation as QueryRelation;
    use dialog_query::attribute::statement::AttributeStatement;
    use dialog_query::query::{Application, Output as _};
    use dialog_query::types::{Any, Scalar};
    use dialog_query::{
        AttributeQuery, Conclusion, EvaluationError, InductiveRule, Match, Selection, Term,
    };
    #[cfg(any(feature = "integration-tests", feature = "web-integration-tests"))]
    use dialog_remote_s3::helpers::S3Address;
    use dialog_storage::provider::storage::VolatileSpace;
    use futures_util::future::join;
    use std::fmt::Debug;

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

        let claims: Vec<Row> = session
            .query()
            .select(Located(selected_query().with_layer(Term::var("where"))))
            .perform(&operator)
            .try_vec()
            .await?;
        let mut rows: Vec<String> = claims
            .iter()
            .map(|Row { claim, layer }| format!("{} in {layer}", claim.of))
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

        // A batch staged before the head moved carries a share for each
        // tick that wrote the layer, and lands none of them when its
        // publish loses the compare-and-swap.
        let counter: Entity = "ctr:1".parse()?;
        session.primary().refresh(&operator).await?;
        session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let start = memory.revision();
        let batch = session
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

    /// The primary's revision as its `dialog.layer/revision` row names it.
    fn tree_text(revision: &crate::Revision) -> String {
        use base58::ToBase58 as _;
        let tree: &[u8] = revision.tree.hash();
        format!("{:?}", dialog_corpus::Value::String(tree.to_base58()))
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

    /// A rule premised on `dialog.layer/revision` fires for a layer that
    /// moved since the session last recorded it, with the revision it
    /// stood at as the dispatch began: on the first dispatch, which finds
    /// nothing recorded, for every layer. The second dispatch finds the
    /// primary and the session's own layer where the first recorded
    /// them, so the rule fires for neither. (It used to fire for the
    /// session's own layer on every dispatch after the first, since the
    /// recording itself moved that layer; recording is no longer a move
    /// of it, so its row moves only when something wrote into it.)
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
        let own_name = crate::session_layer();
        let own_start = chain_text(&own);
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
        assert_eq!(
            held(&memory, "seen/revision", &own_name)?,
            vec![own_start.clone()],
            "nothing was recorded, so the rule fired for the session's own layer"
        );

        // The next dispatch finds the primary where the last dispatch
        // recorded it, so its revision is no stimulus and the rule does
        // not fire for it again. Nothing but the recording wrote into
        // the session's own layer, and a recording is no move of it, so
        // the rule does not fire for it either.
        session
            .transact(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "seen/revision", &memory_name)?,
            vec![tree_text(&start)],
            "the primary did not move, so the rule did not fire for it"
        );
        assert_eq!(
            held(&memory, "seen/revision", &own_name)?,
            vec![own_start],
            "the session's own layer did not move, so the rule did not fire for it"
        );
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
    /// revision row and mints no instant in `session:`: one that writes
    /// nothing, and one that only dispatches a command no rule answers
    /// (a write into `control:`, which lands in no layer the session
    /// records).
    #[dialog_common::test]
    async fn it_records_no_revision_when_nothing_moved() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        let sequence = branch.overlay().revision().sequence;
        let rows = revision_rows_of(branch.overlay());
        assert_eq!(rows.len(), 3, "the first dispatch recorded every layer");

        session.transact(Changes::new()).perform(&operator).await?;
        assert_eq!(
            branch.overlay().revision().sequence,
            sequence,
            "a dispatch that wrote nothing minted nothing in session:"
        );
        assert_eq!(revision_rows_of(branch.overlay()), rows);

        session
            .transact(flag(&doc, false, &crate::control_layer()))
            .perform(&operator)
            .await?;
        assert_eq!(
            branch.overlay().revision().sequence,
            sequence,
            "a dispatch of a command no rule answers minted nothing in session:"
        );
        assert_eq!(revision_rows_of(branch.overlay()), rows);
        Ok(())
    }

    /// A dispatch that moves the primary alone rewrites the primary's
    /// row alone, to the tree it published, as one instant of
    /// `session:`: the memory layer's row and the session's own stay as
    /// the dispatch before recorded them.
    #[dialog_common::test]
    async fn it_records_only_the_layer_that_moved() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let own = branch.overlay().clone();
        let (memory, own_name) = (crate::memory_layer(), crate::session_layer());
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(flag(&doc, true, &state))
            .perform(&operator)
            .await?;
        let primary_before = revision_of(&own, &memory)?;
        let state_before = revision_of(&own, &state)?;
        let own_before = revision_of(&own, &own_name)?;
        let sequence = own.revision().sequence;

        let published = session
            .transact(
                dialog_query::the!("doc/title")
                    .of(doc.clone())
                    .is("Moved".to_string()),
            )
            .perform(&operator)
            .await?;
        let primary_after = revision_of(&own, &memory)?;
        assert_eq!(primary_after, vec![tree_text(&published)], "the new tree");
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
            sequence + 1,
            "the recording is one instant"
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

    /// A row of a [`Located`] query: the claim and the layer it was read
    /// from.
    #[derive(Clone, Debug, PartialEq)]
    struct Row {
        claim: dialog_query::Claim,
        layer: Entity,
    }

    impl Conclusion for Row {
        fn this(&self) -> &Entity {
            &self.claim.of
        }
    }

    /// `query`'s rows, each with the layer it was read from: the
    /// match's `in` binding, the term the query names for it. A
    /// claim carries no layer.
    #[derive(Clone)]
    struct Located(AttributeQuery);

    impl Application for Located {
        type Conclusion = Row;

        fn evaluate<'a, Env, M: Selection + 'a>(
            self,
            selection: M,
            env: &'a Env,
        ) -> impl Selection + 'a
        where
            Env: dialog_query::Scope<'a>,
        {
            Application::evaluate(self.0, selection, env)
        }

        fn realize(&self, input: Match) -> Result<Self::Conclusion, EvaluationError> {
            let layer = input
                .lookup(&Term::<Any>::from(self.0.layer()))?
                .content()?;
            Ok(Row {
                claim: self.0.realize(input)?,
                layer: Entity::try_from(layer)?,
            })
        }
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

    /// `query`'s rows through `session`, each rendered with the layer
    /// its `in` binds, sorted.
    async fn located(
        session: &Session,
        operator: &Operator,
        query: AttributeQuery,
    ) -> Result<Vec<String>> {
        let claims: Vec<Row> = session
            .query()
            .select(Located(query))
            .perform(operator)
            .try_vec()
            .await?;
        let mut rows: Vec<String> = claims
            .iter()
            .map(|Row { claim, layer }| format!("{:?} in {layer}", claim.is))
            .collect();
        rows.sort();
        Ok(rows)
    }

    /// `ui/selected` of every entity, with the value a variable, as a
    /// single-valued read: the cell's rows are elected among.
    fn selected_one() -> AttributeQuery {
        AttributeQuery::new(
            Term::<QueryRelation>::from(QueryRelation::from(
                "ui/selected".parse::<dialog_corpus::Relation>().unwrap(),
            )),
            Term::<Entity>::var("e"),
            Term::<Any>::from(&Term::<bool>::var("v")),
            Term::blank(),
            Some(Cardinality::One),
        )
    }

    /// A single-valued read with `in` a variable elects within each
    /// layer: the same cell in two layers is two rows, each bound to
    /// its layer. With `in` bound it elects among that layer's rows
    /// alone.
    #[dialog_common::test]
    async fn it_elects_within_each_layer_a_single_valued_read_binds() -> Result<()> {
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

        let rows = |query: AttributeQuery| located(&session, &operator, query);
        assert_eq!(
            rows(selected_one().with_layer(Term::var("where"))).await?,
            vec![
                "Boolean(false) in memory:".to_string(),
                format!("Boolean(true) in {state}")
            ],
            "`in` unbound elects within each layer and binds each row's"
        );
        assert_eq!(
            rows(
                selected_one()
                    .with_layer(Term::Constant(dialog_corpus::Value::Entity(state.clone())))
            )
            .await?,
            vec![format!("Boolean(true) in {state}")],
            "`in` bound to the layer elects among its rows alone"
        );
        assert_eq!(
            rows(
                selected_one().with_layer(Term::Constant(dialog_corpus::Value::Entity(
                    crate::memory_layer()
                )))
            )
            .await?,
            vec!["Boolean(false) in memory:".to_string()],
            "`in` bound to the primary elects among its rows alone"
        );
        Ok(())
    }

    /// A read naming no `in` reads every layer as one: the rows of the
    /// primary and of a memory layer come out together, as they did
    /// before a row knew its layer, and the claims are the same.
    #[dialog_common::test]
    async fn it_reads_every_layer_as_one_without_in() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let one: Entity = "doc:1".parse()?;
        let two: Entity = "doc:2".parse()?;
        session
            .transact(flag(&one, true, &state))
            .assert(primary_flag(&two, false))
            .perform(&operator)
            .await?;

        assert_eq!(
            fresh(&session, &operator, selected_query()).await?,
            vec![
                "doc:1 Boolean(true)".to_string(),
                "doc:2 Boolean(false)".to_string()
            ],
            "every layer's rows, with no layer named"
        );
        let claims: Vec<dialog_query::Claim> = session
            .query()
            .select(selected_one())
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(
            selected(&claims),
            vec![
                "doc:1 Boolean(true)".to_string(),
                "doc:2 Boolean(false)".to_string()
            ],
            "a single-valued read elects each cell across the layers"
        );
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

        let rows = |query: AttributeQuery| located(&session, &operator, query);
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let mut subscription = session.subscribe(count_query());
        subscription.poll(&operator).await?;

        let start = memory.revision().sequence;
        session
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
        assert_eq!(retained, fresh(&session, &operator, count_query()).await?);
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;

        let start = memory.revision().sequence;
        session
            .transact(count(&counter, 2, &state))
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

    /// A head under `last` landing in the session's own layer succeeds
    /// the row that layer already holds, within the dispatch: the rounds
    /// after read the head, not the older row, so a chain over
    /// `session:` runs as one over a declared layer does.
    #[dialog_common::test]
    async fn it_succeeds_the_sessions_own_row_within_a_dispatch() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        declare_steps_in(&branch, &operator, crate::SESSION).await?;
        branch.refresh(&operator).await?;
        let session = branch.session().perform(&operator).await?;
        let own = crate::session_layer();
        let counter: Entity = "ctr:1".parse()?;

        // A row the layer holds before the dispatch, which no step follows.
        session
            .transact(count(&counter, 1, &own))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(branch.overlay(), "counter/count", &counter)?,
            vec!["UnsignedInt(1)".to_string()]
        );
        // The dispatch writes 2 above it; the steps follow to 4.
        session
            .transact(count(&counter, 2, &own))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(branch.overlay(), "counter/count", &counter)?,
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;
        let increment = || {
            dialog_query::the!("cmd.increment/counter")
                .of("cmd:1".parse::<Entity>().expect("an entity"))
                .is(counter.clone())
        };

        let start = memory.revision().sequence;
        session
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
        session
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
        session
            .transact(
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let mut subscription = session.subscribe(selected_query());
        subscription.poll(&operator).await?;

        let start = memory.revision().sequence;
        session
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
        assert_eq!(
            retained,
            fresh(&session, &operator, selected_query()).await?
        );
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
    async fn it_agrees_with_a_fresh_evaluation_through_session_churn() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_state(&branch, &operator).await?;
        declare_spread(&branch, &operator).await?;
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
                let label = match rng.below(7) {
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
                    5 => {
                        session
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
    /// the state layer. A subscription through the session sees the
    /// capture, nothing durable is written, the expiry of the command is
    /// no arrival, and nothing stays in `control:`.
    #[dialog_common::test]
    async fn it_answers_a_command_through_a_provider() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state = declare_fetch(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let control = crate::control_layer();
        let mut fetches = session
            .arrivals(
                &control,
                Located(fetch_query().with_layer(Term::var("where"))),
            )
            .expect("every session has control:");
        assert!(
            fetches.poll(&operator).await?.is_empty(),
            "nothing dispatched yet"
        );
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

        session
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        assert!(
            held(&memory, "node/fetched", &node)?.is_empty(),
            "nothing answered yet"
        );
        let located = fetches.poll(&operator).await?;
        assert!(
            located.iter().all(|row| row.layer == control),
            "read as a row of control:"
        );
        let commands: Vec<dialog_query::Claim> = located.into_iter().map(|row| row.claim).collect();
        assert_eq!(subjects(&commands), vec!["cmd:1".to_string()]);
        for command in &commands {
            session.invoke(fetched(command)?).perform(&operator).await?;
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
        assert!(
            session.layer(&control).expect("control:").is_empty(),
            "nothing stays in control:"
        );
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
        let session = branch.session().perform(&operator).await?;
        let mut fetches = session
            .arrivals(&crate::control_layer(), fetch_query())
            .expect("every session has control:");
        let node: Entity = "node:1".parse()?;
        let (commands, invoked) = join(fetches.next(&operator), async {
            session
                .invoke(fetch("cmd:1", &node))
                .perform(&operator)
                .await
        })
        .await;
        invoked?;
        assert_eq!(subjects(&commands?), vec!["cmd:1".to_string()]);
        Ok(())
    }

    /// Declare, beside [`declare_fetch`]'s capture, a channel
    /// `control:host`: a layer for a consumer outside the invoke loop.
    async fn declare_host(branch: &Branch, env: &Operator) -> Result<(Entity, Entity)> {
        let state = declare_fetch(branch, env).await?;
        let host: Entity = "control:host".parse()?;
        branch
            .transaction()
            .assert(LayerDeclaration::new(host.clone(), Store::Control))
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok((state, host))
    }

    /// A consumer of a channel reads the rows the dispatches wrote there,
    /// each once, as rows of the channel, and answers by invoking, retracting
    /// what it consumed. The rows stay until then; after, the channel holds
    /// nothing, and the consumer reads nothing: its own retraction is no
    /// arrival.
    #[dialog_common::test]
    async fn it_reads_a_channel_until_its_consumer_retracts_the_rows() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let (state, host) = declare_host(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        assert!(session.layers().contains(&host), "the channel is held");
        let memory = session.layer(&state).expect("the state layer").clone();
        let channel = session.layer(&host).expect("the channel").clone();
        let mut fetches = session
            .arrivals(&host, Located(fetch_query().with_layer(Term::var("where"))))
            .expect("the channel");
        assert!(fetches.poll(&operator).await?.is_empty());
        let before = branch.revision();
        let node: Entity = "node:1".parse()?;
        for command in ["cmd:1", "cmd:2"] {
            session
                .transact(fetch(command, &node).at(host.clone()))
                .perform(&operator)
                .await?;
        }
        assert_eq!(
            held(&channel, "cmd.fetch/node", &"cmd:1".parse()?)?,
            vec!["Entity(node:1)".to_string()],
            "a channel's row outlives its dispatch"
        );
        assert!(held(&memory, "node/fetched", &node)?.is_empty());

        let located = fetches.poll(&operator).await?;
        assert!(located.iter().all(|row| row.layer == host));
        let commands: Vec<dialog_query::Claim> = located.into_iter().map(|row| row.claim).collect();
        assert_eq!(
            subjects(&commands),
            vec!["cmd:1".to_string(), "cmd:2".to_string()]
        );
        let mut answer = session.transact(Changes::new());
        for command in &commands {
            answer = answer
                .dispatch(fetched(command)?)
                .retract(fetch(&command.of.to_string(), &node).at(host.clone()));
        }
        answer.perform(&operator).await?;

        assert_eq!(
            held(&memory, "node/fetched", &node)?,
            vec!["Entity(cmd:1)".to_string(), "Entity(cmd:2)".to_string()]
        );
        assert!(channel.is_empty(), "the consumer retracted what it read");
        assert!(
            fetches.poll(&operator).await?.is_empty(),
            "read once: a retraction is no arrival"
        );
        assert_eq!(branch.revision(), before, "nothing durable was written");
        Ok(())
    }

    /// A consumer that was not reading loses nothing: a channel keeps
    /// every row until it is retracted, so a reader built after the rows
    /// landed reads every one, and so does a reader the layer's bounded
    /// instants moved past, from what the channel holds.
    #[dialog_common::test]
    async fn it_keeps_a_channels_rows_for_a_consumer_that_was_not_reading() -> Result<()> {
        // One more than the instants a layer keeps.
        const PAST_THE_INSTANTS: usize = 1025;
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let (_, host) = declare_host(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let mut behind = session.arrivals(&host, fetch_query()).expect("the channel");
        let node: Entity = "node:1".parse()?;
        for index in 0..PAST_THE_INSTANTS {
            session
                .transact(fetch(&format!("cmd:{index}"), &node).at(host.clone()))
                .perform(&operator)
                .await?;
        }
        assert_eq!(
            session.layer(&host).expect("the channel").len(),
            PAST_THE_INSTANTS,
            "every row is still there"
        );
        let mut late = session.arrivals(&host, fetch_query()).expect("the channel");
        assert_eq!(late.poll(&operator).await?.len(), PAST_THE_INSTANTS);
        assert!(late.poll(&operator).await?.is_empty());
        assert_eq!(
            behind.poll(&operator).await?.len(),
            PAST_THE_INSTANTS,
            "moved past, it reads what the channel holds"
        );
        assert!(behind.poll(&operator).await?.is_empty());
        Ok(())
    }

    /// [`declare_state`]'s counter, with its command premise read from
    /// `control:` alone (`in: control:`).
    async fn declare_state_on_control(branch: &Branch, env: &Operator) -> Result<Entity> {
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
                                "the": "cmd.increment/counter", "as": "Entity", "in": "control:"
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
            .assert(LayerDeclaration::new(state.clone(), Store::Memory))
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let counter: Entity = "ctr:1".parse()?;
        session
            .transact(count(&counter, 1, &state))
            .perform(&operator)
            .await?;

        session
            .invoke(increment("cmd:1", &counter))
            .perform(&operator)
            .await?;
        assert_eq!(
            held(&memory, "counter/count", &counter)?,
            vec!["UnsignedInt(2)".to_string()],
            "one step, in the tick the command was stimulus of"
        );

        session
            .transact(increment("cmd:2", &counter))
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

        session
            .transact(increment("cmd:3", &counter).at(crate::control_layer()))
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
            .assert(LayerDeclaration::new(state.clone(), Store::Memory))
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let control = crate::control_layer();
        let target: Entity = "doc:1".parse()?;

        let transaction = session.transaction().dispatch(ping("cmd:1", &target));
        let claims: Vec<Row> = transaction
            .query()
            .select(Located(anywhere("cmd.ping/target")))
            .perform(&operator)
            .try_vec()
            .await?;
        assert_eq!(claims.len(), 1);
        assert_eq!(claims[0].layer, control, "a transaction reads it");

        let before = branch.revision();
        let mut pongs = session
            .arrivals(&control, anywhere("cmd.pong/target"))
            .expect("every session has control:");
        session
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
            let rows: Vec<dialog_query::Claim> = session
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
        let session = branch.session().perform(&operator).await?;
        let control = crate::control_layer();
        let mut arrivals = session
            .arrivals(&control, selected_query())
            .expect("every session has control:");
        let before = branch.revision();
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(flag(&doc, true, &control))
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
        assert!(held(session.layer(&state).expect("state"), "ui/selected", &doc)?.is_empty());
        assert!(session.layer(&control).expect("control:").is_empty());
        assert!(
            fresh(
                &session,
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
            .assert(LayerDeclaration::new(notes.clone(), Store::Durable))
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

    /// The pointer fact a primary holding `branch`'s head as a durable
    /// layer's revision carries, as [`stored`] renders it.
    fn pointer_to(branch: &Branch) -> Result<String> {
        let revision = branch
            .revision()
            .ok_or_else(|| anyhow::anyhow!("the layer's branch has a head"))?;
        Ok(format!("{:?}", tree_revision(&revision)))
    }

    /// The `doc/note` values `query` reads through `session`, each with
    /// the layer its `in` binds, sorted. A row with no layer stamped is
    /// the primary's.
    async fn notes_read(
        session: &Session,
        env: &Operator,
        query: AttributeQuery,
    ) -> Result<Vec<String>> {
        let located: Vec<Row> = session
            .query()
            .select(Located(query))
            .perform(env)
            .try_vec()
            .await?;
        let mut rows: Vec<String> = located
            .iter()
            .map(|Row { claim, layer }| format!("{:?} in {layer}", claim.is))
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

    /// A schema declaring `memory:notes` durable opens a session holding
    /// it as the repository's `notes` branch, with no memory cell of its
    /// own. A write `in: memory:notes` is committed on that branch and
    /// lands nowhere in the primary's tree, and the primary gains the
    /// pointer fact naming the layer's new revision: one claim, which the
    /// next write into the layer succeeds.
    #[dialog_common::test]
    async fn it_commits_a_write_into_a_durable_layer_on_its_branch() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        assert_eq!(session.layers(), vec![session_layer(), notes.clone()]);
        let layer = session.durable(&notes).expect("the notes layer").clone();
        assert_eq!(layer.name(), "notes");
        assert!(session.layer(&notes).is_none(), "no memory cell holds it");
        assert_eq!(layer.revision(), None, "the branch starts empty");

        let doc: Entity = "doc:1".parse()?;
        session
            .transact(note(&doc, "first", &notes))
            .perform(&operator)
            .await?;
        let on_layer = repo.branch("notes").open().perform(&operator).await?;
        assert_eq!(
            stored(&on_layer, &operator, "doc/note", &doc).await?,
            vec!["String(\"first\")".to_string()],
            "the write is committed on the layer's branch"
        );
        assert!(
            stored(&branch, &operator, "doc/note", &doc)
                .await?
                .is_empty(),
            "the primary's tree holds none of it"
        );
        assert!(held(branch.overlay(), "doc/note", &doc)?.is_empty());
        assert_eq!(
            stored(&branch, &operator, "dialog.layer/revision", &notes).await?,
            vec![pointer_to(&on_layer)?],
            "the primary points at the layer's new revision"
        );

        session
            .transact(note(&doc, "second", &notes))
            .perform(&operator)
            .await?;
        on_layer.refresh(&operator).await?;
        assert_eq!(
            stored(&on_layer, &operator, "doc/note", &doc).await?,
            vec![
                "String(\"first\")".to_string(),
                "String(\"second\")".to_string()
            ]
        );
        assert_eq!(
            stored(&branch, &operator, "dialog.layer/revision", &notes).await?,
            vec![pointer_to(&on_layer)?],
            "the new pointer succeeds the old one"
        );
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
        let session = branch.session().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(note(&doc, "kept", &notes))
            .assert(note(&doc, "primary", &crate::memory_layer()))
            .perform(&operator)
            .await?;
        assert_eq!(
            notes_read(
                &session,
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
            notes_read(
                &session,
                &operator,
                note_query()
                    .with_layer(Term::Constant(dialog_corpus::Value::Entity(notes.clone())))
            )
            .await?,
            vec!["String(\"kept\") in memory:notes".to_string()],
            "`in` bound to the durable layer reads it alone"
        );
        assert_eq!(
            notes_read(
                &session,
                &operator,
                note_query().with_layer(Term::Constant(dialog_corpus::Value::Entity(
                    crate::memory_layer()
                )))
            )
            .await?,
            vec!["String(\"primary\") in memory:".to_string()],
            "`in` bound to the primary reads it alone"
        );
        Ok(())
    }

    /// A dispatch whose publish of the durable layer loses its
    /// compare-and-swap, because another handle moved the layer's branch
    /// under it, re-runs from the new heads and lands once on each
    /// branch: one commit on the layer after the other writer's, one on
    /// the primary, which points at the layer's tip.
    #[dialog_common::test]
    async fn it_re_runs_a_dispatch_whose_durable_layer_moved_under_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(note(&doc, "first", &notes))
            .perform(&operator)
            .await?;
        let primary_before = branch.revision().expect("a head");

        // Another handle of the layer's branch moves it; the session's
        // handle is stale, so its publish of the layer loses the CAS.
        let other = repo.branch("notes").open().perform(&operator).await?;
        let moved = other
            .transaction()
            .assert(
                dialog_query::the!("doc/note")
                    .of("doc:2".parse::<Entity>()?)
                    .is("other".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        assert_ne!(
            session.durable(&notes).expect("notes").revision().as_ref(),
            Some(&moved),
            "the session's handle has not seen the move"
        );

        let published = session
            .transact(note(&doc, "second", &notes))
            .perform(&operator)
            .await?;
        let layer = session.durable(&notes).expect("notes");
        let tip = layer.revision().expect("the layer has a head");
        assert_eq!(
            tip.version().edition,
            moved.version().edition.successor(),
            "the layer took one commit, after the other writer's"
        );
        assert_eq!(
            published.version().edition,
            primary_before.version().edition.successor(),
            "the primary took one commit"
        );
        assert_eq!(
            stored(layer, &operator, "doc/note", &"doc:2".parse()?).await?,
            vec!["String(\"other\")".to_string()],
            "the other writer's commit stands"
        );
        assert_eq!(
            stored(layer, &operator, "doc/note", &doc).await?,
            vec![
                "String(\"first\")".to_string(),
                "String(\"second\")".to_string()
            ]
        );
        assert_eq!(
            stored(&branch, &operator, "dialog.layer/revision", &notes).await?,
            vec![pointer_to(layer)?],
            "the primary points at the layer's tip"
        );
        Ok(())
    }

    /// A write `in: memory:missing`, a layer declared nowhere, is refused
    /// with `UnboundLayer` before anything is committed: the primary and
    /// the durable layer stay where they were, and no `missing` branch
    /// is written.
    #[dialog_common::test]
    async fn it_refuses_a_write_into_a_durable_layer_declared_nowhere() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(note(&doc, "kept", &notes))
            .perform(&operator)
            .await?;
        let primary = branch.revision();
        let layer = session.durable(&notes).expect("notes").revision();
        let missing: Entity = "memory:missing".parse()?;

        let refused = session
            .transact(note(&doc, "lost", &notes))
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
        let on_layer = repo.branch("notes").open().perform(&operator).await?;
        assert_eq!(on_layer.revision(), layer, "the layer did not move");
        assert_eq!(
            stored(&on_layer, &operator, "doc/note", &doc).await?,
            vec!["String(\"kept\")".to_string()]
        );
        let nowhere = repo.branch("missing").open().perform(&operator).await?;
        assert_eq!(nowhere.revision(), None, "no branch was written for it");
        Ok(())
    }

    /// `session.sync()` pulls a durable layer whose branch pulls from an
    /// upstream before the primary, and records where the pull left it as
    /// the primary's pointer fact; the pulled rows read with `in` bound to
    /// the layer. It pushes the layer to the upstream it pushes to. A
    /// memory layer, and a primary with no upstream, are left alone.
    #[dialog_common::test]
    async fn it_syncs_a_durable_layer_and_records_where_the_pull_left_it() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let upstream = repo
            .branch("notes-upstream")
            .open()
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        upstream
            .transaction()
            .assert(
                dialog_query::the!("doc/note")
                    .of(doc.clone())
                    .is("upstream".to_string()),
            )
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let layer = session.durable(&notes).expect("notes");
        layer.set_upstream(&upstream).perform(&operator).await?;

        session
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        assert_eq!(
            layer.revision(),
            upstream.revision(),
            "the layer was pulled"
        );
        assert_eq!(
            stored(&branch, &operator, "dialog.layer/revision", &notes).await?,
            vec![pointer_to(layer)?],
            "the primary points where the pull left the layer"
        );
        assert_eq!(
            notes_read(
                &session,
                &operator,
                note_query()
                    .with_layer(Term::Constant(dialog_corpus::Value::Entity(notes.clone())))
            )
            .await?,
            vec!["String(\"upstream\") in memory:notes".to_string()]
        );

        session
            .transact(note(&doc, "local", &notes))
            .perform(&operator)
            .await?;
        let pointed = branch.revision();
        session
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        upstream.refresh(&operator).await?;
        assert_eq!(
            upstream.revision(),
            layer.revision(),
            "the layer was pushed"
        );
        assert_eq!(
            branch.revision(),
            pointed,
            "a pointer that already names the layer is not written again"
        );
        Ok(())
    }

    /// A sync whose pull moved the primary records the primary's new
    /// revision in `session:`, as a commit would, and a rule premised on
    /// `dialog.layer/revision` of `memory:` fires on it once, with that
    /// revision. A second sync, which pulls nothing, records nothing:
    /// `session:` mints no instant, and the rule does not fire again.
    /// Replica A is a second branch of the repository tracking the same
    /// upstream as the session's primary.
    #[dialog_common::test]
    async fn it_records_the_revisions_a_pull_moved() -> Result<()> {
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
        let session = branch.session().perform(&operator).await?;
        let memory = session.layer(&state).expect("the state layer").clone();
        let own = branch.overlay().clone();
        let memory_name = crate::memory_layer();
        session
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
        session
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
            revision_of(&own, &memory_name)?,
            vec![tree_text(&head)],
            "session: holds the primary's revision as the pull left it"
        );
        assert_eq!(
            held(&memory, "seen/revision", &memory_name)?,
            vec![tree_text(&head)],
            "the rule fired on the pulled revision"
        );
        assert_eq!(
            memory.revision().sequence,
            fired + 1,
            "the rule fired once: one instant in its layer"
        );

        let (own_sequence, memory_sequence) = (own.revision().sequence, memory.revision().sequence);
        session
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

    /// A subscription through a session reads its durable layers too: a
    /// write into one is a delta on the next poll, read afresh since the
    /// layer's tree moved, and the subscription then agrees with a fresh
    /// read.
    #[dialog_common::test]
    async fn it_subscribes_to_a_durable_layer_through_a_session() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let notes = declare_notes(&branch, &operator).await?;
        let session = branch.session().perform(&operator).await?;
        let every = || note_query().with_layer(Term::var("where"));
        let mut subscription = session.subscribe(Located(every()));
        let initial = subscription.poll(&operator).await?.expect("initial");
        assert!(initial.asserted.is_empty());

        let doc: Entity = "doc:1".parse()?;
        session
            .transact(note(&doc, "kept", &notes))
            .perform(&operator)
            .await?;
        let delta = subscription
            .poll(&operator)
            .await?
            .expect("the layer moved");
        let asserted: Vec<String> = delta
            .asserted
            .iter()
            .map(|Row { claim, layer }| format!("{:?} in {layer}", claim.is))
            .collect();
        assert_eq!(
            asserted,
            vec!["String(\"kept\") in memory:notes".to_string()]
        );
        assert!(delta.retracted.is_empty());
        assert!(subscription.poll(&operator).await?.is_none());
        assert_eq!(
            notes_read(&session, &operator, every()).await?,
            vec!["String(\"kept\") in memory:notes".to_string()]
        );
        Ok(())
    }

    /// Two replicas of a space sync a session through one S3 upstream:
    /// the first writes into its primary and its durable layer and syncs;
    /// the second, whose primary and layer track the same remote
    /// branches, syncs and reads the same rows with the same `in`.
    #[cfg(any(feature = "integration-tests", feature = "web-integration-tests"))]
    #[dialog_common::test]
    async fn it_syncs_a_durable_layer_to_a_second_replica(s3: S3Address) -> Result<()> {
        use crate::RepositoryExt as _;
        use crate::helpers::connect;
        use dialog_peer::helpers::unique_name;
        use dialog_remote_s3::{Address as S3SiteAddress, S3Credential};

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
        let session = main.session().perform(&operator).await?;
        main.set_upstream(origin.branch("main").open().perform(&operator).await?)
            .perform(&operator)
            .await?;
        session
            .durable(&notes)
            .expect("notes")
            .set_upstream(origin.branch("notes").open().perform(&operator).await?)
            .perform(&operator)
            .await?;
        let doc: Entity = "doc:1".parse()?;
        session
            .transact(note(&doc, "kept", &notes))
            .assert(note(&doc, "primary", &crate::memory_layer()))
            .perform(&operator)
            .await?;
        session
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
        replica
            .pull(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;
        let mirrored = replica.session().perform(&operator).await?;
        mirrored
            .durable(&notes)
            .expect("the declaration replicated with the primary")
            .set_upstream(origin.branch("notes").open().perform(&operator).await?)
            .perform(&operator)
            .await?;
        mirrored
            .sync(crate::Download::MinimalRanges)
            .perform(&operator)
            .await?;

        let every = note_query().with_layer(Term::var("where"));
        assert_eq!(
            notes_read(&mirrored, &operator, every.clone()).await?,
            notes_read(&session, &operator, every).await?,
            "the second replica reads the same rows with the same `in`"
        );
        assert_eq!(
            notes_read(
                &mirrored,
                &operator,
                note_query().with_layer(Term::var("where"))
            )
            .await?,
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

    /// A transaction the session hands out, committed and published
    /// through the transaction API rather than invoked, lands the
    /// commands it dispatched in `control:` once its publish has won: a
    /// write `.at(control_layer())` published in one step, and a
    /// `dispatch` of a staged chain published after. Each is read once,
    /// as a row of `control:`, nothing stays there, and the durable
    /// write beside the first is in the tree.
    #[dialog_common::test]
    async fn it_lands_the_commands_of_a_transaction_published_through_the_session() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let session = branch.session().perform(&operator).await?;
        let control = crate::control_layer();
        let mut fetches = session
            .arrivals(
                &control,
                Located(fetch_query().with_layer(Term::var("where"))),
            )
            .expect("every session has control:");
        let node: Entity = "node:1".parse()?;
        let doc: Entity = "doc:1".parse()?;

        session
            .transaction()
            .assert(fetch("cmd:1", &node).at(control.clone()))
            .assert(title(&doc, "kept"))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let located = fetches.poll(&operator).await?;
        assert!(
            located.iter().all(|row| row.layer == control),
            "a row of control:"
        );
        let commands: Vec<dialog_query::Claim> = located.into_iter().map(|row| row.claim).collect();
        assert_eq!(subjects(&commands), vec!["cmd:1".to_string()]);
        assert!(fetches.poll(&operator).await?.is_empty(), "read once");
        assert!(
            session.layer(&control).expect("control:").is_empty(),
            "nothing stays in control:"
        );
        assert_eq!(
            committed::<String>(&branch, &operator, "doc/title", &doc).await?,
            vec!["String(\"kept\")".to_string()],
            "the durable write is in the tree"
        );

        let batch = session
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
        let commands: Vec<dialog_query::Claim> = fetches
            .poll(&operator)
            .await?
            .into_iter()
            .map(|row| row.claim)
            .collect();
        assert_eq!(subjects(&commands), vec!["cmd:2".to_string()]);
        assert!(fetches.poll(&operator).await?.is_empty(), "read once");
        Ok(())
    }

    /// A transaction through the session whose head moved under it is
    /// refused at commit with `VersionMismatch`, and lands nothing: no
    /// command in `control:`, and no durable write in the tree.
    #[dialog_common::test]
    async fn it_lands_nothing_of_a_session_transaction_that_lost_its_head() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let session = branch.session().perform(&operator).await?;
        let control = crate::control_layer();
        let mut fetches = session
            .arrivals(&control, fetch_query())
            .expect("every session has control:");
        let node: Entity = "node:1".parse()?;
        let doc: Entity = "doc:1".parse()?;

        let transaction = session
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
        assert!(session.layer(&control).expect("control:").is_empty());
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
        let session = branch.session().perform(&operator).await?;
        let control = crate::control_layer();
        let mut arrivals = session
            .arrivals(&control, whole(&control))
            .expect("every session has control:");
        let node: Entity = "node:1".parse()?;
        let doc: Entity = "doc:1".parse()?;
        let expected = vec![
            "cmd:1 cmd.fetch/node".to_string(),
            "cmd:1 cmd.fetch/priority".to_string(),
        ];

        let transaction = session
            .transaction()
            .dispatch(fetch("cmd:1", &node))
            .assert(priority("cmd:1", 2).at(control.clone()))
            .assert(title(&doc, "kept"))
            .assert(flag(&doc, true, &state));
        let during: Vec<Row> = transaction
            .query()
            .select(Located(whole(&control)))
            .perform(&operator)
            .try_vec()
            .await?;
        assert!(during.iter().all(|row| row.layer == control));
        let during: Vec<dialog_query::Claim> = during.into_iter().map(|row| row.claim).collect();
        assert_eq!(cells(&during), expected, "the tick's commands alone");
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
        let session = branch.session().perform(&operator).await?;
        let control = crate::control_layer();
        let mut rows = session
            .arrivals_rows(&control)
            .expect("every session has control:");
        let node: Entity = "node:1".parse()?;
        let text = |facts: Vec<dialog_corpus::Fact>| -> Vec<String> {
            facts
                .into_iter()
                .map(|fact| format!("{} {} {:?}", fact.of, fact.the, fact.is))
                .collect()
        };
        assert!(rows.rows().is_empty(), "nothing dispatched yet");

        session
            .invoke(fetch("cmd:1", &node))
            .perform(&operator)
            .await?;
        assert_eq!(
            text(rows.rows()),
            vec!["cmd:1 cmd.fetch/node Entity(node:1)".to_string()]
        );

        session
            .invoke(fetch("cmd:2", &node))
            .dispatch(priority("cmd:2", 3))
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

    /// An environment that provides what opening a session and reading
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

    /// A session opens through an environment that can only read: one
    /// that opens branches and queries them, and provides nothing a
    /// commit needs. It builds the memory layer and opens the durable
    /// layer the primary declares, and reads the primary and what
    /// arrives in `control:` through the same environment.
    #[dialog_common::test]
    async fn it_opens_a_session_through_an_environment_that_only_reads() -> Result<()> {
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
        let reader = ReadOnly(operator.clone());

        let session = branch.session().perform(&reader).await?;
        assert_eq!(
            session.layers(),
            vec![session_layer(), state, notes.clone()]
        );
        assert!(
            session.durable(&notes).is_some(),
            "the durable layer opened"
        );
        let titles: Vec<dialog_query::Claim> = session
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
        let mut fetches = session
            .arrivals(&crate::control_layer(), fetch_query())
            .expect("every session has control:");
        assert!(fetches.poll(&reader).await?.is_empty());
        Ok(())
    }
}
