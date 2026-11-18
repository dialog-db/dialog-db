//! Backing up a stack's session layers, and restoring them on a stack
//! opened afresh, in this process or another.
//!
//! A stack's [`SESSION`](crate::SESSION) layer and the session layers
//! its schema declares ([`LayerType::Session`])
//! live in memory and are gone with the process. [`Stack::backup`] takes
//! them as a [`Backup`], which serializes; [`OpenStack::restore`] opens a
//! stack on the same branch holding them again:
//!
//! ```no_run
//! # use dialog_repository::{Backup, Branch};
//! # async fn example(stack: dialog_repository::Stack, branch: Branch, env: impl dialog_repository::StackEnv) -> anyhow::Result<()> {
//! let bytes = serde_json::to_vec(&stack.backup())?;
//! // ... carried to another worker, which opens its own handle ...
//! let backup: Backup = serde_json::from_slice(&bytes)?;
//! let stack = branch.stack().restore(backup).perform(&env).await?;
//! # Ok(())
//! # }
//! ```
//!
//! What a backup holds and what it leaves out:
//!
//! - every row of `session:` and of each declared session layer,
//!   tombstones among them ([`Ephemeral::export`]), with the revision
//!   the layer stood at as its `dialog.layer/revision` row renders it;
//! - the primary's branch entity, and the revision the stack last
//!   recorded the primary at, as its `dialog.branch/revision` row names
//!   it (the revision's entity);
//! - not the channels, whose rows live one tick; not the durable
//!   layers, which the primary points at and which a restored stack reads
//!   where the primary points them then; and not the primary's session
//!   overlay ([`Branch::overlay`](crate::Branch::overlay)), which is the
//!   embedder's and which the stack never writes.
//!
//! Restoring replays each layer's rows into the new stack's layer of
//! the same name, then recomputes the rows that describe the process
//! that took the backup rather than the session: a channel created at
//! run time ([`Stack::channel`]) and a provider's discovery row
//! ([`Stack::provide`]) are not restored, and are written again when
//! the embedder creates the channel and registers the provider again. A
//! layer the backup names that the schema no longer declares is dropped
//! with its rows, and the restore goes on. The revision rows are diffed
//! as at any open: a layer that stood where its row said when the backup
//! was taken is recorded where its replayed rows leave it, and one that
//! had moved unrecorded, like the primary when it moved while the backup
//! was away (a commit or a pull through another handle), is the first
//! dispatch's stimulus, so a rule premised on its revision fires once.
//!
//! A backup is process-local state carried by the embedder: nothing of
//! it is stored or synced by dialog.

use std::collections::HashSet;

use base58::ToBase58 as _;
use dialog_corpus::{Change, Changes, Entity, Update as _, Value};
use serde::{Deserialize, Serialize};

use super::{OpenStack, Stack};
use crate::placement::{LayerType, control_layer, layer_type_attr, revision_attr, session_layer};
use crate::{CommitError, Ephemeral, provided_attr};

/// A stack's session layers, as [`Stack::backup`] takes them: restore
/// it with [`OpenStack::restore`]. Serializes with any serde format.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Backup {
    branch: Entity,
    primary: Option<Entity>,
    layers: Vec<LayerBackup>,
}

/// One session layer of a [`Backup`]: its name, its rows, and the
/// revision it stood at.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LayerBackup {
    layer: Entity,
    revision: String,
    rows: Changes,
}

impl LayerBackup {
    /// The layer's name: [`session_layer`] or a
    /// declared `session:<name>`.
    pub fn layer(&self) -> &Entity {
        &self.layer
    }

    /// The revision the layer stood at, as its `dialog.layer/revision`
    /// row renders it: the base58 hash of its instant chain, and of
    /// `session:` its chain as it stood before the records that followed
    /// its last change.
    pub fn revision(&self) -> &str {
        &self.revision
    }

    /// Every row the layer held, each tombstone as the retraction that
    /// hides its fact ([`Ephemeral::export`]).
    pub fn rows(&self) -> &Changes {
        &self.rows
    }

    fn of(layer: Entity, memory: &Ephemeral, revision: impl AsRef<[u8]>) -> Self {
        Self {
            layer,
            revision: revision.as_ref().to_base58(),
            rows: memory.export(),
        }
    }

    /// Whether `own`, the backed-up rows of `session:`, records this
    /// layer where it stood: its `dialog.layer/revision` row there is the
    /// revision the backup took it at.
    fn recorded_in(&self, own: Option<&LayerBackup>) -> bool {
        let at = Value::String(self.revision.clone());
        own.is_some_and(|own| {
            own.rows.iter().any(|(of, the, change)| {
                *of == self.layer
                    && *the == revision_attr()
                    && matches!(change, Change::Assert(is, _) if *is == at)
            })
        })
    }
}

impl Backup {
    /// The branch entity of the primary of the stack the backup was taken
    /// of: a restore on a stack of any other is refused.
    pub fn branch(&self) -> &Entity {
        &self.branch
    }

    /// The entity of the revision the stack had last recorded its primary
    /// at, as its `dialog.branch/revision` row names it; `None` for a
    /// primary with no revision.
    pub fn primary(&self) -> Option<&Entity> {
        self.primary.as_ref()
    }

    /// The session layers backed up: `session:` first, then those the
    /// schema declared, in its order.
    pub fn layers(&self) -> &[LayerBackup] {
        &self.layers
    }

    /// Refuse a restore on the stack whose primary's branch entity is
    /// `branch`, unless it is the one the backup was taken of.
    pub(crate) fn check(&self, branch: &Entity) -> Result<(), CommitError> {
        if self.branch == *branch {
            return Ok(());
        }
        Err(CommitError::ForeignBackup {
            backup: self.branch.to_string(),
            branch: branch.to_string(),
        })
    }

    /// Replay the backed-up rows into `own`, the new stack's
    /// `session:`, and each of `memories`, its declared session layers,
    /// by name; a backed-up layer none of them is named for is dropped,
    /// and so are its rows in `session:`. Of `session:`'s rows, those
    /// describing the process that took the backup are left out: the
    /// discovery rows of its providers, and every row of a channel it
    /// created. Returns the layers that had moved since `session:` last
    /// recorded them when the backup was taken: their rows are left as
    /// the backup holds them, to be the first dispatch's stimulus.
    pub(crate) fn replay(
        &self,
        own: &Ephemeral,
        memories: &[(Entity, Ephemeral)],
    ) -> Result<HashSet<Entity>, CommitError> {
        let session = session_layer();
        let own_backup = self.layers.iter().find(|layer| layer.layer == session);
        let target = |layer: &Entity| -> Option<&Ephemeral> {
            if *layer == session {
                return Some(own);
            }
            memories
                .iter()
                .find(|(held, _)| held == layer)
                .map(|(_, memory)| memory)
        };
        let dropped: HashSet<Entity> = self
            .layers
            .iter()
            .filter(|layer| target(&layer.layer).is_none())
            .map(|layer| layer.layer.clone())
            .collect();
        let mut stale = HashSet::new();
        for layer in &self.layers {
            let Some(memory) = target(&layer.layer) else {
                continue;
            };
            let rows = if layer.layer == session {
                own_rows(&layer.rows, &dropped)
            } else {
                layer.rows.clone()
            };
            memory.apply(rows)?;
            if !layer.recorded_in(own_backup) {
                stale.insert(layer.layer.clone());
            }
        }
        Ok(stale)
    }
}

/// The rows of a backed-up `session:` a restore replays: all but the
/// discovery rows of the providers registered on the stack that took
/// the backup, every row of a channel it created at run time (one whose
/// `dialog.layer/type channel:` row it held, the loopback aside), and
/// every row of a layer in `dropped`.
fn own_rows(rows: &Changes, dropped: &HashSet<Entity>) -> Changes {
    let channel = Value::Entity(LayerType::Channel.entity());
    let control = control_layer();
    let created: HashSet<&Entity> = rows
        .iter()
        .filter(|(of, the, change)| {
            **of != control
                && **the == layer_type_attr()
                && matches!(change, Change::Assert(is, _) if *is == channel)
        })
        .map(|(of, _, _)| of)
        .collect();
    let provided = provided_attr();
    let mut kept = Changes::new();
    for (of, the, change) in rows.iter() {
        if created.contains(of) || dropped.contains(of) || *the == provided {
            continue;
        }
        match change {
            Change::Assert(is, pick) => {
                kept.associate(the.clone(), of.clone(), is.clone(), pick.clone())
            }
            Change::Retract(is) => kept.dissociate(the.clone(), of.clone(), is.clone()),
        }
    }
    kept
}

impl Stack {
    /// Back up the stack's session layers: every row of
    /// [`SESSION`](crate::SESSION) and of each session layer the schema
    /// declares, with the revision each stood at, and where the stack
    /// last recorded the primary. Channels, durable layers and the
    /// primary's session overlay are not backed up (see the
    /// [module docs](self)). Restore it with [`OpenStack::restore`].
    pub fn backup(&self) -> Backup {
        let own = self.own();
        let mut layers = vec![LayerBackup::of(
            session_layer(),
            own,
            own.settled().as_bytes(),
        )];
        for (layer, memory) in &self.inner.layers {
            layers.push(LayerBackup::of(
                layer.clone(),
                memory,
                memory.revision().hash.as_bytes(),
            ));
        }
        Backup {
            branch: self.inner.branch.clone(),
            primary: self.inner.observed.get(),
            layers,
        }
    }
}

impl OpenStack {
    /// Open the stack holding what `backup` holds (see the
    /// [module docs](self)): each backed-up session layer the schema
    /// still declares, and `session:`, start with the rows it held; a
    /// layer the schema no longer declares is dropped and the rest are
    /// restored. A backup taken of a stack on another branch fails the
    /// open with [`CommitError::ForeignBackup`] before any layer is read
    /// or built.
    pub fn restore(mut self, backup: Backup) -> Self {
        self.backup = Some(backup);
        self
    }
}

// `session:` and the declared session layers live in memory, so nothing
// here is storage-shaped: the tests run on the volatile provider alone,
// and no filesystem run applies.
#[cfg(test)]
mod tests {
    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_dedicated_worker);

    use super::*;
    use crate::helpers::test_repo;
    use crate::{Branch, LayerDeclaration, Revision, memory_layer};
    use anyhow::Result;
    use dialog_corpus::{Selector, Statement};
    use dialog_peer::Peer;
    use dialog_peer::helpers::test_session_with_peer;
    use dialog_query::attribute::Cardinality;
    use dialog_query::attribute::Relation as QueryRelation;
    use dialog_query::query::Output;
    use dialog_query::{InductiveRule, Term};
    use dialog_storage::provider::storage::VolatileSpace;

    type Operator = Peer<VolatileSpace, dialog_peer::Session>;

    /// The values of `the` on `of` a memory layer holds, as text, sorted.
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

    fn layer_of<'a>(stack: &'a Stack, name: &Entity) -> &'a Ephemeral {
        stack
            .layer(name)
            .and_then(|layer| layer.ephemeral())
            .expect("a layer held in memory")
    }

    fn flag(doc: &str, at: &Entity) -> Result<impl Statement + use<>> {
        Ok(dialog_query::the!("ui/selected")
            .of(doc.parse::<Entity>()?)
            .is(true)
            .at(at.clone()))
    }

    fn entity_text(revision: &Revision) -> String {
        format!("{:?}", Value::Entity(revision.entity()))
    }

    /// Declare the session layer `session:state` on `branch`.
    async fn declare(branch: &Branch, env: &Operator, layer: &Entity) -> Result<()> {
        branch
            .transaction()
            .assert(LayerDeclaration::new(layer.clone(), LayerType::Session))
            .commit()
            .publish()
            .perform(env)
            .await?;
        branch.refresh(env).await?;
        Ok(())
    }

    /// A commit of a title through `branch`, moving its head.
    async fn commit(branch: &Branch, env: &Operator, title: &str) -> Result<()> {
        branch
            .transaction()
            .assert(
                dialog_query::the!("doc/title")
                    .of("doc:0".parse::<Entity>()?)
                    .is(title.to_string()),
            )
            .commit()
            .publish()
            .perform(env)
            .await?;
        Ok(())
    }

    /// A rule writing the primary's `dialog.branch/revision`, read in
    /// `session:`, into `session:state` as `seen/branch-revision`.
    fn watch_primary() -> Result<InductiveRule> {
        Ok(serde_json::from_value(serde_json::json!({
            "assert!": {
                "with": {
                    "revision": {
                        "the": "seen/branch-revision", "as": "Entity", "pick": "last",
                        "in": "session:state"
                    }
                }
            },
            "when": [
                {
                    "assert": {
                        "with": {
                            "revision": {
                                "the": "dialog.branch/revision", "as": "Entity",
                                "in": "session:"
                            }
                        }
                    },
                    "where": {
                        "this": { "?": { "name": "this" } },
                        "revision": { "?": { "name": "revision" } }
                    }
                }
            ]
        }))?)
    }

    /// A backup round-trips through `serde_json` whole: every layer's
    /// rows, revisions, the primary's branch and revision.
    #[dialog_common::test]
    async fn it_round_trips_through_serde_json() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state: Entity = "session:state".parse()?;
        declare(&branch, &operator, &state).await?;
        let stack = branch.stack().perform(&operator).await?;
        stack
            .assert(flag("doc:1", &state)?)
            .assert(flag("doc:2", &session_layer())?)
            .perform(&operator)
            .await?;
        let backup = stack.backup();
        assert_eq!(
            backup
                .layers()
                .iter()
                .map(|layer| layer.layer().clone())
                .collect::<Vec<_>>(),
            vec![session_layer(), state.clone()],
            "session: first, then the declared layers"
        );
        assert_eq!(
            Some(backup.branch()),
            stack.branch_of(&memory_layer()),
            "of the primary's branch"
        );
        assert_eq!(
            backup.primary(),
            stack.primary().revision().map(|at| at.entity()).as_ref(),
            "where the stack recorded the primary"
        );
        let bytes = serde_json::to_vec(&backup)?;
        let back: Backup = serde_json::from_slice(&bytes)?;
        assert_eq!(back, backup);
        Ok(())
    }

    /// Tonk's `export_overlays` / `import_overlays` is `backup` /
    /// `restore` across two stacks on two `Branch` handles of one
    /// repository: the restored stack reads every backed-up row of
    /// `session:` and of the declared session layer, and the selection,
    /// through its layers and through a query. What described the
    /// process that took the backup is not restored: its providers'
    /// discovery rows and the channel it created.
    #[dialog_common::test]
    async fn it_restores_every_session_row_on_a_fresh_stack() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state: Entity = "session:state".parse()?;
        declare(&branch, &operator, &state).await?;
        let stack = branch.stack().perform(&operator).await?;
        stack
            .assert(flag("doc:1", &state)?)
            .assert(flag("doc:2", &session_layer())?)
            .perform(&operator)
            .await?;
        stack.select(Some(state.clone())).perform(&operator).await?;
        let concept = dialog_query::ConceptDescriptor::try_from([(
            "node",
            dialog_query::AttributeDescriptor::new(
                dialog_query::the!("cmd.fetch/node"),
                "",
                Cardinality::One,
                None,
            ),
        )])?;
        stack.provide(crate::Provider::new(&concept, |_| async {
            Ok(Changes::new())
        }))?;
        let mailbox: Entity = "mail:box".parse()?;
        stack.channel(&mailbox)?;
        let bytes = serde_json::to_vec(&stack.backup())?;

        let handle = repo.branch("main").open().perform(&operator).await?;
        let restored = handle
            .stack()
            .restore(serde_json::from_slice(&bytes)?)
            .perform(&operator)
            .await?;
        let own = layer_of(&restored, &session_layer());
        assert_eq!(
            held(own, "ui/selected", &"doc:2".parse()?)?,
            vec!["Boolean(true)".to_string()],
            "session:'s row"
        );
        assert_eq!(
            held(
                layer_of(&restored, &state),
                "ui/selected",
                &"doc:1".parse()?
            )?,
            vec!["Boolean(true)".to_string()],
            "the declared layer's row"
        );
        assert_eq!(restored.selected(), Some(state.clone()), "the selection");
        let query = restored.query();
        let read = query
            .select(dialog_query::AttributeQuery::from(
                Term::<QueryRelation>::from(QueryRelation::from(
                    "ui/selected".parse::<dialog_corpus::Relation>()?,
                ))
                .of(Term::<Entity>::var("of"))
                .is(Term::<bool>::var("is")),
            ))
            .perform(&operator);
        let mut docs: Vec<String> = Output::try_vec(read)
            .await?
            .into_iter()
            .map(|claim| claim.of.to_string())
            .collect();
        docs.sort();
        assert_eq!(docs, vec!["doc:1".to_string(), "doc:2".to_string()]);
        assert!(
            held(own, "dialog.provider/command", &session_layer())?.is_empty(),
            "no provider is registered on the restored stack"
        );
        assert!(
            held(own, "dialog.layer/type", &mailbox)?.is_empty(),
            "nor the channel the other created"
        );
        assert_eq!(
            restored.layers(),
            handle.stack().perform(&operator).await?.layers()
        );
        assert!(
            handle.overlay().is_empty(),
            "nothing restored into the overlay"
        );
        Ok(())
    }

    /// A layer that moved while the backup was away is the restored
    /// stack's first dispatch's stimulus, once: the primary, committed
    /// through another handle between the backup and the restore, fires
    /// a rule premised on its `dialog.branch/revision` at the first
    /// dispatch and not at the second. The declared layer, which stood
    /// where its row said, is recorded again at open where its replayed
    /// rows left it, so it is no stimulus.
    #[dialog_common::test]
    async fn it_takes_a_layer_moved_while_backed_up_as_stimulus_once() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let state: Entity = "session:state".parse()?;
        declare(&branch, &operator, &state).await?;
        let stack = branch.stack().perform(&operator).await?;
        stack.assert(watch_primary()?).perform(&operator).await?;
        let primary = stack
            .branch_of(&memory_layer())
            .expect("the primary's branch")
            .clone();
        let backup = stack.backup();
        let backed_up = stack.primary().revision().expect("a head");
        assert_eq!(backup.primary(), Some(&backed_up.entity()));

        commit(&branch, &operator, "Away").await?;
        let handle = repo.branch("main").open().perform(&operator).await?;
        let moved = handle.revision().expect("a head");
        assert_ne!(moved, backed_up);
        let restored = handle.stack().restore(backup).perform(&operator).await?;
        let memory = layer_of(&restored, &state).clone();
        let own = layer_of(&restored, &session_layer()).clone();
        let chain = {
            let hash = memory.revision().hash;
            format!("{:?}", Value::String(hash.as_bytes().to_base58()))
        };
        assert_eq!(
            held(&own, "dialog.layer/revision", &state)?,
            vec![chain],
            "the declared layer is recorded where its replayed rows left it"
        );
        let seen = || held(&memory, "seen/branch-revision", &primary);
        assert_ne!(
            seen()?,
            vec![entity_text(&moved)],
            "not yet fired on the move"
        );

        let sequence = memory.revision().sequence;
        restored.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(seen()?, vec![entity_text(&moved)], "fired on the move");
        assert_eq!(memory.revision().sequence, sequence + 1, "once");
        restored.assert(Changes::new()).perform(&operator).await?;
        assert_eq!(
            memory.revision().sequence,
            sequence + 1,
            "and not again: nothing moved since"
        );
        Ok(())
    }

    /// A backup of a stack on one branch restored on another is refused
    /// with `ForeignBackup` before any layer is built, and nothing is
    /// written: not the branch's overlay, not its head.
    #[dialog_common::test]
    async fn it_refuses_a_backup_of_another_branch() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let main = repo.branch("main").open().perform(&operator).await?;
        let stack = main.stack().perform(&operator).await?;
        stack
            .assert(flag("doc:1", &session_layer())?)
            .perform(&operator)
            .await?;
        let other = repo.branch("other").open().perform(&operator).await?;
        let head = other.revision();
        let opened = other
            .stack()
            .restore(stack.backup())
            .perform(&operator)
            .await;
        assert!(
            matches!(opened, Err(CommitError::ForeignBackup { .. })),
            "{:?}",
            opened.err()
        );
        assert!(
            other.overlay().is_empty(),
            "nothing written into the overlay"
        );
        other.refresh(&operator).await?;
        assert_eq!(other.revision(), head, "nor committed");
        Ok(())
    }

    /// A backup naming a session layer the schema no longer declares
    /// restores the rest: `session:` and the layers that exist hold their
    /// rows, the undeclared layer is not built, and its revision row in
    /// `session:` is dropped with it.
    #[dialog_common::test]
    async fn it_restores_the_rest_of_a_backup_naming_an_undeclared_layer() -> Result<()> {
        let (operator, peer) = test_session_with_peer().await;
        let repo = test_repo(&operator, &peer).await;
        let branch = repo.branch("main").open().perform(&operator).await?;
        let (state, tab): (Entity, Entity) = ("session:state".parse()?, "session:tab".parse()?);
        declare(&branch, &operator, &state).await?;
        declare(&branch, &operator, &tab).await?;
        let stack = branch.stack().perform(&operator).await?;
        stack
            .assert(flag("doc:1", &state)?)
            .assert(flag("doc:2", &tab)?)
            .assert(flag("doc:3", &session_layer())?)
            .perform(&operator)
            .await?;
        let backup = stack.backup();
        assert_eq!(backup.layers().len(), 3);

        branch
            .transaction()
            .retract(LayerDeclaration::new(tab.clone(), LayerType::Session))
            .commit()
            .publish()
            .perform(&operator)
            .await?;
        let handle = repo.branch("main").open().perform(&operator).await?;
        let restored = handle.stack().restore(backup).perform(&operator).await?;
        assert!(
            restored.layer(&tab).is_none(),
            "the undeclared layer is not built"
        );
        assert!(!restored.layers().contains(&tab));
        let own = layer_of(&restored, &session_layer());
        assert_eq!(
            held(own, "ui/selected", &"doc:3".parse()?)?,
            vec!["Boolean(true)".to_string()]
        );
        assert_eq!(
            held(
                layer_of(&restored, &state),
                "ui/selected",
                &"doc:1".parse()?
            )?,
            vec!["Boolean(true)".to_string()]
        );
        assert!(
            held(own, "dialog.layer/revision", &tab)?.is_empty(),
            "the undeclared layer's row is dropped"
        );
        Ok(())
    }
}
