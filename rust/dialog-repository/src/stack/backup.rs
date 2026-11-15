//! Backing up a stack's session layers, and restoring them on a stack
//! opened afresh, in this process or another.
//!
//! A stack's [`SESSION`](crate::SESSION) layer and the session layers
//! its schema declares ([`LayerType::Session`](crate::LayerType::Session))
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
    /// The layer's name: [`session_layer`](crate::session_layer) or a
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
            Change::Assert(is, pick) => kept.associate(the.clone(), of.clone(), is.clone(), pick.clone()),
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
        let mut layers = vec![LayerBackup::of(session_layer(), own, own.settled().as_bytes())];
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
