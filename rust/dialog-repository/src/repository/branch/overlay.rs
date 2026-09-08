//! Branch-scoped transient overlay.

use std::sync::{Arc, Mutex};

use dialog_artifacts::{Changes, Entity, Instruction, Statement, Update as _};

use crate::Branch;

/// Ephemeral session facts folded into every read of the branch —
/// queries, transaction queries, and standing subscriptions — but
/// never committed to the tree. Obtained via
/// [`Branch::overlay`]; shared across branch clones, so a fact
/// asserted through any clone is visible to readers of all of them.
///
/// This is the store behind the [`Procedural`](crate::Layer::Procedural)
/// layer: a transaction routes every instruction whose attribute is
/// placed there into the overlay instead of the tree, so the usual
/// way to write session facts is to declare the attribute's
/// [`Placement`](crate::Placement) once and then `assert` through an
/// ordinary transaction. The direct [`assert`](Self::assert) /
/// [`retract`](Self::retract) surface below stays for facts that are
/// session-scoped by circumstance rather than by schema.
///
/// Asserts surface alongside branch facts; retracts tombstone
/// matching branch facts for readers without touching the tree.
/// Every mutation bumps an epoch that subscriptions snapshot at each
/// poll: a poll re-evaluates when the epoch moved even though the
/// tree did not, which is how overlay changes propagate to the
/// branch's subscriptions.
#[derive(Debug, Clone, Default)]
pub struct Overlay {
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    /// Bumped on every mutation. Subscriptions pin the epoch they
    /// last evaluated at; an off-tree change is invisible to the
    /// poll's tree-diff gate, so the epoch is what re-triggers.
    epoch: u64,
    changes: Changes,
}

impl Overlay {
    /// Assert an ephemeral statement into the session overlay.
    pub fn assert<S: Statement>(&self, statement: S) -> &Self {
        let mut state = self.state.lock().expect("overlay lock");
        statement.assert(&mut state.changes);
        state.epoch += 1;
        self
    }

    /// Retract a statement for the session: matching branch facts
    /// are tombstoned for readers; the tree is untouched.
    pub fn retract<S: Statement>(&self, statement: S) -> &Self {
        let mut state = self.state.lock().expect("overlay lock");
        statement.retract(&mut state.changes);
        state.epoch += 1;
        self
    }

    /// Drop every session fact recorded for entities that fail
    /// `keep` — asserts and retracts alike, removed outright rather
    /// than tombstoned. Bumps the epoch only when something was
    /// removed, so subscriptions re-evaluate exactly when the
    /// overlay's readable contents changed. Returns whether anything
    /// was removed.
    ///
    /// This is the garbage-collection primitive for per-session
    /// facts keyed by short-lived entities (e.g. a service worker's
    /// per-client `site:` stamps): retracting them would grow the
    /// overlay with tombstones, clearing would drop unrelated
    /// sessions' facts.
    pub fn retain_entities<F: FnMut(&Entity) -> bool>(&self, keep: F) -> bool {
        let mut state = self.state.lock().expect("overlay lock");
        let changed = state.changes.retain_entities(keep);
        if changed {
            state.epoch += 1;
        }
        changed
    }

    /// Land a settled batch of session-layer instructions: the
    /// procedural half of a transaction commit (see
    /// [`placement`](crate::placement)). Unlike
    /// [`retract`](Self::retract), a retract here removes the fact
    /// from the session outright — the session *is* the store for a
    /// procedural attribute, so there is no tree fact to tombstone —
    /// and falls back to a tombstone only when the session held no
    /// such fact. Bumps the epoch once for the whole batch, so the
    /// branch's subscriptions re-evaluate exactly once.
    pub(crate) fn apply(&self, changes: Changes) {
        if changes.is_empty() {
            return;
        }
        let mut state = self.state.lock().expect("overlay lock");
        for instruction in changes.into_instructions() {
            match instruction {
                Instruction::Assert(a) => state.changes.associate(a.the, a.of, a.is),
                Instruction::Replace(a) => state.changes.associate_unique(a.the, a.of, a.is),
                Instruction::Retract(a) => {
                    if !state.changes.cancel(&a.the, &a.of, &a.is) {
                        state.changes.dissociate(a.the, a.of, a.is);
                    }
                }
            }
        }
        state.epoch += 1;
    }

    /// Drop every session fact.
    pub fn clear(&self) -> &Self {
        let mut state = self.state.lock().expect("overlay lock");
        state.changes = Changes::new();
        state.epoch += 1;
        self
    }

    /// The current session changes, folded into a [`QueryLayer`] at
    /// construction so every read path sees them.
    ///
    /// [`QueryLayer`]: crate::QueryLayer
    pub(crate) fn changes(&self) -> Changes {
        self.state.lock().expect("overlay lock").changes.clone()
    }

    /// The current epoch: subscriptions compare it against the one
    /// they pinned to decide whether the overlay moved.
    pub(crate) fn epoch(&self) -> u64 {
        self.state.lock().expect("overlay lock").epoch
    }
}

impl Branch {
    /// The branch's transient session overlay: assert or retract
    /// ephemeral facts that every read of this branch observes but
    /// no commit persists. See [`Overlay`].
    pub fn overlay(&self) -> &Overlay {
        &self.overlay
    }
}
