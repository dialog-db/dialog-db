//! Branch-scoped transient overlay.

use std::sync::{Arc, Mutex};

use dialog_artifacts::{Changes, Entity, Statement};

use crate::Branch;

/// Ephemeral session facts folded into every read of the branch —
/// queries, transaction queries, and standing subscriptions — but
/// never committed to the tree. Obtained via
/// [`Branch::overlay`]; shared across branch clones, so a fact
/// asserted through any clone is visible to readers of all of them.
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
