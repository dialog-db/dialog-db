//! One evaluation of a body for the rules of a round that state it.
//!
//! The seeded candidates of a round are partitioned by body
//! (`SharedBody::same` in `rules/induced.rs`): the same premises under
//! the same types, up to the names of variables. A part
//! of two rules or more is evaluated once, under its first rule, and
//! every other rule of it reads that rule's matches through a renaming
//! and writes its own head.
//!
//! ```text
//!  candidates of a round, in firing order      parts          evaluations
//!
//!  rule A   move ∧ list ∧ owner  ─▶ a ┐
//!  rule B   move ∧ list ∧ owner  ─▶ b ├─────── body 1 ─────── once, under A
//!  rule C   move ∧ list ∧ owner  ─▶ c ┘                       B and C read A's matches
//!  rule D   move ∧ team          ─▶ d ──────── alone ──────── once, as before
//!  rule E   (fired in full)      ─▶ e ──────── alone ──────── once, as before
//! ```
//!
//! What a rule writes from another rule's matches is what it writes
//! from its own when the order of the matches decides nothing. The two
//! evaluations find the same matches, but not in one order: a rule's
//! premises are seeded and planned in the order it lists them, which is
//! its own. [`order_decides`] says when the order would decide what is
//! written, and such a rule is evaluated on its own after all.

use std::collections::HashMap;
use std::sync::Arc;

use dialog_corpus::{Entity, Pick, Relation, Value};
use dialog_query::Match;

use super::WrittenHead;
use crate::rules::{Induced, Reads};

/// How one candidate of a round is fired.
pub(super) enum Part {
    /// On its own: it is fired in full, its body is not one a round can
    /// share, or no other candidate of the round states it.
    Alone,
    /// First of the rules stating its body: it is evaluated as it would
    /// be alone, and its matches kept for the others under this index.
    First(usize),
    /// Another rule stating the body of the `First` at this index, and
    /// how it reads that rule's matches.
    Other(usize, Reads),
}

/// The parts of one round's candidates and the matches kept for them.
pub(super) struct Round {
    parts: Vec<Part>,
    matches: Vec<Vec<Match>>,
}

impl Round {
    /// Partition a round's candidates, given in firing order: `None`
    /// for one that is fired in full, which shares nothing.
    pub(super) fn of<'r>(candidates: impl Iterator<Item = Option<&'r Arc<Induced>>>) -> Self {
        let candidates: Vec<Option<&Arc<Induced>>> = candidates.collect();
        let mut parts: Vec<Part> = candidates.iter().map(|_| Part::Alone).collect();
        #[cfg(test)]
        if probe::is_off() {
            return Round {
                parts,
                matches: Vec::new(),
            };
        }

        // The first candidate of each body met so far, and how many
        // candidates state it.
        let mut firsts: Vec<(usize, usize)> = Vec::new();
        for (at, candidate) in candidates.iter().enumerate() {
            let Some(body) = candidate.and_then(|induced| induced.body()) else {
                continue;
            };
            let found = firsts.iter().position(|(first, _)| {
                candidates[*first]
                    .and_then(|induced| induced.body())
                    .is_some_and(|known| known.bucket() == body.bucket() && known.same(body))
            });
            match found {
                Some(group) => {
                    let (first, members) = &mut firsts[group];
                    *members += 1;
                    let known = candidates[*first]
                        .and_then(|induced| induced.body())
                        .expect("a first has a body");
                    parts[at] = Part::Other(group, body.reads(known));
                }
                None => firsts.push((at, 1)),
            }
        }
        for (group, (first, members)) in firsts.iter().enumerate() {
            if *members > 1 {
                parts[*first] = Part::First(group);
            }
        }
        Round {
            parts,
            matches: firsts.iter().map(|_| Vec::new()).collect(),
        }
    }

    /// The part of the candidate at `at`, taken: each candidate is
    /// fired once.
    pub(super) fn take(&mut self, at: usize) -> Part {
        std::mem::replace(&mut self.parts[at], Part::Alone)
    }

    /// Keep what the `First` of `group` matched.
    pub(super) fn keep(&mut self, group: usize, matches: Vec<Match>) {
        self.matches[group] = matches;
    }

    /// What the `First` of `group` matched. It fires before the others
    /// of its part, which come after it in firing order.
    pub(super) fn matches(&self, group: usize) -> &[Match] {
        &self.matches[group]
    }
}

/// Whether the order `heads` are written in decides what is written.
///
/// A head's writes land in its cells, each cell keeping its writes in
/// the order they came. Writes to two cells do not meet, so the order
/// of two heads matters only where both write one cell, and there only
/// when they do not write it the same: two values under `last`, of
/// which the later stands, or two members of a set, which are kept in
/// the order written. Heads that write a cell the same leave it the
/// same in either order.
pub(super) fn order_decides(heads: &[WrittenHead]) -> bool {
    if heads.len() < 2 {
        return false;
    }
    #[cfg(test)]
    if probe::is_unordered() {
        return false;
    }
    type Writes<'a> = Vec<(&'a Value, &'a Pick)>;
    let mut written: HashMap<(&Entity, &Relation), Writes<'_>> =
        HashMap::with_capacity(heads.len());
    for (this, cells) in heads {
        for (at, (relation, _, _)) in cells.iter().enumerate() {
            // A relation this head writes twice is taken once, at its
            // first field, with every write in field order.
            if cells[..at]
                .iter()
                .any(|(earlier, _, _)| earlier == relation)
            {
                continue;
            }
            let writes: Writes<'_> = cells[at..]
                .iter()
                .filter(|(later, _, _)| later == relation)
                .map(|(_, value, pick)| (value, pick))
                .collect();
            match written.get(&(this, relation)) {
                Some(known) if *known != writes => return true,
                Some(_) => {}
                None => {
                    written.insert((this, relation), writes);
                }
            }
        }
    }
    false
}

/// In tests: sharing turned off, so the same commits can be run both
/// ways, and counts of what a round shared, so a test of sharing can
/// tell that it happened.
///
/// `DIALOG_TEST_SHARE` turns a part off for a whole run of the tests,
/// to see which of them hold it up: `off` shares nothing, which the
/// tests that count what was shared then fail on, and `unordered`
/// writes from shared matches whatever their order decides, which the
/// check of a shared firing against the rule's own evaluation then
/// fails on wherever the two orders differ.
#[cfg(test)]
pub(super) mod probe {
    use std::cell::Cell;
    use std::sync::OnceLock;

    /// What `DIALOG_TEST_SHARE` asks for, read once.
    fn asked() -> &'static str {
        static ASKED: OnceLock<String> = OnceLock::new();
        ASKED.get_or_init(|| std::env::var("DIALOG_TEST_SHARE").unwrap_or_default())
    }

    /// Whether the order of matches is left to decide, for a run that
    /// shows what the guard is for.
    pub(super) fn is_unordered() -> bool {
        asked() == "unordered"
    }

    // Per thread: the test runtime drives each test on one thread.
    thread_local! {
        static OFF: Cell<bool> = const { Cell::new(false) };
        static SHARED: Cell<usize> = const { Cell::new(0) };
        static OWN: Cell<usize> = const { Cell::new(0) };
    }

    /// Turn sharing off or back on for this thread.
    pub(crate) fn off(off: bool) {
        OFF.with(|cell| cell.set(off));
    }

    pub(super) fn is_off() -> bool {
        OFF.with(Cell::get) || asked() == "off"
    }

    /// Note a rule that wrote its head from another rule's matches.
    pub(crate) fn shared() {
        SHARED.with(|cell| cell.set(cell.get() + 1));
    }

    /// Note a rule that stated a shared body and was evaluated on its
    /// own because the order of its matches decides what it writes.
    pub(crate) fn own() {
        OWN.with(|cell| cell.set(cell.get() + 1));
    }

    /// How many rules wrote from another rule's matches, and how many
    /// were evaluated on their own for the order's sake, on this thread
    /// since the last call.
    pub(crate) fn take() -> (usize, usize) {
        (
            SHARED.with(|cell| cell.replace(0)),
            OWN.with(|cell| cell.replace(0)),
        )
    }
}

#[cfg(test)]
mod tests;
