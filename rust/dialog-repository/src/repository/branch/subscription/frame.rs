//! What one poll reads: a [`Frame`], a position for each layer of what a
//! subscription reads, taken at once.
//!
//! A poll reads many times: the changes since the last poll, the
//! entities they touch, the rules, each touched entity derived again, or
//! the whole query. Every one of those reads goes to the frame the poll
//! is at, so what the poll reports is the change of the subscription's
//! answer from one frame to the next, of no other state, however the
//! branch and its layers move while the poll runs. A caller that polls
//! several subscriptions at one frame ([`Subscription::poll_at`]) has
//! them agree.
//!
//! A frame has a position for each layer it can hold still:
//!
//! - the branch's head, a revision, whose tree never changes;
//! - the branch's session overlay, and through a stack each of its
//!   memory layers, each frozen as it stood ([`Ephemeral::frozen`]): a
//!   value later writes to the line never change, taken for a few
//!   reference counts whatever the line holds;
//! - through a stack, each durable layer's revision, as the stack last
//!   read where the primary points it, each a tree that never changes.
//!
//! It has none for a channel, which a subscription does not read, and it
//! cannot hold still which layers a stack has, nor the remotes a read
//! that misses locally fetches from.
//!
//! [`Subscription::poll_at`]: super::Subscription::poll_at

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

use dialog_capability::Capability;
use dialog_corpus::Entity;
use dialog_effects::authority::Operator;
use dialog_query::error::EvaluationError;

use crate::repository::branch::session::{Composite, shared_metadata};
use crate::repository::source::Source;
use crate::{Branch, Ephemeral, Revision, Stack};

#[cfg(test)]
mod tests;

/// How many frames this process has taken: each frame is numbered as it
/// is taken, which is the order frames move forward in.
static TAKEN: AtomicU64 = AtomicU64::new(0);

/// Where everything a subscription reads stands at one moment: the
/// branch's head, its session overlay, and through a stack each memory
/// layer and each durable layer. Taken with [`Branch::frame`],
/// [`Stack::frame`] or [`Subscription::frame`](super::Subscription::frame),
/// and polled at with [`Subscription::poll_at`](super::Subscription::poll_at).
///
/// Taking one costs a reference count or two for each layer and copies
/// no fact. Each part is immutable once taken: a revision names a tree
/// that never changes, a frozen line shares with the live one only what
/// neither changes, and a durable layer is read at its revision. Holding
/// a frame keeps those values alive, so a write to a line while one is
/// held copies the part of the line it changes (see
/// [`Frozen`](crate::Frozen)).
///
/// Cheap to clone: one frame can be polled at by any number of the
/// subscriptions of what it was taken of.
#[derive(Clone)]
pub struct Frame {
    /// The frame's number among every frame this process took.
    taken: u64,
    /// The live session line of the branch the frame was taken of: which
    /// branch that is, for a subscription to check it reads it.
    origin: Ephemeral,
    /// The stack the frame was taken through, if any.
    stack: Option<Stack>,
    /// The branch at the head it stood at, read through a handle whose
    /// session line is the overlay as it stood.
    primary: Source,
    /// The session overlay's sequence when it was frozen.
    overlay: u64,
    /// Each memory layer of the stack, frozen, with its sequence.
    memories: Vec<(Entity, Ephemeral, u64)>,
    /// Each durable layer of the stack, at its revision.
    durables: Vec<(Entity, Source)>,
}

impl fmt::Debug for Frame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Frame")
            .field("taken", &self.taken)
            .field("revision", &self.revision())
            .field("overlay", &self.overlay)
            .field("memories", &self.memory_sequences())
            .field("durables", &self.durable_heads())
            .finish()
    }
}

/// Which parts of what a subscription reads moved from one frame to
/// another.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Moved {
    pub(super) head: bool,
    pub(super) overlay: bool,
    pub(super) memories: bool,
    pub(super) durables: bool,
}

impl Moved {
    /// Whether anything moved.
    pub(super) fn any(&self) -> bool {
        self.head || self.overlay || self.memories || self.durables
    }
}

impl Frame {
    /// Take a frame of `branch`, read through `stack` when there is one.
    ///
    /// A layer that stands where it stood in `last`, a frame of the same
    /// lines, is held as `last` holds it: a line's sequence names what it
    /// holds, so the value frozen then is the one it would freeze now,
    /// and a poll that finds nothing moved takes its frame for the
    /// reference counts alone.
    pub(super) fn take(branch: &Branch, stack: Option<&Stack>, last: Option<&Frame>) -> Self {
        let taken = TAKEN.fetch_add(1, Ordering::Relaxed);
        let last = last.filter(|last| last.is_of(branch, stack));
        let revision = branch.revision();
        let frozen = branch.overlay().frozen();
        let overlay = frozen.revision().sequence;
        let primary = match last {
            Some(last) if last.overlay == overlay => last.primary.rebased(revision),
            _ => Source::at(branch.with_overlay(Ephemeral::fixed(frozen)), revision),
        };
        let mut memories = Vec::new();
        let mut durables = Vec::new();
        if let Some(stack) = stack {
            let held = stack.memories();
            memories.reserve(held.len());
            for (index, (layer, memory)) in held.into_iter().enumerate() {
                let frozen = memory.frozen();
                let sequence = frozen.revision().sequence;
                let line = match last.and_then(|last| last.memories.get(index)) {
                    Some((named, line, at)) if *named == layer && *at == sequence => line.clone(),
                    _ => Ephemeral::fixed(frozen),
                };
                memories.push((layer, line, sequence));
            }
            let held = stack.durables();
            durables = held.sources_at(&held.pinned());
        }
        Frame {
            taken,
            origin: branch.overlay().clone(),
            stack: stack.cloned(),
            primary,
            overlay,
            memories,
            durables,
        }
    }

    /// The branch's head as the frame holds it: `None` before its first
    /// commit.
    pub fn revision(&self) -> Option<&Revision> {
        self.primary.revision()
    }

    /// The sequence the branch's session overlay stood at.
    pub fn overlay_sequence(&self) -> u64 {
        self.overlay
    }

    /// The sequence each memory layer of the stack stood at, in the
    /// stack's order; none for a frame of a branch alone.
    pub fn memory_sequences(&self) -> Vec<u64> {
        self.memories
            .iter()
            .map(|(_, _, sequence)| *sequence)
            .collect()
    }

    /// The revision each durable layer of the stack stood at, in the
    /// stack's order; none for a frame of a branch alone.
    pub fn durable_heads(&self) -> Vec<Option<Revision>> {
        self.durables
            .iter()
            .map(|(_, source)| source.revision().cloned())
            .collect()
    }

    /// Whether this frame was taken after `other`.
    pub fn is_after(&self, other: &Frame) -> bool {
        self.taken > other.taken
    }

    /// Whether this frame and `other` are one frame: taken together, or
    /// one a clone of the other.
    pub(super) fn is(&self, other: &Frame) -> bool {
        self.taken == other.taken
    }

    /// Whether this frame is of `branch` read through `stack`: what a
    /// subscription reading those may poll at.
    pub(super) fn is_of(&self, branch: &Branch, stack: Option<&Stack>) -> bool {
        let stacked = match (&self.stack, stack) {
            (None, None) => true,
            (Some(ours), Some(theirs)) => ours.same(theirs),
            _ => false,
        };
        stacked && self.origin.same(branch.overlay())
    }

    /// What moved from `before` to this frame.
    pub(super) fn moved_since(&self, before: &Frame) -> Moved {
        let durables = self.durables.len() != before.durables.len()
            || self
                .durables
                .iter()
                .zip(&before.durables)
                .any(|((_, now), (_, then))| now.revision() != then.revision());
        let memories = self.memories.len() != before.memories.len()
            || self
                .memories
                .iter()
                .zip(&before.memories)
                .any(|((_, _, now), (_, _, then))| now != then);
        Moved {
            head: self.revision() != before.revision(),
            overlay: self.overlay != before.overlay,
            memories,
            durables,
        }
    }

    /// The branch as the frame holds it, as a read source.
    pub(super) fn primary(&self) -> &Source {
        &self.primary
    }

    /// The memory layers as the frame froze them, by name, with the
    /// sequence each stood at.
    pub(super) fn memories(&self) -> &[(Entity, Ephemeral, u64)] {
        &self.memories
    }

    /// What a query reads at this frame: the branch, then each durable
    /// layer, each at its revision, and each memory layer frozen.
    pub(super) fn composite(&self) -> Composite {
        let mut sources = Vec::with_capacity(1 + self.durables.len());
        let mut names = Vec::with_capacity(1 + self.durables.len());
        sources.push(self.primary.clone());
        names.push(None);
        for (layer, source) in &self.durables {
            sources.push(source.clone());
            names.push(Some(layer.clone()));
        }
        Composite {
            sources,
            memories: self
                .memories
                .iter()
                .map(|(layer, memory, _)| (layer.clone(), memory.clone()))
                .collect(),
            names,
            channels: Vec::new(),
        }
    }

    /// The metadata a query over the branch folds in, as of the frame's
    /// head: the per-query overlay a query of the branch reads.
    pub(super) fn metadata(&self, operator: &Capability<Operator>) -> Ephemeral {
        shared_metadata(&[self.primary.as_view()], operator)
    }
}

impl Branch {
    /// Where this branch stands now, as one [`Frame`]: its head and its
    /// session overlay. A subscription of the branch polled at it
    /// ([`Subscription::poll_at`](super::Subscription::poll_at)) reads
    /// that and nothing later.
    pub fn frame(&self) -> Frame {
        Frame::take(self, None, None)
    }
}

impl Stack {
    /// Where this stack stands now, as one [`Frame`]: the primary's head
    /// and session overlay, each memory layer and each durable layer.
    /// Every subscription of the stack polled at it reads that, so they
    /// agree.
    pub fn frame(&self) -> Frame {
        Frame::take(self.primary(), Some(self), None)
    }
}

/// Why a poll at a frame was refused, or failed.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PollError {
    /// The frame was taken before the one the subscription last polled
    /// at: frames only move forward, and the subscription's answer is
    /// already of a later one.
    #[error("the frame was taken before the one the subscription last polled at")]
    Stale,
    /// The frame is of another branch, or of a stack the subscription
    /// does not read through.
    #[error("the frame is not of what the subscription reads")]
    Foreign,
    /// The evaluation failed.
    #[error(transparent)]
    Evaluation(#[from] EvaluationError),
}
