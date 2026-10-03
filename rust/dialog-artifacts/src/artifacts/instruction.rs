//! Instructions for modifying artifacts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to artifacts during commit transactions.

use crate::Artifact;

/// The instruction variants a commit applies to the artifact indexes.
pub enum Instruction {
    /// Add this [`Artifact`] to the indexes. Purely additive:
    /// any prior entries at the same `(entity, attribute)` are left in
    /// place. Use [`Instruction::Replace`] for cardinality-one supersession.
    Assert(Artifact),
    /// Replace any prior artifact at the same `(entity, attribute)` with this
    /// one, regardless of value. Every existing entry for the pair is
    /// removed from all three indexes; the new artifact's `cause` is
    /// populated from the first prior found. Asserting the same value that
    /// already exists is a no-op.
    Replace(Artifact),
    /// Retract a [`Artifact`], removing it from the indexes.
    Retract(Artifact),
}
