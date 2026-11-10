//! Instructions for modifying artifacts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to artifacts during commit transactions.

use crate::{Artifact, Pick};

/// The instruction variants a commit applies to the artifact indexes.
pub enum Instruction {
    /// Add this [`Artifact`] to the indexes under a pick: the pick
    /// its attribute is read under. Under [`Pick::All`] the write is
    /// purely additive, and any prior entries at the same `(entity,
    /// attribute)` are left in place. Under any other pick the one
    /// claim of the cell the pick elects among those stored, the
    /// claim a read under the pick returns, is retracted beside the
    /// new artifact, whose `cause` is the elected claim's versions;
    /// every other claim of the cell stays. Asserting a value the cell
    /// already holds is a no-op.
    Assert(Artifact, Pick),
    /// Retract a [`Artifact`], removing it from the indexes.
    Retract(Artifact),
}
