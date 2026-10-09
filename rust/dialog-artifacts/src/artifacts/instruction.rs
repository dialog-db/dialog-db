//! Instructions for modifying artifacts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to artifacts during commit transactions.

use crate::{Artifact, Policy};

/// The instruction variants a commit applies to the artifact indexes.
pub enum Instruction {
    /// Add this [`Artifact`] to the indexes under a policy: the policy
    /// its attribute is read under. Under [`Policy::All`] the write is
    /// purely additive, and any prior entries at the same `(entity,
    /// attribute)` are left in place. Under any other policy the one
    /// claim of the cell the policy elects among those stored, the
    /// claim a read under the policy returns, is retracted beside the
    /// new artifact, whose `cause` is the elected claim's versions;
    /// every other claim of the cell stays. Asserting a value the cell
    /// already holds is a no-op.
    Assert(Artifact, Policy),
    /// Retract a [`Artifact`], removing it from the indexes.
    Retract(Artifact),
}
