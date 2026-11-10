//! Instructions for modifying facts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to facts during commit transactions.

use crate::{Fact, Pick};

/// The instruction variants a commit applies to the fact indexes.
pub enum Instruction {
    /// Add this [`Fact`] to the indexes under a pick: the pick
    /// its attribute is read under. Under [`Pick::All`] the write is
    /// purely additive, and any prior entries at the same `(entity,
    /// attribute)` are left in place. Under any other pick the one
    /// claim of the cell the pick elects among those stored, the
    /// claim a read under the pick returns, is retracted beside the
    /// new fact, whose `cause` is the elected claim's versions;
    /// every other claim of the cell stays. Asserting a value the cell
    /// already holds is a no-op.
    Assert(Fact, Pick),
    /// Retract a [`Fact`], removing it from the indexes.
    Retract(Fact),
}
