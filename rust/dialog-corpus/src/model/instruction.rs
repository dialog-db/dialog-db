//! Instructions for modifying facts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to facts during commit transactions.

use crate::{Cardinality, Fact};

/// The instruction variants a commit applies to the fact indexes.
#[derive(Clone)]
pub enum Instruction {
    /// Add this [`Fact`] to the indexes under its attribute's
    /// cardinality. Under [`Cardinality::Many`] the write is purely
    /// additive, and any prior entries at the same `(entity, attribute)`
    /// are left in place. Under [`Cardinality::One`] the newest claim of
    /// the cell among those stored, the claim a read returns, is
    /// retracted beside the new fact, whose `cause` is that claim's
    /// versions; every other claim of the cell stays. Asserting a value
    /// the cell already holds is a no-op.
    Assert(Fact, Cardinality),
    /// Retract a [`Fact`], removing it from the indexes.
    Retract(Fact),
}
