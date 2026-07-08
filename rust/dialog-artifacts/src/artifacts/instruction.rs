//! Instructions for modifying artifacts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to artifacts during commit transactions.

use crate::Artifact;

#[cfg(doc)]
use crate::ArtifactStoreMut;

/// The instruction variants that are accepted by [`ArtifactStoreMut::commit`].
pub enum Instruction {
    /// Add this [`Artifact`] to the [`ArtifactStoreMut`]. Purely additive:
    /// any prior entries at the same `(entity, attribute)` are left in
    /// place. Use [`Instruction::Replace`] for cardinality-one supersession.
    Assert(Artifact),
    /// Replace any prior artifact at the same `(entity, attribute)` with this
    /// one, regardless of value. Every *different-valued* entry for the pair
    /// is removed from all three indexes and this artifact is inserted with
    /// its `cause` unchanged. Asserting the same value that already exists is
    /// a no-op (the prior is left in place; nothing is written).
    ///
    /// Note: `cause` is inserted verbatim — the superseded priors are *not*
    /// cited on the new artifact. Populating `cause` from them (multi-parent
    /// `Cause(Vec<Version>)`) is the province of `notes/version-control.md`,
    /// not this layer; today production writes carry `cause: None`.
    Replace(Artifact),
    /// Retract a [`Artifact`], removing it from the [`ArtifactStoreMut`]
    Retract(Artifact),
}
