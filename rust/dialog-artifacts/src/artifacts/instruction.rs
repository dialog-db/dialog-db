//! Instructions for modifying artifacts in the store.
//!
//! This module defines the [`Instruction`] enum which represents operations
//! that can be applied to artifacts during commit transactions.

use crate::Artifact;

#[cfg(doc)]
use crate::ArtifactStoreMut;

/// The instruction variants that are accepted by [`ArtifactStoreMut::commit`].
pub enum Instruction {
    /// Assert a [`Artifact`], persisting it in the [`ArtifactStoreMut`]
    Assert(Artifact),
    /// Retract a [`Artifact`], removing it from the [`ArtifactStoreMut`]
    Retract(Artifact),
}
