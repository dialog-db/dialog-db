use crate::Artifact;

#[cfg(doc)]
use crate::FactStoreMut;

/// The instruction variants that are accepted by [`FactStoreMut::commit`].
pub enum Instruction {
    /// Assert a [`Artifact`], persisting it in the [`FactStoreMut`]
    Assert(Artifact),
    /// Retract a [`Artifact`], removing it from the [`FactStoreMut`]
    Retract(Artifact),
}
